import logging as log
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from ala import ala_config, cluster_setup
from ala.ala_helper import (
    step_bash_cmd,
    get_dr_count,
    get_default_args,
    get_success_notification_operator,
    get_metadata_as_json,
    update_registry_metadata,
)

DAG_ID = "Preingest_datasets"

datasetIds = "{{ dag_run.conf['datasetIds'] }}"
load_images = "{{ dag_run.conf['load_images'] }}"
instanceType = "{{ dag_run.conf['instanceType'] }}"
extra_args = "{{ dag_run.conf['extra_args'] }}"
override_uuid_percentage_check = "{{ dag_run.conf['override_uuid_percentage_check'] }}"


def setup_cluster_init(datasetIds, inst_type, extra_args, run_id_path, **kwargs):
    cluster_setup.setup_cluster(
        dag_id=DAG_ID,
        dataset_ids=datasetIds,
        cluster_type=cluster_setup.ClusterType.PREINGESTION,
        inst_type=inst_type,
        **kwargs,
    )
    steps = []
    for dr in datasetIds.split():
        dwca_loc = f"/data/dmgt"
        hdfs_s3_dwca_loc = f"s3://{ala_config.S3_BUCKET_DWCA}/dwca-imports"
        steps.extend(
            [
                step_bash_cmd(
                    f"Preingest {dr}",
                    f" /data/dmgt/preingestion/preingest.sh \
                          {ala_config.ENVIRONMENT_TYPE} \
                          {dr} \
                          {hdfs_s3_dwca_loc} \
                          {dwca_loc} \
                          {ala_config.BACKUP_LOCATION} \
                          {ala_config.COLLECTORY_SERVER} \
                          {ala_config.ALA_API_KEY} \
                          {ala_config.SOLR_URL} \
                          '{extra_args}' \
                          {run_id_path}",
                )
            ]
        )
    kwargs["ti"].xcom_push(key="steps", value=steps)


with DAG(
    dag_id=DAG_ID,
    description="Running preingestion job for multiple drs",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "preingestion"],
    params={
        "datasetIds": "dr1411 dr8128",
        "load_images": "false",
        "instanceType": ala_config.EC2_SMALL_INSTANCE_TYPE,
        "extra_args": "{}",
        "override_uuid_percentage_check": "false",
    },
) as dag:

    def get_report_name(**kwargs):
        registry_uids = kwargs["dag_run"].conf["datasetIds"]
        run_id_path = ""
        log.info(f"Checking param: {registry_uids}")
        uid_list = registry_uids.split()
        if len(uid_list) > 1:
            if any(uid.startswith("dp") for uid in uid_list):
                raise AirflowSkipException
                log.info("Preingest dp can only be run by itself")
        elif len(uid_list) == 1:
            uid = uid_list[0]
            if uid.startswith("dp"):
                current_time = datetime.now()
                str_timestamp = current_time.strftime("%Y%m%dT%H%M%S")
                run_id = f".{uid}-{str_timestamp}"
                run_id_path = f"s3://{ala_config.S3_BUCKET_AVRO}/preingestion-report/{run_id}"
        kwargs["ti"].xcom_push(key="run_id_path", value=run_id_path)

    get_report_name = PythonOperator(task_id="get_report_name", python_callable=get_report_name, provide_context=True)

    cluster_creator = PythonOperator(
        task_id="setup_cluster",
        python_callable=setup_cluster_init,
        op_kwargs={
            "datasetIds": datasetIds,
            "inst_type": instanceType,
            "extra_args": extra_args,
            "run_id_path": "{{ task_instance.xcom_pull(task_ids='get_report_name', key='run_id_path') }}",
        },
        provide_context=True,
    )

    preingest_drs = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='setup_cluster', key='job_flow_id') }}",
        aws_conn_id="aws_default",
        steps="{{ task_instance.xcom_pull(task_ids='setup_cluster', key='steps') }}",
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('setup_cluster', key='job_flow_id') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    wait_for_termination = EmrJobFlowSensor(
        task_id="wait_for_cluster_termination",
        job_flow_id="{{ task_instance.xcom_pull('setup_cluster', key='job_flow_id') }}",
        aws_conn_id="aws_default",
    )

    def update_checked_date(**kwargs):

        def update_last_checked(dr_uid):
            try:
                update_registry_metadata(
                    ala_config.COLLECTORY_SERVER,
                    dr_uid,
                    ala_config.ALA_API_KEY,
                    {'lastChecked': datetime.now().strftime("%Y-%m-%dT%H:%M:%S")},
                )
                log.info(f"Updated lastChecked for dr {uid}")
            except Exception as e:
                log.error(f"Error updating lastChecked for dr {dr_uid}: {e}")

        datasetIds = kwargs["dag_run"].conf["datasetIds"]
        log.info("update_checked_date uids: %s", datasetIds)

        for uid in datasetIds.split():
            if uid.startswith("dr"):
                update_last_checked(uid)

            elif uid.startswith("dp"):
                md_json = get_metadata_as_json(ala_config.COLLECTORY_SERVER, uid, ala_config.ALA_API_KEY)
                log.info(f"Going to update lastChecked for data resources in dp {uid}")
                for dr in md_json.get("dataResources", []):
                    update_last_checked(dr['uid'])

    update_checked_date_task = PythonOperator(
        task_id="update_checked_date",
        python_callable=update_checked_date,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def get_datasets(**kwargs):
        datasetIds = kwargs["dag_run"].conf["datasetIds"]
        log.info("get_datasets uids: %s", datasetIds)
        ti = kwargs["ti"]
        run_id_path = ti.xcom_pull(task_ids="get_report_name", key="run_id_path")
        log.info("get_datasets run id path: %s", run_id_path)
        if run_id_path:
            import boto3
            from urllib.parse import urlparse

            s3 = boto3.resource("s3")
            path_result = urlparse(run_id_path)
            report_path = path_result.path[1:] if path_result.path.startswith("/") else path_result.path
            dr_list_str = s3.Object(path_result.hostname, report_path).get()["Body"].read().decode("utf-8")
            log.info(f"DR list: {dr_list_str}")
            kwargs["ti"].xcom_push(key="datasets", value=dr_list_str)
        else:
            kwargs["ti"].xcom_push(key="datasets", value=datasetIds)

    get_datasets = PythonOperator(
        task_id="get_datasets", python_callable=get_datasets, provide_context=True, trigger_rule=TriggerRule.ALL_SUCCESS
    )

    def check_for_batching(**kwargs):
        ti = kwargs["ti"]
        datasets = ti.xcom_pull(task_ids="get_datasets", key="datasets")
        log.info("check_for_batching datasets: %s", datasets)
        dataset_list = datasets.split()
        if len(dataset_list) == 0:
            log.info("No datasets to ingest, skipping ingest dataset")
            return "skip_ingest"
        elif len(dataset_list) > ala_config.MIN_DRS_PER_BATCH:
            log.info(f"Found {len(dataset_list)} datasets for ingestion, processing in batches")
            return "ingest_batch"
        else:
            return "ingest_datasets"

    # Check trigger rule explanation: #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    check_for_batching = BranchPythonOperator(
        task_id="check_for_batching",
        provide_context=True,
        python_callable=check_for_batching,
        do_xcom_push=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    skip_ingest = EmptyOperator(task_id="skip_ingest", trigger_rule=TriggerRule.ALL_SUCCESS)

    ingest_batch = EmptyOperator(task_id="ingest_batch", trigger_rule=TriggerRule.ALL_SUCCESS)

    ingest_datasets = TriggerDagRunOperator(
        task_id="ingest_datasets",
        trigger_dag_id="Load_dataset",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        conf={
            "datasetIds": "{{ task_instance.xcom_pull(task_ids='get_datasets', key='datasets') }}",
            "load_images": f"{load_images}",
            "override_uuid_percentage_check": override_uuid_percentage_check,
            "skip_collectory_download": "true",
            "run_indexing": "false",
        },
    )

    def delete_run_id(**kwargs):
        ti = kwargs["ti"]
        run_id_path = ti.xcom_pull(task_ids="get_report_name", key="run_id_path")
        if run_id_path:
            import boto3
            from urllib.parse import urlparse

            s3 = boto3.resource("s3")
            path_result = urlparse(run_id_path)
            report_path = path_result.path[1:] if path_result.path.startswith("/") else path_result.path
            s3.Object(path_result.hostname, report_path).delete()
            log.info(f"Run id deleted {run_id_path}")

    delete_run_id = PythonOperator(
        task_id="delete_run_id",
        python_callable=delete_run_id,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    with TaskGroup(group_id="ingest_batch_task_grp") as ingest_batch_task_grp:

        def create_batches_func(**kwargs):
            ti = kwargs["ti"]
            datasets = ti.xcom_pull(task_ids="get_datasets", key="datasets")
            dataset_list = datasets.split()
            dataset_batches = [""] * ala_config.NO_OF_DATASET_BATCHES
            import math

            no_of_dataset_batches = min(
                math.ceil(len(dataset_list) / ala_config.MIN_DRS_PER_BATCH), ala_config.NO_OF_DATASET_BATCHES
            )
            for idx, dataset in enumerate(dataset_list):
                batch = idx % no_of_dataset_batches
                dataset_batches[batch] = dataset + " " + dataset_batches[batch]

            for batch_no in range(ala_config.NO_OF_DATASET_BATCHES):
                kwargs["ti"].xcom_push(key=f"batch{str(batch_no)}", value=dataset_batches[batch_no])

        create_batches = PythonOperator(
            task_id="create_batches",
            python_callable=create_batches_func,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        def generate_task(batch_no: int):
            # Due to limitation of current airflow version (2.2), the number of task batches needs to be known before runtime.
            # Hence, if the total datasets are less than NO_OF_DATASET_BATCHES * MIN_DRS_PER_BATCH, some batches may be empty
            # the empty batch task will still be triggered, however, it won't launch the emr cluster if it's empty.
            return TriggerDagRunOperator(
                task_id=f"ingest_batch{batch_no + 1}",
                trigger_dag_id="Load_dataset",
                wait_for_completion=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                conf={
                    "datasetIds": "{{ task_instance.xcom_pull(task_ids='ingest_batch_task_grp.create_batches', key='batch"
                    + str(batch_no)
                    + "') }}",
                    "load_images": f"{load_images}",
                    "override_uuid_percentage_check": override_uuid_percentage_check,
                    "skip_collectory_download": "true",
                },
            )

        check_ingest_batches = EmptyOperator(
            task_id="check_ingest_batches", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        (
            create_batches
            >> [generate_task(batch) for batch in range(ala_config.NO_OF_DATASET_BATCHES)]
            >> check_ingest_batches
        )

    (
        get_report_name
        >> update_checked_date_task
        >> cluster_creator
        >> preingest_drs
        >> step_checker
        >> wait_for_termination
        >> get_datasets
        >> check_for_batching
        >> [skip_ingest, ingest_datasets, ingest_batch]
    )
    skip_ingest >> delete_run_id
    ingest_datasets >> delete_run_id
    ingest_batch >> ingest_batch_task_grp >> delete_run_id
    delete_run_id >> get_success_notification_operator()
