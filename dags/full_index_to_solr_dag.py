#
# SOLR indexing for all datasets.
# This will recreate the full index and swap the SOLR alias on successful completion.
# It will also remove old collections.
#
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from distutils.util import strtobool
from datetime import date, timedelta, datetime
import logging
from ala import ala_config, ala_helper, cluster_setup
from ala.ala_helper import step_bash_cmd, s3_cp, emr_python_step, get_default_args

DAG_ID = 'Full_index_to_solr'

solrCollectionName = f"{ala_config.SOLR_COLLECTION}-" + datetime.now().strftime("%Y-%m-%d-%H-%M")

def get_spark_steps(solr_collection_name, include_sampling, include_jack_knife, include_clustering, include_outlier, num_partitions):

    return [
        emr_python_step("a. Create SOLR collection",
                        f"/tmp/create_solr_collection_cli.py -c {ala_config.SOLR_CONFIGSET} -s {ala_config.SOLR_URL}"
                        f" -a create_solr_collection {solrCollectionName}"),
        emr_python_step("b. Add initial replicas",
                        f"/tmp/create_solr_collection_cli.py -c {ala_config.SOLR_CONFIGSET} -s {ala_config.SOLR_URL}"
                        f" -a add_initial_replicas {solrCollectionName}"),
        s3_cp("c. Copy Sampling and IndexRecord to S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets", f"hdfs:///pipelines-all-datasets"),
        step_bash_cmd("d. Sampling pipeline", f" la-pipelines sample all --cluster"),
        step_bash_cmd("e. JackKnife pipeline", f" la-pipelines jackknife all --cluster"),
        step_bash_cmd("f. Clustering", f" la-pipelines clustering all --cluster"),
        s3_cp("g. Copy Outlier from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier", "hdfs:///pipelines-outlier", action_on_failure="CONTINUE"),
        s3_cp("e. Copy Annotations from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-annotations/", "hdfs:///pipelines-annotations/", action_on_failure="CONTINUE"),
        step_bash_cmd("h. Outlier distribution", f" la-pipelines outlier all --cluster"),
        step_bash_cmd("i. SOLR indexing", f" la-pipelines solr all --cluster --extra-args=\"numOfPartitions={num_partitions},solrCollection={solr_collection_name},includeSampling={include_sampling},includeJackKnife={include_jack_knife},includeClustering={include_clustering},includeOutlier={include_outlier}\""),
        step_bash_cmd(f'j. Commit the changes to the collection {solr_collection_name}', f'sudo -u hadoop curl -X POST "{ala_config.SOLR_URL}/{solr_collection_name}/update?commit=true&waitSearcher=true"'),
        emr_python_step("k. Add additional replicas",
                        f"/tmp/create_solr_collection_cli.py -c {ala_config.SOLR_CONFIGSET} -s {ala_config.SOLR_URL} -r {ala_config.SOLR_REPLICATION_FACTOR}"
                        f" -a add_additional_replicas {solr_collection_name}"),
        step_bash_cmd("l. Clear S3 outlier ", f" sudo -u hadoop aws s3 rm s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier/all --recursive"),
        step_bash_cmd("m. Delete temp SCP directories created by s3-dist-cp", f" sudo -u hadoop hdfs dfs -rm -f \"/pipelines-outlier/pipelines-outlier_\\$folder\\$\"", action_on_failure='CONTINUE'),
        s3_cp("n. Copy Outliers results to S3", "hdfs:///pipelines-outlier", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier", action_on_failure='CONTINUE'),
        s3_cp("o. Copy Sampling results to S3", "hdfs:///pipelines-all-datasets/sampling", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/sampling", action_on_failure='CONTINUE'),
        s3_cp("p. Copy Sampling metrics to S3", "hdfs:///pipelines-all-datasets/sampling-metrics.yml", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/", action_on_failure='CONTINUE'),
        emr_python_step(f'k. Check index and update collection alias',
                        f'/tmp/update_collection_alias_cli.py --solr_base {ala_config.SOLR_URL} --new_collection {solr_collection_name}'
                        f' --collection_to_keep {ala_config.SOLR_COLLECTION_TO_KEEP} --solr_alias {ala_config.SOLR_COLLECTION} '
                        f' --old_collection {ala_config.SOLR_COLLECTION}  auto_all')
    ]


with DAG(
    dag_id=DAG_ID,
    description="SOLR indexing for all datasets. This will recreate the full index and swap the SOLR alias on successful completion",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=12),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr','all-datasets'],
    params={
        "includeSampling": "true",
        "includeJackKnife": "true",
        "includeClustering": "true",
        "includeOutlier": "true",
        "skipImageSync": "false",
        "time_range": (None, None)
    }
) as dag:

    new_collection = f'{ala_config.SOLR_COLLECTION}-{datetime.now().strftime("%Y-%m-%d-%H-%M")}'

    def check_image_sync_flag(**kwargs):
        skip_image_sync = strtobool(kwargs['dag_run'].conf['skipImageSync'])
        logging.info(f"skipImageSync:{skip_image_sync}")
        if not skip_image_sync:
            return 'image_sync'
        return 'full_index_to_solr'


    check_image_sync_flag = BranchPythonOperator(
        task_id="check_image_sync_flag",
        provide_context=True,
        python_callable=check_image_sync_flag,
        do_xcom_push=False
    )


    def get_drs_for_image_sync_index(**kwargs):
        bucket = ala_config.S3_BUCKET_AVRO
        logging.info(
            "Getting the list of DRS from image-service that have new images uploaded since last successful index.")
        logging.info(f"Time range: {kwargs['dag_run'].conf['time_range']}")
        time_range = None if 'time_range' not in kwargs['dag_run'].conf else list(
            kwargs['dag_run'].conf['time_range'])
        if time_range and (time_range[0] or time_range[1]):
            logging.info(f"Using time range for image sync from: {time_range[0]} to {time_range[1]}")
            drs_to_be_image_synced = ala_helper.list_drs_ingested_since(bucket=bucket, time_range=time_range)
        else:
            logging.info(
                f"No time ranges specified and going to get the list of DRs from image-service that have new images"
                f" uploaded since last successful index.")
            index_date = ala_helper.read_solr_collection_date()
            one_week_before_index_date = (date.fromisoformat(index_date) - timedelta(days=7)).isoformat()
            logging.info(
                f"Last successful solr collection date: {index_date}. Getting image service uploaded date: {one_week_before_index_date}")
            images_drs_info = ala_helper.get_recent_images_drs(one_week_before_index_date)
            logging.info(f"Found {len(images_drs_info)} DRs with new images uploaded since {one_week_before_index_date}. "
                         f"Drs: {images_drs_info}")
            avro_drs = ala_helper.list_drs_index_avro_in_bucket(bucket=bucket,
                                                                time_range=[one_week_before_index_date, None])
            logging.info(f"Found {len(avro_drs)} DRs ingested since since {one_week_before_index_date}. Drs: {avro_drs}")
            drs_to_be_image_synced = set(images_drs_info.keys()).intersection(set(avro_drs.keys()))
            if not drs_to_be_image_synced:
                logging.warning("No DRs found for image sync")
            else:
                logging.info(f"{len(drs_to_be_image_synced)} DRs need image synchronisation: {drs_to_be_image_synced}")

        return ' '.join(drs_to_be_image_synced)


    get_drs_for_image_sync_index = PythonOperator(
        task_id='get_drs_for_image_sync_index',
        python_callable=get_drs_for_image_sync_index,
        provide_context=True
    )


    def check_image_sync_count(**kwargs):
        ti = kwargs['ti']
        datasets = ti.xcom_pull(task_ids='get_drs_for_image_sync_index')
        if datasets is None:
            logging.warning("No datasets found for image sync")
            return 'full_index_to_solr'
        dataset_list = datasets.split()
        if len(dataset_list) > ala_config.MIN_DRS_PER_BATCH:
            logging.info(f"Found {len(dataset_list)} datasets for image sync, processing in batches")
            return 'image_sync_batch'
        else:
            logging.info(f"Found {len(dataset_list)} datasets for image sync, performing full index to Solr")
            return 'full_index_to_solr'


    # Check trigger rule explanation: #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    check_image_sync_count = BranchPythonOperator(
        task_id="check_image_sync_count",
        provide_context=True,
        python_callable=check_image_sync_count,
        do_xcom_push=False,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    image_sync = DummyOperator(
        task_id='image_sync',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    image_sync_batch = DummyOperator(
        task_id='image_sync_batch',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    full_index_to_solr = DummyOperator(
        task_id='full_index_to_solr',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    with TaskGroup(group_id='image_sync_batch_task_grp') as image_sync_batch_task_grp:

        def create_batches_func(**kwargs):
            ti = kwargs['ti']
            datasets = ti.xcom_pull(task_ids='get_drs_for_image_sync_index')
            dataset_batches = [""] * ala_config.NO_OF_DATASET_BATCHES
            dataset_list = datasets.split()
            import math
            no_of_dataset_batches = min(math.ceil(len(dataset_list) / ala_config.MIN_DRS_PER_BATCH), ala_config.NO_OF_DATASET_BATCHES)
            for idx, dataset in enumerate(dataset_list):
                batch = idx % no_of_dataset_batches
                dataset_batches[batch] = dataset + " " + dataset_batches[batch]

            for batch_no in range(ala_config.NO_OF_DATASET_BATCHES):
                kwargs['ti'].xcom_push(key=f'batch{str(batch_no)}', value=dataset_batches[batch_no])


        create_batches = PythonOperator(
            task_id='create_batches',
            python_callable=create_batches_func,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS
        )


        def generate_task(batch_no: int):
            # Due to limitation of current airflow version (2.2), the number of task batches needs to be known before runtime.
            # Hence, if the total datasets are less than NO_OF_DATASET_BATCHES * MIN_DRS_PER_BATCH, some batches may be empty
            # the empty batch task will still be triggered, however, it won't launch the emr cluster if it's empty.
            return TriggerDagRunOperator(
                task_id=f"image_sync_all_batch{batch_no + 1}",
                trigger_dag_id="Image_sync_datasets",
                wait_for_completion=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                conf={
                    'datasetIds': "{{ task_instance.xcom_pull(task_ids='image_sync_batch_task_grp.create_batches', key='batch"
                                  + str(batch_no) + "') }}"})


        check_image_sync_batches = DummyOperator(
            task_id='check_image_sync_batches',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        create_batches >> [generate_task(batch) for batch in range(ala_config.NO_OF_DATASET_BATCHES)] >> check_image_sync_batches

    with TaskGroup(group_id='full_index_to_solr_task_grp') as full_index_to_solr_task_grp:

        def construct_full_index_steps(**kwargs):
            skip_image_sync = strtobool(kwargs['dag_run'].conf['skipImageSync'])
            spark_steps = []
            logging.info(f"skip_image_sync: {skip_image_sync}")
            if not skip_image_sync:
                ti = kwargs['ti']
                datasets = ti.xcom_pull(task_ids='get_drs_for_image_sync_index')
                logging.info(f"datasets: {datasets}")
                dataset_list = datasets.split()
                if 0 < len(dataset_list) <= ala_config.MIN_DRS_PER_BATCH:
                    logging.info(f"Found {len(dataset_list)} datasets for image sync, performing adding image sync steps")
                    spark_steps = ala_helper.get_image_sync_steps(s3_bucket_avro=ala_config.S3_BUCKET_AVRO, dataset_list=datasets)
                elif len(dataset_list) > ala_config.MIN_DRS_PER_BATCH:
                    logging.info(f"Found {len(dataset_list)} datasets for image sync, skipping image sync steps in this cluster"
                                 f" as it has been handled in a image-sync cluster")
                else:
                    logging.info(f"Found {len(dataset_list)} datasets for image sync, skipping image sync steps in this cluster"
                                 f" as no datasets found")

            include_sampling = kwargs['dag_run'].conf['includeSampling']
            include_jack_knife = kwargs['dag_run'].conf['includeJackKnife']
            include_clustering = kwargs['dag_run'].conf['includeClustering']
            include_outlier = kwargs['dag_run'].conf['includeOutlier']

            num_partitions = 1
            if strtobool(include_sampling):
                num_partitions = 8

            spark_steps.extend(get_spark_steps(solr_collection_name=new_collection, include_sampling=include_sampling,
                                               include_jack_knife=include_jack_knife, include_clustering=include_clustering,
                                               include_outlier=include_outlier, num_partitions=num_partitions))
            return spark_steps


        construct_full_index_steps = PythonOperator(
            dag=dag,
            task_id='construct_full_index_steps',
            provide_context=True,
            op_kwargs={},
            python_callable=construct_full_index_steps
        )

        cluster_creator_task = EmrCreateJobFlowOperator(
            dag=dag,
            task_id='create_emr_cluster',
            emr_conn_id='emr_default',
            job_flow_overrides=cluster_setup.get_large_cluster(DAG_ID, "bootstrap-index-actions.sh"),
            aws_conn_id='aws_default'
        )

        step_adder_task = EmrAddStepsOperator(
            dag=dag,
            task_id='add_steps',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='full_index_to_solr_task_grp.create_emr_cluster', key='return_value') }}",
            aws_conn_id='aws_default',
            steps="{{ task_instance.xcom_pull(task_ids='full_index_to_solr_task_grp.construct_full_index_steps', key='return_value') }}"
        )

        step_adder_wait_task = EmrStepSensor(
            dag=dag,
            task_id='wait_for_adding_steps',
            job_flow_id="{{ task_instance.xcom_pull('full_index_to_solr_task_grp.create_emr_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='full_index_to_solr_task_grp.add_steps', key='return_value')[0] }}",
            aws_conn_id='aws_default',
        )

        wait_for_termination = EmrJobFlowSensor(
            dag=dag,
            task_id='wait_for_termination',
            job_flow_id="{{ task_instance.xcom_pull('full_index_to_solr_task_grp.create_emr_cluster', key='return_value') }}",
            aws_conn_id='aws_default',
            poke_interval=60,
        )

        construct_full_index_steps >> cluster_creator_task >> step_adder_task >> step_adder_wait_task >> wait_for_termination

    assertion_sync = TriggerDagRunOperator(
        task_id='assertion_sync',
        trigger_dag_id="Assertions-Sync",
        trigger_rule=TriggerRule.ALL_DONE,
        wait_for_completion=True,
        conf={}
    )

    check_image_sync_flag >> [image_sync, full_index_to_solr]
    image_sync >> get_drs_for_image_sync_index >> check_image_sync_count >> [full_index_to_solr, image_sync_batch]
    image_sync_batch >> image_sync_batch_task_grp >> full_index_to_solr
    full_index_to_solr >> full_index_to_solr_task_grp >> assertion_sync
