import logging
from distutils.util import strtobool

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

from airflow.utils.dates import days_ago
from datetime import timedelta

from ala import cluster_setup, ala_config, ala_helper
from ala.ala_helper import step_bash_cmd, get_default_args


DAG_ID = 'Ingest_large_datasets'


def get_dwca_steps(dataset_list):
    return [
        step_bash_cmd("a. Download data", f" /tmp/download-datasets-hdfs.sh {ala_config.S3_BUCKET_DWCA} {ala_config.S3_BUCKET_AVRO} {dataset_list}"),
        step_bash_cmd("b. DwCA to Verbatim", f" la-pipelines dwca-avro {dataset_list}")
    ]


def get_avro_steps(dataset_list):
    return [
        step_bash_cmd("a. Download verbatim AVRO", f" /tmp/download-datasets-avro-hdfs.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}")
    ]


def get_pre_image_steps(dataset_list, override_uuid_percentage=False):

    extra_args = ""
    if override_uuid_percentage:
        extra_args = '--extra-args="overridePercentageCheck=true"'

    return [
        step_bash_cmd("c. Interpretation", f" la-pipelines interpret {dataset_list} --cluster"),
        step_bash_cmd("d. UUID", f" la-pipelines uuid {dataset_list}  --cluster {extra_args}"),
        step_bash_cmd("e. SDS", f" la-pipelines sds {dataset_list}  --cluster")
    ]


def get_load_image_steps(dataset_list):
    return [
        step_bash_cmd("f. Image loading", f" la-pipelines image-load {dataset_list} --cluster")
    ]


def get_post_image_steps(dataset_list):
    return [
        step_bash_cmd("f. Image syncing", f" la-pipelines image-sync {dataset_list} --cluster"),
        step_bash_cmd("g. Index", f" la-pipelines index {dataset_list} --cluster"),
        step_bash_cmd("h. Export", f" la-pipelines dwca-export {dataset_list} --cluster"),
        step_bash_cmd("i. Add Frictionless", f" /tmp/frictionless.sh {dataset_list}"),
        step_bash_cmd("j. Upload data", f" /tmp/upload-hdfs-datasets.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}"),
        step_bash_cmd("k. Upload export", f" /tmp/upload-export.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}")
    ]


with DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(),
        description="Ingest DwCA from S3 and run all pipelines (not including SOLR indexing)",
        dagrun_timeout=timedelta(hours=12),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'multiple-dataset'],
        params={
            "datasetIds": "dr18391",
            "load_images": "false",
            "skip_dwca_to_verbatim": "false",
            "override_uuid_percentage_check": "false"
        }
) as dag:


    def is_empty(**kwargs):
        dataset_list = kwargs['dag_run'].conf['datasetIds']
        if dataset_list:
            return True
        return False

    def construct_steps_with_options(**kwargs):

        load_images = strtobool(kwargs['dag_run'].conf['load_images'])
        skip_dwca_to_verbatim = strtobool(kwargs['dag_run'].conf['skip_dwca_to_verbatim'])
        override_uuid_percentage = strtobool(kwargs['dag_run'].conf['override_uuid_percentage_check'])
        dataset_list = kwargs['dag_run'].conf['datasetIds']

        logging.info(f"Args {dataset_list} load_images: {load_images} skip_dwca_to_verbatim: {skip_dwca_to_verbatim}")

        steps = []

        if skip_dwca_to_verbatim:
            steps.extend(get_avro_steps(dataset_list))
        else:
            steps.extend(get_dwca_steps(dataset_list))

        steps.extend(get_pre_image_steps(dataset_list, override_uuid_percentage))

        if load_images:
            steps.extend(get_load_image_steps(dataset_list))

        steps.extend(get_post_image_steps(dataset_list))
        return steps


    is_empty = ShortCircuitOperator(
        task_id='is_empty',
        python_callable=is_empty,
    )

    construct_steps = PythonOperator(
        dag=dag,
        task_id='construct_steps',
        provide_context=True,
        op_kwargs={},
        python_callable=construct_steps_with_options
    )

    cluster_creator = EmrCreateJobFlowOperator(
        dag=dag,
        task_id='create_emr_cluster',
        emr_conn_id='emr_default',
        job_flow_overrides=cluster_setup.get_large_cluster(DAG_ID, "bootstrap-ingest-large-actions.sh"),
        aws_conn_id='aws_default'
    )

    step_adder = EmrAddStepsOperator(
        dag=dag,
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps="{{ task_instance.xcom_pull(task_ids='construct_steps', key='return_value') }}"
    )

    step_checker = EmrStepSensor(
        dag=dag,
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    wait_for_termination = EmrJobFlowSensor(
        dag=dag,
        task_id='wait_for_cluster_termination',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    is_empty >> construct_steps >> cluster_creator >> step_adder >> step_checker >> wait_for_termination
