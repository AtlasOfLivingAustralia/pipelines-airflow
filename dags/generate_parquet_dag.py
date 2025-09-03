from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, get_default_args
import logging

DAG_ID = 'Generate_parquet'

s3_default_source = (f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/dr19123/*.avro "
                     f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/dr345/*.avro")
s3_default_destination = f"s3://{ala_config.S3_BUCKET_AVRO}/parquet-output/"
spark_submit_args = ala_config.SPARK_SUBMIT_ARGS

with DAG(
        dag_id=DAG_ID,
        description="Generate parquet for all datasets. "
                    "Source avro and destination parquet s3 paths can be overridden at runtime. "
                    "Accepts multiple source avro paths separated by space. E.g. -s 's3://path1 s3://path2'. "
                    "Only one destination path is allowed",
        default_args=get_default_args(),
        dagrun_timeout=timedelta(hours=18),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'all-datasets'],
        params={
            "s3_source_avro": s3_default_source,
            "s3_parquet_output": s3_default_destination,
        },
) as dag:
    def is_empty(**kwargs):
        s3_source = kwargs["dag_run"].conf["s3_source_avro"]
        s3_dest = kwargs["dag_run"].conf["s3_source_avro"]
        if s3_source and s3_dest:
            return True
        return False


    is_empty = ShortCircuitOperator(
        task_id="is_empty",
        python_callable=is_empty,
    )


    def construct_steps_with_options(**kwargs):
        s3_source_avro = kwargs["dag_run"].conf["s3_source_avro"]
        s3_parquet_output = kwargs["dag_run"].conf["s3_parquet_output"]

        logging.info(
            f"Reading avro from {s3_source_avro}. Generated parquet will be written to: {s3_parquet_output}"
        )

        steps = [
            step_bash_cmd("c. Run Generate Parquet",
                          f"spark-submit {spark_submit_args} "
                          f"--packages org.apache.spark:spark-avro_2.11:2.4.8 "
                          f"/tmp/generate_parquet.py "
                          f"-s {s3_source_avro} "
                          f"-d {s3_parquet_output}"),
        ]
        return steps


    construct_steps = PythonOperator(
        dag=dag,
        task_id="construct_steps",
        provide_context=True,
        op_kwargs={},
        python_callable=construct_steps_with_options,
    )

    cluster_creator = EmrCreateJobFlowOperator(
        dag=dag,
        task_id="create_emr_cluster",
        emr_conn_id="emr_default",
        job_flow_overrides=cluster_setup.get_large_cluster(
            DAG_ID, "bootstrap-generate-parquet.sh"
        ),
        aws_conn_id="aws_default",
    )

    step_adder = EmrAddStepsOperator(
        dag=dag,
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps="{{ task_instance.xcom_pull(task_ids='construct_steps', key='return_value') }}",
    )

    step_checker = EmrStepSensor(
        dag=dag,
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    wait_for_termination = EmrJobFlowSensor(
        dag=dag,
        task_id="wait_for_cluster_termination",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    (
            is_empty
            >> construct_steps
            >> cluster_creator
            >> step_adder
            >> step_checker
            >> wait_for_termination
    )
