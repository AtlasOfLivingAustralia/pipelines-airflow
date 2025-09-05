from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.models.baseoperator import chain

from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, get_default_args, get_success_notification_operator
import logging as log
import copy


DAG_ID = 'Generate_parquet'

s3_default_source = (
    f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/dr19123/*.avro "
    f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/dr345/*.avro"
)
s3_default_destination = f"s3://{ala_config.S3_BUCKET_AVRO}/parquet-output/"
spark_submit_args = ala_config.SPARK_SUBMIT_ARGS

# Sanitize spark-submit args for EMR 7.x (Spark 3.5.x):
# - Convert "--flag=value" to "--flag value" (Spark CLI expects space-separated)
# - Strip any Windows CRLF/newlines to avoid bash splitting errors
def _sanitize_spark_submit_args(args: str) -> str:
    if not args:
        return ""
    cleaned = args.replace("\r", " ").replace("\n", " ")
    # Replace common equals patterns with space
    for opt in [
        "--num-executors",
        "--executor-cores",
        "--executor-memory",
        "--driver-memory",
        "--driver-cores",
        "--total-executor-cores",
    ]:
        cleaned = cleaned.replace(f"{opt}=", f"{opt} ")
    # Collapse repeated whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned

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
    params={"s3_source_avro": s3_default_source, "s3_parquet_output": s3_default_destination},
) as dag:

    def is_empty(**kwargs):
        s3_source = kwargs["dag_run"].conf["s3_source_avro"]
        # Fixed: destination should read from s3_parquet_output param
        s3_dest = kwargs["dag_run"].conf["s3_parquet_output"]
        if s3_source and s3_dest:
            return True
        log.info("s3_source or s3_dest is empty. Skipping the run")
        return False

    is_empty_task = ShortCircuitOperator(task_id="is_empty", python_callable=is_empty)

    def construct_steps_with_options(**kwargs):
        s3_source_avro = kwargs["dag_run"].conf["s3_source_avro"]
        s3_parquet_output = kwargs["dag_run"].conf["s3_parquet_output"]

        log.info("Reading avro from %s. Generated parquet will be written to: %s", s3_source_avro, s3_parquet_output)
        steps = [
            step_bash_cmd(
                "c. Run Generate Parquet",
                # On EMR 7.x (Spark 3.5.x), Avro is bundled with Spark; no external package needed
                f"spark-submit {_sanitize_spark_submit_args(spark_submit_args)} "
                f"/tmp/generate_parquet.py "
                f"-s {s3_source_avro} "
                f"-d {s3_parquet_output}",
            )
        ]
        return steps

    construct_steps = PythonOperator(
        dag=dag,
        task_id="construct_steps",
        provide_context=True,
        op_kwargs={},
        python_callable=construct_steps_with_options,
    )

    # Base cluster config from shared helper, then override only what we need for this DAG
    _base_cluster_cfg = cluster_setup.get_large_cluster(DAG_ID, "bootstrap-generate-parquet.sh")
    job_flow_overrides = copy.deepcopy(_base_cluster_cfg)
    # Use EMR 7.10.0 which includes Spark 3.5.5-amzn-1
    job_flow_overrides["ReleaseLabel"] = "emr-7.10.0"

    cluster_creator = EmrCreateJobFlowOperator(
        dag=dag,
        task_id="create_emr_cluster",
        emr_conn_id="emr_default",
        job_flow_overrides=job_flow_overrides,
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

    chain(
        is_empty_task,
        construct_steps,
        cluster_creator,
        step_adder,
        step_checker,
        wait_for_termination,
        get_success_notification_operator(),
    )
