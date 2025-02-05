from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'Jackknife'

SPARK_STEPS = [
    s3_cp("a. Copy IndexRecord to S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record", "hdfs:///pipelines-all-datasets/index-record"),
    s3_cp("b. Copy Sampling to S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/sampling", "hdfs:///pipelines-all-datasets/sampling"),
    step_bash_cmd("c. Run JackKnife processing", f" la-pipelines jackknife all --cluster"),
    s3_cp("d. Copy JackKnife results to S3", f"hdfs:///pipelines-jackknife", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-jackknife")
]

with DAG(
    dag_id=DAG_ID,
    description="Run jackknife outlier detection for all datasets",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=1),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'all-datasets']
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh")
