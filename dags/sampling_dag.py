from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config, ala_helper
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'Sampling'

SPARK_STEPS = [
    s3_cp("a. Copy IndexRecord and Sampling from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets", "hdfs:///pipelines-all-datasets"),
    step_bash_cmd("b. Sampling", f" la-pipelines sample all --cluster"),
    s3_cp("c. Copy Sampling results to S3", "hdfs:///pipelines-all-datasets/sampling", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/sampling"),
    s3_cp("d. Copy Sampling metrics to S3", "hdfs:///pipelines-all-datasets/sampling-metrics.yml", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/")
]

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    description="Run sampling for all datasets",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=5),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'all-datasets']
) as dag:

    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh")
