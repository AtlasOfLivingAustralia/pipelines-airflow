from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_helper, ala_config

DAG_ID = 'Clustering'

SPARK_STEPS = [
    ala_helper.s3_cp("a. Copy IndexRecord to S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record", "hdfs:///pipelines-all-datasets/index-record"),
    ala_helper.step_bash_cmd("b. Clustering", f" la-pipelines clustering all --cluster"),
    ala_helper.s3_cp("c. Copy Clustering results to S3", "hdfs:///pipelines-clustering", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-clustering")
]

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    description="Run clustering for all datasets",
    default_args=ala_helper.get_default_args(),
    dagrun_timeout=timedelta(hours=1),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr','all-datasets']
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh")
