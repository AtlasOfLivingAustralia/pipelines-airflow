from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'Image_load_dataset'

datasetId = "{{ dag_run.conf['datasetId'] }}"

SPARK_STEPS = [
    s3_cp(f"Copy Interpreted AVRO from S3 for {datasetId}", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-data/{datasetId}/1", f"hdfs:///pipelines-data/{datasetId}/1"),
    step_bash_cmd("Image loading", f" la-pipelines image-load {datasetId} --cluster")
]

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    default_args=get_default_args(),
    description="Run image load pipeline for this dataset",
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'single-dataset'],
    params={"datasetId": "dr1010"}
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-ingest-large-actions.sh", cluster_size=4)
