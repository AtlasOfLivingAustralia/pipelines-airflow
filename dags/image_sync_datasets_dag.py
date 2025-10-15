from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala.ala_helper import get_image_sync_steps, get_default_args
from ala import ala_config, cluster_setup

DAG_ID = 'Image_sync_datasets'

datasetId = "{{ dag_run.conf['datasetIds'] }}"


def get_spark_steps(dataset_list: str):
    return get_image_sync_steps(s3_bucket_avro=ala_config.S3_BUCKET_AVRO, dataset_list=dataset_list)


with DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    description="Image Sync Datasets",
    dagrun_timeout=timedelta(hours=8),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'multiple-dataset'],
    params={"datasetIds": "dr359 dr2009"},
) as dag:
    cluster_setup.run_large_emr(
        dag, get_spark_steps(datasetId), "bootstrap-image-sync-actions.sh", cluster_size=4, dataset_ids=datasetId
    )
