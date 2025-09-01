from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import ala_helper, cluster_setup, ala_config

DAG_ID = 'Ingest_publication_dag'

dataset_list = "{{ dag_run.conf['datasetId'] }}"

SPARK_STEPS = [
    ala_helper.step_bash_cmd(
        "a. Download annotation dataset", f" /tmp/download-annotations.sh {ala_config.S3_BUCKET_DWCA} {dataset_list}"
    ),
    ala_helper.step_bash_cmd("b. Bulk annotations", f" la-pipelines bulk-annotations {dataset_list}"),
    ala_helper.step_bash_cmd(
        "c. Upload annotation dataset", f" /tmp/upload-annotations.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}"
    ),
]

with DAG(
    dag_id=DAG_ID,
    default_args=ala_helper.get_default_args(),
    description="Process publication supplied datasets",
    dagrun_timeout=timedelta(hours=6),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'multiple-dataset'],
    params={"datasetId": "dr18388"},
) as dag:
    cluster_setup.run_large_emr(
        dag, SPARK_STEPS, "bootstrap-index-actions.sh", cluster_size=4, dataset_ids=dataset_list
    )
