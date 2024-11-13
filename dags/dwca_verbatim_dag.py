from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, get_default_args

DAG_ID = 'Dwca_verbatim_dag'

dataset_list = "{{ dag_run.conf['datasetId'] }}"

SPARK_STEPS = [
    step_bash_cmd("a. Download data", f" /tmp/download-datasets-hdfs.sh {ala_config.S3_BUCKET_DWCA} {ala_config.S3_BUCKET_AVRO}  {dataset_list}"),
    step_bash_cmd("b. DwCA to Verbatim", f" la-pipelines dwca-avro {dataset_list}"),
    step_bash_cmd("c. Upload data", f" /tmp/upload-hdfs-datasets.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}")
]

with DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(),
        description="DWCA to Verbatim AVRO supplied datasets",
        dagrun_timeout=timedelta(hours=8),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'multiple-dataset'],
        params={"datasetId": "dr18388"}
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-ingest-large-actions.sh", cluster_size=1)
