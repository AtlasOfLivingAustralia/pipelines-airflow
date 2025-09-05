from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, get_default_args

DAG_ID = 'Elastic_dataset_indexing'

dataset_list = "{{ dag_run.conf['datasetId'] }}"

SPARK_STEPS = [
    step_bash_cmd(
        "a. Download datasets", f" /tmp/download-datasets-for-indexing.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}"
    ),
    step_bash_cmd(
        "b. Remove existing indices",
        f" /tmp/elastic-cleanup.sh \"{ala_config.ES_HOSTS}\" {ala_config.ES_ALIAS} {dataset_list}",
    ),
    step_bash_cmd("c. Event Core Elastic Search", f" la-pipelines elastic {dataset_list} --cluster"),
]

with DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    description="Elastic indexing for the supplied datasets",
    dagrun_timeout=timedelta(hours=6),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'multiple-dataset'],
    params={"datasetId": "dr18388"},
) as dag:
    cluster_setup.run_large_emr(
        dag, SPARK_STEPS, "bootstrap-event-core-actions.sh", cluster_size=4, dataset_ids=dataset_list
    )
