from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'SOLR_dataset_indexing'

datasetId = "{{ dag_run.conf['datasetId'] }}"
curlDeleteDataset = f"{ala_config.SOLR_URL}/{ala_config.SOLR_COLLECTION}/update?commit=true"

SPARK_STEPS = [
    s3_cp(
        "a. Copy IndexRecord to S3",
        f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/" + datasetId,
        "hdfs:///pipelines-all-datasets/index-record/" + datasetId,
    ),
    step_bash_cmd(
        "b. Delete dataset from SOLR",
        " sudo -u hadoop curl \""
        + curlDeleteDataset
        + f"\" -H \"Content-Type: text/xml\"  --data-binary '<delete><query>dataResourceUid:{datasetId}</query></delete>'",
    ),
    step_bash_cmd("c. SOLR indexing", f" la-pipelines solr {datasetId} --cluster"),
]

with DAG(
    dag_id=DAG_ID,
    description="SOLR indexing for single dataset. This will delete the dataset from the index and reindex",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=1),
    start_date=days_ago(1),
    schedule_interval=None,
    params={"datasetId": "dr1010"},
    tags=['emr', 'single-datasets'],
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh", dataset_ids=datasetId)
