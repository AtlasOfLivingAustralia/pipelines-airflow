#
# SOLR indexing for all datasets.
# This will recreate the full index and swap the SOLR alias on successful completion.
# It will also remove old collections.
#

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args
from ala import cluster_setup, ala_config

s3_bucket = ala_config.S3_BUCKET
solrCollection = ala_config.SOLR_COLLECTION
solrConfigset = ala_config.SOLR_CONFIGSET
solrUrl = ala_config.SOLR_URL
zkUrl = ala_config.ZK_URL

DAG_ID = 'SOLR_indexing'

solrCollectionName = f"{solrCollection}_" + datetime.now().strftime("%d_%m_%H_%M_%S")

includeSampling = "{{ dag_run.conf['includeSampling'] }}".strip()
includeJackKnife = "{{ dag_run.conf['includeJackKnife'] }}".strip()
includeClustering = "{{ dag_run.conf['includeClustering'] }}".strip()
includeOutlier = "{{ dag_run.conf['includeOutlier'] }}".strip()

# If including sampling, we need to use partitions to avoid hotspots
numOfPartitions = 1
if includeSampling:
    numOfPartitions = 8

curlUrl = f"{solrUrl}/admin/collections" \
          "?action=CREATE" \
          f"&name={solrCollectionName}" \
          "&numShards=4" \
          "&replicationFactor=1" \
          f"&collection.configName={solrConfigset}"

createAlias = f"{solrUrl}/admin/collections" \
          "?action=CREATEALIAS" \
          f"&collections={solrCollectionName}" \
          f"&name={solrCollection}"

print("Creating collection with URL: " + curlUrl)

SPARK_STEPS = [
    step_bash_cmd("a. Create SOLR Collection", " sudo -u hadoop curl -X GET \"" + curlUrl + "\""),
    s3_cp("b. Copy Sampling and IndexRecord from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets", f"hdfs:///pipelines-all-datasets"),
    s3_cp("c. Copy Clustering from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-clustering/", "hdfs:///pipelines-clustering/", action_on_failure="CONTINUE"),
    s3_cp("d. Copy Jack knife from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-jackknife/", "hdfs:///pipelines-jackknife/", action_on_failure="CONTINUE"),
    s3_cp("e. Copy Outlier from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier/", "hdfs:///pipelines-outlier/", action_on_failure="CONTINUE"),
    s3_cp("e. Copy Annotations from S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-annotations/", "hdfs:///pipelines-annotations/", action_on_failure="CONTINUE"),
    step_bash_cmd("f. SOLR indexing", f" la-pipelines solr all --cluster --extra-args=\"solrCollection={solrCollectionName},includeSampling={includeSampling},includeJackKnife={includeJackKnife},includeClustering={includeClustering},includeOutlier={includeOutlier}\""),
    step_bash_cmd("g. Create new SOLR alias", " sudo -u hadoop curl -X GET \"" + createAlias + "\""),
    step_bash_cmd("h. Delete old SOLR indexes", f" sudo -u hadoop python3 /tmp/solr_cleanup.py {solrUrl} {solrCollection} {solrCollectionName}")
]

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    description="SOLR indexing for all datasets. This will recreate the full index and swap the SOLR alias on successful completion",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=4),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr','all-datasets'],
    params={
        "includeSampling": "true",
        "includeJackKnife": "true",
        "includeClustering": "true",
        "includeOutlier": "true"
    }
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh")
