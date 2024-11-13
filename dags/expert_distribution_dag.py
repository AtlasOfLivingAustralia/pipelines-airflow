#
# Expert distribution for all datasets.
#
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from ala import cluster_setup, ala_config
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'Expert_distribution'

SPARK_STEPS = [
    s3_cp("a. Copy IndexRecord to S3", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record", "hdfs:///pipelines-all-datasets/index-record"),
    s3_cp("b. Copy existing Outlier Distribution Cache", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier/", "hdfs:///pipelines-outlier/", action_on_failure="CONTINUE"),
    step_bash_cmd("c. Outlier detection", f" la-pipelines outlier all --cluster"),
    step_bash_cmd("d. Delete AVRO output on S3", f" sudo -u hadoop aws s3 rm s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier/all --recursive"),
    step_bash_cmd("e. Delete temp SCP directories created by s3-dist-cp", f" sudo -u hadoop hdfs dfs -rm -f /pipelines-outlier/pipelines-outlier_$folder$"),
    s3_cp("f. Copy Outliers results to S3", "hdfs:///pipelines-outlier", f"s3://{ala_config.S3_BUCKET_AVRO}/pipelines-outlier")
]

with DAG(
    dag_id=DAG_ID,
    description="Expert distribution",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=4),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'all-datasets']
) as dag:
    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-index-actions.sh")
