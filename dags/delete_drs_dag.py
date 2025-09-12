import boto3
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from ala import ala_config
from ala.ala_helper import get_default_args, get_success_notification_operator, strtobool
import logging


DAG_ID = "Delete_dataset_dag"


def _delete_prefix_objects(
    *,
    bucket_name: str,
    prefix: str,
    s3_resource,
    s3_client,
    description: str,
    exclude_contains: list[str] | None = None,
) -> int:
    """Delete objects under a prefix with optional substring-based exclusions.

    Returns number of deleted objects (post exclusion). Logs findings.
    """
    exclude_contains = exclude_contains or []
    bucket = s3_resource.Bucket(bucket_name)
    objs = [o.key for o in bucket.objects.filter(Prefix=prefix)]
    if not objs:
        logging.info("No objects found under s3://%s/%s — skipping %s", bucket_name, prefix, description)
        return 0
    filtered = [k for k in objs if all(excl not in k for excl in exclude_contains)]
    if not filtered:
        logging.info(
            "All %d objects under s3://%s/%s excluded for %s (exclusions=%s)",
            len(objs),
            bucket_name,
            prefix,
            description,
            exclude_contains,
        )
        return 0
    logging.info(
        "Deleting %d/%d %s objects under s3://%s/%s (exclusions=%s)",
        len(filtered),
        len(objs),
        description,
        bucket_name,
        prefix,
        exclude_contains,
    )
    for i in range(0, len(filtered), 1000):
        chunk = filtered[i : i + 1000]
        s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True})
    return len(filtered)


with DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    description="Delete datasets from pipelines",
    dagrun_timeout=timedelta(hours=8),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "multiple-dataset"],
    params={
        "datasetIds": "dr1 dr2",
        "remove_records_in_solr": "false",
        "remove_records_in_es": "false",
        "delete_avro_files": "false",
        "retain_dwca": "true",
        "retain_uuid": "true",
    },
) as dag:

    def delete_dataset_files(**kwargs):
        """Delete dataset files in S3 for provided datasetIds string (space separated)."""
        datasets_param = kwargs["dag_run"].conf["datasetIds"]
        delete_avro_files = strtobool(kwargs["dag_run"].conf["delete_avro_files"])
        retain_dwca = strtobool(kwargs["dag_run"].conf["retain_dwca"])
        retain_uuid = strtobool(kwargs["dag_run"].conf["retain_uuid"])

        s3_client = boto3.client("s3")
        s3 = boto3.resource("s3")

        from ala.ala_helper import enable_debugpy

        enable_debugpy()

        if not retain_dwca:
            _delete_prefix_objects(
                bucket_name=ala_config.S3_BUCKET_DWCA,
                prefix=f"dwca-imports/{datasets_param}/",
                s3_resource=s3,
                s3_client=s3_client,
                description="DWCA",
            )

        if delete_avro_files:
            _delete_prefix_objects(
                bucket_name=ala_config.S3_BUCKET_AVRO,
                prefix=f"pipelines-all-datasets/index-record/{datasets_param}/",
                s3_resource=s3,
                s3_client=s3_client,
                description="index-record",
            )
            _delete_prefix_objects(
                bucket_name=ala_config.S3_BUCKET_AVRO,
                prefix=f"dwca-exports/{datasets_param}.zip",
                s3_resource=s3,
                s3_client=s3_client,
                description="dwca-exports-zip",
            )

            _delete_prefix_objects(
                bucket_name=ala_config.S3_BUCKET_AVRO,
                prefix=f"pipelines-data/{datasets_param}/",
                s3_resource=s3,
                s3_client=s3_client,
                description="pipelines-data",
                exclude_contains=["identifiers/", "identifiers-backup/"] if retain_uuid else None,
            )

    delete_dataset_in_s3 = PythonOperator(
        task_id="delete_dataset", provide_context=True, op_kwargs={}, python_callable=delete_dataset_files
    )

    def remove_from_solr(**kwargs):
        """
        Delete dr records from the solr collection alias.
        Although the records are removed from solr collection, querying dataResourceUid facet query will still show the dr with 0 count.
        :param kwargs:
        :return: None
        """

        remove_records_in_solr = strtobool(kwargs["dag_run"].conf["remove_records_in_solr"])
        if not remove_records_in_solr:
            logging.info("Records not removed from solr")
            return

        def get_cmd(dr: str) -> str:
            solr_ws = f"{ala_config.SOLR_URL}/{ala_config.SOLR_COLLECTION}/update?commit=true"
            return f"curl {solr_ws} -H 'Content-Type: text/xml' --data-binary '<delete><query>dataResourceUid:{dr}</query></delete>'"

        datasets_param = kwargs["dag_run"].conf["datasetIds"]
        dataset_list = datasets_param.split()
        for count, dr in enumerate(dataset_list):
            bash_operator = BashOperator(task_id=f"delete_solr_{dr}_task{count}", bash_command=get_cmd(dr=dr))
            bash_operator.execute(context=kwargs)

    delete_dataset_in_solr = PythonOperator(
        task_id="delete_dataset_in_solr", provide_context=True, op_kwargs={}, python_callable=remove_from_solr
    )

    def remove_from_es(**kwargs):
        """
        Delete dataset document record from the events elasticsearch.
        :param kwargs:
        :return: None
        """

        remove_document_in_es = strtobool(kwargs["dag_run"].conf["remove_records_in_es"])
        if not remove_document_in_es:
            logging.info("Skipped removing from es")
            return

        def get_cmd(dr: str) -> str:
            es_hosts = ala_config.ES_HOSTS.split(",")
            es_host = es_hosts[0] if es_hosts else ""
            es_ws = f"{es_host}/{ala_config.ES_ALIAS}_{dr}"
            return f"curl -X DELETE {es_ws}"

        datasets_param = kwargs["dag_run"].conf["datasetIds"]
        dataset_list = datasets_param.split()
        for count, dr in enumerate(dataset_list):
            bash_operator = BashOperator(task_id=f"delete_es_{dr}_task{count}", bash_command=get_cmd(dr=dr))
            bash_operator.execute(context=kwargs)

    delete_dataset_in_es = PythonOperator(
        task_id="delete_dataset_in_es", provide_context=True, op_kwargs={}, python_callable=remove_from_es
    )

    notify = get_success_notification_operator()
    _workflow = delete_dataset_in_s3 >> delete_dataset_in_solr >> delete_dataset_in_es >> notify
