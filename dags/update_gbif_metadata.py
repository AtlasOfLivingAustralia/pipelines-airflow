import os

import boto3 as boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from urllib.request import Request, urlopen

from ala import ala_helper, ala_config

DAG_ID = "Update_gbif_metadata"


with DAG(
    dag_id=DAG_ID,
    catchup=False,
    default_args=ala_helper.get_default_args(),
    description="Update GBIF metadata",
    dagrun_timeout=timedelta(hours=1),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["multiple-dataset"],
    params={},
) as dag:

    def update_gbif_metadata(**kwargs):
        """
        Updates GBIF metadata by making a request to the registry URL.
        Args:
            **kwargs: Arbitrary keyword arguments.
                - ala_api_key (str): The API key for ALA.
                - registry_url (str): The URL of the ALA registry.
        Raises:
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """

        ala_api_key = kwargs["ala_api_key"]
        registry_url = kwargs["registry_url"]

        req = Request(ala_helper.join_url(registry_url, "/syncGBIF"))
        req.add_header("Authorization", ala_api_key)
        response = urlopen(req)
        response.raise_for_status()
        print(f"calling {req.full_url} was successful.")
        content = response.read()
        print(f"content: {content}")

    if ala_config.ENVIRONMENT_TYPE == "PROD":
        sync_s3_buckets_bash_op = BashOperator(
            task_id="sync_s3_buckets",
            bash_command=f"aws s3 sync s3://{ala_config.S3_BUCKET_AVRO}/dwca-exports s3://{ala_config.S3_BUCKET_DWCA_EXPORTS}/dwca-exports",
        )

        update_gbif_metadata_op = PythonOperator(
            task_id="call_collectory",
            provide_context=True,
            op_kwargs={"ala_api_key": ala_config.ALA_API_KEY, "registry_url": ala_config.COLLECTORY_SERVER},
            python_callable=update_gbif_metadata,
        )
        sync_s3_buckets_bash_op >> update_gbif_metadata_op >> ala_helper.get_success_notification_operator()
