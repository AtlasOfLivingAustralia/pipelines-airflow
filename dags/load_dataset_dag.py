import os
import urllib

import boto3
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from urllib.request import Request, urlopen
import json
from distutils.util import strtobool
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError

from ala import ala_config
from ala.ala_helper import get_default_args, get_success_notification_operator, get_metadata_as_json

load_images = "{{ dag_run.conf['load_images'] }}"
override_uuid_percentage_check = "{{ dag_run.conf['override_uuid_percentage_check'] }}"

DAG_ID = "Load_dataset"

with DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(),
    description="Loads DwCA from the collectory into S3 and run all pipelines for a single dataset",
    dagrun_timeout=timedelta(hours=8),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "multiple-dataset"],
    params={
        "datasetIds": "dr893",
        "load_images": "false",
        "skip_collectory_download": "false",
        "override_uuid_percentage_check": "false",
    },
) as dag:

    def upload_file(file_name, bucket, object_name=None):
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client("s3")
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
            return False
        return True

    def refresh_archive_for_dataset(**kwargs):
        """
        Support only single dr if download from collectory. If this is triggered by preingest, multiple drs can be passed
        :param kwargs:
        :return:
        """
        skip_download = strtobool(kwargs["dag_run"].conf["skip_collectory_download"])

        if skip_download:
            print("Skipped download from collectory. Triggered from pre-ingest")
            return

        ala_api_key = kwargs["ala_api_key"]
        registry_url = kwargs["registry_url"]

        dataset_uid = kwargs["dag_run"].conf["datasetIds"]

        data_resource_content = get_metadata_as_json(registry_url, dataset_uid, ala_api_key)

        conn_params_debug = json.dumps(data_resource_content["connectionParameters"])

        print("Connection parameters: " + conn_params_debug)
        url_to_download = data_resource_content["connectionParameters"]["url"]
        print("URL to download: " + data_resource_content["connectionParameters"]["url"])

        if url_to_download.startswith("http"):
            urllib.request.urlretrieve(url_to_download.replace(" ", "%20"), f"/tmp/{dataset_uid}.zip")
            upload_file(
                f"/tmp/{dataset_uid}.zip", ala_config.S3_BUCKET_DWCA, f"dwca-imports/{dataset_uid}/{dataset_uid}.zip"
            )
            os.remove(f"/tmp/{dataset_uid}.zip")
        else:
            print("URL to download is not HTTP or HTTPS")
            raise AirflowSkipException
        print("Finished")

    def get_dataset_size_list(**kwargs):
        datasets_param = kwargs["dag_run"].conf["datasetIds"]
        bucket = kwargs["bucket"]

        # lookup size on S3
        datasets = {}
        s3 = boto3.resource("s3")
        my_bucket = s3.Bucket(bucket)
        dataset_list = datasets_param.split()
        for dataset in dataset_list:
            archive_files = my_bucket.objects.filter(Prefix=f"dwca-imports/{dataset}/{dataset}.zip")
            for archive_file in archive_files:
                datasets[dataset] = archive_file.size
                print(f"{dataset} = {archive_file.size}")
        datasets = dict(sorted(datasets.items(), key=lambda item: item[1], reverse=True))
        return datasets

    def list_small_datasets(**kwargs):
        ti = kwargs["ti"]
        datasets = ti.xcom_pull(task_ids="get_dataset_list")
        small_datasets = dict((k, v) for k, v in datasets.items() if v <= 5000000)
        kwargs["ti"].xcom_push(key="process_small", value=small_datasets)
        dataset_list = " ".join(small_datasets.keys())
        print("Small datasets to process " + dataset_list)
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    def list_large_datasets(**kwargs):
        ti = kwargs["ti"]
        datasets = ti.xcom_pull(task_ids="get_dataset_list")
        large_datasets = dict((k, v) for k, v in datasets.items() if (5000000 < v < 5000000000))
        kwargs["ti"].xcom_push(key="process_large", value=large_datasets)
        dataset_list = " ".join(large_datasets.keys()).strip()
        print("Large datasets to process " + dataset_list)
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    def list_xlarge_datasets(**kwargs):
        ti = kwargs["ti"]
        datasets = ti.xcom_pull(task_ids="get_dataset_list")
        xlarge_datasets = dict((k, v) for k, v in datasets.items() if (v > 5000000000))
        kwargs["ti"].xcom_push(key="process_xlarge", value=xlarge_datasets)
        dataset_list = " ".join(xlarge_datasets.keys())
        print("Xlarge datasets to process " + dataset_list)
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    refresh_archive = PythonOperator(
        task_id="refresh_archive_from_collectory",
        provide_context=True,
        op_kwargs={
            "bucket": ala_config.S3_BUCKET_DWCA,
            "ala_api_key": ala_config.ALA_API_KEY,
            "registry_url": ala_config.COLLECTORY_SERVER,
        },
        python_callable=refresh_archive_for_dataset,
    )

    get_dataset_list = PythonOperator(
        task_id="get_dataset_list",
        provide_context=True,
        op_kwargs={
            "bucket": ala_config.S3_BUCKET_DWCA,
            "ala_api_key": ala_config.ALA_API_KEY,
            "registry_url": ala_config.COLLECTORY_SERVER,
        },
        python_callable=get_dataset_size_list,
    )

    process_small = PythonOperator(task_id="process_small", provide_context=True, python_callable=list_small_datasets)

    process_large = PythonOperator(task_id="process_large", provide_context=True, python_callable=list_large_datasets)

    process_xlarge = PythonOperator(
        task_id="process_xlarge", provide_context=True, python_callable=list_xlarge_datasets
    )

    ingest_small_datasets_task = TriggerDagRunOperator(
        task_id="ingest_small_datasets_task",
        trigger_dag_id="Ingest_small_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={
            "datasetIds": "{{ task_instance.xcom_pull(task_ids='process_small', key='return_value') }}",
            "skip_dwca_to_verbatim": "false",
            "load_images": f"{load_images}",
            "override_uuid_percentage_check": override_uuid_percentage_check,
            "run_indexing": "false",
        },
    )

    ingest_large_datasets_task = TriggerDagRunOperator(
        task_id="ingest_large_datasets_task",
        trigger_dag_id="Ingest_large_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={
            "datasetIds": "{{ task_instance.xcom_pull(task_ids='process_large', key='return_value') }}",
            "skip_dwca_to_verbatim": "false",
            "load_images": f"{load_images}",
            "override_uuid_percentage_check": override_uuid_percentage_check,
            "run_indexing": "false",
        },
    )

    ingest_xlarge_datasets_task = TriggerDagRunOperator(
        task_id="ingest_xlarge_datasets_task",
        trigger_dag_id="Ingest_large_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={
            "datasetIds": "{{ task_instance.xcom_pull(task_ids='process_xlarge', key='return_value') }}",
            "skip_dwca_to_verbatim": "false",
            "load_images": f"{load_images}",
            "override_uuid_percentage_check": override_uuid_percentage_check,
            "run_indexing": "false",
        },
    )

    (refresh_archive >> get_dataset_list >> [process_large, process_small, process_xlarge])
    process_small >> ingest_small_datasets_task
    process_large >> ingest_large_datasets_task
    process_xlarge >> ingest_xlarge_datasets_task
    [
        ingest_small_datasets_task,
        ingest_large_datasets_task,
        ingest_xlarge_datasets_task,
    ] >> get_success_notification_operator()
