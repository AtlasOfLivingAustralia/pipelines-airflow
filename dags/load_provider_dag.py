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

from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError

import ala.ala_helper
from ala import ala_config

DAG_ID = 'Load_provider'


with DAG(
        dag_id=DAG_ID,
        catchup=False,
        default_args=ala.ala_helper.get_default_args(),
        description="Loads DwCAs for data provider from the collectory into S3 and run all pipelines for a single dataset",
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'multiple-dataset'],
        params={"dataProviderId": "dp42", "load_images": "false", "override_uuid_percentage_check": "false"}
) as dag:

    def upload_file(file_name, bucket, object_name=None):
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
            return False
        return True

    def refresh_archives_for_provider(**kwargs):

        ala_api_key = kwargs['ala_api_key']
        registry_url = kwargs['registry_url']

        provider_uid = kwargs['dag_run'].conf['dataProviderId']
        provider_url = registry_url + "dataProvider/" + provider_uid
        with urlopen(provider_url) as url:
            data = json.loads(url.read().decode())
            data_resources = data["dataResources"]
            for dataResource in data_resources:
                print("opening: {" + dataResource["uid"] + " " + dataResource["uri"])
                resource_url = dataResource["uri"]
                req = Request(resource_url)
                req.add_header('Authorization', ala_api_key)
                resp = urlopen(req)
                data_resource_content = json.loads(resp.read().decode())

                url_to_download = data_resource_content["connectionParameters"]["url"]
                print("URL to download: " + data_resource_content["connectionParameters"]["url"])

                urllib.request.urlretrieve(url_to_download, "/tmp/" + dataResource["uid"] + ".zip")
                upload_file("/tmp/" + dataResource["uid"] + ".zip", ala_config.S3_BUCKET_DWCA,
                            "dwca-imports/" + dataResource["uid"] + "/" + dataResource["uid"] + ".zip")
                os.remove("/tmp/" + dataResource["uid"] + ".zip")
        print("Finished")

    def get_dataset_size_list_for_provider(**kwargs):
        data_provider_id = kwargs['dag_run'].conf['dataProviderId']

        bucket = kwargs['bucket']
        registry_url = kwargs['registry_url']

        provider_url = f"{registry_url}/dataProvider/{data_provider_id}"
        print(provider_url)
        dataset_list = []
        with urlopen(provider_url) as url:
            data = json.loads(url.read().decode())
            dataResources = data["dataResources"]
            for dataResource in dataResources:
                print("opening: " + dataResource["uid"])
                dataset_list.append(dataResource["uid"])

        # lookup size on S3
        datasets = {}
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(bucket)
        for dataset in dataset_list:
            archive_files = my_bucket.objects.filter(Prefix=f'dwca-imports/{dataset}/{dataset}.zip')
            for archive_file in  archive_files:
                datasets[dataset] = archive_file.size
                print(f"{dataset} = {archive_file.size}")
        datasets = dict(sorted(datasets.items(), key=lambda item: item[1], reverse=True))
        return datasets

    def list_small_datasets(**kwargs):
        ti = kwargs['ti']
        datasets = ti.xcom_pull(task_ids='get_dataset_list')
        small_datasets = dict((k, v) for k, v in datasets.items() if v <= 5000000)
        kwargs['ti'].xcom_push(key='process_small', value=small_datasets)
        dataset_list = " ".join(small_datasets.keys())
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    def list_large_datasets(**kwargs):
        ti = kwargs['ti']
        datasets = ti.xcom_pull(task_ids='get_dataset_list')
        xlarge_datasets = dict((k, v) for k, v in datasets.items() if (5000000 < v < 5000000000))
        kwargs['ti'].xcom_push(key='process_xlarge', value=xlarge_datasets)
        dataset_list = " ".join(xlarge_datasets.keys()).strip()
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    def list_xlarge_datasets(**kwargs):
        ti = kwargs['ti']
        datasets = ti.xcom_pull(task_ids='get_dataset_list')
        xlarge_datasets = dict((k, v) for k, v in datasets.items() if (v > 5000000000))
        kwargs['ti'].xcom_push(key='process_xlarge', value=xlarge_datasets)
        dataset_list = " ".join(xlarge_datasets.keys())
        if not dataset_list:
            raise AirflowSkipException
        return dataset_list

    refresh_archives = PythonOperator(
        task_id='refresh_archives',
        provide_context=True,
        op_kwargs={'bucket': ala_config.S3_BUCKET_DWCA, 'ala_api_key': ala_config.ALA_API_KEY, 'registry_url': ala_config.COLLECTORY_SERVER},
        python_callable=refresh_archives_for_provider)

    get_dataset_list = PythonOperator(
        task_id='get_dataset_list',
        provide_context=True,
        op_kwargs={'bucket': ala_config.S3_BUCKET_DWCA, 'ala_api_key': ala_config.ALA_API_KEY, 'registry_url': ala_config.COLLECTORY_SERVER},
        python_callable=get_dataset_size_list_for_provider)

    process_small = PythonOperator(
        task_id='process_small',
        provide_context=True,
        python_callable=list_small_datasets
    )

    process_large = PythonOperator(
        task_id='process_large',
        provide_context=True,
        python_callable=list_large_datasets
    )

    process_xlarge = PythonOperator(
        task_id='process_xlarge',
        provide_context=True,
        python_callable=list_xlarge_datasets
    )

    ingest_small_datasets_task = TriggerDagRunOperator(
        task_id='ingest_small_datasets_task',
        trigger_dag_id="Ingest_small_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={
            'datasetIds': "{{ task_instance.xcom_pull(task_ids='process_small', key='return_value') }}",
            "load_images": "{{ dag_run.conf['load_images'] }}",
            "override_uuid_percentage_check": "{{ dag_run.conf['override_uuid_percentage_check'] }}",
            "skip_dwca_to_verbatim": "false"
        }
    )

    ingest_large_datasets_task = TriggerDagRunOperator(
        task_id='ingest_large_datasets_task',
        trigger_dag_id="Ingest_large_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={'datasetIds': "{{ task_instance.xcom_pull(task_ids='process_large', key='return_value') }}",
              "load_images": "{{ dag_run.conf['load_images'] }}",
              "override_uuid_percentage_check": "{{ dag_run.conf['override_uuid_percentage_check'] }}",
              "skip_dwca_to_verbatim": "false"
        }
    )

    ingest_xlarge_datasets_task = TriggerDagRunOperator(
        task_id='ingest_xlarge_datasets_task',
        trigger_dag_id="Ingest_large_datasets",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        conf={'datasetIds': "{{ task_instance.xcom_pull(task_ids='process_xlarge', key='return_value') }}",
              "load_images": "{{ dag_run.conf['load_images'] }}",
              "override_uuid_percentage_check": "{{ dag_run.conf['override_uuid_percentage_check'] }}",
              "skip_dwca_to_verbatim": "false"
        }
    )

    refresh_archives >> get_dataset_list >> [process_small, process_large, process_xlarge]
    process_small >> ingest_small_datasets_task
    process_large >> ingest_large_datasets_task
    process_xlarge >> ingest_xlarge_datasets_task
