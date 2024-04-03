import os

import boto3 as boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from urllib.request import Request, urlopen

from ala import ala_helper, ala_config

DAG_ID = 'Update_gbif_metadata'


with DAG(
        dag_id=DAG_ID,
        catchup=False,
        default_args=ala_helper.get_default_args(),
        description="Update GBIF metadata",
        dagrun_timeout=timedelta(hours=1),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['multiple-dataset'],
        params={}
) as dag:

    def sync_s3_folders():

        if ala_config.S3_BUCKET_DWCA_EXPORTS == ala_config.S3_BUCKET_AVRO :
            return

        s3_client = boto3.client('s3')

        # List objects in the source folder
        source_objects = s3_client.list_objects(Bucket=ala_config.S3_BUCKET_AVRO, Prefix="dwca-exports")['Contents']

        # List objects in the destination folder
        destination_objects_list = s3_client.list_objects(Bucket=ala_config.S3_BUCKET_DWCA_EXPORTS, Prefix="dwca-exports")

        destination_objects = []

        if "Contents" in destination_objects_list:
            destination_objects = destination_objects_list['Contents']
        else:
            print("No dwca-exports in current destination.")

        # Sync each object in the source folder
        for source_object in source_objects:
            # Check if the object exists in the destination folder
            object_key = source_object['Key']

            # Retrieve the last modified timestamp of the source object
            source_timestamp = s3_client.head_object(Bucket=ala_config.S3_BUCKET_AVRO, Key=object_key)['LastModified']

            # Find the corresponding object in the destination folder
            destination_object = next((obj for obj in destination_objects if obj['Key'] == object_key), None)

            if destination_object:
                # Retrieve the last modified timestamp of the destination object
                destination_timestamp = s3_client.head_object(Bucket=ala_config.S3_BUCKET_DWCA_EXPORTS, Key=object_key)['LastModified']

                if source_timestamp > destination_timestamp:
                    print(f"Syncing updated resource '{object_key}' as it has newer timestamp.")
                    # Download the object from the source bucket
                    source_path = f"s3://{ala_config.S3_BUCKET_AVRO}/{object_key}"
                    local_filename = "/tmp/" + object_key.replace("dwca-exports", '', 1)
                    s3_client.download_file(ala_config.S3_BUCKET_AVRO, object_key, local_filename)

                    # Upload the object to the destination bucket
                    destination_path = f"s3://{ala_config.S3_BUCKET_DWCA_EXPORTS}/{object_key}"
                    s3_client.upload_file(local_filename, ala_config.S3_BUCKET_DWCA_EXPORTS, object_key)
                    print(f"Synced '{source_path}' to '{destination_path}'")

                    # Delete the locally downloaded file
                    os.remove(local_filename)
                else:
                    print(f"Skipping '{object_key}' as it already exists in the destination bucket and has an equal or older timestamp.")
            else:

                print(f"Syncing new data resource '{object_key}'")

                # Download the object from the source bucket
                source_path = f"s3://{ala_config.S3_BUCKET_AVRO}/{object_key}"
                local_filename = "/tmp/" + object_key.replace("dwca-exports", '', 1)
                s3_client.download_file(ala_config.S3_BUCKET_AVRO, object_key, local_filename)

                # Upload the object to the destination bucket
                destination_path = f"s3://{ala_config.S3_BUCKET_DWCA_EXPORTS}/{object_key}"
                s3_client.upload_file(local_filename, ala_config.S3_BUCKET_DWCA_EXPORTS, object_key)
                print(f"Synced '{source_path}' to '{destination_path}'")

                # Delete the locally downloaded file
                os.remove(local_filename)

    def update_gbif_metadata(**kwargs):

        # curl -L https://collections.ala.org.au/ws/syncGBIF -H "Authorization: <API_KEY>"
        ala_api_key = kwargs['ala_api_key']
        registry_url = kwargs['registry_url']

        # call URL
        req = Request(f"{registry_url}/syncGBIF")
        req.add_header('Authorization', ala_api_key)

        response = urlopen(req)
        print("Response code from collectory: " + str(response.getcode()))
        content = response.read()
        print(content)

    sync_s3_buckets_op = PythonOperator(
        task_id='sync_s3_buckets',
        provide_context=True,
        python_callable=sync_s3_folders)

    update_gbif_metadata_op = PythonOperator(
        task_id='call_collectory',
        provide_context=True,
        op_kwargs={ 'ala_api_key': ala_config.ALA_API_KEY, 'registry_url': ala_config.COLLECTORY_SERVER},
        python_callable=update_gbif_metadata)

    sync_s3_buckets_op >> update_gbif_metadata_op
