import logging
import os
import zipfile
from datetime import timedelta

import boto3
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from ala import ala_helper, ala_config

log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

_CHUNK_SIZE = 4096
OUT_DIR = "/tmp/"
VOCAB_URL = "https://repository.gbif.org/repository/releases/org/gbif/vocabulary/export/LifeStage/1.0.3/LifeStage-1.0.3.zip"
LOCAL_ZIP_PATH = f"{OUT_DIR}/LifeStage.zip"
S3_PATH = "pipelines-vocabularies/LifeStage.json"

alert_email = ala_config.ALERT_EMAIL
s3_bucket = ala_config.S3_BUCKET

DAG_ID = "Vocabulary_refresh"


@dag(
    dag_id=DAG_ID,
    default_args=ala_helper.get_default_args(),
    description="Loads LifeStage.json file from GBIF to S3 Bucket",
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval="@daily",
    tags=["vocabulary"],
    params={},
)
def taskflow():

    @task
    def download(url=VOCAB_URL, local_path: str = LOCAL_ZIP_PATH):
        """
        If downloading a zip file, use the local path as filename. Otherwise, try to get the filename from the url
        :param url: filename url
        :param local_path: download output folder
        :return: return a list of files
        """
        log.info(f"Getting this URL: {url}")
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(local_path, "wb") as of:
                for chunk in response.iter_content(_CHUNK_SIZE):
                    of.write(chunk)
        log.info(f"Downloading {local_path} finished.")
        return local_path

    @task
    def upload_file(file_to_upload):
        """Upload a file to an S3 bucket
        :param file_to_upload: File to upload
        :return: True if file was uploaded, else False
        """

        s3_session = boto3.Session()
        s3_resource = s3_session.resource("s3")
        bucket = s3_resource.Bucket(s3_bucket)

        try:
            log.info(f"Uploading file {file_to_upload} to S3...")
            result = bucket.upload_file(file_to_upload, S3_PATH)
            log.info(f"Upload finished successfully to s3://{s3_bucket}/{S3_PATH}")
        except Exception as e:
            log.error(e)
        finally:
            os.remove(file_to_upload)

    @task
    def decompress(zip_file):
        log.info(f"Unzipping file {zip_file} ...")
        with zipfile.ZipFile(zip_file, "r") as zipObj:
            for file in zipObj.namelist():
                zipObj.extract(file, path=OUT_DIR)
        os.remove(zip_file)
        json_file = os.path.join(OUT_DIR, file)
        log.info(f"Extracted file: {json_file} ")
        return json_file

    (
        upload_file(decompress(download()))
        >> ala_helper.get_success_notification_operator()
    )


dag = taskflow()
