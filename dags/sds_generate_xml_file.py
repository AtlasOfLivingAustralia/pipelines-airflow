from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os
import requests
from datetime import datetime
import subprocess
from ala import ala_config
from ala import ala_helper
from airflow.decorators import dag,task
# Define the DAG
DAG_ID = 'SDS_generate_xml_file'

@dag(
    DAG_ID,
    description="Generate XML file for SDS",
    default_args=ala_helper.get_default_args(),
    tags=["sds"],
    schedule_interval=None,
    catchup=False,
)
def taskflow():
    pass

    # Airflow variables (config values)
    sds_artifact_url = ala_config.SDS_ARTIFACT_URL
    sds_nexus_directory = ala_config.SDS_NEXUS_STABLE_DIRECTORY
    sds_version = ala_config.SDS_VERSION
    s3_bucket = ala_config.SDS_BUCKET
    s3_directory = ala_config.SDS_S3_DIRECTORY
    sds_config_filename = ala_config.SDS_CONFIG_FILENAME

    # Derived variables
    jar_url = ala_helper.join_url(sds_artifact_url, sds_nexus_directory, sds_version, f"sds-{sds_version}-shaded.jar")
    jar_file = f"/tmp/sds-{sds_version}-shaded.jar"


    # XML sensitive species file with timestamp in the filename
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    xml_filename = f"sensitive-species-data-{timestamp}.xml"
    xml_file_path = f"/tmp/{xml_filename}"
    run_aws = True  # Set to False on local machine to avoid S3 access
    s3 = boto3.client("s3")


    @task
    def download_jar():
        # Download the JAR file
        print(f"Getting URL: {jar_url}, saving to {jar_file}")
        response = ala_helper.call_url(jar_url) 
        with open(jar_file, "wb") as f:
            f.write(response.content)

        # Check if the JAR file is valid
        if not os.path.exists(jar_file):
            raise FileNotFoundError("JAR file not found")

        # Check the jar file is valid (can be an ascii file when jar is not found in nexus)
        result = subprocess.run(["file", jar_file], capture_output=True, text=True)
        if "Java archive" not in result.stdout:
            raise ValueError(f"{jar_file} is not a valid Java archive")
        print(f"{jar_file} is a valid Java archive")
        
        return jar_file

    @task
    def download_xmls():
        files_to_download = [
            sds_config_filename,
            # "layers.json",
            "sensitivity-zones.xml",
            "sensitivity-categories.xml",
        ]

        for file_name in files_to_download:
            print(f"Downloading {file_name} from s3://{s3_bucket}/{s3_directory}")
            s3.download_file(
                s3_bucket,
                f"{s3_directory}/{file_name}",
                f"/tmp/{file_name}",
            )
        print(f"Downloaded {', '.join(files_to_download)} from s3")


    @task
    def generate_xml(jar_file):
        # Run the JAR file
        print(f"Running JAR file {jar_file} to generate XML")
        command = [
            "java",
            f"-Dsds.config.file=/tmp/{sds_config_filename}",
            "-jar",
            jar_file,
            "au.org.ala.sds.util.SensitiveSpeciesXmlBuilder",
        ]
        print(f"Generating XML file via {' '.join(command)}")
        with open(xml_file_path, "w") as output_file:
            subprocess.run(command, stdout=output_file, check=True)
        print(f"Generated XML file at {xml_file_path}")

        # List the files in /tmp (for logging purposes)
        subprocess.run(["ls", "-l", "/tmp/"], check=True)

    @task
    def upload_generated_xml():
        # Push the results to S3
        s3.upload_file(
            xml_file_path, s3_bucket, s3_directory + "/" + xml_filename
        )
        print(f"Uploaded {xml_filename} to s3://{s3_bucket}/{s3_directory}")
            
    download_jar_op = download_jar()
    download_xmls_op = download_xmls()
    generate_xml_op = generate_xml(download_jar_op)
    generate_xml_op.set_upstream(download_xmls_op)
    upload_generated_xml().set_upstream(generate_xml_op)
    
    
dag = taskflow()