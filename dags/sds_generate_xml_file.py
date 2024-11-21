import boto3
import os
from datetime import datetime
import subprocess
from ala import ala_config
from ala import ala_helper
from airflow.decorators import dag,task

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
    s3_bucket = ala_config.SDS_BUCKET
    s3_directory = ala_config.SDS_S3_DIRECTORY
    sds_config_filename = ala_config.SDS_CONFIG_FILENAME


    @task
    def download_jar():
        sds_artifact_url = ala_config.SDS_ARTIFACT_URL
        sds_nexus_directory = ala_config.SDS_NEXUS_STABLE_DIRECTORY
        sds_version = ala_config.SDS_VERSION
        jar_url = ala_helper.join_url(sds_artifact_url, sds_nexus_directory, sds_version, f"sds-{sds_version}-shaded.jar")
        jar_file = f"/tmp/sds-{sds_version}-shaded.jar"
        print(f"Getting URL: {jar_url}, saving to {jar_file}")
        response = ala_helper.call_url(jar_url) 
        with open(jar_file, "wb") as f:
            f.write(response.content)

        # Check if the JAR file is valid
        if not os.path.exists(jar_file):
            raise FileNotFoundError("JAR file not found")

        ala_helper.validate_jar(jar_file)
        return jar_file

    @task
    def download_xmls():
        s3 = boto3.client("s3")

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
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
        xml_filename = f"sensitive-species-data-{timestamp}.xml"
        generated_xml_file_path = f"/tmp/{xml_filename}"
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
        with open(generated_xml_file_path, "w") as output_file:
            subprocess.run(command, stdout=output_file, check=True)
        print(f"Generated XML file at {generated_xml_file_path}")

        # List the files in /tmp (for logging purposes)
        subprocess.run(["ls", "-l", "/tmp/"], check=True)
        return generated_xml_file_path

    @task
    def upload_generated_xml(xml_file_path):

        s3 = boto3.client("s3")
        # Push the results to S3
        s3.upload_file(
            xml_file_path, s3_bucket, s3_directory + "/" + os.path.basename(xml_file_path)
        )
        print(f"Uploaded {xml_file_path} to s3://{s3_bucket}/{s3_directory}")
            
    download_jar_op = download_jar()
    download_xmls_op = download_xmls()
    generate_xml_op = generate_xml(download_jar_op)
    generate_xml_op.set_upstream(download_xmls_op)
    upload_generated_xml(generate_xml_op)
    
    
dag = taskflow()