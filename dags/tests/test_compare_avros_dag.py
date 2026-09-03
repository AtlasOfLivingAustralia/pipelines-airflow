
import copy
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
from pendulum import now

from ala import cluster_setup, ala_config
from ala.ala_helper import get_dataset_sizing_from_datasets, step_bash_cmd, get_default_args, step_s3_cp_file

DAG_ID = "Test_avros"
PYSPARK_FILE = "compare_avros.py"
PYSPARK_FILE_PATH = f"s3://{ala_config.S3_BUCKET}/airflow/dags/tests/{PYSPARK_FILE}"
EMR_RELEASE = "emr-7.10.0"

with (DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(),
        description="Compare Dataset avro files in source and target buckets",
        dagrun_timeout=timedelta(hours=12),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=["emr", "multiple-dataset"],
        params={
            "datasetId": "dr15085",
            "source_bucket": "s3://ala-databox-avro",
            "target_bucket": "s3://ala-databox-dev",
        },
) as dag):

    @task
    def get_dataset_emr_sizing(**kwargs) -> list[dict]:
        bucket = kwargs["dag_run"].conf["source_bucket"]
        datasets_param = kwargs["dag_run"].conf["datasetId"]
        dataset_list = datasets_param.split()
        small, large, xlarge = get_dataset_sizing_from_datasets(
            bucket=bucket, datasets_list=dataset_list, sort_desc=True
        )
        return [
            {"dr": dr, "size": size}
            for size, group in [("small", small), ("large", large), ("xlarge", xlarge)]
            for dr in group.keys()
        ]

    @task
    def get_subfolders(dr: str, **kwargs) -> list[str]:
        """Find immediate subfolders under pipelines-data/{dr}/1/ that contain avro files."""
        s3_bucket = kwargs["dag_run"].conf["source_bucket"]
        s3_bucket = s3_bucket.replace("s3://", "")
        s3 = boto3.client("s3")

        prefix = f"pipelines-data/{dr}/1/"
        subfolders_with_avro = set()

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".avro"):
                    relative = key[len(prefix):]
                    top_level = relative.split("/")[0]
                    if top_level:
                        subfolders_with_avro.add(top_level)
                    else:
                        subfolders_with_avro.add(prefix)

        return sorted(subfolders_with_avro)

    @task
    def get_test_avro_steps(dr: str, subfolders: list[str], spark_args: str, **kwargs) -> list[dict]:
        """Build one EMR step dict per subfolder."""
        s3_source_bucket = kwargs["dag_run"].conf["source_bucket"]
        s3_target_bucket = kwargs["dag_run"].conf["target_bucket"]
        # Current time
        current = now().strftime("%Y-%m-%d-%H-%M")
        output_folder = f"/tmp/{current}/{dr}"
        test_steps = [
            step_bash_cmd(
                step_name=f"Compare avro files in {s3_source_bucket} and {s3_target_bucket} for pipelines-data/{dr}/1/{folder}",
                cmd=(
                    f"spark-submit {spark_args} {PYSPARK_FILE_PATH} "
                    f"-s {s3_source_bucket} "
                    f"-t {s3_target_bucket} "
                    f"-p pipelines-data "
                    f"-d {dr} "
                    f"-f 1/{folder} "
                    f"-o {output_folder}/pipelines-data "
                ),
            )
            for folder in subfolders
        ]
        test_steps.extend([
            step_bash_cmd(
                step_name=f"Compare avro files in {s3_source_bucket} and {s3_target_bucket} for pipelines-all-datasets/index-record/{dr}",
                cmd=(
                    f"spark-submit {spark_args} {PYSPARK_FILE_PATH} "
                    f"-s {s3_source_bucket} "
                    f"-t {s3_target_bucket} "
                    f"-p pipelines-all-datasets/index-record "
                    f"-d {dr} "
                    f"-f '' "
                    f"-o {output_folder}/index-record "
                ),
            ),
            step_s3_cp_file(dr, output_folder, f"{s3_target_bucket}/test_avros_result/{current}/{dr}", "--recursive --content-disposition inline --content-type text/plain")
        ])
        return test_steps

    @task
    def get_cluster_config(dr: str, size: str) -> dict:
        cluster_name = f"{DAG_ID} for {dr}"
        if size == "small":
            base = cluster_setup.get_small_cluster(cluster_name, "")
            spark_args = ""
        elif size == "large":
            base = cluster_setup.get_medium_cluster(cluster_name, "")
            spark_args = (
                "--num-executors 8 "
                "--executor-cores 7 "
                "--executor-memory 18g "
                "--driver-memory 16g "
                "--driver-cores 2 "
            )
        else:  # xlarge
            base = cluster_setup.get_large_cluster(cluster_name, "")
            spark_args = (
                "--num-executors 8 "
                "--executor-cores 7 "
                "--executor-memory 18g "
                "--driver-memory 16g "
                "--driver-cores 2 "
            )

        config = copy.deepcopy(base)
        config["ReleaseLabel"] = EMR_RELEASE
        config["BootstrapActions"] = []

        # Return both cluster config and spark args together
        return {"cluster_config": config, "spark_args": spark_args}

    @task_group(group_id="per_dataset")
    def per_dataset_pipeline(dr: str, size: str):
        # 1. Resolve cluster config and subfolders (can run in parallel)
        cluster_config = get_cluster_config(dr=dr, size=size)
        subfolders = get_subfolders(dr=dr)

        # 2. Build the list of EMR steps from discovered subfolders
        steps = get_test_avro_steps(dr=dr, subfolders=subfolders, spark_args=cluster_config["spark_args"])

        # 3. Create the EMR cluster
        create_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            emr_conn_id="emr_default",
            job_flow_overrides=cluster_config["cluster_config"],
            aws_conn_id="aws_default",
        )

        # 4. Add all steps at once
        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id=create_cluster.output,  # output of the create_cluster
            aws_conn_id="aws_default",
            steps=steps,                         # XCom list from get_test_pipelines_data_steps
        )

        # Then still use EmrJobFlowSensor as a final gate before marking the cluster done
        termination_sensor = EmrJobFlowSensor(
            task_id="wait_for_cluster_termination",
            job_flow_id=create_cluster.output,
            target_states=["TERMINATED"],
            failed_states=["TERMINATED_WITH_ERRORS"],
            aws_conn_id="aws_default",
        )

        # Execute cluster_config and subfolders in parallel and wait for it to complete before proceeding to
        # create_cluster task
        [cluster_config, subfolders] >> steps
        steps >> create_cluster >> add_steps >> termination_sensor

    # Expand one task group per dataset
    per_dataset_pipeline.expand_kwargs(get_dataset_emr_sizing())