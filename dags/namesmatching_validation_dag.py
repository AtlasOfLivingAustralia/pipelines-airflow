from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from ala.ala_helper import get_default_args
from ala.namesmatching_service import Env, Method
from pathlib import Path
import namesmatching_validation_cli as nmcli
from dataclasses import asdict

@dag(
    dag_id="Validate-Namesmatching",
    description="Compares names matching in prod and test",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "testing", "namesmatching"],
)
def validate_namesmatching(record_limit: int = 0, chunk_size: int = 100000, api_workers: int = 10, use_post_request: bool = True, regenerate_sample_data: bool = False, use_latest_prod: bool = True, use_latest_test: bool = False):

    @task
    def build_sample(regenerate_sample_data: bool) -> str:
        s3_sample_path = nmcli.get_test_data(regenerate_sample_data)
        if s3_sample_path is None:
            raise AirflowException("Unable to find or generate a sample file for testing")
        
        return s3_data_sample

    def create_retrieve_task_group(local_folder: Path, s3_data_sample: str, env: Env, sample_params: nmcli.SampleParams, use_latest: bool):
        group_id = env.name.lower()

        @task_group(group_id=group_id)
        def retrieve(local_folder: Path, data_sample: str, env: Env, sample_params: nmcli.SampleParams, use_latest: bool, group_id: str) -> str:

            @task(multiple_outputs=False)
            def check_previous(env: Env, records: int, use_latest: bool) -> str | None:
                res = nmcli.check_previous_nm_data(env, records, use_latest)
                if res is not None:
                    print(f"Using previous file: {res}")
                    return res
                
                print("No valid previous file found")
                print(f"Generating new '{env.name.lower()}' file with {records} records")

            @task.branch
            def sample_if_empty(run_sampling: bool, group_id: str) -> str:
                return f"{group_id}.sample" if run_sampling else f"{group_id}.resolve_output"

            @task.virtualenv(requirements=["pandas"])
            def sample(local_folder: Path, data_sample: str, env: Env, sample_params: dict) -> str:
                from namesmatching_validation_cli import sample, DataSample, SampleParams # Required to be run within virtual env
                return sample(local_folder, data_sample, env, SampleParams(**sample_params))

            @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
            def resolve_output(previous_output: str | None, sampled_output: str) -> str:
                return previous_output or sampled_output

            previous = check_previous(env, sample_params.records, use_latest)
            branch_decision = sample_if_empty(previous is None, group_id)
            sample_output = sample(local_folder, data_sample, env, asdict(sample_params))
            final_output = resolve_output(previous, sample_output)

            branch_decision >> final_output # Branch to output if previous isn't empty
            branch_decision >> sample_output # Branch to sample if no previous
            sample_output >> final_output # Sample passes its value to final output when sampling

            return final_output

        return retrieve(local_folder, s3_data_sample, env, sample_params, use_latest, group_id)

    @task.virtualenv(requirements=["pandas"])
    def compare(local_folder: Path, s3_prod_path: str, s3_test_path: str, records: int, chunksize: int) -> None:
        from namesmatching_validation_cli import compare # Required to be run within virtual env
        compare(local_folder, s3_prod_path, s3_test_path, records, chunksize)

    @task.virtualenv(requirements=["pandas"], trigger_rule=TriggerRule.ALL_DONE)
    def cleanup(local_folder: Path) -> None:
        from namesmatching_validation_cli import FileManager # Required to be run within virtual env
        FileManager.clear_folder(local_folder, True)

    local_folder = Path("/tmp/namesmatching")
    method = Method.POST if use_post_request else Method.GET
    sample_params = nmcli.SampleParams(method, api_workers, record_limit, chunk_size, {"User-Agent": "ala-names-matching-test/0.1"})

    s3_data_sample = build_sample(regenerate_sample_data)
    prod_output = create_retrieve_task_group(local_folder, s3_data_sample, Env.PROD, sample_params, use_latest_prod)
    test_output = create_retrieve_task_group(local_folder, s3_data_sample, Env.TEST, sample_params, use_latest_test)
    compare(local_folder, prod_output, test_output, record_limit, chunk_size) >> cleanup(local_folder)

validate_namesmatching()
