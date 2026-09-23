from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
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
def validate_namesmatching(record_limit: int = 0, chunk_size: int = 100000, api_workers: int = 10, use_post_request: bool = True, use_latest_prod: bool = True, use_latest_test: bool = False):

    @task
    def build_sample() -> nmcli.DataSample:
        return nmcli.get_test_data()

    def create_retrieve_task_group(local_folder: Path, data_sample: nmcli.DataSample, env: Env, sample_params: nmcli.SampleParams, use_latest: bool):
        group_id = env.name.lower()

        @task_group(group_id=group_id)
        def retrieve(local_folder: Path, data_sample: nmcli.DataSample, env: Env, sample_params: nmcli.SampleParams, use_latest: bool, group_id: str) -> nmcli.S3File:

            @task(multiple_outputs=False)
            def check_previous(local_folder: Path, env: Env, records: int, use_latest: bool) -> nmcli.S3File | None:
                def _log_generate() -> None:
                    print(f"Generating new '{env.name.lower()}' file with {records} records")

                if not use_latest:
                    _log_generate()
                    return
                    
                last = nmcli.most_recent(local_folder, env, records)
                if last is None:
                    print("No valid previous file found")
                    _log_generate()
                    return

                print(f"Using previous file: {last.s3_path}")
                return last

            @task.branch
            def sample_if_empty(previous: dict, group_id: str) -> str:
                return f"{group_id}.sample" if previous is None else f"{group_id}.resolve_output" # Run sample task if not using previous data

            @task.virtualenv(requirements=["pandas"], multiple_outputs=False)
            def sample(local_folder: Path, data_sample: dict, env: Env, sample_params: dict) -> dict[str, str]:
                from namesmatching_validation_cli import sample, SampleParams, DataSample # Required to be run within virtual env
                return sample(local_folder, DataSample(**data_sample), env, SampleParams(**sample_params))

            @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
            def resolve_output(previous_output: dict, sampled_output: dict) -> dict:
                return previous_output or sampled_output

            previous = check_previous(local_folder, env, sample_params.records, use_latest)
            branch_decision = sample_if_empty(previous, group_id)
            sample_output = sample(local_folder, asdict(data_sample), env, asdict(sample_params))
            final_output = resolve_output(previous, sample_output)

            branch_decision >> final_output # Branch to output if previous isn't empty
            branch_decision >> sample_output # Branch to sample if no previous
            sample_output >> final_output # Sample passes its value to final output when sampling

            return final_output

        return retrieve(local_folder, data_sample, env, sample_params, use_latest, group_id)

    @task.virtualenv(requirements=["pandas"])
    def compare(local_folder: Path, prod_info: nmcli.S3File, test_info: nmcli.S3File, records: int, chunksize: int) -> None:
        from namesmatching_validation_cli import compare # Required to be run within virtual env
        compare(local_folder, prod_info, test_info, records, chunksize)

    @task.virtualenv(requirements=["pandas"], trigger_rule=TriggerRule.ALL_DONE)
    def cleanup(local_folder: Path) -> None:
        from namesmatching_validation_cli import FileManager # Required to be run within virtual env
        FileManager.clear_folder(local_folder, True)

    local_folder = Path("/tmp/namesmatching")
    method = Method.POST if use_post_request else Method.GET
    sample_params = nmcli.SampleParams(method, api_workers, record_limit, chunk_size, {"User-Agent": "ala-names-matching-test/0.1"})

    data_sample = build_sample()
    prod_output = create_retrieve_task_group(local_folder, data_sample, Env.PROD, sample_params, use_latest_prod)
    test_output = create_retrieve_task_group(local_folder, data_sample, Env.TEST, sample_params, use_latest_test)
    compare(local_folder, prod_output, test_output, record_limit, chunk_size) >> cleanup(local_folder)

validate_namesmatching()
