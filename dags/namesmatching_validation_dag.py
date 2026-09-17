from datetime import datetime, timedelta
from airflow.decorators import dag, task
from ala.ala_helper import get_default_args
from ala.namesmatching_service import Env, Method
from pathlib import Path

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
    def build_sample() -> str:
        return "namesmatching-testdata-july2026.csv"

    @task.virtualenv(requirements=["pandas"], multiple_outputs=False)
    def retrieve(local_folder: Path, sample_file: str, env: Env, method: Method, workers: int, records: int, chunksize: int, use_latest: bool) -> dict[str, str]:
        import namesmatching_validation_cli as nmcli
        headers = {"User-Agent": "ala-names-matching-test/0.1"}
        return nmcli.retrieve(local_folder, sample_file, env, method, workers, records, chunksize, headers, use_latest)

    @task.virtualenv(requirements=["pandas"])
    def compare(local_folder: Path, prod_info: dict[str, str], test_info: dict[str, str]) -> None:
        import namesmatching_validation_cli as nmcli
        nmcli.compare(local_folder, prod_info, test_info)

    @task.virtualenv(requirements=["pandas"])
    def cleanup(local_folder: Path) -> None:
        import namesmatching_validation_cli as nmcli
        nmcli.FileManager.clear_folder(local_folder, True)

    local_folder = Path("/tmp/namesmatching")
    method = Method.POST if use_post_request else Method.GET

    sample_file = build_sample()
    prod_outputs = retrieve.override(task_id=f"retrieve_prod")(local_folder, sample_file, Env.PROD, method, api_workers, record_limit, chunk_size, use_latest_prod)
    test_outputs = retrieve.override(task_id=f"retrieve_test")(local_folder, sample_file, Env.TEST, method, api_workers, record_limit, chunk_size, use_latest_test)
    compare(local_folder, prod_outputs, test_outputs) >> cleanup(local_folder)

validate_namesmatching()
