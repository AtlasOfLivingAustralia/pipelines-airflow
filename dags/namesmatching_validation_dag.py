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

    @task.virtualenv(requirements=["pandas"])
    def retrieve(local_folder: Path, sample_file: str, env: Env, method: Method, workers: int, records: int, chunksize: int, use_latest: bool) -> tuple[Path, str]:
        import namesmatching_validation_cli as nmcli
        headers = {"User-Agent": "ala-names-matching-test/0.1"}
        return nmcli.retrieve(local_folder, sample_file, env, method, workers, records, chunksize, headers, use_latest)

    @task.virtualenv(requirements=["pandas"])
    def compare(local_folder: Path, local_prod: Path, s3_prod: str, local_test: Path, s3_test: str) -> tuple[str, str]:
        import namesmatching_validation_cli as nmcli
        return nmcli.compare(local_folder, local_prod, s3_prod, local_test, s3_test)

    @task.virtualenv(requirements=["pandas"])
    def cleanup(local_folder: Path) -> None:
        import namesmatching_validation_cli as nmcli
        nmcli.FileManager.clear_folder(local_folder, True)

    local_folder = Path("/tmp/namesmatching")
    method = Method.POST if use_post_request else Method.GET

    sample_file = build_sample()
    prod_local, prod_s3 = retrieve.override(task_id=f"retrieve_prod")(local_folder, sample_file, Env.PROD, method, api_workers, record_limit, chunk_size, use_latest_prod)
    test_local, test_s3 = retrieve.override(task_id=f"retrieve_test")(local_folder, sample_file, Env.TEST, method, api_workers, record_limit, chunk_size, use_latest_test)
    compare(local_folder, prod_local, prod_s3, test_local, test_s3)
    cleanup(local_folder)

validate_namesmatching()
