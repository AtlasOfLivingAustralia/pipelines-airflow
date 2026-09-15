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
def validate_namesmatching(record_limit: int = 0, chunk_size: int = 100000, api_workers: int = 10, use_post_request: bool = True):

    @task
    def build_sample() -> str:
        return "namesmatching-testdata-july2026.csv"

    @task.virtualenv(requirements=["pandas"])
    def retrieve(local_folder: Path, sample_file: str, env: Env, method: Method, workers: int, records: int, chunksize: int) -> str:
        import namesmatching_validation_cli as nmcli
        return nmcli.retrieve(local_folder, sample_file, env, method, workers, records, chunksize)

    @task.virtualenv(requirements=["pandas"])
    def compare(local_folder: Path, s3_prod: str, s3_test: str) -> None:
        import namesmatching_validation_cli as nmcli
        return nmcli.compare(local_folder, s3_prod, s3_test)

    local_folder = Path("/tmp/namesmatching")
    method = Method.POST if use_post_request else Method.GET

    sample_file = build_sample()

    outputs = {}
    for env in (Env.PROD, Env.TEST):
        outputs[env.name.lower()] = retrieve.override(task_id=f"retrieve_{env.name.lower()}")(local_folder, sample_file, env, method, api_workers, record_limit, chunk_size)

    compare(local_folder, outputs[Env.PROD.name.lower()], outputs[Env.TEST.name.lower()])

validate_namesmatching()
