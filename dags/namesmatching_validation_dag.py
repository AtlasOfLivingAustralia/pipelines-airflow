import logging as log
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args
import boto3
from ala.namesmatching_service import Env

class FileManager:

    _s3_bucket = "ala-databox-avro"
    _s3_base_path = "name-matching-reporting"
    _local_folder = "/tmp"

    def __init__(self):
        self.s3_client = None

    def get_bucket_path(self, object_name: str) -> str:
        return f"{self._s3_base_path}/{object_name}"

    def get_local_path(self, file_name: str) -> str:
        return f"{self._local_folder}/{file_name}"

    def get_uri_path(self, object_name: str) -> str:
        return f"s3://{self._s3_bucket}/{self.get_bucket_path(object_name)}"

    @staticmethod
    def _get_client(func) -> callable:
        def wrapper(self, *args, **kwargs) -> any:
            if self.s3_client is None:
                self.s3_client = boto3.client("s3")

            return func(self, *args, **kwargs)
        return wrapper

    @_get_client
    def s3_to_local(self, s3_filename: str) -> str:
        local_path = self.get_local_path(s3_filename)
        self.s3_client.download_file(self._s3_bucket, self.get_bucket_path(s3_filename), local_path)
        log.info(f"Copied file from {self.get_uri_path(s3_filename)} to {local_path}")
        return local_path

    @_get_client
    def local_to_s3(self, local_path: str) -> str:
        s3_filename = local_path.rsplit("/", 1)[-1]
        self.s3_client.upload_file(local_path, self._s3_bucketbucket, self.get_bucket_path(s3_filename))
        log.info(f"Copied file from {local_path} to {self.get_uri_path(s3_filename)}")
        return s3_filename

@dag(
    dag_id="Validate-Namesmatching",
    description="Compares names matching in prod and test",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "testing", "namesmatching"],
)
def validate_namesmatching(record_limit: int = 0, chunk_size: int = 100000, use_post_request: bool = True):

    manager = FileManager()

    def s3_process(s3_file_name: str, env: Env) -> str:
        from ala.namesmatching_service import NamesMatching, Method, Param

        method = Method.POST if use_post_request else Method.GET
        mappings = {
            Param.KINGDOM: "rawkingdom",
            Param.PHYLUM: "rawphylum",
            Param.CLASS: "rawclass",
            Param.ORDER: "raworder",
            Param.FAMILY: "rawfamily",
            Param.GENUS: "rawgenus",
            Param.S_EPITHET: "rawspecificepithet",
            Param.I_EPITHET: "rawinfraspecificepithet",
            Param.RANK: "rawtaxonrank",
            Param.VERB_RANK: "verbatimtaxonrank",
            Param.AUTHORSHIP: "rawscientificnameauthorship",
            Param.SCI_NAME: "rawscientificname",
            Param.VERN_NAME: "rawvernacularname",
            Param.TAXON_ID: "rawtaxonid"
        }

        local_sample_path = manager.s3_to_local(s3_file_name)
        local_processed_path = manager.get_local_path(f"{env.name.lower()}.csv")

        nm = NamesMatching(env, method, 10)
        nm.run_file(local_sample_path, local_processed_path, mappings, record_limit, chunk_size)
        return manager.local_to_s3(local_processed_path)

    @task
    def build_sample() -> str:
        return "namesmatching-testdata-july2026.csv"

    @task.virtualenv(requirements=["pandas"])
    def retrieve_test(s3_file_name: str) -> str:
        return s3_process(s3_file_name, Env.TEST)

    @task.virtualenv(requirements=["pandas"])
    def retrieve_prod(s3_file_name: str) -> str:
        return s3_process(s3_file_name, Env.PROD)

    @task.virtualenv(requirements=["pandas"])
    def compare(prod_file: str, test_file: str):
        import pandas as pd
        
        local_comparison_file = manager.get_local_path("comparison.csv")
        local_diff_file = manager.get_local_path("diff.csv")
        local_prod_path = manager.s3_to_local(prod_file)
        local_test_path = manager.s3_to_local(test_file)

        prod_df = pd.read_csv(local_prod_path, dtype=str)
        test_df = pd.read_csv(local_test_path, dtype=str)
        
        issues = pd.DataFrame()
        diffs = pd.DataFrame()
        for column in prod_df.columns:
            mask = prod_df[column] == test_df[column]
            expected = prod_df[column].mask(mask)
            actual = test_df[column].mask(mask)

            if not expected.dropna().empty:
                col_name = column.split("_", 1)[-1]
                issues[f"expected_{col_name}"] = expected
                issues[f"actual_{col_name}"] = actual
                diffs[col_name] = expected + " -> " + actual

        issues.insert(0, RetParam.PARAMS.value, prod_df[RetParam.PARAMS.value])
        issues = issues.dropna(how="all", subset=issues.columns.difference([RetParam.PARAMS.value]))

        error_percent = 100 * len(issues) / len(prod_df)
        log.info(f"Found {len(issues)} incorrect matches from {len(prod_df)} records ({error_percent:.02f}%)")

        if issues.empty:
            log.info(f"Output from test matches output from prod")
            return

        issues.index.name = "source_row"
        issues.to_csv(local_comparison_file)
        manager.local_to_s3(local_comparison_file)

        diff_list = []
        for column in diffs.columns:
            vc = diffs[column].value_counts()
            vc.index.name = "changes"
            vc = vc.reset_index()
            totals = pd.DataFrame({"changes": "TOTAL", "count": vc["count"].sum()}, index=[0])
            vc = pd.concat([totals, vc], ignore_index=True)
            vc.insert(0, "column", column)
            vc["percentage"] = vc["count"].apply(lambda x: f"{100 * x / len(prod_df):0.2f}%")
            diff_list.append(vc)

        global_totals = pd.DataFrame({"column": "OVERALL", "changes": "TOTAL", "count": len(issues), "percentage": f"{error_percent:.02f}%"}, index=[0])
        diffs = pd.concat([global_totals] + diff_list)
        diffs.to_csv(local_diff_file)
        manager.local_to_s3(local_diff_file)

    sample_file = build_sample()
    test_file = retrieve_test(sample_file)
    prod_file = retrieve_prod(sample_file)
    compare(prod_file, test_file)

validate_namesmatching()
