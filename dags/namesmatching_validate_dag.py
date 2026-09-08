import logging as log
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args
from ala.namesmatching_service import NamesMatching, Method, Env, Param, RetParam
from ala import ala_config
import boto3
import pandas as pd
from io import BytesIO, StringIO

class S3Manager:

    bucket = "ala-databox-avro"
    s3_base_path = "name-matching-reporting"

    def __init__(self):
        self.s3_client = None

    @staticmethod
    def _full_path(object_path: str) -> str:
        return f"{S3Manager.s3_base_path}/{object_path}"

    @staticmethod
    def _uri_path(object_path: str) -> str:
        return f"s3://{S3Manager.bucket}/{S3Manager._full_path(object_path)}"

    @staticmethod
    def _get_client(func) -> callable:
        def wrapper(self, *args, **kwargs) -> any:
            if self.s3_client is None:
                self.s3_client = boto3.client("s3")

            return func(self, *args, **kwargs)
        return wrapper

    @_get_client
    def load_from_s3(self, s3_path: str) -> pd.DataFrame:
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self._full_path(s3_path))
        df = pd.read_csv(BytesIO(response['Body'].read()), dtype=str)
        log.info(f"Read csv at {self._uri_path(s3_path)}")
        return df

    @_get_client
    def save_to_s3(self, s3_path: str, df: pd.DataFrame) -> str:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(Bucket=self.bucket, Key=self._full_path(s3_path), Body=csv_buffer.getvalue())
        log.info(f"Saved csv to {self._uri_path(s3_path)}")
        return s3_path

@dag(
    dag_id="Validate-Namesmatching",
    description="Compares names matching in prod and test",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "testing", "namesmatching"],
)
def validate_namesmatching(use_post_request: bool = True):

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

    manager = S3Manager()

    @task
    def build_sample() -> str:
        return "namesmatching-testdata-july2026.csv"

    @task
    def retrieve_test(sample_file: str) -> str:
        df = manager.load_from_s3(sample_file)
        nm = NamesMatching(Env.TEST, method, ala_config.NAME_MATCHING_WORKERS)

        df = nm.process_df(df, mappings)
        return manager.save_to_s3("test.csv", df)

    @task
    def retrieve_prod(sample_file: str) -> str:
        df = manager.load_from_s3(sample_file)
        nm = NamesMatching(Env.PROD, method, ala_config.NAME_MATCHING_WORKERS)

        df = nm.process_df(df, mappings)
        return manager.save_to_s3("prod.csv", df)

    @task
    def compare(prod_file: str, test_file: str):
        prod_df = manager.load_from_s3(prod_file)
        test_df = manager.load_from_s3(test_file)

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
        manager.save_to_s3("comparison.csv", issues)

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
        manager.save_to_s3("diffs.csv", diffs)

    sample_file = build_sample()
    test_file = retrieve_test(sample_file)
    prod_file = retrieve_prod(sample_file)
    compare(prod_file, test_file)

validate_namesmatching()
