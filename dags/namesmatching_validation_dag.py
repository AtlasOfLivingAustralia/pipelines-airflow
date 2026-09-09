from datetime import datetime, timedelta
from airflow.decorators import dag, task
from ala.ala_helper import get_default_args
from ala.namesmatching_service import Method, Param, Env

@dag(
    dag_id="Validate-Namesmatching",
    description="Compares names matching in prod and test",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "testing", "namesmatching"],
)
def validate_namesmatching(record_limit: int = 0, chunk_size: int = 100000, use_post_request: bool = True):

    @task
    def build_sample() -> str:
        return "namesmatching-testdata-july2026.csv"

    @task.virtualenv(requirements=["pandas"])
    def retrieve(s3_manager_kwargs: dict[str, str], s3_output_path: str, sample_file: str, env: Env, method: Method, mappings: dict[Param, str], records: int, chunksize: int) -> str:
        from ala.namesmatching_service import NamesMatching, S3FileManager

        s3 = S3FileManager(**s3_manager_kwargs)
        local_sample = s3.download(sample_file, "sample.csv")
        processed = s3.local_path("processed.csv")

        nm = NamesMatching(env, method, 10)
        nm.run_file(local_sample, processed, mappings, records, chunksize)

        return s3.upload(processed, f"{s3_output_path.rstrip('/')}/{env.name.lower()}_{records}.csv")

    @task.virtualenv(requirements=["pandas"])
    def compare(s3_manager_kwargs: dict[str, str], s3_output_path: str, prod_file: str, test_file: str) -> None:
        import pandas as pd
        from ala.namesmatching_service import S3FileManager, RetParam

        s3 = S3FileManager(**s3_manager_kwargs)

        local_prod = s3.download(prod_file, "prod.csv")
        local_test = s3.download(test_file, "test.csv")

        prod_df = pd.read_csv(local_prod, dtype=str)
        test_df = pd.read_csv(local_test, dtype=str)
        
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
        print(f"Found {len(issues)} incorrect matches from {len(prod_df)} records ({error_percent:.02f}%)")

        if issues.empty:
            print(f"Output from test matches output from prod")
            return

        issues.index.name = "source_row"

        local_comp = s3.local_path("comparison.csv")
        issues.to_csv(local_comp)
        s3.upload(local_comp, f"{s3_output_path.rstrip('/')}/comparison.csv")

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

        local_diff = s3.local_path("diff.csv")
        diffs.to_csv(local_diff)
        s3.upload(local_diff, f"{s3_output_path.rstrip('/')}/diff.csv")

    s3_bucket = "ala-databox-avro"
    s3_base_path = "name-matching-reporting"
    s3_output_path = f"testing/{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    local_folder = "/tmp"

    s3_manager_kwargs = {
        "bucket": s3_bucket,
        "base_path": s3_base_path,
        "local_folder": local_folder
    }

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

    sample_file = build_sample()

    outputs = {}
    for env in (Env.PROD, Env.TEST):
        outputs[env.name.lower()] = retrieve.override(task_id=f"retrieve_{env.name.lower()}")(s3_manager_kwargs, s3_output_path, sample_file, env, method, mappings, record_limit, chunk_size)

    compare(s3_manager_kwargs, s3_output_path, outputs[Env.PROD.name.lower()], outputs[Env.TEST.name.lower()])

validate_namesmatching()
