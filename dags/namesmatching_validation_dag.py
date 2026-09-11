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
    def retrieve(fm_kwargs: dict[str, str], s3_output_path: str, sample_file: str, env: Env, method: Method, mappings: dict[Param, str], records: int, chunksize: int) -> str:
        from ala.namesmatching_service import NamesMatching, FileManager

        fm = FileManager(**fm_kwargs)
        sample_path = fm.download(sample_file, fm.local_path("sample.csv"))
        processed_path = fm.local_path("processed.csv")

        nm = NamesMatching(env, method, 10)
        nm.run_file(sample_path, processed_path, mappings, records, chunksize)

        uploaded = fm.upload(processed_path, f"{s3_output_path.rstrip('/')}/{env.name.lower()}_{records}.csv")
        fm.delete_paths(processed_path)
        fm.delete_last_path(sample_path)

        return uploaded

    @task.virtualenv(requirements=["pandas"])
    def compare(fm_kwargs: dict[str, str], s3_output_path: str, prod_file: str, test_file: str) -> None:
        import pandas as pd
        from ala.namesmatching_service import FileManager, RetParam

        fm = FileManager(**fm_kwargs)

        prod_path = fm.download(prod_file, fm.local_path("prod.csv"))
        test_path = fm.download(test_file, fm.local_path("test.csv"))

        prod_df = pd.read_csv(prod_path, dtype=str)
        test_df = pd.read_csv(test_path, dtype=str)
        
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

        fm.delete_paths(prod_path, test_path)

        issues.insert(0, RetParam.PARAMS.value, prod_df[RetParam.PARAMS.value])
        issues = issues.dropna(how="all", subset=issues.columns.difference([RetParam.PARAMS.value]))

        error_percent = 100 * len(issues) / len(prod_df)
        print(f"Found {len(issues)} incorrect matches from {len(prod_df)} records ({error_percent:.02f}%)")

        if issues.empty:
            print(f"Output from test matches output from prod")
            return

        issues.index.name = "source_row"

        comp_path = fm.local_path("comparison.csv")
        comp_path.unlink(missing_ok=True)

        issues.to_csv(comp_path)
        fm.upload(comp_path, f"{s3_output_path.rstrip('/')}/comparison.csv")
        fm.delete_paths(comp_path)

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

        diff_path = fm.local_path("diff.csv")
        diff_path.unlink(missing_ok=True)

        diffs.to_csv(diff_path)
        fm.upload(diff_path, f"{s3_output_path.rstrip('/')}/diff.csv")
        fm.delete_paths(diff_path)

    s3_bucket = "ala-databox-avro"
    s3_base_path = "name-matching-reporting"
    s3_output_path = f"testing/{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    local_folder = "/tmp/namesmatching"

    fm_kwargs = {
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
        outputs[env.name.lower()] = retrieve.override(task_id=f"retrieve_{env.name.lower()}")(fm_kwargs, s3_output_path, sample_file, env, method, mappings, record_limit, chunk_size)

    compare(fm_kwargs, s3_output_path, outputs[Env.PROD.name.lower()], outputs[Env.TEST.name.lower()])

validate_namesmatching()
