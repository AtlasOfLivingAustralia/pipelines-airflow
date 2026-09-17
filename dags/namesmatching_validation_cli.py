from pathlib import Path
from datetime import datetime
import boto3
import argparse
from ala.namesmatching_service import Method, Param, Env, NamesMatching, RetParam
import pandas as pd

s3_bucket = "ala-databox-avro"
s3_base_path = "name-matching-reporting"
s3_output_dir = "testing"

path_key = "path"
s3_key = "s3"

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

class FileManager:
    def __init__(self, local_folder: Path):
        self._local_folder = local_folder
        if not self._local_folder.exists():
            self._local_folder.mkdir(parents=True)

        self.s3_client = None

    @staticmethod
    def bucket_loc(object_path: str) -> str:
        return FileManager.join_path_parts(s3_base_path, object_path)

    @staticmethod
    def object_uri(object_path: str) -> str:
        return f"s3://{FileManager.join_path_parts(s3_bucket, s3_base_path, object_path)}"

    def local_path(self, file_path: str) -> Path:
        return self._local_folder / file_path

    @staticmethod
    def join_path_parts(*parts: str) -> str:
        return "/".join(part.strip("/") for part in parts)

    @staticmethod
    def timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d_%H%M%S")

    @staticmethod
    def delete_paths(*paths: Path) -> None:
        for path in paths:
            if path.exists():
                path.unlink()
                print(f"Cleaned up file: {path}")

    @staticmethod
    def clear_folder(folder_path: Path, delete_folder: bool = False) -> None:
        for item in folder_path.iterdir():
            if item.is_file():
                FileManager.delete_paths(item)
            else:
                FileManager.clear_folder(item, True)

        if delete_folder:
            print(f"Cleaned up folder: {folder_path}")
            folder_path.rmdir()

    @staticmethod
    def _get_client(func) -> callable:
        def wrapper(self, *args, **kwargs) -> any:
            if self.s3_client is None:
                self.s3_client = boto3.client("s3")

            return func(self, *args, **kwargs)
        return wrapper

    @_get_client
    def list_objects(self, object_path: str) -> list[str]:
        content = self.s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=self.bucket_loc(object_path))
        return [item["Key"].split(object_path)[-1].strip("/") for item in content["Contents"]]

    @_get_client
    def download(self, s3_path: str, local_path: Path, overwrite: bool = True) -> Path:
        if local_path.exists() and not overwrite:
            print(f"Local path {local_path} exists and not overwriting, skipping download")
            return local_path

        print(f"Copying file from {self.object_uri(s3_path)} to {local_path}")
        self.s3_client.download_file(s3_bucket, self.bucket_loc(s3_path), local_path)
        return local_path

    @_get_client
    def upload(self, local_path: Path, s3_path: str, delete_local: bool = False) -> str:
        print(f"Copying file from {local_path} to {self.object_uri(s3_path)}")
        self.s3_client.upload_file(local_path, s3_bucket, self.bucket_loc(s3_path))

        if delete_local:
            self.delete_paths(local_path)

        return s3_path

def retrieve(local_folder: Path, s3_sample: str, env: Env, method: Method, workers: int, records: int, chunksize: int, headers: dict = {}, use_prev: bool = False) -> dict[str, str]: 

    def most_recent(file_names: list[str]) -> str:
        last = ""
        last_time = None
        for file_name in file_names:
            name, suffix = file_name.rsplit(".", 1)
            if suffix != "csv":
                continue

            date, time, _, entries = name.split("_")
            entries = int(entries)

            if entries < records:
                continue

            timestamp = datetime.fromisoformat(f"{date}_{time}")
            if (not last) or (timestamp > last_time):
                last = file_name
                last_time = timestamp

        return last

    fm = FileManager(local_folder)
    env_str = env.name.lower()
    s3_folder = fm.join_path_parts(s3_output_dir, env_str)

    if use_prev:
        last = most_recent(fm.list_objects(s3_folder))
        if last:
            print(f"Using most recent file in s3 {last}")
            return {
                path_key: str(fm.local_path(last)),
                s3_key: last
            }

        print(f"No suitable file found in {s3_folder}, generating file")

    nm = NamesMatching(env, method, workers, headers)

    sample_file = fm.download(s3_sample, fm.local_path("sample.csv"), overwrite=False)
    output_file = fm.local_path(f"{fm.timestamp()}_{env_str}_{records}.csv")
    
    nm.run_file(sample_file, output_file, mappings, records, chunksize)
    return {
        path_key: str(output_file),
        s3_key: fm.upload(output_file, fm.join_path_parts(s3_output_dir, env_str, output_file.name))
    }

def compare(local_folder: Path, source_info: dict[str, str], compare_info: dict[str, str]) -> None:
    fm = FileManager(local_folder)

    def get_s3(type: str, info: dict[str, str]) -> Path:
        local_path = Path(info[path_key])
        s3_path = info[s3_key]

        if not local_path.exists():
            return fm.download(s3_path, local_path.name)

        print(f"Local file for {s3_path} already exists at {local_path}, using as {type} file")
        return local_path

    source_path = get_s3("source", source_info)
    compare_path = get_s3("compare", compare_info)

    source_df = pd.read_csv(source_path, dtype=str)
    compare_df = pd.read_csv(compare_path, dtype=str)

    s3_folder = f"{source_path.stem}+{compare_path.stem}" 

    issues = pd.DataFrame()
    diffs = pd.DataFrame()
    for column in source_df.columns:
        mask = source_df[column] == compare_df[column]
        expected = source_df[column].mask(mask)
        actual = compare_df[column].mask(mask)
    
        if not expected.dropna().empty:
            col_name = column.split("_", 1)[-1]
            issues[f"expected_{col_name}"] = expected
            issues[f"actual_{col_name}"] = actual
            diffs[col_name] = expected + " -> " + actual
    
    issues.insert(0, RetParam.PARAMS.value, source_df[RetParam.PARAMS.value])
    issues = issues.dropna(how="all", subset=issues.columns.difference([RetParam.PARAMS.value]))
    
    error_percent = 100 * len(issues) / len(source_df)
    print(f"Found {len(issues)} incorrect matches from {len(source_df)} records ({error_percent:.02f}%)")

    if issues.empty:
        print(f"Output from test matches output from prod")

    issues.index.name = "source_row"
    issues_path = fm.local_path(f"{fm.timestamp()}_issues.csv")
    issues.to_csv(issues_path)
    
    diff_list = []
    for column in diffs.columns:
        vc = diffs[column].value_counts()
        vc.index.name = "changes"
        vc = vc.reset_index()
        totals = pd.DataFrame({"changes": "TOTAL", "count": vc["count"].sum()}, index=[0])
        vc = pd.concat([totals, vc], ignore_index=True)
        vc.insert(0, "column", column)
        vc["percentage"] = vc["count"].apply(lambda x: f"{100 * x / len(source_df):0.2f}%")
        diff_list.append(vc)
    
    global_totals = pd.DataFrame({"column": "OVERALL", "changes": "TOTAL", "count": len(issues), "percentage": f"{error_percent:.02f}%"}, index=[0])
    diffs = pd.concat([global_totals] + diff_list)
    diffs_path = fm.local_path(f"{fm.timestamp()}_diffs.csv")
    diffs.to_csv(diffs_path, index=False)

    fm.upload(issues_path, fm.join_path_parts(s3_output_dir, "comparison", s3_folder, issues_path.name))
    fm.upload(diffs_path, fm.join_path_parts(s3_output_dir, "comparison", s3_folder, diffs_path.name))

def main(records: int, chunksize: int, workers: int, use_get: bool, use_prod: bool, use_test: bool) -> None:
    if workers < 1:
        print("Clamping workers to minimum value of 1")
        workers = 1

    if records < 0:
        print("Claming records to minimum value of 0")
        records = 0

    if chunksize < 0:
        print("Clamping chunksize to minimum value of 0")
        chunksize = 0

    method = Method.GET if use_get else Method.POST

    data_folder = Path(__file__).parents[1] / "data"
    s3_file = "namesmatching-testdata-july2026.csv"
    
    # Get prod results
    prod_info = retrieve(data_folder, s3_file, Env.PROD, method, workers, records, chunksize, use_prev=use_prod)

    # Get test results
    test_info = retrieve(data_folder, s3_file, Env.TEST, method, workers, records, chunksize, use_prev=use_test)

    # Compare test and prod
    compare(data_folder, prod_info, test_info)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate namesmatching in test with prod")
    parser.add_argument("-r", "--records", type=int, default=0, help="Amount of records to test, 0 for all (default: %(default)s)")
    parser.add_argument("-c", "--chunksize", type=int, default=100000, help="Chunksize to process in (default: %(default)s)")
    parser.add_argument("-w", "--workers", type=int, default=10, help="Amount of workers to run against names matching (default: %(default)s)")
    parser.add_argument("-g", "--useget", action="store_true", help="Use GET method for testing instead of default POST method")
    parser.add_argument("-p", "--useprod", action="store_true", help="Use latest prod file with sufficient records instead of generating")
    parser.add_argument("-t", "--usetest", action="store_true", help="Usel latest test file with sufficient records instead og generating")
    args = parser.parse_args()

    main(args.records, args.chunksize, args.workers, args.useget, args.useprod, args.usetest)
