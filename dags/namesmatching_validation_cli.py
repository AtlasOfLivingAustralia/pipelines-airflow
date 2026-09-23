from pathlib import Path
from datetime import datetime
import boto3
import argparse
from ala.namesmatching_service import Method, Param, Env, NamesMatching, RetParam
from dataclasses import dataclass, field

s3_bucket = "ala-databox-avro"
s3_base_path = "name-matching-reporting"
s3_output_dir = "testing"

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

@dataclass
class SampleParams:
    method: Method
    workers: int
    records: int
    chunksize: int
    headers: dict = field(default_factory=dict)

@dataclass
class S3File:
    local_path: str
    s3_path: str

@dataclass
class DataSample:
    s3_path: str
    mappings: dict[Param, str]

def most_recent(local_folder: Path, env: Env, records: int) -> S3File | None:
    fm = FileManager(local_folder)
    env_str = env.name.lower()
    s3_folder = fm.join_path_parts(s3_output_dir, env_str)

    last = ""
    last_time = None
    for file_name in fm.list_objects(s3_folder):
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

    if last:
        return S3File(str(fm.local_path(last)), fm.join_path_parts(s3_folder, last))

def get_test_data() -> DataSample:
    s3_path = "namesmatching-testdata-july2026.csv"
    mappings = mappings = {
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

    return DataSample(s3_path, mappings)

def retrieve(local_folder: Path, sample_data: DataSample, env: Env, sample_params: SampleParams, use_prev: bool = False) -> S3File:
    if use_prev:
        last = most_recent(local_folder, env, sample_params.records)
        if last is not None:
            print(f"Using most recent file in s3 {last.s3_path}")
            return last

    return sample(local_folder, sample_data, env, sample_params)

def sample(local_folder: Path, sample_data: DataSample, env: Env, sample_params: SampleParams) -> S3File: 
    fm = FileManager(local_folder)
    nm = NamesMatching(env, sample_params.method, sample_params.workers, sample_params.headers)
    env_str = env.name.lower()

    sample_file = fm.download(sample_data.s3_path, fm.local_path("sample.csv"), overwrite=False)
    output_file = fm.local_path(f"{fm.timestamp()}_{env_str}_{sample_params.records}.csv")
    
    nm.run_file(sample_file, output_file, sample_data.mappings, sample_params.records, sample_params.chunksize)
    s3_path = fm.upload(output_file, fm.join_path_parts(s3_output_dir, env_str, output_file.name))
    return S3File(str(output_file), s3_path)

def compare(local_folder: Path, source_info: S3File, compare_info: S3File, records: int, chunksize: int) -> None:
    import pandas as pd

    fm = FileManager(local_folder)

    def get_s3(type: str, info: S3File) -> Path:
        local_path = Path(info.local_path)

        if not local_path.exists():
            return fm.download(info.s3_path, local_path)

        print(f"Local file for {info.local_path} already exists at {local_path}, using as {type} file")
        return local_path

    source_path = get_s3("source", source_info)
    compare_path = get_s3("compare", compare_info)
    s3_folder = f"{source_path.stem}+{compare_path.stem}"

    read_kwargs = {
        "dtype": str,
        "keep_default_na": False,
        "nrows": records,
        "chunksize": chunksize // 2, # Half chunk size as 2 files open at a time
    }

    # Length totals across chunks
    issues_len = 0
    source_len = 0

    diffs_path = fm.local_path(f"{fm.timestamp()}_diffs.csv")
    diff_data: dict[str, dict[str, int]] = {}

    iterator: zip[tuple[pd.DataFrame, pd.DataFrame]] = zip(pd.read_csv(source_path, **read_kwargs), pd.read_csv(compare_path, **read_kwargs))
    for source_df, compare_df in iterator:
        compare_df = compare_df[source_df.columns]

        source_len += len(source_df)
        issues_len += (source_df != compare_df).any(axis=1).sum()

        for column in source_df.columns:
            mask = source_df[column] == compare_df[column]
            expected = source_df[column].mask(mask)
            actual = compare_df[column].mask(mask)

            if not (~mask).sum():
                continue

            diff = expected.str.cat(actual, sep=" -> ")
            vc_data = diff.value_counts().to_dict() # {change: count}

            if column not in diff_data:
                diff_data[column] = vc_data
                continue

            for change, count in vc_data.items():
                if change not in diff_data[column]:
                    diff_data[column][change] = count
                else:
                    diff_data[column][change] += count

    percentage = lambda count, total: f"{(count * 100 / total):.02f}" 
    error_percent = percentage(issues_len, source_len)

    diff_data = {"OVERALL": {"TOTAL": issues_len}} | diff_data
    flat_diff = [{"column": col_name, "change": change, "count": count, "error(%)": percentage(count, source_len)} for col_name, col_data in diff_data.items() for change, count in col_data.items()]
    diffs = pd.DataFrame.from_records(flat_diff)
    diffs.to_csv(diffs_path, index=False)

    print(f"Found {issues_len} incorrect matches from {source_len} records ({error_percent}%)")
    if issues_len == 0:
        print(f"Output from test matches output from prod")

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
    sample_params = SampleParams(method, workers, records, chunksize)

    # Get sample file
    data_sample = get_test_data()
    
    # Get prod results
    prod_info = retrieve(data_folder, data_sample, Env.PROD, sample_params, use_prev=use_prod)

    # Get test results
    test_info = retrieve(data_folder, data_sample, Env.TEST, sample_params, use_prev=use_test)

    # Compare test and prod
    compare(data_folder, prod_info, test_info, records, chunksize)

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
