from pathlib import Path
from datetime import datetime
import boto3
import argparse
from ala.namesmatching_service import Method, Param, Env, NamesMatching, RetParam
from dataclasses import dataclass, field
import time
from enum import StrEnum

query = "Select distinct raw_scientificName as rawScientificName, raw_kingdom as rawKingdom,\
 raw_phylum as rawPhylum, raw_class as rawClass, raw_order as rawOrder, raw_family as rawFamily,\
 raw_genus as rawGenus, raw_specificepithet as rawSpecificEpithet, raw_infraspecificepithet as rawInfraspecificEpithet,\
 verbatimtaxonrank as verbatimtaxonrank, specificepithet, infraspecificepithet, raw_species as rawSpecies,\
 matchtype as matchType, scientificName, taxonconceptid, raw_vernacularname as rawvernacularname,\
 raw_scientificnameauthorship as rawscientificnameauthorship, taxonid as rawtaxonid from taxon t,\
 additional a where t.id_prefix=a.id_prefix and t.dataresourceuid = a.dataresourceuid and t.id = a.id"

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

class S3Folder(StrEnum):
    SAMPLES = "samples"
    PROD = "prod"
    TEST = "test"
    COMP = "comparison"

@dataclass
class DataSample:
    s3_full_path: str
    mappings: dict = field(default_factory=dict)

@dataclass
class SampleParams:
    method: Method
    workers: int
    records: int
    chunksize: int
    headers: dict = field(default_factory=dict)

class AWSManager:

    class Module(StrEnum):
        S3 = "s3"
        ATHENA = "athena"

    def __init__(self, module: str, client_kwargs: dict = None):
        self._module = module
        self._client_kwargs = client_kwargs or {}
        self._client = None

    @staticmethod
    def requires_client(func) -> callable:
        def wrapper(self, *args, **kwargs) -> any:
            if self._client is None:
                self._client = boto3.client(self._module, **self._client_kwargs)

            return func(self, *args, **kwargs)
        return wrapper

class S3Manager(AWSManager):

    bucket = "ala-databox-avro"
    base_path = "name-matching-reporting/testing"

    class Property(StrEnum):
        KEY = "Key"
        MODIFIED = "LastModified"
        SIZE = "Size"

    def __init__(self):
        super().__init__(self.Module.S3)

    @staticmethod
    def join_path_parts(*parts: str) -> str:
        return "/".join(part.strip("/") for part in parts)

    def full_path(self, *path_parts: str) -> str:
        return self.join_path_parts(self.base_path, *path_parts)

    def uri_from_path(self, full_path: str) -> str:
        return f"s3://{self.bucket}/{full_path}"

    def full_uri(self, *path_parts: str) -> str:
        return self.uri_from_path(self.full_path(*path_parts))

    def most_recent(self, full_path: str) -> list[str]:
        return self.list_objects(full_path, self.Property.MODIFIED, True)

    @AWSManager.requires_client
    def list_objects(self, full_path: str, sort_by: Property = None, descending: bool = False) -> list[str]:
        response = self._client.list_objects_v2(Bucket=self.bucket, Prefix=full_path)
        contents = response["Contents"]

        if sort_by is None:
            return [item[self.Property.KEY] for item in contents]

        return [item[self.Property.KEY] for item in sorted(response["Contents"], key=lambda x: x[sort_by.value], reverse=descending)]

    @AWSManager.requires_client
    def download(self, full_path: str, local_path: Path) -> None:
        print(f"Copying file from {self.uri_from_path(full_path)} to {local_path}")
        self._client.download_file(self.bucket, full_path, local_path)

    @AWSManager.requires_client
    def upload(self, local_path: Path, full_path: str) -> None:
        print(f"Copying file from {local_path} to {self.uri_from_path(full_path)}")
        self._client.upload_file(local_path, self.bucket, full_path)

    @AWSManager.requires_client
    def move(self, from_full_path: str, to_full_path: str) -> None:
       print(f"Moving file from {self.uri_from_path(from_full_path)} to {self.uri_from_path(to_full_path)}")
       self._client.copy_object(Bucket=self.bucket, Key=to_full_path, CopySource={"Bucket": self.bucket, "Key": from_full_path})
       self._client.delete_object(Bucket=self.bucket, Key=from_full_path)

class AthenaManager(AWSManager):

    region = "ap-southeast-2"
    database = "prod"
    s3_dir = "samples"

    class State(StrEnum):
        SUCCESS = "SUCCEEDED"
        FAILED = "FAILED"
        CANCELLED = "CANCELLED"

    def __init__(self):
        super().__init__(self.Module.ATHENA)

        self._update_interval = 0.5
        self._updates_recheck = 20

    def _poll(self, query_id: str) -> tuple[bool, str]:
        _at = -1
        _cycle = "/-\\|"

        while True:
            response = self._client.get_query_execution(QueryExecutionId=query_id)
            state = response['QueryExecution']['Status']['State']

            if state in self.State._value2member_map_:
                print() # Skip to line after poll text
                return state == self.State.SUCCESS, response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')

            # Wait for next poll
            for _ in range(self._updates_recheck):
                print(f"Running Query ({_cycle[_at := (_at + 1) % len(_cycle)]})", end="\r")
                time.sleep(self._update_interval)

    @AWSManager.requires_client
    def query(self, query: str, output_uri: str) -> str:
        response = self._client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": self.database
            },
            ResultConfiguration={
                "OutputLocation": output_uri
            }
        )

        query_id = response['QueryExecutionId']
        output_file = f"{output_uri.rstrip('/')}/{query_id}.csv"
        print(f"Query started, execution ID: {query_id}")

        success, reason = self._poll(query_id)
        if success: 
            print("Query finished successfully")
            print(f"Output saved at: {output_file}")
            return output_file
        
        print(f"Query failed")
        print(f"Reason: {reason}")
        return ""

class FileManager:

    file_name_part_char = "_"

    def __init__(self, local_folder: Path = None):
        self._local_folder = local_folder or Path.cwd()
        
        if not self._local_folder.exists():
            self._local_folder.mkdir(parents=True)

    def local_path(self, file_path: str) -> Path:
        return self._local_folder / file_path

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

    def timestamp(self) -> str:
        return datetime.now().strftime(f"%Y-%m-%d{self.file_name_part_char}%H%M%S")

    def create_local_path(self, core: str, suffix: str = "csv", records: int = -1, prefix_timestamp: bool = False) -> Path:
        parts = [core]

        if records >= 0:
            parts.append(records)

        if prefix_timestamp:
            parts.insert(0, self.timestamp())

        return self.local_path(f"{self.file_name_part_char.join(parts)}.{suffix}")

    def get_records(self, file_name: str) -> int:
        record_component = file_name.rsplit(self.file_name_part_char, 1)[-1].split(".")[0]
        if record_component.isnumeric():
            return int(record_component)

        return -1
    
def get_test_data(use_prev: bool) -> DataSample:
    s3 = S3Manager()
    data_dir = s3.full_path(S3Folder.SAMPLES)

    if use_prev:
        all_samples = s3.most_recent(data_dir)
        if all_samples:
            latest = all_samples[0]
            print(f"Using most recent file in s3 {latest}")
            return latest

    athena = AthenaManager()
    output_name = athena.query(query, s3.uri_from_path(data_dir))
    return DataSample(f"{data_dir}/{output_name}", mappings) if output_name else DataSample()

def retrieve(local_folder: Path, sample_path: str, env: Env, sample_params: SampleParams, use_prev: bool = False) -> str:
    if use_prev:
        s3 = S3Manager()
        fm = FileManager()

        for file_path in s3.most_recent(s3.full_path(env.name.lower())):
            records = fm.get_records(file_path)
            if records == 0 or records >= sample_params.records:
                print(f"Using most recent file in s3 {file_path}")
                return file_path

    return sample(local_folder, sample_path, env, sample_params)

def sample(local_folder: Path, sample_path: str, env: Env, sample_params: SampleParams) -> str: 
    fm = FileManager(local_folder)
    s3 = S3Manager()
    nm = NamesMatching(env, sample_params.method, sample_params.workers, sample_params.headers)

    local_sample_path = fm.local_path("sample.csv")
    if local_sample_path.exists():
        print(f"Using local file: {local_sample_path}")
    else:
        s3.download(sample_path, local_sample_path)

    output_file_path = fm.create_local_path(env.name.lower(), records=sample_params.records, prefix_timestamp=True)
    if output_file_path.exists():
        fm.delete_paths(output_file_path)

    nm.run_file(local_sample_path, output_file_path, rows=sample_params.records, chunksize=sample_params.chunksize)

    upload_location = s3.full_path(env.name.lower(), output_file_path.name)
    s3.upload(output_file_path, upload_location)
    return upload_location

def compare(local_folder: Path, s3_source_path: str, s3_compare_path: str, records: int, chunksize: int) -> None:
    import pandas as pd

    fm = FileManager(local_folder)
    s3 = S3Manager()

    def get_s3(type: str, s3_path: str) -> Path:
        local_path = fm.local_path(s3_path.rsplit("/", 1)[-1])

        if local_path.exists():
            print(f"Local file for {s3_path} already exists at {local_path}, using as {type} file")
        else:
            s3.download(s3_path, local_path)

        return local_path

    source_path = get_s3("source", s3_source_path)
    compare_path = get_s3("compare", s3_compare_path)

    read_kwargs = {
        "dtype": str,
        "keep_default_na": False,
        "nrows": records,
        "chunksize": chunksize // 2, # Half chunk size as 2 files open at a time
    }

    # Length totals across chunks
    issues_len = 0
    source_len = 0

    diffs_path = fm.create_local_path("diffs", prefix_timestamp=True)
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

    upload_path = s3.full_path(S3Folder.COMP, f"{source_path.stem}+{compare_path.stem}", diffs_path.name)
    s3.upload(diffs_path, upload_path)

def main(sample_params: SampleParams, use_sample: bool, use_prod: bool, use_test: bool) -> None:
    data_folder = Path(__file__).parents[1] / "data"

    # Get sample file
    s3_data_sample = get_test_data(use_sample)
    if not s3_data_sample:
        print("Error getting sample data")
        return

    # Get prod results
    s3_prod_file = retrieve(data_folder, s3_data_sample, Env.PROD, sample_params, use_prev=use_prod)

    # Get test results
    s3_test_file = retrieve(data_folder, s3_data_sample, Env.TEST, sample_params, use_prev=use_test)

    # Compare test and prod
    compare(data_folder, s3_prod_file, s3_test_file, sample_params.records, sample_params.chunksize)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate namesmatching in test with prod")
    parser.add_argument("records", type=int, help="Amount of records to test, 0 for all")
    parser.add_argument("-c", "--chunksize", type=int, default=100000, help="Chunksize to process in (default: %(default)s)")
    parser.add_argument("-w", "--workers", type=int, default=10, help="Amount of workers to run against names matching (default: %(default)s)")
    parser.add_argument("-g", "--useget", action="store_true", help="Use GET method for testing instead of default POST method")
    parser.add_argument("-s", "--usesample", action="store_true", help="Use latest sample file instead of generating")
    parser.add_argument("-p", "--useprod", action="store_true", help="Use latest prod file with sufficient records instead of generating")
    parser.add_argument("-t", "--usetest", action="store_true", help="Usel latest test file with sufficient records instead og generating")
    args = parser.parse_args()

    minimums = {
        "workers": 1,
        "records": 0,
        "chunksize": 0
    }

    for key, value in minimums.items():
        if getattr(args, key) < value:
            print(f"Clamping {key} to minimum value of {value}")
            setattr(args, key, value)

    sample_params = SampleParams(
        Method.GET if args.useget else Method.POST,
        args.workers,
        args.records,
        args.chunksize
    )

    main(sample_params, args.usesample, args.useprod, args.usetest)
