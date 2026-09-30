from pathlib import Path
from datetime import datetime
import boto3
import argparse
from ala.namesmatching_service import Method, Param, Env, NamesMatching
from dataclasses import dataclass, field
import time
from enum import StrEnum

dr_uids_key = "drUids"

query = f"""SELECT raw_scientificName AS {Param.SCI_NAME},
    raw_kingdom AS {Param.KINGDOM},
    raw_phylum AS {Param.PHYLUM},
    raw_class AS {Param.CLASS},
    raw_order AS "{Param.ORDER}",
    raw_family AS {Param.FAMILY},
    raw_genus AS {Param.GENUS},
    verbatimtaxonrank AS {Param.VERB_RANK},
    specificepithet AS {Param.S_EPITHET},
    infraspecificepithet AS {Param.I_EPITHET},
    raw_species AS expectedSpecies,
    matchtype AS expectedMatchType,
    scientificName AS expectedScientificName,
    taxonconceptid AS expectedTaxonConceptId,
    raw_vernacularname AS {Param.VERN_NAME},
    raw_scientificnameauthorship AS {Param.AUTHORSHIP},
    taxonid AS {Param.TAXON_ID},
    ARRAY_JOIN(ARRAY_AGG(DISTINCT t.dataresourceuid), ',') AS {dr_uids_key}
FROM taxon t
    JOIN additional a ON t.id_prefix = a.id_prefix
    AND t.dataresourceuid = a.dataresourceuid
    AND t.id = a.id
GROUP BY raw_scientificName,
    raw_kingdom,
    raw_phylum,
    raw_class,
    raw_order,
    raw_family,
    raw_genus,
    raw_specificepithet,
    raw_infraspecificepithet,
    verbatimtaxonrank,
    specificepithet,
    infraspecificepithet,
    raw_species,
    matchtype,
    scientificName,
    taxonconceptid,
    raw_vernacularname,
    raw_scientificnameauthorship,
    taxonid;"""

class S3Folder(StrEnum):
    SAMPLES = "samples"
    PROD = "prod"
    TEST = "test"
    COMP = "comparison"

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

        if sort_by:
            contents = sorted(contents, key=lambda x: x[sort_by.value], reverse=descending)

        return [item[self.Property.KEY] for item in contents if not item[self.Property.KEY].endswith("/")]

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

    @AWSManager.requires_client
    def rename(self, full_path: str, name: str) -> str:
        new_path = full_path.rsplit("/", 1)[0] + f"/{name}"
        self.move(full_path, new_path)
        return new_path

    @AWSManager.requires_client
    def delete(self, full_path: str) -> None:
        print(f"Deleting file {self.uri_from_path(full_path)}")
        self._client.delete_object(Bucket=self.bucket, Key=full_path)

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

        self._recheck_delay = 5
        self._updates_per_second = 4

    def _poll(self, query_id: str) -> tuple[bool, str]:
        _at = -1
        _cycle = "/-\\|"

        while True:
            response = self._client.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]

            if state in self.State._value2member_map_:
                print() # Skip to line after poll text
                return state == self.State.SUCCESS, response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")

            seconds = response["QueryExecution"]["Statistics"].get("TotalExecutionTimeInMillis", 0) / 1000
            duration = f"{int(seconds // 3600):02}:{int(seconds // 60) % 60:02}:{seconds % 60:05.2f}"

            bytes = response["QueryExecution"]["Statistics"].get("DataScannedInBytes", 0)
            size = next(f"{bytes / (1024**idx):.02f}{prefix}B" for idx, prefix in enumerate(("", "K", "M", "G", "T")) if (bytes >> (10 * idx)) < 1024)

            # Wait for next poll
            for _ in range(self._recheck_delay):
                try:
                    print(f"> Running Query ({_cycle[_at := (_at + 1) % len(_cycle)]}): Total time: {duration} | Data Scanned: {size}", end="\r")
                    time.sleep(1 / self._updates_per_second)

                except KeyboardInterrupt:
                    response = self._client.stop_query_execution(QueryExecutionId=query_id)
                    print()
                    
                    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                        return False, "Successfully cancelled by user"
                    return False, "Cancelled by user, query did not stop"

    @AWSManager.requires_client
    def query(self, query: str, output_uri: str) -> str | None:
        response = self._client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": self.database
            },
            ResultConfiguration={
                "OutputLocation": output_uri
            }
        )

        query_id = response["QueryExecutionId"]
        output_file = f"{output_uri.rstrip('/')}/{query_id}.csv"
        print(f"Query started, execution ID: {query_id}")

        success, reason = self._poll(query_id)
        if success: 
            print("Query finished successfully")
            print(f"Output saved at: {output_file}")
            return output_file
        
        print(f"Query failed")
        print(f"Reason: {reason}")

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

    @staticmethod
    def timestamp() -> str:
        return datetime.now().strftime(f"%Y-%m-%d{FileManager.file_name_part_char}%H%M%S")

    def create_local_path(self, core: str, suffix: str = "csv", records: int = -1, prefix_timestamp: bool = False) -> Path:
        parts = [core]

        if records >= 0:
            parts.append(str(records))

        if prefix_timestamp:
            parts.insert(0, self.timestamp())

        return self.local_path(f"{self.file_name_part_char.join(parts)}.{suffix}")

    @staticmethod
    def get_records(file_name: str) -> int:
        record_component = file_name.rsplit(FileManager.file_name_part_char, 1)[-1].split(".")[0]
        if record_component.isnumeric():
            return int(record_component)

        return -1

def get_s3(fm: FileManager, s3: S3Manager, s3_path: str, file_usage: str) -> Path:
    local_path = fm.local_path(s3_path.rsplit("/", 1)[-1])

    if local_path.exists():
        print(f"Local file for {s3_path} already exists at {local_path}, using as {file_usage} file")
    else:
        s3.download(s3_path, local_path)

    return local_path

def get_test_data(generate_sample: bool) -> str:
    s3 = S3Manager()
    data_dir = s3.full_path(S3Folder.SAMPLES)

    if not generate_sample:
        all_samples = s3.most_recent(data_dir)
        if all_samples:
            latest = all_samples[0]
            print(f"Using most recent file in s3 {latest}")
            return latest

        print("No previous sample found in s3, generating...")

    athena = AthenaManager()
    output_name = athena.query(query, s3.uri_from_path(data_dir))
    if output_name is None:
        return

    output_name = f"{data_dir}/{output_name}"
    s3.delete(f"{output_name}.metadata") # Delete generated metadata file
    return s3.rename(output_name, f"{FileManager.timestamp()}_sample_data.csv") 

def check_previous_nm_data(env: Env, min_records: int, use_prev: bool) -> str | None:
    if use_prev:
        s3 = S3Manager()
        for file_path in s3.most_recent(s3.full_path(env.name.lower())):
            file_records = FileManager.get_records(file_path)
            if file_records == 0 or file_records >= min_records:
                print(f"Using most recent file in s3 {file_path}")
                return file_path

def retrieve(local_folder: Path, sample_path: str, env: Env, sample_params: SampleParams, use_prev: bool = False) -> str:
    return check_previous_nm_data(env, sample_params.records, use_prev) or \
        sample(local_folder, sample_path, env, sample_params)

def sample(local_folder: Path, sample_path: str, env: Env, sample_params: SampleParams) -> str:
    fm = FileManager(local_folder)
    s3 = S3Manager()
    nm = NamesMatching(env, sample_params.method, sample_params.workers, sample_params.headers)

    local_sample_path = get_s3(fm, s3, sample_path, env.name.lower())

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

    source_path = get_s3(fm, s3, s3_source_path, "source")
    compare_path = get_s3(fm, s3, s3_compare_path, "compare")

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

def main(sample_params: SampleParams, generate_sample: bool, prev_prod: bool, prev_test: bool) -> None:
    data_folder = Path(__file__).parents[1] / "data"

    # Get sample file
    s3_data_sample = get_test_data(generate_sample)
    if not s3_data_sample:
        print("Error getting sample data")
        return

    # Get prod results
    s3_prod_file = retrieve(data_folder, s3_data_sample, Env.PROD, sample_params, prev_prod)

    # Get test results
    s3_test_file = retrieve(data_folder, s3_data_sample, Env.TEST, sample_params, prev_test)

    # Compare test and prod
    compare(data_folder, s3_prod_file, s3_test_file, sample_params.records, sample_params.chunksize)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate namesmatching in test with prod")
    parser.add_argument("records", type=int, help="Amount of records to test, 0 for all")
    parser.add_argument("-c", "--chunksize", type=int, default=100000, help="Chunksize to process in (default: %(default)s)")
    parser.add_argument("-w", "--workers", type=int, default=10, help="Amount of workers to run against names matching (default: %(default)s)")
    parser.add_argument("-g", "--get", action="store_true", help="Use GET method for testing instead of default POST method")
    parser.add_argument("-s", "--gensample", action="store_true", help="Generate sample file again instead of using latest")
    parser.add_argument("-p", "--prevprod", action="store_true", help="Use latest prod file with sufficient records instead of generating")
    parser.add_argument("-t", "--prevtest", action="store_true", help="Use latest test file with sufficient records instead og generating")
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
        Method.GET if args.get else Method.POST,
        args.workers,
        args.records,
        args.chunksize
    )

    main(sample_params, args.gensample, args.prevprod, args.prevtest)
