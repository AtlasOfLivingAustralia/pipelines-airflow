"""
PySpark Avro Comparison Tool
Compares Avro files across two S3 buckets and produces a detailed diff report.

Requirements:
    pip install pyspark spark-avro boto3

Spark packages (submit with --packages):
    org.apache.spark:spark-avro_2.12:3.4.0
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StringType, ArrayType, MapType, StructType
)
import boto3
from urllib.parse import urlparse
import argparse
#import glob
import subprocess
import shutil
import os
from pyspark.sql.types import StructType, MapType, ArrayType


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


#SOURCE_BUCKET_PATH = "/Users/koh032/workspace/compare-avros/databox/pipelines-data/dr15085/1/occurrence/ala_taxonomy"
#TARGET_BUCKET_PATH = "/Users/koh032/workspace/compare-avros/dev/pipelines-data/dr15085/1/occurrence/ala_taxonomy"
#"s3a://your-target-bucket/path/to/avro/"

#SOURCE_BUCKET_PATH = "s3://ala-databox-avro/pipelines-data/dr15085/1/occurrence"
#TARGET_BUCKET_PATH = "s3://ala-databox-dev/pipelines-data/dr15085/1/occurrence"

SOURCE_BUCKET = "s3://ala-databox-avro"
TARGET_BUCKET = "s3://ala-databox-dev"
#PATH="pipelines-data"
PATH = "pipelines-all-datasets/index-record"
DATASET_ID = "dr15085"
PARENT_FOLDER = "" #"1/occurrence"

# Column(s) that uniquely identify a row — edit to match your schema
PRIMARY_KEYS = ["id"]

# Optional: restrict comparison to specific columns (None = compare all)
COLUMNS_TO_COMPARE = None  # e.g. ["id", "name", "amount"]

"""
COLUMNS_TO_IGNORE_BY_FOLDER = {
    "audoborn": ["created"],
    "basic": ["created"],
    "location": ["created"],
    "multimedia": ["created"],
    "event": ["created"],
    "identifier": ["firstLoaded"],
    # Add other subfolders as needed...
}
"""

COMMON_COLUMNS_TO_IGNORE = ["created", "firstLoaded", "lastLoadDate", "lastProcessedDate"]


# AWS credentials (prefer IAM roles / instance profiles over hardcoding)
AWS_ACCESS_KEY = None   # or set via environment / Spark config
AWS_SECRET_KEY = None


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark_session(app_name: str = "AvroS3Comparison") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Avro support
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.0")
        # S3A settings
        #.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        #        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )

    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        )

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def list_subfolders(s3_path: str) -> list[str]:
    """
    Inspects an S3 path and returns a list of valid S3 paths (using s3a://) that
    contain .avro files — including the root path itself AND any matching subfolders.
    """
    parsed = urlparse(s3_path.replace("s3a://", "s3://"))
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3_session = boto3.Session(profile_name="prod-data-team" if "PYCHARM_HOSTED" in os.environ else None)
    s3 = s3_session.client("s3")

    root_objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    valid_paths = []

    # 1. Check if the root directory itself contains direct .avro files
    root_contents = root_objects.get("Contents", [])
    if any(obj["Key"].endswith(".avro") for obj in root_contents):
        valid_paths.append(f"s3a://{bucket}/{prefix}".rstrip("/"))

    # 2. Check each immediate subfolder for .avro files
    subfolders = [cp["Prefix"] for cp in root_objects.get("CommonPrefixes", [])]
    for sub_prefix in subfolders:
        sub_objs = s3.list_objects_v2(Bucket=bucket, Prefix=sub_prefix, MaxKeys=50)
        sub_contents = sub_objs.get("Contents", [])

        if any(obj["Key"].endswith(".avro") for obj in sub_contents):
            valid_paths.append(f"s3a://{bucket}/{sub_prefix}".rstrip("/"))

    return valid_paths


def read_avro(spark: SparkSession, path: str):
    """Read all Avro files under *path* into a DataFrame."""
    print(f"  Reading: {path}")
    return spark.read.format("avro").load(path)


# ---------------------------------------------------------------------------
# Schema comparison
# ---------------------------------------------------------------------------

def compare_schemas(df_source, df_target) -> bool:
    src_fields = {f.name: str(f.dataType) for f in df_source.schema.fields}
    tgt_fields = {f.name: str(f.dataType) for f in df_target.schema.fields}

    only_in_source = set(src_fields) - set(tgt_fields)
    only_in_target = set(tgt_fields) - set(src_fields)
    type_mismatches = {
        col for col in src_fields.keys() & tgt_fields.keys()
        if src_fields[col] != tgt_fields[col]
    }

    print("\n=== Schema Comparison ===")
    if not (only_in_source or only_in_target or type_mismatches):
        print("  Schemas are identical.")
        return True

    if only_in_source:
        print(f"  Columns only in SOURCE : {sorted(only_in_source)}")
    if only_in_target:
        print(f"  Columns only in TARGET : {sorted(only_in_target)}")
    for col in sorted(type_mismatches):
        print(f"  Type mismatch [{col}]: source={src_fields[col]}  target={tgt_fields[col]} \n")
    return False


# ---------------------------------------------------------------------------
# Row-count comparison
# ---------------------------------------------------------------------------

def compare_row_counts(df_source, df_target):
    src_count = df_source.count()
    tgt_count = df_target.count()
    diff = src_count - tgt_count

    print("\n=== Row Count Comparison ===")
    print(f"  Source rows : {src_count:,}")
    print(f"  Target rows : {tgt_count:,}")
    print(f"  Difference  : {diff:+,}")
    return src_count, tgt_count


# ---------------------------------------------------------------------------
# Data comparison
# ---------------------------------------------------------------------------

def normalize_column(col_ref, data_type):
    """
    Recursively normalizes MapType, StructType, and ArrayType columns while
    filtering out any fields/keys present in COLUMNS_TO_IGNORE.
    """
    if col_ref is None:
        return None

    if isinstance(data_type, MapType):
        value_type = data_type.valueType

        # 1. Filter out ignored keys from the map
        all_keys = F.map_keys(col_ref)
        filtered_keys = F.array_remove(all_keys, None)
        for ignore_key in COMMON_COLUMNS_TO_IGNORE:
            filtered_keys = F.array_remove(filtered_keys, ignore_key)

        # 2. Sort remaining keys and recursively normalize their values
        sorted_keys = F.sort_array(filtered_keys)
        return F.map_from_entries(
            F.transform(
                sorted_keys,
                lambda k: F.struct(k.alias("key"), normalize_column(col_ref[k], value_type).alias("value"))
            )
        )

    elif isinstance(data_type, StructType):
        # Filter out fields whose names are in COMMON_COLUMNS_TO_IGNORE
        valid_fields = [f for f in data_type.fields if f.name not in COMMON_COLUMNS_TO_IGNORE]

        return F.struct(*[
            normalize_column(col_ref[field.name], field.dataType).alias(field.name)
            for field in valid_fields
        ])

    elif isinstance(data_type, ArrayType):
        elem_type = data_type.elementType
        if isinstance(elem_type, (MapType, StructType)):
            normalized_elems = F.transform(col_ref, lambda elem: normalize_column(elem, elem_type))
            # Arrays of structs/maps aren't guaranteed orderable directly (esp. if they
            # contain nested maps), so sort by each element's canonical JSON string.
            # For eg:
            return F.array_sort(
                normalized_elems,
                lambda a, b: (
                    F.when(F.to_json(a) < F.to_json(b), F.lit(-1))
                    .when(F.to_json(a) > F.to_json(b), F.lit(1))
                    .otherwise(F.lit(0))
                )
            )
        else:
            return F.sort_array(col_ref)

    return col_ref


def normalize_dataframe(df, cols):
    """Applies recursive map sorting across all requested schema columns."""
    return df.select([
        normalize_column(F.col(f.name), f.dataType).alias(f.name)
        if f.name in cols else F.col(f.name)
        for f in df.schema.fields
    ])


def compare_data(df_source, df_target, primary_keys: list, columns: list = None):
    common_cols = [
        f.name for f in df_source.schema.fields
        if f.name in {f2.name for f2 in df_target.schema.fields}
    ]
    compare_cols = [c for c in columns if c in common_cols] if columns else common_cols
    non_pk_cols = [c for c in compare_cols if c not in primary_keys]

    # Pre-normalize deep nested types in both DataFrames
    norm_source = normalize_dataframe(df_source.select(*compare_cols), compare_cols)
    norm_target = normalize_dataframe(df_target.select(*compare_cols), compare_cols)

    def add_row_hash(df, alias):
        concat_expr = F.concat_ws("||", *[
            F.coalesce(F.col(c).cast(StringType()), F.lit("NULL"))
            for c in non_pk_cols
        ])
        return df.withColumn("_row_hash", F.md5(concat_expr)).alias(alias)

    src = add_row_hash(norm_source, "src")
    tgt = add_row_hash(norm_target, "tgt")

    join_cond = [F.col(f"src.{k}") == F.col(f"tgt.{k}") for k in primary_keys]
    joined = src.join(tgt, on=join_cond, how="full_outer")

    rows_only_in_source = (
        joined.filter(F.col(f"tgt.{primary_keys[0]}").isNull())
        .select([F.col(f"src.{c}") for c in compare_cols])
    )

    rows_only_in_target = (
        joined.filter(F.col(f"src.{primary_keys[0]}").isNull())
        .select([F.col(f"tgt.{c}") for c in compare_cols])
    )

    rows_with_differences = (
        joined
        .filter(
            F.col(f"src.{primary_keys[0]}").isNotNull() &
            F.col(f"tgt.{primary_keys[0]}").isNotNull() &
            (F.col("src._row_hash") != F.col("tgt._row_hash"))
        )
        .select(
            *[F.col(f"src.{k}").alias(k) for k in primary_keys],
            *[F.col(f"src.{c}").alias(f"source_{c}") for c in non_pk_cols],
            *[F.col(f"tgt.{c}").alias(f"target_{c}") for c in non_pk_cols],
        )
    )

    return rows_only_in_source, rows_only_in_target, rows_with_differences

# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def flatten_for_csv(df):
    """
    Serialise any complex-typed column (ARRAY, MAP, STRUCT) to a JSON string
    in-place so the CSV writer doesn't raise UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE.

        ARRAY<STRING>          -> '["Cat","Dog"]'
        MAP<STRING,INT>        -> '{"a":1,"b":2}'
        STRUCT<x:INT,y:STRING> -> '{"x":1,"y":"hello"}'

    Primitive types are left untouched and stay in the same column.
    """
    if df is None:
        return None

    select_exprs = []
    for field in df.schema.fields:
        col_name = field.name
        data_type = field.dataType

        # If the column is a Struct, Map, or Array, serialize it to a JSON string
        if isinstance(data_type, (StructType, MapType, ArrayType)):
            select_exprs.append(F.to_json(F.col(col_name)).alias(col_name))
        else:
            select_exprs.append(F.col(col_name))

    return df.select(*select_exprs)

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_diff_report(rows_only_in_source, rows_only_in_target, rows_with_differences,
                      show_sample: int = 10):
    src_only_count = rows_only_in_source.count()
    tgt_only_count = rows_only_in_target.count()
    diff_count     = rows_with_differences.count()

    print("\n=== Data Diff Summary ===")
    print(f"  Rows only in SOURCE           : {src_only_count:,}")
    print(f"  Rows only in TARGET           : {tgt_only_count:,}")
    print(f"  Rows present in both but diff : {diff_count:,}")

    if src_only_count == 0 and tgt_only_count == 0 and diff_count == 0:
        print("\n  Files are identical.")
        return

    if src_only_count > 0:
        print(f"\n--- Sample rows only in SOURCE (up to {show_sample}) ---")
        rows_only_in_source.show(show_sample, truncate=False)

    if tgt_only_count > 0:
        print(f"\n--- Sample rows only in TARGET (up to {show_sample}) ---")
        rows_only_in_target.show(show_sample, truncate=False)

    if diff_count > 0:
        print(f"\n--- Sample rows with value differences (up to {show_sample}) ---")
        rows_with_differences.show(show_sample, truncate=False)

def write_csv(df, final_csv_filepath: str):
    """
    Writes a PySpark DataFrame to a temporary folder, extracts the single
    CSV partition file, and renames it to final_csv_filepath.
    """
    temp_dir = f"{final_csv_filepath}_temp"

    # Ensure target directory exists, but remove any pre-existing output file/folder with the exact name
    os.makedirs(os.path.dirname(os.path.abspath(final_csv_filepath)), exist_ok=True)
    if os.path.exists(final_csv_filepath):
        if os.path.isdir(final_csv_filepath):
            shutil.rmtree(final_csv_filepath)
        else:
            os.remove(final_csv_filepath)

    (df.coalesce(1)
     .write.mode("overwrite")
     .option("sep", "|")
     .option("header", True)
     .csv(temp_dir))

    # Copy from HDFS to local using hdfs dfs command
    subprocess.run(["hdfs", "dfs", "-get", f"{temp_dir}/part-00000*", final_csv_filepath], check=True)

    # Clean up HDFS temp directory
    subprocess.run(["hdfs", "dfs", "-rm", "-r", temp_dir], check=True)

def pivot_diff_for_csv(rows_with_differences, primary_keys: list):
    """
    Reshape the wide diff DataFrame into a long pivot format:

        id   | column       | source                          | target
        -----+--------------+---------------------------------+-------------------
        uuid | speciesGroup | ["Plants","Angiosperms","Dicots"]| ["Angiosperms"]
        uuid | species      | Allocasuarina fraseriana        | Allocasuarina sp.

    One row per (primary-key, changed column).

    flatten_for_csv is applied first so ARRAY/MAP/STRUCT values are already
    JSON strings before pivoting — they land cleanly in the source/target cell.
    All columns are then cast to STRING so the union schema is consistent and
    Spark's RangePartitioner never attempts a BIGINT cast during orderBy.
    """
    flat = flatten_for_csv(rows_with_differences)

    non_pk_source_cols = [
        c for c in flat.columns
        if c.startswith("source_") and c[len("source_"):] not in primary_keys
    ]
    value_col_names = [c[len("source_"):] for c in non_pk_source_cols]

    if not value_col_names:
        return flat

    # Cast PKs to STRING — prevents RangePartitioner BIGINT cast on UUID/string keys
    pk_exprs = [F.col(k).cast(StringType()).alias(k) for k in primary_keys]

    per_col_dfs = [
        flat.select(
            *pk_exprs,
            F.lit(col_name).alias("column"),
            F.col(f"source_{col_name}").cast(StringType()).alias("source"),
            F.col(f"target_{col_name}").cast(StringType()).alias("target"),
        )
        for col_name in value_col_names
    ]

    pivoted = per_col_dfs[0]
    for df in per_col_dfs[1:]:
        pivoted = pivoted.union(df)

    return pivoted.orderBy(*primary_keys, "column")

def print_csv_preview(csv_filepath: str, n_rows: int = 10):
    """Prints the header and first n data rows of a raw CSV file to the console."""
    if not os.path.exists(csv_filepath):
        return

    print(f"\n--- Preview of {os.path.basename(csv_filepath)} (up to {n_rows} rows) ---")
    with open(csv_filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i > n_rows:  # Header (row 0) + n_rows data lines
                break
            print(line.strip())

def save_diff_results(rows_only_in_source, rows_only_in_target, only_differing_cols,
                      output_path: str, folder_name: str, sample_size: int = 10):
    """
    Persist the diff DataFrames as named CSV files and print a preview to the screen.
    """
    files_to_save = [
        (flatten_for_csv(rows_only_in_source), f"{output_path}/{folder_name}_only_in_source.csv"),
        (flatten_for_csv(rows_only_in_target), f"{output_path}/{folder_name}_only_in_target.csv"),
        (only_differing_cols, f"{output_path}/{folder_name}_value_differences.csv")
    ]

    non_empty_files = []
    for df, filepath in files_to_save:
        row_count = df.count()
        if row_count > 0:
            non_empty_files.append((folder_name, os.path.basename(filepath), row_count))
            if len(non_empty_files) == 1:
                print(f"\nSaving diff results to: {output_path}")

            write_csv(df, filepath)
            print_csv_preview(filepath, sample_size)

    if len(non_empty_files) > 0:
        print("\n  Saved and displayed previews.\n")
    else:
        print("\n  No results to saved. All matched. \n")

    return non_empty_files


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compare Avro files across two S3 buckets.")
    parser.add_argument("-s", "--source",      default=SOURCE_BUCKET, help="S3 path for source Avro files")
    parser.add_argument("-t", "--target",      default=TARGET_BUCKET, help="S3 path for target Avro files")
    parser.add_argument("-p", "--path",      default=PATH, help="S3 path for target Avro files")
    parser.add_argument("-d", "--datasetId",      default=DATASET_ID, help="dataset id")
    parser.add_argument("-f", "--parent-folder", default=PARENT_FOLDER, help="Parent folder for comparison")
    parser.add_argument("-k", "--keys",        default=",".join(PRIMARY_KEYS), help="Comma-separated primary key columns")
    #parser.add_argument("--columns",     default=None, help="Comma-separated columns to compare (default: all)")
    parser.add_argument("-o", "--output",      default="result", help="S3/local path to save diff CSVs (optional)")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively compare Avro files")
    parser.add_argument("-v", "--preview",      default=10, type=int, help="Number of sample diff rows to preview")

    args = parser.parse_args()

    primary_keys = [k.strip() for k in args.keys.split(",")]
    #columns      = [c.strip() for c in args.columns.split(",")] if args.columns else None

    print("=" * 120)
    print("  PySpark Avro S3 Comparator")
    print("=" * 120)
    print(f"  Source : {args.source}")
    print(f"  Target : {args.target}")
    print(f"  PKs    : {primary_keys}")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    src_parent_path = f"{args.source}/{args.path}/{args.datasetId}/{args.parent_folder}"
    tgt_parent_path = f"{args.target}/{args.path}/{args.datasetId}/{args.parent_folder}"

    print(f"Using src_path: {src_parent_path}")
    print(f"Using tgt_path: {tgt_parent_path}")

    # Discover matching subfolders in both buckets
    src_folders = {f.rstrip("/").split("/")[-1]: f for f in list_subfolders(src_parent_path)}
    tgt_folders = {f.rstrip("/").split("/")[-1]: f for f in list_subfolders(tgt_parent_path)}

    common     = sorted(src_folders.keys() & tgt_folders.keys())
    src_only   = sorted(src_folders.keys() - tgt_folders.keys())
    tgt_only   = sorted(tgt_folders.keys() - src_folders.keys())

    print(f"\n  Subfolders in common : {common}")
    if src_only:
        print(f"  Only in source       : {src_only}")
    if tgt_only:
        print(f"  Only in target       : {tgt_only}")

    if not common:
        print("\nNo matching subfolders found. Exiting.")
        spark.stop()
        return

    summary_records = []

    for folder_name in common:
        src_path = src_folders[folder_name]
        tgt_path = tgt_folders[folder_name]

        #src_path = src_path.replace("s3a://ala-databox-avro/", "/Users/koh032/workspace/compare-avros/databox/")
        #tgt_path = tgt_path.replace("s3a://ala-databox-dev/", "/Users/koh032/workspace/compare-avros/dev/")

        print(f"\n{'=' * 120}")
        print(f"  Comparing avros in source:{src_path} and target:{tgt_path}")
        print(f"{'=' * 120}")

        print("\n--- Loading data ---")
        df_source = read_avro(spark, src_path)
        df_target = read_avro(spark, tgt_path)

        compare_schemas(df_source, df_target)
        compare_row_counts(df_source, df_target)

        # -------------------------------------------------------------------
        # Dynamic Column Filtering per Folder
        # -------------------------------------------------------------------


        #if args.columns:
        # Command-line explicit column list
        #    active_columns = [c.strip() for c in args.columns.split(",") if c not in ignored_cols]
        #else:
        # Default: Compare all columns except ignored ones
        active_columns = [c for c in df_source.columns if c not in COMMON_COLUMNS_TO_IGNORE]

        ignored_cols = set(df_source.columns) - set(active_columns) #[c for c in df_source.columns if c in COMMON_COLUMNS_TO_IGNORE] #COLUMNS_TO_IGNORE_BY_FOLDER.get(folder_name, [])

        print("\n--- Running row-level diff ---")

        if ignored_cols:
            print(f"\n---  Ignoring columns for '{folder_name}': {ignored_cols}")

        rows_only_in_source, rows_only_in_target, rows_with_differences = compare_data(
            df_source, df_target, primary_keys, active_columns
        )

        # Reshape the differences to show ONLY columns with value mismatches per PK
        pivoted_diffs = pivot_diff_for_csv(rows_with_differences, primary_keys)

        # Filter out matching rows (if any survived) and print
        only_differing_cols = pivoted_diffs.filter(
            ~(F.col("source").eqNullSafe(F.col("target")))
        )

        #print(f"\n--- Differing Columns Only (up to {args.sample}) ---")
        #only_differing_cols.show(args.sample, truncate=False)

        #print_diff_report(rows_only_in_source, rows_only_in_target, only_differing_cols, args.sample)

        if args.output:
            folder_diff_files = save_diff_results(
                rows_only_in_source,
                rows_only_in_target,
                only_differing_cols,
                args.output,
                folder_name,
                args.preview
            )
            summary_records.extend(folder_diff_files)

        #if args.output:
        #    save_diff_results(rows_only_in_source, rows_only_in_target, only_differing_cols, args.output, folder_name, args.sample)
        #   save_diff_results(rows_only_in_source, rows_only_in_target, rows_with_differences, args.output, primary_keys)
    # ---------------------------------------------------------------------------
    # Final Execution Summary Report
    # ---------------------------------------------------------------------------
    print(f"\n{'=' * 120}")
    print("  FINAL DIFF SUMMARY REPORT")
    print(f"{'=' * 120}")

    if not summary_records:
        print("  All compared folders match perfectly. No difference CSVs contain data.")
    else:
        print("  The following generated CSV files contain differences:\n")
        print(f"  {'SUBFOLDER':<20} | {'FILE NAME':<40} | {'ROW COUNT':<10}")
        print(f"  {'-'*20}-+-{'-'*40}-+-{'-'*10}")
        for folder, file_name, count in summary_records:
            print(f"  {folder:<20} | {file_name:<40} | {count:<10,}")

    print(f"\n{'=' * 120}")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()