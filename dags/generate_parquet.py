
"""
sudo -u hadoop spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.8 /tmp/generate_parquet.py -s s3://ala-databox-dev/pipelines-all-datasets/index-record/dr19123/* -d s3://ala-databox-dev/parquet-output/
"""
import argparse
import logging

from pyspark.sql.functions import explode, explode_outer, first, col, regexp_replace, lower, row_number
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime

log = logging.getLogger('pyspark')
log.setLevel(logging.INFO)

# Create a console handler and set its format
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
log.addHandler(handler)

def generate_parquet(source: list, destination: str):
    # Create SparkSession
    spark = SparkSession.builder.appName("PySparkGenerateParquetApp").getOrCreate()

    log.info("Reading parquet files from %s", source)
    df = spark.read.format("avro").option("header", "true").load(source)
    df.show()
    log.info("Records read from avro files: %d", df.count())

    def remove_duplicates(edf, existing_cols):
        edf_with_lower = edf.withColumn("key_lower", lower(col("key")))

        # pick the lower case version of the key when there are duplicates with different cases
        window_spec = Window.partitionBy("key_lower").orderBy(col("key").desc())
        df_ranked = edf_with_lower.withColumn("row_num", row_number().over(window_spec))
        df_ranked.filter(col("row_num") > 1).select("key")
        df_result = (df_ranked.filter(col("row_num") == 1).select("id", "key", "value"))

        # remove columns that already exist in the list of existing columns
        df_result = df_result.filter(~df_result.key.isin(existing_cols))
        return df_result

    def get_expanded_df(col: str, existing_cols: list):
        exp_df = df.select(
            "id",
            explode_outer(col).alias("key", "value"))

        if col == "strings":
            # Drop sensitive columns
            exp_df = exp_df.filter(~exp_df.key.startswith("sensitive_"))

        # Make sure the key doesn't contain special characters as these are going form the columns
        exp_df = exp_df.withColumn("key", regexp_replace("key", "[^0-9a-zA-Z]+", "_"))

        # TODO: Look into why some datasets have duplicate data
        #  Some datasets have duplicate data in strings for eg: kingdom and Kingdom
        #  Some datasets have duplicate data for eg: organismQuantity in strings and longs
        #  Some datasets have duplicate data for eg: eventHierarchy in strings and multiValues
        #  Remove duplicate rows which will be problematic when the columns are formed.
        #exp_df = remove_duplicates(exp_df, existing_cols)

        pivoted_df = exp_df.groupBy("id").pivot("key").agg(first("value"))
        pivoted_df = pivoted_df.withColumnRenamed("id", "partial_id")

        return pivoted_df

    # latlng, taxonID, dynamicProperties are already part of the map in strings column
    expanded_df = df.select("id", "multimedia", "annotations")

    ### TODO: Requires optimization
    for column in ["strings", "doubles", "ints", "longs", "booleans", "dates", "multiValues"]:
        partial_df = get_expanded_df(column, expanded_df.columns)
        expanded_df = expanded_df.join(partial_df, expanded_df.id == partial_df.partial_id, "outer")
        expanded_df = expanded_df.drop("partial_id")

    expanded_df.show()
    expanded_columns = expanded_df.columns
    log.info("Expanded columns: %d", len(expanded_columns))
    log.info("Columns: %s", ','.join(expanded_columns))

    """"
    ### Reorder columns
    columns_to_move = ["multimedia", "annotations"]

    move_columns_to_end= lambda l, e: [x for x in l if x not in e] + e
    new_column_list = move_columns_to_end(expanded_columns, columns_to_move)

    expanded_df = expanded_df.selectExpr(new_column_list)
    """
    expanded_df = expanded_df.drop("NULL")
    #expanded_df.display()
    expanded_df.show()

    log.info("Expanded columns: %d", len(expanded_columns))
    log.info("Columns: %s", ','.join(expanded_columns))
    log.info("Total records: %d", expanded_df.count())

    # delta format is parquet with metadata -not available with spark 2.4
    #expanded_df.write.mode("overwrite").format("delta").save("s3://ala-databox-dev/parquet-output/all-datasets")
    current_time = datetime.now()
    str_timestamp = current_time.strftime("%Y%m%dT%H%M%S")

    log.info("Output parquet files location %s", destination)

    dest_location = f"{destination}/{str_timestamp}"
    log.info("Parquet files will be written to %s", dest_location)
    expanded_df.write.mode("overwrite").format("parquet").save(dest_location)


def main():
    parser = argparse.ArgumentParser(description="Generate parquet from s3")
    parser.add_argument(
        "-s", "--s3_source",
        nargs="+",
        help="S3 path of the pipelines index avro files. If more than one s3 path, separate with space",
        required=True,
    )

    parser.add_argument(
        "-d", "--s3_dest",
        help="S3 path of the generated parquet files. Only accept one s3 path",
        required=True,
    )

    args = parser.parse_args()
    s3_source = args.s3_source # list of s3 paths
    s3_dest = args.s3_dest

    log.info("Generating parquet from source: %s", s3_source)
    log.info("Output to be written to: %s", s3_dest)

    if len(s3_source) > 0 and s3_dest:
        generate_parquet(s3_source, s3_dest)
    else:
        log.error("Please provide s3_source or s3_dest")

if __name__ == "__main__":
    main()
