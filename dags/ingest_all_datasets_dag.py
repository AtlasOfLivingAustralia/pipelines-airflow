"""DAG to ingest all datasets with partitioning into small/large/xlarge groups.

Updated logic: thresholds are applied per cluster (batch) rather than per-category total.
Datasets (DRs) are sorted ascending by size then distributed in round-robin "layers":
    * Pass 1 assigns the smallest remaining dataset to each cluster (if it fits)
    * Pass 2 repeats, etc.
Stopping condition for a category: when the smallest remaining dataset no longer fits in ANY
cluster (all clusters would exceed their per-cluster threshold). Remaining datasets flow
into the next, larger category (with larger threshold / capacity). XLarge category is
unbounded (no threshold) so will accept all remaining datasets.
"""

from datetime import timedelta
import logging

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from ala import ala_helper, ala_config
from ala.ala_helper import strtobool

excluded_datasets = ala_config.EXCLUDED_DATASETS

DAG_ID = "Ingest_all_datasets"

# Thresholds control cumulative total size per category (ascending order of capacity)
SMALL_TOTAL_THRESHOLD = 25_000_000  # cumulative threshold for small category per cluster in bytes
LARGE_TOTAL_THRESHOLD = 10_000_000_000  # cumulative threshold for large category per cluster in bytes

SMALL_INGEST_TASKS = 6
LARGE_INGEST_TASKS = 3
XLARGE_INGEST_TASKS = 1
TASKS_CATEGORIES = {"small": SMALL_INGEST_TASKS, "large": LARGE_INGEST_TASKS, "xlarge": XLARGE_INGEST_TASKS}


def check_args(**kwargs):
    load_images = strtobool(kwargs["dag_run"].conf["load_images"])
    skip_dwca_to_verbatim = strtobool(kwargs["dag_run"].conf["skip_dwca_to_verbatim"])
    override_uuid_percentage_check = strtobool(kwargs["dag_run"].conf["override_uuid_percentage_check"])
    kwargs["ti"].xcom_push(key="load_images", value=load_images)
    kwargs["ti"].xcom_push(key="skip_dwca_to_verbatim", value=skip_dwca_to_verbatim)
    kwargs["ti"].xcom_push(key="override_uuid_percentage_check", value=override_uuid_percentage_check)


def list_datasets_in_bucket_callable(**kwargs):
    if strtobool(kwargs["dag_run"].conf["skip_dwca_to_verbatim"]):
        return ala_helper.list_drs_verbatim_avro_in_bucket(**kwargs)
    else:
        return ala_helper.list_drs_dwca_in_bucket(**kwargs)


def partition_datasets_callable(**kwargs):
    """Partition datasets by assigning them to per-cluster thresholds in each category.

    Algorithm per category (small -> large -> xlarge):
      1. Maintain remaining (sorted) list of datasets ([(id, size), ...]).
      2. For each category create N clusters (N = *_INGEST_TASKS) with an independent capacity
         of that category's threshold (None for unbounded).
      3. Perform round-robin passes over clusters. In each pass, attempt to assign the *current
         smallest* remaining dataset to each cluster in order if it fits in that cluster's
         remaining capacity. Remove assigned datasets from the remaining list as you go.
      4. If a full pass assigns nothing (no cluster could fit the smallest dataset) stop this
         category and move on to the next; leftover datasets escalate upward.
      5. XLarge category has no threshold: all remaining datasets go there in round-robin order.
    """
    ti = kwargs["ti"]
    datasets = ti.xcom_pull(task_ids="list_datasets_in_bucket") or {}
    if not datasets:
        raise AirflowSkipException("No datasets discovered")

    remaining = sorted(datasets.items(), key=lambda kv: (kv[1], kv[0]))  # ascending by size then id
    logging.info(
        "[partition] Starting with %d datasets (min_size=%s, max_size=%s)",
        len(remaining),
        remaining[0][1] if remaining else None,
        remaining[-1][1] if remaining else None,
    )

    category_specs = [
        ("small", SMALL_INGEST_TASKS, SMALL_TOTAL_THRESHOLD),
        ("large", LARGE_INGEST_TASKS, LARGE_TOTAL_THRESHOLD),
        ("xlarge", XLARGE_INGEST_TASKS, None),  # None => unlimited
    ]

    for category, cluster_count, threshold in category_specs:
        logging.info(
            "[partition] Category=%s clusters=%d threshold=%s remaining_before=%d",
            category,
            cluster_count,
            threshold if threshold is not None else "unbounded",
            len(remaining),
        )
        # Initialize cluster tracking
        cluster_sizes = [0] * cluster_count
        cluster_ds = [[] for _ in range(cluster_count)]

        if not remaining:
            # Still push empty batches for downstream templating consistency
            for idx in range(1, cluster_count + 1):
                ti.xcom_push(key=f"process_{category}_batch{idx}", value="")
            ti.xcom_push(key=f"process_{category}", value="")
            continue

        # Round-robin layered assignment
        pass_index = 0
        while remaining:
            assigned_in_pass = False
            for cluster_index in range(cluster_count):
                if not remaining:
                    break
                ds_id, size = remaining[0]  # always consider current smallest remaining
                if threshold is not None and cluster_sizes[cluster_index] + size > threshold:
                    continue
                cluster_ds[cluster_index].append(ds_id)
                cluster_sizes[cluster_index] += size
                remaining.pop(0)
                assigned_in_pass = True
            if not assigned_in_pass:
                logging.info(
                    "[partition] Category=%s stopping: smallest dataset size=%s does not fit any cluster (filled_sizes=%s)",
                    category,
                    remaining[0][1] if remaining else None,
                    cluster_sizes,
                )
                break
            pass_index += 1
            if pass_index % 5 == 0:
                logging.info(
                    "[partition] Category=%s progress: pass=%d remaining=%d cluster_sizes=%s",
                    category,
                    pass_index,
                    len(remaining),
                    cluster_sizes,
                )

        category_map = {ds_id: datasets[ds_id] for sub in cluster_ds for ds_id in sub}
        ti.xcom_push(key=f"process_{category}", value=category_map)

        for idx, ds_list in enumerate(cluster_ds, start=1):
            batch_value = " ".join(ds_list)
            ti.xcom_push(key=f"process_{category}_batch{idx}", value=batch_value)
        logging.info(
            "[partition] Category=%s assigned_datasets=%d total_size=%s cluster_sizes=%s remaining_after=%d",
            category,
            len(category_map),
            sum(category_map.values()),
            cluster_sizes,
            len(remaining),
        )

        # If we filled zero datasets (all empty) ensure empty strings pushed (already done above)
        # Continue loop to next category with any remaining datasets.


def check_proceed(**kwargs):
    run_index = strtobool(kwargs["dag_run"].conf["run_index"])
    if not run_index:
        raise AirflowSkipException("Skipping index step")


with DAG(
    dag_id=DAG_ID,
    default_args=ala_helper.get_default_args(),
    description="Ingest all DwCAs available on S3 and run all pipelines (not including SOLR indexing)",
    dagrun_timeout=timedelta(hours=24),
    start_date=days_ago(1),
    schedule_interval=None,
    params={
        "load_images": "false",
        "skip_dwca_to_verbatim": "false",
        "run_index": "false",
        "override_uuid_percentage_check": "false",
    },
    tags=["emr", "multiple-dataset"],
) as dag:
    check_args_task = PythonOperator(
        task_id="check_args_task", provide_context=True, op_kwargs={}, python_callable=check_args
    )

    check_proceed_to_index = PythonOperator(
        task_id="check_proceed_to_index",
        provide_context=True,
        op_kwargs={},
        trigger_rule=TriggerRule.NONE_FAILED,
        python_callable=check_proceed,
    )

    list_datasets_in_bucket = PythonOperator(
        task_id="list_datasets_in_bucket",
        provide_context=True,
        op_kwargs={"bucket": ala_config.S3_BUCKET_DWCA},
        python_callable=list_datasets_in_bucket_callable,
    )

    partition = PythonOperator(
        task_id="partition_datasets", provide_context=True, python_callable=partition_datasets_callable
    )

    ingest_datasets_tasks = {}
    for cat, ingest_task_count in TASKS_CATEGORIES.items():
        ingest_datasets_tasks[cat] = []
        for batch_num in range(1, ingest_task_count + 1):
            conf_template = {
                "datasetIds": "{{ task_instance.xcom_pull(task_ids='partition_datasets', key='process_%s_batch%i') }}"
                % (cat, batch_num),
                "load_images": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='load_images') }}",
                "run_indexing": "false",
                "skip_dwca_to_verbatim": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='skip_dwca_to_verbatim') }}",
                "override_uuid_percentage_check": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='override_uuid_percentage_check') }}",
            }
            ingest_datasets_tasks[cat].append(
                TriggerDagRunOperator(
                    task_id=f"ingest_{cat}_datasets_batch{batch_num}_task",
                    trigger_dag_id=f"Ingest_{cat.replace('x', '')}_datasets",
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.NONE_FAILED,
                    conf=conf_template,
                )
            )

    full_index_to_solr = TriggerDagRunOperator(
        task_id="full_index_to_solr",
        trigger_dag_id="Full_index_to_solr",
        wait_for_completion=True,
        conf={
            "includeSampling": "true",
            "includeJackKnife": "true",
            "includeClustering": "true",
            "includeOutlier": "true",
            "skipImageSync": "true",
            "time_range": ("1991-01-01", None),
        },
    )

    # Explicit dependency wiring to avoid 'statement has no effect' lint noise
    check_args_task.set_downstream(list_datasets_in_bucket)
    list_datasets_in_bucket.set_downstream(partition)
    for t in ingest_datasets_tasks["small"]:
        partition.set_downstream(t)
        t.set_downstream(check_proceed_to_index)
    for t in ingest_datasets_tasks["large"]:
        partition.set_downstream(t)
        t.set_downstream(check_proceed_to_index)
    for t in ingest_datasets_tasks["xlarge"]:
        partition.set_downstream(t)
        t.set_downstream(check_proceed_to_index)
    check_proceed_to_index.set_downstream(full_index_to_solr)
    full_index_to_solr.set_downstream(ala_helper.get_success_notification_operator())
