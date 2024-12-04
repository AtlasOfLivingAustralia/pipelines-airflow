from distutils.util import strtobool

import math
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.utils.trigger_rule import TriggerRule

from ala import ala_helper, ala_config

excluded_datasets = ala_config.EXCLUDED_DATASETS

DAG_ID = "Ingest_all_datasets"


# list all archives under /dwca-export with file sizes
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
    SMALL_INGEST_TASKS = 6
    LARGE_INGEST_TASKS = 3
    XLARGE_INGEST_TASKS = 1
    TASKS_CATEGORIES = {
        "small": SMALL_INGEST_TASKS,
        "large": LARGE_INGEST_TASKS,
        "xlarge": XLARGE_INGEST_TASKS,
    }

    def check_args(**kwargs):
        load_images = strtobool(kwargs["dag_run"].conf["load_images"])
        skip_dwca_to_verbatim = strtobool(
            kwargs["dag_run"].conf["skip_dwca_to_verbatim"]
        )
        override_uuid_percentage_check = strtobool(
            kwargs["dag_run"].conf["override_uuid_percentage_check"]
        )
        kwargs["ti"].xcom_push(key="load_images", value=load_images)
        kwargs["ti"].xcom_push(key="skip_dwca_to_verbatim", value=skip_dwca_to_verbatim)
        kwargs["ti"].xcom_push(
            key="override_uuid_percentage_check", value=override_uuid_percentage_check
        )

    def list_datasets_in_bucket(**kwargs):
        if strtobool(kwargs["dag_run"].conf["skip_dwca_to_verbatim"]):
            return ala_helper.list_drs_verbatim_avro_in_bucket(**kwargs)
        else:
            return ala_helper.list_drs_dwca_in_bucket(**kwargs)

    def classify_datasets(category, batch_count, criteria, ti):
        min_dataset_per_ingest = 10
        datasets = ti.xcom_pull(task_ids="list_datasets_in_bucket")
        categorised_datasets = dict((k, v) for k, v in datasets.items() if criteria(v))
        ti.xcom_push(key=f"process_{category}", value=categorised_datasets)
        dataset_count = len(categorised_datasets)
        cal_batch_count = min(
            batch_count, math.ceil(dataset_count / min_dataset_per_ingest)
        )
        if datasets:
            batches = [""] * batch_count
            for index, datasetId in enumerate(categorised_datasets):
                batches[index % cal_batch_count] += " " + datasetId
            for idx, batch in enumerate(batches):
                ti.xcom_push(key=f"process_{category}_batch{idx + 1}", value=batch)
        else:
            raise AirflowSkipException

    def list_small_datasets(**kwargs):
        ti = kwargs["ti"]
        classify_datasets(
            category="small",
            batch_count=SMALL_INGEST_TASKS,
            criteria=(lambda x: x <= 5000000),
            ti=ti,
        )

    def list_large_datasets(**kwargs):
        ti = kwargs["ti"]
        classify_datasets(
            category="large",
            batch_count=LARGE_INGEST_TASKS,
            criteria=(lambda x: 5000000 < x < 5000000000),
            ti=ti,
        )

    def list_xlarge_datasets(**kwargs):
        ti = kwargs["ti"]
        classify_datasets(
            category="xlarge",
            batch_count=XLARGE_INGEST_TASKS,
            criteria=(lambda x: x > 5000000000),
            ti=ti,
        )

    def check_proceed(**kwargs):
        run_index = strtobool(kwargs["dag_run"].conf["run_index"])
        if not run_index:
            raise AirflowSkipException("Skipping index step")

    check_args_task = PythonOperator(
        task_id="check_args_task",
        provide_context=True,
        op_kwargs={},
        python_callable=check_args,
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
        python_callable=list_datasets_in_bucket,
    )

    process_small = PythonOperator(
        task_id="process_small",
        provide_context=True,
        python_callable=list_small_datasets,
    )

    process_large = PythonOperator(
        task_id="process_large",
        provide_context=True,
        python_callable=list_large_datasets,
    )

    process_xlarge = PythonOperator(
        task_id="process_xlarge",
        provide_context=True,
        python_callable=list_xlarge_datasets,
    )
    ingest_datasets_tasks = {}
    for cat, ingest_task_count in TASKS_CATEGORIES.items():
        ingest_datasets_tasks[cat] = []
        for i in range(1, ingest_task_count + 1):
            ingest_datasets_tasks[cat].append(
                TriggerDagRunOperator(
                    task_id=f"ingest_{cat}_datasets_batch{i}_task",
                    trigger_dag_id=f"Ingest_{cat.replace('x', '')}_datasets",
                    wait_for_completion=True,
                    trigger_rule=TriggerRule.NONE_FAILED,
                    conf={
                        "datasetIds": "{{ task_instance.xcom_pull(task_ids='process_%s', key='process_%s_batch%i') }}"
                        % (cat, cat, i),
                        "load_images": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='load_images') }}",
                        "run_indexing": "false",
                        "skip_dwca_to_verbatim": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='skip_dwca_to_verbatim') }}",
                        "override_uuid_percentage_check": "{{ task_instance.xcom_pull(task_ids='check_args_task', key='override_uuid_percentage_check') }}",
                    },
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

    (
        check_args_task
        >> list_datasets_in_bucket
        >> [process_xlarge, process_large, process_small]
    )
    process_small >> ingest_datasets_tasks["small"] >> check_proceed_to_index
    process_large >> ingest_datasets_tasks["large"] >> check_proceed_to_index
    process_xlarge >> ingest_datasets_tasks["xlarge"] >> check_proceed_to_index
    (
        check_proceed_to_index
        >> full_index_to_solr
        >> ala_helper.get_success_notification_operator()
    )
