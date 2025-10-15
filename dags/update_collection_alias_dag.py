from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import update_collection_alias_cli as ucli
from ala import ala_helper, ala_config
import logging

DAG_ID = "Update-Collection-Alias"

checks_available = [
    "check_total_count",
    "check_data_resources",
    "check_min_fields_for_random_records",
    "check_conservation_status",
    "check_spatial_layer_country",
    "check_spatial_layer_state",
    "check_sensitivity",
    "check_species_list_uid",
    "check_spatial_layer_cl959",
    "check_spatial_layer_cl1048",
    "check_spatial_layer_cl966",
    "check_spatial_layer_cl21",
]
checks_available.sort()
checks_selected = {}
with DAG(
    dag_id=DAG_ID,
    description="Update Solr Collection Alias",
    default_args=ala_helper.get_default_args(),
    dagrun_timeout=timedelta(hours=4),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "preingestion", "index"],
    params={
        **{c: True for c in checks_available},
        "new_collection": "new-biocache",
        "old_collection": "biocache",
        "collection_to_keep": "biocache-to-keep",
        "minimum_field_count": 10,
        "random_records_count": 50,
        "run_checks_only": False,
        "remove_collection_if_fails": False,
        "solr_alias": ala_config.SOLR_COLLECTION,
    },
    catchup=False,
) as dag:
    RETRY_COUNT = 10
    RETRY_SLEEP_MILSEC = 1 * 60 * 1000
    NUMBER_OF_COLLECTIONS_TO_KEEP = 2

    solr_base = ala_config.SOLR_URL
    solr_base_new = None

    def parse_params(kwargs):
        params = kwargs["dag_run"].conf.copy()
        logging.info("Parsing params")
        logging.info(f"Here is the params before adding extra items:{params}")
        params["solr_base"] = solr_base
        params["solr_base_new"] = solr_base_new
        params["solr_alias"] = ala_config.SOLR_COLLECTION
        logging.info(f"Here is the params to return :{params}")
        return params

    def check_function(check_str, check_func, **kwargs):
        params = parse_params(kwargs)
        if params[check_str]:
            ret = check_func(**params)
            if ret == ucli.ResultStatus.FAIL:
                raise Exception(f"Check {check_str} failed")
        else:
            ret = ucli.ResultStatus.SKIP
        return ret

    def check_min_fields_for_random_records(**kwargs):
        return check_function("check_min_fields_for_random_records", ucli.check_min_fields_for_random_records, **kwargs)

    def check_compare_random_records(**kwargs):
        return check_function("check_compare_random_records", ucli.check_compare_random_records, **kwargs)

    def check_total_count(**kwargs):
        return check_function("check_total_count", ucli.check_total_count, **kwargs)

    def check_data_resources(**kwargs):
        return check_function("check_data_resources", ucli.check_data_resources, **kwargs)

    def check_sensitivity(**kwargs):
        return check_function("check_sensitivity", ucli.check_sensitivity, **kwargs)

    def check_species_list_uid(**kwargs):
        return check_function("check_species_list_uid", ucli.check_species_list_uid, **kwargs)

    def check_conservation_status(**kwargs):
        return check_function("check_conservation_status", ucli.check_conservation_status, **kwargs)

    def check_spatial_layer_country(**kwargs):
        return check_function("check_spatial_layer_country", ucli.check_spatial_layer_country, **kwargs)

    def check_spatial_layer_state(**kwargs):
        return check_function("check_spatial_layer_state", ucli.check_spatial_layer_state, **kwargs)

    # Local government

    def check_spatial_layer_cl959(**kwargs):
        return check_function("check_spatial_layer_cl959", ucli.check_spatial_layer_cl959, **kwargs)

    # IBRA 7 region

    def check_spatial_layer_cl1048(**kwargs):
        return check_function("check_spatial_layer_cl1048", ucli.check_spatial_layer_cl1048, **kwargs)

    # IMCRA Meso-scale Bioregions
    def check_spatial_layer_cl966(**kwargs):
        return check_function("check_spatial_layer_cl966", ucli.check_spatial_layer_cl966, **kwargs)

    # IMCRA 4 Regions
    def check_spatial_layer_cl21(**kwargs):
        return check_function("check_spatial_layer_cl21", ucli.check_spatial_layer_cl21, **kwargs)

    def switch_collection_alias(**kwargs):
        ti = kwargs["ti"]
        params = parse_params(kwargs)
        if params["run_checks_only"]:
            print(
                f"INFO- Switching collection alias is skipped as the run_checks_only param set to {params['run_checks_only']}"
            )
            ret = ucli.ResultStatus.SKIP
        else:
            for k in checks_available:
                check_result = ti.xcom_pull(task_ids=k)
                if check_result == ucli.ResultStatus.FAIL:
                    raise Exception(f"Check {k} failed")
            ret = ucli.switch_collection_alias(**params)
        return ret

    def remove_old_collections(**kwargs):
        ti = kwargs["ti"]
        params = parse_params(kwargs)
        if params["run_checks_only"]:
            print(
                f"INFO- Removing old collections is skipped as the run_checks_only param set to {params['run_checks_only']}"
            )
            ret = ucli.ResultStatus.SKIP
        else:
            if ti.xcom_pull(task_ids="switch_collection_alias") != ucli.ResultStatus.PASS:
                print(f"INFO- Removing old collections is skipped as the switch_collection_alias failed/skipped")
                ret = ucli.ResultStatus.SKIP
            else:
                ret = ucli.remove_old_collections(**params)
        return ret

    def get_current_asserted_records(**kwargs):
        ti = kwargs["ti"]
        records_with_assertions_count = ala_helper.get_assertion_records_count()
        logging.info(f"Current asserted records count: {records_with_assertions_count}")
        ti.xcom_push(key="records_with_assertions_count", value=records_with_assertions_count)
        return records_with_assertions_count

    current_asserted_records_task = PythonOperator(
        task_id="current_asserted_records_task", python_callable=get_current_asserted_records, provide_context=True
    )

    switch_collection_alias_op = PythonOperator(
        task_id="switch_collection_alias", python_callable=switch_collection_alias, provide_context=True
    )
    remove_old_collections_op = PythonOperator(
        task_id="remove_old_collections", python_callable=remove_old_collections, provide_context=True
    )

    assertion_sync_task = TriggerDagRunOperator(
        task_id="assertion_sync_task",
        trigger_dag_id="Assertions-Sync",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        conf={"pre_sync_count": "{{ task_instance.xcom_pull(task_ids='current_asserted_records_task') }}"},
    )

    update_gbif_task = TriggerDagRunOperator(
        task_id="update_gbif_task",
        trigger_dag_id="Update_gbif_metadata",
        wait_for_completion=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    clear_dashboards_task = PythonOperator(
        task_id="clear_dashboards",
        python_callable=lambda url: ala_helper.call_url(url).status_code,
        provide_context=True,
        op_args=[ala_config.DASHBOARD_CACHE_CLEAR_URL],
    )

    with TaskGroup(group_id="index_checks") as index_checks_task_grp:
        for check in checks_available:
            (PythonOperator(task_id=check, python_callable=locals()[check], provide_context=True))
    current_asserted_records_task >> index_checks_task_grp >> switch_collection_alias_op
    switch_collection_alias_op >> remove_old_collections_op
    remove_old_collections_op >> assertion_sync_task >> clear_dashboards_task
    remove_old_collections_op >> update_gbif_task
    [update_gbif_task, clear_dashboards_task] >> ala_helper.get_success_notification_operator()
