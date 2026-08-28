import logging as log
from datetime import datetime, timedelta
import boto3
import requests
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args, update_registry_metadata
from ala import ala_config

DAG_ID = "Update-Data-Currency"

with DAG(
    dag_id=DAG_ID,
    description="Update data currency field in collectory of all datasets in solr",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "all-datasets"],
    params={
        "datasetIds": ""
    }
) as dag:
    
    def update_dates_from_solr(datasetIDs: str) -> None:
        filter_uid_list = datasetIDs.split()

        solr_facet = "lastLoadDate"
        collectory_value = "dataCurrency"

        url = f"{ala_config.SOLR_URL}/biocache/select"
        headers = {
            "User-Agent": "dataCurrencyDAG"
        }

        params = {
            "query": "*:*",
            "limit": 0,
            "facet": {
                "distinct_items": {
                    "type": "terms",
                    "field": "dataResourceUid",
                    "limit": -1,
                    "numBuckets": True,
                    "facet": {solr_facet: f"max({solr_facet})"}
                }
            }
        }

        log.info(f"Querying solr at {url}")
        response = requests.post(url, headers=headers, json=params)
        payload = response.json()

        if response.status_code != 200:
            log.info(f"Got {response.status_code} from solr: {response.reason}")
            AirflowException(f"Got error from solr: {payload['error']['msg']}")

        solr_errors = []
        collectory_errors = []
        distinct_items = payload["facets"]["distinct_items"]
        for bucket in distinct_items["buckets"]:
            dr_uid = bucket["val"]

            if filter_uid_list and dr_uid not in filter_uid_list:
                continue

            value = bucket.get(solr_facet, "")
            if not value:
                log.warning(f"Skipping {collectory_value} update for dr {dr_uid}, solr value is empty")
                solr_errors.append(dr_uid)
                continue

            try:
                update_registry_metadata(dr_uid, {collectory_value: value})
                log.info(f"Updated {collectory_value} for dr {dr_uid} to: {value}")
            except Exception as e:
                log.error(f"Error updating {collectory_value} for dr {dr_uid}: {e}")
                collectory_errors.append(dr_uid)

        def _item_str(item_list: list[str]) -> str:
            return '' if not item_list else (": " + ", ".join(item_list))

        def _dr_count_str(count: int) -> str:
            return f"{count} data resource{'s' if count > 1 else ''}"

        total_buckets = distinct_items["numBuckets"]
        selected_buckets = total_buckets if not filter_uid_list else len(filter_uid_list)
        total_failed = len(solr_errors) + len(collectory_errors)

        log.info(f"Found {_dr_count_str(total_buckets)} in solr with '{solr_facet}'")
        if filter_uid_list:
            log.info(f"Update attempted on subset of {_dr_count_str(len(filter_uid_list))}")
        log.info(f"Succecssfully updated '{collectory_value}' for {_dr_count_str(selected_buckets - total_failed)} in collectory")
        log.info(f"Failed to update {_dr_count_str(len(solr_errors))} with no solr value{_item_str(solr_errors)}")
        log.info(f"Failed to update {_dr_count_str(len(collectory_errors))} with collectory issue{_item_str(collectory_errors)}")

    update_data_currency_values = PythonOperator(
        task_id="update_date_currency_dates",
        python_callable=update_dates_from_solr,
        op_args=["{{ params.datasetIds }}"]
    )
