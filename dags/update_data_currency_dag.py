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
    params={}
) as dag:
    
    def update_dates_from_solr() -> None:
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

            value = bucket.get(solr_facet, "")
            if not value:
                log.warning(f"Skipping {collectory_value} update for dr {dr_uid}, solr value is empty")
                solr_errors.append(dr_uid)
                continue

            try:
                # update_registry_metadata(dr_uid, {collectory_value: value})
                log.info(f"Updated {collectory_value} for dr {dr_uid} to: {value}")
            except Exception as e:
                log.error(f"Error updating {collectory_value} for dr {dr_uid}: {e}")
                collectory_errors.append(dr_uid)

        def _print_errors(error_uids: list[str]) -> str:
            return '' if not error_uids else (": " + ", ".join(error_uids))

        total_buckets = distinct_items["numBuckets"]
        total_failed = len(solr_errors) + len(collectory_errors)
        log.info(f"Total data resources with '{solr_facet}' in solr: {total_buckets}")
        log.info(f"Succecssfully updated '{collectory_value}' for {total_buckets - total_failed} data resources in collectory")
        log.info(f"Failed to update {len(solr_errors)} data resoures with no solr value{_print_errors(solr_errors)}")
        log.info(f"Failed to update {len(collectory_errors)} data resources with collectory issue{_print_errors(collectory_errors)}")

    update_data_currency_values = PythonOperator(
        task_id="update_date_currency_dates",
        python_callable=update_dates_from_solr
    )
