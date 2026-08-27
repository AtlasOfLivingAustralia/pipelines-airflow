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
    
    def update_dates_from_solr():
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

        distinct_items = payload["facets"]["distinct_items"]
        log.info(f"Retrieved {distinct_items['numBuckets']} distinct items with facet: {solr_facet}")

        for bucket in distinct_items["buckets"]:
            dr_uid = bucket["val"]

            value = bucket.get(solr_facet, "")
            if not value:
                log.info(f"Skipping {collectory_value} update for dr {dr_uid}, solr value is empty")
                continue

            try:
                # update_registry_metadata(dr_uid, {collectory_value: value})
                log.info(f"Updated {collectory_value} for dr {dr_uid} to: {value}")
            except Exception as e:
                log.error(f"Error updating {collectory_value} for dr {dr_uid}: {e}")

    update_data_currency_values = PythonOperator(
        task_id="update_date_currency_dates",
        python_callable=update_dates_from_solr
    )
