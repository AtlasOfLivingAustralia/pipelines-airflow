import logging as log
from datetime import timedelta
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args, update_registry_metadata
from ala import ala_config

@dag(
    dag_id="Update-Data-Currency",
    description="Update data currency field of all datasets in solr",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "all-datasets"],
)
def update_data_currency_values(datasetIDs: str = ""):
    """
    Updates dataCurrency field in collectory from the max lastLoadDate value for the respective data resource in solr  
    Passing no datasetIDs gets every available data resource in solr and updates collectory
    """

    solr_facet = "lastLoadDate"
    collectory_value = "dataCurrency"

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def get_solr_load_dates(datasetIDs: str) -> dict[str, str]:

        def sanitise_input() -> list[str]:
            if not isinstance(datasetIDs, str): # Airflow passes empty string as None
                return []

            filter_list = datasetIDs.split()
            if not filter_list: # datasetIDs provided as empty string
                return []

            bad_uids = []
            for uid in filter_list:
                if not uid.startswith("dr"):
                    log.warning(f"Bad data resource provided: {uid}")
                    bad_uids.append(uid)

            if bad_uids == filter_list:
                raise AirflowException(f"No valid data resources provided")

            for uid in bad_uids:
                log.warning(f"Dropping bad provided data resource: {uid}")
                filter_list.remove(uid)

            return filter_list

        filter_uid_list = sanitise_input()

        solr_bucket_name = "distinct_items"
        url = f"{ala_config.SOLR_URL}/biocache/select"
        headers = {
            "User-Agent": "dataCurrencyDAG"
        }

        params = {
            "query": "*:*",
            "limit": 0,
            "facet": {
                solr_bucket_name: {
                    "type": "terms",
                    "field": "dataResourceUid",
                    "limit": -1,
                    "numBuckets": True,
                    "facet": {solr_facet: f"max({solr_facet})"}
                }
            }
        }

        log.info(f"Querying solr at {url}")
        response = requests.post(url, headers=headers, json=params, timeout=60)

        try:
            payload = response.json()
        except ValueError as e:
            raise AirflowException(f"Non-JSON response from solr ({response.status_code}): {response.text}") from e

        if response.status_code != 200:
            raise AirflowException(f"Got error from solr({response.status_code} - {response.reason}): {payload['error']['msg']}")

        payload = payload["facets"][solr_bucket_name] # Overwrite initial payload with just the returned bucket
        log.info(f"Found {payload['numBuckets']} data resources in solr with facet '{solr_facet}'")

        # Get required updates from payload
        updates = {}
        for bucket in payload["buckets"]:
            dr_uid = bucket["val"]

            if filter_uid_list and (dr_uid not in filter_uid_list):
                continue

            updates[dr_uid] = bucket.get(solr_facet, "")

        # Put None in place of updates that aren't in solr
        for dr_uid in filter_uid_list:
            if dr_uid not in updates:
                updates[dr_uid] = None

        log.info(f"There are {len(updates)} data resources to update")
        return updates

    @task
    def update_collectory_data_currency(updates: dict) -> None:
        solr_errors = []
        value_errors = []
        collectory_errors = []

        for dr_uid, value in updates.items():
            if value is None: # dr_uid not returned from solr
                log.error(f"Error updating {dr_uid}, data resource not found in solr")
                solr_errors.append(dr_uid)
                continue

            if not value: # Value from solr was empty
                log.warning(f"Skipping {collectory_value} update for {dr_uid}, solr value is empty")
                value_errors.append(dr_uid)
                continue

            try:
                update_registry_metadata(dr_uid, {collectory_value: value})
                log.info(f"Updated '{collectory_value}' for {dr_uid}: {value}")
            except Exception as e:
                log.error(f"Error updating {collectory_value} for {dr_uid}: {e}")
                collectory_errors.append(dr_uid)

        # Log update activity
        def _item_str(item_list: list[str]) -> str:
            return '' if not item_list else (": " + ", ".join(item_list))

        failed = sum(len(errors) for errors in [solr_errors, value_errors, collectory_errors])

        log.info(f"Successfully updated '{collectory_value}' for {len(updates) - failed} data resource(s) in collectory")
        log.info(f"Failed to update {len(solr_errors)} data resource(s) as dr doesn't exist in solr{_item_str(solr_errors)}")
        log.info(f"Failed to update {len(value_errors)} data resource(s) with no solr value{_item_str(value_errors)}")
        log.info(f"Failed to update {len(collectory_errors)} data resource(s) due to collectory issue{_item_str(collectory_errors)}")

    updates = get_solr_load_dates(datasetIDs)
    update_collectory_data_currency(updates)

update_data_currency_values()
