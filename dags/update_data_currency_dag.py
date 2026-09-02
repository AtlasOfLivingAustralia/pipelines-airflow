import logging as log
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args, update_registry_metadata
from ala import ala_config

def validate_response_payload(response: requests.Response) -> dict:
    try:
        payload = response.json()
    except ValueError as e:
        raise AirflowException(f"Non-JSON response from solr ({response.status_code}): {response.text}") from e

    if response.status_code != 200:
        raise AirflowException(f"Got error from solr({response.status_code} - {response.reason}): {payload['error']['msg']}")

    return payload

@dag(
    dag_id="Update-Data-Currency",
    description="Update data currency field of all datasets in solr",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=8),
    schedule_interval=None,
    tags=["emr", "all-datasets"],
)
def update_data_currency_values(datasetIDs: str = "", days_before_collection: int = 1, force_update: bool = False):
    """
    Updates dataCurrency field in collectory from the max lastLoadDate value for the respective data resource in solr  
    Passing no datasetIDs gets every available data resource in solr and updates collectory  
    Parameter days_before_collection is a delta to set back solr colletion date by when checking for new updates  
    Forcing update will overwrite this check completely and allow all values to be updated
    """

    # Data values
    solr_facet = "lastLoadDate"
    collectory_value = "dataCurrency"

    # Request properties
    solr_alias = "biocache"
    request_headers = {
        "User-Agent": "dataCurrencyDAG"
    }
    request_timeout = 60

    @task
    def get_solr_reference_date(days_before_collection: int, force_update: bool) -> str | None:
        if force_update:
            log.info("Force update selected, skipping last solr collection date check")
            return None
        
        url = f"{ala_config.SOLR_URL}/admin/collections"
        params = {
            "action": "LISTALIASES"
        }

        log.info(f"Getting solr date from {url}")
        response = requests.get(url, headers=request_headers, params=params, timeout=request_timeout)
        payload = validate_response_payload(response)

        collection_name = payload["aliases"][solr_alias]
        collection_timestamp = datetime.fromisoformat(collection_name.split("-", 1)[-1])
        log.info(f"Got solr collection date: {collection_timestamp}")
        collection_timestamp -= timedelta(days=days_before_collection)
        log.info(f"Using solr collection date rolled back by {days_before_collection} day(s) as update reference point")
        return collection_timestamp.isoformat()

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def get_solr_load_dates(datasetIDs: str, solr_reference_date: str | None) -> dict[str, str]:

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
        url = f"{ala_config.SOLR_URL}/{solr_alias}/select"
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
        response = requests.post(url, headers=request_headers, json=params, timeout=request_timeout)
        payload = validate_response_payload(response)

        payload = payload["facets"][solr_bucket_name] # Overwrite initial payload with just the returned bucket
        log.info(f"Found {payload['numBuckets']} data resources in solr with facet '{solr_facet}'")

        # Get required updates from payload
        updates = {}
        for bucket in payload["buckets"]:
            dr_uid = bucket["val"]

            if filter_uid_list and (dr_uid not in filter_uid_list):
                continue

            value = bucket.get(solr_facet, "")
            if (
                not value # Keep empty values to report errors later
                or not solr_reference_date # Keep all values if no ref date
                or datetime.fromisoformat(value) > datetime.fromisoformat(solr_reference_date) # Keep valid values
            ):
                updates[dr_uid] = value

        # Put None in place of updates that aren't in solr to report errors later
        for dr_uid in filter_uid_list:
            if dr_uid not in updates:
                updates[dr_uid] = None

        info_message = f"There are {len(updates)} data resources to update"
        if filter_uid_list:
            info_message += f" after applying data resource filter {filter_uid_list}"
        if solr_reference_date:
            info_message += f" with a date after {solr_reference_date}"

        log.info(info_message)
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

    solr_ref_date = get_solr_reference_date(days_before_collection, force_update)
    updates = get_solr_load_dates(datasetIDs, solr_ref_date)
    update_collectory_data_currency(updates)

update_data_currency_values()
