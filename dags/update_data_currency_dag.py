import logging as log
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from ala.ala_helper import get_default_args, PersistentRegistryUpdater
from ala import ala_config
import boto3
from zoneinfo import ZoneInfo

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
def update_data_currency_values(datasetIDs: str = "", force_update: bool = False):
    """
    Updates dataCurrency field in collectory from the max lastLoadDate value for the respective data resource in solr  
    Value is only updated if last change was within a set amount of days before last solr update  
    
    Passing no datasetIDs gets every available data resource in solr and updates collectory  
    Forcing update will overwrite the date check for last solr update and force all values to be updated
    """

    # Data values
    s3_bucket = ala_config.S3_BUCKET_DWCA
    s3_folder = "dwca-imports/"
    collectory_value = "dataCurrency"

    # Request properties
    base_url = ala_config.SOLR_URL
    request_headers = {
        "User-Agent": "dataCurrencyDAG"
    }
    request_timeout = 60

    @task
    def get_solr_reference_date(force_update: bool) -> str | None:
        if force_update:
            log.info("Force update selected, skipping last solr collection date check")
            return None
        
        url = f"{base_url}/admin/collections"
        params = {
            "action": "LIST"
        }

        log.info(f"Getting solr date from {url}")
        response = requests.get(url, headers=request_headers, params=params, timeout=request_timeout)
        payload = validate_response_payload(response)

        collections = [collection for collection in payload.get("collections", []) if collection.startswith("biocache-")]
        if not collections:
            AirflowException(f"No collections found in solr")

        oldest_collection = sorted(collections)[0] # Sorted by date, oldest first (sorted ascending order)
        log.info(f"Using oldest collection available as reference date: {oldest_collection}")

        collection_name = oldest_collection.split("-", 1)[-1] # Remove 'biocache' from start of name
        collection_timestamp = datetime.strptime(collection_name, "%Y-%m-%d-%H-%M")
        log.info(f"Got solr collection date: {collection_timestamp}")

        days_before_collection = ala_config.CURRENCY_UPDATE_DAYS_PRIOR
        collection_timestamp -= timedelta(days=days_before_collection)
        log.info(f"Using solr collection date rolled back by {days_before_collection} day(s) as update reference point")

        return collection_timestamp.isoformat()

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def get_s3_upload_dates(datasetIDs: str, solr_reference_date: str) -> dict[str, str]:

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
        reference_timestamp = datetime.fromisoformat(solr_reference_date) if solr_reference_date else None 

        log.info(f"Getting all dwca-import files from s3://{s3_bucket}/{s3_folder}")
        s3 = boto3.client("s3")
        paginator = s3.get_paginator('list_objects_v2')
        iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_folder)

        updates = {}
        for page in iterator:
            for object in page.get("Contents", []):
                key = object["Key"]
                _, dr_uid, filename = key.split("/", 2)

                if filename != f"{dr_uid}.zip": # Only keep used file
                    continue

                timestamp = object["LastModified"]
                local_timestamp = timestamp.astimezone(ZoneInfo("Australia/Sydney"))

                if reference_timestamp is None or local_timestamp >= reference_timestamp:
                    updates[dr_uid] = local_timestamp

        info_message = f"There are {len(updates)} data resources to update"
        if filter_uid_list:
            info_message += f" after applying data resource filter {filter_uid_list}"
        if reference_timestamp is not None:
            info_message += f" with a date after {reference_timestamp}"

        log.info(info_message)
        return updates

    @task
    def update_collectory_data_currency(updates: dict) -> None:
        pru = PersistentRegistryUpdater() 

        collectory_errors = []
        for dr_uid, value in updates.items():
            try:
                pru.update_registry_metadata(dr_uid, {collectory_value: value})
                log.info(f"Updated '{collectory_value}' for {dr_uid}: {value}")
            except Exception as e:
                log.error(f"Error updating {collectory_value} for {dr_uid}: {e}")
                collectory_errors.append(dr_uid)

        # Log update activity
        def _item_str(item_list: list[str]) -> str:
            return '' if not item_list else (": " + ", ".join(item_list))

        success_amount = len(updates) - len(collectory_errors)
        if success_amount:
            log.info(f"Successfully updated '{collectory_value}' for {success_amount} data resource(s) in collectory")
        
        if collectory_errors:
            log.info(f"Failed to update {len(collectory_errors)} data resource(s) due to collectory issue{_item_str(collectory_errors)}")

        if not success_amount and len(updates):
            raise AirflowException("All tasks failed to update due to some collectory error")

    solr_ref_date = get_solr_reference_date(force_update)
    updates = get_s3_upload_dates(datasetIDs, solr_ref_date)
    update_collectory_data_currency(updates)

update_data_currency_values()
