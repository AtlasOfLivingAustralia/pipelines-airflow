import json
from datetime import timedelta
import jwt
import time
import requests
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import logging

from ala import ala_config, ala_helper
from ala import jwt_auth as ja

DAG_ID = "Assertions-Sync"


@dag(
    dag_id=DAG_ID,
    description="Update Solr Collection Alias",
    default_args=ala_helper.get_default_args(),
    start_date=days_ago(1),
    dagrun_timeout=timedelta(hours=1),
    schedule_interval=None,
    tags=["emr", "preingestion"],
    params={"pre_sync_count": 0},
    catchup=False,
)
def taskflow():
    @task
    def authenticate():
        auth = ja.Authenticator(
            ala_config.AUTH_TOKEN_URL, ala_config.AUTH_CLIENT_ID, ala_config.AUTH_CLIENT_SECRET, ala_config.AUTH_SCOPE
        )
        return auth.get_token()

    @task
    def call_api(jwt_token):

        headers = {"user-agent": "token-refresh/0.1.1", "Authorization": f"Bearer {jwt_token}"}
        context = get_current_context()
        pre_sync_count = int(context["params"]["pre_sync_count"])

        def check_status(sync_url):
            MAX_COUNT = 30
            SLEEP_TIME = 30
            cnt = 1
            while cnt <= MAX_COUNT:
                time.sleep(SLEEP_TIME)
                assertions_status = requests.get(f"{sync_url}/status", headers=headers)
                records_with_assertions_count = ala_helper.get_assertion_records_count()
                print(
                    f"{cnt}: Sync Status API called successfully and here is status code: {assertions_status.status_code}, text:{assertions_status.text}, number of records with assertions:{records_with_assertions_count}"
                )
                if assertions_status.text == "No task is running":
                    break
                cnt += 1
            if cnt == MAX_COUNT:
                print(
                    f"Sync task is not completed in {cnt*SLEEP_TIME/60:.2f} minutes, number of records with assertions:{records_with_assertions_count}."
                )
                raise SystemExit(
                    f"Sync task is not completed in 15 minutes. Number of records with assertions: {records_with_assertions_count}. Please check the status manually."
                )
            else:
                records_with_assertions_count = ala_helper.get_assertion_records_count()
                print(
                    f"Sync task is completed successfully in {cnt*SLEEP_TIME/60:.2f} minutes. Number of records with assertions: {records_with_assertions_count}."
                )
                if pre_sync_count > 0:
                    if records_with_assertions_count < pre_sync_count:
                        raise SystemExit(
                            f"Number of records with assertions in the new index is less than the previous count. Previous count: {pre_sync_count}, New count: {records_with_assertions_count}, Diff: {records_with_assertions_count - pre_sync_count}."
                        )
                    print(
                        f"There are {records_with_assertions_count - pre_sync_count} more records with assertions in the new index."
                    )

        try:
            sync_url = ala_helper.join_url(ala_config.BIOCACHE_URL, "sync")
            r = requests.get(sync_url, headers=headers)
            r.raise_for_status()
            print(f"Sync API called successfully and here is status code: {r.status_code}")
            check_status(sync_url)

        except requests.exceptions.HTTPError as err:
            print("Error encountered during request ", err)
            raise SystemExit(err)

    call_api(authenticate()) >> ala_helper.get_success_notification_operator()


dag = taskflow()
