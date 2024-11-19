import json
from datetime import timedelta
import jwt
import time
import requests
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
import logging

from ala import ala_config, ala_helper
from ala import jwt_auth as ja

DAG_ID = 'Assertions-Sync'


@dag(dag_id=DAG_ID,
     description="Update Solr Collection Alias",
     default_args=ala_helper.get_default_args(),
     start_date=days_ago(1),
     dagrun_timeout=timedelta(hours=1),
     schedule_interval=None,
     tags=['emr', 'preingestion'],
     params={},
     catchup=False
     )
def taskflow():
    @task
    def authenticate():   
        auth = ja.Authenticator(ala_config.AUTH_TOKEN_URL, ala_config.AUTH_CLIENT_ID, ala_config.AUTH_CLIENT_SECRET, ala_config.AUTH_SCOPE)
        return auth.get_token()

    @task
    def call_api(jwt_token):

        headers = {'user-agent': 'token-refresh/0.1.1', 'Authorization': f'Bearer {jwt_token}'}
        
        def check_status(sync_url):
            MAX_COUNT = 30
            SLEEP_TIME = 30
            cnt = 1
            while cnt <=    MAX_COUNT:
                time.sleep(SLEEP_TIME)
                r = requests.get(f'{sync_url}/status', headers=headers)
                print(f"{cnt}: Sync Status API called successfully and here is status code: {r.status_code}, and text:{r.text}")
                if r.text == 'No task is running':
                    break
                cnt += 1
            if cnt == MAX_COUNT:
                print(f"Sync task is not completed in {cnt*SLEEP_TIME/60:.2f} minutes. Please check the status manually.")
                raise SystemExit("Sync task is not completed in 15 minutes. Please check the status manually.")
            else:
                print(f"Sync task is completed successfully in {cnt*SLEEP_TIME/60:.2f} minutes.")
            
        
        try:
            sync_url = ala_helper.join_url(ala_config.BIOCACHE_URL, 'sync')
            r = requests.get(sync_url, headers=headers)
            r.raise_for_status()
            print(f"Sync API called successfully and here is status code: {r.status_code}, and text:{r.text}")
            check_status(sync_url)
            
        except requests.exceptions.HTTPError as err:
            print("Error encountered during request ", err)
            raise SystemExit(err)

    call_api(authenticate())


dag = taskflow()
