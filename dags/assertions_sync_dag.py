import json
from datetime import timedelta
import jwt
import time
import requests
from requests.compat import urljoin
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
        auth = ja.Authenticator(ala_config.AUTH_TOKEN_URL, ala_config.AUTH_CLIENT_ID, ala_config.AUTH_CLIENT_SECRET)
        return auth.get_token()

    @task
    def call_api(jwt_token):
        headers = {'user-agent': 'token-refresh/0.1.1', 'Authorization': f'Bearer {jwt_token}'}
        try:
            r = requests.get(urljoin(ala_config.BIOCACHE_URL, '/sync'), headers=headers)
            r.raise_for_status()
            print(f"API called successfully and here is status code: {r.status_code}, and text:{r.text}")
        except requests.exceptions.HTTPError as err:
            print("Error encountered during request ", err)
            raise SystemExit(err)

    call_api(authenticate())


dag = taskflow()
