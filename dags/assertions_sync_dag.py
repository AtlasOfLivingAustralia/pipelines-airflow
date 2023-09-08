import json
from datetime import timedelta
import jwt
import time
import requests
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
import logging

from ala import ala_config, ala_helper

DAG_ID = 'Assertions-Sync'


class Authenticator:

    def __init__(self, token_url, client_id, client_secret) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_obj = {}
        self.__read_token()

    # read the JSON file and save to global token_obj
    def __read_token(self):
        self.token_obj = json.loads(ala_config.AUTH_TOKEN)
        logging.info(f'Token read successfully.')

    def get_token(self):
        decoded = jwt.decode(self.token_obj["access_token"], algorithms='RS256',
                             options={"verify_signature": False}, verify=False)
        # re-generate token when expired.
        if decoded["exp"] < int(time.time()):
            # regenerate token and update token_obj
            logging.info("Token expired. Refreshing token...")
            self.regenerate_token()
        return self.token_obj["access_token"]

    # regenerate token, return new token and update token_obj
    def regenerate_token(self):
        payload = {'refresh_token': self.token_obj["refresh_token"], 'grant_type': 'refresh_token',
                   'scope': self.token_obj["scope"]}
        # refreshing token
        logging.info(f'Sending request to {self.token_url} to read new tokens.')
        r = requests.post(self.token_url, data=payload,
                          auth=(self.client_id, self.client_secret))
        if r.ok:
            data = r.json()

            # update token_obj with the new token data
            self.token_obj |= data
            print("Token refreshed")
        else:
            print("Unable to refresh access token. ", r.status_code)


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
        auth = Authenticator(ala_config.AUTH_TOKEN_URL, ala_config.AUTH_CLIENT_ID, ala_config.AUTH_CLIENT_SECRET)
        return auth.get_token()

    @task
    def call_api(jwt_token):
        headers = {'user-agent': 'token-refresh/0.1.1', 'Authorization': f'Bearer {jwt_token}'}
        try:
            r = requests.get(ala_config.BIOCACHE_URL + '/sync', headers=headers)
            r.raise_for_status()
            print(f"API called successfully and here is status code: {r.status_code}, and text:{r.text}")
        except requests.exceptions.HTTPError as err:
            print("Error encountered during request ", err)
            raise SystemExit(err)

    call_api(authenticate())


dag = taskflow()
