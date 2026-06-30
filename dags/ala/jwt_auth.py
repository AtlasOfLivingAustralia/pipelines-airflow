import json
import logging
import time
import jwt
import requests

from ala import ala_config, ala_helper


class Authenticator:

    def __init__(self, token_url, client_id, client_secret, scope) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

    def get_token(self):
        print(f'Authencticating with {self.token_url}')
        response = ala_helper.http_request_with_retry(
            "POST",
            self.token_url,
            data={"grant_type": "client_credentials", "scope": self.scope},
            auth=(self.client_id, self.client_secret),
            max_retries=5,
        )
        response_text = response.json()
        if "access_token" in response_text:
            print(f"Access token is acquired successfully from {self.token_url}, client id {self.client_id} ")
            return response_text["access_token"]
