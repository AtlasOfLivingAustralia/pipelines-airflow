import json
import logging
import time
import jwt
import requests

from ala import ala_config


class Authenticator:

    def __init__(self, token_url, client_id, client_secret, scope) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

    def get_token(self):
        print(f'Authencticating with {self.token_url}')
        response = requests.post(self.token_url,
                                 data={"grant_type": "client_credentials", "scope": self.scope},
                                 auth=(self.client_id, self.client_secret))
        response.raise_for_status()
        response_text = response.json()
        if "access_token" in response_text:
            print(f"Access token is acquired successfully from {self.token_url}, client id {self.client_id} ")
            return response_text["access_token"]

