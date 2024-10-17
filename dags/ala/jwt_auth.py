import json
import logging
import time
import jwt
import requests

from ala import ala_config


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
        payload = {'refresh_token': self.token_obj["refresh_token"],
                   'grant_type': 'refresh_token',
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
