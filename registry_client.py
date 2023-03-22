import functools

import requests

from common import NifiError


class RegistryClient:
    token = None

    def __init__(self, api_uri: str, verify_tls: bool = True, insecure: bool = False):
        self.api_uri = api_uri
        self.verify_tls = verify_tls
        self.insecure = insecure

    def require_auth(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            if not self.insecure and not self.token:
                raise NifiError("RegistryClient: Method " + func.__qualname__ + " requires authentication to use")
            return func(self, *args, **kwargs)

        return wrap

    def get_auth_headers(self):
        if self.insecure:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def authenticate(self, username: str, password: str):
        response = requests.post(self.api_uri + "/access/token", headers={
            "Content-Type": "application/x-www-form-urlencoded"
        }, data={
            "username": username,
            "password": password
        }, verify=self.verify_tls)

        if not response.ok:
            raise NifiError("Authentication: " + response.text)

        self.token = response.text

    @require_auth
    def get_buckets(self):
        response = requests.get(
            self.api_uri + "/buckets",
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Buckets: " + response.text)

        return response.json()

    @require_auth
    def get_flows(self, bucket_id: str):
        response = requests.get(
            self.api_uri + "/buckets/" + bucket_id + "/flows",
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Flows: " + response.text)

        return response.json()
