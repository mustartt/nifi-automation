import functools

import requests
from urllib3.exceptions import InsecureRequestWarning

from common import NifiError

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class NifiClient:
    token = None

    def __init__(self, api_uri: str, verify_tls: bool = True, insecure: bool = False):
        self.api_uri = api_uri
        self.verify_tls = verify_tls
        self.insecure = insecure

    def require_auth(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            if not self.insecure and not self.token:
                raise NifiError("NifiClient: Method " + func.__qualname__ + " requires authentication to use")
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
    def get_process_group_details(self, pgid: str):
        response = requests.get(
            self.api_uri + "/process-groups/" + pgid,
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Process Group: " + response.text)

        return response.json()

    @require_auth
    def get_process_group(self, pgid: str = "root"):
        response = requests.get(
            self.api_uri + "/flow/process-groups/" + pgid,
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Process Group: " + response.text)

        return response.json()

    @require_auth
    def get_suggested_process_group_position(self, parent_pgid: str):
        response = requests.get(
            self.api_uri + "/flow/process-groups/" + parent_pgid,
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Suggested Group Position: " + response.text)

        components = []
        flows = response.json()["processGroupFlow"]["flow"]
        components += flows["processGroups"]
        components += flows["remoteProcessGroups"]
        components += flows["processors"]
        components += flows["inputPorts"]
        components += flows["outputPorts"]
        components += flows["connections"]
        components += flows["labels"]
        components += flows["funnels"]
        if not components:
            return 0, 0

        # suggest next to the top-right corner of bounding box
        right_x = max(comp["position"]["x"] for comp in components)
        top_y = min(comp["position"]["y"] for comp in components)

        return right_x + 380 + 50, top_y

    @require_auth
    def create_version_change_request(self, pgid: str, revision, version_info):
        data = {
            "processGroupRevision": revision,
            "disconnectedNodeAcknowledged": False,
            "versionControlInformation": version_info
        }
        response = requests.post(
            self.api_uri + "/versions/update-requests/process-groups/" + pgid,
            headers=self.get_auth_headers(),
            json=data,
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Change Version: " + response.text)

        return response.json()

    @require_auth
    def create_process_group(self, parent_pgid: str,
                             registry_id: str, bucket_id: str, flow_id: str, version: int,
                             pos_x: int, pos_y: int):
        pg_entity = {
            "component": {
                "position": {
                    "x": pos_x,
                    "y": pos_y
                },
                "versionControlInformation": {
                    "registryId": registry_id,
                    "bucketId": bucket_id,
                    "flowId": flow_id,
                    "version": version
                },
            },
            "revision": {
                "version": 0
            }
        }
        response = requests.post(
            self.api_uri + f"/process-groups/{parent_pgid}/process-groups",
            headers=self.get_auth_headers(),
            json=pg_entity,
            verify=self.verify_tls
        )

        if not response:
            raise NifiError("Create Process Group: " + response.text)

        return response.json()

    @require_auth
    def change_process_group_name(self, pgid: str, name: str, revision):
        pg_entity = {
            "component": {
                "name": name,
                "id": pgid
            },
            "revision": revision
        }
        response = requests.put(
            self.api_uri + "/process-groups/" + pgid,
            headers=self.get_auth_headers(),
            json=pg_entity,
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Change Process Group Name: " + response.text)

        return response.json()

    @require_auth
    def get_update_request_status(self, request_id: str):
        response = requests.get(
            self.api_uri + "/versions/update-requests/" + request_id,
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Update Request Status: " + response.text)

        return response.json()

    @require_auth
    def get_registry_clients(self):
        response = requests.get(
            self.api_uri + "/controller/registry-clients",
            headers=self.get_auth_headers(),
            verify=self.verify_tls
        )

        if not response.ok:
            raise NifiError("Get Registry Clients: " + response.text)

        return response.json()
