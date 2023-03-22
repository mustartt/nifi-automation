import argparse
import time
from typing import Optional, Tuple

from common import NifiError
from nifi_client import NifiClient
from registry_client import RegistryClient


def update_process_group(client: NifiClient, pgid: str, version: int, timeout=30) -> str:
    details = client.get_process_group_details(pgid)
    print(f"Upgrading process group: {details['id']}",
          f"current version {details['component']['versionControlInformation']['version']}",
          f"desired version {version}")

    revision_info = details["revision"]
    current_version = details["component"]["versionControlInformation"]
    current_version["version"] = version
    update_request = client.create_version_change_request(pgid, revision=revision_info, version_info=current_version)

    start_time = time.monotonic()
    print(f"Upgrade Progress: Starting")
    request_status = client.get_update_request_status(update_request["request"]["requestId"])

    while not request_status["request"]["complete"]:
        progress = request_status["request"]["percentCompleted"]
        print(f"Upgrade Progress: {progress}%")
        current_time = time.monotonic()
        if current_time - start_time > timeout:
            break
        time.sleep(0.05)
        request_status = client.get_update_request_status(update_request["request"]["requestId"])

    if not request_status["request"]["complete"]:
        raise NifiError("Change Version: Upgrade timed out " + request_status)

    print(f"Upgrade Progress: Done")
    return pgid


def import_process_group(client: NifiClient,
                         name: str,
                         bucket_id: str, flow_id: str, version: int,
                         parent_pgid: Optional[str] = None, registry_client: Optional[str] = None,
                         position: Optional[Tuple[int, int]] = None) -> str:
    if not registry_client:
        registries = client.get_registry_clients()["registries"]
        if len(registries) != 1:
            raise NifiError("Import Process Group: must provide registry client if clients are not unique.")
        registry_client = registries[0]["id"]

    if not parent_pgid:
        parent_pgid = client.get_process_group()["processGroupFlow"]["id"]

    if not position:
        position = client.get_suggested_process_group_position(parent_pgid)

    print(f"Importing process group: {name}",
          f"desired version {version}",
          f"from bucket {bucket_id}",
          f"with flow {flow_id}",
          f"from registry id {registry_client}",
          f"with parent processor group {parent_pgid}")

    details = client.create_process_group(
        parent_pgid=parent_pgid,
        registry_id=registry_client,
        bucket_id=bucket_id,
        flow_id=flow_id,
        version=version,
        pos_x=position[0],
        pos_y=position[1]
    )

    print("Import Progress: Done")

    updated_process_group = client.change_process_group_name(details["id"], name, details["revision"])
    return updated_process_group["id"]


def traverse_process_groups(client: NifiClient, root: str):
    curr = [root]
    while curr:
        pgid = curr.pop(0)
        current_group = client.get_process_group(pgid)
        yield current_group
        for pg in current_group["processGroupFlow"]["flow"]["processGroups"]:
            curr.append(pg["id"])


def do_execute(nifi: NifiClient, registry: RegistryClient, args: dict):
    # traverse processor group tree
    target_pg_name = args["<process group name>"]
    target_pg_version = args["<version>"]
    root_id = nifi.get_process_group()["processGroupFlow"]["id"]

    target_group = next((
        pg for pg in traverse_process_groups(nifi, root_id)
        if pg["processGroupFlow"]["breadcrumb"]["breadcrumb"]["name"] == target_pg_name
    ), None)
    if not target_group:
        # importing new process group
        if not args["bucket"] or not args["flow"]:
            raise NifiError("Importing Process Group needs --bucket and --flow defined")
        buckets = registry.get_buckets()
        bucket = next((b for b in buckets if b["name"] == args["bucket"]), None)
        if not bucket:
            raise NifiError("Importing Process Group: bucket " + args["bucket"] + " does not exist")

        flows = registry.get_flows(bucket["identifier"])
        flow = next((f for f in flows if f["name"] == args["flow"]), None)
        if not flow:
            raise NifiError("Importing Process Group: flow " + args["flow"] + " does not exist")

        target_registry = None
        if args["registry"]:
            client = next((registry for registry in nifi.get_registry_clients()["registries"]
                           if registry["component"]["name"] == args["registry"]), None)
            if not client:
                raise NifiError("Registry Client " + args["registry"] + " does not exist")
            target_registry = client["id"]

        parent_pgid = None
        if args["parent"]:
            target_parent_group = next((
                pg for pg in traverse_process_groups(nifi, root_id)
                if pg["processGroupFlow"]["breadcrumb"]["breadcrumb"]["name"] == args["parent"]
            ), None)
            if not target_parent_group:
                raise NifiError("Parent Processor Group " + args["parent"] + " does not exists")
            parent_pgid = target_parent_group["processGroupFlow"]["id"]

        import_process_group(nifi, name=target_pg_name, version=target_pg_version,
                             bucket_id=bucket["identifier"], flow_id=flow["identifier"],
                             parent_pgid=parent_pgid,
                             registry_client=target_registry)
    else:
        # upgrading existing process group
        update_process_group(nifi, target_group["processGroupFlow"]["id"], target_pg_version)


# upgrade existing
#  - process group name
#  - version
# does not exists
#  - process group name
#  - version
#  - bucket name
#  - flow name
#  - parent process group name: optional
#  - registry name: optional
def main():
    parser = argparse.ArgumentParser(description="CLI for import/upgrade Nifi Process Group")
    parser.add_argument("<process group name>", type=str, help="Nifi process group name (must be unique)")
    parser.add_argument("<version>", type=int, help="Nifi registry version to upgrade to")

    parser.add_argument("--nifi-api", type=str, dest="nifi-api", required=True,
                        help="Nifi API Endpoint (https://nifi:8443/nifi-api)")
    parser.add_argument("--registry-api", type=str, dest="registry-api", required=True,
                        help="Nifi Registry API Endpoint (https://nifi:18080/nifi-registry-api)")

    parser.add_argument("--username", dest="username", required=False, type=str)
    parser.add_argument("--password", dest="password", required=False, type=str)

    parser.add_argument("--insecure-nifi", dest="nifi-insecure", required=False, action="store_true",
                        help="allow use of anonymous access")
    parser.add_argument("--insecure-registry", dest="registry-insecure", required=False, action="store_true",
                        help="allow use of anonymous access")

    parser.add_argument("--no-verify-tls", dest="no-verify-tls", required=False, action="store_true",
                        help="disable verify tls")

    parser.add_argument("-b", "--bucket", type=str, dest="bucket", required=False,
                        help="Registry Bucket name (supplied if process group does not exist and unique)")
    parser.add_argument("-f", "--flow", type=str, dest="flow", required=False,
                        help="Registry Flow Name (supplied if process group does not exist and unique)")
    parser.add_argument("-p", "--parent", type=str, dest="parent", required=False,
                        help="Parent process group name to import into")
    parser.add_argument("-r", "--registry", type=str, dest="registry", required=False,
                        help="Registry client (supplied if there are more than one registry client)")

    args = vars(parser.parse_args([
        "nested 123456", "2",
        "--nifi-api", "https://localhost:8443/nifi-api",
        "--registry-api", "http://localhost:18080/nifi-registry-api",
        "--no-verify-tls", "--insecure-registry",
        "--username", "admin", "--password", "supersecret1",
        "--bucket", "docker local test", "--flow", "test flow",
        "--registry", "docker local",
        "--parent", "test group"
    ]))
    print(args)

    if not args["nifi-insecure"] or not args["registry-insecure"]:
        has_cred = args["username"] and args["password"]
        if not has_cred:
            raise NifiError("Username and password must be provided if --insecure")

    nifi = NifiClient(args["nifi-api"], not args["no-verify-tls"], args["nifi-insecure"])
    if not args["nifi-insecure"]:
        nifi.authenticate(args["username"], args["password"])

    registry = RegistryClient(args["registry-api"], not args["no-verify-tls"], args["registry-insecure"])
    if not args["registry-insecure"]:
        registry.authenticate(args["username"], args["password"])

    do_execute(nifi, registry, args)


if __name__ == '__main__':
    main()
