import requests
import os
from absl import flags

FLAGS = flags.FLAGS


class YandexEndpointsError(Exception):
    """Error for problems with gathering endpoints"""


class YandexIAMTokenError(Exception):
    """Error for problems with Yandex IAM token"""


if FLAGS.cloud == "Yandex":
    token = os.getenv("YANDEX_IAM")
    if not token:
        raise YandexIAMTokenError("Please provide Yandex IAM token via \'YANDEX_IAM\' environmental variable")

    endpoint_response = requests.get("https://api.cloud.yandex.net/endpoints")
    if not endpoint_response.ok:
        raise YandexEndpointsError(
            f"HTTP status code: {endpoint_response.status_code}, Text: {endpoint_response.text}")
    else:
        ENDPOINTS = {endpoint["id"]: endpoint["address"] for endpoint in endpoint_response.json()["endpoints"]}

FLAGS = flags.FLAGS

CIDR_BLOCK = "192.168.100.0/24"
SUBNET_NAME = NETWORK_NAME = "test"

auth_header = {"Authorization": f"Bearer {os.getenv('YANDEX_IAM')}"}


class YandexFolderError(Exception):
    """Error for creating Folder for VM"""


class YandexSubnetError(Exception):
    pass


def GetOrCreateSubnet(folder_id):
    subnets = requests.get(f"https://{ENDPOINTS['vpc']}/vpc/v1/subnets?folderId={folder_id}", headers=auth_header)
    if not subnets.ok:
        raise YandexSubnetError(f"Status code: {subnets.status_code}, Text: {subnets.text}")
    subnets_json = subnets.json()
    if subnets_json:
        return subnets_json["subnets"][0]["id"]
    network_id = GetOrCreateNetwork(folder_id)
    return CreateSubnet(folder_id, network_id)


def CreateSubnet(folder_id, network_id):
    body = {
        "folderId": folder_id,
        "name": SUBNET_NAME,
        "networkId": network_id,
        "zoneId": FLAGS.yandex_zone,
        "v4CidrBlocks": [CIDR_BLOCK]
    }
    resp = requests.post(f"https://{ENDPOINTS['vpc']}/vpc/v1/subnets", json=body, headers=auth_header)
    if not resp.ok:
        raise YandexSubnetError(f"Status code: {resp.status_code}, Text: {resp.text}")
    resp_json = resp.json()
    return resp_json["response"]["id"]


def GetOrCreateNetwork(folder_id):
    networks = requests.get(
        f"https://{ENDPOINTS['vpc']}/vpc/v1/networks?folderId={folder_id}", headers=auth_header)
    if not networks.ok:
        YandexSubnetError(f"Status code: {networks.status_code}, Text: {networks.text}")
    networks_json = networks.json()
    if networks_json:
        return networks_json["networks"][0]["id"]
    return CreateNetwork(folder_id)


def CreateNetwork(folder_id):
    body = {
        "name": NETWORK_NAME,
        "folderId": folder_id
    }
    resp = requests.post(f"https://{ENDPOINTS['vpc']}/vpc/v1/networks", json=body, headers=auth_header)
    if not resp.ok:
        raise YandexSubnetError(f"Status code: {resp.status_code}, Text: {resp.text}")
    json_resp = resp.json()
    return json_resp["response"]["id"]


def GetFolderID(name):
    folders_request = requests.get(
        f"https://{ENDPOINTS['resource-manager']}/resource-manager/v1/folders?cloudId={FLAGS.yandex_cloud_id}",
        headers=auth_header)
    if not folders_request.ok:
        raise YandexFolderError(f"Status code: {folders_request.status_code}, Text: {folders_request.text}")
    next_page = True
    folders = folders_request.json()
    while next_page:
        for folder in folders["folders"]:
            if folder["name"] == name:
                return folder["id"]
        if "nextPageToken" in folders.keys() and folders["nextPageToken"]:
            folders_request = requests.get(
                f"https://{ENDPOINTS['resource-manager']}/resource-manager/v1/folders?pageToken={folders['nextPageToken']}",
                headers=auth_header)
            if not folders_request.ok:
                raise YandexFolderError(f"Status code: {folders_request.status_code}, Text: {folders_request.text}")
            folders = folders_request.json()
            next_page = True
        else:
            next_page = False
    # here we assume that folder doesn't exist
    return _CreateFolder(name)


def _CreateFolder(name):
    folder_body = {
        "cloudId": FLAGS.yandex_cloud_id,
        "name": name
    }
    folder_response = requests.post(
        f"https://{ENDPOINTS['resource-manager']}/resource-manager/v1/folders", folder_body, headers=auth_header)
    if not folder_response.ok:
        raise YandexFolderError(f"Status code: {folder_response.status_code}, Text: {folder_response.text}")
    folder = folder_response.json()
    return folder["resource"]["id"]
