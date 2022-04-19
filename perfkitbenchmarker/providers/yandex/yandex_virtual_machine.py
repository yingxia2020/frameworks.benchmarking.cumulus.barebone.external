from time import sleep

import requests

from absl import flags
from perfkitbenchmarker import virtual_machine, providers, linux_virtual_machine
from perfkitbenchmarker.providers.yandex.util import GetOrCreateSubnet, GetFolderID, auth_header, ENDPOINTS
from perfkitbenchmarker.providers.yandex.yandex_disk import YandexDisk

FLAGS = flags.FLAGS

DEFAULT_USERNAME = "pkb"


class YandexTooSmallDiskError(Exception):
    """Error for disk required for image smaller than requested disk"""


class YandexCreateInstanceError(Exception):
    """Error for creating VM"""


class YandexDeleteInstanceError(Exception):
    """Error for deleteing VM"""


class YandexCheckStatusError(Exception):
    """Error for creating VM"""


class YandexVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing an Yandex Virtual Machine."""

    IMAGE_NAME_FILTER = None
    CLOUD = providers.YANDEX

    def __init__(self, vm_spec):
        """Initialize a Yandex virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the VM.
    """
        super(YandexVirtualMachine, self).__init__(vm_spec)
        self.boot_disk_size = FLAGS.yandex_boot_disk_size
        self.user_name = DEFAULT_USERNAME
        self.folder_id = GetFolderID(FLAGS.yandex_folder_name)
        self.image_id = self._GetLatestImage(self.IMAGE_NAME_MATCH)
        self.zone = FLAGS.yandex_zone

        self.subnet_id = GetOrCreateSubnet(self.folder_id)

    def _GetLatestImage(self, name):
        image_request = requests.get(f"https://{ENDPOINTS['compute']}"
                                     f"/compute/v1/images:latestByFamily?folderId=standard-images&family={name}",
                                     headers=auth_header)
        if not image_request.ok:
            raise YandexCreateInstanceError(
                f"HTTP status code: {image_request.status_code}. Text: {image_request.text}")
        image = image_request.json()
        if int(image["minDiskSize"]) > self.boot_disk_size * 2 ** 30:
            raise YandexTooSmallDiskError(f"Disk size should be at least {image['minDiskSize']} bytes. "
                                          f"Actual: {self.boot_disk_size * 2 ** 30} bytes")
        return image["id"]

    def _Create(self):
        """Create a VM instance."""
        with open(self.ssh_public_key) as f:
            public_key = f.readline()
        compute_spec = {"folderId": self.folder_id,
                        "name": self.name,
                        "zoneId": self.zone,
                        "platformId": FLAGS.yandex_platform,
                        "resourcesSpec": {
                            # Yandex Cloud api expects memory in bytes, so we convert GB into bytes
                            "memory": FLAGS.yandex_memory * 2 ** 30,
                            "cores": FLAGS.yandex_cores
                        },
                        "metadata": {
                            "user-data": f"#cloud-config\nusers:\n  - name: {self.user_name}\n    groups: sudo\n    shell: /bin/bash\n"
                                         "    sudo: ['ALL=(ALL) NOPASSWD:ALL']\n    ssh-authorized-keys:\n"
                                         f"      -  {public_key}",
                        },
                        "bootDiskSpec": {
                            "diskSpec": {
                                # Yandex Cloud api expects size in bytes, so we convert GB into bytes
                                "size": self.boot_disk_size * 2 ** 30,
                                "imageId": self.image_id
                            }
                        },
                        "networkInterfaceSpecs": [{
                            "subnetId": self.subnet_id,
                            "primaryV4AddressSpec": {
                                "oneToOneNatSpec": {
                                    "ipVersion": "IPV4"
                                }
                            }
                        }]
                        }
        response = requests.post(
            f"https://{ENDPOINTS['compute']}/compute/v1/instances", json=compute_spec, headers=auth_header)
        if not response.ok:
            raise YandexCreateInstanceError(f"HTTP status code {response.status_code}. Text: {response.text}")
        response = response.json()
        self.id = response["metadata"]["instanceId"]
        self._WaitForInstance()

    def CreateScratchDisk(self, disk_spec):
        yandex_disk = YandexDisk(disk_spec, self)
        disks = [yandex_disk]
        self._CreateScratchDiskFromDisks(disk_spec, disks)

    def _Delete(self):
        """Delete a VM instance."""
        compute_response = requests.get(f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.id}",
                                        headers=auth_header)
        if not compute_response.ok:
            raise YandexDeleteInstanceError(
                f"HTTP status code: {compute_response.status_code}. Text: {compute_response.text}")
        compute_json = compute_response.json()
        disk_ids = [disk["diskId"] for disk in compute_json["secondaryDisks"]] \
            if hasattr(compute_json, "secondaryDisks") else []
        disk_ids.append(compute_json["bootDisk"]["diskId"])
        response = requests.delete(
            f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.id}", headers=auth_header)

        if not response.ok:
            raise YandexDeleteInstanceError(f"HTTP status code: {response.status_code}. Text: {response.text}")

        while True:
            resp = requests.get(f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.id}", headers=auth_header)
            if resp.status_code == 404:
                break
            if resp.status_code not in [200, 404]:
                raise YandexDeleteInstanceError(f"HTTP status code: {response.status_code}. Text: {response.text}")
            sleep(1)

        for disk in disk_ids:
            response = requests.delete(f"https://{ENDPOINTS['compute']}/compute/v1/disks/{disk}", headers=auth_header)
            if not response.ok:
                raise YandexDeleteInstanceError(f"HTTP status code: {response.status_code}. Text: {response.text}")

    def _WaitForInstance(self):
        while True:
            resp = requests.get(f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.id}", headers=auth_header)
            if not resp.ok:
                raise YandexDeleteInstanceError(f"HTTP status code: {resp.status_code}. Text: {resp.text}")
            json_resp = resp.json()
            if json_resp["status"] == "RUNNING":
                break
            sleep(1)
        self.internal_ip = json_resp["networkInterfaces"][0]["primaryV4Address"]["address"]
        self.ip_address = json_resp["networkInterfaces"][0]["primaryV4Address"]["oneToOneNat"]["address"]

    def _Exists(self):
        """Returns whether the VM exists."""
        response = requests.get(f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.id}", headers=auth_header)
        if response.status_code not in [200, 404]:
            raise YandexCheckStatusError(f"HTTP status: {response.status_code}, Error: {response.text}")
        else:
            return response.status_code == 200


class CentOs7BasedYandexVirtualMachine(YandexVirtualMachine,
                                       linux_virtual_machine.CentOs8Mixin):
    IMAGE_NAME_MATCH = 'centos-7'

    def __init__(self, vm_spec):
        super(CentOs7BasedYandexVirtualMachine, self).__init__(vm_spec)


class CentOs8BasedYandexVirtualMachine(YandexVirtualMachine,
                                       linux_virtual_machine.CentOs7Mixin):
    IMAGE_NAME_MATCH = 'centos-8'

    def __init__(self, vm_spec):
        super(CentOs8BasedYandexVirtualMachine, self).__init__(vm_spec)


class Debian9BasedYandexVirtualMachine(YandexVirtualMachine,
                                       linux_virtual_machine.Debian9Mixin):
    IMAGE_NAME_MATCH = 'debian-9'


class Debian10BasedYandexVirtualMachine(YandexVirtualMachine,
                                        linux_virtual_machine.Debian10Mixin):
    IMAGE_NAME_MATCH = 'debian-10'


class Ubuntu1604BasedYandexVirtualMachine(YandexVirtualMachine,
                                          linux_virtual_machine.Ubuntu1604Mixin):
    IMAGE_NAME_MATCH = 'ubuntu-1604-lts'


class Ubuntu1804BasedYandexVirtualMachine(YandexVirtualMachine,
                                          linux_virtual_machine.Ubuntu1804Mixin):
    IMAGE_NAME_MATCH = 'ubuntu-1804-lts'


class CoreOsBasedAwsVirtualMachine(YandexVirtualMachine,
                                   linux_virtual_machine.CoreOsMixin):
    IMAGE_NAME_MATCH = 'coreos'


class Ubuntu2004BasedYandexVirtualMachine(YandexVirtualMachine,
                                          linux_virtual_machine.Ubuntu2004Mixin):
    IMAGE_NAME_MATCH = 'ubuntu-2004-lts'
