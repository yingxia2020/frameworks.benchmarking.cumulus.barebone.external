from time import sleep

import requests
from absl import flags

from perfkitbenchmarker import disk
from perfkitbenchmarker.providers.yandex.util import GetFolderID, GetOrCreateSubnet, auth_header, ENDPOINTS

FLAGS = flags.FLAGS


class YandexDiskError(Exception):
    """Error for problems with disk"""


class YandexDisk(disk.BaseDisk):
    """Object representing a Yandex Disk."""

    def __init__(self, disk_spec, vm):
        super(YandexDisk, self).__init__(disk_spec)

        self.id = None
        self.folder_id = GetFolderID(FLAGS.yandex_folder_name)
        self.vm = vm
        self.zone = FLAGS.yandex_zone
        self.subnet_id = GetOrCreateSubnet(self.folder_id)

    def _Create(self):
        """Creates the disk."""
        disk_body = {"folderId": self.folder_id,
                     "typeId": self.disk_type,
                     "zoneId": self.zone,
                     "size": self.disk_size * 2 ** 30

                     }
        create_response = requests.post(f"https://{ENDPOINTS['compute']}/compute/v1/disks", json=disk_body,
                                        headers=auth_header)
        if not create_response.ok:
            raise YandexDiskError(f"HTTP status code: {create_response.status_code}, Text: {create_response.text}")
        create_json = create_response.json()
        self.id = create_json["metadata"]["diskId"]
        while not self._CheckStatus():
            sleep(1)

    def _Delete(self):
        """Deletes the disk."""
        delete_response = requests.delete(f"https://{ENDPOINTS['compute']}/compute/v1/disks/{self.id}",
                                          headers=auth_header)
        if not delete_response.ok:
            raise YandexDiskError(f"HTTP status code: {delete_response.status_code}. Text: {delete_response.text}")

    def _Exists(self):
        """Returns true if the disk exists."""
        return self._CheckStatus()

    def _CheckStatus(self):
        response = requests.get(f"https://{ENDPOINTS['compute']}/compute/v1/disks/{self.id}", headers=auth_header)
        if response.status_code not in [200, 404]:
            raise YandexDiskError(f"HTTP status: {response.status_code}. Text: {response.text}")
        else:
            if response.status_code == 404:
                return False
            json_response = response.json()
            return json_response["status"] == "READY"

    def Attach(self, vm):
        """Attaches the disk to a VM.

    Args:
      vm: The Yandex instance to which the disk will be attached.
    """
        body = {
            "attachedDiskSpec": {
                "autoDelete": False,
                "deviceName": self.id,
                "diskId": self.id
            }
        }
        attach_response = requests.post(
            f"https://{ENDPOINTS['compute']}/compute/v1/instances/{vm.id}:attachDisk", json=body, headers=auth_header)
        if not attach_response.ok:
            raise YandexDiskError(f"HTTP status code: {attach_response.status_code}. Text: {attach_response.text}")

    def Detach(self):
        """Detaches the disk from a VM."""
        body = {"diskId": self.id}
        response = requests.post(
            f"https://{ENDPOINTS['compute']}/compute/v1/instances/{self.vm.id}:detachDisk", json=body,
            headers=auth_header)
        if not response.ok:
            raise YandexDiskError(f"HTTP status code: {response.status_code}. Text: {response.text}")

    def GetDevicePath(self):
        """Returns the path to the device inside the VM."""
        if not self.device_path:
            self._GetPathFromRemoteHost()
        return self.device_path

    def _GetPathFromRemoteHost(self):
        """Waits until VM is has booted."""
        readlink_cmd = f"readlink -e /dev/disk/by-id/virtio-{self.id}"
        resp, _ = self.vm.RemoteHostCommand(readlink_cmd, suppress_warning=True)
        self.device_path = resp[:-1]
