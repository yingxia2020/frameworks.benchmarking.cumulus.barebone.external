import json
import logging
from os import path
import posixpath
import tempfile
from time import sleep

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import intel_s3_transfer
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string("intel_internal_registry_key", "amr-registry",
                    "Keyword of Intel internal docker registry name. "
                    "Use it to decide if docker image is internal or not.")
flags.DEFINE_boolean('intel_docker_pre_install', False,
                     'Whether to install docker and Intel CA on host machine. '
                     'Usually this step only needs to be run once on host. '
                     'For CI/CD pipeline always set this flag to False.')
flags.DEFINE_boolean('intel_docker_remove_image', False,
                     'Whether to remove docker image after copy transfer')
flags.DEFINE_boolean('intel_docker_disable_s3_cache', False,
                     'Whether to disable docker images AWS s3 cache')

INTEL_CAAS = "amr-registry"
INTERNAL_DIR = "internal_resources_docker_install"
INSTALL_URL = "https://cumulus.s3.us-east-2.amazonaws.com/docker_images/docker_ca_install.sh"
HTTP_PROXY = "http://proxy-chain.intel.com:911"
HTTPS_PROXY = "http://proxy-chain.intel.com:912"
TMP_TAR = "tmp.tar"
DAEMON_FILE = "daemon.json"
S3_PREFIX = "s3://"

# Records to keep track of docker image names and their corresponding docker file locations
# Assume we always get images from cumulus s3 repository:
# https://cumulus.s3.us-east-2.amazonaws.com/
DOCKER_REPOS = {
    'amr-registry.caas.intel.com/cumulus/dlrs/aixprt:v1.0':
    'docker_images/intel_dlrs/aixprt.tar.gz',
}

# Service framework related docker images will follow the same pattern
# "amr-registry.caas.intel.com/sf-cwr/aa:v21.47": "sf-cwr/aa-v21.47.tar"
SF_REPOS = ("amr-registry.caas.intel.com/sf-cwr", "amr-registry-pre.caas.intel.com/sf-cwr-test")


def _SupportedSFRepo(image_name):
  for repo in SF_REPOS:
    if repo in image_name:
      return True
  return False


# If docker image is Intel internal image
def IsInternalImage(image):
  return FLAGS.intel_internal_registry_key in image


# Install pre-requirements on control machine, only run one time
def PreInstall():
  """
  Install docker, set proxy and install CA certificate on control machine
  """
  internal_dir = vm_util.PrependTempDir(INTERNAL_DIR)
  vm_util.IssueCommand("mkdir -p {0}".format(internal_dir).split())
  install_file = path.basename(INSTALL_URL)
  curl_dest_path = posixpath.join(internal_dir, install_file)
  vm_util.IssueCommand("curl -o {0} {1}".format(curl_dest_path, INSTALL_URL).split(),
                       timeout=60)
  vm_util.IssueCommand("chmod +x {}".format(curl_dest_path).split())
  http_proxy = HTTP_PROXY
  if FLAGS["http_proxy"].present:
    http_proxy = FLAGS.http_proxy
  https_proxy = HTTPS_PROXY
  if FLAGS["https_proxy"].present:
    https_proxy = FLAGS.https_proxy
  vm_util.IssueCommand("sudo {0} {1} {2}".format(curl_dest_path, http_proxy, https_proxy).split(),
                       force_info_log=True,
                       suppress_warning=True,
                       raise_on_failure=False)


def SafeInstallDocker(vm):
  """
  Install docker only if not already installed
  """
  if _IsDockerNotInstalled(vm):
    logging.info("Install docker")
    vm.Install('docker_ce')
  else:
    logging.info("Docker found. Skip install")


# For docker images hosted on Intel internal registry and could not be accessed by cloud VMs directly
def PullDockerImages(vms, images):
  """
  Pull docker images and copy to remote machines
  """
  # Check input image names
  for image in images:
    if not IsInternalImage(image):
      raise errors.Benchmarks.PrepareException(
          'Only Docker images hosted on Intel internal registry are supported!')

  if FLAGS.intel_docker_pre_install:
    PreInstall()

  for image in images:
    if not FLAGS.intel_docker_disable_s3_cache:
      if image in DOCKER_REPOS:
        GetDockerImages(vms, image)
      elif _SupportedSFRepo(image):
        try:
          GetDockerImages(vms, image)
        except RuntimeError:
          _TransferDockerImage(vms, image)
      else:
        _TransferDockerImage(vms, image)
    else:
      _TransferDockerImage(vms, image)


# For docker images hosted on cumulus CAAS and tar files stored with cumulus S3
def GetDockerImages(vms, images):
  """
  Download docker images and copy to remote machines
  """
  if isinstance(images, list):
    for image in images:
      _GetDockerImage(vms, image)
  elif isinstance(images, str):
    _GetDockerImage(vms, images)
  else:
    raise errors.Benchmarks.PrepareException(
        'Docker image name need to be string or list of string')


def _PullImage(image):
  """
  Pull docker image from internal registry with 3 times retries
  """
  retry = 3
  success = False
  pull_cmd = "sudo docker pull {0}".format(image).split()
  logging.info("Pulling docker image {} from registry".format(image))
  while retry > 0:
    _, _, ret_code = vm_util.IssueCommand(pull_cmd, timeout=None)
    if ret_code != 0:
      retry -= 1
      sleep(5)
    else:
      success = True
      break
  return success


def _TransferDockerImage(vms, image):
  """
  Transfer docker images to remote machines
  """
  # Pull docker image from registry
  if not _PullImage(image):
    raise errors.Benchmarks.PrepareException('Failed to pull image {} from registry'.format(image))

  internal_dir = tempfile.mkdtemp()
  temp_tar_path = posixpath.join(internal_dir, TMP_TAR)
  vm_util.IssueCommand("sudo docker save --output {0} {1}".format(temp_tar_path, image).split(), timeout=None)

  # Copy tar file to remote machine
  vm_util.IssueCommand("sudo chmod 666 {}".format(temp_tar_path).split())

  if isinstance(vms, list):
    vm_util.RunThreaded(
        lambda vm: _CopyDockerImage(vm, temp_tar_path), vms)
  else:
    _CopyDockerImage(vms, temp_tar_path)

  # Remove temporary file
  vm_util.IssueCommand("sudo rm -rf {0}".format(internal_dir).split())

  # Remove the docker image
  if FLAGS.intel_docker_remove_image:
    vm_util.IssueCommand("sudo docker image rm -f {0}".format(image).split())


def _CopyDockerImage(vm, temp_tar_path):
  """
  Copy and load docker image to remote machines
  """
  vm.RemoteCopy(temp_tar_path, INSTALL_DIR)
  # Install docker on remote machine if not installed
  SafeInstallDocker(vm)

  # Load docker image on remote machine and remove temp file
  vm.RemoteCommand("cd {0} && sudo docker load < {1}".format(
      INSTALL_DIR, TMP_TAR))
  vm.RemoteCommand("cd {0} && sudo rm -f {1}".format(INSTALL_DIR, TMP_TAR))


def _GetDockerImage(vms, image):
  """
  Download one docker image and copy to remote machines
  """
  dockerimage_url, dockerimage_tar = _CheckDockerImageName(image)

  if isinstance(vms, list):
    vm_util.RunThreaded(
        lambda vm: _LoadDockerImage(vm, dockerimage_url, dockerimage_tar), vms)
  else:
    _LoadDockerImage(vms, dockerimage_url, dockerimage_tar)


def _GetS3URLFromImage(image):
  # amr-registry.caas.intel.com/sf-cwr/aa:v21.47 -> sf-cwr/aa-v21.47.tar
  # get s3 name from image name
  tokens = image.strip().split("/")
  if len(tokens) < 3:
    raise errors.Benchmarks.PrepareException(f"Invalid image name: {image}")
  s3_name = tokens[-2]

  base_name = path.basename(image)
  tokens = base_name.strip().split(":")
  return path.join(s3_name, f"{tokens[0]}-{tokens[1]}.tar")


def _CheckDockerImageName(image):
  """
  Check if docker image name is a valid one
  """
  if not image:
    raise errors.Benchmarks.PrepareException(
        'The docker image name could not be empty!')

  if image in DOCKER_REPOS:
    dockerimage_url = DOCKER_REPOS[image]
    dockerimage_tar = path.basename(dockerimage_url)
    return dockerimage_url, dockerimage_tar
  elif _SupportedSFRepo(image):
    dockerimage_url = _GetS3URLFromImage(image)
    dockerimage_tar = path.basename(dockerimage_url)
    return dockerimage_url, dockerimage_tar
  else:
    raise errors.Benchmarks.PrepareException(
        'The docker image %s is not supported by PKB!', image)


def _LoadDockerImage(vm, dockerimage_url, dockerimage_tar):
  """
  Copy docker image to remote machine and remove temporary files
  """
  # Make sure docker is installed
  SafeInstallDocker(vm)

  target = posixpath.join(INSTALL_DIR, dockerimage_tar)
  if dockerimage_url.startswith(S3_PREFIX):
    intel_s3_transfer.GetFileFromS3WithUriString(vm, dockerimage_url, target, True)
  else:
    intel_s3_transfer.GetFileFromS3(vm, dockerimage_url, target, True)

  vm.RemoteCommand("cd {0} && sudo docker load < {1}".format(
      INSTALL_DIR, dockerimage_tar))
  vm.RemoteCommand("cd {0} && sudo rm -f {1}".format(INSTALL_DIR, dockerimage_tar))


def _IsDockerNotInstalled(vm):
  """
  Checks whether docker is installed on the VM.
  """
  resp, _ = vm.RemoteCommand('command -v docker',
                             ignore_failure=True,
                             suppress_warning=True)
  return resp.rstrip() == ""


def _CreateConfigFile(registry_url):
  internal_dir = vm_util.PrependTempDir(INTERNAL_DIR)
  vm_util.IssueCommand("mkdir -p {0}".format(internal_dir).split())

  daemon = {
      'insecure-registries': [registry_url]
  }
  source_dir = posixpath.join(internal_dir, DAEMON_FILE)
  with open(source_dir, 'w') as outfile:
    json.dump(daemon, outfile)

  return source_dir


def _EnableHTTPRegistry(vm, source_dir):
  # Make sure docker is installed
  SafeInstallDocker(vm)

  dest_dir = posixpath.join(INSTALL_DIR, DAEMON_FILE)
  vm.RemoteCopy(source_dir, dest_dir)
  vm.RemoteCommand(f"sudo mv {dest_dir} /etc/docker")

  # Restart docker for changes to take effects
  vm.RemoteCommand("sudo service docker restart")


def InstallPrivateRegistry(registry_vm, worker_vms, images, registry_port="5050"):
  """
  Install and enable Docker private registry on CSP VMs as well as load Docker images
  """
  resp, _ = registry_vm.RemoteCommand("hostname -f")
  registry_name = resp.strip()
  registry_url = registry_name + ":" + registry_port

  # Install Docker and enable HTTP registry on all CSP VMs
  source_dir = _CreateConfigFile(registry_url)
  vms = [registry_vm]
  vms.extend(worker_vms)
  vm_util.RunThreaded(
      lambda vm_item: _EnableHTTPRegistry(vm_item, source_dir), vms)

  # Only need get all images to the VM where the Docker private registry is set up
  PullDockerImages(registry_vm, images)

  # Start the private registry
  cmd = (
      f"sudo docker run -d --restart=always -e REGISTRY_HTTP_ADDR=0.0.0.0:{registry_port}"
      f" -p {registry_port}:{registry_port} --name registry registry:2"
  )
  _, _, ret_code = registry_vm.RemoteCommandWithReturnCode(cmd)
  if ret_code != 0:
    raise errors.Benchmarks.PrepareException('Failed to start Docker private registry!')

  # Tag and push the images to private registry
  for image in images:
    base_name = posixpath.basename(image)

    # Tag the docker image
    cmd = f"sudo docker tag {image} {registry_url}/{base_name}"
    registry_vm.RemoteCommand(cmd)

    # Push the docker image to private registry
    cmd = f"sudo docker push {registry_url}/{base_name}"
    _, _, ret_code = registry_vm.RemoteCommandWithReturnCode(cmd)
    if ret_code != 0:
      raise errors.Benchmarks.PrepareException(
          'Failed to push Docker image %s to private registry!', image)

  # Return the registry URL to user so they could update docker image names
  return registry_url
