import glob
import logging
import json
import os
import posixpath

import perfkitbenchmarker
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker import vm_util
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('installk8s_tarball', None,
                    'Path to installk8s package. eg --installk8s_tarball=/tmp/installk8scsp.tar.gz')
flags.DEFINE_string('installk8s_images_tarball', None,
                    'Path to installk8s docker images package. '
                    'eg --installk8s_images_tarball=/tmp/docker_images.2.17.1.tar.gz')
flags.DEFINE_boolean('intel_k8s_preload_images', False,
                     'Whether to preload k8s related docker images on cluster')
flags.DEFINE_boolean('intel_k8s_enable_nfd', False,
                     'Whether to enable node feature discovery')
flags.DEFINE_string('intel_k8s_nfd_version', '0.10.1',
                    'Specify the NFD version')
flags.DEFINE_enum('intel_k8s_kubespray_version', '2.17.1',
                  ['2.17.1', '2.16.0'],
                  'Specify the Kubespray version, [2.17.1, 2.16.0] are supported now')
flags.DEFINE_enum('intel_k8s_network_plugin', 'flannel',
                  ['cilium', 'calico', 'weave', 'flannel'],
                  'K8s network plugin types. 4 plugin types are supported now')
flags.DEFINE_string('intel_k8s_cni_mtu', None, 'CNI custom MTU value')
flags.DEFINE_boolean('intel_k8s_should_log', False,
                     'Whether to log the messages during k8s installations')
flags.DEFINE_list('intel_k8s_offline_csp_list', ['Tencent'],
                  'List of CSPs that need use offline options')

INSTALL_K8S_S3 = "https://cumulus.s3.us-east-2.amazonaws.com/install_k8s/"
INSTALLK8S_TAR = "installk8scsp"
DOCKER_IMAGES_TAR = "docker_images"
INSTALLK8S_DIR = "{0}/installk8scsp".format(INSTALL_DIR)
CONFIG_FILE = "cluster_config.json"
DIR_NAME = "internal_resources_installk8s"
NFD_CMD = "kubectl apply -k " \
          "https://github.com/kubernetes-sigs/node-feature-discovery/deployment/overlays/default?ref=v{} && " \
          "kubectl --namespace=node-feature-discovery wait pod --all --timeout=30s --for=condition=ready"
K8S_CLUSTER_YAML = "k8s-cluster.yml"
K8S_CLUSTER_YAML_DIR = "installk8scsp/kubespray/inventory/cumulus/group_vars/k8s_cluster"
K8S_ROLES_NETWORK_PLUGIN_DIR = "installk8scsp/kubespray/roles/network_plugin"
K8S_OFFLINE_FILE = "intel_k8s/offline.yml"
K8S_CLUSTER_OFFLINE_FILE = "installk8scsp/kubespray/inventory/cumulus/group_vars/all/offline.yml"
K8S_CNI_SETTING = "kube_network_plugin: "
FLANNEL_SETTING = "flannel"
CILIUM_SETTING = "cilium"
NFD_DATA_DIR = "nfd"
REMOTE_NFD_DIR = "/etc/kubernetes/node-feature-discovery/source.d"


def _UpdateNFDScriptsFiles(vm):
    _, _, ret_val = vm.RemoteCommandWithReturnCode('test -d {0}'.format(REMOTE_NFD_DIR),
                                                   ignore_failure=True)
    # If the remote directory does not exist, do nothing
    if ret_val != 0:
        return

    nfd_file_pattern = posixpath.join(os.path.dirname(os.path.abspath(perfkitbenchmarker.__file__)),
                                      f'data/{NFD_DATA_DIR}', '*.sh')
    nfd_files = glob.glob(nfd_file_pattern)
    if len(nfd_files) == 0:
        return

    for file in nfd_files:
        if not os.path.isfile(file):
            continue
        file_name = os.path.basename(file)
        remote_file = posixpath.join(INSTALL_DIR, file_name)
        vm.RemoteCopy(file, remote_file)
        vm.RemoteCommand(f'sudo mv {remote_file} {REMOTE_NFD_DIR}')
    # restart kubectl
    vm.RemoteCommand('sudo systemctl restart kubelet', ignore_failure=True)


def _UseOfflineSettings(vm):
    offline_remote_path = posixpath.join(INSTALL_DIR, K8S_CLUSTER_OFFLINE_FILE)
    offline_file_path = data.ResourcePath(K8S_OFFLINE_FILE)
    if not os.path.isfile(offline_file_path):
        logging.info('file ({}) does not exist'.format(offline_file_path))
        return None
    vm.RemoteCopy(offline_file_path, offline_remote_path)


def _GetTarfileByType(file_type):
    if FLAGS.intel_k8s_kubespray_version:
        return file_type + "." + FLAGS.intel_k8s_kubespray_version + ".tar.gz"
    else:
        return file_type + ".tar.gz"


def _GetAbsPath(path):
  absPath = os.path.abspath(os.path.expanduser(path))
  if not os.path.isfile(absPath):
    raise RuntimeError('File (%s) does not exist.' % path)

  return absPath


def _GetTarfileName():
    if FLAGS.installk8s_tarball:
        _, installk8s_tarball = os.path.split(FLAGS.installk8s_tarball)
        return installk8s_tarball
    else:
        return _GetTarfileByType(INSTALLK8S_TAR)


def _GetInstallPackage(vm, url):
  """
  Get k8s installation package
  """
  if FLAGS.installk8s_tarball:
    logging.info("Copying local installk8s tarball ({}) to remote SUT location ({})"
                 .format(FLAGS.installk8s_tarball, INSTALL_DIR))
    tarfile_path = _GetAbsPath(FLAGS.installk8s_tarball)
    vm.RemoteCopy(tarfile_path, INSTALL_DIR)
  elif url:
    internal_dir = vm_util.PrependTempDir(DIR_NAME)
    curl_dest_path = posixpath.join(internal_dir, _GetTarfileByType(INSTALLK8S_TAR))
    vm_util.IssueCommand(f"mkdir -p {internal_dir}".split())
    vm_util.IssueCommand(f"curl --retry 10 -o {curl_dest_path} {url}".split())
    vm.RemoteCopy(curl_dest_path, INSTALL_DIR)
  else:
    raise RuntimeError('Failed to get install k8s tarball')


def _LoadDockerImages(vm, curl_dest_path):
  vm.RemoteCopy(curl_dest_path, INSTALL_DIR)
  vm.RemoteCommand("cd {0} && tar xfz {1}".format(INSTALL_DIR, _GetTarfileByType(DOCKER_IMAGES_TAR)))
  vm.RemoteCommand("cd {0}/docker_images && ls -1 *.tar | xargs --no-run-if-empty -L 1 sudo docker load -i"
                   .format(INSTALL_DIR))
  vm.RemoteCommand("cd {0} && rm -rf docker_images*".format(INSTALL_DIR))


def _LoadK8SDockerImages(vms):
  url = INSTALL_K8S_S3 + _GetTarfileByType(DOCKER_IMAGES_TAR)
  internal_dir = vm_util.PrependTempDir(DIR_NAME)
  curl_dest_path = posixpath.join(internal_dir, _GetTarfileByType(DOCKER_IMAGES_TAR))
  if FLAGS.installk8s_images_tarball:
    curl_dest_path = _GetAbsPath(FLAGS.installk8s_images_tarball)
  else:
    vm_util.IssueCommand(f"mkdir -p {internal_dir}".split())
    vm_util.IssueCommand(f"curl --retry 10 -o {curl_dest_path} {url}".split())
  vm_util.RunThreaded(lambda vm: _LoadDockerImages(vm, curl_dest_path), vms)


def _UpdateCustomeMTU(vm):
  plugin_path = posixpath.join(INSTALL_DIR, K8S_ROLES_NETWORK_PLUGIN_DIR)
  if FLAGS.intel_k8s_network_plugin == CILIUM_SETTING:
    vm.RemoteCommand(
        f"cd {plugin_path} && echo {FLAGS.intel_k8s_network_plugin}_mtu: "
        f"\'\"{FLAGS.intel_k8s_cni_mtu}\"\' >> {FLAGS.intel_k8s_network_plugin}/defaults/main.yml")
  else:
    vm.RemoteCommand(
        f"cd {plugin_path} && echo {FLAGS.intel_k8s_network_plugin}_mtu: "
        f"{FLAGS.intel_k8s_cni_mtu} >> {FLAGS.intel_k8s_network_plugin}/defaults/main.yml")


def _InstallK8SCluster(vm):
  logging.info('Setup k8s cluster:')
  vm.RemoteCommand("cd {0} && sudo ./prepare-cluster.sh".format(INSTALLK8S_DIR),
                   should_log=FLAGS.intel_k8s_should_log)
  stdout, _ = vm.RemoteCommand("cd {0} && sudo ./create-cluster.sh".format(INSTALLK8S_DIR),
                               should_log=FLAGS.intel_k8s_should_log)

  # If first time failed, try one more time with log turned on
  if "successfully" not in stdout:
      vm.RemoteCommand("cd {0} && sudo ./prepare-cluster.sh".format(INSTALLK8S_DIR),
                       should_log=True)
      stdout, _ = vm.RemoteCommand("cd {0} && sudo ./create-cluster.sh".format(INSTALLK8S_DIR),
                                   should_log=True)
  return "successfully" in stdout


def InstallK8sCSP(controller_vm, worker_vms, taint_controller=True):
  """
  Install Kubernetes cluster on CSP VMs
  """
  all_ips = [controller_vm.internal_ip]
  for vm in worker_vms:
    all_ips.append(vm.internal_ip)
    vm.Install('aufs-tools')
    if FLAGS.intel_k8s_preload_images:
      vm.Install('docker_ce')

  vm = controller_vm
  vm.Install('docker_ce')

  sshkey = vm_util.GetPrivateKeyPath()
  vm.RemoteCopy(sshkey, "~/.ssh/id_rsa")

  res = INSTALL_K8S_S3 + _GetTarfileByType(INSTALLK8S_TAR)
  _GetInstallPackage(vm, res)
  vm.RemoteCommand("cd {0} && tar xfz {1}".format(INSTALL_DIR, _GetTarfileName()))

  # Update k8s CNI
  if FLAGS.intel_k8s_network_plugin != FLANNEL_SETTING:
    config_path = posixpath.join(INSTALL_DIR, K8S_CLUSTER_YAML_DIR)
    logging.info(config_path)
    orig_setting = K8S_CNI_SETTING + FLANNEL_SETTING
    mod_setting = K8S_CNI_SETTING + FLAGS.intel_k8s_network_plugin
    vm.RemoteCommand(f"cd {config_path} && sed -i 's/{orig_setting}/{mod_setting}/' {K8S_CLUSTER_YAML}")

  # Update custom mtu value, supported in Calico, Weave and Cilium only
  if FLAGS.intel_k8s_cni_mtu and FLAGS.intel_k8s_network_plugin != FLANNEL_SETTING:
    _UpdateCustomeMTU(vm)

  # Need extra steps to load k8s related docker images on k8s cluster nodes
  if FLAGS.intel_k8s_preload_images:
    all_vms = worker_vms.copy()
    all_vms.append(controller_vm)
    _LoadK8SDockerImages(list(set(all_vms)))

  # For Tencent cloud, need use extra offline settings for installations to succeed
  if FLAGS.cloud in FLAGS.intel_k8s_offline_csp_list:
    _UseOfflineSettings(controller_vm)

  # create config file
  cfg_data = {'nodes': []}
  for ip in all_ips:
    cfg_data['nodes'].append({
        'ip_address': ip,
        'hostname': ''
    })
  vm.RemoteCommand("cd {0} && echo '{1}' > {2}".format(
      INSTALLK8S_DIR, json.dumps(cfg_data, indent=4), CONFIG_FILE))

  if not _InstallK8SCluster(vm):
    raise errors.Benchmarks.PrepareException('Kubernetes cluster installation failed!')
  else:
    logging.info('Kubernetes cluster has been installed successfully!')
    if taint_controller:
      stdout, _ = vm.RemoteCommand("kubectl taint nodes node1 controller=true:NoSchedule")
      if "node/node1 tainted" not in stdout:
        raise errors.Benchmarks.PrepareException('Kubernetes controller node taint failed!')
    if FLAGS.intel_k8s_enable_nfd:
      vm.RemoteCommand(NFD_CMD.format(FLAGS.intel_k8s_nfd_version), ignore_failure=True)
      vm_util.RunThreaded(
          lambda worker_vm: _UpdateNFDScriptsFiles(worker_vm), list(set(worker_vms)))
