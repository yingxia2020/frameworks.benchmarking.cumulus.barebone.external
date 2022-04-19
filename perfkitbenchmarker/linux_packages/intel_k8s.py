import logging
import json
import posixpath

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_boolean('intel_k8s_preload_images', False,
                     'Whether to preload k8s related docker images on cluster')
flags.DEFINE_boolean('intel_k8s_enable_nfd', False,
                     'Whether to enable node feature discovery')
flags.DEFINE_string('intel_k8s_nfd_version', '0.10.1',
                    'Specify the NFD version')
flags.DEFINE_enum('intel_k8s_kubespray_version', '2.17.1',
                  ['2.17.1', '2.16.0'],
                  'Specify the Kubespray version, [2.17.1, 2.16.0] are supported now')
flags.DEFINE_enum('intel_k8s_network_plugin', 'calico',
                  ['cilium', 'calico', 'weave', 'flannel'],
                  'K8s network plugin types. 4 plugin types are supported now')
flags.DEFINE_string('intel_k8s_cni_mtu', None, 'CNI custom MTU value')
flags.DEFINE_boolean('intel_k8s_should_log', False,
                     'Whether to log the messages during k8s installations')

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
K8S_CNI_SETTING = "kube_network_plugin: "
WEAVE_SETTING = "weave"
FLANNEL_SETTING = "flannel"
CILIUM_SETTING = "cilium"


def _GetTarfileByType(file_type):
    if FLAGS.intel_k8s_kubespray_version:
        return file_type + "." + FLAGS.intel_k8s_kubespray_version + ".tar.gz"
    else:
        return file_type + ".tar.gz"


def _GetInstallPackage(vm, url):
  """
  Download k8s installation package
  """
  if url:
    internal_dir = vm_util.PrependTempDir(DIR_NAME)
    vm_util.IssueCommand("mkdir -p {0}".format(internal_dir).split())
    curl_dest_path = posixpath.join(internal_dir, _GetTarfileByType(INSTALLK8S_TAR))
    vm_util.IssueCommand("curl -o {0} {1}".format(curl_dest_path, url).split(),
                         timeout=60)
    vm.RemoteCopy(curl_dest_path, INSTALL_DIR)


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
  vm_util.IssueCommand("curl -o {0} {1}".format(curl_dest_path, url).split(),
                       timeout=300)
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


def InstallK8sCSP(controller_vm, worker_vms, taint_controller=True):
  """
  Install Kubernetes cluster on CSP VMs
  """
  all_ips = [controller_vm.internal_ip]
  for vm in worker_vms:
    all_ips.append(vm.internal_ip)
    if vm.OS_TYPE == os_types.UBUNTU2004 or vm.OS_TYPE == os_types.UBUNTU1804:
      vm.RemoteCommand("sudo apt-get update")
    if FLAGS.intel_k8s_preload_images:
      vm.Install('docker_ce')

  vm = controller_vm
  vm.Install('docker_ce')

  sshkey = vm_util.GetPrivateKeyPath()
  vm.RemoteCopy(sshkey, "~/.ssh/id_rsa")

  res = INSTALL_K8S_S3 + _GetTarfileByType(INSTALLK8S_TAR)
  _GetInstallPackage(vm, res)
  vm.RemoteCommand("cd {0} && tar xfz {1}".format(INSTALL_DIR, _GetTarfileByType(INSTALLK8S_TAR)))

  # Update k8s CNI
  if FLAGS.intel_k8s_network_plugin != WEAVE_SETTING:
    config_path = posixpath.join(INSTALL_DIR, K8S_CLUSTER_YAML_DIR)
    logging.info(config_path)
    orig_setting = K8S_CNI_SETTING + WEAVE_SETTING
    mod_setting = K8S_CNI_SETTING + FLAGS.intel_k8s_network_plugin
    vm.RemoteCommand(f"cd {config_path} && sed -i 's/{orig_setting}/{mod_setting}/' {K8S_CLUSTER_YAML}")

  # Update custom mtu value, supported in Calico, Weave and Cilium only
  if FLAGS.intel_k8s_cni_mtu and FLAGS.intel_k8s_network_plugin != FLANNEL_SETTING:
    _UpdateCustomeMTU(vm)

  # Need extra steps to load k8s related docker images on k8s cluster nodes
  if FLAGS.intel_k8s_preload_images:
    worker_vms.append(controller_vm)
    _LoadK8SDockerImages(list(set(worker_vms)))

  # create config file
  cfg_data = {'nodes': []}
  for ip in all_ips:
    cfg_data['nodes'].append({
        'ip_address': ip,
        'hostname': ''
    })
  vm.RemoteCommand("cd {0} && echo '{1}' > {2}".format(
      INSTALLK8S_DIR, json.dumps(cfg_data, indent=4), CONFIG_FILE))

  logging.info('Setup k8s cluster:')
  vm.RemoteCommand("cd {0} && sudo ./prepare-cluster.sh".format(INSTALLK8S_DIR),
                   should_log=FLAGS.intel_k8s_should_log)
  stdout, _ = vm.RemoteCommand("cd {0} && sudo ./create-cluster.sh".format(INSTALLK8S_DIR),
                               should_log=FLAGS.intel_k8s_should_log)

  if "successfully" not in stdout:
    raise errors.Benchmarks.PrepareException('Kubernetes cluster installation failed!')
  else:
    logging.info('Kubernetes cluster has been installed successfully!')
    if taint_controller:
      stdout, _ = vm.RemoteCommand("kubectl taint nodes node1 controller=true:NoSchedule")
      if "node/node1 tainted" not in stdout:
        raise errors.Benchmarks.PrepareException('Kubernetes controller node taint failed!')
    if FLAGS.intel_k8s_enable_nfd:
      vm.RemoteCommand(NFD_CMD.format(FLAGS.intel_k8s_nfd_version), ignore_failure=True)
