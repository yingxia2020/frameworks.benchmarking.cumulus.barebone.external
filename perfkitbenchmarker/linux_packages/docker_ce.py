from absl import flags

FLAGS = flags.FLAGS


def YumInstall(vm):
  vm.InstallPackages("yum-utils device-mapper-persistent-data lvm2")
  # Package for RHEL8 containerd.io does not yet exist - this is a workaround
  if vm.OS_TYPE == "centos8" or vm.OS_TYPE == "rhel8":
    cmd = "sudo yum install -y https://download.docker.com/linux/centos/7/x86_64/stable/Packages/containerd.io-1.2.6-3.3.el7.x86_64.rpm"
    cmd += " && sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo"
  else:
    cmd = 'sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo'
  vm.RemoteCommand(cmd)
  vm.InstallPackages('docker-ce')
  _AddProxy(vm)
  _AddUserToDockerGroup(vm)


def AptInstall(vm):
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages("apt-transport-https ca-certificates curl gnupg-agent software-properties-common")
  cmds = [
      'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -',
      'sudo -E add-apt-repository "deb [arch=$(dpkg --print-architecture)] '
      'https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"',
      'sudo -E apt-get update',
      'sudo -E apt-get -y install docker-ce'
  ]
  vm.RemoteCommand(' && '.join(cmds))
  _AddProxy(vm)
  _AddUserToDockerGroup(vm)


def SwupdInstall(vm):
  vm.RemoteCommand("sudo swupd update")
  vm.InstallPackages("containers-basic")
  _AddProxy(vm)
  _AddUserToDockerGroup(vm)


def _AddProxy(vm):
  proxy_file = """
[Service]
Environment="%s"
"""
  vm.RemoteCommand("sudo mkdir -p /etc/systemd/system/docker.service.d")
  if FLAGS["http_proxy"].present:
    http_proxy_variable = "HTTP_PROXY=%s" % FLAGS.http_proxy
    vm.RemoteCommand("echo '%s' | sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf"
                     % (proxy_file % http_proxy_variable))
  if FLAGS["https_proxy"].present:
    https_proxy_variable = "HTTPS_PROXY=%s" % FLAGS.https_proxy
    vm.RemoteCommand("echo '%s' | sudo tee /etc/systemd/system/docker.service.d/https-proxy.conf"
                     % (proxy_file % https_proxy_variable))
  vm.RemoteCommand("sudo systemctl daemon-reload && sudo systemctl restart docker")


def _AddUserToDockerGroup(vm):
    """
    Add user to the docker group so docker commands can be executed without sudo
    """
    vm.RemoteCommand("sudo usermod --append --groups docker {}".format(vm.user_name))
    vm.RemoteCommand("sudo systemctl restart docker")

    # SSH uses multiplexing to reuse connections without going through the SSH handshake
    # for a remote host. Typically we need to logout / login after adding the user to
    # the docker group as group memberships are evaluated at login.
    # See: https://docs.docker.com/engine/install/linux-postinstall/
    # This requirement along with the multiplexing causes subsequent docker commands run in the
    # reused session to fail with "permission denied" errors.
    # This command will cause the ssh multiplexing for this particular VM to stop causing the next
    # SSH command to the VM to restart a multiplex session with ControlMaster=auto. This new session
    # will start with docker group membership and will be able to execute docker commands without root.
    vm.RemoteCommand('', ssh_args=['-O', 'stop'])
