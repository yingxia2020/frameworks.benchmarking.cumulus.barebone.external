from absl import flags
from perfkitbenchmarker import configs, events, stages, vm_util, sample
from perfkitbenchmarker.linux_packages.habana import RegisterWithDocker, RegisterWithContainerD, RegisterKubernetesPlugins
from perfkitbenchmarker.linux_packages import intel_k8s
from posixpath import join
from uuid import uuid4
from yaml import safe_load_all, dump_all
import logging
import time
import os

BENCHMARK_NAME = "docker_pt"
BENCHMARK_CONFIG = """
docker_pt:
  description: Docker Passthrough Benchmark
  vm_groups: {}
  flags:
    dpt_docker_image: ""
    dpt_docker_dataset: []
    dpt_docker_options: ""
    dpt_kubernetes_yaml: ""
    dpt_kubernetes_job: ""
    dpt_namespace: ""
    dpt_logs_dir: ""
    dpt_timeout: "300"
    dpt_name: ""
    dpt_script_args: ""
    dpt_cluster_yaml: ""
    dpt_registry_map: []
    dpt_trace_mode: []
    dpt_params: ""
    dpt_tunables: ""
    dpt_debug: []
    dpt_vm_groups: "worker"
    dpt_aptgetlock_workaround: True
"""

registries = {}
run_seq = 0
SUT_VM_CTR = "controller"
KUBERNETES_CONFIG = "kubernetes_config.yaml"
HUGEPAGE_NR = "/sys/kernel/mm/hugepages/hugepages-{}/nr_hugepages"
LOGS_TARFILE = "{}.tgz"
REMOVE_LOCK = (
    "for lockfile in /var/lib/dpkg/lock /var/lib/apt/lists/lock /var/cache/apt/archives/lock /var/lib/dpkg/lock-frontend; do "
    "sudo kill -9 $(sudo lsof $lockfile 2>/dev/null) 2>/dev/null; "
    "sudo rm -f $lockfile; "
    "done; "
    "sudo dpkg --configure -a")
ITERATION_DIR = "itr-{}"
KPISH = "kpi.sh"
SF_NS_LABEL = "cn-benchmarking.intel.com/sf_namespace=true"

FLAGS = flags.FLAGS
flags.DEFINE_string("dpt_name", "", "Benchmark name")
flags.DEFINE_list("dpt_script_args", [], "The KPI and setup script args")
flags.DEFINE_string("dpt_docker_image", "", "Docker image name")
flags.DEFINE_list("dpt_docker_dataset", [], "Docker dataset images")
flags.DEFINE_string("dpt_docker_options", "", "Docker run options")
flags.DEFINE_string("dpt_kubernetes_yaml", "", "Kubernetes run yaml file")
flags.DEFINE_string("dpt_kubernetes_job", "benchmark", "Benchmark job name")
flags.DEFINE_string("dpt_namespace", str(uuid4()), "namespace")
flags.DEFINE_string("dpt_logs_dir", "", "The logs directory")
flags.DEFINE_string("dpt_timeout", "300", "Execution timeout")
flags.DEFINE_string("dpt_cluster_yaml", "", "The cluster configuration file")
flags.DEFINE_string("dpt_params", "", "The workload configuration parameters")
flags.DEFINE_string("dpt_tunables", "", "The workload tunable configuration parameters")
flags.DEFINE_list("dpt_registry_map", [], "Replace the registries")
flags.DEFINE_list("dpt_vm_groups", ["worker"], "Define the mapping of cluster-config groups to vm_groups")
flags.DEFINE_boolean("dpt_aptgetlock_workaround", True, "Work around the apt-get lock")
flags.DEFINE_list("dpt_debug", [], "Set debug breakpoints")
flags.DEFINE_list("dpt_trace_mode", [], "Specify the trace mode triple")
flags.DEFINE_boolean("dpt_iwos_cloud", False, "Enable when IWOS running  in cloud")


def _SetBreakPoint(breakpoint):
  if breakpoint in FLAGS.dpt_debug:
    try:
      logging.info("Pause for debugging at %s", breakpoint)
      while not os.path.exists(join(vm_util.GetTempDir(), "Resume" + breakpoint)):
        time.sleep(5)
    except:
      pass
    logging.info("Resume after debugging %s", breakpoint)


def _FormatKPI(line):
  key, _, value = line.rpartition(":")
  key = key.strip()
  value = float(value.strip())
  if key.endswith(")"):
    key, _, unit = key.rpartition("(")
    unit = unit[0:-1].strip()
    key = key.strip()
  else:
    unit = "-"
  return key, value, unit


def _ParseKPI():
  cmd = "cd {} && ./{} {}".format(join(FLAGS.dpt_logs_dir, ITERATION_DIR.format(run_seq)), KPISH, " ".join(FLAGS.dpt_script_args))
  stdout, _, retcode = vm_util.IssueCommand(["sh", "-c", cmd])

  samples = []
  for line in stdout.split("\n"):
    try:
      k, v, u = _FormatKPI(line)
      if k.startswith("*"):
        samples.append(sample.Sample(k[1:], v, u, {"primary_sample": True}))
      elif not k.startswith("#"):
        samples.append(sample.Sample(k, v, u))
    except Exception:
      pass
  if len(samples) == 1:
    samples[0].metadata["primary_sample"] = True
  return samples


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  pass


def _ReplaceImage(image):
  for r1 in registries:
    if image.startswith(r1):
      return image.replace(r1, registries[r1])
  return image


def _WalkTo(node, name):
  try:
    if name in node:
      return node
    for item1 in node:
      node1 = _WalkTo(node[item1], name)
      if node1:
        return node1
  except Exception:
    pass
  return None


def _GetNodes(controller0):
  nodes = {}
  stdout, _ = controller0.RemoteCommand(
      "kubectl get nodes -o='custom-columns=name:.metadata.name,ip:.status.addresses[?(@.type==\"InternalIP\")].address' --no-headers")
  for line in stdout.split("\n"):
    fields = line.strip().split(" ")
    nodes[fields[-1]] = fields[0]
  return nodes


def _ParseClusterConfigs(vm_groups, nodes={}):
  vimages = []
  workers = []
  nidx = {}
  options = FLAGS.dpt_docker_options.split(' ')

  with open(FLAGS.dpt_cluster_yaml, "rt") as fd:
    for doc in safe_load_all(fd):
      if "cluster" in doc:
        for i, cluster1 in enumerate(doc["cluster"]):
          name = FLAGS.dpt_vm_groups[i % len(FLAGS.dpt_vm_groups)]
          if name not in nidx:
            nidx[name] = 0
          worker1 = {
              "vm": vm_groups[name][nidx[name] % len(vm_groups[name])],
              "labels": cluster1["labels"],
          }
          worker1["labels"]["VM_GROUP_" + name.upper()] = "required"
          internal_ip = worker1["vm"].internal_ip
          try:
            worker1["name"] = nodes[internal_ip]
          except Exception:
            worker1["name"] = internal_ip
          nidx[name] = nidx[name] + 1
          workers.append(worker1)
      if "vm" in doc:
        for vim1 in doc["vm"]:
          for vm10 in vm_groups[vim1["name"]]:
            vimages.append({
                "name": vim1["name"],
                "vm": vm10,
                "env": vim1["env"],
            })
            if "setup" in vim1:
              vimages[-1]["setup"] = vim1["setup"]
            options.extend(["-e", "{}={}".format(vim1["env"], vm10.internal_ip)])

  FLAGS.dpt_docker_options = " ".join(options)
  return workers, vimages


def _AddNodeAffinity(spec, workers):
  if "affinity" not in spec:
    spec["affinity"] = {}

  if "nodeAffinity" not in spec["affinity"]:
    spec["affinity"]["nodeAffinity"] = {}

  if "requiredDuringSchedulingIgnoredDuringExecution" not in spec["affinity"]["nodeAffinity"]:
    spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"] = {}

  if "nodeSelectorTerms" not in spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]:
    spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"] = []

  spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"].append({
      "matchExpressions": [{
          "key": "kubernetes.io/hostname",
          "operator": "In",
          "values": [x["name"] for x in workers],
      }],
  })


def _ModifyImageRef(spec, images, ifcloud):
  for c1 in spec:
    c1["image"] = _ReplaceImage(c1["image"])
    if ifcloud:
      c1["imagePullPolicy"] = "IfNotPresent"


def _ScanImages(images, spec):
  for c2 in spec:
    image = c2["image"]
    for r1 in registries:
      if image.startswith(r1):
        images[image] = 1


def _ModifyEnvs(spec, vimages):
  if vimages:
    for c1 in spec:
      if "env" not in c1:
        c1["env"] = []
      elif not c1["env"]:
        c1["env"] = []
      for vm1 in vimages:
        c1["env"].append({
            "name": vm1["env"],
            "value": vm1["vm"].internal_ip,
        })


def _UpdateK8sConfig(controller0, workers, vimages):
  with open(FLAGS.dpt_kubernetes_yaml, "rt") as fd:
    docs = [d for d in safe_load_all(fd) if d]

  images = {}
  for doc in docs:
    for c1 in ["containers", "initContainers"]:
      spec = _WalkTo(doc, c1)
      if spec:
        _ScanImages(images, spec[c1])

  modified_docs = []
  for doc in docs:
    modified_docs.append(doc)

    spec = _WalkTo(doc, "containers")
    if spec:
      _AddNodeAffinity(spec, workers)
      _ModifyImageRef(spec["containers"], images, controller0.CLOUD != "Static" or FLAGS.dpt_iwos_cloud)
      _ModifyEnvs(spec["containers"], vimages)

    spec = _WalkTo(doc, "initContainers")
    if spec:
      _ModifyImageRef(spec["initContainers"], images, controller0.CLOUD != "Static" or FLAGS.dpt_iwos_cloud)
      _ModifyEnvs(spec["initContainers"], vimages)

  modified_filename = FLAGS.dpt_kubernetes_yaml + ".mod.yaml"
  with open(modified_filename, "wt") as fd:
    dump_all(modified_docs, fd)

  return modified_filename, images


def _ParseParams(params, metadata):
  for kv in params.split(";"):
    k, p, v = kv.partition(":")
    try:
      v = float(v.strip())
    except Exception:
      pass
    metadata[k.strip()] = v


def _AddNodeLabels(controller0, workers, vm):
  node = None
  labels = {}
  for worker in workers:
    if worker["vm"] == vm:
      for k in worker["labels"]:
        if worker["labels"][k] == "required":
          labels[k] = "yes"
          node = worker["name"]
  if labels and node:
    cmd = ["kubectl", "label", "--overwrite", "node", node] + [k + "=" + labels[k] for k in labels]
    controller0.RemoteCommand(" ".join(cmd))


def _GetController(vm_groups):
  if SUT_VM_CTR in vm_groups:
    return vm_groups[SUT_VM_CTR][0]
  return vm_groups[FLAGS.dpt_vm_groups[0]][-1]


def _GetWorkers(vm_groups):
  workers = []
  for g1 in vm_groups:
    if (g1 != SUT_VM_CTR) and (g1 in FLAGS.dpt_vm_groups):
      for vm1 in vm_groups[g1]:
        if vm1 not in workers:
          workers.append(vm1)
  return workers


# Best effort: Hugepage not supported that requires a reboot or kernel boot parameters
def _SetHugePages(workers, vm, ifkubelet):
  reqs = {}
  for worker in workers:
    if worker["vm"] == vm:
      for k in worker["labels"]:
        if k.startswith("HAS-SETUP-HUGEPAGE-") and worker["labels"][k] == "required":
          req = k.split("-")[-2:]
          if req[0] not in reqs:
            reqs[req[0]] = 0
          if int(req[1]) > reqs[req[0]]:
            reqs[req[0]] = int(req[1])

  for size in reqs:
    try:
      cmd = ["sudo", "cat", HUGEPAGE_NR.format(size)]
      stdout, _ = vm.RemoteCommand(" ".join(cmd))
      if reqs[size] > int(stdout):
        cmd = ["echo", str(reqs[size]), "|", "sudo", "tee", HUGEPAGE_NR.format(size)]
        vm.RemoteCommand(" ".join(cmd))
    except Exception as e:
      logging.warning('Exception: Hugepage %s: %s', str(size), str(e))

  if ifkubelet:
    vm.RemoteCommand("sudo systemctl restart kubelet")


def _ProbeModules(workers, vm):
  modules = []
  for worker in workers:
    if worker["vm"] == vm:
      for k in worker["labels"]:
        if k.startswith("HAS-SETUP-MODULE-") and worker["labels"][k] == "required":
          modules.append(k.replace("HAS-SETUP-MODULE-", "").lower())
  if modules:
    cmd = ["sudo", "modprobe"] + modules
    vm.RemoteCommand(" ".join(cmd), ignore_failure=True, suppress_warning=True)


# We use Habana Gaudi AMI. Assume the driver is already installed. Just need to refresh
# docker and containerd configurations after a new installation (k8s).
def _SetupHabanaWorker(controller0, workers, vm):
  for worker in workers:
    if worker["vm"] == vm:
      for k in worker["labels"]:
        if k.startswith("HAS-SETUP-HABANA-") and worker["labels"][k] == "required":
          vm.Install("habana")
          if controller0:
            RegisterWithContainerD(vm)
          else:
            RegisterWithDocker(vm)
          return


def _SetupHabanaController(controller0, workers):
  for worker in workers:
    for k in worker["labels"]:
      if k.startswith("HAS-SETUP-HABANA-") and worker["labels"][k] == "required":
        RegisterKubernetesPlugins(controller0)
        return


def _UniqueVms(workers, controller0=None):
  vms = []
  for worker in workers:
    if worker["vm"] != controller0 and worker["vm"] not in vms:
      vms.append(worker["vm"])
  return vms


def _PrepareWorker(controller0, workers, vm):
  _SetHugePages(workers, vm, controller0 is not None)
  _ProbeModules(workers, vm)
  _SetupHabanaWorker(controller0, workers, vm)
  if controller0:
    _AddNodeLabels(controller0, workers, vm)


def _PrepareVM(vimage):
  if "setup" in vimage:
    setup = vimage["setup"]
    vm1 = vimage["vm"]

    while True:
      try:
        vm1.RemoteCommand("sudo mkdir -p /opt/pkb/vmsetup && sudo chown -R {} /opt/pkb".format(vm1.user_name))
        vm1.PushFile("{}/{}.tgz".format(FLAGS.dpt_logs_dir, setup), "/opt/pkb")
        vm1.RemoteCommand("cd /opt/pkb/vmsetup && sudo tar xfz ../{}.tgz && sudo ./setup.sh {}".format(setup, " ".join(FLAGS.dpt_script_args)))
        break
      except Exception as e:
        logging.warning("vm setup failed: %s", str(e))
      vm1.RemoteCommand("sleep 10s", ignore_failure=True)
      vm1.WaitForBootCompletion()


# workaround apt-get lock by removing background processes
def _AptGetLockWA(vm):
  if vm.OS_TYPE.find("ubuntu") >= 0:
    vm.RemoteCommand("bash -c '{}'".format(REMOVE_LOCK), ignore_failure=True)


def _SaveConfigFiles(spec):
  spec.s3_reports.append((FLAGS.dpt_cluster_yaml, 'text/yaml'))
  cumulus_config_file = join(FLAGS.dpt_logs_dir, "cumulus-config.yaml")
  spec.s3_reports.append((cumulus_config_file, 'text/yaml'))

  if FLAGS.dpt_kubernetes_yaml:
    spec.s3_reports.append((FLAGS.dpt_kubernetes_yaml, 'text/yaml'))

  test_config_file = join(FLAGS.dpt_logs_dir, "test-config.yaml")
  _, _, sts = vm_util.IssueCommand(["file", "-f", test_config_file],
                                   raise_on_failure=False,
                                   suppress_warning=True)
  if sts == 0:
    spec.s3_reports.append((test_config_file, 'text/yaml'))


def Prepare(benchmark_spec):
  _SetBreakPoint("PrepareStage")
  benchmark_spec.name = FLAGS.dpt_name.replace(" ", "_").lower()
  benchmark_spec.workload_name = FLAGS.dpt_name
  benchmark_spec.sut_vm_group = FLAGS.dpt_vm_groups[0]
  benchmark_spec.always_call_cleanup = True

  benchmark_spec.control_traces = True
  FLAGS.dpt_trace_mode = [x.strip() for x in FLAGS.dpt_trace_mode]

  _SaveConfigFiles(benchmark_spec)
  _ParseParams(FLAGS.dpt_params, benchmark_spec.software_config_metadata)
  _ParseParams(FLAGS.dpt_tunables, benchmark_spec.tunable_parameters_metadata)
  tmp_dir = vm_util.GetTempDir()

  if FLAGS.dpt_docker_image:
    controller0 = _GetWorkers(benchmark_spec.vm_groups)[0]
    controller0.RemoteCommand("mkdir -p '{}'".format(tmp_dir))
    if controller0.CLOUD != "Static":
      if FLAGS.dpt_aptgetlock_workaround:
        _AptGetLockWA(controller0)

    workers, vimages = _ParseClusterConfigs(benchmark_spec.vm_groups)
    _PrepareWorker(None, workers, controller0)
    if controller0.CLOUD != "Static":
      _SetBreakPoint("SetupVM")
      vm_util.RunThreaded(lambda vim1: _PrepareVM(vim1), vimages)
      # Don't modify these logging info. Read by external procees to build ansible inventory for IWOS
      logging.info(f"docker_pt VM Info: Worker {controller0.ip_address} {controller0.internal_ip}")

  if FLAGS.dpt_kubernetes_yaml:
    controller0 = _GetController(benchmark_spec.vm_groups)
    controller0.RemoteCommand("mkdir -p '{}'".format(tmp_dir))

    if controller0.CLOUD != "Static":
      workers = _GetWorkers(benchmark_spec.vm_groups)
      if FLAGS.dpt_aptgetlock_workaround:
        vm_util.RunThreaded(lambda vm1: _AptGetLockWA(vm1), workers)
      workers = [vm1 for vm1 in workers if vm1 != controller0]
      taint = SUT_VM_CTR in benchmark_spec.vm_groups
      intel_k8s.InstallK8sCSP(controller0, workers, taint)
      # Don't modify these logging info. Read by external procees to build ansible inventory for IWOS
      logging.info(f"docker_pt VM Info: Controller {controller0.ip_address} {controller0.internal_ip}")
      for worker in workers:
        logging.info(f"docker_pt VM Info: Worker {worker.ip_address} {worker.internal_ip}")

    nodes = _GetNodes(controller0)
    workers, vimages = _ParseClusterConfigs(benchmark_spec.vm_groups, nodes)
    k8s_config_yaml, images = _UpdateK8sConfig(controller0, workers, vimages)

    if controller0.CLOUD != "Static":
      vm_util.RunThreaded(lambda vm1: _PrepareWorker(controller0, workers, vm1), _UniqueVms(workers))
      _SetupHabanaController(controller0, workers)
      _SetBreakPoint("SetupVM")
      vm_util.RunThreaded(lambda vim1: _PrepareVM(vim1), vimages)

    remote_yaml_file = join(tmp_dir, KUBERNETES_CONFIG)
    controller0.PushFile(k8s_config_yaml, remote_yaml_file)

  kpish = f"{FLAGS.dpt_logs_dir}/{KPISH}"
  for i in range(1, FLAGS.run_stage_iterations + 1):
    local_logs_dir = join(FLAGS.dpt_logs_dir, ITERATION_DIR.format(i))
    vm_util.IssueCommand(["mkdir", "-p", local_logs_dir])
    vm_util.IssueCommand(["cp", "-f", kpish, local_logs_dir])

  test_string = f"cd {ITERATION_DIR.format(1)}"
  if test_string not in open(kpish).read():
    vm_util.IssueCommand(["sh", "-c", "sed -i '1a[ -d {} ] && cd {}' {}".
                         format(ITERATION_DIR.format(1), ITERATION_DIR.format(1), kpish)])


def _PullExtractLogs(controller0, pods, remote_logs_dir):
  estr = None
  for pod1 in pods:
    remote_logs_tarfile = join(remote_logs_dir, LOGS_TARFILE.format(pod1))
    local_logs_dir = join(FLAGS.dpt_logs_dir, join(ITERATION_DIR.format(run_seq), f"{pod1}"))
    local_logs_tarfile = join(local_logs_dir, LOGS_TARFILE.format(pod1))
    try:
      vm_util.IssueCommand(["mkdir", "-p", local_logs_dir])
      controller0.PullFile(local_logs_tarfile, remote_logs_tarfile)
      vm_util.IssueCommand(["tar", "xf", local_logs_tarfile, "-C", local_logs_dir])
      vm_util.IssueCommand(["rm", "-f", local_logs_tarfile])
    except Exception as e:
      estr = str(e)
  if estr:
    raise Exception("Exception in extracting logs: " + estr)


def _TraceByTime(benchmark_spec, controller0):
  controller0.RemoteCommand(f"sleep {FLAGS.dpt_trace_mode[1]}s", ignore_failure=True)
  events.start_trace.send(stages.RUN, benchmark_spec=benchmark_spec)
  controller0.RemoteCommand(f"sleep {FLAGS.dpt_trace_mode[2]}s", ignore_failure=True)
  events.stop_trace.send(stages.RUN, benchmark_spec=benchmark_spec)


def _TraceByROI(benchmark_spec, controller0, timeout, cmds):
  timeout_h2 = int(int(timeout) / 2)
  _, _, status = controller0.RemoteCommandWithReturnCode("timeout {}s sh -c '({}&wait) | grep -q \"{}\"'".format(timeout_h2, cmds, FLAGS.dpt_trace_mode[1]), ignore_failure=True)
  if status == 0:
    events.start_trace.send(stages.RUN, benchmark_spec=benchmark_spec)
    controller0.RemoteCommand("timeout {}s sh -c '({}&wait) | grep -q \"{}\"'".format(timeout_h2, cmds, FLAGS.dpt_trace_mode[2]), ignore_failure=True)
    events.stop_trace.send(stages.RUN, benchmark_spec=benchmark_spec)


def Run(benchmark_spec):
  global run_seq
  run_seq = run_seq + 1

  _SetBreakPoint("RunStage")

  for vm in benchmark_spec.vms:
    thread_count = vm.num_cpus
    logging.debug(f"VM thread count: {thread_count}")

  tmp_dir = vm_util.GetTempDir()
  logging.debug(f"tmp_dir: {tmp_dir}")
  timeout = (FLAGS.dpt_timeout + "," + FLAGS.dpt_timeout).split(",")

  pull_logs = False
  if FLAGS.dpt_docker_image:
    controller0 = _GetWorkers(benchmark_spec.vm_groups)[0]

    options = FLAGS.dpt_docker_options.split(' ')
    if controller0.CLOUD == "Static" and not FLAGS.dpt_iwos_cloud:
      options.extend(["--pull", "always"])

    containers = [FLAGS.dpt_namespace]
    options1 = "--pull always" if controller0.CLOUD == "Static" else ""
    for image1 in FLAGS.dpt_docker_dataset:
      stdout, _ = controller0.RemoteCommand("sudo docker create {} {} -".format(options1, _ReplaceImage(image1)))
      container_id = stdout.strip()
      containers.append(container_id)
      options.extend(["--volumes-from", container_id])

    cmd = ["sudo", "docker", "run", "--name", FLAGS.dpt_namespace, "--rm", "--detach"] + options + \
          [_ReplaceImage(FLAGS.dpt_docker_image)]
    controller0.RemoteCommand(" ".join(cmd))
    _SetBreakPoint("DockerRun")

    if events.start_trace.receivers:
      if not FLAGS.dpt_trace_mode:
        events.start_trace.send(stages.RUN, benchmark_spec=benchmark_spec)

      elif FLAGS.dpt_trace_mode[0] == "roi":
        _TraceByROI(benchmark_spec, controller0, timeout[0],
                    f"sudo docker logs -f {FLAGS.dpt_namespace}")

      elif FLAGS.dpt_trace_mode[0] == "time":
        _TraceByTime(benchmark_spec, controller0)

    pods = [FLAGS.dpt_namespace]
    try:
      controller0.RemoteCommand("timeout {}s sudo docker exec {} sh -c 'cat /export-logs' > {}".format(timeout[0], FLAGS.dpt_namespace, join(tmp_dir, LOGS_TARFILE.format(pods[0]))))
      pull_logs = True
    except Exception as e:
      logging.fatal("Exception: %s", str(e))
      _SetBreakPoint("DockerExecFailed")

    if events.start_trace.receivers and (not FLAGS.dpt_trace_mode):
      events.stop_trace.send(stages.RUN, benchmark_spec=benchmark_spec)

    controller0.RemoteCommand("sudo docker logs {}".format(FLAGS.dpt_namespace),
                              ignore_failure=True, should_log=True)
    controller0.RemoteCommand("sudo docker rm -v -f {}".format(" ".join(containers)),
                              ignore_failure=True)

  if FLAGS.dpt_kubernetes_yaml:
    controller0 = _GetController(benchmark_spec.vm_groups)
    remote_yaml_file = join(tmp_dir, KUBERNETES_CONFIG)

    controller0.RemoteCommand(f"kubectl create namespace {FLAGS.dpt_namespace}")
    controller0.RemoteCommand(f"kubectl label namespace {FLAGS.dpt_namespace} {SF_NS_LABEL}")
    controller0.RemoteCommand(f"kubectl create --namespace={FLAGS.dpt_namespace} -f {remote_yaml_file}")
    _SetBreakPoint("KubectlApply")

    try:
      controller0.RemoteCommand("timeout {0}s sh -c 'q=0;until kubectl --namespace={1} wait pod --all --for=condition=Ready --timeout=1s 1>/dev/null 2>&1; do if kubectl --namespace={1} get pod -o json | grep -q Unschedulable; then q=1; break; fi; done; exit $q'".format(timeout[1], FLAGS.dpt_namespace))

      pods, _ = controller0.RemoteCommand("kubectl get --namespace=" + FLAGS.dpt_namespace + " pod --selector=" + FLAGS.dpt_kubernetes_job + " '-o=jsonpath={.items[*].metadata.name}'")
      pods = pods.strip(" \t\n").split(" ")
      container = FLAGS.dpt_kubernetes_job.rpartition("=")[2]

      if events.start_trace.receivers:
        if not FLAGS.dpt_trace_mode:
          events.start_trace.send(stages.RUN, benchmark_spec=benchmark_spec)

        elif FLAGS.dpt_trace_mode[0] == "roi":
          cmd = []
          for pod1 in pods:
            cmd.append(f"kubectl logs --ignore-errors --prefix=false -f {pod1} -c {container} --namespace={FLAGS.dpt_namespace}")
          _TraceByROI(benchmark_spec, controller0, timeout[0], "&".join(cmd))

        elif FLAGS.dpt_trace_mode[0] == "time":
          _TraceByTime(benchmark_spec, controller0)

      cmd = []
      for pod1 in pods:
        remote_logs_tarfile = join(tmp_dir, LOGS_TARFILE.format(pod1))
        cmd.append(f"kubectl exec --namespace={FLAGS.dpt_namespace} {pod1} -c {container} -- cat /export-logs > {remote_logs_tarfile};x=$?;test $x -ne 0 && r=$x")

      try:
        controller0.RemoteCommand("timeout {}s sh -c 'r=0;{};exit $r'".format(timeout[0], ";".join(cmd)))
        pull_logs = True
      except Exception as e:
        logging.fatal("Exception: %s", str(e))
        _SetBreakPoint("KubectlExecFailed")

      if events.start_trace.receivers and (not FLAGS.dpt_trace_mode):
        events.stop_trace.send(stages.RUN, benchmark_spec=benchmark_spec)

    except Exception as e:
      logging.fatal("Exception: %s", str(e))
      _SetBreakPoint("KubectlWaitFailed")

    controller0.RemoteCommand("kubectl get node -o json",
                              ignore_failure=True, should_log=True)
    controller0.RemoteCommand(f"kubectl describe pod --namespace={FLAGS.dpt_namespace}",
                              ignore_failure=True, should_log=True)
    controller0.RemoteCommand('for p in $(kubectl get pod -n {0} --no-headers -o custom-columns=:metadata.name);do echo "pod $p:";kubectl -n {0} logs --all-containers=true $p;done'.format(FLAGS.dpt_namespace),
                              ignore_failure=True, should_log=True)

    if (controller0.CLOUD == "Static") or (run_seq < FLAGS.run_stage_iterations):
      controller0.RemoteCommand(f"kubectl delete --namespace={FLAGS.dpt_namespace} -f {remote_yaml_file} --force --wait",
                                ignore_failure=True)
      controller0.RemoteCommand(f"kubectl delete namespace {FLAGS.dpt_namespace} --force --wait",
                                ignore_failure=True)

  # pull the logs tarfile back
  samples = []
  if pull_logs:
    _PullExtractLogs(controller0, pods, tmp_dir)
    samples = _ParseKPI()

  if not samples:
    _SetBreakPoint("TestFailed")
    raise Exception("Test failed with no KPI data")

  return samples


def Cleanup(benchmark_spec):
  _SetBreakPoint("CleanupStage")
  tmp_dir = vm_util.GetTempDir()

  _, vimages = _ParseClusterConfigs(benchmark_spec.vm_groups)
  for i, vim1 in enumerate(vimages):
    if "setup" in vim1:
      local_dir = "{}/{}/{}".format(FLAGS.dpt_logs_dir, vim1["name"], i)
      vm_util.IssueCommand(["mkdir", "-p", local_dir])
      try:
        vm1 = vim1["vm"]
        vm1.RemoteCommand("cd /opt/pkb/vmsetup && sudo ./cleanup.sh {}".format(" ".join(FLAGS.dpt_script_args)))
        vm1.PullFile("{}/vmlogs.tgz".format(local_dir), "/opt/pkb/vmsetup/vmlogs.tgz")
      except Exception as e:
        logging.warning("Exception: %s", str(e))

  if FLAGS.dpt_docker_image:
    controller0 = _GetWorkers(benchmark_spec.vm_groups)[0]

  if FLAGS.dpt_kubernetes_yaml:
    controller0 = _GetController(benchmark_spec.vm_groups)

  # cleanup containers
  if controller0.CLOUD == "Static":
    controller0.RemoteCommand(f"rm -rf '{tmp_dir}'", ignore_failure=True)