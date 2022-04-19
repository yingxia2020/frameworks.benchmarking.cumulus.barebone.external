# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing abstract classes related to collectors."""


import abc
import functools
import logging
import os
import posixpath
import threading
import time
import uuid
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import trace_util
import six

FLAGS = flags.FLAGS
flags.DEFINE_string('trace_vm_groups', None,
                    'A comma separated list of vm_groups on which trace '
                    'will be run. If not specified, trace is run on all VMs.')
flags.DEFINE_boolean('trace_skip_install', False,
                     'Skip installations before trace collection.')
flags.DEFINE_boolean('trace_skip_cleanup', False,
                     'Skip cleanups after trace collection.')


def Register(parsed_flags):
  """Registers the collector if FLAGS.<collector> is set.

  See dstat.py for an example on how to register a collector.

  Args:
    parsed_flags: argument passed into each call to Register()
  """
  del parsed_flags  # unused


def IsEnabled():
  return False


class BaseCollector(object):
  """Object representing a Base Collector.

  A Collector is a utility that is ran alongside benchmarks to record stats
  at various points when running a benchmark. A Base collector is an abstract
  class with common routines that derived collectors use.
  """

  def __init__(self, interval=None, output_directory=None):
    """Runs collector on 'vms'.

    Start collector collection via `Start`. Stop via `Stop`.

    Args:
      interval: Optional int. Interval in seconds in which to collect samples.
      output_directory: Optional directory where to save collection output.
    Raises:
      IOError: for when the output directory doesn't exist.
    """
    self.interval = interval
    self.output_directory = output_directory or vm_util.GetTempDir()
    self._lock = threading.Lock()
    self._pid_files = {}
    self._role_mapping = {}  # mapping vm role to output file
    self._start_time = 0

    if not os.path.isdir(self.output_directory):
      raise IOError('collector output directory does not exist: {0}'.format(
          self.output_directory))

  @abc.abstractmethod
  def _CollectorName(self):
    pass

  @abc.abstractmethod
  def _InstallCollector(self, vm):
    pass

  @abc.abstractmethod
  def _CollectorRunCommand(self, vm, collector_file):
    pass

  def _KillCommand(self, pid):
    """Command to kill off the collector."""
    return 'kill {0}'.format(pid)

  def _StartOnVm(self, vm, suffix=''):
    """Start collector, having it write to an output file."""

    suffix = '{0}-{1}'.format(suffix, self._CollectorName())
    collector_file = posixpath.join(
        vm_util.VM_TMP_DIR, '{0}{1}.stdout'.format(vm.name, suffix))

    cmd = self._CollectorRunCommand(vm, collector_file)
    logging.info("Starting {0} on {1} @ {2}".format(self._CollectorName(), vm.name, vm.ip_address))
    stdout, _ = vm.RemoteCommand(cmd)
    pid = stdout.strip()
    with self._lock:
      self._pid_files[vm.name] = (pid, collector_file)

  def _StopOnVm(self, vm, vm_role):
    """Stop collector on 'vm' and copy the files back."""
    if vm.name not in self._pid_files:
      logging.warn('No collector PID for %s', vm.name)
      return
    else:
      with self._lock:
        pid, file_name = self._pid_files.pop(vm.name)
    vm.RemoteCommand(self._KillCommand(pid), ignore_failure=True)

    try:
      vm.PullFile(self.output_directory, file_name)
      self._role_mapping[vm_role] = file_name
    except errors.VirtualMachine.RemoteCommandError as ex:
      logging.exception('Failed fetching collector result from %s.', vm.name)
      raise ex

  def Install(self, sender, benchmark_spec):
    """Install and start collector on all VMs in 'benchmark_spec'. Can be
       limited by setting FLAGS.trace_vm_groups.
    """
    vms = trace_util.GetVMsToTrace(benchmark_spec, FLAGS.trace_vm_groups)
    vm_util.RunThreaded(self._InstallCollector, vms)

  def Start(self, sender, benchmark_spec):
    """Starts a collector on vms in the benchmark
    """
    suffix = '-{0}-{1}'.format(benchmark_spec.uid, str(uuid.uuid4())[:8])
    vms = trace_util.GetVMsToTrace(benchmark_spec, FLAGS.trace_vm_groups)
    func = functools.partial(self._StartOnVm, suffix=suffix)
    vm_util.RunThreaded(func, vms)
    self._start_time = time.time()
    return

  def Stop(self, sender, benchmark_spec, name=''):
    """Stop collector on all VMs in 'benchmark_spec', fetch results."""
    vm_groups = trace_util.GetVMGroupsToTrace(benchmark_spec, FLAGS.trace_vm_groups)
    self.StopOnVms(sender, vm_groups, name)

  def StopOnVms(self, sender, vm_groups, name):
    """Stop collector on given subset of vms, fetch results.

    Args:
      sender: sender of the event to stop the collector.
      vm_groups: vm_groups to stop the collector on.
      name: name of event to be stopped.
    """
    events.record_event.send(sender, event=name,
                             start_timestamp=self._start_time,
                             end_timestamp=time.time(),
                             metadata={})
    args = []
    for role, vms in six.iteritems(vm_groups):
      args.extend([((
          vm, '%s_%s' % (role, idx)), {}) for idx, vm in enumerate(vms)])
    vm_util.RunThreaded(self._StopOnVm, args)
    return

  @abc.abstractmethod
  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze collector file and record samples."""
    pass
