# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs STREAM benchmark.

"""

import logging
import re
from six import StringIO
import time
import copy

from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import errors
from statistics import stdev
from statistics import mean
from perfkitbenchmarker.linux_packages import stream

FLAGS = flags.FLAGS

flags.DEFINE_integer('stream_array_size', 0,
                     'Override calculated/dynamic array size (LLC * 4).')
flags.DEFINE_integer('stream_ntimes', 10,
                     'STREAM runs each kernel "NTIMES" times and reports the '
                     '*best* result for any iteration after the first')
flags.DEFINE_integer('stream_iterations', 0,
                     'STREAM number of iteration per run, the median of best 3 runs '
                     'will be reported, minimum 3 or set 0 for benchmark default. '
                     'WARNING! In some cases reporting median of best 3 runs is equivalent'
                     'to cherry picking the result. Be aware and use with caution, '
                     'an RSD > 1% may indicate something is off.')
flags.DEFINE_string('stream_binary_url', None,
                    'Pass here the url from where the stream pre-compiled binary can'
                    'be downloaded.')
flags.DEFINE_integer('stream_offset', 0,
                     '"OFFSET" variable, which *may* change the relative '
                     'alignment of the arrays')
flags.DEFINE_string('stream_compiler_flags', '-O3 -mcmodel=medium',
                    'override flags to pass to the compiler command line, e.g. '
                    '-O3 -mcmodel=medium -fopenmp -march=skylake-avx512')
flags.DEFINE_integer('stream_omp_num_threads', 0,
                     'the number of threads/cores used when the resulting '
                     'openmp-enabled program is executed. Zero means use '
                     'a number of threads equal to the number of VCPUs.')

BENCHMARK_NAME = 'stream'
BENCHMARK_CONFIG = """
stream:
  description: STREAM benchmark.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      os_type: ubuntu1804
"""
VM_GROUP = 'default'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetLLCacheSize(vm):
  output, _ = vm.RemoteCommand('lscpu | grep "L3 cache" | grep -Eo "[0-9]+" | tail -n1')
  if output:
    pass
  else:
    output, _ = vm.RemoteCommand('lscpu | grep "L2 cache" | grep -Eo "[0-9]+" | tail -n1')
  llc_size = int(output) * 1024
  logging.info("LLC Cache Size is: {0}".format(llc_size))
  return int(llc_size)


def Prepare(benchmark_spec):
  """Prepare the client test VM, installs STREAM.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  benchmark_spec.workload_name = 'STREAM'
  benchmark_spec.sut_vm_group = 'default'
  vm = benchmark_spec.vms[0]
  if FLAGS.stream_array_size == 0:
    FLAGS.stream_array_size = _GetLLCacheSize(vm) * 4
  vm.Install('stream')


def _CreateMetadataFromFlags(benchmark_spec):
  sw_params = benchmark_spec.software_config_metadata
  tuning_params = benchmark_spec.tunable_parameters_metadata
  sw_params["stream_compiler_flags"] = FLAGS.stream_compiler_flags
  tuning_params["stream_array_size"] = FLAGS.stream_array_size
  tuning_params["stream_ntimes"] = FLAGS.stream_ntimes
  tuning_params["stream_offset"] = FLAGS.stream_offset
  tuning_params["stream_compiler_flags"] = FLAGS.stream_compiler_flags
  tuning_params["stream_omp_num_threads"] = FLAGS.stream_omp_num_threads
  tuning_params["stream_iterations"] = FLAGS.stream_iterations


def _GetFloatFromLine(line):
  val = 0.0
  match = re.search(r'([0-9]*\.?[0-9]+)', line)
  if match is None:
    logging.error("Parsing error -- regex doesn't match for string: %s", line)
  else:
    try:
      val = float(match.group(1))
    except ValueError:
      logging.error("Parsing error -- type conversion failed for: %s", match.group(1))
  return val


def _GetMetricsFromLine(line):
  match = re.search(r'(\w+):\s+([0-9]*\.?[0-9]+)\s+([0-9]*\.?[0-9]+)\s+([0-9]*\.?[0-9]+)\s+([0-9]*\.?[0-9]+)', line)
  if match is None:
    logging.error("Parsing error -- regex doesn't match for string: %s", line)
  else:
    try:
      function = match.group(1)
      rate = float(match.group(2))
      avg_time = float(match.group(3))
      min_time = float(match.group(4))
      max_time = float(match.group(5))
    except ValueError:
      logging.error("Parsing error -- type conversion failed")
      raise
  return function, rate, avg_time, min_time, max_time


""" Sample STREAM output:
-------------------------------------------------------------
STREAM version $Revision: 5.10 $
-------------------------------------------------------------
This system uses 8 bytes per array element.
-------------------------------------------------------------
Array size = 10000000 (elements), Offset = 0 (elements)
Memory per array = 76.3 MiB (= 0.1 GiB).
Total memory required = 228.9 MiB (= 0.2 GiB).
Each kernel will be executed 10 times.
 The *best* time for each kernel (excluding the first iteration)
 will be used to compute the reported bandwidth.
-------------------------------------------------------------
Your clock granularity/precision appears to be 1 microseconds.
Each test below will take on the order of 14125 microseconds.
   (= 14125 clock ticks)
Increase the size of the arrays if this shows that
you are not getting at least 20 clock ticks per test.
-------------------------------------------------------------
WARNING -- The above is only a rough guideline.
For best results, please be sure you know the
precision of your system timer.
-------------------------------------------------------------
Function    Best Rate MB/s  Avg time     Min time     Max time
Copy:            7729.5     0.020729     0.020700     0.020790
Scale:           7690.2     0.020863     0.020806     0.020929
Add:             8718.1     0.027545     0.027529     0.027570
Triad:           8705.4     0.027639     0.027569     0.027774
-------------------------------------------------------------
Solution Validates: avg error less than 1.000000e-13 on all three arrays
-------------------------------------------------------------
"""


def _ParseStreamOutput(stream_output, benchmark_spec):
  """Parse output into an array of samples"""
  results = []
  tbr = 0
  stream_output_io = StringIO(stream_output)
  for line in stream_output_io:
    sline = line.strip()
    if sline.startswith("STREAM version"):
      benchmark_spec.software_config_metadata['STREAM version'] = str(_GetFloatFromLine(sline))
    elif sline.startswith("Memory per array"):
      benchmark_spec.software_config_metadata['Memory per array'] = str(_GetFloatFromLine(sline)) + " MiB"
    elif sline.startswith("Total memory"):
      benchmark_spec.software_config_metadata['Total memory required'] = str(_GetFloatFromLine(sline)) + " MiB"
    elif sline.startswith("Copy:") or sline.startswith("Scale:") or sline.startswith("Add:") or sline.startswith("Triad:"):
      function, rate, avg_time, min_time, max_time = _GetMetricsFromLine(sline)
      if sline.startswith("Triad:"):
        tbr = rate
      if function == "Triad":
        if FLAGS.stream_iterations >= 3:
          warning_metadata = {"WARNING": "In some cases reporting median of best 3 runs is equivalent "
                                         "to cherry picking the result.", 'primary_sample': True}
          results.append(sample.Sample(function + " Best Rate", rate, "MB/s", warning_metadata))
        else:
          results.append(sample.Sample(function + " Best Rate", rate, "MB/s", {'primary_sample': True}))
      else:
        results.append(sample.Sample(function + " Best Rate", rate, "MB/s", {}))
      results.append(sample.Sample(function + " Avg time", avg_time, "s", {}))
      results.append(sample.Sample(function + " Min time", min_time, "s", {}))
      results.append(sample.Sample(function + " Max time", max_time, "s", {}))

  return tbr, results


def Run(benchmark_spec):
  """Run the STREAM benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Results.
  """
  logging.info('Start benchmarking STREAM')
  vm = benchmark_spec.vms[0]
  _CreateMetadataFromFlags(benchmark_spec)
  if FLAGS.stream_iterations == 0:
    stdout, _ = vm.RemoteCommand(stream.GetStreamExec(vm))
    logging.info('\n Parsing STREAM Results...\n')
    tbr, results = _ParseStreamOutput(stdout, benchmark_spec)
  elif FLAGS.stream_iterations >= 3:
    all_results = {}
    rates = []
    for it in range(FLAGS.stream_iterations):
      stdout, _ = vm.RemoteCommand(stream.GetStreamExec(vm))
      tbr, results = _ParseStreamOutput(stdout, benchmark_spec)
      all_results[tbr] = results
      all_results[str(tbr) + "_stdout"] = stdout
      rates.append(tbr)
    logging.info('\n Parsing STREAM Results...\n')
    rates.sort(reverse=True)
    rsd = stdev(rates) / mean(rates) * 100
    results = all_results[rates[1]]
    logging.info('All results for triad best rate (MB/s): {0}'.format(rates))
    logging.info('Relative standard deviation (%):        {0}'.format(rsd))
    logging.info('Reporting result for triad best rate:   {0} MB/s'.format(rates[1]))
    stdout = all_results[str(rates[1]) + "_stdout"]
    results.append(sample.Sample("RSD for all {} iterations".format(FLAGS.stream_iterations), rsd, "%"))
  else:
    raise errors.Config.InvalidValue('Wrong number of iterations: {0}\nFor the median of best 3 runs '
                                     'a minimum set of 3 iterations is required, optionally set 0 for '
                                     'benchmark legacy mode with no iterations'.format(FLAGS.stream_iterations))
  compiler = FLAGS.compiler
  logging.info('Using {} compiler for this run'.format(compiler))
  compiler_version, _ = vm.RemoteCommand('{0} --version | head -n 1'.format(compiler), retries=1, ignore_failure=True)
  if FLAGS.stream_binary_url:
    benchmark_spec.software_config_metadata['stream_binary'] = '{}'.format(FLAGS.stream_binary_url)
  else:
    benchmark_spec.software_config_metadata['stream_repo'] = stream.GIT_REPO
    benchmark_spec.software_config_metadata['compiler_version'] = compiler_version.strip()
  logging.info('STREAM output: \n{0}'.format(stdout))
  return results


def Cleanup(benchmark_spec):
  """Clean up Sysbench CPU benchmark related states.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Uninstall('stream')
  del benchmark_spec
