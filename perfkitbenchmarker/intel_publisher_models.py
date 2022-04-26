import uuid
import copy
import os
import json
import logging

PERFKITRUN_COLLECTION = 'perfKitRuns'
PLATFORM_COLLECTION = 'platforms'
SAMPLEPOINT_COLLECTION = 'samplePoints'
METADATA_COLLECTION = 'metadata'
METADATA_SW_CONFIG_TYPE = 'software_config'
METADATA_PARAMS_TYPE = 'params'
METADATA_MISC_TYPE = 'misc'
METADATA_SUT_TYPE = 'sut'


def InitializeCollections():
  return {
      PERFKITRUN_COLLECTION: [],
      PLATFORM_COLLECTION: [],
      SAMPLEPOINT_COLLECTION: [],
      METADATA_COLLECTION: [],
  }


class IntelPublisherDocument:

  def todict(self):
    return self.__dict__


class PerfkitRun(IntelPublisherDocument):

  def __init__(self, benchmark_spec, owner):
    self.uri = benchmark_spec.uuid
    self._id = self.uri
    self.timestamp = None
    self.cmd_line = None
    self.run_uri_short = benchmark_spec.uuid.split('-')[0]
    self.perfkitbenchmarker_version = None
    self.tester = owner
    self.workload_name = benchmark_spec.workload_name or benchmark_spec.name
    self.tags = []
    self.softw_config_uri = None
    self.tune_param_uri = None
    self.misc_metadata_uri = None
    self.s3_archive_url = benchmark_spec.s3_archive_url
    self.s3_report_urls = []
    self.sut_platform = {}
    self.primary_sample_point = {}

  def SetSutPlatform(self, platform_short_form):
    self.sut_platform = copy.deepcopy(platform_short_form)

  def SetPrimarySamplePoint(self, sample_point_short_form):
    self.primary_sample_point = copy.deepcopy(sample_point_short_form)


class Platform(IntelPublisherDocument):

  def __init__(self, run_uri, vm, vm_group):
    self.uri = str(uuid.uuid4())
    self._id = self.uri
    self.run_uri = run_uri
    self.pkb_name = vm.name
    self.vm_group = vm_group
    self.server_info = {}
    self.machine_type = vm.machine_type
    self.cloud = vm.CLOUD
    self.zone = vm.zone
    self.kernel_release = vm.kernel_release
    self.num_of_sockets = None
    self.cpu_model = None
    self.total_cpus = vm.num_cpus
    self.numa_node_count = vm.numa_node_count
    self.frequency = None
    self.microarchitecture = None
    self.manufacturer = None
    self.product_name = None
    self.os_name = vm.os_info
    self.server_info_html_url = ""
    self.ip_address = None
    self.memory = "{:.2f} GB".format(float(vm.total_memory_kb / 1000 / 1000))
    self.memory_spec = None
    self.sut_metadata_uri = None
    self.cpu_rolling_avg_5m = None

  def ToShortForm(self):
    short = {
        'uri': self.uri,
        'cloud': self.cloud,
        'cpu_model': self.cpu_model,
        'machine_type': self.machine_type,
        'os_name': self.os_name,
        'total_cpus': self.total_cpus,
    }
    if self.microarchitecture:
      short['microarchitecture'] = self.microarchitecture
    return short

  def AddCpuUtilRollingAvg(self, pkb_dir):
    cpu_nice = 'nice'
    cpu_system = 'system'
    cpu_user = 'user'
    path_to_avg = os.path.join(pkb_dir, f'{self.pkb_name}-collectd', 'aggregation-cpu-average')
    aggregation_by_timestamp = {}
    for (_, _, files) in os.walk(path_to_avg):
      for file in files:
        # strip date from file name
        _, cpu_time_type = file.split('-')[:-3]
        if cpu_time_type in [cpu_nice, cpu_system, cpu_user]:
          try:
            with open(os.path.join(path_to_avg, file), "r") as csvfile:
              next(csvfile)
              for line in csvfile:
                epoch, value = line.strip().split(',', 1)
                epoch = int(float(epoch))
                aggregation_by_timestamp.setdefault(epoch, 0)
                if value != 'nan':
                  aggregation_by_timestamp[epoch] += float(value)
          except Exception as e:
            logging.error(f'Encountered exception {e} when trying to parse {file}. '
                          f'Skipping CPU time parsing as state is unknown')
            self.cpu_rolling_avg_5m = 'unknown'
            return
    window_duration = 300
    best_window_avg_value = 0
    best_window = []
    current_window = []

    def _Avg(window):
      window_values = [value for (_, value) in window]
      return round((sum(window_values) / len(window_values)), 2)

    for epoch in sorted(aggregation_by_timestamp.keys()):
      value = aggregation_by_timestamp[epoch]
      current_window.append((epoch, value))
      if (epoch - current_window[0][0]) > window_duration:
        current_window.pop(0)
      current_avg = _Avg(current_window)
      if len(current_window) > len(best_window) or current_avg > best_window_avg_value:
        best_window = current_window
        best_window_avg_value = current_avg
    if best_window:
      self.cpu_rolling_avg_5m = best_window_avg_value
    logging.info(f'Best rolling average for a {window_duration}s window was {best_window_avg_value}')

  def AddSvrinfo(self, pkb_dir, ip_address, s3_bucket_url):
    self.ip_address = ip_address
    json_path = self.GetLocalSvrinfoFilename(pkb_dir, self.pkb_name, ip_address, '.json')
    self.server_info_html_url = os.path.join(s3_bucket_url, self.pkb_name + '-svrinfo.html')
    try:
      with open(json_path, 'r') as f:
        self.server_info = json.load(f)
    except Exception as err:
      logging.error("Encountered exception '{}' while attempting read and parse svr_info.".format(err))
    self.cpu_model = self.SvrinfoValue("CPU", "CPU Model")
    logging.info("cpu model: %s", self.cpu_model)
    self.num_of_sockets = int(self.SvrinfoValue("CPU", "Sockets", "0"))
    logging.info("num sockets: %s", self.num_of_sockets)
    self.microarchitecture = self.SvrinfoValue("CPU", "Microarchitecture")
    logging.info("microarch: %s", self.microarchitecture)
    self.frequency = \
        "{} (Base), {} (Max), {} (All-core Max)".format(self.SvrinfoValue("CPU", 'Base Frequency', '-'),
                                                        self.SvrinfoValue("CPU", 'Maximum Frequency', '-'),
                                                        self.SvrinfoValue("CPU", 'All-core Maximum Frequency', '-')) \
        if any((self.SvrinfoValue("CPU", 'Base Frequency'), self.SvrinfoValue("CPU", 'Maximum Frequency'), self.SvrinfoValue("CPU", 'All-core Maximum Frequency'))) else None
    logging.info("frequency: %s", self.frequency)
    self.manufacturer = self.SvrinfoValue("System", "Manufacturer")
    logging.info("manufacturer: %s", self.manufacturer)
    self.product_name = self.SvrinfoValue("System", "Product Name")
    logging.info("product name: %s", self.product_name)
    self.memory_spec = self.SvrinfoValue("Memory", "Installed Memory")
    logging.info("memory spec: %s", self.memory_spec)

  def SvrinfoValue(self, table_name, value_name, no_value=""):
    value = no_value
    for table in self.server_info:
      if table["Name"] == table_name:
        try:
          value_index = table["AllHostValues"][0]["ValueNames"].index(value_name)
          value = table["AllHostValues"][0]["Values"][0][value_index]
        except ValueError as err:
          logging.error("Did not find svr_info value '{}' in the '{}' table, {}".format(value_name, table_name, err))
          return
        except IndexError as err:
          logging.error("Unexpected svr_info json format, {}".format(err))
          return
        except KeyError as err:
          logging.error("Unexpected svr_info json format, {}".format(err))
          return
        break
    return value


  @staticmethod
  def GetLocalSvrinfoFilename(pkb_dir, pkb_vm_name, ip_address, ext):
    local_results_dir = os.path.join(pkb_dir, pkb_vm_name + '-svrinfo')
    return os.path.join(local_results_dir, ip_address + ext)


class SamplePoint(IntelPublisherDocument):
  def __init__(self, workload_name, run_uri, sample):
    self.run_uri = run_uri
    self.workload_name = workload_name
    self.uri = str(uuid.uuid4())
    self._id = self.uri
    self.metric = sample.metric
    self.unit = sample.unit
    self.value = sample.value
    self.timestamp = sample.timestamp
    self.sample_metadata = sample.metadata

  def ToShortForm(self):
    return {
        'uri': self.uri,
        'metric': self.metric,
        'unit': self.unit,
        'value': self.value
    }


class Metadata(IntelPublisherDocument):
  def __init__(self, json, type):
    self.uri = str(uuid.uuid4())
    self._id = self.uri
    self.type = type
    self.json_data = json
