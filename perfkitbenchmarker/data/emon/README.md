### EMON

This telemetry data collection tool can be used to collect EMON data on the SUT(s) while the workload is running on the same SUT(s). This is one of the traces based tool set in the Perfkitbenchmark (PKB) framework. By default, the telemetry tool will be fired at the same time the benchmark is launched, and terminated when the benchmark run is completed.  The result will be pulled back to the PKB host in the PKB output folder.

"--emon" is the only required command line flag for EMON collection. All other EMON related flags are optional.

⚠️ **Please note that currently EMON runs only on bare metal instances.**

```

Note :- By default the latest emon is available for you to run. If you want to use a custom version,
use --emon_tarball=<emon_version>

For AMD runs, download and use the emon AMD version from emon website with --emon_tarball=<emon_amd_version>

```

#### Use Case 1: Collect EMON data and conduct EDP post-processing on-prem.

```
python3 ./pkb.py --emon --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

##### Here is an example of sysbench_cpu_config.yaml:
```bash
static_vms:
  - &worker
    ip_address: 10.165.57.29
    user_name: pkb
    ssh_private_key: ~/.ssh/id_rsa
    internal_ip: 10.165.57.29
    tag: server
sysbench_cpu:
  vm_groups:
    vm_1:
      static_vms:
        - *worker

flags:
  sysbench_cpu_time: 60           # run for 60 seconds
  sysbench_cpu_events: 0          # don't limit runtime by event count
  sysbench_cpu_thread_counts: 1,0 # zero sets threadcount to number of VCPUs
```
##### The on-prem system-under-test (SUT) needs to be in HW and SW provisioned status. See more details here: https://wiki.ith.intel.com/display/cloudperf/How+to+run+PKB+On-Prem


#### Use Case 2: Collect EMON data and conduct EDP post processing on AWS.

```
python3 ./pkb.py --emon --benchmarks=sysbench_cpu --cloud=AWS --machine_type=m5.24xlarge --os_type=ubuntu1804 
```
##### The cloud testing is easier than on-prem testing, since the HW and SW provision is done automatically. The caveat is, the users must apply reasonable doubt on what counters can be trusted on this VM, --machine_type=m5.24xlarge. For more accurate counters, try bare metal machines, such as --machine_type=m5.metal on AWS.

#### Use Case 3: Use customized EMON tarball.

```
python3 ./pkb.py --emon --emon_tarball=sep_private_5_19_linux_07062101c5153a9.tar.bz2 --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 4: Collect EMON data but skip EDP post processing.

```
python3 ./pkb.py --emon --emon_post_process_skip --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 5: Use customized EMON event list input.

```
python3 ./pkb.py --emon --edp_events_file=<path to edp events file>  --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 6: Use customized EMON EDP post processing config input.

```
python3 ./pkb.py --emon --edp_config_file=<path to emon_edp_config.txt>  --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 7: Use customized EMON EDP post processing script type such as Python3. Ruby will be used by default.

```
python3 ./pkb.py --emon --edp_script_type=python3  --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 8: Keep the EMON driver, automation scripts for data collection, and output on the SUT(s) post data collection. They are removed by default.

```
python3 ./pkb.py --emon --emon_debug --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```

#### Use Case 9: Delay the EMON collection by 15 sec after benchmark is launched, and collect EMON for 60 sec only.

```
python3 ./pkb.py --emon  --trace_start_delay=15 --trace_duration=60 --benchmarks=sysbench_cpu --benchmark_config_file=sysbench_cpu_config.yaml
```
##### This is a time-driven example. For event-driven use cases, please refer to specjbb implementation as an example, and see how a workload like spebjbb detects an event to decide when start or stop EMON collection, by sending a signal back to the framework: https://gitlab.devtools.intel.com/PerfKitBenchmarker/perfkitbenchmarker/-/blob/master/perfkitbenchmarker/linux_benchmarks/specjbb_benchmark.py

##### To get a complete command line options or flags.

```
python3 ./pkb.py --helpmatch=emon
```

##### A sample output file list. The CSV files can be imported manually into an Excel spreadsheet, and be used to create charts, such as CPU Utilition% during the collection.

```bash
pxwang@pxwang-desk1:~/pkb-gitlab/pxwang/perfkitbenchmarker$ ls -ltr /tmp/perfkitbenchmarker/runs/aba6a151/pkb-aba6a151-1-emon/
total 122276
-rwxrwxr-x 1 pxwang pxwang       61 Aug 21 10:00 start_emon-sanity-check.sh
-rw-rw-r-- 1 pxwang pxwang      317 Aug 21 10:00 emon-sanity-check.sh
-rw-r--r-- 1 pxwang pxwang     2179 Aug 21 10:00 emon-M.dat
-rwxrwxr-x 1 pxwang pxwang       55 Aug 21 10:00 start_emon.sh
-rw-rw-r-- 1 pxwang pxwang      173 Aug 21 10:00 emon_stop.sh
-rw-rw-r-- 1 pxwang pxwang      298 Aug 21 10:00 emon_start.sh
-rwxrwxr-x 1 pxwang pxwang       44 Aug 21 10:00 stop_emon.sh
-rw-rw-r-- 1 pxwang pxwang      665 Aug 21 10:00 post_process_emon.sh
-rwxrwxr-x 1 pxwang pxwang       66 Aug 21 10:00 start_post_process_emon.sh
-rw-r--r-- 1 pxwang pxwang  6694115 Aug 21 10:04 emon.dat
-rw-rw---- 1 pxwang pxwang     2293 Aug 21 10:05 emon-m.dat
-rw-r--r-- 1 pxwang pxwang     3397 Aug 21 10:05 emon-v.dat
-rw-rw---- 1 pxwang pxwang    18829 Aug 21 10:06 __edp_system_view_summary.per_txn.csv
-rw-rw---- 1 pxwang pxwang    34650 Aug 21 10:06 __edp_system_view_summary.csv
-rw-rw---- 1 pxwang pxwang   980729 Aug 21 10:06 __edp_system_view_details.csv
-rw-rw---- 1 pxwang pxwang    23076 Aug 21 10:06 __edp_socket_view_summary.per_txn.csv
-rw-rw---- 1 pxwang pxwang    23112 Aug 21 10:06 __edp_socket_view_summary.csv
-rw-rw---- 1 pxwang pxwang  1919702 Aug 21 10:06 __edp_socket_view_details.csv
-rw-rw---- 1 pxwang pxwang   190105 Aug 21 10:06 __edp_core_view_summary.per_txn.csv
-rw-rw---- 1 pxwang pxwang   192399 Aug 21 10:06 __edp_core_view_summary.csv
-rw-rw---- 1 pxwang pxwang 32247772 Aug 21 10:06 __edp_core_view_details.csv
-rw-rw---- 1 pxwang pxwang   364113 Aug 21 10:06 __edp_thread_view_summary.per_txn.csv
-rw-rw---- 1 pxwang pxwang   368693 Aug 21 10:06 __edp_thread_view_summary.csv
-rw-rw---- 1 pxwang pxwang 63962017 Aug 21 10:06 __edp_thread_view_details.csv
-rw-r--r-- 1 pxwang pxwang 18121814 Aug 21 10:06 emon_result.tar.gz
pxwang@pxwang-desk1:~/pkb-gitlab/pxwang/perfkitbenchmarker$
```

##### Refer to https://gitlab.devtools.intel.com/PerfKitBenchmarker/perfkitbenchmarker/-/merge_requests/604 for more details about the change request or history for the current release
