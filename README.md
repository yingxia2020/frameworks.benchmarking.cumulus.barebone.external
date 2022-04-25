## Test repo for Service Framework external release

### To test, download 3 packages from cumulus s3 bucket and set tarball flags for emon, svr_info and installk8s as below:
python3 pkb.py --trace_vm_groups=worker --trace_allow_benchmark_control --benchmarks=docker_pt --benchmark_config_file=/home/pkb/xytest/sftest/tmp/applications.benchmarking.benchmark.platform-hero-features/build/workload/BoringSSL/logs-aws_boringssl_gated/cumulus-config.yaml --cloud=AWS --emon --collectd --emon_tarball=/home/share/yingxia/packages/sep_private_linux_pkb.tar.bz2 --installk8s_tarball=/home/share/yingxia/packages/installk8scsp.tar.gz  --svrinfo_tarball=/home/share/yingxia/packages/svr_info_internal.tgz

### To test external release inside intel network:
1. Update proxy_ip_list.txt file under the directory of  perfkitbenchmarker/data/proxy_ip_list

2. Use runtime flag, --proxy_cidr_list="134.134.0.0/16,192.55.0.0/16,134.191.0.0/16". If this flag is set, it will overwrite what's inside proxy_ip_list.txt file.

#### Note: If use Intel CSP account, please make sure proxy CIDRs are set either way to avoid warnings from Intel IT.
