## Test repo for Service Framework external release

### To test, download 3 packages from cumulus s3 bucket and set tarball flags for emon, svr_info and installk8s as below:
python3 pkb.py --trace_vm_groups=worker --trace_allow_benchmark_control --benchmarks=docker_pt --benchmark_config_file=/home/pkb/xytest/sftest/tmp/applications.benchmarking.benchmark.platform-hero-features/build/workload/BoringSSL/logs-aws_boringssl_gated/cumulus-config.yaml --cloud=AWS --emon --collectd --emon_tarball=/home/share/yingxia/packages/sep_private_linux_pkb.tar.bz2 --installk8s_tarball=/home/share/yingxia/packages/installk8scsp.tar.gz  --svrinfo_tarball=/home/share/yingxia/packages/svr_info_internal.tgz
