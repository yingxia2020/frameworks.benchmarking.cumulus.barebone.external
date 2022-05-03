# Template Generator

The template generator is used to create pkb benchmarks starting from a json file.

## Prerequisites
```
cd perfkitbenchmarker/scripts/generator
sudo pip install -r requirements.txt
```

## How to add a new workload
1. Create a new directory for your workload
2. Copy the sample template.json file to your directory and update it with information of the workload
3. Add the necessary scripts to this directory
```
cd perfkitbenchmarker/scripts/generator
mkdir workloads/<new workload>
cp templates/template.json workloads/<new workload>
```

## How to run
```
cd perfkitbenchmarker/scripts/generator
./run.py -w workloads/<new workload>/workload_template
```

## How to modify an existing workload with overwrite flag
```
cd perfkitbenchmarker/scripts/generator
./run.py -w workloads/<existing workload>/workload_template -o True
```

The `workload_template` variable is the template json file used in the benchmark.

## Examples

### Mpich compile benchmark

#### Single mode 

Generate with: `cd perfkitbenchmarker/scripts/generator && ./run.py -w workloads/mpich_compile/mpich_template.json`

Run on Centos with: `./pkb.py --cloud=AWS  --benchmarks=mpich_compile --machine_type=m5.24xlarge --os_type=centos7`

Run on Ubuntu with: `./pkb.py --cloud=GCP  --benchmarks=mpich_compile --machine_type=n1-standard-4 --os_type=ubuntu1604`

#### Multi node

Generate with: `cd perfkitbenchmarker/scripts/generator && ./run.py -w workloads/mpich_compile_multi_node/mpich_multi_node_template.json`

Run on Centos with: `./pkb.py --cloud=AWS  --benchmarks=mpich_compile_multi_node --machine_type=m5.24xlarge --os_type=centos7`

Run on Ubuntu with: `./pkb.py --cloud=GCP  --benchmarks=mpich_compile_multi_node --machine_type=n1-standard-4 --os_type=ubuntu1604`

### Mysql benchmark

#### Single node 

Generate with: `cd perfkitbenchmarker/scripts/generator &&  ./run.py -w workloads/mysql_workload/mysql_template.json`

Run on Centos with: `./pkb.py --cloud=AWS --benchmarks=intel_mysql --machine_type=m5.24xlarge --os_type=centos7`

Run on Ubuntu with: `./pkb.py --cloud=AWS --benchmarks=intel_mysql --machine_type=m5.24xlarge --os_type=ubuntu1804`

#### Multi node 

Generate with: `cd perfkitbenchmarker/scripts/generator && ./run.py -w workloads/mysql_multi_node_workload/mysql_multi_node_template.json`

Run on Centos with: `./pkb.py --cloud=AWS --benchmarks=intel_mysql_multi_node --machine_type=m5.24xlarge --os_type=centos7`

Run on Ubuntu with: `./pkb.py --cloud=AWS --benchmarks=intel_mysql_multi_node --machine_type=m5.24xlarge --os_type=ubuntu1804`

#### Django benchmark

#### Single node

Generate with: `cd perfkitbenchmarker/scripts/generator && ./run.py -w workloads/django_workload/django_wl_pkb_template.json`

Run on Ubuntu with: `./pkb.py --cloud=AWS --benchmarks=intel_python_django --os_type=ubuntu1804`

Run on baremetal with: `./pkb.py --benchmarks=intel_python_django --os_type=ubuntu1804 --benchmark_config_file=./intel_python_django_config.yaml --http_proxy=http://proxy-chain.intel.com:911 --https_proxy=http://proxy-chain.intel.com:912` . You will also need to specify a benchmark configuration file (`intel_python_django_config.yaml` in this example) with a similar structure and content as below.

```yaml
static_vms:
  - &vm0
    ip_address: x.x.x.x
    user_name: pkb
    ssh_private_key: ~/.ssh/id_rsa
    internal_ip: x.x.x.x
 
intel_python_django:
  vm_groups:
    vm_group1:
      static_vms:
        - *vm0
``` 
