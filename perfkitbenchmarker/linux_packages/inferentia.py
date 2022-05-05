# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Installs inferentia tools or configure environment that needed on the VM."""


def AptInstall(vm):
  """Installs NEURON runtime on the VM. then restart neuron-rtd service so that make sure it in active status"""
  vm.RemoteCommand(f"""sudo apt-get --reinstall install -y --allow-downgrades --allow-change-held-packages \
                      aws-neuron-runtime=1.5.0.0
                    """)
  vm.RemoteCommand("sudo systemctl restart neuron-rtd")
