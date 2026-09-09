#!/usr/bin/env bash

# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

workspace=$(cd "${1:-.}" && pwd)
tester_dir=${2:-"${workspace}/mo-tester"}
python_bin=${PYTHON_UDF_PYTHON:-python3}

if [[ ! -x "${workspace}/mo-service" ]]; then
  (cd "${workspace}" && make build)
fi

if [[ ! -d "${tester_dir}" ]]; then
  git clone --depth=1 https://github.com/matrixorigin/mo-tester.git "${tester_dir}"
fi

"${python_bin}" -c 'import pyarrow; assert pyarrow.__version__ == "24.0.0", pyarrow.__version__'

SKIP_JSTFU=true "${workspace}/optools/run_bvt.sh" "${workspace}" launch-with-python-udf-worker

cd "${tester_dir}"
./run.sh -n -g -o -p "${workspace}/test/distributed/cases/udf_python" 2>&1
