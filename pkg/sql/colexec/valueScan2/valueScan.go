// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package valueScan2

import (
    "github.com/matrixorigin/matrixone/pkg/vm"
    "github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (valueScan *ValueScan) Prepare(proc *process.Process) error {
    return nil
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
    return vm.CallResult{}, nil
}

func (ValueScan *ValueScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
    return
}

func (ValueScan *ValueScan) Free(proc *process.Process, pipelineFailed bool, err error) {
    return
}