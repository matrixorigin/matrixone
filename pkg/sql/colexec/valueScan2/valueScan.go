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
    "fmt"
    "github.com/google/uuid"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/container/batch"
    "github.com/matrixorigin/matrixone/pkg/vm"
    "github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (valueScan *ValueScan) Prepare(proc *process.Process) error {
    return nil
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
    return vm.CallResult{}, nil
}

func (valueScan *ValueScan) getReadOnlyBatchFromProcess(proc *process.Process) (bat *batch.Batch, err error) {
    // if this is a select without source table.
    // for example, select 1.
    if valueScan.RowsetData == nil {
        return batch.EmptyForConstFoldBatch, nil
    }

    // if this is an execute sql for prepared-stmt.
    // for example, execute s1 and s1 is `insert into t select 1.`
    // or
    // this is simple value_scan.
    if bat = proc.GetPrepareBatch(); bat == nil {
        if bat = proc.GetValueScanBatch(uuid.UUID(valueScan.Uuid)); bat == nil {
            return nil, moerr.NewInfo(proc.Ctx, fmt.Sprintf("makeValueScanBatch failed, node id: %s", uuid.UUID(valueScan.Uuid).String()))
        }
    }

    // refer to old makeValueScanBatch.
    // todo: 下周再弄.

    return bat, nil
}

func (ValueScan *ValueScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
    return
}

func (ValueScan *ValueScan) Free(proc *process.Process, pipelineFailed bool, err error) {
    return
}