// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (update *MultiUpdate) String(buf *bytes.Buffer) {
	buf.WriteString(": " + opName)
}

func (update *MultiUpdate) OpType() vm.OpType {
	return vm.MultiUpdate
}

func (update *MultiUpdate) Prepare(proc *process.Process) error {
	if update.OpAnalyzer == nil {
		update.OpAnalyzer = process.NewAnalyzer(update.GetIdx(), update.IsFirst, update.IsLast, opName)
	} else {
		update.OpAnalyzer.Reset()
	}

	if len(update.ctr.insertBuf) == 0 {
		update.ctr.insertBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}
	if len(update.ctr.deleteBuf) == 0 {
		update.ctr.deleteBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}

	eng := update.Engine
	for _, updateCtx := range update.MultiUpdateCtx {
		ref := updateCtx.ref
		partitionNames := updateCtx.partitionTableNames
		rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
		if err != nil {
			return err
		}
		updateCtx.source = rel
		updateCtx.partitionSources = partitionRels
	}

	return nil
}

func (update *MultiUpdate) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := update.OpAnalyzer
	analyzer.Start()

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
		analyzer.Stop()
	}()

	if update.ToWriteS3 {
		return update.update_s3(proc, analyzer)
	}
	return update.update(proc, analyzer)
}

func (update *MultiUpdate) update_s3(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()
	ctr := &update.ctr

	if ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
			if err != nil {
				return input, err
			}

			if input.Batch == nil {
				ctr.state = vm.Eval
				break
			}

			if input.Batch.IsEmpty() {
				continue
			}
		}
	}

	if ctr.state == vm.Eval {
	}

	return vm.CancelResult, nil
}

func (update *MultiUpdate) update(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}

	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	ctr := &update.ctr

	for i, updateCtx := range update.MultiUpdateCtx {
		// insert rows
		if len(updateCtx.insertCols) > 0 {
			switch updateCtx.tableType {
			case MainTable:
				err = update.insert_main_table(proc, i, input.Batch)
			case UniqueIndexTable:
				err = update.insert_uniuqe_index_table(proc, i, input.Batch)
			case SecondaryIndexTable:
				err = update.insert_secondary_index_table(proc, i, input.Batch)
			}
		}
		if err != nil {
			return vm.CancelResult, err
		}

		// delete rows
		if len(updateCtx.deleteCols) > 0 {
			// init buf
			if ctr.deleteBuf[i] == nil {
				mainPkIdx := updateCtx.deleteCols[1]
				bat := batch.New(false, []string{catalog.Row_ID, "pk"})
				bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
				bat.SetVector(1, vector.NewVec(*input.Batch.Vecs[mainPkIdx].GetType()))
				ctr.deleteBuf[i] = bat
			}
			err = update.delete_table(proc, updateCtx, input.Batch, ctr.deleteBuf[i])
		}
	}

	ctr.affectedRows += uint64(input.Batch.RowCount())
	analyzer.Output(input.Batch)

	return input, nil
}
