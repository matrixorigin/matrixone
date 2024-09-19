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

	if update.ToWriteS3 {
		if update.ctr.s3Writer == nil {
			writer, err := newS3Writer(update)
			if err != nil {
				return err
			}
			update.ctr.s3Writer = writer
		}

		writer := update.ctr.s3Writer
		for _, updateCtx := range writer.updateCtxs {
			ref := updateCtx.ref
			partitionNames := updateCtx.partitionTableNames
			rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
			if err != nil {
				return err
			}
			updateCtx.source = rel
			updateCtx.partitionSources = partitionRels
		}
	} else {
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

			ctr.affectedRows += uint64(input.Batch.RowCount())
			err = ctr.s3Writer.append(proc, input.Batch)
			if err != nil {
				return vm.CancelResult, err
			}
		}
	}

	if ctr.state == vm.Eval {
		err := ctr.s3Writer.flushTailAndWriteToWorkspace(proc, update)
		if err != nil {
			return vm.CancelResult, err
		}
		return vm.NewCallResult(), nil
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

	err = update.updateOneBatch(proc, input.Batch)
	if err != nil {
		return vm.CancelResult, err
	}

	update.ctr.affectedRows += uint64(input.Batch.RowCount())
	analyzer.Output(input.Batch)

	return input, nil
}

func (update *MultiUpdate) updateOneBatch(proc *process.Process, bat *batch.Batch) (err error) {
	ctr := &update.ctr
	for i, updateCtx := range update.MultiUpdateCtx {
		// delete rows
		if len(updateCtx.deleteCols) > 0 {
			// init buf
			if ctr.deleteBuf[i] == nil {
				mainPkIdx := updateCtx.deleteCols[1]
				buf := batch.New(false, []string{catalog.Row_ID, "pk"})
				buf.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
				buf.SetVector(1, vector.NewVec(*bat.Vecs[mainPkIdx].GetType()))
				ctr.deleteBuf[i] = buf
			}
			err = update.delete_table(proc, updateCtx, bat, ctr.deleteBuf[i])
			if err != nil {
				return
			}
		}

		// insert rows
		if len(updateCtx.insertCols) > 0 {
			switch updateCtx.tableType {
			case updateMainTable:
				err = update.insert_main_table(proc, i, bat)
			case updateUniqueIndexTable:
				err = update.insert_uniuqe_index_table(proc, i, bat)
			case updateSecondaryIndexTable:
				err = update.insert_secondary_index_table(proc, i, bat)
			}
			if err != nil {
				return
			}
		}
	}

	return nil
}
