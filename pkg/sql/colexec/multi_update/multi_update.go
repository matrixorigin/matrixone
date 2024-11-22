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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
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

	if update.ctr.updateCtxInfos == nil {
		update.ctr.updateCtxInfos = make(map[string]*updateCtxInfo)
		for _, updateCtx := range update.MultiUpdateCtx {
			info := new(updateCtxInfo)
			for _, col := range updateCtx.TableDef.Cols {
				if col.Name != catalog.Row_ID {
					info.insertAttrs = append(info.insertAttrs, col.Name)
				}
			}

			tableType := UpdateMainTable
			if strings.HasPrefix(updateCtx.TableDef.Name, catalog.UniqueIndexTableNamePrefix) {
				tableType = UpdateUniqueIndexTable
			} else if strings.HasPrefix(updateCtx.TableDef.Name, catalog.SecondaryIndexTableNamePrefix) {
				tableType = UpdateSecondaryIndexTable
			}
			info.tableType = tableType
			update.ctr.updateCtxInfos[updateCtx.TableDef.Name] = info
		}
	}

	for _, updateCtx := range update.MultiUpdateCtx {
		info := update.ctr.updateCtxInfos[updateCtx.TableDef.Name]
		info.Sources = nil
		if update.Action != UpdateWriteS3 {
			rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, update.Engine, updateCtx.ObjRef, updateCtx.PartitionTableNames)
			if err != nil {
				return err
			}
			if len(updateCtx.PartitionTableNames) > 0 {
				info.Sources = append(info.Sources, partitionRels...)
			} else {
				info.Sources = append(info.Sources, rel)
			}
		}
	}

	if len(update.ctr.insertBuf) == 0 {
		update.ctr.insertBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}
	if len(update.ctr.deleteBuf) == 0 {
		update.ctr.deleteBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}
	update.ctr.affectedRows = 0

	switch update.Action {
	case UpdateWriteS3:
		if update.ctr.s3Writer == nil {
			writer, err := newS3Writer(update)
			if err != nil {
				return err
			}
			update.ctr.s3Writer = writer
		}

	case UpdateFlushS3Info:
		//resort updateCtxs
		writer, err := newS3Writer(update)
		if err != nil {
			return err
		}

		update.MultiUpdateCtx = writer.updateCtxs

		err = writer.free(proc)
		if err != nil {
			return err
		}
		writer.updateCtxs = nil

	case UpdateWriteTable:
		//do nothing
	}

	mainCtx := update.MultiUpdateCtx[0]
	if len(mainCtx.DeleteCols) > 0 && len(mainCtx.InsertCols) > 0 {
		update.ctr.action = actionUpdate
	} else if len(mainCtx.InsertCols) > 0 {
		update.ctr.action = actionInsert
	} else {
		update.ctr.action = actionDelete
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

	switch update.Action {
	case UpdateWriteS3:
		return update.update_s3(proc, analyzer)
	case UpdateWriteTable:
		return update.update(proc, analyzer)
	case UpdateFlushS3Info:
		return update.updateFlushS3Info(proc, analyzer)
	default:
	}

	panic(fmt.Sprintf("unexpected multi_update.UpdateAction: %#v", update.Action))
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

			err = ctr.s3Writer.append(proc, analyzer, input.Batch)
			if err != nil {
				return vm.CancelResult, err
			}
		}
	}

	if ctr.state == vm.Eval {
		ctr.state = vm.End
		err := ctr.s3Writer.flushTailAndWriteToOutput(proc, analyzer)
		if err != nil {
			return vm.CancelResult, err
		}
		if ctr.s3Writer.outputBat.RowCount() == 0 {
			return vm.CancelResult, err
		}
		result := vm.NewCallResult()
		result.Batch = ctr.s3Writer.outputBat
		analyzer.Output(result.Batch)
		return result, nil
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

	err = update.updateOneBatch(proc, analyzer, input.Batch)
	if err != nil {
		return vm.CancelResult, err
	}

	analyzer.Output(input.Batch)

	return input, nil
}

func (update *MultiUpdate) updateFlushS3Info(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}

	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	actions := vector.MustFixedColNoTypeCheck[uint8](input.Batch.Vecs[0])
	updateCtxIdx := vector.MustFixedColNoTypeCheck[uint16](input.Batch.Vecs[1])
	partitionIdx := vector.MustFixedColNoTypeCheck[uint16](input.Batch.Vecs[2])
	rowCounts := vector.MustFixedColNoTypeCheck[uint64](input.Batch.Vecs[3])
	nameData, nameArea := vector.MustVarlenaRawData(input.Batch.Vecs[4])
	batData, batArea := vector.MustVarlenaRawData(input.Batch.Vecs[5])

	ctx := proc.Ctx
	batBufs := make(map[actionType]*batch.Batch)
	defer func() {
		for _, bat := range batBufs {
			bat.Clean(proc.Mp())
		}
	}()

	for i, action := range actions {
		updateCtx := update.MultiUpdateCtx[updateCtxIdx[i]]

		switch actionType(action) {
		case actionDelete:
			if batBufs[actionDelete] == nil {
				batBufs[actionDelete] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionDelete].CleanOnlyData()
			}
			if err := batBufs[actionDelete].UnmarshalBinary(batData[i].GetByteSlice(batArea)); err != nil {
				return input, err
			}
			tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
			update.addDeleteAffectRows(tableType, rowCounts[i])
			name := nameData[i].UnsafeGetString(nameArea)
			source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[partitionIdx[i]]

			crs := analyzer.GetOpCounterSet()
			newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
			err = source.Delete(newCtx, batBufs[actionDelete], name)
			if err != nil {
				return input, err
			}
			analyzer.AddDeletedRows(int64(batBufs[actionDelete].RowCount()))
			analyzer.AddS3RequestCount(crs)
			analyzer.AddDiskIO(crs)

		case actionInsert:
			if batBufs[actionInsert] == nil {
				batBufs[actionInsert] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionInsert].CleanOnlyData()
			}
			if err := batBufs[actionInsert].UnmarshalBinary(batData[i].GetByteSlice(batArea)); err != nil {
				return input, err
			}

			tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
			update.addInsertAffectRows(tableType, rowCounts[i])
			source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[partitionIdx[i]]

			crs := analyzer.GetOpCounterSet()
			newCtx := perfcounter.AttachS3RequestKey(ctx, crs)
			err = source.Write(newCtx, batBufs[actionInsert])
			if err != nil {
				return input, err
			}
			analyzer.AddWrittenRows(int64(batBufs[actionInsert].RowCount()))
			analyzer.AddS3RequestCount(crs)
			analyzer.AddDiskIO(crs)

		case actionUpdate:
			if batBufs[actionUpdate] == nil {
				batBufs[actionUpdate] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionUpdate].CleanOnlyData()
			}
			if err := batBufs[actionUpdate].UnmarshalBinary(batData[i].GetByteSlice(batArea)); err != nil {
				return input, err
			}

			err = update.updateOneBatch(proc, analyzer, batBufs[actionUpdate])
		default:
			panic("unexpected multi_update.actionType")
		}

		if err != nil {
			return vm.CancelResult, err
		}
	}

	return input, nil
}

func (update *MultiUpdate) updateOneBatch(proc *process.Process, analyzer process.Analyzer, bat *batch.Batch) (err error) {
	for i, updateCtx := range update.MultiUpdateCtx {
		// delete rows
		if len(updateCtx.DeleteCols) > 0 {
			err = update.delete_table(proc, analyzer, updateCtx, bat, i)
			if err != nil {
				return
			}
		}

		// insert rows
		if len(updateCtx.InsertCols) > 0 {
			tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
			switch tableType {
			case UpdateMainTable:
				err = update.insert_main_table(proc, analyzer, i, bat)
			case UpdateUniqueIndexTable:
				err = update.insert_uniuqe_index_table(proc, analyzer, i, bat)
			case UpdateSecondaryIndexTable:
				err = update.insert_secondary_index_table(proc, analyzer, i, bat)
			}
			if err != nil {
				return
			}
		}
	}

	return nil
}
