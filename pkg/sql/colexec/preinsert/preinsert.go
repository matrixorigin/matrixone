// Copyright 2022 Matrix Origin
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

package preinsert

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"go.uber.org/zap"
)

const opName = "preinsert"

func (preInsert *PreInsert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert")
}

func (preInsert *PreInsert) OpType() vm.OpType {
	return vm.PreInsert
}

func (preInsert *PreInsert) Prepare(_ *proc) error {
	if preInsert.OpAnalyzer == nil {
		preInsert.OpAnalyzer = process.NewAnalyzer(preInsert.GetIdx(), preInsert.IsFirst, preInsert.IsLast, "preinsert")
	} else {
		preInsert.OpAnalyzer.Reset()
	}

	if preInsert.ctr.canFreeVecIdx == nil {
		preInsert.ctr.canFreeVecIdx = make(map[int]bool)
	}
	return nil
}

func (preInsert *PreInsert) initBuf(proc *proc, bat *batch.Batch) (err error) {
	tableDef := preInsert.TableDef
	if preInsert.ctr.buf != nil {
		for i := len(preInsert.Attrs); i < len(preInsert.ctr.buf.Vecs); i++ {
			preInsert.ctr.buf.Vecs[i].Free(proc.Mp())
			preInsert.ctr.buf.SetVector(int32(i), nil)
		}
		preInsert.ctr.buf.Vecs = preInsert.ctr.buf.Vecs[:len(preInsert.Attrs)]
		preInsert.ctr.buf.Attrs = preInsert.ctr.buf.Attrs[:len(preInsert.Attrs)]
		preInsert.ctr.buf.SetRowCount(0)
	} else {
		for idx := range preInsert.Attrs {
			if tableDef.Cols[idx].Typ.AutoIncr {
				preInsert.ctr.canFreeVecIdx[idx] = true
			}
		}
		preInsert.ctr.buf = batch.NewWithSize(len(preInsert.Attrs))
		preInsert.ctr.buf.Attrs = make([]string, 0, len(preInsert.Attrs))
		preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, preInsert.Attrs...)
	}
	// if col is AutoIncr, genAutoIncrCol function may change the vector of this col, so wo should copy the vec from children vec
	// and the other cols of preInsert.Attrs is stable, we just use the vecs of children's vecs, so just preInsert.ctr.buf.SetVector using bat.Vecs
	for idx := range preInsert.Attrs {
		if _, ok := preInsert.ctr.canFreeVecIdx[idx]; ok {
			typ := bat.Vecs[idx].GetType()
			if preInsert.ctr.buf.Vecs[idx] != nil {
				preInsert.ctr.buf.Vecs[idx].CleanOnlyData()
			} else {
				preInsert.ctr.buf.Vecs[idx] = vector.NewVec(*typ)
			}
			if err := vector.GetUnionAllFunction(*typ, proc.Mp())(preInsert.ctr.buf.Vecs[idx], bat.Vecs[idx]); err != nil {
				return err
			}
		} else {
			if bat.Vecs[idx].IsConst() {
				preInsert.ctr.canFreeVecIdx[idx] = true
				//expland const vector
				typ := bat.Vecs[idx].GetType()
				tmpVec := vector.NewVec(*typ)
				if err := vector.GetUnionAllFunction(*typ, proc.Mp())(tmpVec, bat.Vecs[idx]); err != nil {
					return err
				}
				preInsert.ctr.buf.Vecs[idx] = tmpVec
			} else {
				preInsert.ctr.buf.SetVector(int32(idx), bat.Vecs[idx])
			}
		}
	}
	return err
}

func (preInsert *PreInsert) Call(proc *proc) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := preInsert.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result, err := vm.ChildrenCall(preInsert.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	//anal.Input(result.Batch, preInsert.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch
	preInsert.initBuf(proc, bat)

	// keep shuffleIDX unchanged
	preInsert.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	preInsert.ctr.buf.AddRowCount(bat.RowCount())

	if preInsert.HasAutoCol {
		start := time.Now()
		err = genAutoIncrCol(preInsert.ctr.buf, proc, preInsert)
		if err != nil {
			return result, err
		}
		analyzer.ServiceInvokeTime(start)
	}
	// check new rows not null
	err = colexec.BatchDataNotNullCheck(preInsert.ctr.buf, preInsert.TableDef, proc.Ctx)
	if err != nil {
		return result, err
	}

	// calculate the composite primary key column and append the result vector to batch
	err = genCompositePrimaryKey(preInsert.ctr.buf, proc, preInsert.TableDef)
	if err != nil {
		return result, err
	}
	err = genClusterBy(preInsert.ctr.buf, proc, preInsert.TableDef)
	if err != nil {
		return result, err
	}
	if preInsert.IsUpdate {
		idx := len(bat.Vecs) - 1
		preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, catalog.Row_ID)
		rowIdVec := vector.NewVec(*bat.GetVector(int32(idx)).GetType())
		err = rowIdVec.UnionBatch(bat.Vecs[idx], 0, bat.Vecs[idx].Length(), nil, proc.Mp())
		if err != nil {
			rowIdVec.Free(proc.Mp())
			return result, err
		}
		preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, rowIdVec)
	}

	result.Batch = preInsert.ctr.buf
	//anal.Output(result.Batch, preInsert.IsLast)
	analyzer.Output(result.Batch)
	return result, nil
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, preInsert *PreInsert) error {
	lastInsertValue, err := proc.GetIncrService().InsertValues(
		proc.Ctx,
		preInsert.TableDef.TblId,
		bat,
		preInsert.EstimatedRowCount,
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			logutil.Error("insert auto increment column failed", zap.Error(err))
			return moerr.NewNoSuchTableNoCtx(preInsert.SchemaName, preInsert.TableDef.Name)
		}
		return err
	}
	proc.SetLastInsertID(lastInsertValue)
	return nil
}

func genCompositePrimaryKey(bat *batch.Batch, proc *proc, tableDef *pb.TableDef) error {
	// Check whether the composite primary key column is included
	if tableDef.Pkey.CompPkeyCol == nil {
		return nil
	}

	return util.FillCompositeKeyBatch(bat, catalog.CPrimaryKeyColName, tableDef.Pkey.Names, proc)
}

func genClusterBy(bat *batch.Batch, proc *proc, tableDef *pb.TableDef) error {
	if tableDef.ClusterBy == nil {
		return nil
	}
	clusterBy := tableDef.ClusterBy.Name
	if clusterBy == "" || !util.JudgeIsCompositeClusterByColumn(clusterBy) {
		return nil
	}
	return util.FillCompositeClusterByBatch(bat, clusterBy, proc)
}
