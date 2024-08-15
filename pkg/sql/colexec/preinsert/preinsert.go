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
	preInsert.ctr = new(container)
	return nil
}

func (preInsert *PreInsert) Call(proc *proc) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(preInsert.GetIdx(), preInsert.GetParallelIdx(), preInsert.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(preInsert.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, preInsert.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	if preInsert.ctr.buf != nil {
		proc.PutBatch(preInsert.ctr.buf)
		preInsert.ctr.buf = nil
	}

	preInsert.ctr.buf = batch.NewWithSize(len(preInsert.Attrs))
	// keep shuffleIDX unchanged
	preInsert.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	preInsert.ctr.buf.Attrs = make([]string, 0, len(preInsert.Attrs))
	for idx := range preInsert.Attrs {
		preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, preInsert.Attrs[idx])
		srcVec := bat.Vecs[idx]
		vec := proc.GetVector(*srcVec.GetType())
		if err := vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(vec, srcVec); err != nil {
			vec.Free(proc.Mp())
			return result, err
		}
		preInsert.ctr.buf.SetVector(int32(idx), vec)
	}
	preInsert.ctr.buf.AddRowCount(bat.RowCount())

	if preInsert.HasAutoCol {
		err := genAutoIncrCol(preInsert.ctr.buf, proc, preInsert)
		if err != nil {
			return result, err
		}
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
		rowIdVec := proc.GetVector(*bat.GetVector(int32(idx)).GetType())
		err = rowIdVec.UnionBatch(bat.Vecs[idx], 0, bat.Vecs[idx].Length(), nil, proc.Mp())
		if err != nil {
			rowIdVec.Free(proc.Mp())
			return result, err
		}
		preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, rowIdVec)
	}

	result.Batch = preInsert.ctr.buf
	anal.Output(result.Batch, preInsert.IsLast)
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
