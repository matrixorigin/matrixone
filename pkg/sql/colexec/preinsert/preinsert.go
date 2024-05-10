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

const argName = "preinsert"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": pre processing insert")
}

func (arg *Argument) Prepare(_ *proc) error {
	return nil
}

func (arg *Argument) Call(proc *proc) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}
	analy := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	analy.Start()
	defer analy.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	arg.buf = batch.NewWithSize(len(arg.Attrs))
	// keep shuffleIDX unchanged
	arg.buf.ShuffleIDX = bat.ShuffleIDX
	arg.buf.Attrs = make([]string, 0, len(arg.Attrs))
	for idx := range arg.Attrs {
		arg.buf.Attrs = append(arg.buf.Attrs, arg.Attrs[idx])
		srcVec := bat.Vecs[idx]
		vec := proc.GetVector(*srcVec.GetType())
		if err := vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(vec, srcVec); err != nil {
			vec.Free(proc.Mp())
			return result, err
		}
		arg.buf.SetVector(int32(idx), vec)
	}
	arg.buf.AddRowCount(bat.RowCount())

	if arg.HasAutoCol {
		err := genAutoIncrCol(arg.buf, proc, arg)
		if err != nil {
			return result, err
		}
	}
	// check new rows not null
	err = colexec.BatchDataNotNullCheck(arg.buf, arg.TableDef, proc.Ctx)
	if err != nil {
		return result, err
	}

	// calculate the composite primary key column and append the result vector to batch
	err = genCompositePrimaryKey(arg.buf, proc, arg.TableDef)
	if err != nil {
		return result, err
	}
	err = genClusterBy(arg.buf, proc, arg.TableDef)
	if err != nil {
		return result, err
	}
	if arg.IsUpdate {
		idx := len(bat.Vecs) - 1
		arg.buf.Attrs = append(arg.buf.Attrs, catalog.Row_ID)
		rowIdVec := proc.GetVector(*bat.GetVector(int32(idx)).GetType())
		err = rowIdVec.UnionBatch(bat.Vecs[idx], 0, bat.Vecs[idx].Length(), nil, proc.Mp())
		if err != nil {
			rowIdVec.Free(proc.Mp())
			return result, err
		}
		arg.buf.Vecs = append(arg.buf.Vecs, rowIdVec)
	}

	result.Batch = arg.buf
	return result, nil
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, arg *Argument) error {
	lastInsertValue, err := proc.IncrService.InsertValues(
		proc.Ctx,
		arg.TableDef.TblId,
		bat,
		arg.EstimatedRowCount,
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			logutil.Error("insert auto increment column failed", zap.Error(err))
			return moerr.NewNoSuchTableNoCtx(arg.SchemaName, arg.TableDef.Name)
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
