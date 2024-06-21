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

package preinsertunique

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	indexColPos int32 = iota
	pkColPos
	rowIdColPos
)

const argName = "pre_insert_unique"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": pre processing insert unique key")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
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

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	inputBat := result.Batch
	var vec *vector.Vector
	var bitMap *nulls.Nulls

	uniqueColumnPos := arg.PreInsertCtx.Columns
	pkPos := int(arg.PreInsertCtx.PkColumn)

	if arg.ctr.buf != nil {
		proc.PutBatch(arg.ctr.buf)
		arg.ctr.buf = nil
	}
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	if isUpdate {
		arg.ctr.buf = batch.NewWithSize(3)
		arg.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
	} else {
		arg.ctr.buf = batch.NewWithSize(2)
		arg.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	colCount := len(uniqueColumnPos)

	if colCount == 1 {
		pos := uniqueColumnPos[indexColPos]
		vec, bitMap, err = util.CompactSingleIndexCol(inputBat.Vecs[pos], proc)
		if err != nil {
			return result, err
		}
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range uniqueColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		vec, bitMap, err = util.SerialWithCompacted(vs, proc, &arg.packers)
		if err != nil {
			return result, err
		}
	}
	arg.ctr.buf.SetVector(indexColPos, vec)
	arg.ctr.buf.SetRowCount(vec.Length())

	vec, err = util.CompactPrimaryCol(inputBat.Vecs[pkPos], bitMap, proc)
	if err != nil {
		return result, err
	}
	arg.ctr.buf.SetVector(pkColPos, vec)

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		arg.ctr.buf.SetVector(rowIdColPos, proc.GetVector(*inputBat.Vecs[rowIdInBat].GetType()))
		if err = arg.ctr.buf.Vecs[rowIdColPos].UnionBatch(inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp()); err != nil {
			return result, err
		}
	}
	result.Batch = arg.ctr.buf
	return result, nil
}
