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

const opName = "pre_insert_unique"

func (preInsertUnique *PreInsertUnique) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert unique key")
}

func (preInsertUnique *PreInsertUnique) OpType() vm.OpType {
	return vm.PreInsertUnique
}

func (preInsertUnique *PreInsertUnique) Prepare(proc *process.Process) error {
	preInsertUnique.ctr = new(container)
	return nil
}

func (preInsertUnique *PreInsertUnique) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(preInsertUnique.GetIdx(), preInsertUnique.GetParallelIdx(), preInsertUnique.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(preInsertUnique.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, preInsertUnique.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	inputBat := result.Batch
	var vec *vector.Vector
	var bitMap *nulls.Nulls

	uniqueColumnPos := preInsertUnique.PreInsertCtx.Columns
	pkPos := int(preInsertUnique.PreInsertCtx.PkColumn)

	if preInsertUnique.ctr.buf != nil {
		proc.PutBatch(preInsertUnique.ctr.buf)
		preInsertUnique.ctr.buf = nil
	}
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	if isUpdate {
		preInsertUnique.ctr.buf = batch.NewWithSize(3)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
	} else {
		preInsertUnique.ctr.buf = batch.NewWithSize(2)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
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
		vec, bitMap, err = util.SerialWithCompacted(vs, proc, &preInsertUnique.packers)
		if err != nil {
			return result, err
		}
	}
	preInsertUnique.ctr.buf.SetVector(indexColPos, vec)
	preInsertUnique.ctr.buf.SetRowCount(vec.Length())

	vec, err = util.CompactPrimaryCol(inputBat.Vecs[pkPos], bitMap, proc)
	if err != nil {
		return result, err
	}
	preInsertUnique.ctr.buf.SetVector(pkColPos, vec)

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		preInsertUnique.ctr.buf.SetVector(rowIdColPos, proc.GetVector(*inputBat.Vecs[rowIdInBat].GetType()))
		if err = preInsertUnique.ctr.buf.Vecs[rowIdColPos].UnionBatch(inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp()); err != nil {
			return result, err
		}
	}
	result.Batch = preInsertUnique.ctr.buf
	anal.Output(result.Batch, preInsertUnique.IsLast)
	return result, nil
}
