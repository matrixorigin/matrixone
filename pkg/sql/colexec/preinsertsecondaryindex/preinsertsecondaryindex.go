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

package preinsertsecondaryindex

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

const opName = "pre_insert_secondary_index"

func (preInsertSecIdx *PreInsertSecIdx) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert secondary key")
}

func (preInsertSecIdx *PreInsertSecIdx) OpType() vm.OpType {
	return vm.PreInsertSecondaryIndex
}

func (preInsertSecIdx *PreInsertSecIdx) Prepare(proc *process.Process) error {
	return nil
}

func (preInsertSecIdx *PreInsertSecIdx) initBuf(bat *batch.Batch, secondaryColumnPos []int32, pkPos int, isUpdate bool) {
	if preInsertSecIdx.ctr.buf != nil {
		preInsertSecIdx.ctr.buf.CleanOnlyData()
		return
	}

	if isUpdate {
		preInsertSecIdx.ctr.buf = batch.NewWithSize(3)
		preInsertSecIdx.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
		preInsertSecIdx.ctr.buf.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	} else {
		preInsertSecIdx.ctr.buf = batch.NewWithSize(2)
		preInsertSecIdx.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	if len(secondaryColumnPos) == 1 {
		preInsertSecIdx.ctr.buf.Vecs[0] = vector.NewVec(*bat.Vecs[secondaryColumnPos[0]].GetType())
	} else {
		preInsertSecIdx.ctr.buf.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	}
	preInsertSecIdx.ctr.buf.Vecs[1] = vector.NewVec(*bat.Vecs[pkPos].GetType())
}

func (preInsertSecIdx *PreInsertSecIdx) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(preInsertSecIdx.GetIdx(), preInsertSecIdx.GetParallelIdx(), preInsertSecIdx.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(preInsertSecIdx.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, preInsertSecIdx.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	inputBat := result.Batch
	var bitMap *nulls.Nulls

	secondaryColumnPos := preInsertSecIdx.PreInsertCtx.Columns
	pkPos := int(preInsertSecIdx.PreInsertCtx.PkColumn)
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	preInsertSecIdx.initBuf(inputBat, secondaryColumnPos, pkPos, isUpdate)

	colCount := len(secondaryColumnPos)

	// colCount = 1 means the user chose SK as only PK.
	// colCount >= 2 is more common.
	if colCount == 1 {
		pos := secondaryColumnPos[indexColPos]
		bitMap, err = util.CompactSingleIndexCol(inputBat.Vecs[pos], preInsertSecIdx.ctr.buf.Vecs[indexColPos], proc)
		if err != nil {
			return result, err
		}
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range secondaryColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		bitMap, err = util.SerialWithoutCompacted(vs, preInsertSecIdx.ctr.buf.Vecs[indexColPos], proc, &preInsertSecIdx.packer)
		if err != nil {
			return result, err
		}
	}
	preInsertSecIdx.ctr.buf.SetRowCount(preInsertSecIdx.ctr.buf.Vecs[indexColPos].Length())

	if err = util.CompactPrimaryCol(inputBat.Vecs[pkPos], preInsertSecIdx.ctr.buf.Vecs[pkColPos], bitMap, proc); err != nil {
		return result, err
	}

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		if err = preInsertSecIdx.ctr.buf.Vecs[rowIdColPos].UnionBatch(inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp()); err != nil {
			return result, err
		}
	}

	result.Batch = preInsertSecIdx.ctr.buf
	anal.Output(result.Batch, preInsertSecIdx.IsLast)
	return result, nil
}
