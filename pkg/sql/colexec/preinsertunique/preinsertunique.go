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
	if preInsertUnique.OpAnalyzer == nil {
		preInsertUnique.OpAnalyzer = process.NewAnalyzer(preInsertUnique.GetIdx(), preInsertUnique.IsFirst, preInsertUnique.IsLast, "pre_insert_unique")
	} else {
		preInsertUnique.OpAnalyzer.Reset()
	}

	return nil
}

func (preInsertUnique *PreInsertUnique) initBuf(bat *batch.Batch, uniqueColumnPos []int32, pkPos int, isUpdate bool) {
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.CleanOnlyData()
		return
	}

	if isUpdate {
		preInsertUnique.ctr.buf = batch.NewWithSize(3)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
		preInsertUnique.ctr.buf.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	} else {
		preInsertUnique.ctr.buf = batch.NewWithSize(2)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	if len(uniqueColumnPos) == 1 {
		preInsertUnique.ctr.buf.Vecs[0] = vector.NewVec(*bat.Vecs[uniqueColumnPos[0]].GetType())
	} else {
		preInsertUnique.ctr.buf.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	}
	preInsertUnique.ctr.buf.Vecs[1] = vector.NewVec(*bat.Vecs[pkPos].GetType())
}

func (preInsertUnique *PreInsertUnique) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := preInsertUnique.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result, err := vm.ChildrenCall(preInsertUnique.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	inputBat := result.Batch
	var bitMap *nulls.Nulls

	uniqueColumnPos := preInsertUnique.PreInsertCtx.Columns
	pkPos := int(preInsertUnique.PreInsertCtx.PkColumn)
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	preInsertUnique.initBuf(inputBat, uniqueColumnPos, pkPos, isUpdate)

	colCount := len(uniqueColumnPos)

	if colCount == 1 {
		pos := uniqueColumnPos[indexColPos]
		bitMap, err = util.CompactSingleIndexCol(inputBat.Vecs[pos], preInsertUnique.ctr.buf.Vecs[indexColPos], proc)
		if err != nil {
			return result, err
		}
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range uniqueColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		bitMap, err = util.SerialWithCompacted(vs, preInsertUnique.ctr.buf.Vecs[indexColPos], proc, &preInsertUnique.packers)
		if err != nil {
			return result, err
		}
	}
	preInsertUnique.ctr.buf.SetRowCount(preInsertUnique.ctr.buf.Vecs[0].Length())

	if err = util.CompactPrimaryCol(inputBat.Vecs[pkPos], preInsertUnique.ctr.buf.Vecs[pkColPos], bitMap, proc); err != nil {
		return result, err
	}

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		if err = preInsertUnique.ctr.buf.Vecs[rowIdColPos].UnionBatch(inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp()); err != nil {
			return result, err
		}
	}
	result.Batch = preInsertUnique.ctr.buf
	analyzer.Output(result.Batch)
	return result, nil
}
