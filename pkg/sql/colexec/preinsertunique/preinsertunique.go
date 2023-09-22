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

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString("pre processing insert unique key")
}

func (arg *Argument) Prepare(_ *process.Process) error {
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	argument := arg

	analy := proc.GetAnalyze(arg.info.Idx)
	analy.Start()
	defer analy.Stop()

	inputBat := proc.InputBatch()
	result := vm.NewCallResult()
	if inputBat == nil {
		result.Status = vm.ExecStop
		return result, nil
	}

	if inputBat.IsEmpty() {
		proc.PutBatch(inputBat)
		proc.SetInputBatch(batch.EmptyBatch)
		return result, nil
	}
	defer proc.PutBatch(inputBat)

	var vec *vector.Vector
	var bitMap *nulls.Nulls

	uniqueColumnPos := argument.PreInsertCtx.Columns
	pkPos := int(argument.PreInsertCtx.PkColumn)
	// tableDef := argument.PreInsertCtx.TableDef

	var insertUniqueBat *batch.Batch
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	if isUpdate {
		insertUniqueBat = batch.NewWithSize(3)
		insertUniqueBat.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
	} else {
		insertUniqueBat = batch.NewWithSize(2)
		insertUniqueBat.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	colCount := len(uniqueColumnPos)

	if colCount == 1 {
		pos := uniqueColumnPos[indexColPos]
		vec, bitMap = util.CompactSingleIndexCol(inputBat.Vecs[pos], proc)
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range uniqueColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		vec, bitMap = util.SerialWithCompacted(vs, proc)
	}
	insertUniqueBat.SetVector(indexColPos, vec)
	insertUniqueBat.SetRowCount(vec.Length())

	vec = util.CompactPrimaryCol(inputBat.Vecs[pkPos], bitMap, proc)
	insertUniqueBat.SetVector(pkColPos, vec)

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		insertUniqueBat.SetVector(rowIdColPos, proc.GetVector(*inputBat.GetVector(int32(rowIdInBat)).GetType()))
		err := insertUniqueBat.Vecs[rowIdColPos].UnionBatch(inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp())
		if err != nil {
			insertUniqueBat.Clean(proc.GetMPool())
			return result, err
		}
	}
	proc.SetInputBatch(insertUniqueBat)
	return result, nil
}
