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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("pre processing insert unique key")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(idx int, proc *process.Process, arg any, _, _ bool) (bool, error) {
	argument := arg.(*Argument)

	analy := proc.GetAnalyze(idx)
	analy.Start()
	defer analy.Stop()

	inputBat := proc.InputBatch()
	if inputBat == nil {
		return true, nil
	}

	if len(inputBat.Zs) == 0 {
		return false, nil
	}
	defer proc.PutBatch(inputBat)

	var insertUniqueBat *batch.Batch
	var vec *vector.Vector
	var bitMap *nulls.Nulls

	uniqueColumnPos := argument.PreInsertCtx.Columns
	pkPos := argument.PreInsertCtx.PkColumn

	if pkPos == -1 {
		// have no primary key
		insertUniqueBat = batch.NewWithSize(1)
		insertUniqueBat.Attrs = []string{catalog.IndexTableIndexColName}
	} else {
		insertUniqueBat = batch.NewWithSize(2)
		insertUniqueBat.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	colCount := len(uniqueColumnPos)

	if colCount == 1 {
		pos := uniqueColumnPos[0]
		vec, bitMap = util.CompactSingleIndexCol(inputBat.Vecs[pos], proc)
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range uniqueColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		vec, bitMap = util.SerialWithCompacted(vs, proc)
	}
	insertUniqueBat.SetVector(0, vec)
	insertUniqueBat.SetZs(vec.Length(), proc.Mp())

	if pkPos != -1 {
		// have pk, append primary key vector
		vec = util.CompactPrimaryCol(inputBat.Vecs[pkPos], bitMap, proc)
		insertUniqueBat.SetVector(1, vec)
	}
	proc.SetInputBatch(insertUniqueBat)
	return false, nil
}
