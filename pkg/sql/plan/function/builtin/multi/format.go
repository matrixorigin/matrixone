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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Format(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(vecs)
	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() {
		return proc.AllocScalarNullVector(vecs[0].Typ), nil
	}

	//get the first arg number
	numberCols := vector.MustStrCols(vecs[0])
	//get the second arg precision
	precisionCols := vector.MustStrCols(vecs[1])
	//get the third arg locale
	var localeCols []string
	if paramNum == 2 || vecs[2].IsScalarNull() {
		localeCols = []string{"en_US"}
	} else {
		localeCols = vector.MustStrCols(vecs[2])
	}

	//calcute rows
	rowCount := vector.Length(vecs[0])

	var resultVec *vector.Vector = nil
	resultValues := make([]string, rowCount)
	resultNsp := nulls.NewWithSize(rowCount)

	// set null row
	nulls.Or(vecs[0].Nsp, vecs[1].Nsp, resultNsp)

	var constVectors []bool
	if paramNum == 2 || vecs[2].IsScalarNull() {
		constVectors = []bool{vecs[0].IsScalar(), vecs[1].IsScalar(), true}
	} else {
		constVectors = []bool{vecs[0].IsScalar(), vecs[1].IsScalar(), vecs[2].IsScalar()}
	}

	//get result values
	err := format.Format(numberCols, precisionCols, localeCols, rowCount, constVectors, resultValues)
	if err != nil {
		return nil, err
	}
	resultVec = vector.NewWithStrings(types.T_varchar.ToType(), resultValues, resultNsp, proc.Mp())

	return resultVec, nil
}
