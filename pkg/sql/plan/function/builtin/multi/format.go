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
	if vecs[0].IsConstNull() || vecs[1].IsConstNull() {
		return proc.AllocScalarNullVector(*vecs[0].GetType()), nil
	}

	//get the first arg number
	numberCols := vector.MustStrCols(vecs[0])
	//get the second arg precision
	precisionCols := vector.MustStrCols(vecs[1])
	//get the third arg locale
	var localeCols []string
	if paramNum == 2 || vecs[2].IsConstNull() {
		localeCols = []string{"en_US"}
	} else {
		localeCols = vector.MustStrCols(vecs[2])
	}

	//calcute rows
	rowCount := vecs[0].Length()

	var resultVec *vector.Vector = nil
	resultValues := make([]string, rowCount)
	resultNsp := nulls.NewWithSize(rowCount)

	// set null row
	nulls.Or(vecs[0].GetNulls(), vecs[1].GetNulls(), resultNsp)

	var constVectors []bool
	if paramNum == 2 || vecs[2].IsConstNull() {
		constVectors = []bool{vecs[0].IsConst(), vecs[1].IsConst(), true}
	} else {
		constVectors = []bool{vecs[0].IsConst(), vecs[1].IsConst(), vecs[2].IsConst()}
	}

	//get result values
	err := format.Format(numberCols, precisionCols, localeCols, rowCount, constVectors, resultValues)
	if err != nil {
		return nil, err
	}
	resultVec = vector.New(vector.FLAT, types.T_varchar.ToType())
	vector.AppendStringList(resultVec, resultValues, nil, proc.Mp())

	return resultVec, nil
}
