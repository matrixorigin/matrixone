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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularSubstr(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	resultType := types.T_varchar.ToType()

	//maxLen
	var maxLen int
	if len(firstValues) > len(secondValues) {
		maxLen = len(firstValues)
	} else {
		maxLen = len(secondValues)
	}
	//option parameters
	pos := make([]int64, maxLen)
	occ := make([]int64, maxLen)
	match_type := make([]string, maxLen)

	switch len(vectors) {
	case 2:
		for i := range pos {
			pos[i] = 1
			occ[i] = 1
			match_type[i] = ""
		}
	case 3:
		pos = vector.MustTCols[int64](vectors[2])
		for i := range pos {
			occ[i] = 1
			match_type[i] = ""
		}
	case 4:
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		for i := range pos {
			match_type[i] = ""
		}
	}

	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		resultValues := make([]types.Varlena, 0, 1)
		vector.SetCol(resultVector, resultValues)
		err := regular.RegularSubstrWithArrays(firstValues, secondValues, pos, occ, match_type, firstVector.Nsp, secondVector.Nsp, resultVector, proc)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, 0, nil)
		if err != nil {
			return nil, err
		}
		err = regular.RegularSubstrWithArrays(firstValues, secondValues, pos, occ, match_type, firstVector.Nsp, secondVector.Nsp, resultVector, proc)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	}
}
