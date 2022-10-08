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

func RegularReplace(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	thirdVector := vectors[2]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	thirdValues := vector.MustStrCols(thirdVector)
	resultType := types.T_varchar.ToType()

	//maxLen
	maxLen := vector.Length(vectors[0])
	for i := range vectors {
		val := vector.Length(vectors[i])
		if val > maxLen {
			maxLen = val
		}
	}

	//optional arguments
	var pos []int64
	var occ []int64
	var match_type []string

	//different parameter length conditions
	switch len(vectors) {
	case 3:
		pos = []int64{1}
		occ = []int64{0}
		match_type = []string{"c"}

	case 4:
		pos = vector.MustTCols[int64](vectors[3])
		occ = []int64{0}
		match_type = []string{"c"}
	case 5:
		pos = vector.MustTCols[int64](vectors[3])
		occ = vector.MustTCols[int64](vectors[4])
		match_type = []string{"c"}
	}

	if firstVector.IsScalarNull() || secondVector.IsScalarNull() || thirdVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, 0, nil)
	if err != nil {
		return nil, err
	}
	err = regular.RegularReplaceWithArrays(firstValues, secondValues, thirdValues, pos, occ, match_type, firstVector.Nsp, secondVector.Nsp, thirdVector.Nsp, resultVector, proc, maxLen)
	if err != nil {
		return nil, err
	}
	return resultVector, nil
}
