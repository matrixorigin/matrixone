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

func RegularInstr(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	resultType := types.T_int64.ToType()

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
	var opt []uint8
	var match_type []string

	//different parameter length conditions
	switch len(vectors) {
	case 2:
		pos = []int64{1}
		occ = []int64{1}
		opt = []uint8{0}
		match_type = []string{"c"}

	case 3:
		pos = vector.MustTCols[int64](vectors[2])
		occ = []int64{1}
		opt = []uint8{0}
		match_type = []string{"c"}
	case 4:
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		opt = []uint8{0}
		match_type = []string{"c"}
	case 5:
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		opt = vector.MustTCols[uint8](vectors[4])
		match_type = []string{"c"}
	}

	if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[int64](resultVector)
	err = regular.RegularInstrWithArrays(firstValues, secondValues, pos, occ, opt, match_type, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues, maxLen)
	if err != nil {
		return nil, err
	}
	return resultVector, nil

}
