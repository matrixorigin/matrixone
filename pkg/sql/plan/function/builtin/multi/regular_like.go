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
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularLike(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	resultType := types.T_bool.ToType()

	var thirdNsp *nulls.Nulls
	thirdNsp = nil
	//maxLen
	maxLen := vector.Length(vectors[0])
	for i := range vectors {
		val := vector.Length(vectors[i])
		if val > maxLen {
			maxLen = val
		}
	}

	//optional arguments
	var match_type []string

	//different parameter length conditions
	switch len(vectors) {
	case 2:
		match_type = []string{"c"}
	case 3:
		match_type = vector.MustStrCols(vectors[2])
		if vectors[2].IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		thirdNsp = vectors[2].Nsp
	}

	if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[bool](resultVector)
	err = regular.RegularLikeWithArrays(firstValues, secondValues, match_type, firstVector.Nsp, secondVector.Nsp, thirdNsp, resultVector.Nsp, resultValues, maxLen)
	if err != nil {
		return nil, err
	}
	return resultVector, nil
}
