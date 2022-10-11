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
	"github.com/matrixorigin/matrixone/pkg/vectorize/datediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TimeStampDiff(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//input vectors
	firstVector := vectors[0]
	secondVector := vectors[1]
	thirdVector := vectors[2]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustTCols[types.Datetime](secondVector)
	thirdValues := vector.MustTCols[types.Datetime](thirdVector)
	resultType := types.T_int64.ToType()

	//the max Length of all vectors
	maxLen := vector.Length(vectors[0])
	for i := range vectors {
		val := vector.Length(vectors[i])
		if val > maxLen {
			maxLen = val
		}
	}

	if firstVector.IsScalarNull() || secondVector.IsScalarNull() || thirdVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
	if err != nil {
		return nil, err
	}
	resultValues := vector.MustTCols[int64](resultVector)
	err = datediff.TimeStampDiffWithCols(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, thirdVector.Nsp, resultVector.Nsp, resultValues, maxLen)
	if err != nil {
		return nil, err
	}
	return resultVector, nil
}
