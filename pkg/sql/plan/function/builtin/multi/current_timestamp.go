// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// a general round method is needed for timestamp fsp
func CurrentTimestamp(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultPrecision := int32(6)
	if len(vectors) == 1 {
		resultPrecision = int32(vector.MustTCols[int64](vectors[0])[0])
	}
	resultType := types.Type{Oid: types.T_timestamp, Size: 8, Precision: resultPrecision}
	resultVector := vector.NewConst(resultType, 1)
	result := make([]types.Timestamp, 1)
	result[0] = types.UnixNanoToTimestamp(proc.UnixTime)
	vector.SetCol(resultVector, result)
	return resultVector, nil
}
