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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Cast to binary but no right-padding.
func Binary(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstv := vs[0]
	resultVector, err := proc.AllocVectorOfRows(types.T_binary.ToType(), 0, nil)
	if err != nil {
		return nil, err
	}

	firstss := vector.MustStrCols(firstv)

	// Cast varchar to binary.
	sbytess := make([][]byte, 0)
	for i, s := range firstss {
		//Check nulls.
		if nulls.Contains(firstv.Nsp, uint64(i)) {
			nulls.Add(resultVector.Nsp, uint64(i))
		}
		sbytes := []byte(s)
		// Truncation.
		if len(sbytes) > types.MaxBinaryLen {
			sbytes = sbytes[:256]
		}
		// No right-padding.
		sbytess = append(sbytess, sbytes)
	}
	vector.AppendBytes(resultVector, sbytess, proc.Mp())
	return resultVector, nil
}
