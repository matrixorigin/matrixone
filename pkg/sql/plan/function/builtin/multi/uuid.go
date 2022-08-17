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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const UUID_LENGTH uint32 = 36

func UUID(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(inputVecs) != 1 {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "uuid function requires a hidden parameter")
	}
	rows := inputVecs[0].Length
	resultType := types.T_varchar.ToType()
	resultVector := vector.New(resultType)

	results := &types.Bytes{
		Data:    make([]byte, int(UUID_LENGTH)*rows),
		Offsets: make([]uint32, rows),
		Lengths: make([]uint32, rows),
	}
	var retCursor uint32 = 0
	for i := 0; i < rows; i++ {
		id, err := uuid.NewUUID()
		if err != nil {
			return nil, moerr.NewError(moerr.INTERNAL_ERROR, "generation uuid error")
		}
		slice := []byte(id.String())
		for _, b := range slice {
			results.Data[retCursor] = b
			retCursor++
		}
		if i != 0 {
			results.Offsets[i] = results.Offsets[i-1] + results.Lengths[i-1]
		} else {
			results.Offsets[i] = uint32(0)
		}
		results.Lengths[i] = UUID_LENGTH
	}
	vector.SetCol(resultVector, results)
	return resultVector, nil
}
