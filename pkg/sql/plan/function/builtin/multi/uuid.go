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
		return nil, moerr.NewInvalidArgNoCtx("uuid function num args", len(inputVecs))
	}
	rows := inputVecs[0].Length()
	results := make([]string, rows)
	for i := 0; i < rows; i++ {
		id, err := uuid.NewUUID()
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("newuuid failed")
		}
		results[i] = id.String()
	}
	resultVector := vector.NewWithStrings(types.T_varchar.ToType(), results, nil, proc.Mp())
	return resultVector, nil
}
