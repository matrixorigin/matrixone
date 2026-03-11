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

func UUID(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(ivecs) != 1 {
		return nil, moerr.NewInvalidArg(proc.Ctx, "uuid function num args", len(ivecs))
	}
	rows := ivecs[0].Length()
	rvec, err := proc.AllocVectorOfRows(types.T_uuid.ToType(), rows, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[types.Uuid](rvec)
	for i := 0; i < rows; i++ {
		val, err := uuid.NewUUID()
		if err != nil {
			rvec.Free(proc.Mp())
			return nil, moerr.NewInternalError(proc.Ctx, "newuuid failed")
		}
		rvals[i] = types.Uuid(val)
	}
	return rvec, nil
}
