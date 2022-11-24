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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

const dayFromUnixEpoch = 719163

func CurDate(vec []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vec) == 0 {
		return curDate(proc)
	}
	return curDateWithArg(vec[0], proc)
}

func curDateWithArg(vec *vector.Vector, proc *process.Process) (resultVec *vector.Vector, err error) {
	defer func() {
		if err != nil && resultVec != nil {
			resultVec.Free(proc.Mp())
		}
	}()
	if !vec.IsScalar() {
		err = moerr.NewNotSupported("curdate with non-scalar argument")
		return
	}
	resultType := types.T_int64.ToType()
	if vec.IsScalarNull() {
		resultVec = proc.AllocScalarNullVector(resultType)
		return
	}
	offset := vector.MustTCols[int64](vec)[0]
	resultVec, err = proc.AllocVectorOfRows(resultType, 1, nil)
	if err != nil {
		return
	}
	loc := proc.SessionInfo.TimeZone
	if loc == nil {
		loc = time.Local
	}
	cur := types.Today(loc)
	resultSlice := vector.MustTCols[int64](resultVec)
	resultSlice[0] = int64(cur) - dayFromUnixEpoch + offset + 1
	return
}

func curDate(proc *process.Process) (resultVec *vector.Vector, err error) {
	defer func() {
		if err != nil && resultVec != nil {
			resultVec.Free(proc.Mp())
		}
	}()
	resultType := types.T_date.ToType()
	resultVec, err = proc.AllocVectorOfRows(resultType, 1, nil)
	if err != nil {
		return
	}
	loc := proc.SessionInfo.TimeZone
	if loc == nil {
		loc = time.Local
	}
	resultSlice := vector.MustTCols[types.Date](resultVec)
	resultSlice[0] = types.Today(loc)
	return
}
