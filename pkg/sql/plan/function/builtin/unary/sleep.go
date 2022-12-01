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

type number interface {
	uint64 | float64
}

func Sleep[T number](vs []*vector.Vector, proc *process.Process) (rs *vector.Vector, err error) {
	defer func() {
		if err != nil && rs != nil {
			rs.Free(proc.Mp())
		}
	}()
	resultType := types.T_uint8.ToType()
	inputs := vs[0]
	if inputs.IsScalarNull() {
		err = moerr.NewInvalidArg("sleep", "input is null")
		return
	}
	if inputs.IsScalar() {
		sleepSeconds := vector.MustTCols[T](inputs)[0]
		if sleepSeconds < 0 {
			err = moerr.NewInvalidArg("sleep", "input is negative")
			return
		}
		sleepNano := time.Nanosecond * time.Duration(sleepSeconds*1e9)
		rs = proc.AllocScalarVector(resultType)
		result := vector.MustTCols[uint8](rs)
		select {
		case <-time.After(sleepNano):
			result[0] = 0
		case <-proc.Ctx.Done(): //query aborted
			result[0] = 1
		}
		return
	}
	if inputs.Nsp.Any() {
		err = moerr.NewInvalidArg("sleep", "input contains null")
		return
	}
	sleepSlice := vector.MustTCols[T](inputs)
	rs, err = proc.AllocVectorOfRows(resultType, int64(len(sleepSlice)), inputs.Nsp)
	if err != nil {
		return
	}
	result := vector.MustTCols[uint8](rs)
	for i, sleepSeconds := range sleepSlice {
		if sleepSeconds < 0 {
			err = moerr.NewInvalidArg("sleep", "input is negative")
			break
		}
		sleepNano := time.Nanosecond * time.Duration(sleepSeconds*1e9)
		select {
		case <-time.After(sleepNano):
			result[i] = 0
		case <-proc.Ctx.Done(): //query aborted
			for ; i < len(sleepSlice); i++ {
				result[i] = 1
			}
			return
		}
	}
	return
}
