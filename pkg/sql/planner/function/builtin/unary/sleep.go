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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	rtyp := types.T_uint8.ToType()
	inputs := vs[0]
	if inputs.IsConstNull() || inputs.GetNulls().Any() {
		err = moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains null")
		return
	}
	sleepSlice := vector.MustFixedCol[T](inputs)
	if checkNegative(sleepSlice) {
		err = moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains negative")
		return
	}
	if inputs.IsConst() {
		sleepSeconds := sleepSlice[0]
		sleepNano := time.Nanosecond * time.Duration(sleepSeconds*1e9)
		length := inputs.Length()
		if length == 1 {
			var result uint8
			select {
			case <-time.After(sleepNano):
				result = 0
			case <-proc.Ctx.Done(): //query aborted
				result = 1
			}
			rs = vector.NewConstFixed(rtyp, result, 1, proc.Mp())
			return
		}
		rs, err = proc.AllocVectorOfRows(rtyp, length, nil)
		if err != nil {
			return
		}
		result := vector.MustFixedCol[uint8](rs)
		for i := 0; i < length; i++ {
			select {
			case <-time.After(sleepNano):
				result[i] = 0
			case <-proc.Ctx.Done(): //query aborted
				for ; i < length; i++ {
					result[i] = 1
				}
				return
			}
		}
		return
	}
	rs, err = proc.AllocVectorOfRows(rtyp, len(sleepSlice), inputs.GetNulls())
	if err != nil {
		return
	}
	result := vector.MustFixedCol[uint8](rs)
	for i, sleepSeconds := range sleepSlice {
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

func checkNegative[T number](rs []T) bool {
	for _, v := range rs {
		if v < 0 {
			return true
		}
	}
	return false
}
