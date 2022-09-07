// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/vectorize/fromunixtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FromUnixTime(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	times := vector.MustTCols[int64](inVec)
	size := types.T(types.T_datetime).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_datetime, Size: int32(size)}), nil
	}

	if inVec.IsScalar() {
		rs := make([]types.Datetime, 1)
		fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)

		vec := vector.NewConstFixed(types.Type{Oid: types.T_datetime, Size: int32(size)}, 1, rs[0])
		if times[0] < 0 || times[0] > 32536771199 {
			nulls.Add(inVec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		return vec, nil
	}

	rs := make([]types.Datetime, len(times))
	fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, times, rs)

	for i := 0; i < len(times); i++ {
		if times[i] < 0 || times[i] > 32536771199 {
			nulls.Add(inVec.Nsp, uint64(i))
		}
	}
	vec := vector.NewWithFixed(types.Type{Oid: types.T_datetime, Size: int32(size)}, rs, nulls.NewWithSize(len(rs)), proc.GetMheap())
	nulls.Set(vec.Nsp, inVec.Nsp)
	return vec, nil
}

func FromUnixTimeUint64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	uint64ToInt64 := func(from []uint64) []int64 {
		to := make([]int64, len(from))
		for i := range from {
			to[i] = int64(from[i])
		}
		return to
	}
	inVec := lv[0]
	times := vector.MustTCols[uint64](inVec)
	size := types.T(types.T_datetime).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_datetime, Size: int32(size)}), nil
	}
	if inVec.IsScalar() {
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_datetime, Size: int32(size)})
		rs := make([]types.Datetime, 1)
		if times[0] > 32536771199 {
			nulls.Add(inVec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		vector.SetCol(vec, fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(types.Type{Oid: types.T_datetime, Size: int32(size)}, int64(len(times))*int64(size))
	if err != nil {
		return nil, err
	}
	rs := make([]types.Datetime, len(times))
	for i := 0; i < len(times); i++ {
		if times[i] > 32536771199 {
			nulls.Add(inVec.Nsp, uint64(i))
		}
	}
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, uint64ToInt64(times), rs))
	return vec, nil
}

func FromUnixTimeFloat64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	float64Toint64 := func(from []float64) []int64 {
		to := make([]int64, len(from))
		for i := range from {
			to[i] = int64(from[i])
		}
		return to
	}
	inVec := lv[0]
	times := vector.MustTCols[float64](inVec)
	size := types.T(types.T_datetime).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_datetime, Size: int32(size)}), nil
	}
	if inVec.IsScalar() {
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_datetime, Size: int32(size)})
		rs := make([]types.Datetime, 1)
		if times[0] < 0 || times[0] > 32536771199 {
			nulls.Add(inVec.Nsp, 0)
		}
		nulls.Set(vec.Nsp, inVec.Nsp)
		vector.SetCol(vec, fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, float64Toint64(times), rs))
		return vec, nil
	}
	vec, err := proc.AllocVector(types.Type{Oid: types.T_datetime, Size: int32(size)}, int64(len(times))*int64(size))
	if err != nil {
		return nil, err
	}
	rs := make([]types.Datetime, len(times))
	for i := 0; i < len(times); i++ {
		if times[i] < 0 || times[i] > 32536771199 {
			nulls.Add(inVec.Nsp, uint64(i))
		}
	}
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, fromunixtime.UnixToDatetime(proc.SessionInfo.TimeZone, float64Toint64(times), rs))
	return vec, nil
}

func MustDatetime(s string) types.Datetime {
	dt, err := types.ParseDatetime(s, 6)
	if err != nil {
		panic("bad datetime")
	}
	return dt
}
