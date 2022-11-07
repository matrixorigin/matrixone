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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/unixtimestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UnixTimestamp(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(lv) == 0 {
		rs := make([]int64, 1)
		unixtimestamp.UnixTimestampToInt([]types.Timestamp{types.CurrentTimestamp()}, rs)
		return vector.NewConstFixed(types.T_int64.ToType(), 1, rs[0], proc.Mp()), nil
	}

	inVec := lv[0]
	size := types.T(types.T_int64).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: int32(size)}), nil
	}
	times := vector.MustTCols[types.Timestamp](inVec)

	if inVec.IsScalar() {
		rs := make([]int64, 1)
		unixtimestamp.UnixTimestampToInt(times, rs)
		if rs[0] >= 0 {
			return vector.NewConstFixed(types.T_int64.ToType(), inVec.Length(), rs[0], proc.Mp()), nil
		} else {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: int32(size)}), nil
		}
	}

	vec, err := proc.AllocVectorOfRows(types.T_int64.ToType(), int64(len(times)), inVec.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	for i := 0; i < len(times); i++ {
		// XXX This is simply wrong.  We should raise error.
		if times[i] < 0 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	unixtimestamp.UnixTimestampToInt(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	return vec, nil
}

func UnixTimestampVarcharToInt64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	resultType := types.T_int64.ToType()

	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	if inVec.IsScalar() {
		tms := make([]types.Timestamp, 1)
		rs := make([]int64, 1)
		tms[0] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(0))
		unixtimestamp.UnixTimestampToInt(tms, rs)
		if rs[0] >= 0 {
			return vector.NewConstFixed(resultType, inVec.Length(), rs[0], proc.Mp()), nil
		} else {
			return proc.AllocScalarNullVector(resultType), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(int64(i)))
	}
	vec, err := proc.AllocVectorOfRows(resultType, int64(vlen), inVec.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[int64](vec)
	unixtimestamp.UnixTimestampToInt(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	return vec, nil
}

func UnixTimestampVarcharToFloat64(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	resultType := types.T_float64.ToType()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	if inVec.IsScalar() {
		tms := make([]types.Timestamp, 1)
		rs := make([]float64, 1)
		tms[0] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(0))
		unixtimestamp.UnixTimestampToFloat(tms, rs)
		if rs[0] >= 0 {
			return vector.NewConstFixed(resultType, inVec.Length(), rs[0], proc.Mp()), nil
		} else {
			return proc.AllocScalarNullVector(resultType), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(int64(i)))
	}
	vec, err := proc.AllocVectorOfRows(resultType, int64(vlen), inVec.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[float64](vec)
	unixtimestamp.UnixTimestampToFloat(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	return vec, nil
}

var (
	Decimal128Zero = types.Decimal128_FromInt32(0)
)

func UnixTimestampVarcharToDecimal128(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	resultType := types.Type{Oid: types.T_decimal128, Size: 16, Scale: 6}
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	if inVec.IsScalar() {
		tms := make([]types.Timestamp, 1)
		rs := make([]types.Decimal128, 1)
		tms[0] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(0))
		unixtimestamp.UnixTimestampToDecimal128(tms, rs)
		if rs[0].Ge(Decimal128Zero) {
			return vector.NewConstFixed(resultType, inVec.Length(), rs[0], proc.Mp()), nil
		} else {
			return proc.AllocScalarNullVector(resultType), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetString(int64(i)))
	}
	vec, err := proc.AllocVectorOfRows(resultType, int64(vlen), inVec.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[types.Decimal128](vec)
	unixtimestamp.UnixTimestampToDecimal128(times, rs)
	for i, r := range rs {
		if r.Lt(Decimal128Zero) {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	return vec, nil
}

func MustTimestamp(loc *time.Location, s string) types.Timestamp {
	ts, err := types.ParseTimestamp(loc, s, 6)
	if err != nil {
		ts = 0
	}
	return ts
}
