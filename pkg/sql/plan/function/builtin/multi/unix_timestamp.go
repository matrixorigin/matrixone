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

func UnixTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_int64.ToType()

	if len(ivecs) == 0 {
		rs := make([]int64, 1)
		unixtimestamp.UnixTimestampToInt([]types.Timestamp{types.CurrentTimestamp()}, rs)
		vec := vector.NewConstFixed(rtyp, rs[0], 1, proc.Mp())
		return vec, nil
	}

	inVec := ivecs[0]
	if inVec.IsConstNull() {
		return vector.NewConstNull(types.T_int64.ToType(), inVec.Length(), proc.Mp()), nil
	}
	times := vector.MustFixedCol[types.Timestamp](inVec)

	if inVec.IsConst() {
		var rs [1]int64
		unixtimestamp.UnixTimestampToInt(times, rs[:])
		if rs[0] >= 0 {
			return vector.NewConstFixed(rtyp, rs[0], inVec.Length(), proc.Mp()), nil
		} else {
			return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
		}
	}

	vec, err := proc.AllocVectorOfRows(rtyp, len(times), inVec.GetNulls())
	if err != nil {
		return nil, err
	}
	rs := vector.MustFixedCol[int64](vec)
	for i := 0; i < len(times); i++ {
		// XXX This is simply wrong.  We should raise error.
		if times[i] < 0 {
			nulls.Add(vec.GetNulls(), uint64(i))
		}
	}
	unixtimestamp.UnixTimestampToInt(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.GetNulls(), uint64(i))
		}
	}
	return vec, nil
}

func UnixTimestampVarcharToInt64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	rtyp := types.T_int64.ToType()

	if inVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	if inVec.IsConst() {
		var rs [1]int64
		tms := [1]types.Timestamp{MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(0))}
		unixtimestamp.UnixTimestampToInt(tms[:], rs[:])
		if rs[0] >= 0 {
			return vector.NewConstFixed(rtyp, rs[0], inVec.Length(), proc.Mp()), nil
		} else {
			return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(i))
	}
	vec, err := proc.AllocVectorOfRows(rtyp, vlen, inVec.GetNulls())
	if err != nil {
		return nil, err
	}
	rs := vector.MustFixedCol[int64](vec)
	unixtimestamp.UnixTimestampToInt(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.GetNulls(), uint64(i))
		}
	}
	return vec, nil
}

func UnixTimestampVarcharToFloat64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	rtyp := types.T_float64.ToType()
	if inVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	if inVec.IsConst() {
		var rs [1]float64
		tms := [1]types.Timestamp{MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(0))}
		unixtimestamp.UnixTimestampToFloat(tms[:], rs[:])
		if rs[0] >= 0 {
			vec := vector.NewConstFixed(rtyp, rs[0], inVec.Length(), proc.Mp())
			return vec, nil
		} else {
			return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(i))
	}
	vec, err := proc.AllocVectorOfRows(rtyp, vlen, inVec.GetNulls())
	if err != nil {
		return nil, err
	}
	rs := vector.MustFixedCol[float64](vec)
	unixtimestamp.UnixTimestampToFloat(times, rs)
	for i, r := range rs {
		if r < 0 {
			nulls.Add(vec.GetNulls(), uint64(i))
		}
	}
	return vec, nil
}

var (
	Decimal128Zero = types.Decimal128{B0_63: 0, B64_127: 0}
)

func UnixTimestampVarcharToDecimal128(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := ivecs[0]
	rtyp := types.New(types.T_decimal128, 38, 6)
	if inVec.IsConstNull() {
		return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
	}

	if inVec.IsConst() {
		var rs [1]types.Decimal128
		tms := [1]types.Timestamp{MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(0))}
		unixtimestamp.UnixTimestampToDecimal128(tms[:], rs[:])
		if rs[0].Compare(types.Decimal128{B0_63: 0, B64_127: 0}) > 0 {
			vec := vector.NewConstFixed(rtyp, rs[0], inVec.Length(), proc.Mp())
			return vec, nil
		} else {
			return vector.NewConstNull(rtyp, inVec.Length(), proc.Mp()), nil
		}
	}

	vlen := inVec.Length()
	times := make([]types.Timestamp, vlen)
	for i := 0; i < vlen; i++ {
		times[i] = MustTimestamp(proc.SessionInfo.TimeZone, inVec.GetStringAt(i))
	}
	vec, err := proc.AllocVectorOfRows(rtyp, vlen, inVec.GetNulls())
	if err != nil {
		return nil, err
	}
	rs := vector.MustFixedCol[types.Decimal128](vec)
	unixtimestamp.UnixTimestampToDecimal128(times, rs)
	for i, r := range rs {
		if r.Lt(Decimal128Zero) {
			nulls.Add(vec.GetNulls(), uint64(i))
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
