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
	"github.com/matrixorigin/matrixone/pkg/vectorize/unixtimestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UnixTimestamp(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	size := types.T(types.T_int64).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: int32(size)}), nil
	}
	times := vector.MustTCols[types.Datetime](inVec)

	if inVec.IsScalar() {
		{
			vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: int32(size)})
			rs := make([]int64, 1)
			nulls.Set(vec.Nsp, inVec.Nsp)
			vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
			return vec, nil
		}
	}
	vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: int32(size)}, int64(len(times))*int64(size))
	if err != nil {
		return nil, err
	}
	rs := make([]int64, len(times))
	for i := 0; i < len(times); i++ {
		if times[i] < 0 {
			nulls.Add(inVec.Nsp, uint64(i))
		}
	}
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
	return vec, nil
}

func UnixTimestampVarchar(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inVec := lv[0]
	size := types.T(types.T_int64).TypeLen()
	if inVec.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: int32(size)}), nil
	}
	times_ := vector.MustBytesCols(inVec)
	var times []types.Datetime
	for i := 0; i < len(times_.Lengths); i++ {
		times = append(times, MustDatetimeMe(string(times_.Get(int64(i)))))
	}
	if inVec.IsScalar() {
		if inVec.IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: int32(size)}), nil
		} else {
			vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: int32(size)})
			rs := make([]int64, 1)
			nulls.Set(vec.Nsp, inVec.Nsp)
			vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
			return vec, nil
		}
	}

	vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: int32(size)}, int64(len(times))*int64(size))
	if err != nil {
		return nil, err
	}
	rs := make([]int64, len(times))
	nulls.Set(vec.Nsp, inVec.Nsp)
	vector.SetCol(vec, unixtimestamp.UnixTimestamp(times, rs))
	return vec, nil
}

func MustDatetimeMe(s string) types.Datetime {
	datetime, err := types.ParseDatetime(s, 6)
	if err != nil {
		panic("bad datetime")
	}
	return datetime
}
