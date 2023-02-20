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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ascii"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	intStartMap = map[types.T]int{
		types.T_int8:  3,
		types.T_int16: 2,
		types.T_int32: 1,
		types.T_int64: 0,
	}
	uintStartMap = map[types.T]int{
		types.T_uint8:  3,
		types.T_uint16: 2,
		types.T_uint32: 1,
		types.T_uint64: 0,
	}
)

func AsciiInt[T types.Ints](vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := vecs[0]
	resultType := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsScalarNull() {
		ret = proc.AllocScalarNullVector(resultType)
		return
	}
	start := intStartMap[vec.Typ.Oid]
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		rs := vector.MustTCols[uint8](ret)
		v := vector.MustTCols[T](vec)[0]
		rs[0] = ascii.IntSingle(int64(v), start)
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustTCols[T](vec)
	ascii.IntBatch(vs, start, rs, ret.Nsp)
	return
}

func AsciiUint[T types.UInts](vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := vecs[0]
	resultType := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsScalarNull() {
		ret = proc.AllocScalarNullVector(resultType)
		return
	}
	start := uintStartMap[vec.Typ.Oid]
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		rs := vector.MustTCols[uint8](ret)
		v := vector.MustTCols[T](vec)[0]
		rs[0] = ascii.UintSingle(uint64(v), start)
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustTCols[T](vec)
	ascii.UintBatch(vs, start, rs, ret.Nsp)
	return
}

func AsciiString(vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := vecs[0]
	resultType := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsScalarNull() {
		ret = proc.AllocScalarNullVector(resultType)
		return
	}
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		rs := vector.MustTCols[uint8](ret)
		v := vector.MustBytesCols(vec)[0]
		rs[0] = ascii.StringSingle(v)
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustBytesCols(vec)
	ascii.StringBatch(vs, rs, ret.Nsp)
	return
}
