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
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		rs := vector.MustTCols[uint8](ret)
		v := vector.MustTCols[T](vec)[0]
		rs[0] = ascii.IntSingle(int64(v))
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustTCols[T](vec)
	ascii.IntBatch(vs, rs, ret.Nsp)
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
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		rs := vector.MustTCols[uint8](ret)
		v := vector.MustTCols[T](vec)[0]
		rs[0] = ascii.UintSingle(uint64(v))
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustTCols[T](vec)
	ascii.UintBatch(vs, rs, ret.Nsp)
	return
}

func AsciiFloat[T types.Floats](vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
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
		v := vector.MustTCols[T](vec)[0]
		rs[0] = ascii.FloatSingle(float64(v))
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[uint8](ret)
	vs := vector.MustTCols[T](vec)
	ascii.FloatBatch(vs, rs, ret.Nsp)
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
