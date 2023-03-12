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

func AsciiInt[T types.Ints](ivecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := ivecs[0]
	rtyp := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsConstNull() {
		ret = vector.NewConstNull(rtyp, vec.Length(), proc.Mp())
		return
	}
	start := intStartMap[vec.GetType().Oid]
	if vec.IsConst() {
		v := vector.MustFixedCol[T](vec)[0]
		ret = vector.NewConstFixed(rtyp, ascii.IntSingle(int64(v), start), vec.Length(), proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(rtyp, vec.Length(), vec.GetNulls())
	if err != nil {
		return
	}
	rs := vector.MustFixedCol[uint8](ret)
	vs := vector.MustFixedCol[T](vec)
	ascii.IntBatch(vs, start, rs, ret.GetNulls())
	return
}

func AsciiUint[T types.UInts](ivecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := ivecs[0]
	rtyp := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsConstNull() {
		ret = vector.NewConstNull(rtyp, vec.Length(), proc.Mp())
		return
	}
	start := uintStartMap[vec.GetType().Oid]
	if vec.IsConst() {
		v := vector.MustFixedCol[T](vec)[0]
		ret = vector.NewConstFixed(rtyp, ascii.UintSingle(uint64(v), start), vec.Length(), proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(rtyp, vec.Length(), vec.GetNulls())
	if err != nil {
		return
	}
	rs := vector.MustFixedCol[uint8](ret)
	vs := vector.MustFixedCol[T](vec)
	ascii.UintBatch(vs, start, rs, ret.GetNulls())
	return
}

func AsciiString(ivecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	vec := ivecs[0]
	rtyp := types.T_uint8.ToType()
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	if vec.IsConstNull() {
		ret = vector.NewConstNull(rtyp, vec.Length(), proc.Mp())
		return
	}
	if vec.IsConst() {
		v := vector.MustBytesCol(vec)[0]
		ret = vector.NewConstFixed(rtyp, ascii.StringSingle(v), vec.Length(), proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(rtyp, vec.Length(), vec.GetNulls())
	if err != nil {
		return
	}
	rs := vector.MustFixedCol[uint8](ret)
	vs := vector.MustBytesCol(vec)
	ascii.StringBatch(vs, rs, ret.GetNulls())
	return
}
