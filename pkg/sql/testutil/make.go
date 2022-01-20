// Copyright 2021 Matrix Origin
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

/*
	Package testutil implements some function may be used while doing unit-test for
	functions in compute-layer.

	Such as:
	1. init functions for common structure, likes vector, types.Bytes and so on.
	2.
*/

package testutil

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func MakeBytes(strs []string) *types.Bytes {
	ret := &types.Bytes{}
	next := uint32(0)
	for _, s := range strs {
		l := uint32(len(s))
		ret.Data = append(ret.Data, []byte(s)...)
		ret.Lengths = append(ret.Lengths, l)
		ret.Offsets = append(ret.Offsets, next)
		next += l
	}
	return ret
}

func MakeInt8Vector(vs []int8, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int8, Size: 1})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeInt16Vector(vs []int16, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int16, Size: 2})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeInt32Vector(vs []int32, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int32, Size: 4})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeInt64Vector(vs []int64, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeUint8Vector(vs []uint8, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_uint8, Size: 1})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeUint16Vector(vs []uint16, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_uint16, Size: 2})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeUint32Vector(vs []uint32, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_uint32, Size: 4})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeUint64Vector(vs []uint64, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeStringVector(vs []string, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
	ret.Ref = ref
	vector.SetCol(ret, MakeBytes(vs))
	return ret
}

func MakeFloat32Vector(vs []float32, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_float32, Size: 4})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}

func MakeFloat64Vector(vs []float64, ref uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_float64, Size: 8})
	ret.Ref = ref
	vector.SetCol(ret, vs)
	vector.SetLength(ret, len(vs))
	return ret
}
