// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func MakeVector(typ types.Type, nullable bool, opts ...Options) (vec Vector) {
	switch typ.Oid {
	case types.T_any:
		vec = NewCnTaeVector[any](typ, nullable, opts...)
	case types.T_bool:
		vec = NewCnTaeVector[bool](typ, nullable, opts...)
	case types.T_int8:
		vec = NewCnTaeVector[int8](typ, nullable, opts...)
	case types.T_int16:
		vec = NewCnTaeVector[int16](typ, nullable, opts...)
	case types.T_int32:
		vec = NewCnTaeVector[int32](typ, nullable, opts...)
	case types.T_int64:
		vec = NewCnTaeVector[int64](typ, nullable, opts...)
	case types.T_uint8:
		vec = NewCnTaeVector[uint8](typ, nullable, opts...)
	case types.T_uint16:
		vec = NewCnTaeVector[uint16](typ, nullable, opts...)
	case types.T_uint32:
		vec = NewCnTaeVector[uint32](typ, nullable, opts...)
	case types.T_uint64:
		vec = NewCnTaeVector[uint64](typ, nullable, opts...)
	case types.T_decimal64:
		vec = NewCnTaeVector[types.Decimal64](typ, nullable, opts...)
	case types.T_decimal128:
		vec = NewCnTaeVector[types.Decimal128](typ, nullable, opts...)
	case types.T_uuid:
		vec = NewCnTaeVector[types.Uuid](typ, nullable, opts...)
	case types.T_float32:
		vec = NewCnTaeVector[float32](typ, nullable, opts...)
	case types.T_float64:
		vec = NewCnTaeVector[float64](typ, nullable, opts...)
	case types.T_date:
		vec = NewCnTaeVector[types.Date](typ, nullable, opts...)
	case types.T_timestamp:
		vec = NewCnTaeVector[types.Timestamp](typ, nullable, opts...)
	case types.T_datetime:
		vec = NewCnTaeVector[types.Datetime](typ, nullable, opts...)
	case types.T_time:
		vec = NewCnTaeVector[types.Time](typ, nullable, opts...)
	case types.T_TS:
		vec = NewCnTaeVector[types.TS](typ, nullable, opts...)
	case types.T_Rowid:
		vec = NewCnTaeVector[types.Rowid](typ, nullable, opts...)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
		vec = NewCnTaeVector[[]byte](typ, nullable, opts...)
	default:
		panic("not support")
	}
	return
}

func BuildBatch(
	attrs []string,
	colTypes []types.Type,
	nullables []bool,
	opts Options) *Batch {
	bat := &Batch{
		Attrs:   make([]string, 0, len(attrs)),
		nameidx: make(map[string]int, len(attrs)),
		Vecs:    make([]Vector, 0, len(attrs)),
	}
	for i, attr := range attrs {
		vec := MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(attr, vec)
	}
	return bat
}

func NewEmptyBatch() *Batch {
	return &Batch{
		Attrs:   make([]string, 0),
		Vecs:    make([]Vector, 0),
		nameidx: make(map[string]int),
	}
}
