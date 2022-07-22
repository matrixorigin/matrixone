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

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"

func MakeVector(typ types.Type, nullable bool, opts ...*Options) (vec Vector) {
	switch typ.Oid {
	case types.Type_ANY:
		vec = NewVector[any](typ, nullable, opts...)
	case types.Type_BOOL:
		vec = NewVector[bool](typ, nullable, opts...)
	case types.Type_INT8:
		vec = NewVector[int8](typ, nullable, opts...)
	case types.Type_INT16:
		vec = NewVector[int16](typ, nullable, opts...)
	case types.Type_INT32:
		vec = NewVector[int32](typ, nullable, opts...)
	case types.Type_INT64:
		vec = NewVector[int64](typ, nullable, opts...)
	case types.Type_UINT8:
		vec = NewVector[uint8](typ, nullable, opts...)
	case types.Type_UINT16:
		vec = NewVector[uint16](typ, nullable, opts...)
	case types.Type_UINT32:
		vec = NewVector[uint32](typ, nullable, opts...)
	case types.Type_UINT64:
		vec = NewVector[uint64](typ, nullable, opts...)
	case types.Type_DECIMAL64:
		vec = NewVector[types.Decimal64](typ, nullable, opts...)
	case types.Type_DECIMAL128:
		vec = NewVector[types.Decimal128](typ, nullable, opts...)
	case types.Type_FLOAT32:
		vec = NewVector[float32](typ, nullable, opts...)
	case types.Type_FLOAT64:
		vec = NewVector[float64](typ, nullable, opts...)
	case types.Type_DATE:
		vec = NewVector[types.Date](typ, nullable, opts...)
	case types.Type_TIMESTAMP:
		vec = NewVector[types.Timestamp](typ, nullable, opts...)
	case types.Type_DATETIME:
		vec = NewVector[types.Datetime](typ, nullable, opts...)
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		vec = NewVector[[]byte](typ, nullable, opts...)
	default:
		panic("not support")
	}
	return
}

func BuildBatch(
	attrs []string,
	colTypes []types.Type,
	nullables []bool,
	capacity int) *Batch {
	opts := new(Options)
	opts.Capacity = capacity
	bat := NewBatch()
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
