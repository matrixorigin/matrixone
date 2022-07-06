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

package types

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type T uint8

const (
	// any family
	T_any T = T(plan.Type_ANY)

	// bool family
	T_bool T = T(plan.Type_BOOL)

	// numeric/integer family
	T_int8   T = T(plan.Type_INT8)
	T_int16  T = T(plan.Type_INT16)
	T_int32  T = T(plan.Type_INT32)
	T_int64  T = T(plan.Type_INT64)
	T_uint8  T = T(plan.Type_UINT8)
	T_uint16 T = T(plan.Type_UINT16)
	T_uint32 T = T(plan.Type_UINT32)
	T_uint64 T = T(plan.Type_UINT64)

	// numeric/float family - unsigned attribute is deprecated
	T_float32 T = T(plan.Type_FLOAT32)
	T_float64 T = T(plan.Type_FLOAT64)

	// date family
	T_date      T = T(plan.Type_DATE)
	T_datetime  T = T(plan.Type_DATETIME)
	T_timestamp T = T(plan.Type_TIMESTAMP)
	T_interval  T = T(plan.Type_INTERVAL)
	T_time      T = T(plan.Type_TIME)

	// string family
	T_char    T = T(plan.Type_CHAR)
	T_varchar T = T(plan.Type_VARCHAR)

	// json family
	T_json T = T(plan.Type_JSON)

	// numeric/decimal family - unsigned attribute is deprecated
	T_decimal64  = T(plan.Type_DECIMAL64)
	T_decimal128 = T(plan.Type_DECIMAL128)

	// system family
	T_sel   T = T(plan.Type_SEL)   //selection
	T_tuple T = T(plan.Type_TUPLE) // immutable, size = 24
)

type Element interface {
	Size() int // return the size of space  the Element need
}

type Type struct {
	Oid  T     `json:"oid,string"`
	Size int32 `json:"size,string"` // e.g. int8.Size = 1, int16.Size = 2, char.Size = 24(SliceHeader size)

	// Width means max Display width for float and double, char and varchar // todo: need to add new attribute DisplayWidth ?
	Width int32 `json:"width,string"`

	Scale int32 `json:"Scale,string"`

	Precision int32 `json:"Precision,string"`
}

type Bool bool
type Int8 int8
type Int16 int16
type Int32 int32
type Int64 int64
type UInt8 uint8
type UInt16 uint16
type UInt32 uint32
type UInt64 uint64
type Float32 float32
type Float64 float64

type Date int32
type Datetime int64
type Timestamp int64

type Decimal64 int64
type Decimal128 struct {
	Lo int64
	Hi int64
}

type String []byte

type Ints interface {
	Int8 | Int16 | Int32 | Int64
}

type UInts interface {
	UInt8 | UInt16 | UInt32 | UInt64
}

type Floats interface {
	Float32 | Float64
}

type Decimal interface {
	Decimal64 | Decimal128
}

type Number interface {
	Ints | UInts | Floats | Decimal
}

type Generic interface {
	Ints | UInts | Floats | Date | Datetime | Timestamp | Decimal64
}

type All interface {
	Bool | Ints | UInts | Floats | Date | Datetime | Timestamp | Decimal | String
}

func New(oid T, width, scale, precision int32) Type {
	return Type{
		Oid:       oid,
		Width:     width,
		Scale:     scale,
		Precision: precision,
		Size:      int32(TypeSize(oid)),
	}
}

func TypeSize(oid T) int {
	switch oid {
	case T_bool:
		return 1
	case T_int8, T_uint8:
		return 1
	case T_int16, T_uint16:
		return 2
	case T_int32, T_uint32, T_float32, T_date:
		return 4
	case T_int64, T_uint64, T_float64, T_datetime, T_timestamp, T_decimal64:
		return 8
	case T_char, T_varchar:
		return 24
	case T_decimal128:
		return 16
	}
	panic(moerr.NewInternalError("Unknow type %s", oid))
}

func (t Type) TypeSize() int {
	return TypeSize(t.Oid)
}

func (t Type) String() string {
	return t.Oid.String()
}

func (t T) String() string {
	switch t {
	case T_any:
		return "ANY"
	case T_bool:
		return "BOOL"
	case T_int8:
		return "TINYINT"
	case T_int16:
		return "SMALLINT"
	case T_int32:
		return "INT"
	case T_int64:
		return "BIGINT"
	case T_uint8:
		return "TINYINT UNSIGNED"
	case T_uint16:
		return "SMALLINT UNSIGNED"
	case T_uint32:
		return "INT UNSIGNED"
	case T_uint64:
		return "BIGINT UNSIGNED"
	case T_float32:
		return "FLOAT"
	case T_float64:
		return "DOUBLE"
	case T_date:
		return "DATE"
	case T_datetime:
		return "DATETIME"
	case T_timestamp:
		return "TIMESTAMP"
	case T_char:
		return "CHAR"
	case T_varchar:
		return "VARCHAR"
	case T_json:
		return "JSON"
	case T_sel:
		return "SEL"
	case T_tuple:
		return "TUPLE"
	case T_decimal64:
		return "DECIMAL64"
	case T_decimal128:
		return "DECIMAL128"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}
