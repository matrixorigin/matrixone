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

import "fmt"

const (
	// any family
	T_any = 0

	// numeric/integer family
	T_int8   = 1
	T_int16  = 2
	T_int32  = 3
	T_int64  = 5
	T_uint8  = 6
	T_uint16 = 7
	T_uint32 = 9
	T_uint64 = 10

	// numeric/decimal family - unsigned attribute is deprecated
	T_decimal = 11

	// numeric/float family - unsigned attribute is deprecated
	T_float32 = 12
	T_float64 = 13

	// date family
	T_date     = 15 // 3 byte
	T_datetime = 18 // 8 byte

	// string family
	T_char    = 20
	T_varchar = 21

	// json family
	T_json = 32

	// system family
	T_sel   = 200 //selection
	T_tuple = 201 // immutable, size = 24
)

type T uint8

type Type struct {
	Oid       T
	Size      int32 // e.g. int8.Size = 1, int16.Size = 2, char.Size = 24(SliceHeader size)
	Width     int32
	Precision int32
}

type Bytes struct {
	Data    []byte
	Offsets []uint32
	Lengths []uint32
}

type Date int32

type Datetime int64

type Decimal struct {
}

var Types map[string]T = map[string]T{
	"tinyint":  T_int8,
	"smallint": T_int16,
	"int":      T_int32,
	"integer":  T_int32,
	"bigint":   T_int64,

	"tinyint unsigned":  T_int8,
	"smallint unsigned": T_int16,
	"int unsigned":      T_int32,
	"integer unsigned":  T_int32,
	"bigint unsigned":   T_int64,

	"decimal": T_decimal,

	"float":  T_float32,
	"double": T_float64,

	"date":     T_date,
	"datetime": T_datetime,

	"char":    T_char,
	"varchar": T_varchar,

	"json": T_json,
}

func (t Type) String() string {
	return t.Oid.String()
}

func (a Type) Eq(b Type) bool {
	return a.Oid == b.Oid && a.Size == b.Size && a.Width == b.Width && a.Precision == b.Width
}

func (t T) ToType() Type {
	var typ Type

	typ.Oid = t
	switch t {
	case T_int8:
		typ.Size = 1
	case T_int16:
		typ.Size = 2
	case T_int32:
		typ.Size = 4
	case T_int64:
		typ.Size = 8
	case T_uint8:
		typ.Size = 1
	case T_uint16:
		typ.Size = 2
	case T_uint32:
		typ.Size = 4
	case T_uint64:
		typ.Size = 8
	case T_float32:
		typ.Size = 4
	case T_float64:
		typ.Size = 8
	case T_char:
		typ.Size = 24
	case T_varchar:
		typ.Size = 24
	case T_sel:
		typ.Size = 8
	}
	return typ
}

func (t T) String() string {
	switch t {
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
	case T_decimal:
		return "DECIMAL"
	case T_float32:
		return "FLOAT"
	case T_float64:
		return "DOUBLE"
	case T_date:
		return "DATE"
	case T_datetime:
		return "DATETIME"
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
	}
	return fmt.Sprintf("unexpected type: %d", t)
}
