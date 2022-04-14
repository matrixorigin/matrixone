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
	"strings"
)

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

	// numeric/decimal family - unsigned attribute is deprecated
	T_decimal64  = 40
	T_decimal128 = 41

	// system family
	T_sel   = 200 //selection
	T_tuple = 201 // immutable, size = 24
)

type T uint8

type Type struct {
	Oid  T     `json:"oid,string"`
	Size int32 `json:"size,string"` // e.g. int8.Size = 1, int16.Size = 2, char.Size = 24(SliceHeader size)

	// Width means max Display width for float and double, char and varchar // todo: need to add new attribute DisplayWidth ?
	Width int32 `json:"width,string"`

	Scale int32 `json:"Scale,string"`
}

type Bytes struct {
	Data    []byte
	Offsets []uint32
	Lengths []uint32
}

type Date int32

type Datetime int64

type Decimal64 int64
type Decimal128 struct {
	lo int64
	hi int64
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

	"decimal64":  T_decimal64,
	"decimal128": T_decimal128,

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
	return a.Oid == b.Oid && a.Size == b.Size && a.Width == b.Width && a.Scale == b.Scale
}

func (t T) ToType() Type {
	var typ Type

	typ.Oid = t
	switch t {
	case T_int8:
		typ.Size = 1
	case T_int16:
		typ.Size = 2
	case T_int32, T_date:
		typ.Size = 4
	case T_int64, T_datetime:
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
	case T_decimal64:
		typ.Size = 8
	case T_decimal128:
		typ.Size = 16
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
	case T_decimal64:
		return "DECIMAL64"
	case T_decimal128:
		return "DECIMAL128"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}

// functions only used to generate pkg/sql/colexec/extend/overload

// OidString returns T string
func (t T) OidString() string {
	switch t {
	case T_int64:
		return "T_int64"
	case T_int32:
		return "T_int32"
	case T_int16:
		return "T_int16"
	case T_int8:
		return "T_int8"
	case T_float64:
		return "T_float64"
	case T_float32:
		return "T_float32"
	case T_uint8:
		return "T_uint8"
	case T_uint16:
		return "T_uint16"
	case T_uint32:
		return "T_uint32"
	case T_uint64:
		return "T_uint64"
	case T_sel:
		return "T_sel"
	case T_char:
		return "T_char"
	case T_varchar:
		return "T_varchar"
	case T_date:
		return "T_date"
	case T_datetime:
		return "T_datetime"
	case T_decimal64:
		return "T_decimal64"
	case T_decimal128:
		return "T_decimal128"
	}
	return "unknown_type"
}

// GoType returns go type string for T
func (t T) GoType() string {
	switch t {
	case T_int64:
		return "int64"
	case T_int32:
		return "int32"
	case T_int16:
		return "int16"
	case T_int8:
		return "int8"
	case T_float64:
		return "float64"
	case T_float32:
		return "float32"
	case T_uint8:
		return "uint8"
	case T_uint16:
		return "uint16"
	case T_uint32:
		return "uint32"
	case T_uint64:
		return "uint64"
	case T_sel:
		return "int64"
	case T_char:
		return "string"
	case T_varchar:
		return "string"
	case T_date:
		return "date"
	case T_datetime:
		return "datetime"
	case T_decimal64:
		return "decimal64"
	case T_decimal128:
		return "decimal128"
	}
	return "unknown type"
}

// GoGoType returns special go type string for T
func (t T) GoGoType() string {
	if t == T_char || t == T_varchar {
		return "Str"
	}
	k := t.GoType()
	return strings.ToUpper(k[:1]) + k[1:]
}

// TypeLen returns type's length whose type oid is T
func (t T) TypeLen() int {
	switch t {
	case T_int8:
		return 1
	case T_int16:
		return 2
	case T_int32, T_date:
		return 4
	case T_int64, T_datetime:
		return 8
	case T_uint8:
		return 1
	case T_uint16:
		return 2
	case T_uint32:
		return 4
	case T_uint64:
		return 8
	case T_float32:
		return 4
	case T_float64:
		return 8
	case T_char:
		return 24
	case T_varchar:
		return 24
	case T_sel:
		return 8
	case T_decimal64:
		return 8
	case T_decimal128:
		return 16
	}
	return -1
}
