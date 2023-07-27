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

package tree

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/defines"
)

type Family int32

const (
	UnknownFamily Family = iota
	IntFamily
	FloatFamily
	TimestampFamily
	DateFamily
	IntervalFamily
	TimeFamily
	StringFamily
	BitFamily
	BoolFamily
	JsonFamily
	EnumFamily
	SetFamily
	UuidFamily

	//test
	BlobFamily

	GeometryFamily

	EmbeddingFamily
)

type IntervalDurationField struct {
}

type GeoMetadata struct {
}

type PersistentUserDefinedTypeMetadata struct {
}

// for sql type
type InternalType struct {
	//the group of types that are compatible with each other
	Family       Family
	FamilyString string
	/*
		From: https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html

		For integer data types, M indicates the maximum display width.
		The maximum display width is 255. Display width is unrelated to the range of values a type can store.
		For floating-point and fixed-point data types, M is the total number of digits that can be stored.

		M is the total number of digits (the width) and D is the number of digits after the decimal point (the scale).
	*/
	//the size or scale of the data type, such as number of bits or characters.
	Width int32

	/*
		From: https://dev.mysql.com/doc/refman/8.0/en/numeric-type-attributes.html
		display width
	*/
	DisplayWith int32

	//the accuracy of the data type.
	Scale int32

	//Unsigned or not
	Unsigned bool

	//binary or not
	Binary bool

	Zerofill bool

	Locale *string

	//The slice containing the type of each tuple field.
	TupleContents []*T

	//the slice containing the labels of each tuple field.
	TupleLabels []string

	//type id
	Oid uint32

	EnumValues []string

	//the type of array elements.
	ArrayContents *T

	//indicates whether the precision was explicitly set.
	//From https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html
	//In mysql 8.0. Precision must be in the range 0 to 6.
	//A value of 0 signifies that there is no fractional part.
	//If omitted, the default precision is 0. (This differs from the standard SQL default of 6, for compatibility with previous MySQL versions.)
	// if Precision > 0, the precision
	// if TimePrecisionIsSet = true and precision = 0, then the precision = 0
	// if TimePrecisionIsSet = false and precision = 0, then the precision has not been indicated,
	//	so the default value is 0.
	TimePrecisionIsSet bool

	//interval type
	IntervalDurationField *IntervalDurationField

	//geospatial types.
	GeoMetadata *GeoMetadata

	//user defined types that are not arrays.
	UDTMetadata *PersistentUserDefinedTypeMetadata
}

func (node *InternalType) Format(ctx *FmtCtx) {
	fs := strings.ToLower(node.FamilyString)
	ctx.WriteString(fs)

	if node.Unsigned {
		if fs != "" {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("unsigned")
	}
	if node.Zerofill {
		ctx.WriteString(" zerofill")
	}

	switch fs {
	case "set", "enum":
	case "char":
		if node.DisplayWith >= 0 {
			ctx.WriteByte('(')
			ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			ctx.WriteByte(')')
		}
	case "varchar":
		if node.DisplayWith >= 0 {
			ctx.WriteByte('(')
			ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			ctx.WriteByte(')')
		}
	case "binary":
		if node.DisplayWith >= 0 {
			ctx.WriteByte('(')
			ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			ctx.WriteByte(')')
		}
	case "varbinary":
		if node.DisplayWith >= 0 {
			ctx.WriteByte('(')
			ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			ctx.WriteByte(')')
		}
	case "embedding":
		if node.DisplayWith >= 0 {
			ctx.WriteByte('(') //TODO: What is this?
			ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			ctx.WriteByte(')')
		}
	default:
		if node.Scale > 0 {
			ctx.WriteByte('(')
			if node.DisplayWith == -1 {
				ctx.WriteString(strconv.FormatInt(int64(0), 10))
			} else {
				ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			}
			ctx.WriteString(", ")
			ctx.WriteString(strconv.FormatInt(int64(node.Scale), 10))
			ctx.WriteByte(')')
		} else if node.DisplayWith > 0 || node.DisplayWith == -1 {
			ctx.WriteByte('(')
			if node.DisplayWith == -1 {
				ctx.WriteString(strconv.FormatInt(int64(0), 10))
			} else {
				ctx.WriteString(strconv.FormatInt(int64(node.DisplayWith), 10))
			}
			ctx.WriteByte(')')
		}
	}
}

// sql type
type T struct {
	InternalType InternalType
}

type LengthScaleOpt struct {
	DisplayWith int32
	Scale       int32
}

const (
	DefaultDisplayWidth   = -1
	NotDefineDisplayWidth = 0
	NotDefineDec          = -1
)

func GetDisplayWith(val int32) int32 {
	if val == 0 {
		return DefaultDisplayWidth
	}
	return val
}

////fractional seconds precision in TIME,DATETIME,TIMESTAMP
//Fsp int

var (
	emptyLocale = ""
)

var (
	TYPE_TINY = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  8,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_TINY),
	}}

	TYPE_SHORT = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  16,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_SHORT),
	}}

	TYPE_LONG = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  32,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_LONG),
	}}

	TYPE_FLOAT = &T{InternalType: InternalType{
		Family: FloatFamily,
		Width:  32,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_FLOAT),
	}}

	TYPE_DOUBLE = &T{InternalType: InternalType{
		Family: FloatFamily,
		Width:  64,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_DOUBLE),
	}}

	TYPE_NULL = &T{InternalType: InternalType{
		Family: UnknownFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_NULL),
	}}

	TYPE_TIMESTAMP = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Scale:              0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid:                uint32(defines.MYSQL_TYPE_TIMESTAMP),
	}}

	TYPE_LONGLONG = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  64,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_LONGLONG),
	}}

	TYPE_INT24 = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  24,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_INT24),
	}}

	TYPE_DATE = &T{InternalType: InternalType{
		Family: DateFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_DATE),
	}}

	TYPE_DURATION = &T{InternalType: InternalType{
		Family:                IntervalFamily,
		Scale:                 0,
		TimePrecisionIsSet:    false,
		Locale:                &emptyLocale,
		Oid:                   uint32(defines.MYSQL_TYPE_TIME),
		IntervalDurationField: &IntervalDurationField{},
	}}

	TYPE_TIME = &T{InternalType: InternalType{
		Family:             TimeFamily,
		Scale:              0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid:                uint32(defines.MYSQL_TYPE_TIME),
	}}

	TYPE_DATETIME = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Scale:              0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid:                uint32(defines.MYSQL_TYPE_DATETIME),
	}}

	TYPE_YEAR = &T{InternalType: InternalType{
		Family: IntFamily,
		Width:  16,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_YEAR),
	}}

	TYPE_NEWDATE = &T{InternalType: InternalType{
		Family: DateFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_NEWDATE),
	}}

	TYPE_VARCHAR = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
	}}

	TYPE_BINARY = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
	}}

	TYPE_VARBINARY = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_VARCHAR),
	}}

	TYPE_BIT = &T{InternalType: InternalType{
		Family: BitFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_BIT),
	}}

	TYPE_BOOL = &T{InternalType: InternalType{
		Family: BoolFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_BOOL),
	}}

	TYPE_JSON = &T{InternalType: InternalType{
		Family: JsonFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_JSON)}}

	TYPE_UUID = &T{InternalType: InternalType{
		Family: UuidFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_UUID)}}

	TYPE_ENUM = &T{InternalType: InternalType{
		Family: EnumFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_ENUM),
	}}

	TYPE_SET = &T{InternalType: InternalType{
		Family: SetFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_SET),
	}}

	TYPE_TINY_BLOB = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_TINY_BLOB)}}

	TYPE_MEDIUM_BLOB = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_MEDIUM_BLOB),
	}}

	TYPE_LONG_BLOB = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_LONG_BLOB),
	}}

	TYPE_BLOB = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_BLOB),
	}}

	TYPE_TEXT = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_TEXT),
	}}

	TYPE_VARSTRING = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_VAR_STRING),
	}}

	TYPE_STRING = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_STRING),
	}}

	TYPE_GEOMETRY = &T{InternalType: InternalType{
		Family: GeometryFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_GEOMETRY),
	}}

	TYPE_EMBEDDING = &T{InternalType: InternalType{
		Family: EmbeddingFamily,
		Locale: &emptyLocale,
		Oid:    uint32(defines.MYSQL_TYPE_EMBEDDING),
	}}
)
