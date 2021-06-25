package tree

import (
	"matrixone/pkg/client"
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

	//test
	BlobFamily

	GeometryFamily
)

type IntervalDurationField struct {

}

type GeoMetadata struct {

}

type PersistentUserDefinedTypeMetadata struct {

}

//for sql type
type InternalType struct {
	//the group of types that are compatible with each other
	Family Family

	/*
	From: https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html

	For integer data types, M indicates the maximum display width.
	The maximum display width is 255. Display width is unrelated to the range of values a type can store.
	For floating-point and fixed-point data types, M is the total number of digits that can be stored.

	M is the total number of digits (the precision) and D is the number of digits after the decimal point (the scale).
	 */
	//the size or scale of the data type, such as number of bits or characters.
	Width int32

	/*
	From: https://dev.mysql.com/doc/refman/8.0/en/numeric-type-attributes.html
	display width
	 */
	DisplayWith int32

	//the accuracy of the data type.
	Precision int32

	//Unsigned or not
	Unsigned bool

	//binary or not
	Binary bool

	Locale *string

	//The slice containing the type of each tuple field.
	TupleContents []*T

	//the slice containing the labels of each tuple field.
	TupleLabels []string

	//type id
	Oid uint32

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

//sql type
type T struct {
	InternalType InternalType
}

////fractional seconds precision in TIME,DATETIME,TIMESTAMP
//Fsp int

var (
	emptyLocale = ""
)

var (
	TYPE_TINY      = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 8,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_TINY),
	}}

	TYPE_SHORT     = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 16,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_SHORT),
	}}

	TYPE_LONG      = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 32,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_LONG),
	}}

	TYPE_FLOAT     = &T{InternalType: InternalType{
		Family: FloatFamily,
		Width:  32,
		Locale: &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_FLOAT),
	}}

	TYPE_DOUBLE    = &T{InternalType: InternalType{
		Family: FloatFamily,
		Width:  64,
		Locale: &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_DOUBLE),
	}}

	TYPE_NULL      = &T{InternalType: InternalType{
		Family: UnknownFamily,
		Locale: &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_NULL),
	}}

	TYPE_TIMESTAMP = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_TIMESTAMP),
	}}

	TYPE_LONGLONG  = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 64,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_LONGLONG),
	}}

	TYPE_INT24     = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 24,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_INT24),
	}}

	TYPE_DATE      = &T{InternalType: InternalType{
		Family:                DateFamily,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_DATE),
	}}

	TYPE_DURATION  = &T{InternalType: InternalType{
		Family:             IntervalFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_TIME),
		IntervalDurationField: &IntervalDurationField{},
	}}

	TYPE_TIME      = &T{InternalType: InternalType{
		Family:             TimeFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_TIME),
	}}

	TYPE_DATETIME  = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Locale:             &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_DATETIME),
	}}

	TYPE_YEAR      = &T{InternalType: InternalType{
		Family:                IntFamily,
		Width:                 16,
		Locale:                &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_YEAR),
	}}

	TYPE_NEWDATE   = &T{InternalType: InternalType{
		Family: DateFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_NEWDATE),
	}}

	TYPE_VARCHAR   = &T{InternalType: InternalType{
		Family:StringFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_VARCHAR),
	}}

	TYPE_BIT       = &T{InternalType: InternalType{
		Family: BitFamily,
		Locale: &emptyLocale,
		Oid: uint32(client.MYSQL_TYPE_BIT),
	}}

	TYPE_BOOL        = &T{InternalType: InternalType{
		Family: BoolFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_BOOL),
	}}

	TYPE_JSON        = &T{InternalType: InternalType{
		Family: JsonFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_JSON)}}

	TYPE_ENUM        = &T{InternalType: InternalType{
		Family: EnumFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_ENUM),
	}}

	TYPE_SET         = &T{InternalType: InternalType{
		Family: SetFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_SET),
	}}

	TYPE_TINY_BLOB   = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_TINY_BLOB)}}

	TYPE_MEDIUM_BLOB = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_MEDIUM_BLOB),
	}}

	TYPE_LONG_BLOB   = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_LONG_BLOB),
	}}

	TYPE_BLOB        = &T{InternalType: InternalType{
		Family: BlobFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_BLOB),
	}}

	TYPE_VARSTRING   = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_VAR_STRING),
	}}

	TYPE_STRING      = &T{InternalType: InternalType{
		Family: StringFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_STRING),
	}}

	TYPE_GEOMETRY    = &T{InternalType: InternalType{
		Family: GeometryFamily,
		Locale: &emptyLocale,
		Oid:uint32(client.MYSQL_TYPE_GEOMETRY),
	}}
)
