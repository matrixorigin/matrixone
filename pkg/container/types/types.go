package types

import "fmt"

const (
	// system family
	T_any = 0

	// numeric/integer family
	T_int8   = 1
	T_int16  = 2
	T_int24  = 3
	T_int32  = 4
	T_int64  = 5
	T_uint8  = 6
	T_uint16 = 7
	T_uint24 = 8
	T_uint32 = 9
	T_uint64 = 10

	// numeric/decimal family - unsigned attribute is deprecated
	T_decimal = 11

	// numeric/float family - unsigned attribute is deprecated
	T_float32 = 12
	T_float64 = 13

	// numeric/bit family
	T_bit = 14

	// date family
	T_date      = 15
	T_time      = 16
	T_year      = 17
	T_datetime  = 18
	T_timestamp = 19

	// string family
	T_char      = 20
	T_varchar   = 21
	T_binary    = 22
	T_varbinary = 23

	// string/text family
	T_tinytext   = 24
	T_mediumtext = 25
	T_text       = 26
	T_longtext   = 27

	// string/blob family
	T_tinyblob   = 28
	T_mediumblob = 29
	T_blob       = 30
	T_longblob   = 31

	// system family
	T_sel   = 200 //selection
	T_tuple = 201 // immutable
)

type T uint8

type Type struct {
	Typ       T
	Width     int32
	Precision int32
}

var Types map[string]T = map[string]T{
	"tinyint":   T_int8,
	"smallint":  T_int16,
	"mediumint": T_int24,
	"int":       T_int32,
	"integer":   T_int32,
	"bigint":    T_int64,

	"tinyint unsigned":   T_int8,
	"smallint unsigned":  T_int16,
	"mediumint unsigned": T_int24,
	"int unsigned":       T_int32,
	"integer unsigned":   T_int32,
	"bigint unsigned":    T_int64,

	"decimal": T_decimal,

	"float":  T_float32,
	"double": T_float64,

	"bit": T_bit,

	"date":      T_date,
	"time":      T_time,
	"year":      T_year,
	"datetime":  T_datetime,
	"timestamp": T_timestamp,

	"char":      T_char,
	"varchar":   T_varchar,
	"binary":    T_binary,
	"varbinary": T_varbinary,

	"tinytext":   T_tinytext,
	"mediumtext": T_mediumtext,
	"text":       T_text,
	"longtext":   T_longtext,

	"tinyblob":   T_tinyblob,
	"mediumblob": T_mediumblob,
	"blob":       T_blob,
	"longblob":   T_longblob,
}

func (t T) String() string {
	switch t {
	case T_int8:
		return "TINYINT"
	case T_int16:
		return "SMALLINT"
	case T_int24:
		return "MEDIUMINT"
	case T_int32:
		return "INT"
	case T_int64:
		return "BIGINT"
	case T_uint8:
		return "TINYINT UNSIGNED"
	case T_uint16:
		return "SMALLINT UNSIGNED"
	case T_uint24:
		return "MEDIUMINT UNSIGNED"
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
	case T_bit:
		return "BIT"
	case T_date:
		return "DATE"
	case T_time:
		return "TIME"
	case T_year:
		return "YEAR"
	case T_datetime:
		return "DATETIME"
	case T_timestamp:
		return "TIMESTAMP"
	case T_char:
		return "CHAR"
	case T_varchar:
		return "VARCHAR"
	case T_binary:
		return "BINARY"
	case T_varbinary:
		return "VARBINARY"
	case T_tinytext:
		return "TINYTEXT"
	case T_mediumtext:
		return "MEDIUMTEXT"
	case T_text:
		return "TEXT"
	case T_longtext:
		return "LONGTEXT"
	case T_tinyblob:
		return "TINYBLOB"
	case T_mediumblob:
		return "MEDIUMBLOB"
	case T_blob:
		return "BLOB"
	case T_longblob:
		return "LONGBLOB"
	case T_sel:
		return "SEL"
	case T_tuple:
		return "TUPLE"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}
