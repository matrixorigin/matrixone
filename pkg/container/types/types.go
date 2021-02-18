package types

import "fmt"

const (
	// user type
	T_any   = 0
	T_int   = 1 // int64
	T_bool  = 2
	T_float = 3 // float64
	T_bytes = 4
	T_json  = 5

	// system type
	T_sel   = 30 // selection
	T_tuple = 31 // immutable
)

type T uint8

var Types map[string]T = map[string]T{
	"int":   T_int,
	"bool":  T_bool,
	"float": T_float,
	"json":  T_json,
	"bytes": T_bytes,
}

func (t T) String() string {
	switch t {
	case T_int:
		return "INT"
	case T_bool:
		return "BOOL"
	case T_float:
		return "FLOAT"
	case T_bytes:
		return "BYTES"
	case T_json:
		return "JSON"
	case T_sel:
		return "SEL"
	case T_tuple:
		return "TUPLE"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}
