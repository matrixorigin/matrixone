package max

import "matrixbase/pkg/container/types"

type int8Max struct {
	v   int8
	cnt int64
	typ types.Type
}

type int16Max struct {
	v   int16
	cnt int64
	typ types.Type
}

type int32Max struct {
	v   int32
	cnt int64
	typ types.Type
}

type int64Max struct {
	v   int64
	cnt int64
	typ types.Type
}

type uint8Max struct {
	v   uint8
	cnt int64
	typ types.Type
}

type uint16Max struct {
	cnt int64
	v   uint16
	typ types.Type
}

type uint32Max struct {
	cnt int64
	v   uint32
	typ types.Type
}

type uint64Max struct {
	cnt int64
	v   uint64
	typ types.Type
}

type float32Max struct {
	cnt int64
	v   float32
	typ types.Type
}

type float64Max struct {
	cnt int64
	v   float64
	typ types.Type
}

type strMax struct {
	cnt int64
	v   []byte
	typ types.Type
}
