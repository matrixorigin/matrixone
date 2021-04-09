package min

import "matrixone/pkg/container/types"

type int8Min struct {
	v   int8
	cnt int64
	typ types.Type
}

type int16Min struct {
	v   int16
	cnt int64
	typ types.Type
}

type int32Min struct {
	v   int32
	cnt int64
	typ types.Type
}

type int64Min struct {
	v   int64
	cnt int64
	typ types.Type
}

type uint8Min struct {
	v   uint8
	cnt int64
	typ types.Type
}

type uint16Min struct {
	cnt int64
	v   uint16
	typ types.Type
}

type uint32Min struct {
	cnt int64
	v   uint32
	typ types.Type
}

type uint64Min struct {
	cnt int64
	v   uint64
	typ types.Type
}

type float32Min struct {
	cnt int64
	v   float32
	typ types.Type
}

type float64Min struct {
	cnt int64
	v   float64
	typ types.Type
}

type strMin struct {
	cnt int64
	v   []byte
	typ types.Type
}
