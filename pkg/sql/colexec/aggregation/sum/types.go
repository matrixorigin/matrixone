package sum

import "matrixone/pkg/container/types"

type intSum struct {
	cnt int64
	sum int64
	typ types.Type
}

type uintSum struct {
	cnt int64
	sum uint64
	typ types.Type
}

type floatSum struct {
	cnt int64
	sum float64
	typ types.Type
}

type sumCount struct {
	cnt int64
	sum float64
	typ types.Type
}

type intSumCount struct {
	cnt int64
	sum int64
	typ types.Type
}

type uintSumCount struct {
	cnt int64
	sum uint64
	typ types.Type
}

type floatSumCount struct {
	cnt int64
	sum float64
	typ types.Type
}
