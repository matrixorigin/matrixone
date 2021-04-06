package avg

import "matrixbase/pkg/container/types"

type intAvg struct {
	cnt int64
	sum int64
	typ types.Type
}

type uintAvg struct {
	cnt int64
	sum uint64
	typ types.Type
}

type floatAvg struct {
	cnt int64
	sum float64
	typ types.Type
}

type sumCountAvg struct {
	cnt int64
	sum float64
	typ types.Type
}
