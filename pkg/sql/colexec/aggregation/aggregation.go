package aggregation

import "matrixbase/pkg/container/types"

func ReturnType(op int, typ types.T) types.T {
	switch op {
	case Avg:
		return types.T_float64
	case Max:
		return typ
	case Min:
		return typ
	case Sum:
		return typ
	case Count:
		return types.T_int64
	case StarCount:
		return types.T_int64
	case SumCount:
		return types.T_tuple
	}
	return 0
}
