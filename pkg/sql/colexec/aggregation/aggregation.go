package aggregation

import (
	"matrixone/pkg/container/types"
)

var sumReturnTypes = map[types.T]types.Type{
	types.T_int8:    types.Type{types.T_int64, 8, 8, 0},
	types.T_int16:   types.Type{types.T_int64, 8, 8, 0},
	types.T_int32:   types.Type{types.T_int64, 8, 8, 0},
	types.T_int64:   types.Type{types.T_int64, 8, 8, 0},
	types.T_uint8:   types.Type{types.T_uint64, 8, 8, 0},
	types.T_uint16:  types.Type{types.T_uint64, 8, 8, 0},
	types.T_uint32:  types.Type{types.T_uint64, 8, 8, 0},
	types.T_uint64:  types.Type{types.T_uint64, 8, 8, 0},
	types.T_float32: types.Type{types.T_float64, 8, 8, 0},
	types.T_float64: types.Type{types.T_float64, 8, 8, 0},
}

func ReturnType(op int, typ types.Type) types.Type {
	switch op {
	case Avg:
		return types.Type{types.T_float64, 8, 8, 0}
	case Max:
		return typ
	case Min:
		return typ
	case Sum:
		return sumReturnTypes[typ.Oid]
	case Count:
		return types.Type{types.T_int64, 8, 8, 0}
	case StarCount:
		return types.Type{types.T_int64, 8, 8, 0}
	case SumCount:
		return types.Type{types.T_tuple, 24, 0, 0}
	}
	return types.Type{}
}
