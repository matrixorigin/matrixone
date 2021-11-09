package transformer

import (
	"matrixone/pkg/container/types"
)

func init() {
	TransformerNamesMap = make(map[string]int)
	for i := range TransformerNames {
		TransformerNamesMap[TransformerNames[i]] = i
	}
}

var sumReturnTypes = map[types.T]types.T{
	types.T_int8:    types.T_int64,
	types.T_int16:   types.T_int64,
	types.T_int32:   types.T_int64,
	types.T_int64:   types.T_int64,
	types.T_uint8:   types.T_uint64,
	types.T_uint16:  types.T_uint64,
	types.T_uint32:  types.T_uint64,
	types.T_uint64:  types.T_uint64,
	types.T_float32: types.T_float64,
	types.T_float64: types.T_float64,
}

func ReturnType(op int, typ types.T) types.T {
	switch op {
	case Avg:
		return types.T_float64
	case Max:
		return typ
	case Min:
		return typ
	case Sum:
		return sumReturnTypes[typ]
	case Count:
		return types.T_int64
	case StarCount:
		return types.T_int64
	}
	return 0
}
