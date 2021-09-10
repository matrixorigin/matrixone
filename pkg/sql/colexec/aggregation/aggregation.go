// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"matrixone/pkg/container/types"
)

var sumReturnTypes = map[types.T]types.Type{
	types.T_int8:    types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0},
	types.T_int16:   types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0},
	types.T_int32:   types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0},
	types.T_int64:   types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0},
	types.T_uint8:   types.Type{Oid: types.T_uint64, Size: 8, Width: 8, Precision: 0},
	types.T_uint16:  types.Type{Oid: types.T_uint64, Size: 8, Width: 8, Precision: 0},
	types.T_uint32:  types.Type{Oid: types.T_uint64, Size: 8, Width: 8, Precision: 0},
	types.T_uint64:  types.Type{Oid: types.T_uint64, Size: 8, Width: 8, Precision: 0},
	types.T_float32: types.Type{Oid: types.T_float64, Size: 8, Width: 8, Precision: 0},
	types.T_float64: types.Type{Oid: types.T_float64, Size: 8, Width: 8, Precision: 0},
}

func ReturnType(op int, typ types.Type) types.Type {
	switch op {
	case Avg:
		return types.Type{Oid: types.T_float64, Size: 8, Width: 8, Precision: 0}
	case Max:
		return typ
	case Min:
		return typ
	case Sum:
		return sumReturnTypes[typ.Oid]
	case Count:
		return types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0}
	case StarCount:
		return types.Type{Oid: types.T_int64, Size: 8, Width: 8, Precision: 0}
	case SumCount:
		return types.Type{Oid: types.T_tuple, Size: 24, Width: 0, Precision: 0}
	}
	return types.Type{}
}
