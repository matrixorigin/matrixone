// Copyright 2021 - 2022 Matrix Origin
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

package functionAgg

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

func NewAggMin(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		aggPriv := agg.NewBoolMin()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_uint8:
		return newGenericMin[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericMin[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericMin[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericMin[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericMin[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericMin[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericMin[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericMin[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericMin[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericMin[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericMin[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericMin[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericMin[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericMin[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericMin[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := agg.NewD64Min()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_decimal128:
		aggPriv := agg.NewD128Min()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_uuid:
		aggPriv := agg.NewUuidMin()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_binary, types.T_varbinary, types.T_char, types.T_varchar, types.T_blob, types.T_text:
		aggPriv := agg.NewStrMin()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for min", inputTypes[0])
}

func newGenericMin[T compare](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewMin[T]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
