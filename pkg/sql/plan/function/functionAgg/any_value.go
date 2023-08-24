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

func NewAggAnyValue(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		return newGenericAnyValue[bool](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericAnyValue[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericAnyValue[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericAnyValue[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericAnyValue[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericAnyValue[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericAnyValue[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericAnyValue[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericAnyValue[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericAnyValue[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericAnyValue[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericAnyValue[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericAnyValue[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericAnyValue[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericAnyValue[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericAnyValue[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		return newGenericAnyValue[types.Decimal64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal128:
		return newGenericAnyValue[types.Decimal128](overloadID, inputTypes[0], outputType, dist)
	case types.T_uuid:
		return newGenericAnyValue[types.Uuid](overloadID, inputTypes[0], outputType, dist)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary:
		aggPriv := agg.NewStrAnyValue()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for any_value", inputTypes[0])
}

func newGenericAnyValue[T any](overloadID int64, typ, otyp types.Type, dist bool) (agg.Agg[T], error) {
	aggPriv := agg.NewAnyValue[T]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
