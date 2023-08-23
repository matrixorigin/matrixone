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

func NewAggCount(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	return newCount(overloadID, false, inputTypes[0], outputType, dist)
}

func NewAggStarCount(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	return newCount(overloadID, true, inputTypes[0], outputType, dist)
}

func newCount(overloadID int64, isCountStart bool, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	switch inputType.Oid {
	case types.T_bool:
		return newGenericCount[bool](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_uint8:
		return newGenericCount[uint8](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_uint16:
		return newGenericCount[uint16](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_uint32:
		return newGenericCount[uint32](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_uint64:
		return newGenericCount[uint64](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_int8:
		return newGenericCount[int8](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_int16:
		return newGenericCount[int16](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_int32:
		return newGenericCount[int32](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_int64:
		return newGenericCount[int64](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_float32:
		return newGenericCount[float32](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_float64:
		return newGenericCount[float64](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary:
		return newGenericCount[[]byte](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_date:
		return newGenericCount[types.Date](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_datetime:
		return newGenericCount[types.Datetime](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_timestamp:
		return newGenericCount[types.Timestamp](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_time:
		return newGenericCount[types.Time](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_enum:
		return newGenericCount[types.Enum](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_decimal64:
		return newGenericCount[types.Decimal64](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_decimal128:
		return newGenericCount[types.Decimal128](overloadID, isCountStart, inputType, outputType, dist)
	case types.T_uuid:
		return newGenericCount[types.Uuid](overloadID, isCountStart, inputType, outputType, dist)
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for count", inputType)
}

func newGenericCount[T types.OrderedT | types.Decimal | bool | types.Uuid | []byte](
	overloadID int64, isCountStart bool, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewCount[T](isCountStart)
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
