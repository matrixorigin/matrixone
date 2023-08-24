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

func NewAggApproxCount(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		return newGenericApprox[bool](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericApprox[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericApprox[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericApprox[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericApprox[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericApprox[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericApprox[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericApprox[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericApprox[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericApprox[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericApprox[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericApprox[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericApprox[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericApprox[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericApprox[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericApprox[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		return newGenericApprox[types.Decimal64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal128:
		return newGenericApprox[types.Decimal128](overloadID, inputTypes[0], outputType, dist)
	case types.T_uuid:
		return newGenericApprox[types.Uuid](overloadID, inputTypes[0], outputType, dist)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary:
		return newGenericApprox[[]byte](overloadID, inputTypes[0], outputType, dist)
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for approx_count", inputTypes[0])
}

func newGenericApprox[T any](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewApproxc[T]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
