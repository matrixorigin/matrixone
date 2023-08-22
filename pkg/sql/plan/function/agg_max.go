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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

func newAggMax(dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericMax[uint8](inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericMax[uint16](inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericMax[uint32](inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericMax[uint64](inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericMax[int8](inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericMax[int16](inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericMax[int32](inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericMax[int64](inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericMax[float32](inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericMax[float64](inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericMax[types.Date](inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericMax[types.Datetime](inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericMax[types.Timestamp](inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericMax[types.Time](inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericMax[types.Enum](inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := agg.NewD64Max()
		if dist {
			return agg.NewUnaryDistAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_decimal128:
		aggPriv := agg.NewD128Max()
		if dist {
			return agg.NewUnaryDistAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_uuid:
		aggPriv := agg.NewUuidMax()
		if dist {
			return agg.NewUnaryDistAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_binary, types.T_varbinary, types.T_char, types.T_varchar, types.T_blob, types.T_text:
		aggPriv := agg.NewStrMax()
		if dist {
			return agg.NewUnaryDistAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(MAX, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for max", inputTypes[0])
}

func newGenericMax[T compare](typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewMax[T]()
	if dist {
		return agg.NewUnaryDistAgg(MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
