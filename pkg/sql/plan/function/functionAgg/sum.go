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

func NewAggSum(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericSum[uint8, uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericSum[uint16, uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericSum[uint32, uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericSum[uint64, uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericSum[int8, int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericSum[int16, int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericSum[int32, int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericSum[int64, int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericSum[float32, float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericSum[float64, float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := agg.NewD64Sum()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_decimal128:
		aggPriv := agg.NewD64Sum()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for sum", inputTypes[0])
}

func newGenericSum[T1 numeric, T2 maxScaleNumeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewSum[T1, T2]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
