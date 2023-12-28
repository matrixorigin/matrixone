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

var (
	// stddev_pop() supported input type and output type.
	AggStdDevSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggStdDevReturnType = AggVarianceReturnType
)

func NewAggStdDevPop(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericStdDevPop[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericStdDevPop[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericStdDevPop[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericStdDevPop[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericStdDevPop[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericStdDevPop[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericStdDevPop[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericStdDevPop[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericStdDevPop[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericStdDevPop[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := newVarianceDecimal(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.FillD64), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.FillD64), nil
	case types.T_decimal128:
		aggPriv := newVarianceDecimal(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.FillD128), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.FillD128), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for std_dev_pop", inputTypes[0])
}

func newGenericStdDevPop[T numeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggVarPop[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.EvalStdDevPop, aggPriv.Merge, aggPriv.Fill), nil
}
