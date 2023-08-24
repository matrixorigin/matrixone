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

func NewAggBitOr(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericBitOr[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericBitOr[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericBitOr[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericBitOr[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericBitOr[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericBitOr[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericBitOr[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericBitOr[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericBitOr[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericBitOr[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := agg.NewBitOrBinary()
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for bit_or", inputTypes[0])
}

func newGenericBitOr[T numeric](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewBitOr[T]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}
