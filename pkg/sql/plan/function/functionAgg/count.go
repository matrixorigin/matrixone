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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// count() supported input type and output type.
	AggCountReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
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
	case types.T_array_float32, types.T_array_float64:
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

func newGenericCount[T allTypes](
	overloadID int64, isCountStart bool, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggCount[T]{isCountStar: isCountStart}
	if dist {
		return agg.NewUnaryDistAgg[T, int64](overloadID, aggPriv, true, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[T, int64](overloadID, aggPriv, true, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggCount[T allTypes] struct {
	isCountStar bool
}

func (s *sAggCount[T]) Dup() agg.AggStruct {
	return &sAggCount[T]{
		isCountStar: s.isCountStar,
	}
}
func (s *sAggCount[T]) Grows(_ int)         {}
func (s *sAggCount[T]) Free(_ *mpool.MPool) {}
func (s *sAggCount[T]) Fill(groupNumber int64, value T, lastResult int64, count int64, isEmpty bool, isNull bool) (int64, bool, error) {
	if s.isCountStar {
		return lastResult + count, false, nil
	}
	if isNull {
		return lastResult, false, nil
	}
	return lastResult + count, false, nil
}
func (s *sAggCount[T]) BatchFill(results []int64, values []T, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	if s.isCountStar || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}
			results[groupIndexs[i]-1]++
		}
	} else {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(i+offset)) {
				continue
			}
			results[groupIndexs[i]-1]++
		}
	}
	return nil
}
func (s *sAggCount[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 int64, isEmpty1, isEmpty2 bool, _ any) (int64, bool, error) {
	return result1 + result2, isEmpty1 && isEmpty2, nil
}
func (s *sAggCount[T]) Eval(vs []int64) ([]int64, error) {
	return vs, nil
}
func (s *sAggCount[T]) MarshalBinary() ([]byte, error) {
	return types.EncodeBool(&s.isCountStar), nil
}
func (s *sAggCount[T]) UnmarshalBinary(data []byte) error {
	b := types.DecodeBool(data)
	s.isCountStar = b
	return nil
}
