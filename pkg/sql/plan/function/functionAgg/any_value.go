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
	// any_value() supported input type and output type.
	AggAnyValueSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime,
		types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_varchar, types.T_char, types.T_blob, types.T_text,
		types.T_uuid,
		types.T_binary, types.T_varbinary,
		types.T_Rowid,
	}
	AggAnyValueReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}
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
		aggPriv := &sAggAnyValue[[]byte]{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillBytes), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillBytes), nil
	case types.T_Rowid:
		return newGenericAnyValue[types.Rowid](overloadID, inputTypes[0], outputType, dist)
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for any_value", inputTypes[0])
}

func newGenericAnyValue[T any](overloadID int64, typ, otyp types.Type, dist bool) (agg.Agg[T], error) {
	aggPriv := &sAggAnyValue[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggAnyValue[T any] struct{}

func (s *sAggAnyValue[T]) Dup() agg.AggStruct {
	return &sAggAnyValue[T]{}
}
func (s *sAggAnyValue[T]) Grows(cnt int)       {}
func (s *sAggAnyValue[T]) Free(_ *mpool.MPool) {}
func (s *sAggAnyValue[T]) Fill(groupNumber int64, value T, lastResult T, count int64, isEmpty bool, isNull bool) (T, bool, error) {
	if !isNull && isEmpty {
		return value, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggAnyValue[T]) FillBytes(groupNumber int64, value []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull && isEmpty {
		result := make([]byte, len(value))
		copy(result, value)
		return result, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggAnyValue[T]) BatchFill(results []T, values []T, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}
			results[groupIndexs[i]-1] = values[i+offset]
		}
	} else {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(i)) {
				continue
			}
			results[groupIndexs[i]-1] = values[i+offset]
		}
	}
	return nil
}
func (s *sAggAnyValue[T]) BatchFillBytes(results [][]byte, values [][]byte, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || len(results[groupIndexs[i]-1]) > 0 {
				continue
			}
			results[groupIndexs[i]-1] = make([]byte, len(values[i+offset]))
			copy(results[groupIndexs[i]-1], values[i+offset])
		}
	} else {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(i)) || len(results[groupIndexs[i]-1]) > 0 {
				continue
			}
			results[groupIndexs[i]-1] = make([]byte, len(values[i+offset]))
			copy(results[groupIndexs[i]-1], values[i+offset])
		}
	}
	return nil
}
func (s *sAggAnyValue[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 T, isEmpty1, isEmpty2 bool, _ any) (T, bool, error) {
	if !isEmpty2 {
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggAnyValue[T]) Eval(vs []T) ([]T, error) {
	return vs, nil
}
func (s *sAggAnyValue[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggAnyValue[T]) UnmarshalBinary([]byte) error {
	return nil
}
