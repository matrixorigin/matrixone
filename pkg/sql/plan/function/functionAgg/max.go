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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	AggMaxSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime,
		types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_bit,
		types.T_varchar, types.T_char, types.T_blob, types.T_text,
		types.T_uuid,
		types.T_binary, types.T_varbinary,
	}
	AggMaxReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}
)

func NewAggMax(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		aggPriv := &sAggBoolMax{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_bit:
		return newGenericMax[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericMax[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericMax[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericMax[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericMax[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericMax[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericMax[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericMax[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericMax[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericMax[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericMax[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericMax[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericMax[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericMax[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericMax[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericMax[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := &sAggDecimal64Max{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_decimal128:
		aggPriv := &sAggDecimal128Max{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_uuid:
		aggPriv := &sAggUuidMax{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_binary, types.T_varbinary, types.T_char, types.T_varchar, types.T_blob, types.T_text:
		aggPriv := &sAggStrMax{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for max", inputTypes[0])
}

func newGenericMax[T compare](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggMax[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggMax[T compare] struct{}
type sAggBoolMax struct{}
type sAggDecimal64Max struct{}
type sAggDecimal128Max struct{}
type sAggUuidMax struct{}
type sAggStrMax struct{}

func (s *sAggMax[T]) Dup() agg.AggStruct {
	return &sAggMax[T]{}
}
func (s *sAggMax[T]) Grows(_ int)         {}
func (s *sAggMax[T]) Free(_ *mpool.MPool) {}
func (s *sAggMax[T]) Fill(groupNumber int64, values T, lastResult T, count int64, isEmpty bool, isNull bool) (newResult T, isStillEmpty bool, err error) {
	if !isNull {
		if values > lastResult || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggMax[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 T, result2 T, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult T, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1 > result2 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggMax[T]) Eval(lastResult []T) (result []T, err error) {
	return lastResult, nil
}
func (s *sAggMax[T]) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggMax[T]) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggBoolMax) Dup() agg.AggStruct {
	return &sAggBoolMax{}
}
func (s *sAggBoolMax) Grows(_ int)         {}
func (s *sAggBoolMax) Free(_ *mpool.MPool) {}
func (s *sAggBoolMax) Fill(groupNumber int64, values bool, lastResult bool, count int64, isEmpty bool, isNull bool) (newResult bool, isStillEmpty bool, err error) {
	if !isNull {
		if values || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBoolMax) Merge(groupNumber1 int64, groupNumber2 int64, result1 bool, result2 bool, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult bool, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggBoolMax) Eval(lastResult []bool) (result []bool, err error) {
	return lastResult, nil
}
func (s *sAggBoolMax) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggBoolMax) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal64Max) Dup() agg.AggStruct {
	return &sAggDecimal64Max{}
}
func (s *sAggDecimal64Max) Grows(_ int)         {}
func (s *sAggDecimal64Max) Free(_ *mpool.MPool) {}
func (s *sAggDecimal64Max) Fill(groupNumber int64, values types.Decimal64, lastResult types.Decimal64, count int64, isEmpty bool, isNull bool) (newResult types.Decimal64, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) > 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal64Max) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal64, result2 types.Decimal64, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal64, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) > 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal64Max) Eval(lastResult []types.Decimal64) (result []types.Decimal64, err error) {
	return lastResult, nil
}
func (s *sAggDecimal64Max) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal64Max) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal128Max) Dup() agg.AggStruct {
	return &sAggDecimal128Max{}
}
func (s *sAggDecimal128Max) Grows(_ int)         {}
func (s *sAggDecimal128Max) Free(_ *mpool.MPool) {}
func (s *sAggDecimal128Max) Fill(groupNumber int64, values types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) > 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal128Max) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) > 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal128Max) Eval(lastResult []types.Decimal128) (result []types.Decimal128, err error) {
	return lastResult, nil
}
func (s *sAggDecimal128Max) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal128Max) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggUuidMax) Dup() agg.AggStruct {
	return &sAggUuidMax{}
}
func (s *sAggUuidMax) Grows(_ int)         {}
func (s *sAggUuidMax) Free(_ *mpool.MPool) {}
func (s *sAggUuidMax) Fill(groupNumber int64, values types.Uuid, lastResult types.Uuid, count int64, isEmpty bool, isNull bool) (newResult types.Uuid, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) > 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggUuidMax) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Uuid, result2 types.Uuid, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Uuid, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) > 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggUuidMax) Eval(lastResult []types.Uuid) (result []types.Uuid, err error) {
	return lastResult, nil
}
func (s *sAggUuidMax) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggUuidMax) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggStrMax) Dup() agg.AggStruct {
	return &sAggStrMax{}
}
func (s *sAggStrMax) Grows(_ int)         {}
func (s *sAggStrMax) Free(_ *mpool.MPool) {}
func (s *sAggStrMax) Fill(groupNumber int64, values []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) (newResult []byte, isStillEmpty bool, err error) {
	if !isNull {
		if isEmpty {
			lastResult = make([]byte, len(values))
			copy(lastResult, values)
			return lastResult, false, nil
		}
		if bytes.Compare(values, lastResult) > 0 {
			lastResult = lastResult[:0]
			lastResult = append(lastResult, values...)
			return lastResult, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggStrMax) Merge(groupNumber1 int64, groupNumber2 int64, result1 []byte, result2 []byte, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult []byte, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && bytes.Compare(result1, result2) > 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggStrMax) Eval(lastResult [][]byte) (result [][]byte, err error) {
	return lastResult, nil
}
func (s *sAggStrMax) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggStrMax) UnmarshalBinary([]byte) error   { return nil }
