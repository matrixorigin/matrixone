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
	AggMinSupportedParameters = []types.T{
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
	}
	AggMinxReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}
)

func NewAggMin(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		aggPriv := &sAggBoolMin{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_uint8:
		return newGenericMin[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericMin[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericMin[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericMin[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericMin[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericMin[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericMin[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericMin[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericMin[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericMin[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericMin[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericMin[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericMin[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericMin[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericMin[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := &sAggDecimal64Min{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_decimal128:
		aggPriv := &sAggDecimal128Min{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_uuid:
		aggPriv := &sAggUuidMin{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_binary, types.T_varbinary, types.T_char, types.T_varchar, types.T_blob, types.T_text:
		aggPriv := &sAggStrMin{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for min", inputTypes[0])
}

func newGenericMin[T compare](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggMin[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggMin[T compare] struct{}
type sAggBoolMin struct{}
type sAggDecimal64Min struct{}
type sAggDecimal128Min struct{}
type sAggUuidMin struct{}
type sAggStrMin struct{}

func (s *sAggMin[T]) Dup() agg.AggStruct {
	return &sAggMin[T]{}
}
func (s *sAggMin[T]) Grows(_ int)         {}
func (s *sAggMin[T]) Free(_ *mpool.MPool) {}
func (s *sAggMin[T]) Fill(groupNumber int64, values T, lastResult T, count int64, isEmpty bool, isNull bool) (newResult T, isStillEmpty bool, err error) {
	if !isNull {
		if values < lastResult || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggMin[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 T, result2 T, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult T, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1 < result2 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggMin[T]) Eval(lastResult []T) (result []T, err error) {
	return lastResult, nil
}
func (s *sAggMin[T]) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggMin[T]) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggBoolMin) Dup() agg.AggStruct {
	return &sAggBoolMin{}
}
func (s *sAggBoolMin) Grows(_ int)         {}
func (s *sAggBoolMin) Free(_ *mpool.MPool) {}
func (s *sAggBoolMin) Fill(groupNumber int64, values bool, lastResult bool, count int64, isEmpty bool, isNull bool) (newResult bool, isStillEmpty bool, err error) {
	if !isNull {
		if !values || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBoolMin) Merge(groupNumber1 int64, groupNumber2 int64, result1 bool, result2 bool, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult bool, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && !result1 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggBoolMin) Eval(lastResult []bool) (result []bool, err error) {
	return lastResult, nil
}
func (s *sAggBoolMin) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggBoolMin) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal64Min) Dup() agg.AggStruct {
	return &sAggDecimal64Min{}
}
func (s *sAggDecimal64Min) Grows(_ int)         {}
func (s *sAggDecimal64Min) Free(_ *mpool.MPool) {}
func (s *sAggDecimal64Min) Fill(groupNumber int64, values types.Decimal64, lastResult types.Decimal64, count int64, isEmpty bool, isNull bool) (newResult types.Decimal64, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) < 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal64Min) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal64, result2 types.Decimal64, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal64, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) < 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal64Min) Eval(lastResult []types.Decimal64) (result []types.Decimal64, err error) {
	return lastResult, nil
}
func (s *sAggDecimal64Min) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal64Min) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal128Min) Dup() agg.AggStruct {
	return &sAggDecimal128Min{}
}
func (s *sAggDecimal128Min) Grows(_ int)         {}
func (s *sAggDecimal128Min) Free(_ *mpool.MPool) {}
func (s *sAggDecimal128Min) Fill(groupNumber int64, values types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) < 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal128Min) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) < 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal128Min) Eval(lastResult []types.Decimal128) (result []types.Decimal128, err error) {
	return lastResult, nil
}
func (s *sAggDecimal128Min) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal128Min) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggUuidMin) Dup() agg.AggStruct {
	return &sAggUuidMin{}
}
func (s *sAggUuidMin) Grows(_ int)         {}
func (s *sAggUuidMin) Free(_ *mpool.MPool) {}
func (s *sAggUuidMin) Fill(groupNumber int64, values types.Uuid, lastResult types.Uuid, count int64, isEmpty bool, isNull bool) (newResult types.Uuid, isStillEmpty bool, err error) {
	if !isNull {
		if values.Compare(lastResult) < 0 || isEmpty {
			return values, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggUuidMin) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Uuid, result2 types.Uuid, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Uuid, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && result1.Compare(result2) < 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggUuidMin) Eval(lastResult []types.Uuid) (result []types.Uuid, err error) {
	return lastResult, nil
}
func (s *sAggUuidMin) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggUuidMin) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggStrMin) Dup() agg.AggStruct {
	return &sAggStrMin{}
}
func (s *sAggStrMin) Grows(_ int)         {}
func (s *sAggStrMin) Free(_ *mpool.MPool) {}
func (s *sAggStrMin) Fill(groupNumber int64, values []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) (newResult []byte, isStillEmpty bool, err error) {
	if !isNull {
		if isEmpty {
			lastResult = make([]byte, len(values))
			copy(lastResult, values)
			return lastResult, false, nil
		}
		if bytes.Compare(values, lastResult) < 0 {
			lastResult = lastResult[:0]
			lastResult = append(lastResult, values...)
			return lastResult, false, nil
		}
	}
	return lastResult, isEmpty, nil
}
func (s *sAggStrMin) Merge(groupNumber1 int64, groupNumber2 int64, result1 []byte, result2 []byte, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult []byte, isStillEmpty bool, err error) {
	if !isEmpty2 {
		if !isEmpty1 && bytes.Compare(result1, result2) < 0 {
			return result1, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggStrMin) Eval(lastResult [][]byte) (result [][]byte, err error) {
	return lastResult, nil
}
func (s *sAggStrMin) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggStrMin) UnmarshalBinary([]byte) error   { return nil }
