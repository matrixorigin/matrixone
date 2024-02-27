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
	AggSumSupportedParameters = []types.T{
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggSumReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_float32, types.T_float64:
			return types.T_float64.ToType()
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.T_int64.ToType()
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.T_uint64.ToType()
		case types.T_bit:
			return types.T_uint64.ToType()
		case types.T_decimal64:
			return types.New(types.T_decimal128, 38, typs[0].Scale)
		case types.T_decimal128:
			return types.New(types.T_decimal128, 38, typs[0].Scale)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for sum", typs[0]))
	}
)

func NewAggSum(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bit:
		return newGenericSum[uint64, uint64](overloadID, inputTypes[0], outputType, dist)
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
		aggPriv := &sAggDecimal64Sum{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_decimal128:
		aggPriv := &sAggDecimal128Sum{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for sum", inputTypes[0])
}

func newGenericSum[T1 numeric, T2 maxScaleNumeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggSum[T1, T2]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggSum[Input numeric, Output numeric] struct{}
type sAggDecimal64Sum struct{}
type sAggDecimal128Sum struct{}

func (s *sAggSum[Input, Output]) Dup() agg.AggStruct {
	return &sAggSum[Input, Output]{}
}
func (s *sAggSum[Input, Output]) Grows(_ int)         {}
func (s *sAggSum[Input, Output]) Free(_ *mpool.MPool) {}
func (s *sAggSum[Input, Output]) Fill(groupNumber int64, value Input, lastResult Output, count int64, isEmpty bool, isNull bool) (Output, bool, error) {
	if !isNull {
		return lastResult + Output(value)*Output(count), false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggSum[Input, Output]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 Output, isEmpty1, isEmpty2 bool, _ any) (Output, bool, error) {
	if !isEmpty2 {
		if !isEmpty1 {
			return result1 + result2, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggSum[Input, Output]) Eval(lastResult []Output) ([]Output, error) {
	return lastResult, nil
}
func (s *sAggSum[Input, Output]) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggSum[Input, Output]) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal64Sum) Dup() agg.AggStruct {
	return &sAggDecimal64Sum{}
}
func (s *sAggDecimal64Sum) Grows(_ int)         {}
func (s *sAggDecimal64Sum) Free(_ *mpool.MPool) {}
func (s *sAggDecimal64Sum) Fill(groupNumber int64, value types.Decimal64, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	var err error
	if !isNull {
		if count == 1 {
			lastResult, err = lastResult.Add64(value)
		} else {
			newvalue := types.Decimal128{B0_63: uint64(value), B64_127: 0}
			if value.Sign() {
				newvalue.B64_127 = ^newvalue.B64_127
			}
			newvalue, err = newvalue.Mul128(types.Decimal128{B0_63: uint64(count), B64_127: 0})
			if err == nil {
				lastResult, err = lastResult.Add128(newvalue)
			}
		}
		return lastResult, false, err
	}
	return lastResult, isEmpty, err
}
func (s *sAggDecimal64Sum) BatchFill(results []types.Decimal128, values []types.Decimal64, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}

			groupIndex := groupIndexs[i] - 1
			results[groupIndex], err = results[groupIndex].Add64(values[offset+i])
			if err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(offset+i)) {
				continue
			}

			groupIndex := groupIndexs[i] - 1
			results[groupIndex], err = results[groupIndex].Add64(values[offset+i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *sAggDecimal64Sum) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 types.Decimal128, isEmpty1, isEmpty2 bool, _ any) (types.Decimal128, bool, error) {
	if !isEmpty2 {
		if !isEmpty1 {
			var err error
			result1, err = result1.Add128(result2)
			return result1, false, err
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal64Sum) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	return lastResult, nil
}
func (s *sAggDecimal64Sum) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal64Sum) UnmarshalBinary([]byte) error   { return nil }

func (s *sAggDecimal128Sum) Dup() agg.AggStruct {
	return &sAggDecimal128Sum{}
}
func (s *sAggDecimal128Sum) Grows(_ int)         {}
func (s *sAggDecimal128Sum) Free(_ *mpool.MPool) {}
func (s *sAggDecimal128Sum) Fill(groupNumber int64, value types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	var err error
	if !isNull {
		if count == 1 {
			lastResult, err = lastResult.Add128(value)
		} else {
			value, _, err = value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, 0, 0)
			if err == nil {
				lastResult, err = lastResult.Add128(value)
			}
		}
		return lastResult, false, err
	}
	return lastResult, isEmpty, err
}
func (s *sAggDecimal128Sum) BatchFill(results []types.Decimal128, values []types.Decimal128, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}

			groupIndex := groupIndexs[i] - 1
			results[groupIndex], _, err = results[groupIndex].Add(values[offset+i], 0, 0)
			if err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(offset+i)) {
				continue
			}

			groupIndex := groupIndexs[i] - 1
			results[groupIndex], _, err = results[groupIndex].Add(values[offset+i], 0, 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *sAggDecimal128Sum) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 types.Decimal128, isEmpty1, isEmpty2 bool, _ any) (types.Decimal128, bool, error) {
	if !isEmpty2 {
		if !isEmpty1 {
			var err error
			result1, err = result1.Add128(result2)
			return result1, false, err
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal128Sum) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	return lastResult, nil
}
func (s *sAggDecimal128Sum) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggDecimal128Sum) UnmarshalBinary([]byte) error   { return nil }
