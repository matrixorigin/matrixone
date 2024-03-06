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
	AggAvgSupportedParameters = []types.T{
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggAvgReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_decimal64:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_decimal128:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_float32, types.T_float64:
			return types.New(types.T_float64, 0, 0)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.New(types.T_float64, 0, 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.New(types.T_float64, 0, 0)
		case types.T_bit:
			return types.New(types.T_float64, 0, 0)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for avg", typs[0]))
	}
)

func NewAggAvg(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bit:
		return newGenericAvg[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericAvg[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericAvg[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericAvg[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericAvg[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericAvg[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericAvg[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericAvg[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericAvg[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericAvg[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericAvg[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := &sAggDecimalAvg{typ: inputTypes[0]}
		if dist {
			return agg.NewUnaryDistAgg[types.Decimal64, types.Decimal128](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillDecimal64), nil
		}
		return agg.NewUnaryAgg[types.Decimal64, types.Decimal128](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillDecimal64), nil
	case types.T_decimal128:
		aggPriv := &sAggDecimalAvg{typ: inputTypes[0]}
		if dist {
			return agg.NewUnaryDistAgg[types.Decimal128, types.Decimal128](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillDecimal128), nil
		}
		return agg.NewUnaryAgg[types.Decimal128, types.Decimal128](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillDecimal128), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for avg", inputTypes[0])
}

func newGenericAvg[T numeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggAvg[T]{}
	if dist {
		return agg.NewUnaryDistAgg[T, float64](overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[T, float64](overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggAvg[T numeric] struct{ cnts []int64 }
type sAggDecimalAvg struct {
	cnts []int64
	typ  types.Type

	x, y      types.Decimal128
	tmpResult types.Decimal128
}

func (s *sAggAvg[T]) Dup() agg.AggStruct {
	val := &sAggAvg[T]{
		cnts: make([]int64, len(s.cnts)),
	}
	copy(val.cnts, s.cnts)
	return val
}
func (s *sAggAvg[T]) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		s.cnts = append(s.cnts, 0)
	}
}
func (s *sAggAvg[T]) Free(_ *mpool.MPool) {}
func (s *sAggAvg[T]) Fill(groupNumber int64, values T, lastResult float64, count int64, isEmpty bool, isNull bool) (newResult float64, isStillEmpty bool, err error) {
	if !isNull {
		s.cnts[groupNumber] += count
		return lastResult + float64(values)*float64(count), false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggAvg[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 float64, result2 float64, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult float64, isStillEmpty bool, err error) {
	if !isEmpty2 {
		bPriv := priv2.(*sAggAvg[T])
		if !isEmpty1 {
			s.cnts[groupNumber1] += bPriv.cnts[groupNumber2]
			return result1 + result2, false, nil
		}
		s.cnts[groupNumber1] = bPriv.cnts[groupNumber2]
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggAvg[T]) Eval(lastResult []float64) ([]float64, error) {
	for i := range lastResult {
		if s.cnts[i] == 0 {
			continue
		}
		lastResult[i] = lastResult[i] / float64(s.cnts[i])
	}
	return lastResult, nil
}
func (s *sAggAvg[T]) MarshalBinary() ([]byte, error) { return types.EncodeSlice[int64](s.cnts), nil }
func (s *sAggAvg[T]) UnmarshalBinary(data []byte) error {
	// avoid rpc reusing the buffer.
	copyData := make([]byte, len(data))
	copy(copyData, data)
	s.cnts = types.DecodeSlice[int64](copyData)
	return nil
}

func (s *sAggDecimalAvg) Dup() agg.AggStruct {
	val := &sAggDecimalAvg{
		cnts:      make([]int64, len(s.cnts)),
		typ:       s.typ,
		x:         s.x,
		y:         s.y,
		tmpResult: s.tmpResult,
	}
	copy(val.cnts, s.cnts)
	return val
}
func (s *sAggDecimalAvg) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		s.cnts = append(s.cnts, 0)
	}
}
func (s *sAggDecimalAvg) Free(_ *mpool.MPool) {}
func (s *sAggDecimalAvg) FillDecimal64(groupNumber int64, values types.Decimal64, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isNull {
		s.cnts[groupNumber] += count
		if count == 1 {
			lastResult, err = lastResult.Add64(values)
			return lastResult, false, err
		}
		s.x.B0_63 = uint64(count)
		s.y.B0_63 = uint64(values)
		s.y.B64_127 = 0
		if values>>63 != 0 {
			s.y.B64_127 = ^s.y.B64_127
		}
		s.tmpResult, _, err = s.y.Mul(s.x, s.typ.Scale, 0)
		if err == nil {
			lastResult, err = lastResult.Add128(s.tmpResult)
		}
		return lastResult, false, err
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimalAvg) BatchFillDecimal64(results []types.Decimal128, values []types.Decimal64, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	var groupIndex uint64
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}
			groupIndex = groupIndexs[i] - 1
			results[groupIndex], err = results[groupIndex].Add64(values[i+offset])
			if err != nil {
				return err
			}
			s.cnts[groupIndex]++
		}
	} else {
		var rowIndex int
		for i := 0; i < length; i++ {
			rowIndex = offset + i
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(rowIndex)) {
				continue
			}

			groupIndex = groupIndexs[i] - 1
			results[groupIndex], err = results[groupIndex].Add64(values[rowIndex])
			if err != nil {
				return err
			}
			s.cnts[rowIndex]++
		}
	}
	return nil
}
func (s *sAggDecimalAvg) FillDecimal128(groupNumber int64, values types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isNull {
		s.cnts[groupNumber] += count
		if count == 1 {
			lastResult, err = lastResult.Add128(values)
			return lastResult, false, err
		}
		s.x.B0_63, s.x.B64_127 = uint64(count), 0
		s.tmpResult, _, err = values.Mul(s.x, s.typ.Scale, 0)
		if err == nil {
			lastResult, err = lastResult.Add128(s.tmpResult)
		}
		return lastResult, false, err
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimalAvg) BatchFillDecimal128(results []types.Decimal128, values []types.Decimal128, offset int, length int, groupIndexs []uint64, nsp *nulls.Nulls) (err error) {
	var groupIndex uint64
	if nsp == nil || nsp.IsEmpty() {
		for i := 0; i < length; i++ {
			if groupIndexs[i] == agg.GroupNotMatch {
				continue
			}
			groupIndex = groupIndexs[i] - 1
			results[groupIndex], _, err = results[groupIndex].Add(values[i+offset], 0, 0)
			if err != nil {
				return err
			}
			s.cnts[groupIndex]++
		}
	} else {
		var rowIndex int
		for i := 0; i < length; i++ {
			rowIndex = offset + i
			if groupIndexs[i] == agg.GroupNotMatch || nsp.Contains(uint64(rowIndex)) {
				continue
			}

			groupIndex = groupIndexs[i] - 1
			results[groupIndex], _, err = results[groupIndex].Add(values[rowIndex], 0, 0)
			if err != nil {
				return err
			}
			s.cnts[rowIndex]++
		}
	}
	return nil
}
func (s *sAggDecimalAvg) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal128, isStillEmpty bool, err error) {
	if !isEmpty2 {
		bPriv := priv2.(*sAggDecimalAvg)
		s.cnts[groupNumber1] += bPriv.cnts[groupNumber2]
		if !isEmpty1 {
			s.x, err = result1.Add128(result2)
			return s.x, false, err
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimalAvg) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i := range lastResult {
		if s.cnts[i] == 0 {
			continue
		}
		s.x.B0_63 = uint64(s.cnts[i])
		s.x.B64_127 = 0
		lastResult[i], _, err = lastResult[i].Div(s.x, s.typ.Scale, 0)
		if err != nil {
			return nil, err
		}
	}
	return lastResult, nil
}
func (s *sAggDecimalAvg) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice[int64](s.cnts), nil
}
func (s *sAggDecimalAvg) UnmarshalBinary(data []byte) error {
	// avoid rpc reusing the buffer.
	copyData := make([]byte, len(data))
	copy(copyData, data)
	s.cnts = types.DecodeSlice[int64](copyData)
	return nil
}
