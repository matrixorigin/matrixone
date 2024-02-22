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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// variance() supported input type and output type.
	AggVarianceSupportedParameters = []types.T{
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggVarianceReturnType = func(typs []types.Type) types.Type {
		if typs[0].IsDecimal() {
			s := int32(12)
			if typs[0].Scale > s {
				s = typs[0].Scale
			}
			return types.New(types.T_decimal128, 38, s)
		}
		return types.New(types.T_float64, 0, 0)
	}
)

func NewAggVarPop(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bit:
		return newGenericVarPop[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericVarPop[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericVarPop[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericVarPop[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericVarPop[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericVarPop[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericVarPop[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericVarPop[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericVarPop[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericVarPop[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericVarPop[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := newVarianceDecimal(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillD64), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillD64), nil
	case types.T_decimal128:
		aggPriv := newVarianceDecimal(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillD128), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.FillD128), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for var_pop", inputTypes[0])
}

func newGenericVarPop[T numeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggVarPop[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggVarPop[T numeric] struct {
	sum, counts []float64
}
type EncodeVariance struct {
	Sum    []float64
	Counts []float64
}

type VarianceDecimal struct {
	Sum         []types.Decimal128
	Counts      []int64
	Typ         types.Type
	ScaleMul    int32
	ScaleDiv    int32
	ScaleMulDiv int32
	ScaleDivMul int32

	// ErrOne indicates that there is only one row in the group.
	// but its ^2 is too large.
	ErrOne []bool

	x, y types.Decimal128
}

func (s *sAggVarPop[T]) Dup() agg.AggStruct {
	val := &sAggVarPop[T]{
		sum:    make([]float64, len(s.sum)),
		counts: make([]float64, len(s.counts)),
	}
	copy(val.sum, s.sum)
	copy(val.counts, s.counts)
	return val
}
func (s *sAggVarPop[T]) Grows(cnt int) {
	s.sum = append(s.sum, make([]float64, cnt)...)
	s.counts = append(s.counts, make([]float64, cnt)...)
}
func (s *sAggVarPop[T]) Free(_ *mpool.MPool) {}
func (s *sAggVarPop[T]) Fill(groupNumber int64, v T, lastResult float64, count int64, isEmpty bool, isNull bool) (float64, bool, error) {
	if isNull {
		return lastResult, isEmpty, nil
	}
	value := float64(v)
	fCount := float64(count)
	s.sum[groupNumber] += value * float64(count)
	s.counts[groupNumber] += fCount
	return lastResult + math.Pow(value, 2)*fCount, false, nil
}
func (s *sAggVarPop[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 float64, result2 float64, isEmpty1 bool, isEmpty2 bool, priv2 any) (float64, bool, error) {
	s2 := priv2.(*sAggVarPop[T])
	s.sum[groupNumber1] += s2.sum[groupNumber2]
	s.counts[groupNumber1] += s2.counts[groupNumber2]
	return result1 + result2, isEmpty1 && isEmpty2, nil
}
func (s *sAggVarPop[T]) Eval(lastResult []float64) ([]float64, error) {
	for i := range lastResult {
		if s.counts[i] == 0 {
			continue
		}
		avg := s.sum[i] / s.counts[i]
		lastResult[i] = lastResult[i]/s.counts[i] - math.Pow(avg, 2)
	}
	return lastResult, nil
}
func (s *sAggVarPop[T]) EvalStdDevPop(lastResult []float64) ([]float64, error) {
	var err error
	lastResult, err = s.Eval(lastResult)
	if err == nil {
		for i := range lastResult {
			lastResult[i] = math.Sqrt(lastResult[i])
		}
	}
	return lastResult, err
}
func (s *sAggVarPop[T]) MarshalBinary() ([]byte, error) {
	encodeV := EncodeVariance{
		Sum:    s.sum,
		Counts: s.counts,
	}
	return encodeV.Marshal()
}
func (s *sAggVarPop[T]) UnmarshalBinary(data []byte) error {
	encodeV := EncodeVariance{}
	if err := encodeV.Unmarshal(data); err != nil {
		return err
	}
	s.sum = encodeV.Sum
	s.counts = encodeV.Counts
	return nil
}

func newVarianceDecimal(typ types.Type) *VarianceDecimal {
	scalemul := int32(12)
	scalediv := int32(12)
	scalemuldiv := int32(12)
	scaledivmul := int32(12)
	if typ.Scale > 12 {
		scalemul = typ.Scale
		scalediv = typ.Scale
		scalemuldiv = typ.Scale
		scaledivmul = typ.Scale
	}
	if typ.Scale < 6 {
		scalemul = typ.Scale * 2
		scalediv = typ.Scale + 6
		if typ.Scale < 3 {
			scalemuldiv = typ.Scale*2 + 6
		}
	}
	return &VarianceDecimal{
		Typ:         typ,
		ScaleMul:    scalemul,
		ScaleDiv:    scalediv,
		ScaleMulDiv: scalemuldiv,
		ScaleDivMul: scaledivmul}
}

func (s *VarianceDecimal) Dup() agg.AggStruct {
	val := &VarianceDecimal{
		Sum:         make([]types.Decimal128, len(s.Sum)),
		Counts:      make([]int64, len(s.Counts)),
		Typ:         s.Typ,
		ScaleMul:    s.ScaleMul,
		ScaleDiv:    s.ScaleDiv,
		ScaleMulDiv: s.ScaleMulDiv,
		ScaleDivMul: s.ScaleDivMul,
		ErrOne:      make([]bool, len(s.ErrOne)),
		x:           s.x,
		y:           s.y,
	}
	copy(val.Sum, s.Sum)
	copy(val.Counts, s.Counts)
	copy(val.ErrOne, s.ErrOne)
	return val
}
func (s *VarianceDecimal) Grows(cnt int) {
	s.Sum = append(s.Sum, make([]types.Decimal128, cnt)...)
	s.Counts = append(s.Counts, make([]int64, cnt)...)
	s.ErrOne = append(s.ErrOne, make([]bool, cnt)...)
}
func (s *VarianceDecimal) Free(_ *mpool.MPool) {}
func (s *VarianceDecimal) FillD64(groupNumber int64, v types.Decimal64, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if isNull {
		return lastResult, isEmpty, nil
	}

	var err error
	s.x.B0_63, s.x.B64_127 = uint64(v), 0
	if v>>63 != 0 {
		s.x.B64_127 = ^s.x.B64_127
	}
	s.Counts[groupNumber] += count
	s.y.B0_63, s.y.B64_127 = uint64(count), 0
	s.y, _, err = s.x.Mul(s.y, s.Typ.Scale, 0)
	if err == nil {
		s.Sum[groupNumber], err = s.Sum[groupNumber].Add128(s.y)
		if err == nil {
			s.y, _, err = s.y.Mul(s.x, s.Typ.Scale, s.Typ.Scale)
			if err == nil {
				lastResult, err = lastResult.Add128(s.y)
			}
		}
	}
	if err != nil && s.Counts[groupNumber] == 1 {
		s.ErrOne[groupNumber] = true
		return lastResult, false, nil
	}
	if s.ErrOne[groupNumber] {
		err = moerr.NewInternalErrorNoCtx("Decimal64 overflowed")
	}
	return lastResult, false, err
}
func (s *VarianceDecimal) FillD128(groupNumber int64, v types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if isNull {
		return lastResult, isEmpty, nil
	}

	var err error
	s.Counts[groupNumber] += count

	s.y.B0_63, s.y.B64_127 = uint64(count), 0
	s.y, _, err = v.Mul(s.y, s.Typ.Scale, 0)
	if err == nil {
		s.Sum[groupNumber], err = s.Sum[groupNumber].Add128(s.y)
		if err == nil {
			s.y, _, err = s.y.Mul(v, s.Typ.Scale, s.Typ.Scale)
			if err == nil {
				lastResult, err = lastResult.Add128(s.y)
			}
		}
	}
	if err != nil && s.Counts[groupNumber] == 1 {
		s.ErrOne[groupNumber] = true
		return lastResult, false, nil
	}
	if s.ErrOne[groupNumber] {
		err = moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}
	return lastResult, false, err
}
func (s *VarianceDecimal) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (types.Decimal128, bool, error) {
	if isEmpty1 {
		return result2, isEmpty2, nil
	}
	if isEmpty2 {
		return result1, isEmpty1, nil
	}

	var err error
	s2 := priv2.(*VarianceDecimal)
	s.Counts[groupNumber1] += s2.Counts[groupNumber2]
	s.Sum[groupNumber1], err = s.Sum[groupNumber1].Add128(s2.Sum[groupNumber2])
	if err != nil {
		return result1, false, err
	}

	if s.Counts[groupNumber1] > 1 {
		if s.ErrOne[groupNumber1] || s2.ErrOne[groupNumber2] {
			return result1, false, moerr.NewInternalErrorNoCtx("Decimal overflowed during merge")
		}
	}

	result1, err = result1.Add128(result2)
	return result1, isEmpty1, err
}
func (s *VarianceDecimal) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i := range lastResult {
		if s.Counts[i] == 1 {
			lastResult[i].B0_63, lastResult[i].B64_127 = 0, 0
			continue
		}

		s.y.B0_63, s.y.B64_127 = uint64(s.Counts[i]), 0
		s.x, _, err = s.Sum[i].Div(s.y, s.Typ.Scale, 0)
		if err != nil {
			return nil, err
		}
		s.y, _, err = s.x.Mul(s.x, s.ScaleDiv, s.ScaleDiv)
		if err != nil {
			return nil, err
		}

		s.x.B0_63, s.x.B64_127 = uint64(s.Counts[i]), 0
		s.x, _, err = lastResult[i].Div(s.x, s.ScaleMul, 0)
		if err != nil {
			return nil, err
		}

		lastResult[i], _, err = s.x.Sub(s.y, s.ScaleMulDiv, s.ScaleDivMul)
		if err != nil {
			return nil, err
		}
	}
	return lastResult, nil
}
func (s *VarianceDecimal) EvalStdDevPop(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	lastResult, err = s.Eval(lastResult)
	if err == nil {
		for i := range lastResult {
			tmp := math.Sqrt(types.Decimal128ToFloat64(lastResult[i], s.ScaleDivMul))
			lastResult[i], err = types.Decimal128FromFloat64(tmp, 38, s.ScaleDivMul)
			if err != nil {
				break
			}
		}
	}
	return lastResult, err
}
func (s *VarianceDecimal) MarshalBinary() ([]byte, error) {
	encodeV := EncodeDecimalV{
		Sum:    s.Sum,
		Counts: s.Counts,
		ErrOne: s.ErrOne,
	}
	return encodeV.Marshal()
}
func (s *VarianceDecimal) UnmarshalBinary(data []byte) error {
	encodeV := EncodeDecimalV{}
	if err := encodeV.Unmarshal(data); err != nil {
		return err
	}
	s.Sum = encodeV.Sum
	s.Counts = encodeV.Counts
	s.ErrOne = encodeV.ErrOne
	return nil
}
