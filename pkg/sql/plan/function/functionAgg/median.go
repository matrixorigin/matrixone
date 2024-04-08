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
	"encoding/json"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// median() supported input type and output type.
	AggMedianSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggMedianReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_decimal64:
			return types.New(types.T_decimal128, 38, typs[0].Scale+1)
		case types.T_decimal128:
			return types.New(types.T_decimal128, 38, typs[0].Scale+1)
		case types.T_float32, types.T_float64:
			return types.New(types.T_float64, 0, 0)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.New(types.T_float64, 0, 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.New(types.T_float64, 0, 0)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for median", typs[0]))
	}
)

func NewAggMedian(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_int8:
		return newGenericMedian[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericMedian[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericMedian[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericMedian[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericMedian[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericMedian[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericMedian[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericMedian[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericMedian[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericMedian[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := &sAggDecimal64Median{}
		if dist {
			return nil, moerr.NewNotSupportedNoCtx("median in distinct mode")
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	case types.T_decimal128:
		aggPriv := &sAggDecimal128Median{}
		if dist {
			return nil, moerr.NewNotSupportedNoCtx("median in distinct mode")
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for median", inputTypes[0])
}

func newGenericMedian[T numeric](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggMedian[T]{}
	if dist {
		return nil, moerr.NewNotSupportedNoCtx("median in distinct mode")
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggMedian[T numeric] struct{ values []numericSlice[T] }
type sAggDecimal64Median struct{ values []decimal64Slice }
type sAggDecimal128Median struct{ values []decimal128Slice }

func (s *sAggMedian[T]) Dup() agg.AggStruct {
	val := &sAggMedian[T]{
		values: make([]numericSlice[T], len(s.values)),
	}
	for i, v := range s.values {
		val.values[i] = make(numericSlice[T], len(v))
		copy(s.values[i], v)
	}
	return val
}
func (s *sAggMedian[T]) Grows(cnt int) {
	oldLen := len(s.values)
	s.values = append(s.values, make([]numericSlice[T], cnt)...)
	for i := oldLen; i < len(s.values); i++ {
		s.values[i] = numericSlice[T]{}
	}
}
func (s *sAggMedian[T]) Free(*mpool.MPool) {}
func (s *sAggMedian[T]) Fill(groupNumber int64, values T, lastResult float64, count int64, isEmpty bool, isNull bool) (float64, bool, error) {
	if !isNull {
		for i := int64(0); i < count; i++ {
			s.values[groupNumber] = append(s.values[groupNumber], values)
		}
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggMedian[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 float64, result2 float64, isEmpty1 bool, isEmpty2 bool, priv2 any) (float64, bool, error) {
	if !isEmpty2 {
		s2 := priv2.(*sAggMedian[T])
		if !sort.IsSorted(s2.values[groupNumber2]) {
			sort.Sort(s2.values[groupNumber2])
		}

		if isEmpty1 {
			s.values[groupNumber1] = s2.values[groupNumber2]
			s2.values[groupNumber2] = nil
			return 0, false, nil
		}
		// do merge run.
		if !sort.IsSorted(s.values[groupNumber1]) {
			sort.Sort(s.values[groupNumber1])
		}
		sumCount := len(s.values[groupNumber1]) + len(s2.values[groupNumber2])
		result := make(numericSlice[T], sumCount)
		mergeNumeric(s.values[groupNumber1], s2.values[groupNumber2], result)
		s.values[groupNumber1] = result
		return 0, false, nil
	}
	return 0, isEmpty1, nil
}
func (s *sAggMedian[T]) Eval(lastResult []float64) ([]float64, error) {
	for i := range lastResult {
		count := len(s.values[i])
		if count == 0 {
			continue
		}
		if !sort.IsSorted(s.values[i]) {
			sort.Sort(s.values[i])
		}
		if count&1 == 1 {
			lastResult[i] = float64(s.values[i][count>>1])
		} else {
			lastResult[i] = float64(s.values[i][count>>1]+s.values[i][count>>1-1]) / 2
		}
	}
	return lastResult, nil
}
func (s *sAggMedian[T]) MarshalBinary() ([]byte, error) {
	nm := NumericMedian[T]{Vals: s.values}
	return json.Marshal(nm)
}
func (s *sAggMedian[T]) UnmarshalBinary(data []byte) error {
	nm := NumericMedian[T]{}
	err := json.Unmarshal(data, &nm)
	if err != nil {
		return err
	}
	s.values = nm.Vals
	return nil
}

type NumericMedian[T numeric] struct {
	Vals []numericSlice[T]
}

func (s *sAggDecimal64Median) Dup() agg.AggStruct {
	val := &sAggDecimal64Median{
		values: make([]decimal64Slice, len(s.values)),
	}
	for i, v := range s.values {
		val.values[i] = make(decimal64Slice, len(v))
		copy(s.values[i], v)
	}
	return val
}
func (s *sAggDecimal64Median) Grows(cnt int) {
	oldLen := len(s.values)
	s.values = append(s.values, make([]decimal64Slice, cnt)...)
	for i := oldLen; i < len(s.values); i++ {
		s.values[i] = decimal64Slice{}
	}
}
func (s *sAggDecimal64Median) Free(*mpool.MPool) {}
func (s *sAggDecimal64Median) Fill(groupNumber int64, values types.Decimal64, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if !isNull {
		for i := int64(0); i < count; i++ {
			s.values[groupNumber] = append(s.values[groupNumber], values)
		}
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal64Median) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (types.Decimal128, bool, error) {
	if !isEmpty2 {
		s2 := priv2.(*sAggDecimal64Median)
		if !sort.IsSorted(s2.values[groupNumber2]) {
			sort.Sort(s2.values[groupNumber2])
		}

		if isEmpty1 {
			s.values[groupNumber1] = s2.values[groupNumber2]
			s2.values[groupNumber2] = nil
			return result1, false, nil
		}
		// do merge run.
		if !sort.IsSorted(s.values[groupNumber1]) {
			sort.Sort(s.values[groupNumber1])
		}
		sumCount := len(s.values[groupNumber1]) + len(s2.values[groupNumber2])
		result := make(decimal64Slice, sumCount)
		mergeD64(s.values[groupNumber1], s2.values[groupNumber2], result)
		return result1, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal64Median) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i := range lastResult {
		count := len(s.values[i])
		if count == 0 {
			continue
		}
		if !sort.IsSorted(s.values[i]) {
			sort.Sort(s.values[i])
		}
		if count&1 == 1 {
			lastResult[i], err = types.Decimal128{B0_63: uint64(s.values[i][count>>1]), B64_127: 0}.Scale(1)
		} else {
			a := types.Decimal128{B0_63: uint64(s.values[i][count>>1]), B64_127: 0}
			b := types.Decimal128{B0_63: uint64(s.values[i][count>>1-1]), B64_127: 0}
			lastResult[i], err = a.Add128(b)
			if err == nil {
				if lastResult[i].Sign() {
					lastResult[i] = lastResult[i].Minus()
					lastResult[i], err = lastResult[i].Scale(1)
					lastResult[i] = lastResult[i].Right(1).Minus()
				} else {
					lastResult[i], err = lastResult[i].Scale(1)
					lastResult[i] = lastResult[i].Right(1)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return lastResult, nil
}
func (s *sAggDecimal64Median) MarshalBinary() ([]byte, error) {
	ss := Decimal64Median{Vals: s.values}
	return ss.Marshal()
}
func (s *sAggDecimal64Median) UnmarshalBinary(data []byte) error {
	ss := &Decimal64Median{}
	err := ss.Unmarshal(data)
	if err != nil {
		return err
	}
	s.values = ss.Vals
	return nil
}

func (s *sAggDecimal128Median) Dup() agg.AggStruct {
	val := &sAggDecimal128Median{
		values: make([]decimal128Slice, len(s.values)),
	}
	for i, v := range s.values {
		val.values[i] = make(decimal128Slice, len(v))
		copy(s.values[i], v)
	}
	return val
}
func (s *sAggDecimal128Median) Grows(cnt int) {
	oldLen := len(s.values)
	s.values = append(s.values, make([]decimal128Slice, cnt)...)
	for i := oldLen; i < len(s.values); i++ {
		s.values[i] = decimal128Slice{}
	}
}
func (s *sAggDecimal128Median) Free(*mpool.MPool) {}
func (s *sAggDecimal128Median) Fill(groupNumber int64, values types.Decimal128, lastResult types.Decimal128, count int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if !isNull {
		for i := int64(0); i < count; i++ {
			s.values[groupNumber] = append(s.values[groupNumber], values)
		}
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggDecimal128Median) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal128, result2 types.Decimal128, isEmpty1 bool, isEmpty2 bool, priv2 any) (types.Decimal128, bool, error) {
	if !isEmpty2 {
		s2 := priv2.(*sAggDecimal128Median)
		if !sort.IsSorted(s2.values[groupNumber2]) {
			sort.Sort(s2.values[groupNumber2])
		}

		if isEmpty1 {
			s.values[groupNumber1] = s2.values[groupNumber2]
			s2.values[groupNumber2] = nil
			return result1, false, nil
		}
		// do merge run.
		if !sort.IsSorted(s.values[groupNumber1]) {
			sort.Sort(s.values[groupNumber1])
		}
		sumCount := len(s.values[groupNumber1]) + len(s2.values[groupNumber2])
		result := make(decimal128Slice, sumCount)
		mergeD128(s.values[groupNumber1], s2.values[groupNumber2], result)
		return result1, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggDecimal128Median) Eval(lastResult []types.Decimal128) ([]types.Decimal128, error) {
	var err error
	for i := range lastResult {
		count := len(s.values[i])
		if count == 0 {
			continue
		}
		if !sort.IsSorted(s.values[i]) {
			sort.Sort(s.values[i])
		}
		if count&1 == 1 {
			lastResult[i], err = s.values[i][count>>1].Scale(1)
		} else {
			lastResult[i], err = s.values[i][count>>1].Add128(s.values[i][count>>1-1])
			if err == nil {
				if lastResult[i].Sign() {
					lastResult[i] = lastResult[i].Minus()
					lastResult[i], err = lastResult[i].Scale(1)
					lastResult[i] = lastResult[i].Right(1).Minus()
				} else {
					lastResult[i], err = lastResult[i].Scale(1)
					lastResult[i] = lastResult[i].Right(1)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return lastResult, nil
}
func (s *sAggDecimal128Median) MarshalBinary() ([]byte, error) {
	ss := Decimal128Median{Vals: s.values}
	return ss.Marshal()
}
func (s *sAggDecimal128Median) UnmarshalBinary(data []byte) error {
	ss := &Decimal128Median{}
	err := ss.Unmarshal(data)
	if err != nil {
		return err
	}
	s.values = ss.Vals
	return nil
}

func mergeNumeric[T numeric](s1, s2, rs []T) {
	i, j, cnt := 0, 0, 0
	for i < len(s1) && j < len(s2) {
		if s1[i] < s2[j] {
			rs[cnt] = s1[i]
			i++
		} else {
			rs[cnt] = s2[j]
			j++
		}
		cnt++
	}
	for ; i < len(s1); i++ {
		rs[cnt] = s1[i]
		cnt++
	}
	for ; j < len(s2); j++ {
		rs[cnt] = s2[j]
		cnt++
	}
}

func mergeD64(s1, s2, rs decimal64Slice) {
	i, j, cnt := 0, 0, 0
	for i < len(s1) && j < len(s2) {
		if s1[i].Compare(s2[j]) < 0 {
			rs[cnt] = s1[i]
			i++
		} else {
			rs[cnt] = s2[j]
			j++
		}
		cnt++
	}
	for ; i < len(s1); i++ {
		rs[cnt] = s1[i]
		cnt++
	}
	for ; j < len(s2); j++ {
		rs[cnt] = s2[j]
		cnt++
	}
}

func mergeD128(s1, s2, rs decimal128Slice) {
	i, j, cnt := 0, 0, 0
	for i < len(s1) && j < len(s2) {
		if s1[i].Compare(s2[j]) < 0 {
			rs[cnt] = s1[i]
			i++
		} else {
			rs[cnt] = s2[j]
			j++
		}
		cnt++
	}
	for ; i < len(s1); i++ {
		rs[cnt] = s1[i]
		cnt++
	}
	for ; j < len(s2); j++ {
		rs[cnt] = s2[j]
		cnt++
	}
}
