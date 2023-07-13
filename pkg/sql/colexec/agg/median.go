// Copyright 2022 Matrix Origin
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

package agg

import (
	"encoding/json"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type decimal64Slice []types.Decimal64
type decimal128Slice []types.Decimal128

func (s *decimal64Slice) ToPBVersion() *Decimal64SlicePB {
	return &Decimal64SlicePB{
		Slice: *s,
	}
}

func (s *decimal64Slice) ProtoSize() int {
	return s.ToPBVersion().ProtoSize()
}

func (s *decimal64Slice) MarshalToSizedBuffer(data []byte) (int, error) {
	return s.ToPBVersion().MarshalToSizedBuffer(data)
}

func (s *decimal64Slice) MarshalTo(data []byte) (int, error) {
	size := s.ProtoSize()
	return s.MarshalToSizedBuffer(data[:size])
}

func (s *decimal64Slice) Marshal() ([]byte, error) {
	data := make([]byte, s.ProtoSize())
	n, err := s.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (s *decimal64Slice) Unmarshal(data []byte) error {
	var m Decimal64SlicePB
	if err := m.Unmarshal(data); err != nil {
		return err
	}
	*s = m.Slice
	return nil
}

func (s *decimal128Slice) ToPBVersion() *Decimal128SlicePB {
	return &Decimal128SlicePB{
		Slice: *s,
	}
}
func (s *decimal128Slice) ProtoSize() int {
	return s.ToPBVersion().ProtoSize()
}

func (s *decimal128Slice) MarshalToSizedBuffer(data []byte) (int, error) {
	return s.ToPBVersion().MarshalToSizedBuffer(data)
}

func (s *decimal128Slice) MarshalTo(data []byte) (int, error) {
	size := s.ProtoSize()
	return s.MarshalToSizedBuffer(data[:size])
}

func (s *decimal128Slice) Marshal() ([]byte, error) {
	data := make([]byte, s.ProtoSize())
	n, err := s.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (s *decimal128Slice) Unmarshal(data []byte) error {
	var m Decimal128SlicePB
	if err := m.Unmarshal(data); err != nil {
		return err
	}
	*s = m.Slice
	return nil
}

func (s decimal64Slice) Len() int {
	return len(s)
}

func (s decimal64Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}

func (s decimal64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s decimal128Slice) Len() int {
	return len(s)
}

func (s decimal128Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}

func (s decimal128Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type numericSlice[T Numeric] []T

func (s numericSlice[T]) Len() int {
	return len(s)
}

func (s numericSlice[T]) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s numericSlice[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type Median[T Numeric] struct {
	Vals []numericSlice[T]
}

type Decimal64Median struct {
	Vals []decimal64Slice
}

type Decimal128Median struct {
	Vals []decimal128Slice
}

var MedianSupported = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func MedianReturnType(typs []types.Type) types.Type {
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
	default:
		return types.Type{}
	}
}

func NewMedian[T Numeric]() *Median[T] {
	return &Median[T]{}
}

func (m *Median[T]) Grows(cnt int) {
	if len(m.Vals) == 0 {
		m.Vals = make([]numericSlice[T], 0, cnt)
	}
	for i := 0; i < cnt; i++ {
		m.Vals = append(m.Vals, make(numericSlice[T], 0))
	}
}

func (m *Median[T]) Eval(vs []float64, err error) ([]float64, error) {
	if err != nil {
		return nil, err
	}
	for i := range vs {
		cnt := len(m.Vals[i])
		if cnt == 0 {
			continue
		}
		if !sort.IsSorted(m.Vals[i]) {
			sort.Sort(m.Vals[i])
		}
		if cnt&1 == 1 {
			vs[i] = float64(m.Vals[i][cnt>>1])
		} else {
			vs[i] = float64(m.Vals[i][cnt>>1]+m.Vals[i][(cnt>>1)-1]) / 2
		}
	}
	return vs, nil
}

func (m *Median[T]) Fill(i int64, value T, _ float64, z int64, isEmpty bool, isNull bool) (float64, bool, error) {
	if !isNull {
		for j := int64(0); j < z; j++ {
			m.Vals[i] = append(m.Vals[i], value)
		}
		return 0, false, nil
	}
	return 0, isEmpty, nil
}

func (m *Median[T]) Merge(xIndex int64, yIndex int64, _ float64, _ float64, xEmpty bool, yEmpty bool, yMedian any) (float64, bool, error) {
	if !yEmpty {
		yM := yMedian.(*Median[T])
		if !sort.IsSorted(yM.Vals[yIndex]) {
			sort.Sort(yM.Vals[yIndex])
		}
		if xEmpty {
			m.Vals[xIndex] = append(m.Vals[xIndex], yM.Vals[yIndex]...)
			return 0, false, nil
		}
		newCnt := len(m.Vals[xIndex]) + len(yM.Vals[yIndex])
		newData := make(numericSlice[T], newCnt)
		if !sort.IsSorted(m.Vals[xIndex]) {
			sort.Sort(m.Vals[xIndex])
		}
		merge(m.Vals[xIndex], yM.Vals[yIndex], newData, func(a, b T) bool { return a < b })
		m.Vals[xIndex] = newData
		return 0, false, nil
	}

	return 0, xEmpty, nil
}

func (m *Median[T]) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Median[T]) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

func NewD64Median() *Decimal64Median {
	return &Decimal64Median{}
}

func (m *Decimal64Median) Grows(cnt int) {
	if len(m.Vals) == 0 {
		m.Vals = make([]decimal64Slice, 0, cnt)
	}
	for i := 0; i < cnt; i++ {
		m.Vals = append(m.Vals, make(decimal64Slice, 0))
	}
}

func (m *Decimal64Median) Eval(vs []types.Decimal128, err error) ([]types.Decimal128, error) {
	if err != nil {
		return nil, err
	}
	for i := range vs {
		cnt := len(m.Vals[i])
		if cnt == 0 {
			continue
		}
		if !sort.IsSorted(m.Vals[i]) {
			sort.Sort(m.Vals[i])
		}
		if cnt&1 == 1 {
			vs[i], err = types.Decimal128{B0_63: uint64(m.Vals[i][cnt>>1]), B64_127: 0}.Scale(1)
			if err != nil {
				return nil, err
			}
		} else {
			a := types.Decimal128{B0_63: uint64(m.Vals[i][cnt>>1]), B64_127: 0}
			b := types.Decimal128{B0_63: uint64(m.Vals[i][(cnt>>1)-1]), B64_127: 0}
			vs[i], err = a.Add128(b)
			if err != nil {
				return nil, err
			}
			if vs[i].Sign() {
				vs[i] = vs[i].Minus()
				vs[i], err = vs[i].Scale(1)
				vs[i] = vs[i].Right(1).Minus()
			} else {
				vs[i], err = vs[i].Scale(1)
				vs[i] = vs[i].Right(1)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return vs, nil
}

func (m *Decimal64Median) Fill(i int64, value types.Decimal64, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if !isNull {
		for j := int64(0); j < z; j++ {
			m.Vals[i] = append(m.Vals[i], value)
		}
		return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
	}
	return types.Decimal128{B0_63: 0, B64_127: 0}, isEmpty, nil
}

func (m *Decimal64Median) Merge(xIndex int64, yIndex int64, _ types.Decimal128, _ types.Decimal128, xEmpty bool, yEmpty bool, yMedian any) (types.Decimal128, bool, error) {
	if !yEmpty {
		yM := yMedian.(*Decimal64Median)
		if !sort.IsSorted(yM.Vals[yIndex]) {
			sort.Sort(yM.Vals[yIndex])
		}
		if xEmpty {
			m.Vals[xIndex] = append(m.Vals[xIndex], yM.Vals[yIndex]...)
			return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
		}
		newCnt := len(m.Vals[xIndex]) + len(yM.Vals[yIndex])
		newData := make(decimal64Slice, newCnt)
		if !sort.IsSorted(m.Vals[xIndex]) {
			sort.Sort(m.Vals[xIndex])
		}
		merge(m.Vals[xIndex], yM.Vals[yIndex], newData, func(a, b types.Decimal64) bool { return a.Compare(b) < 0 })
		m.Vals[xIndex] = newData
		return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
	}

	return types.Decimal128{B0_63: 0, B64_127: 0}, xEmpty, nil
}

func (m *Decimal64Median) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}
func (m *Decimal64Median) UnmarshalBinary(dt []byte) error {
	return m.Unmarshal(dt)
}

func NewD128Median() *Decimal128Median {
	return &Decimal128Median{}
}

func (m *Decimal128Median) Grows(cnt int) {
	if len(m.Vals) == 0 {
		m.Vals = make([]decimal128Slice, 0, cnt)
	}
	for i := 0; i < cnt; i++ {
		m.Vals = append(m.Vals, make(decimal128Slice, 0))
	}
}

func (m *Decimal128Median) Eval(vs []types.Decimal128, err error) ([]types.Decimal128, error) {
	if err != nil {
		return nil, err
	}
	for i := range vs {
		cnt := len(m.Vals[i])
		if cnt == 0 {
			continue
		}
		if !sort.IsSorted(m.Vals[i]) {
			sort.Sort(m.Vals[i])
		}
		if cnt&1 == 1 {
			vs[i], err = m.Vals[i][cnt>>1].Scale(1)
			if err != nil {
				return nil, err
			}
		} else {
			vs[i], err = m.Vals[i][cnt>>1].Add128(m.Vals[i][(cnt>>1)-1])
			if err != nil {
				return nil, err
			}
			if vs[i].Sign() {
				vs[i] = vs[i].Minus()
				vs[i], err = vs[i].Scale(1)
				vs[i] = vs[i].Right(1).Minus()
			} else {
				vs[i], err = vs[i].Scale(1)
				vs[i] = vs[i].Right(1)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return vs, nil
}

func (m *Decimal128Median) Fill(i int64, value types.Decimal128, _ types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool, error) {
	if !isNull {
		for j := int64(0); j < z; j++ {
			m.Vals[i] = append(m.Vals[i], value)
		}
		return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
	}
	return types.Decimal128{B0_63: 0, B64_127: 0}, isEmpty, nil
}

func (m *Decimal128Median) Merge(xIndex int64, yIndex int64, _ types.Decimal128, _ types.Decimal128, xEmpty bool, yEmpty bool, yMedian any) (types.Decimal128, bool, error) {
	if !yEmpty {
		yM := yMedian.(*Decimal128Median)
		if !sort.IsSorted(yM.Vals[yIndex]) {
			sort.Sort(yM.Vals[yIndex])
		}
		if xEmpty {
			m.Vals[xIndex] = append(m.Vals[xIndex], yM.Vals[yIndex]...)
			return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
		}
		if !sort.IsSorted(m.Vals[xIndex]) {
			sort.Sort(m.Vals[xIndex])
		}
		newCnt := len(m.Vals[xIndex]) + len(yM.Vals[yIndex])
		newData := make(decimal128Slice, newCnt)
		merge(m.Vals[xIndex], yM.Vals[yIndex], newData, func(a, b types.Decimal128) bool { return a.Compare(b) < 0 })
		m.Vals[xIndex] = newData
		return types.Decimal128{B0_63: 0, B64_127: 0}, false, nil
	}
	return types.Decimal128{B0_63: 0, B64_127: 0}, xEmpty, nil
}

func (m *Decimal128Median) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *Decimal128Median) UnmarshalBinary(dt []byte) error {
	return m.Unmarshal(dt)
}

func merge[T Numeric | types.Decimal64 | types.Decimal128](s1, s2, rs []T, lt func(a, b T) bool) []T {
	i, j, cnt := 0, 0, 0
	for i < len(s1) && j < len(s2) {
		if lt(s1[i], s2[j]) {
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
	return rs
}
