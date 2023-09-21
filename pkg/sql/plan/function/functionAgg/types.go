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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

type numeric interface {
	types.Ints | types.UInts | types.Floats
}

type compare interface {
	constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Timestamp
}

type maxScaleNumeric interface {
	int64 | uint64 | float64
}

type allTypes interface {
	types.OrderedT | types.Decimal | []byte | bool | types.Uuid
}

type numericSlice[T numeric] []T

func (s numericSlice[T]) Len() int {
	return len(s)
}
func (s numericSlice[T]) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s numericSlice[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type decimal64Slice []types.Decimal64

func (s decimal64Slice) Len() int {
	return len(s)
}
func (s decimal64Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s *decimal64Slice) ToPBVersion() *Decimal64SlicePB {
	return &Decimal64SlicePB{Slice: *s}
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

type decimal128Slice []types.Decimal128

func (s decimal128Slice) Len() int {
	return len(s)
}
func (s decimal128Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal128Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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
