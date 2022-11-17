// Copyright 2021 Matrix Origin
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

package common

type OffsetT interface {
	int | uint32
}

type RowGen interface {
	HasNext() bool
	Next() uint32
}

type RowGenWrapper[T OffsetT] struct {
	Sels []T
	Idx  int
}

func NewRowGenWrapper[T OffsetT](sels ...T) *RowGenWrapper[T] {
	return &RowGenWrapper[T]{
		Sels: sels,
	}
}

func (wrapper *RowGenWrapper[T]) HasNext() bool {
	return wrapper.Idx < len(wrapper.Sels)
}

func (wrapper *RowGenWrapper[T]) Next() uint32 {
	row := wrapper.Sels[wrapper.Idx]
	wrapper.Idx++
	return uint32(row)
}
