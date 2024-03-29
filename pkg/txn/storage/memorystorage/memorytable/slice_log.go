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

package memorytable

type SliceLog[
	K Ordered[K],
	V any,
] []*logEntry[K, V]

func NewSliceLog[
	K Ordered[K],
	V any,
]() *SliceLog[K, V] {
	return new(SliceLog[K, V])
}

var _ Log[Int, int] = new(SliceLog[Int, int])

func (s *SliceLog[K, V]) Set(entry *logEntry[K, V]) {
	*s = append(*s, entry)
}

type sliceLogIter[
	K Ordered[K],
	V any,
] struct {
	log   *SliceLog[K, V]
	index int
}

func (s *SliceLog[K, V]) Iter() LogIter[K, V] {
	return &sliceLogIter[K, V]{
		log:   s,
		index: 0,
	}
}

func (s *sliceLogIter[K, V]) Close() error {
	return nil
}

func (s *sliceLogIter[K, V]) First() bool {
	s.index = 0
	return s.index < len(*s.log)
}

func (s *sliceLogIter[K, V]) Next() bool {
	s.index++
	return s.index < len(*s.log)
}

func (s *sliceLogIter[K, V]) Read() (*logEntry[K, V], error) {
	return (*s.log)[s.index], nil
}
