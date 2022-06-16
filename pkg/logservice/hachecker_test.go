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

package logservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDAllocatorDefaultState(t *testing.T) {
	alloc := newIDAllocator()
	assert.Equal(t, uint64(0), alloc.Capacity())
	v, ok := alloc.Next()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)
}

func TestIDAllocatorCapacity(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		assert.Equal(t, tt.capacity, alloc.Capacity())
	}
}

func TestIDAllocatorSet(t *testing.T) {
	alloc := idAllocator{nextID: 100, lastID: 200}
	alloc.Set(200, 300)
	assert.Equal(t, idAllocator{nextID: 200, lastID: 300}, alloc)
}

func TestIDAllocatorNext(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		expected := tt.next
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		for {
			hasID := alloc.Capacity() != 0
			v, ok := alloc.Next()
			assert.Equal(t, hasID, ok)
			if hasID {
				assert.Equal(t, expected, v)
				expected++
			} else {
				assert.Equal(t, uint64(0), v)
				break
			}
		}
	}
}
