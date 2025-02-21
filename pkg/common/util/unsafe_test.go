// Copyright 2021 - 2024 Matrix Origin
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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafeBytesToString(t *testing.T) {
	s := UnsafeBytesToString(nil)
	assert.Equal(t, "", s)
	s = UnsafeBytesToString([]byte{})
	assert.Equal(t, "", s)
	s = UnsafeBytesToString([]byte{'a', 'b'})
	assert.Equal(t, "ab", s)
}

func TestUnsafeStringToBytes(t *testing.T) {
	s := UnsafeStringToBytes("")
	assert.Equal(t, []byte(nil), s)
	s = UnsafeStringToBytes("ab")
	assert.Equal(t, []byte("ab"), s)
}

func TestUnsafeToBytes(t *testing.T) {
	var a struct {
		_ [64]int64
	}
	bs := UnsafeToBytes(&a)
	assert.Equal(t, 64*8, len(bs))
}

func TestUnsafeToBytesWithLength(t *testing.T) {
	b := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	bs := UnsafeToBytesWithLength(&b[4], 4)
	assert.Equal(t, 4, len(bs))
	for i := 4; i < 8; i++ {
		assert.Equal(t, bs[i-4], byte(i))
	}
}

func TestUnsafeSliceCast(t *testing.T) {
	s := UnsafeSliceCast[int]([]int64{1, 2})
	assert.Equal(t, []int{1, 2}, s)
	s = UnsafeSliceCast[int]([]int64(nil))
	assert.Equal(t, []int(nil), s)
}

func TestUnsafeUintptr(t *testing.T) {
	a := int(100)
	ptr := UnsafeUintptr(&a)
	assert.NotEqual(t, ptr, uintptr(0))
}
