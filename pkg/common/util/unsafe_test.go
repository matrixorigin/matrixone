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
	s := UnsafeSliceCast[int32]([]int64{1, 2})
	// this is endian dependent, but we only support little endian anyway.
	assert.Equal(t, []int32{1, 0, 2, 0}, s)
	s = UnsafeSliceCast[int32, []int64](nil)
	assert.Equal(t, []int32(nil), s)

	si := UnsafeSliceCast[int]([]int64{1, 2})
	// this is int size dependent.   the following test says we only support go int is int64.
	assert.Equal(t, []int{1, 2}, si)
	si = UnsafeSliceCast[int, []int64](nil)
	assert.Equal(t, []int(nil), si)

}

func TestUnsafeUintptr(t *testing.T) {
	a := int(100)
	ptr := UnsafeUintptr(&a)
	assert.NotEqual(t, ptr, uintptr(0))
}

func TestUnsafeFromBytes(t *testing.T) {
	b := [8]byte{}
	v := int64(0x0102030405060708)
	bs := UnsafeToBytes(&v)
	copy(b[:], bs)
	p := UnsafeFromBytes[int64](b[:])
	assert.Equal(t, v, *p)
}

func TestUnsafeSliceToBytes(t *testing.T) {
	bs := UnsafeSliceToBytes([]int32{1, 2})
	assert.Equal(t, 8, len(bs))

	bs = UnsafeSliceToBytes[int32](nil)
	assert.Equal(t, []byte(nil), bs)
}

func TestUnsafeSliceCastToLength(t *testing.T) {
	src := []int64{1, 2}
	out := UnsafeSliceCastToLength[int32](src, 4)
	assert.Equal(t, []int32{1, 0, 2, 0}, out)

	out2 := UnsafeSliceCastToLength[int32, int64](nil, 0)
	assert.Equal(t, []int32(nil), out2)

	assert.Panics(t, func() {
		UnsafeSliceCastToLength[int64](src, 4)
	})
}

func TestUnsafePointer(t *testing.T) {
	a := int(100)
	p := UnsafePointer(&a)
	assert.NotNil(t, p)
}

func TestUnsafeSizeOf(t *testing.T) {
	assert.Equal(t, uintptr(8), UnsafeSizeOf[int64]())
	assert.Equal(t, uintptr(4), UnsafeSizeOf[int32]())
	assert.Equal(t, uintptr(1), UnsafeSizeOf[byte]())
}

func TestUnsafeSlice(t *testing.T) {
	src := []int32{1, 2, 3, 4}
	ptr := UnsafePointer(&src[0])
	out := UnsafeSlice[int32](ptr, 4)
	assert.Equal(t, []int32{1, 2, 3, 4}, out)
	assert.Equal(t, 4, len(out))
}

func TestUnsafeSliceCastPanic(t *testing.T) {
	assert.Panics(t, func() {
		// 3 bytes cannot evenly fit in int32
		UnsafeSliceCast[int32]([]byte{1, 2, 3})
	})
}
