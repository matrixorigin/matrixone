// Copyright 2021 - 2023 Matrix Origin
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
	"fmt"
	"unsafe"
)

func UnsafeBytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func UnsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeToBytes[P *T, T any](p P) []byte {
	var zero T
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), unsafe.Sizeof(zero))
}

// Wrapper of unsafe.Slice
func UnsafeToBytesWithLength[P *T, T any](p P, length int) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), length)
}

func UnsafeSliceCast[B any, A any](from []A) []B {
	if len(from) == 0 {
		return nil
	}

	var a A
	var b B
	szA := int(unsafe.Sizeof(a))
	szB := int(unsafe.Sizeof(b))

	lenA := len(from) * szA
	capA := cap(from) * szA

	if lenA%szB != 0 {
		panic(fmt.Sprintf("unsafe slice cast: from length %d is not a multiple of %d", lenA, szB))
	}

	lenB := lenA / szB
	capB := capA / szB

	return unsafe.Slice(
		(*B)(unsafe.Pointer(unsafe.SliceData(from))),
		capB,
	)[:lenB]
}

func UnsafeSliceToBytes[T any](from []T) []byte {
	return UnsafeSliceCast[byte](from)
}

func UnsafeSliceCastToLength[B any, A any](from []A, length int) []B {
	if len(from) == 0 {
		return nil
	}

	var a A
	var b B
	szA := int(unsafe.Sizeof(a))
	szB := int(unsafe.Sizeof(b))

	lenA := len(from) * szA
	capA := cap(from) * szA

	// BUG: #23350
	if lenA < length*szB {
		panic(fmt.Sprintf("unsafe slice cast: from length %d is not enough for length %d", lenA, length*szB))
	}
	// lenA = length * szB
	// lenB := lenA / szB

	// BUG: #23350
	// if lenA%szB != 0 {
	//	panic(fmt.Sprintf("unsafe slice cast: from length %d is not a multiple of %d", lenA, szB))
	// }

	capB := capA / szB
	return unsafe.Slice(
		(*B)(unsafe.Pointer(unsafe.SliceData(from))),
		capB,
	)[:length]
}

func UnsafeUintptr[P *T, T any](p P) uintptr {
	return uintptr(unsafe.Pointer(p))
}

func UnsafePointer[P *T, T any](p P) unsafe.Pointer {
	return unsafe.Pointer(p)
}
