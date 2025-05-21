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

func UnsafeSliceCast[B any, From []A, A any, To []B](from From) To {
	return unsafe.Slice(
		(*B)(unsafe.Pointer(unsafe.SliceData(from))),
		cap(from),
	)[:len(from)]
}

func UnsafeUintptr[P *T, T any](p P) uintptr {
	return uintptr(unsafe.Pointer(p))
}
