// Copyright 2026 Matrix Origin
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

//go:build !amd64 && !arm64

package simdkernels

import "unsafe"

// Decimal64SignExtend widens n Decimal64 (uint64) values to Decimal128 via
// branchless sign extension. Pure Go fallback for unsupported architectures.
func Decimal64SignExtend(dst, src unsafe.Pointer, n int) {
	for i := 0; i < n; i++ {
		val := *(*uint64)(unsafe.Add(src, i*8))
		s := uint64(int64(val) >> 63)
		*(*uint64)(unsafe.Add(dst, i*16)) = val
		*(*uint64)(unsafe.Add(dst, i*16+8)) = s
	}
}
