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

package simdkernels

// Decimal64 element-wise comparisons producing []bool output (1 byte per
// element; 0 = false, 1 = true).
//
// Inputs a, b are uint64-typed slices interpreted as int64 for signed
// compares (Decimal64 is two's-complement int64 with sign-bit-checked
// negativity, so signed integer ordering matches Decimal64.Less ordering).

var (
	D64Eq func(a, b []uint64, out []bool) = scalarD64Eq
	D64Lt func(a, b []uint64, out []bool) = scalarD64Lt
)

func scalarD64Eq(a, b []uint64, out []bool) {
	n := len(a)
	if len(b) < n || len(out) < n {
		return
	}
	for i := 0; i < n; i++ {
		out[i] = a[i] == b[i]
	}
}

func scalarD64Lt(a, b []uint64, out []bool) {
	n := len(a)
	if len(b) < n || len(out) < n {
		return
	}
	for i := 0; i < n; i++ {
		out[i] = int64(a[i]) < int64(b[i])
	}
}
