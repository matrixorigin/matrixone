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

package round

import (
	"math"

	"golang.org/x/sys/cpu"
)

func float32RoundAvx2Asm(xs, rs []float32, scale float32)
func float32RoundAvx2AsmZero(xs, rs []float32)
func float64RoundAvx2Asm(xs, rs []float64, scale float64)
func float64RoundAvx2AsmZero(xs, rs []float64)

func init() {
	if cpu.X86.HasAVX2 {
		RoundFloat32 = roundFloat32Avx2
		RoundFloat64 = roundFloat64Avx2
	}
}

func roundFloat32Avx2(xs []float32, rs []float32, digits int64) []float32 {
	if digits == 0 {
		float32RoundAvx2AsmZero(xs, rs)
	} else if digits >= 38 { // because the range of float32:1.2E-38 to 3.4E+38
		for i := range xs {
			rs[i] = xs[i]
		}
	} else if digits <= -38 {
		for i := range xs {
			rs[i] = 0
		}
	} else {
		scale := float32(math.Pow10(int(digits)))
		float32RoundAvx2Asm(xs, rs, scale)
	}
	return rs
}

func roundFloat64Avx2(xs []float64, rs []float64, digits int64) []float64 {
	if digits == 0 {
		float64RoundAvx2AsmZero(xs, rs)
	} else if digits >= 308 { // because of the float64 range: -1.7e+308 to +1.7e+308.
		for i := range xs {
			rs[i] = xs[i]
		}
	} else if digits <= -308 {
		for i := range xs {
			rs[i] = 0
		}
	} else {
		scale := math.Pow10(int(digits))
		float64RoundAvx2Asm(xs, rs, scale)
	}
	return rs
}
