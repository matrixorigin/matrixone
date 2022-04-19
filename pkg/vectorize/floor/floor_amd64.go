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

//go:build amd64
// +build amd64

package floor

import (
	"golang.org/x/sys/cpu"
)

func float32FloorAvx2Asm(xs, rs []float32, scale float32)
func float32FloorAvx2AsmZero(xs, rs []float32)
func float64FloorAvx2Asm(xs, rs []float64, scale float64)
func float64FloorAvx2AsmZero(xs, rs []float64)

func init() {
	if cpu.X86.HasAVX2 {
		FloorFloat32 = floorFloat32Avx2
		FloorFloat64 = floorFloat64Avx2
	}
}

func floorFloat32Avx2(xs []float32, rs []float32, digits int64) []float32 {
	if digits == 0 {
		float32FloorAvx2AsmZero(xs, rs)
	} else if digits > 0 {
		scale := float32(ScaleTable[digits])
		float32FloorAvx2Asm(xs, rs, scale)
	} else {
		scale := float32(ScaleTable[-digits])
		float32FloorAvx2Asm(xs, rs, 1/scale)
	}
	return rs
}

func floorFloat64Avx2(xs []float64, rs []float64, digits int64) []float64 {
	if digits == 0 {
		float64FloorAvx2AsmZero(xs, rs)
	} else if digits > 0 {
		scale := float64(ScaleTable[digits])
		float64FloorAvx2Asm(xs, rs, scale)
	} else {
		scale := float64(ScaleTable[-digits])
		float64FloorAvx2Asm(xs, rs, 1/scale)
	}
	return rs
}
