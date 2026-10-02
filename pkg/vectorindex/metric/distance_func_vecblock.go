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

package metric

//go:generate go test -run TestVecBlockKernelsGenerated -args -update

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// VecBlockOperand is one distance argument: a vecf8/vecf4 cell, or a vecf32 array in F32
// when Cell.Dim is 0.
type VecBlockOperand struct {
	Cell types.BlockScaledCell
	F32  []float32
}

// Dim returns the operand's dimension.
func (o *VecBlockOperand) Dim() int {
	if o.Cell.Dim != 0 {
		return o.Cell.Dim
	}
	return len(o.F32)
}

func (o *VecBlockOperand) at(i int) float64 {
	if o.Cell.Dim != 0 {
		return float64(o.Cell.At(i))
	}
	return float64(o.F32[i])
}

// vecBlockUnit is the generated kernels' element count per step; it divides both block sizes.
const vecBlockUnit = 16

const (
	vecBlockF8F8 = iota
	vecBlockF4F4
	vecBlockF8F32
	vecBlockF4F32
	vecBlockF8F4
	vecBlockF32F32
)

// vecBlockPlan checks the dimensions and orders x, y into a generated kernel's operand pair;
// swapped reports that x and y were exchanged.
func vecBlockPlan(x, y *VecBlockOperand) (px, py *VecBlockOperand, pair, n int, swapped bool, err error) {
	n = x.Dim()
	if m := y.Dim(); n != m {
		return nil, nil, 0, 0, false, moerr.NewArrayInvalidOpNoCtx(n, m)
	}
	rank := func(o *VecBlockOperand) int {
		switch o.Cell.Format {
		case types.BlockScaledMXFP8:
			return 2
		case types.BlockScaledNVFP4:
			return 1
		}
		return 0
	}
	rx, ry := rank(x), rank(y)
	if rx < ry {
		x, y, rx, ry, swapped = y, x, ry, rx, true
	}
	switch {
	case rx == 2 && ry == 2:
		pair = vecBlockF8F8
	case rx == 1 && ry == 1:
		pair = vecBlockF4F4
	case rx == 2 && ry == 0:
		pair = vecBlockF8F32
	case rx == 1 && ry == 0:
		pair = vecBlockF4F32
	case rx == 2 && ry == 1:
		pair = vecBlockF8F4
	default:
		pair = vecBlockF32F32
	}
	return x, y, pair, n, swapped, nil
}

// VecBlockDot returns the dot product of x and y.
func VecBlockDot(x, y *VecBlockOperand) (float64, error) {
	x, y, pair, n, _, err := vecBlockPlan(x, y)
	if err != nil {
		return 0, err
	}
	units, r := n/vecBlockUnit, 0.0
	switch pair {
	case vecBlockF8F8:
		r = vecBlockDotF8F8(&x.Cell, &y.Cell, units)
	case vecBlockF4F4:
		r = vecBlockDotF4F4(&x.Cell, &y.Cell, units)
	case vecBlockF8F32:
		r = vecBlockDotF8F32(&x.Cell, y.F32, units)
	case vecBlockF4F32:
		r = vecBlockDotF4F32(&x.Cell, y.F32, units)
	case vecBlockF8F4:
		r = vecBlockDotF8F4(&x.Cell, &y.Cell, units)
	default:
		units = 0
	}
	for i := units * vecBlockUnit; i < n; i++ {
		r += x.at(i) * y.at(i)
	}
	return r, nil
}

// VecBlockL2DistanceSq returns the squared L2 distance of x and y.
func VecBlockL2DistanceSq(x, y *VecBlockOperand) (float64, error) {
	x, y, pair, n, _, err := vecBlockPlan(x, y)
	if err != nil {
		return 0, err
	}
	units, r := n/vecBlockUnit, 0.0
	switch pair {
	case vecBlockF8F8:
		r = vecBlockL2SqF8F8(&x.Cell, &y.Cell, units)
	case vecBlockF4F4:
		r = vecBlockL2SqF4F4(&x.Cell, &y.Cell, units)
	case vecBlockF8F32:
		r = vecBlockL2SqF8F32(&x.Cell, y.F32, units)
	case vecBlockF4F32:
		r = vecBlockL2SqF4F32(&x.Cell, y.F32, units)
	case vecBlockF8F4:
		r = vecBlockL2SqF8F4(&x.Cell, &y.Cell, units)
	default:
		units = 0
	}
	for i := units * vecBlockUnit; i < n; i++ {
		d := x.at(i) - y.at(i)
		r += d * d
	}
	return r, nil
}

// VecBlockL1Distance returns the L1 distance of x and y.
func VecBlockL1Distance(x, y *VecBlockOperand) (float64, error) {
	x, y, pair, n, _, err := vecBlockPlan(x, y)
	if err != nil {
		return 0, err
	}
	units, r := n/vecBlockUnit, 0.0
	switch pair {
	case vecBlockF8F8:
		r = vecBlockL1F8F8(&x.Cell, &y.Cell, units)
	case vecBlockF4F4:
		r = vecBlockL1F4F4(&x.Cell, &y.Cell, units)
	case vecBlockF8F32:
		r = vecBlockL1F8F32(&x.Cell, y.F32, units)
	case vecBlockF4F32:
		r = vecBlockL1F4F32(&x.Cell, y.F32, units)
	case vecBlockF8F4:
		r = vecBlockL1F8F4(&x.Cell, &y.Cell, units)
	default:
		units = 0
	}
	for i := units * vecBlockUnit; i < n; i++ {
		r += math.Abs(x.at(i) - y.at(i))
	}
	return r, nil
}

// VecBlockCosineParts returns the dot product and the squared norms of x and y.
func VecBlockCosineParts(x, y *VecBlockOperand) (dot, nx, ny float64, err error) {
	x, y, pair, n, swapped, err := vecBlockPlan(x, y)
	if err != nil {
		return 0, 0, 0, err
	}
	units := n / vecBlockUnit
	switch pair {
	case vecBlockF8F8:
		dot, nx, ny = vecBlockCosF8F8(&x.Cell, &y.Cell, units)
	case vecBlockF4F4:
		dot, nx, ny = vecBlockCosF4F4(&x.Cell, &y.Cell, units)
	case vecBlockF8F32:
		dot, nx, ny = vecBlockCosF8F32(&x.Cell, y.F32, units)
	case vecBlockF4F32:
		dot, nx, ny = vecBlockCosF4F32(&x.Cell, y.F32, units)
	case vecBlockF8F4:
		dot, nx, ny = vecBlockCosF8F4(&x.Cell, &y.Cell, units)
	default:
		units = 0
	}
	for i := units * vecBlockUnit; i < n; i++ {
		a, b := x.at(i), y.at(i)
		dot += a * b
		nx += a * a
		ny += b * b
	}
	if swapped {
		nx, ny = ny, nx
	}
	return dot, nx, ny, nil
}

// vecBlockNaNToPosInf maps a NaN distance to +Inf. A unit accumulates in float32 lanes:
// finite products can overflow one lane to +Inf and another to -Inf, and their sum is
// NaN. +Inf ranks the overflowing candidate last; a genuine +-Inf is left as is.
func vecBlockNaNToPosInf(d float64) float64 {
	if math.IsNaN(d) {
		return math.Inf(1)
	}
	return d
}

// VecBlockInnerProduct returns the inner product distance -dot(x, y); NaN maps to +Inf.
func VecBlockInnerProduct(x, y *VecBlockOperand) (float64, error) {
	dot, err := VecBlockDot(x, y)
	if err != nil {
		return 0, err
	}
	return vecBlockNaNToPosInf(-dot), nil
}

// VecBlockCosineSimilarity returns dot/(|x|*|y|) clamped to [-1, 1]. A zero vector is an error.
func VecBlockCosineSimilarity(x, y *VecBlockOperand) (float64, error) {
	dot, nx, ny, err := VecBlockCosineParts(x, y)
	if err != nil {
		return 0, err
	}
	den := math.Sqrt(nx) * math.Sqrt(ny)
	if den == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	return max(-1, min(1, dot/den)), nil
}

// VecBlockCosineDistance returns 1 - cosine similarity; 1 when either vector is zero.
// NaN maps to +Inf.
func VecBlockCosineDistance(x, y *VecBlockOperand) (float64, error) {
	dot, nx, ny, err := VecBlockCosineParts(x, y)
	if err != nil {
		return 0, err
	}
	den := math.Sqrt(nx) * math.Sqrt(ny)
	if den == 0 {
		return 1, nil
	}
	return vecBlockNaNToPosInf(1 - max(-1, min(1, dot/den))), nil
}
