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

package types

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloatOrderComparators(t *testing.T) {
	float32Values := []float32{
		float32(math.Inf(-1)), -1, math.Float32frombits(0x80000000), 0, 1, float32(math.Inf(1)),
		math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002),
	}
	assertFloat32ComparatorOrder(t, float32Values)
	require.Zero(t, Float32OrderAscCompare(math.Float32frombits(0x80000000), 0))
	require.Zero(t, Float32OrderAscCompare(float32Values[6], float32Values[7]))
	require.Zero(t, Float32OrderDescCompare(float32Values[6], float32Values[7]))

	float64Values := []float64{
		math.Inf(-1), -1, math.Float64frombits(0x8000000000000000), 0, 1, math.Inf(1),
		math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002),
	}
	assertFloat64ComparatorOrder(t, float64Values)
	require.Zero(t, Float64OrderAscCompare(math.Float64frombits(0x8000000000000000), 0))
	require.Zero(t, Float64OrderAscCompare(float64Values[6], float64Values[7]))
	require.Zero(t, Float64OrderDescCompare(float64Values[6], float64Values[7]))
}

func TestFloatTupleComparatorsMatchPackerOrder(t *testing.T) {
	float32Values := []float32{
		math.Float32frombits(math.MaxUint32),
		math.Float32frombits(0xffc00002),
		math.Float32frombits(0xff800001),
		float32(math.Inf(-1)),
		-math.MaxFloat32,
		math.Float32frombits(0x80000000),
		0,
		math.MaxFloat32,
		float32(math.Inf(1)),
		math.Float32frombits(0x7f800001),
		math.Float32frombits(0x7fc00001),
		math.Float32frombits(math.MaxInt32),
	}
	assertFloat32TupleComparatorMatchesPacker(t, float32Values)

	float64Values := []float64{
		math.Float64frombits(math.MaxUint64),
		math.Float64frombits(0xfff8000000000002),
		math.Float64frombits(0xfff0000000000001),
		math.Inf(-1),
		-math.MaxFloat64,
		math.Float64frombits(0x8000000000000000),
		0,
		math.MaxFloat64,
		math.Inf(1),
		math.Float64frombits(0x7ff0000000000001),
		math.Float64frombits(0x7ff8000000000001),
		math.Float64frombits(math.MaxInt64),
	}
	assertFloat64TupleComparatorMatchesPacker(t, float64Values)
}

func assertFloat32TupleComparatorMatchesPacker(t *testing.T, values []float32) {
	t.Helper()
	xp := NewPacker()
	defer xp.Close()
	yp := NewPacker()
	defer yp.Close()
	for i, x := range values {
		for j, y := range values {
			xp.Reset()
			xp.EncodeFloat32(x)
			yp.Reset()
			yp.EncodeFloat32(y)
			require.Equal(t, bytes.Compare(xp.Bytes(), yp.Bytes()), Float32TupleAscCompare(x, y),
				"pair %d,%d", i, j)
		}
	}
}

func assertFloat64TupleComparatorMatchesPacker(t *testing.T, values []float64) {
	t.Helper()
	xp := NewPacker()
	defer xp.Close()
	yp := NewPacker()
	defer yp.Close()
	for i, x := range values {
		for j, y := range values {
			xp.Reset()
			xp.EncodeFloat64(x)
			yp.Reset()
			yp.EncodeFloat64(y)
			require.Equal(t, bytes.Compare(xp.Bytes(), yp.Bytes()), Float64TupleAscCompare(x, y),
				"pair %d,%d", i, j)
		}
	}
}

func assertFloat32ComparatorOrder(t *testing.T, values []float32) {
	t.Helper()
	for i, x := range values {
		for j, y := range values {
			asc := Float32OrderAscCompare(x, y)
			desc := Float32OrderDescCompare(x, y)
			require.Equal(t, -asc, Float32OrderAscCompare(y, x), "asc pair %d,%d", i, j)
			require.Equal(t, -desc, Float32OrderDescCompare(y, x), "desc pair %d,%d", i, j)
			if i < j && !(i == 2 && j == 3) && !(i >= 6 && j >= 6) {
				require.Negative(t, asc, "asc pair %d,%d", i, j)
				if j >= 6 {
					require.Negative(t, desc, "NaNs remain last for desc pair %d,%d", i, j)
				} else {
					require.Positive(t, desc, "desc pair %d,%d", i, j)
				}
			}
		}
	}
	assertComparatorTransitive(t, values, Float32OrderAscCompare)
	assertComparatorTransitive(t, values, Float32OrderDescCompare)
}

func assertFloat64ComparatorOrder(t *testing.T, values []float64) {
	t.Helper()
	for i, x := range values {
		for j, y := range values {
			asc := Float64OrderAscCompare(x, y)
			desc := Float64OrderDescCompare(x, y)
			require.Equal(t, -asc, Float64OrderAscCompare(y, x), "asc pair %d,%d", i, j)
			require.Equal(t, -desc, Float64OrderDescCompare(y, x), "desc pair %d,%d", i, j)
			if i < j && !(i == 2 && j == 3) && !(i >= 6 && j >= 6) {
				require.Negative(t, asc, "asc pair %d,%d", i, j)
				if j >= 6 {
					require.Negative(t, desc, "NaNs remain last for desc pair %d,%d", i, j)
				} else {
					require.Positive(t, desc, "desc pair %d,%d", i, j)
				}
			}
		}
	}
	assertComparatorTransitive(t, values, Float64OrderAscCompare)
	assertComparatorTransitive(t, values, Float64OrderDescCompare)
}

func assertComparatorTransitive[T any](t *testing.T, values []T, compare func(T, T) int) {
	t.Helper()
	for i := range values {
		for j := range values {
			for k := range values {
				if compare(values[i], values[j]) <= 0 && compare(values[j], values[k]) <= 0 {
					require.LessOrEqual(t, compare(values[i], values[k]), 0, "triple %d,%d,%d", i, j, k)
				}
			}
		}
	}
}
