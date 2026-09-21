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

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWideL2DistanceSqFloat32PreservesRangeAndPrecision(t *testing.T) {
	ordinary, err := wideL2DistanceSqFloat32([]float32{3, 4}, []float32{0, 0})
	require.NoError(t, err)
	require.Equal(t, float64(25), ordinary)

	huge := []float32{3e38, 3e38}
	hugeDistance, err := wideL2DistanceSqFloat32(huge, []float32{0, 0})
	require.NoError(t, err)
	require.InEpsilon(t, 2*float64(huge[0])*float64(huge[0]), hugeDistance, 1e-14)

	tiny := []float32{math.SmallestNonzeroFloat32, math.SmallestNonzeroFloat32}
	tinyDistance, err := wideL2DistanceSqFloat32(tiny, []float32{0, 0})
	require.NoError(t, err)
	require.Equal(t, 2*float64(tiny[0])*float64(tiny[0]), tinyDistance)
	require.Greater(t, tinyDistance, float64(0))

	// A float32 accumulator rounds the second term away at this scale. The
	// wide path must preserve the ordering seen by the float64 result type.
	precise, err := wideL2DistanceSqFloat32(
		[]float32{1, math.Float32frombits(math.Float32bits(1) - 1)},
		[]float32{0, 0},
	)
	require.NoError(t, err)
	require.Greater(t, precise, float64(1))
}

func TestWideL2DistanceSqFloat32PreservesNonFiniteSemantics(t *testing.T) {
	cases := []struct {
		name  string
		p     []float32
		q     []float32
		check func(*testing.T, float64)
	}{
		{
			name: "nan",
			p:    []float32{float32(math.NaN()), 0},
			q:    []float32{0, 0},
			check: func(t *testing.T, got float64) {
				require.True(t, math.IsNaN(got))
			},
		},
		{
			name: "inf",
			p:    []float32{float32(math.Inf(1)), 0},
			q:    []float32{0, 0},
			check: func(t *testing.T, got float64) {
				require.True(t, math.IsInf(got, 1))
			},
		},
		{
			name: "inf-minus-inf",
			p:    []float32{float32(math.Inf(1)), 0},
			q:    []float32{float32(math.Inf(1)), 0},
			check: func(t *testing.T, got float64) {
				require.True(t, math.IsNaN(got))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wideL2DistanceSqFloat32(tc.p, tc.q)
			require.NoError(t, err)
			tc.check(t, got)
		})
	}

	_, err := wideL2DistanceSqFloat32([]float32{1}, nil)
	require.Error(t, err)
}

func TestResolveDistanceFnWideFloat32L2UsesWideRange(t *testing.T) {
	input := []float32{3e38, 3e38}
	zero := []float32{0, 0}
	want := 2 * float64(input[0]) * float64(input[0])

	for _, tc := range []struct {
		name   string
		metric MetricType
	}{
		{name: "l2", metric: Metric_L2Distance},
		{name: "l2sq", metric: Metric_L2sqDistance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := ResolveDistanceFn[float32, float64](tc.metric)
			require.NoError(t, err)
			got, err := fn(input, zero)
			require.NoError(t, err)
			require.InEpsilon(t, want, got, 1e-14)
		})
	}
}

func BenchmarkWideL2DistanceSqFloat32(b *testing.B) {
	p := make([]float32, 768)
	q := make([]float32, 768)
	for i := range p {
		p[i] = float32(i%31) / 31
		q[i] = float32((i+7)%29) / 29
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = wideL2DistanceSqFloat32(p, q)
	}
}

func BenchmarkStableL2DistanceSqFloat32(b *testing.B) {
	p := make([]float32, 768)
	q := make([]float32, 768)
	for i := range p {
		p[i] = float32(i%31) / 31
		q[i] = float32((i+7)%29) / 29
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = StableL2DistanceSq(p, q)
	}
}
