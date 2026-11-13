// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diagnostic

import (
	"math"
	"testing"
)

func closeTo(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-10*math.Max(1, math.Abs(want)) {
		t.Fatalf("got %g, want %g", got, want)
	}
}

func TestMoIndexStatsDistinguishesSpreadFromCenterMismatch(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		values               []float64
		shift, mismatch, rms float64
	}{
		{"spread", []float64{-10, 10}, 0, 0, 10},
		{"shifted", []float64{9, 11}, 10, 100.0 / 101, math.Sqrt(101)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBucket(1, []float64{0})
			for _, x := range tc.values {
				if err := b.Add([]float64{x}); err != nil {
					t.Fatal(err)
				}
			}
			g, ok := b.Geometry()
			if !ok {
				t.Fatal("geometry unavailable")
			}
			closeTo(t, g.MeanResidual, 10)
			closeTo(t, g.CenterShift, tc.shift)
			closeTo(t, g.Mismatch, tc.mismatch)
			closeTo(t, g.RMSResidual, tc.rms)
		})
	}
}

func TestMoIndexStatsSummaryWeightsLossAndIncludesEmptyBuckets(t *testing.T) {
	shifted := NewBucket(1, []float64{0})
	if err := shifted.Add([]float64{10}); err != nil {
		t.Fatal(err)
	}
	centered := NewBucket(2, []float64{0})
	for i := 0; i < 100; i++ {
		x := float64(1)
		if i%2 == 0 {
			x = -1
		}
		if err := centered.Add([]float64{x}); err != nil {
			t.Fatal(err)
		}
	}
	s := Summarize([]*Bucket{shifted, centered, NewBucket(3, []float64{0})})
	if s.Count != 101 || s.Centroids != 3 || s.Empty != 1 || s.Min != 0 || s.Max != 100 || !s.HasGeometry {
		t.Fatalf("incorrect summary: %+v", s)
	}
	closeTo(t, s.Geometry.Mismatch, 0.5)
	closeTo(t, s.Geometry.RMSResidual, math.Sqrt(200.0/101))
	closeTo(t, s.Geometry.CenterShift, math.Sqrt(100.0/101))
	closeTo(t, s.Geometry.MeanResidual, 110.0/101)
	mean := 101.0 / 3
	wantCV := math.Sqrt(((1-mean)*(1-mean)+(100-mean)*(100-mean)+mean*mean)/3) / mean
	closeTo(t, s.CV, wantCV)
}

func TestMoIndexStatsZeroResidualAndIncompleteGeometry(t *testing.T) {
	b := NewBucket(1, []float64{1e12, 1e12})
	for _, x := range [][]float64{{1e12 - 1, 1e12}, {1e12 + 1, 1e12}} {
		if err := b.Add(x); err != nil {
			t.Fatal(err)
		}
	}
	g, ok := b.Geometry()
	if !ok {
		t.Fatal("missing geometry")
	}
	closeTo(t, g.Mismatch, 0)
	closeTo(t, g.RMSResidual, 1)
	zero := NewBucket(2, []float64{0})
	if err := zero.Add([]float64{0}); err != nil {
		t.Fatal(err)
	}
	g, ok = zero.Geometry()
	if !ok || g.Mismatch != 0 {
		t.Fatalf("zero residual should produce zero mismatch: %+v", g)
	}
	if err := zero.Add(nil); err != nil {
		t.Fatal(err)
	}
	if Summarize([]*Bucket{b, zero}).HasGeometry {
		t.Fatal("incomplete data must not produce whole-index geometry")
	}
	if s := Summarize([]*Bucket{NewBucket(3, nil)}); s.Empty != 1 || s.HasGeometry || s.CV != 0 {
		t.Fatalf("invalid empty summary: %+v", s)
	}
}

func TestMoIndexStatsRejectsInvalidVectors(t *testing.T) {
	for _, x := range [][]float64{{1, 2}, {math.NaN()}, {math.Inf(1)}} {
		b := NewBucket(1, []float64{0})
		if err := b.Add(x); err == nil || b.Count != 0 {
			t.Fatalf("invalid input accepted: %v", x)
		}
	}
}

func TestMoIndexStatsMismatchInvariantToUniformScaling(t *testing.T) {
	for _, scale := range []float64{.001, 1, 1000} {
		bucket := NewBucket(1, []float64{3 * scale})
		for _, x := range []float64{12 * scale, 14 * scale} {
			if err := bucket.Add([]float64{x}); err != nil {
				t.Fatal(err)
			}
		}
		g, ok := bucket.Geometry()
		if !ok {
			t.Fatal("missing mismatch measurement")
		}
		closeTo(t, g.Mismatch, 100.0/101)
		closeTo(t, Summarize([]*Bucket{bucket}).Geometry.Mismatch, 100.0/101)
	}
}
