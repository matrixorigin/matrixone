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

// Package diagnostic computes IVF statistics and bounded health measurements.
// It never writes index state.
package diagnostic

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Bucket struct {
	ID, Count, Valid int64
	Center           []float64
	// Resolution is the per-coordinate reconstruction uncertainty (zero for exact input).
	Resolution     float64
	meanDelta      []float64
	scale, squared float64
	status         string
}

func NewBucket(id int64, center []float64) *Bucket {
	return &Bucket{ID: id, Center: center, meanDelta: make([]float64, len(center))}
}

// Add counts entries independently of geometry. Scaled squares avoid overflow
// in a dimensionless measurement; unrepresentable subtraction disables only geometry.
func (b *Bucket) Add(x []float64) error {
	if x != nil && b.Center != nil && len(x) != len(b.Center) {
		return moerr.NewInvalidInputNoCtxf("centroid %d: vector dimension %d differs from center dimension %d", b.ID, len(x), len(b.Center))
	}
	for _, v := range x {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return moerr.NewInvalidInputNoCtxf("centroid %d: non-finite vector component", b.ID)
		}
	}
	b.Count++
	if x == nil || len(b.Center) == 0 || b.status != "" {
		return nil
	}
	b.Valid++
	n := float64(b.Valid)
	for i, v := range x {
		delta := v - b.Center[i]
		if math.IsInf(delta, 0) {
			b.status = "center_stats_overflow"
			return nil
		}
		b.meanDelta[i] = b.meanDelta[i]*(1-1/n) + delta/n
		if math.IsInf(b.meanDelta[i], 0) || math.IsNaN(b.meanDelta[i]) {
			b.status = "center_stats_overflow"
			return nil
		}
		a := math.Abs(delta)
		if a > b.scale {
			r := b.scale / a
			b.squared = b.squared * r * r
			b.scale = a
		}
		if b.scale > 0 {
			r := delta / b.scale
			b.squared += r * r
		}
	}
	return nil
}

// Geometry contains only the dimensionless fixed-partition L2 mismatch.
// It does not estimate recall or reconstruction of clipped original vectors.
type Geometry struct{ Mismatch float64 }

func (b *Bucket) GeometryStatus() string {
	if b.status != "" {
		return b.status
	}
	if b.Resolution > 0 && b.Count > 0 && b.Valid == b.Count && len(b.Center) > 0 {
		if b.scale == 0 {
			return "center_stats_quantization_limited"
		}
		r := b.Resolution / b.scale
		if b.squared <= float64(b.Count)*float64(len(b.Center))*r*r {
			return "center_stats_quantization_limited"
		}
	}
	return ""
}

func (b *Bucket) Geometry() (Geometry, bool) {
	if b.Count == 0 || b.Valid != b.Count || b.GeometryStatus() != "" {
		return Geometry{}, false
	}
	var shift float64
	if b.scale > 0 {
		for _, v := range b.meanDelta {
			r := v / b.scale
			shift += r * r
		}
	}
	ratio := float64(0)
	if b.squared > 0 {
		ratio = math.Min(1, float64(b.Count)*shift/b.squared)
	}
	return Geometry{Mismatch: ratio}, true
}

type Summary struct {
	Count, Centroids, Empty, Min, Max int64
	CV                                float64
	Geometry                          Geometry
	HasGeometry                       bool
	Status                            string
}

func Summarize(buckets []*Bucket) Summary {
	s := Summary{Centroids: int64(len(buckets))}
	var mean, m2, scale, squared, numerator float64
	complete := true
	for i, b := range buckets {
		s.Count += b.Count
		if b.Count == 0 {
			s.Empty++
		}
		if i == 0 || b.Count < s.Min {
			s.Min = b.Count
		}
		s.Max = max(s.Max, b.Count)
		delta := float64(b.Count) - mean
		mean += delta / float64(i+1)
		m2 += delta * (float64(b.Count) - mean)
		if b.Count == 0 {
			continue
		}
		g, ok := b.Geometry()
		complete = complete && ok
		if status := b.GeometryStatus(); s.Status == "" {
			s.Status = status
		}
		if b.scale > scale {
			r := scale / b.scale
			squared *= r * r
			numerator *= r * r
			scale = b.scale
		}
		if scale > 0 {
			r := b.scale / scale
			weight := b.squared * r * r
			squared += weight
			numerator += weight * g.Mismatch
		}
	}
	if mean > 0 {
		s.CV = math.Sqrt(m2/float64(len(buckets))) / mean
	}
	if complete && s.Count > 0 {
		s.HasGeometry = true
		if squared > 0 {
			s.Geometry.Mismatch = math.Min(1, numerator/squared)
		}
	}
	return s
}
