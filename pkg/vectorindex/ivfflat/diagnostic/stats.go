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

// Package diagnostic computes current IVF statistics without retaining vectors
// or writing a historical baseline.
package diagnostic

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Bucket struct {
	ID        int64
	Count     int64
	Valid     int64
	Center    []float64
	meanDelta []float64
	residual  float64
	squared   float64
}

func NewBucket(id int64, center []float64) *Bucket {
	return &Bucket{ID: id, Center: center, meanDelta: make([]float64, len(center))}
}

// Add counts every entry. A missing vector is counted but cannot contribute
// geometry. A NULL center is the untrained-index placeholder.
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
	if x == nil || len(b.Center) == 0 {
		return nil
	}
	b.Valid++
	var squared float64
	for i, v := range x {
		delta := v - b.Center[i]
		// Average residual vectors rather than subtracting two large means.
		b.meanDelta[i] += (delta - b.meanDelta[i]) / float64(b.Valid)
		squared += delta * delta
	}
	b.squared += squared
	b.residual += math.Sqrt(squared)
	if math.IsInf(b.squared, 0) || math.IsInf(b.residual, 0) {
		return moerr.NewInvalidInputNoCtxf("centroid %d: residual overflow", b.ID)
	}
	return nil
}

// Geometry contains L2 distances in the original vector's units. Mismatch is
// n*||mean-center||^2 / sum(||x-center||^2), NOT an estimate of recall loss.
type Geometry struct {
	MeanResidual, RMSResidual, CenterShift, Mismatch float64
}

func (b *Bucket) Geometry() (Geometry, bool) {
	if b.Count == 0 || b.Valid != b.Count {
		return Geometry{}, false
	}
	shiftSquared := float64(0)
	for _, delta := range b.meanDelta {
		shiftSquared += delta * delta
	}
	ratio := float64(0)
	if b.squared > 0 {
		ratio = math.Min(1, float64(b.Count)*shiftSquared/b.squared)
	}
	return Geometry{
		MeanResidual: b.residual / float64(b.Count),
		RMSResidual:  math.Sqrt(b.squared / float64(b.Count)),
		CenterShift:  math.Sqrt(shiftSquared),
		Mismatch:     ratio,
	}, true
}

type Summary struct {
	Count, Centroids, Empty, Min, Max int64
	CV                                float64
	Geometry                          Geometry
	HasGeometry                       bool
}

func Summarize(buckets []*Bucket) Summary {
	s := Summary{Centroids: int64(len(buckets))}
	var mean, m2, squared, residual, shiftSquared float64
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
		residual += b.residual
		squared += b.squared
		shiftSquared += float64(b.Count) * g.CenterShift * g.CenterShift
	}
	if mean > 0 {
		s.CV = math.Sqrt(m2/float64(len(buckets))) / mean
	}
	if complete && s.Count > 0 {
		s.HasGeometry = true
		s.Geometry = Geometry{
			MeanResidual: residual / float64(s.Count),
			RMSResidual:  math.Sqrt(squared / float64(s.Count)),
			CenterShift:  math.Sqrt(shiftSquared / float64(s.Count)),
		}
		if squared > 0 {
			s.Geometry.Mismatch = math.Min(1, shiftSquared/squared)
		}
	}
	return s
}
