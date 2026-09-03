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

package analyze

import (
	"errors"
	"math"
)

const SampledNDVAlgorithmV1 = "NDV_DUJ1_BLOCK_GUARDED_V1"

var ErrInvalidNDVInput = errors.New("analyze: invalid sampled NDV input")

// SampledNDVInput is the complete scalar state consumed by the version-one
// sampled estimator. All counts describe non-null values.
type SampledNDVInput struct {
	PopulationRows      float64
	SampleRows          uint64
	SampleDistinct      uint64
	SampleSingletons    uint64
	IncidenceBlocks     uint64
	IncidenceDistinct   uint64
	IncidenceSingletons uint64
	IncidenceDoubletons uint64
	ObservedDistinct    uint64
}

type NDVEstimate struct {
	Algorithm       string
	Point           float64
	ObservedLower   float64
	RelationalUpper float64
	Duj1            float64
	Incidence       float64
	HasDuj1         bool
	HasIncidence    bool
}

// EstimateSampledNDV combines the Haas-Stokes Duj1 row-frequency estimate with
// a guarded Chao incidence estimate. Components are never added: their unseen
// domains may overlap. The exact union of observed hashes is the lower bound.
func EstimateSampledNDV(input SampledNDVInput) (NDVEstimate, error) {
	if !finiteNonNegative(input.PopulationRows) ||
		input.SampleDistinct > input.SampleRows ||
		input.SampleSingletons > input.SampleDistinct ||
		input.IncidenceSingletons > input.IncidenceDistinct ||
		input.IncidenceDoubletons > input.IncidenceDistinct ||
		input.ObservedDistinct < input.SampleDistinct ||
		input.ObservedDistinct < input.IncidenceDistinct {
		return NDVEstimate{}, ErrInvalidNDVInput
	}

	observed := float64(input.ObservedDistinct)
	population := math.Max(input.PopulationRows, observed)
	population = math.Max(population, float64(input.SampleRows))
	estimate := NDVEstimate{
		Algorithm:       SampledNDVAlgorithmV1,
		ObservedLower:   observed,
		RelationalUpper: population,
	}

	if input.SampleRows > 0 {
		n := float64(input.SampleRows)
		d := float64(input.SampleDistinct)
		f1 := float64(input.SampleSingletons)
		denominator := n - f1 + f1*n/population
		if denominator > 0 {
			estimate.Duj1 = n * d / denominator
			estimate.HasDuj1 = finiteNonNegative(estimate.Duj1)
		}
	}

	if input.IncidenceBlocks >= 2 {
		m := float64(input.IncidenceBlocks)
		s := float64(input.IncidenceDistinct)
		q1 := float64(input.IncidenceSingletons)
		q2 := float64(input.IncidenceDoubletons)
		if q2 > 0 {
			estimate.Incidence = s + ((m-1)/m)*q1*q1/(2*q2)
		} else {
			estimate.Incidence = s + ((m-1)/m)*q1*(q1-1)/2
		}
		estimate.HasIncidence = finiteNonNegative(estimate.Incidence)
	}

	point := observed
	if estimate.HasDuj1 {
		point = math.Max(point, estimate.Duj1)
	}
	if estimate.HasIncidence {
		point = math.Max(point, estimate.Incidence)
	}
	if !finiteNonNegative(point) {
		return NDVEstimate{}, ErrInvalidNDVInput
	}
	estimate.Point = math.Min(population, math.Max(observed, point))
	return estimate, nil
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

type Interval struct {
	Lower         float64
	Upper         float64
	StandardError float64
}

// DeleteOneFoldInterval computes the empirical delete-one-cluster jackknife
// interval used by sampled statistics. It is a stability measurement, not a
// distribution-independent confidence guarantee.
func DeleteOneFoldInterval(point, lower, upper float64, replicates []float64) (Interval, bool) {
	if len(replicates) < 2 || !finiteNonNegative(point) || !finiteNonNegative(lower) ||
		!finiteNonNegative(upper) || lower > point || point > upper {
		return Interval{}, false
	}
	mean := 0.0
	for _, replicate := range replicates {
		if !finiteNonNegative(replicate) {
			return Interval{}, false
		}
		mean += replicate
	}
	mean /= float64(len(replicates))
	varianceSum := 0.0
	for _, replicate := range replicates {
		delta := replicate - mean
		varianceSum += delta * delta
	}
	g := float64(len(replicates))
	standardError := math.Sqrt((g - 1) / g * varianceSum)
	interval := Interval{
		Lower:         math.Max(lower, point-1.96*standardError),
		Upper:         math.Min(upper, point+1.96*standardError),
		StandardError: standardError,
	}
	return interval, true
}
