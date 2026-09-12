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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	SampledNDVAlgorithmV2  = "NDV_COLLAPSED_BLOCK_DUJ1_V2"
	FullScanNDVAlgorithmV1 = "NDV_FULLSCAN_HLL_P14_V1"
)

var ErrInvalidNDVInput = moerr.NewInvalidInputNoCtx("analyze: invalid sampled NDV input")

// SampledNDVInput is the complete scalar state consumed by the version-two
// sampled estimator. All counts describe non-null values.
type SampledNDVInput struct {
	PopulationRows         float64
	SampleRows             uint64
	SampleDistinct         uint64
	SampleSingletons       uint64
	IncidenceBlocks        uint64
	IncidenceObservations  uint64
	IncidenceDistinct      uint64
	IncidenceSingletons    uint64
	BlockSampleNumerator   uint64
	BlockSampleDenominator uint64
	ObservedDistinct       uint64
}

type NDVEstimate struct {
	Algorithm        string
	Point            float64
	ObservedLower    float64
	RelationalUpper  float64
	Duj1             float64
	CollapsedDuj1    float64
	HasDuj1          bool
	HasCollapsedDuj1 bool
}

// EstimateSampledNDV applies Haas-Stokes Duj1 to a COLLAPSE frame:
// repeated occurrences within one sampled block count as one observation.
// This makes the estimator compatible with block sampling even when values are
// physically clustered. The row-frequency estimate is diagnostic only: an
// unavailable COLLAPSE frame must fail closed instead of publishing an estimate
// whose assumptions do not match block sampling. The union of observed hashes
// is a lower bound.
func EstimateSampledNDV(input SampledNDVInput) (NDVEstimate, error) {
	hasIncidenceState := input.IncidenceBlocks > 0 || input.IncidenceObservations > 0 ||
		input.IncidenceDistinct > 0 || input.IncidenceSingletons > 0
	if !finiteNonNegative(input.PopulationRows) ||
		input.SampleDistinct > input.SampleRows ||
		input.SampleSingletons > input.SampleDistinct ||
		input.IncidenceDistinct > input.IncidenceObservations ||
		(input.IncidenceObservations > 0 && input.IncidenceDistinct == 0) ||
		input.IncidenceSingletons > input.IncidenceDistinct ||
		input.ObservedDistinct < input.SampleDistinct ||
		input.ObservedDistinct < input.IncidenceDistinct ||
		(hasIncidenceState && (input.IncidenceBlocks == 0 ||
			input.BlockSampleNumerator == 0 || input.BlockSampleDenominator == 0 ||
			input.BlockSampleNumerator > input.BlockSampleDenominator)) ||
		(input.PopulationRows > 0 && input.IncidenceObservations == 0) ||
		(!hasIncidenceState && (input.SampleRows > 0 || input.ObservedDistinct > 0)) {
		return NDVEstimate{}, ErrInvalidNDVInput
	}

	observed := float64(input.ObservedDistinct)
	population := math.Max(input.PopulationRows, observed)
	population = math.Max(population, float64(input.SampleRows))
	estimate := NDVEstimate{
		Algorithm:       SampledNDVAlgorithmV2,
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

	if input.IncidenceObservations > 0 {
		n := float64(input.IncidenceObservations)
		d := float64(input.IncidenceDistinct)
		f1 := float64(input.IncidenceSingletons)
		qBlocks := float64(input.BlockSampleNumerator) /
			float64(input.BlockSampleDenominator)
		denominator := n - f1 + f1*qBlocks
		if denominator > 0 {
			estimate.CollapsedDuj1 = n * d / denominator
			estimate.HasCollapsedDuj1 = finiteNonNegative(estimate.CollapsedDuj1)
		}
		if !estimate.HasCollapsedDuj1 {
			return NDVEstimate{}, ErrInvalidNDVInput
		}
	}

	point := observed
	if estimate.HasCollapsedDuj1 {
		point = math.Max(point, estimate.CollapsedDuj1)
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
