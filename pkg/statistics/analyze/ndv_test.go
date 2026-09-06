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
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateSampledNDVUsesCollapsedBlockFrameInsteadOfRowDiagnostic(t *testing.T) {
	input := SampledNDVInput{
		PopulationRows:         1_000,
		SampleRows:             100,
		SampleDistinct:         60,
		SampleSingletons:       40,
		IncidenceBlocks:        10,
		IncidenceObservations:  200,
		IncidenceDistinct:      80,
		IncidenceSingletons:    30,
		BlockSampleNumerator:   1,
		BlockSampleDenominator: 10,
		ObservedDistinct:       90,
	}
	got, err := EstimateSampledNDV(input)
	require.NoError(t, err)

	n, d, f1, population := 100.0, 60.0, 40.0, 1_000.0
	wantDuj1 := n * d / (n - f1 + f1*n/population)
	wantCollapsedDuj1 := 200.0 * 80.0 / (200.0 - 30.0 + 30.0/10.0)
	require.InDelta(t, wantDuj1, got.Duj1, 1e-12)
	require.InDelta(t, wantCollapsedDuj1, got.CollapsedDuj1, 1e-12)
	require.Equal(t, math.Max(90, wantCollapsedDuj1), got.Point)
	require.Less(t, got.CollapsedDuj1, got.Duj1,
		"the row estimate must not dominate a complete COLLAPSE frame")
}

func TestEstimateSampledNDVGuardsZeroAndBounds(t *testing.T) {
	tests := []struct {
		name  string
		input SampledNDVInput
		point float64
	}{
		{
			name:  "empty",
			input: SampledNDVInput{},
			point: 0,
		},
		{
			name: "all null",
			input: SampledNDVInput{
				IncidenceBlocks:        4,
				BlockSampleNumerator:   1,
				BlockSampleDenominator: 2,
			},
			point: 0,
		},
		{
			name: "clustered block local values",
			input: SampledNDVInput{
				PopulationRows: 100, SampleRows: 10, SampleDistinct: 10, SampleSingletons: 10,
				IncidenceBlocks: 4, IncidenceObservations: 4, IncidenceDistinct: 4,
				IncidenceSingletons: 4, BlockSampleNumerator: 1,
				BlockSampleDenominator: 4, ObservedDistinct: 10,
			},
			point: 16,
		},
		{
			name: "all equal",
			input: SampledNDVInput{
				PopulationRows: 1_000, SampleRows: 100, SampleDistinct: 1,
				IncidenceBlocks: 8, IncidenceObservations: 8, IncidenceDistinct: 1,
				BlockSampleNumerator: 1, BlockSampleDenominator: 8, ObservedDistinct: 1,
			},
			point: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := EstimateSampledNDV(test.input)
			require.NoError(t, err)
			require.Equal(t, test.point, got.Point)
			require.GreaterOrEqual(t, got.Point, got.ObservedLower)
			require.LessOrEqual(t, got.Point, got.RelationalUpper)
			require.False(t, math.IsNaN(got.Point))
			require.False(t, math.IsInf(got.Point, 0))
		})
	}

	_, err := EstimateSampledNDV(SampledNDVInput{PopulationRows: 2, SampleRows: 1, SampleDistinct: 2})
	require.ErrorIs(t, err, ErrInvalidNDVInput)
	_, err = EstimateSampledNDV(SampledNDVInput{
		PopulationRows: 10, IncidenceBlocks: 1, IncidenceObservations: 1,
		IncidenceDistinct: 1, ObservedDistinct: 1,
	})
	require.ErrorIs(t, err, ErrInvalidNDVInput)
	_, err = EstimateSampledNDV(SampledNDVInput{
		PopulationRows: 10, SampleRows: 2, SampleDistinct: 1,
		SampleSingletons: 0, ObservedDistinct: 1,
	})
	require.ErrorIs(t, err, ErrInvalidNDVInput,
		"a row-only estimate must not be published for a block sample")
}

func TestEstimateSampledNDVCollapseCoversLayoutExtremes(t *testing.T) {
	tests := []struct {
		name  string
		input SampledNDVInput
		point float64
	}{
		{
			name: "full block coverage is exact",
			input: SampledNDVInput{
				PopulationRows: 25, IncidenceBlocks: 2, IncidenceObservations: 10,
				IncidenceDistinct: 5, IncidenceSingletons: 0,
				BlockSampleNumerator: 1, BlockSampleDenominator: 1,
				ObservedDistinct: 5,
			},
			point: 5,
		},
		{
			name: "each value belongs to one block",
			input: SampledNDVInput{
				PopulationRows: 1_000, IncidenceBlocks: 5, IncidenceObservations: 20,
				IncidenceDistinct: 20, IncidenceSingletons: 20,
				BlockSampleNumerator: 1, BlockSampleDenominator: 4,
				ObservedDistinct: 20,
			},
			point: 80,
		},
		{
			name: "values repeat across blocks",
			input: SampledNDVInput{
				PopulationRows: 1_000, IncidenceBlocks: 4, IncidenceObservations: 20,
				IncidenceDistinct: 5, IncidenceSingletons: 0,
				BlockSampleNumerator: 1, BlockSampleDenominator: 4,
				ObservedDistinct: 5,
			},
			point: 5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := EstimateSampledNDV(test.input)
			require.NoError(t, err)
			require.Equal(t, test.point, got.Point)
		})
	}
}

func TestCollapsedNDVStaysBoundedAcrossPhysicalLayouts(t *testing.T) {
	const (
		blockCount   = uint64(8)
		rowsPerBlock = uint64(20)
		truth        = float64(20)
	)
	plan, err := PlanBlockSample(
		blockCount*rowsPerBlock,
		blockCount,
		SampleConfig{TargetRows: 80, MaxBlocks: 4, MaxStrata: 4},
		sha256.Sum256([]byte("layout-invariance")),
	)
	require.NoError(t, err)
	require.Equal(t, MustFraction(1, 2), plan.QBlocks)

	sorted := make([]uint64, 0, blockCount*rowsPerBlock)
	for value := uint64(0); value < uint64(truth); value++ {
		for range blockCount {
			sorted = append(sorted, value)
		}
	}
	spread := make([]uint64, 0, blockCount*rowsPerBlock)
	for range blockCount {
		for value := uint64(0); value < uint64(truth); value++ {
			spread = append(spread, value)
		}
	}
	batchAppended := make([]uint64, 0, blockCount*rowsPerBlock)
	for range uint64(4) {
		for value := uint64(0); value < uint64(truth); value++ {
			batchAppended = append(batchAppended, value, value)
		}
	}

	for name, rows := range map[string][]uint64{
		"sorted":         sorted,
		"spread":         spread,
		"batch-appended": batchAppended,
	} {
		t.Run(name, func(t *testing.T) {
			accumulator := NewNDVAccumulator(1_000)
			for _, sampled := range plan.Blocks {
				require.NoError(t, accumulator.BeginIncidenceBlock())
				first := sampled.LogicalOrdinal * rowsPerBlock
				for _, value := range rows[first : first+rowsPerBlock] {
					require.NoError(t, accumulator.ObserveIncidenceValue(
						HashValue([]byte{byte(value)})))
				}
				require.NoError(t, accumulator.EndIncidenceBlock())
			}
			estimate, estimateErr := accumulator.Estimate(
				float64(blockCount*rowsPerBlock), plan.QBlocks)
			require.NoError(t, estimateErr)
			qError := math.Max(estimate.Point/truth, truth/estimate.Point)
			require.LessOrEqual(t, qError, 1.25)
		})
	}
}

func TestNDVAccumulatorSeparatesRowsAndBlockPresence(t *testing.T) {
	a := NewNDVAccumulator(16)
	one := HashValue([]byte("one"))
	two := HashValue([]byte("two"))
	three := HashValue([]byte("three"))
	for _, value := range []ValueHash{one, one, two} {
		require.NoError(t, a.ObserveSampleValue(value))
	}
	for _, values := range [][]ValueHash{{one, one, two}, {one, three}} {
		require.NoError(t, a.BeginIncidenceBlock())
		for _, value := range values {
			require.NoError(t, a.ObserveIncidenceValue(value))
		}
		require.NoError(t, a.EndIncidenceBlock())
	}
	estimate, err := a.Estimate(100, MustFraction(1, 2))
	require.NoError(t, err)
	require.Equal(t, float64(3), estimate.ObservedLower)
	require.True(t, estimate.HasCollapsedDuj1)
	require.Equal(t, float64(4), estimate.Point)
}

func TestFullScanNDVAccumulatorIsBoundedAndMergeable(t *testing.T) {
	const distinct = 100_000
	left := NewFullScanNDVAccumulator()
	right := NewFullScanNDVAccumulator()
	for block, accumulator := range []*NDVAccumulator{left, right} {
		require.NoError(t, accumulator.BeginIncidenceBlock())
		for i := block; i < distinct; i += 2 {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], uint64(i))
			value := HashValue(encoded[:])
			require.NoError(t, accumulator.ObserveIncidenceValue(value))
			require.NoError(t, accumulator.ObserveSampleValue(value))
		}
		require.NoError(t, accumulator.EndIncidenceBlock())
	}
	require.NoError(t, left.Merge(right))
	estimate, err := left.Estimate(distinct, MustFraction(1, 1))
	require.NoError(t, err)
	require.Equal(t, FullScanNDVAlgorithmV1, estimate.Algorithm)
	require.InDelta(t, float64(distinct), estimate.Point, float64(distinct)*0.03)
	require.Equal(t, float64(distinct), estimate.RelationalUpper)
	require.Zero(t, estimate.ObservedLower)
}

func TestNDVAccumulatorIncidenceOverflowFailsClosed(t *testing.T) {
	a := NewNDVAccumulator(1)
	one := HashValue([]byte("one"))
	two := HashValue([]byte("two"))
	require.NoError(t, a.ObserveSampleValue(one))
	require.NoError(t, a.BeginIncidenceBlock())
	require.NoError(t, a.ObserveIncidenceValue(one))
	require.NoError(t, a.EndIncidenceBlock())
	require.NoError(t, a.BeginIncidenceBlock())
	require.NoError(t, a.ObserveIncidenceValue(one))
	require.ErrorIs(t, a.ObserveIncidenceValue(two), ErrAccumulatorLimit)
	require.ErrorIs(t, a.EndIncidenceBlock(), ErrAccumulatorLimit)

	_, err := a.Estimate(10, MustFraction(1, 2))
	require.ErrorIs(t, err, ErrAccumulatorLimit)
}

func TestNDVAccumulatorRowOverflowKeepsCompleteCollapseFrame(t *testing.T) {
	a := NewNDVAccumulator(1)
	one := HashValue([]byte("one"))
	two := HashValue([]byte("two"))
	require.NoError(t, a.ObserveSampleValue(one))
	require.ErrorIs(t, a.ObserveSampleValue(two), ErrAccumulatorLimit)
	require.NoError(t, a.BeginIncidenceBlock())
	require.NoError(t, a.ObserveIncidenceValue(one))
	require.NoError(t, a.EndIncidenceBlock())

	estimate, err := a.Estimate(10, MustFraction(1, 1))
	require.NoError(t, err)
	require.False(t, estimate.HasDuj1)
	require.True(t, estimate.HasCollapsedDuj1)
	require.Equal(t, float64(1), estimate.Point)
}

func TestNDVAccumulatorMergeFailsClosedOnIncidenceOverflow(t *testing.T) {
	one := HashValue([]byte("one"))
	two := HashValue([]byte("two"))
	left := NewNDVAccumulator(1)
	right := NewNDVAccumulator(1)
	for accumulator, value := range map[*NDVAccumulator]ValueHash{
		left: one, right: two,
	} {
		require.NoError(t, accumulator.BeginIncidenceBlock())
		require.NoError(t, accumulator.ObserveIncidenceValue(value))
		require.NoError(t, accumulator.EndIncidenceBlock())
	}

	require.ErrorIs(t, left.Merge(right), ErrAccumulatorLimit)
	require.ErrorIs(t, left.IncidenceStateError(), ErrAccumulatorLimit)
	_, err := left.Estimate(10, MustFraction(1, 2))
	require.ErrorIs(t, err, ErrAccumulatorLimit)
}

func TestNDVAccumulatorMergeToleratesRowOnlyOverflow(t *testing.T) {
	one := HashValue([]byte("one"))
	two := HashValue([]byte("two"))
	left := NewNDVAccumulator(1)
	right := NewNDVAccumulator(1)
	for accumulator, rowValue := range map[*NDVAccumulator]ValueHash{
		left: one, right: two,
	} {
		require.NoError(t, accumulator.ObserveSampleValue(rowValue))
		require.NoError(t, accumulator.BeginIncidenceBlock())
		require.NoError(t, accumulator.ObserveIncidenceValue(one))
		require.NoError(t, accumulator.EndIncidenceBlock())
	}

	require.NoError(t, left.Merge(right))
	require.ErrorIs(t, left.RowStateError(), ErrAccumulatorLimit)
	estimate, err := left.Estimate(10, MustFraction(1, 1))
	require.NoError(t, err)
	require.True(t, estimate.HasCollapsedDuj1)
	require.Equal(t, float64(1), estimate.Point)
}

func TestHashTypedValueSeparatesLogicalTypes(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	require.Equal(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 4, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(2, 4, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 8, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 4, 2, raw))
}
