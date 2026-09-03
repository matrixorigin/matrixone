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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateSampledNDVUsesIndependentFramesWithoutAddingThem(t *testing.T) {
	input := SampledNDVInput{
		PopulationRows:      1_000,
		SampleRows:          100,
		SampleDistinct:      60,
		SampleSingletons:    40,
		IncidenceBlocks:     10,
		IncidenceDistinct:   80,
		IncidenceSingletons: 30,
		IncidenceDoubletons: 10,
		ObservedDistinct:    90,
	}
	got, err := EstimateSampledNDV(input)
	require.NoError(t, err)

	n, d, f1, population := 100.0, 60.0, 40.0, 1_000.0
	wantDuj1 := n * d / (n - f1 + f1*n/population)
	wantIncidence := 80.0 + (9.0/10.0)*30.0*30.0/(2*10.0)
	require.InDelta(t, wantDuj1, got.Duj1, 1e-12)
	require.InDelta(t, wantIncidence, got.Incidence, 1e-12)
	require.Equal(t, math.Max(90, math.Max(wantDuj1, wantIncidence)), got.Point)
	require.Less(t, got.Point, got.Duj1+got.Incidence,
		"overlapping unseen domains must not be added")
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
			name: "no incidence doubletons",
			input: SampledNDVInput{
				PopulationRows: 100, SampleRows: 10, SampleDistinct: 10, SampleSingletons: 10,
				IncidenceBlocks: 4, IncidenceDistinct: 8, IncidenceSingletons: 4, ObservedDistinct: 10,
			},
			point: 100,
		},
		{
			name: "all equal",
			input: SampledNDVInput{
				PopulationRows: 1_000, SampleRows: 100, SampleDistinct: 1,
				IncidenceBlocks: 8, IncidenceDistinct: 1, ObservedDistinct: 1,
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
}

func TestDeleteOneFoldInterval(t *testing.T) {
	interval, ok := DeleteOneFoldInterval(100, 60, 140, []float64{90, 95, 105, 110})
	require.True(t, ok)
	require.Greater(t, interval.StandardError, 0.0)
	require.GreaterOrEqual(t, interval.Lower, 60.0)
	require.LessOrEqual(t, interval.Upper, 140.0)

	_, ok = DeleteOneFoldInterval(100, 60, 140, []float64{100})
	require.False(t, ok)
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
	estimate, err := a.Estimate(100)
	require.NoError(t, err)
	require.Equal(t, float64(3), estimate.ObservedLower)
	require.True(t, estimate.HasIncidence)
}

func TestNDVAccumulatorOverflowDisablesOnlyAffectedFrame(t *testing.T) {
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

	estimate, err := a.Estimate(10)
	require.NoError(t, err)
	require.False(t, estimate.HasIncidence)
	require.True(t, estimate.HasDuj1)
}

func TestHashTypedValueSeparatesLogicalTypes(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	require.Equal(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 4, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(2, 4, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 8, 0, raw))
	require.NotEqual(t, HashTypedValue(1, 4, 0, raw), HashTypedValue(1, 4, 2, raw))
}
