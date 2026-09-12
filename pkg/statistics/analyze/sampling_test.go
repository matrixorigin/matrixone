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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanBlockSampleBoundsAndDeterminism(t *testing.T) {
	seed := sha256.Sum256([]byte("stable table generation"))
	cfg := SampleConfig{TargetRows: 5_000, MaxBlocks: 17, MaxStrata: 8}
	first, err := PlanBlockSample(10_000, 101, cfg, seed)
	require.NoError(t, err)
	second, err := PlanBlockSample(10_000, 101, cfg, seed)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, SampleAlgorithmV1, first.Algorithm)
	require.Len(t, first.Strata, 8)
	require.Len(t, first.Blocks, 17)
	require.Equal(t, uint64(101), sumStratumBlocks(first.Strata))

	seen := make(map[uint64]struct{}, len(first.Blocks))
	for _, block := range first.Blocks {
		require.Less(t, block.LogicalOrdinal, uint64(101))
		_, duplicate := seen[block.LogicalOrdinal]
		require.False(t, duplicate)
		seen[block.LogicalOrdinal] = struct{}{}
		stratum := first.Strata[block.StratumID]
		require.GreaterOrEqual(t, block.LogicalOrdinal, stratum.FirstBlock)
		require.Less(t, block.LogicalOrdinal, stratum.FirstBlock+stratum.Blocks)
		require.Equal(t, stratum.Probability(), block.Physical)
	}

	otherSeed := sha256.Sum256([]byte("next table generation"))
	other, err := PlanBlockSample(10_000, 101, cfg, otherSeed)
	require.NoError(t, err)
	require.NotEqual(t, first.Blocks, other.Blocks)
}

func TestPlanBlockSampleFullCoverageAndDegenerateInputs(t *testing.T) {
	seed := sha256.Sum256([]byte("small table"))
	plan, err := PlanBlockSample(30, 3, SampleConfig{TargetRows: 300, MaxBlocks: 4, MaxStrata: 4}, seed)
	require.NoError(t, err)
	require.Len(t, plan.Blocks, 3)
	require.Equal(t, MustFraction(1, 1), plan.Q)
	require.Equal(t, MustFraction(1, 1), plan.QBlocks)
	for _, block := range plan.Blocks {
		require.True(t, block.RowThresholdAll)
		require.True(t, block.IncidenceAll)
	}

	empty, err := PlanBlockSample(0, 0, DefaultSampleConfig(), seed)
	require.NoError(t, err)
	require.Equal(t, MustFraction(1, 1), empty.Q)

	_, err = PlanBlockSample(1, 0, DefaultSampleConfig(), seed)
	require.ErrorIs(t, err, ErrInvalidPopulation)
	_, err = PlanBlockSample(10, 10, SampleConfig{}, seed)
	require.ErrorIs(t, err, ErrSampleBudget)
}

func TestPlanBlockSampleHonorsMinimumSpatialCoverage(t *testing.T) {
	seed := sha256.Sum256([]byte("clustered table"))
	plan, err := PlanBlockSample(
		1_000_000_000,
		100_000,
		SampleConfig{TargetRows: 300_000, MinBlocks: 512, MaxBlocks: 4_096, MaxStrata: 64},
		seed,
	)
	require.NoError(t, err)
	require.Len(t, plan.Blocks, 512)
	require.Equal(t, MustFraction(3, 10_000), plan.Q)
	require.Equal(t, MustFraction(8, 1563), plan.QBlocks)

	bounded, err := PlanBlockSample(
		1_000_000,
		100,
		SampleConfig{TargetRows: 1, MinBlocks: 512, MaxBlocks: 32, MaxStrata: 64},
		seed,
	)
	require.NoError(t, err)
	require.Len(t, bounded.Blocks, 32)
}

func TestFloydSelectionHasExpectedDeterministicCoverage(t *testing.T) {
	const (
		population = uint64(8)
		sample     = uint64(2)
		seedCount  = 4096
	)
	counts := make([]int, population)
	for i := uint64(0); i < seedCount; i++ {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], i)
		seed := sha256.Sum256(encoded[:])
		selected := floydSample(population, sample, newHashStream(seed, domainBlockSelection, 0))
		require.Len(t, selected, int(sample))
		for _, ordinal := range selected {
			counts[ordinal]++
		}
	}
	expected := float64(seedCount*sample) / float64(population)
	for _, count := range counts {
		// This is a deterministic corpus check, not a flaky random assertion.
		require.InDelta(t, expected, float64(count), expected*0.12)
	}
}

func TestExactThresholdBoundaries(t *testing.T) {
	zero := MustFraction(0, 7)
	threshold, all, err := zero.Threshold128()
	require.NoError(t, err)
	require.False(t, all)
	require.False(t, BelowThreshold([16]byte{}, threshold, all))

	half := MustFraction(1, 2)
	threshold, all, err = half.Threshold128()
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, byte(0x80), threshold[0])
	require.True(t, BelowThreshold([16]byte{0x7f}, threshold, all))
	require.False(t, BelowThreshold([16]byte{0x80}, threshold, all))

	one := MustFraction(1, 1)
	threshold, all, err = one.Threshold128()
	require.NoError(t, err)
	require.True(t, all)
	require.True(t, BelowThreshold([16]byte{0xff}, threshold, all))
}

func sumStratumBlocks(strata []Stratum) uint64 {
	var sum uint64
	for _, stratum := range strata {
		sum += stratum.Blocks
	}
	return sum
}
