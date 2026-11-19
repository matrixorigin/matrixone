// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTemporalRangeEstimateControls(t *testing.T) {
	stats := engine.IndexCostStats{Rows: 1000, Min: 100, Max: 200, NDV: 100, Kind: types.T_timestamp}
	estimate := func(candidate types.TemporalCandidate) float64 {
		var spans [4][2]float64
		count, ok := temporalIndexCostRanges(stats.Kind, candidate, &spans)
		require.True(t, ok)
		sel, ok := estimateIndexRangeUnion(stats, spans[:count])
		require.True(t, ok)
		return sel
	}
	for _, tc := range []struct {
		name string
		c    types.TemporalCandidate
		want float64
	}{
		{"all", types.TemporalCandidate{Lo: 0, Hi: 300}, 1},
		{"half", types.TemporalCandidate{Lo: 150, Hi: 300}, .5},
		{"point", types.TemporalCandidate{Lo: 150, Hi: 150}, .01},
		{"outside", types.TemporalCandidate{Lo: 300, Hi: 400}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.InDelta(t, tc.want, estimate(tc.c), 1e-12)
		})
	}
	stats.Nulls = 250
	require.Equal(t, .75, estimate(types.TemporalCandidate{Lo: 100, Hi: 200}))

	stats.Kind, stats.Nulls = types.T_datetime, 0
	require.Equal(t, .5, estimate(types.TemporalCandidate{Lo: 125, Hi: 175, Sentinel: 150}), "overlapping main and sentinel ranges count once")
	previous := 1.
	for lo := int64(100); lo <= 200; lo += 25 {
		sel := estimate(types.TemporalCandidate{Lo: lo, Hi: 200, Sentinel: lo})
		require.LessOrEqual(t, sel, previous, "shrinking a range cannot increase its estimate")
		previous = sel
	}
}

func TestTemporalRangeEstimateDeclinesUnusableEvidence(t *testing.T) {
	base := engine.IndexCostStats{Rows: 1000, Min: 100, Max: 200, NDV: 100, Kind: types.T_timestamp}
	for _, alter := range []func(*engine.IndexCostStats){
		func(s *engine.IndexCostStats) { s.Rows = math.Inf(1) },
		func(s *engine.IndexCostStats) { s.NDV = math.NaN() },
		func(s *engine.IndexCostStats) { s.Nulls = 1001 },
		func(s *engine.IndexCostStats) { s.Min = 201 },
	} {
		s := base
		alter(&s)
		_, ok := estimateIndexRangeUnion(s, [][2]float64{{125, 175}})
		require.False(t, ok)
	}
	var spans [4][2]float64
	for _, c := range []types.TemporalCandidate{{Full: true}, {Empty: true}, {Lo: 200, Hi: 100}} {
		_, ok := temporalIndexCostRanges(types.T_timestamp, c, &spans)
		require.False(t, ok)
	}
	_, ok := temporalIndexCostRanges(types.T_int64, types.TemporalCandidate{Lo: 125, Hi: 175}, &spans)
	require.False(t, ok)
	atom := indexComparison{column: &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}}}
	_, ok, err := indexComparisonSelectivity(context.Background(), base, atom, indexCostScalar{}, time.UTC)
	require.NoError(t, err)
	require.False(t, ok, "statistics for a different key type cannot price the comparison")
}

func TestEncodedIndexEstimateDoesNotInheritStaleOutputOrMutatePlan(t *testing.T) {
	filter := makeStringEqFilterExpr(0, 2, "HOT")
	filter.Selectivity = .9
	builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
		[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, []*plan.Expr{filter},
		&plan.Stats{TableCnt: 800000, Outcnt: 720000, Cost: 800000, Selectivity: .9}, map[int32]int{2: 1}, false)
	scan := builder.qry.Nodes[scanID]
	filter.GetF().Args[0].GetCol().RelPos = scan.BindingTags[0]
	cost := builder.newEncodedRegularIndexCostContext(scan, refs)
	ordinary, rejected, valid := cost.score(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly)
	require.True(t, valid)
	stale := &encodedRegularIndexEstimate{filter: 0, selectivity: .001, outputRows: 720000}
	staleWork, _, valid := cost.scoreWithEstimate(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly, stale)
	require.True(t, valid)
	fresh := *stale
	fresh.outputRows = 800
	fresh.fixedWork = 16
	freshWork, _, valid := cost.scoreWithEstimate(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly, &fresh)
	require.True(t, valid)
	require.Less(t, freshWork, staleWork/10, "both candidate and output cardinality must belong to this estimate")
	require.Equal(t, .9, filter.Selectivity)
	require.Equal(t, 720000., scan.Stats.Outcnt)
	again, rejectedAgain, valid := cost.score(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly)
	require.True(t, valid)
	require.Equal(t, ordinary, again, "an optional estimate cannot leak into ordinary scoring")
	require.Equal(t, rejected, rejectedAgain)
}

func TestTemporalCBOIndexInputRoundsOnlyInputToBlocks(t *testing.T) {
	block := float64(objectio.BlockMaxRows)
	for _, tc := range []struct{ rows, candidates, want float64 }{
		{1000000, 100, block}, {1000000, block, 2 * block}, {1000000, block + 1, 2 * block},
		{100, 1, 100}, {1000000, 999999, 1000000},
	} {
		require.Equal(t, tc.want, indexBlockInputRows(tc.rows, tc.candidates))
	}
}

func TestTemporalCBOIndexBlockFloorDoesNotChangeRowWork(t *testing.T) {
	for _, serialized := range []bool{false, true} {
		builder, _, index := newIndexCostFixture(t)
		scan := builder.qry.Nodes[0]
		if !serialized {
			index.Unique = true
			index.Parts = []string{"d"}
		}
		refs := map[[2]int32]int{{scan.BindingTags[0], 0}: 1}
		cost := builder.newEncodedRegularIndexCostContext(scan, refs)
		estimate := &encodedRegularIndexEstimate{filter: 0, selectivity: 100 / scan.Stats.TableCnt, outputRows: 100, fixedWork: 16}
		rowWork, _, ok := cost.scoreWithEstimate(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly, estimate)
		require.True(t, ok)
		estimate.blockInput = true
		blockWork, _, ok := cost.scoreWithEstimate(index, []int32{0}, []int32{0}, encodedRegularIndexCostIndexOnly, estimate)
		require.True(t, ok)
		width := cost.columnWidths[0]
		if serialized {
			width = cost.serialWidths[0] + cost.serialWidths[1]
		}
		require.InDelta(t, (float64(objectio.BlockMaxRows)-100)*width, blockWork-rowWork, .000001)
	}
}
