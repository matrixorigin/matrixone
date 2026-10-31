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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTemporalRangeEstimateControls(t *testing.T) {
	stats := TemporalRangeStatistics{Rows: 1000, Min: 100, Max: 200, NDV: 100, Kind: types.T_timestamp}
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
			sel, ok := estimateTemporalRange(stats, tc.c, stats.Kind)
			require.True(t, ok)
			require.InDelta(t, tc.want, sel, 1e-12)
		})
	}
	stats.Nulls = 250
	sel, ok := estimateTemporalRange(stats, types.TemporalCandidate{Lo: 100, Hi: 200}, stats.Kind)
	require.True(t, ok)
	require.Equal(t, .75, sel)

	stats.Kind, stats.Nulls = types.T_datetime, 0
	sel, ok = estimateTemporalRange(stats, types.TemporalCandidate{Lo: 125, Hi: 175, Sentinel: 150}, stats.Kind)
	require.True(t, ok)
	require.Equal(t, .5, sel, "overlapping main and sentinel ranges count once")
	previous := 1.
	for lo := int64(100); lo <= 200; lo += 25 {
		sel, ok = estimateTemporalRange(stats, types.TemporalCandidate{Lo: lo, Hi: 200, Sentinel: lo}, stats.Kind)
		require.True(t, ok)
		require.LessOrEqual(t, sel, previous, "shrinking a range cannot increase its estimate")
		previous = sel
	}
}

func TestTemporalRangeEstimateDeclinesUnusableEvidence(t *testing.T) {
	base := TemporalRangeStatistics{Rows: 1000, Min: 100, Max: 200, NDV: 100, Kind: types.T_timestamp}
	for _, alter := range []func(*TemporalRangeStatistics){
		func(s *TemporalRangeStatistics) { s.Rows = math.Inf(1) },
		func(s *TemporalRangeStatistics) { s.NDV = math.NaN() },
		func(s *TemporalRangeStatistics) { s.Nulls = 1001 },
		func(s *TemporalRangeStatistics) { s.Min = 201 },
		func(s *TemporalRangeStatistics) { s.Kind = types.T_datetime },
	} {
		s := base
		alter(&s)
		_, ok := estimateTemporalRange(s, types.TemporalCandidate{Lo: 125, Hi: 175}, types.T_timestamp)
		require.False(t, ok)
	}
	for _, c := range []types.TemporalCandidate{{Full: true}, {Empty: true}, {Lo: 200, Hi: 100}} {
		_, ok := estimateTemporalRange(base, c, types.T_timestamp)
		require.False(t, ok)
	}
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
		require.Equal(t, tc.want, temporalCBOIndexInputRows(tc.rows, tc.candidates))
	}
}

func TestTemporalCBOIndexBlockFloorDoesNotChangeRowWork(t *testing.T) {
	for _, serialized := range []bool{false, true} {
		builder, _, index := temporalCBOTraversalFixture(t)
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

func TestTemporalCBOBaseUsesExistingOverlapEstimator(t *testing.T) {
	builder, source, _ := temporalCBOTraversalFixture(t)
	scan := builder.qry.Nodes[0]
	scan.BlockFilterList = []*plan.Expr{DeepCopyExpr(scan.FilterList[0])}
	cost := builder.newEncodedRegularIndexCostContext(scan, map[[2]int32]int{{scan.BindingTags[0], 0}: 1})
	observation := source.observation
	observation.Blocks = 512
	observation.HasOverlap = true
	for _, overlap := range []float64{0, math.Nextafter(overlapThreshold/3, 0), overlapThreshold / 3, overlapThreshold, .5, .9, 1} {
		observation.Overlap = overlap
		header := *scan.FilterList[0]
		header.Selectivity = .0001
		stats := &statsinfo.StatsInfo{ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"d": {Overlap: overlap}}}
		blockSel := estimateFilterBlockSelectivity(builder.GetContext(), &header, scan.TableDef, stats)
		rows := max(observation.Rows*.0001, observation.Rows*blockSel, min(observation.Rows, float64(estimatedBlocks(float64(observation.Blocks)*blockSel))*float64(objectio.BlockMaxRows)))
		budget := engine.MaxTemporalBindingWork
		work, ok := builder.temporalCBOBaseWork(scan, observation, .0001, cost, &budget)
		require.True(t, ok)
		require.Equal(t, rows*(cost.baseWidth+regularIndexPredicateRowWork), work)
	}
}
