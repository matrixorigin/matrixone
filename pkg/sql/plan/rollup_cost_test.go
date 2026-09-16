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

package plan

import (
	"context"
	"fmt"
	"math"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSortRollupCostModelSelectsFromKnownStats(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	large := buildAutoRollupPlanWithStats(t, 500_000, "a, b")
	require.False(t, planHasSortRollup(large.GetQuery()),
		"the measured high-cardinality shape should retain hash as the baseline")

	small := buildAutoRollupPlanWithStats(t, 100, "a, b, c")
	require.True(t, planHasSortRollup(small.GetQuery()),
		"one scan plus sort should win for a small three-level input")
}

func TestRollupAlgorithmVariableControlsPlanner(t *testing.T) {
	cost := buildAutoRollupPlanWithAlgorithm(t, 100, "a, b, c", "COST")
	require.True(t, planHasSortRollup(cost.GetQuery()))

	forcedSort := buildAutoRollupPlanWithAlgorithm(t, 500_000, "a, b", "SORT")
	require.True(t, planHasSortRollup(forcedSort.GetQuery()),
		"SORT must remain available without optimizer_hints or SET_VAR")

	forcedHash := buildAutoRollupPlanWithAlgorithm(t, 100, "a, b, c", "HASH")
	require.False(t, planHasSortRollup(forcedHash.GetQuery()))
}

func TestSortRollupCostModelUsesFilterCardinality(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	// The table is large enough that the unfiltered estimate favors hash. The
	// isolated probe binds WHERE and charges the filtered cardinality instead.
	planWithFilter := buildAutoRollupPlanWithStats(t, 500_000,
		"a, b", "where a < 2")
	require.True(t, planHasSortRollup(planWithFilter.GetQuery()),
		"a selective WHERE should make the one-pass path cheaper")
}

func TestSortRollupCostModelUsesOrderedDerivedSource(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	queryPlan := buildAutoRollupPlanSQLWithStats(t, 1_000_000,
		`select d.a, d.b, d.c, count(*) from
			(select a, b, c from select_test.bind_select order by a, b, c) d
			group by d.a, d.b, d.c with rollup`)
	require.True(t, planHasSortRollup(queryPlan.GetQuery()),
		"known ordered input should make streaming rollup cheaper at 1M rows")
}

func TestSortRollupCostModelHonorsHashOverride(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2,rollupSort=2")

	queryPlan := buildAutoRollupPlanWithStats(t, 100, "a, b, c")
	require.False(t, planHasSortRollup(queryPlan.GetQuery()))
}

func TestSortRollupCostModelFallsBackWithoutStats(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL,
		`select a, b, count(*) from select_test.bind_select group by a, b with rollup`, 1)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)
	require.False(t, planHasSortRollup(queryPlan.GetQuery()))
}

func TestSortRollupCostModelChargesEveryRollupLevel(t *testing.T) {
	makeProbe := func(levels int) *sortRollupProbe {
		groupExprs := make([]*Expr, levels)
		for i := range groupExprs {
			groupExprs[i] = &Expr{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Ndv:  10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
			}
		}
		return &sortRollupProbe{
			builder: NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false),
			source: &Node{Stats: &Stats{
				TableCnt: 1001,
				Outcnt:   1000,
				Cost:     1001,
				Rowsize:  64,
			}},
			groupExprs: groupExprs,
		}
	}

	twoLevels, ok := estimateSortRollupCost(makeProbe(2), nil)
	require.True(t, ok)
	threeLevels, ok := estimateSortRollupCost(makeProbe(3), nil)
	require.True(t, ok)
	require.Equal(t, 3, twoLevels.BranchCount)
	require.Equal(t, 4, threeLevels.BranchCount)
	require.Greater(t, threeLevels.SortWork, twoLevels.SortWork)
	require.Greater(t, threeLevels.HashWork, twoLevels.HashWork)
}

func TestSortRollupCostModelAccountsForBranchConcurrency(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	groupExprs := make([]*Expr, 20)
	for i := range groupExprs {
		groupExprs[i] = &Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Ndv:  8,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
		}
	}
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 4097,
			Outcnt:   4096,
			Cost:     4097,
			Rowsize:  64,
		}},
		groupExprs: groupExprs,
	}

	builder.qry.MaxDop = 1
	serial, ok := estimateSortRollupCost(probe, nil)
	require.True(t, ok)
	builder.qry.MaxDop = 64
	parallel, ok := estimateSortRollupCost(probe, nil)
	require.True(t, ok)
	require.Greater(t, serial.HashCost, parallel.HashCost,
		"a max_dop cap must make overlapping UNION ALL branches more expensive")
}

func TestSortRollupCostModelMatchesMeasuredShapeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     float64
		levels   int
		ndv      float64
		wantSort bool
	}{
		{name: "small", rows: 100, levels: 3, ndv: 8, wantSort: true},
		{name: "medium", rows: 4096, levels: 3, ndv: 8, wantSort: false},
		{name: "tiny-many-levels", rows: 256, levels: 12, ndv: 4, wantSort: true},
		{name: "large-one-key", rows: 100000, levels: 1, ndv: 8, wantSort: false},
	} {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
		groupExprs := make([]*Expr, tc.levels)
		for i := range groupExprs {
			groupExprs[i] = &Expr{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Ndv:  tc.ndv,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
			}
		}
		estimate, ok := estimateSortRollupCost(&sortRollupProbe{
			builder: builder,
			source: &Node{Stats: &Stats{
				TableCnt: tc.rows + 1,
				Outcnt:   tc.rows,
				Cost:     tc.rows + 1,
				Rowsize:  64,
			}},
			groupExprs: groupExprs,
		}, nil)
		require.True(t, ok)
		require.Equal(t, tc.wantSort,
			estimate.SortCost < estimate.HashCost*rollupSortSelectFactor,
			"unexpected selection for measured shape %s (sort=%.1f hash=%.1f)",
			tc.name, estimate.SortCost, estimate.HashCost)
	}
}

func TestSortRollupCostModelAccountsForOrderedInputReuse(t *testing.T) {
	makeProbe := func(ordered bool) *sortRollupProbe {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
		// The runtime pre-extends one output batch. Leave enough room for that
		// batch so this test isolates the effect of reusing input order.
		builder.aggSpillMem = 4 << 20
		return &sortRollupProbe{
			builder: builder,
			source: &Node{Stats: &Stats{
				TableCnt: 100001,
				Outcnt:   100000,
				Cost:     100001,
				Rowsize:  64,
			}},
			groupExprs: []*Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
			},
			orderedInput: ordered,
		}
	}

	unreused, ok := estimateSortRollupCost(makeProbe(false), nil)
	require.True(t, ok)
	reused, ok := estimateSortRollupCost(makeProbe(true), nil)
	require.True(t, ok)
	require.True(t, reused.OrderedInput)
	require.Less(t, reused.SortWork, unreused.SortWork,
		"reusing producer order must remove the n log n work term")
	require.Less(t, reused.SortMemory, unreused.SortMemory,
		"streaming state must be bounded by active levels")
	require.True(t, unreused.SortFeasible,
		"the unordered path also uses a streaming aggregate after its internal sort")
	require.True(t, reused.SortFeasible,
		"ordered streaming input must fit its bounded active state")
	require.Equal(t, unreused.SortAggMemory, reused.SortAggMemory,
		"order reuse changes sort workspace, not streaming aggregate state")
}

func TestSortRollupCostModelRejectsAggregateCapacityOverflow(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	builder.aggSpillMem = 100
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 1001,
			Outcnt:   1001,
			Cost:     1001,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 1001, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 1001, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
		},
	}

	estimate, ok := estimateSortRollupCost(probe, nil)
	require.True(t, ok)
	require.False(t, estimate.SortFeasible,
		"active streaming rollup state must be capacity-feasible before sort can be selected")
}

func TestSortRollupCostModelChargesOutputBatchCapacity(t *testing.T) {
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL,
		`select a, b, count(*) from select_test.bind_select group by a, b with rollup`, 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	builder.aggSpillMem = 10_000
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 100_000,
			Outcnt:   100_000,
			Cost:     100_000,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
		},
	}

	estimate, ok := estimateSortRollupCost(probe, selectClause.Exprs)
	require.True(t, ok)
	require.Greater(t, estimate.SortOutputMemory, float64(10_000),
		"the admission estimate must include the pre-extended output vectors")
	require.False(t, estimate.SortFeasible,
		"COST must reject a sort plan whose first output batch exceeds the limit")
}

func TestSortRollupCostModelChargesHiddenAggregateOutput(t *testing.T) {
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL,
		`select a, b from select_test.bind_select
			group by a, b with rollup having sum(c) > 0 order by sum(c)`, 1)
	require.NoError(t, err)
	stmt := stmts[0].(*tree.Select)
	selectClause := stmt.Select.(*tree.SelectClause)
	extra := sortRollupAdditionalExprs(selectClause.Having, stmt.OrderBy)
	require.Len(t, extra, 2, "the test query must contain HAVING and ORDER BY aggregate expressions")

	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	builder.aggSpillMem = 700_000
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 100_000,
			Outcnt:   100_000,
			Cost:     100_000,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: 10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
		},
	}

	withoutHidden, ok := estimateSortRollupCost(probe, selectClause.Exprs)
	require.True(t, ok)
	withHidden, ok := estimateSortRollupCost(probe, selectClause.Exprs, extra...)
	require.True(t, ok)
	require.Greater(t, withHidden.SortOutputMemory, withoutHidden.SortOutputMemory,
		"HAVING/ORDER BY aggregates must be charged as hidden output vectors")
	require.False(t, withHidden.SortFeasible,
		"the memory budget must account for hidden aggregate output")
}

func TestSortRollupCostModelChargesHLLOutputCapacity(t *testing.T) {
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL,
		`select a, hll_add_agg(b) from select_test.bind_select group by a with rollup`, 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	builder.aggSpillMem = 128 << 20
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 100_000,
			Outcnt:   100_000,
			Cost:     100_000,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Ndv:  100_000,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}},
	}

	estimate, ok := estimateSortRollupCost(probe, selectClause.Exprs)
	require.True(t, ok)
	require.Greater(t, estimate.SortOutputMemory, float64(128<<20),
		"a full HLL result batch must include its dense serialized state")
	require.False(t, estimate.SortFeasible,
		"COST must not select sort when an HLL result batch exceeds the limit")
}

func TestSortRollupCostModelAcceptsKnownEmptyStats(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 0,
			Outcnt:   0,
			Cost:     0,
			Rowsize:  0,
		}},
		groupExprs: []*Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}},
	}

	estimate, ok := estimateSortRollupCost(probe, nil)
	require.True(t, ok, "an observed empty table is known statistics, not default statistics")
	require.Equal(t, float64(0), estimate.Rows)
	require.Equal(t, float64(1), estimate.SortGroupUpperBound)
	require.True(t, estimate.SortFeasible)
}

func TestSortRollupCostModelRejectsOverflow(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: math.MaxFloat64,
			Outcnt:   math.MaxFloat64,
			Cost:     math.MaxFloat64,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{{
			Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: math.MaxFloat64,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}, {
			Typ: plan.Type{Id: int32(types.T_int64)}, Ndv: math.MaxFloat64,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
		}},
	}

	_, ok := estimateSortRollupCost(probe, nil)
	require.False(t, ok, "overflowed estimates must fall back to hash")
}

func TestSortRollupCostModelRejectsUnknownNDV(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 1000,
			Outcnt:   1000,
			Cost:     1000,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}},
	}

	_, ok := estimateSortRollupCost(probe, nil)
	require.False(t, ok, "unknown NDV must not let sort win by heuristic")
}

func TestSortRollupCostModelRejectsInconsistentLeafStats(t *testing.T) {
	probe := &sortRollupProbe{
		builder: NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false),
		source: &Node{
			NodeType: plan.Node_TABLE_SCAN,
			Stats:    &Stats{TableCnt: 100, Outcnt: 101, Cost: 100, Rowsize: 64},
		},
		groupExprs: []*Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Ndv:  10,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}},
	}

	_, ok := estimateSortRollupCost(probe, nil)
	require.False(t, ok, "a leaf scan with Outcnt > TableCnt must remain fail-closed")
}

func TestSortRollupCostModelRejectsUnboundedAggregateState(t *testing.T) {
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL,
		`select a, group_concat(b) from select_test.bind_select group by a with rollup`, 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	probe := &sortRollupProbe{
		builder: builder,
		source: &Node{Stats: &Stats{
			TableCnt: 1000,
			Outcnt:   1000,
			Cost:     1000,
			Rowsize:  64,
		}},
		groupExprs: []*Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Ndv:  100,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}},
	}

	estimate, ok := estimateSortRollupCost(probe, selectClause.Exprs)
	require.True(t, ok)
	require.False(t, estimate.SortFeasible,
		"auto sort must require a bounded aggregate state")
}

func TestSortRollupCostModelRejectsVariableWidthValueAggregate(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	defer func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	}()
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	queryPlan := buildAutoRollupPlanSQLWithStats(t, 100,
		`select a, min(repeat('x', 1048576))
		 from select_test.bind_select group by a with rollup`)
	require.False(t, planHasSortRollup(queryPlan.GetQuery()),
		"a variable-width MIN argument must not use an unsafe fixed-state estimate")
}

func TestSortRollupCostModelUsesGroupUnitsForSmallSpillThreshold(t *testing.T) {
	makeProbe := func(spillMem int64) *sortRollupCostEstimate {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
		builder.aggSpillMem = spillMem
		groupExprs := make([]*Expr, 3)
		for i := range groupExprs {
			groupExprs[i] = &Expr{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Ndv:  10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
			}
		}
		estimate, ok := estimateSortRollupCost(&sortRollupProbe{
			builder: builder,
			source: &Node{Stats: &Stats{
				TableCnt: 1000,
				Outcnt:   1000,
				Cost:     1000,
				Rowsize:  64,
			}},
			groupExprs: groupExprs,
		}, nil)
		if !ok {
			return nil
		}
		return &estimate
	}

	withSmallThreshold := makeProbe(1000)
	withoutThreshold := makeProbe(-1)
	require.NotNil(t, withSmallThreshold)
	require.NotNil(t, withoutThreshold)
	require.Equal(t, withoutThreshold.HashCost, withSmallThreshold.HashCost,
		"hash spill penalty must use branch group count for a row-count threshold")
	require.Greater(t, withSmallThreshold.HashMemory, float64(1000),
		"the test must still exceed the threshold when interpreted as bytes")
}

func TestSortRollupCostModelUsesByteUnitsForSortSpill(t *testing.T) {
	makeEstimate := func(sortSpillMem int64) *sortRollupCostEstimate {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
		builder.sortSpillMem = sortSpillMem
		probe := &sortRollupProbe{
			builder: builder,
			source: &Node{Stats: &Stats{
				TableCnt: 100,
				Outcnt:   100,
				Cost:     100,
				Rowsize:  64,
			}},
			groupExprs: []*Expr{{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Ndv:  10,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			}},
		}
		estimate, ok := estimateSortRollupCost(probe, nil)
		if !ok {
			return nil
		}
		return &estimate
	}

	withSmallByteThreshold := makeEstimate(1000)
	withoutThreshold := makeEstimate(-1)
	require.NotNil(t, withSmallByteThreshold)
	require.NotNil(t, withoutThreshold)
	require.Greater(t, withSmallByteThreshold.SortCost, withoutThreshold.SortCost,
		"sort spill thresholds are byte limits even below 10K")
	require.Greater(t, withSmallByteThreshold.SortMemory, float64(1000))
}

func buildAutoRollupPlanWithStats(t *testing.T, rows float64, grouping string, suffix ...string) *Plan {
	t.Helper()
	where := ""
	if len(suffix) > 0 {
		where = " " + suffix[0]
	}
	return buildAutoRollupPlanSQLWithStats(t, rows, fmt.Sprintf(
		"select %s, count(*) from select_test.bind_select%s group by %s with rollup",
		grouping, where, grouping))
}

func buildAutoRollupPlanSQLWithStats(t *testing.T, rows float64, sql string) *Plan {
	t.Helper()
	mock := NewMockCompilerContext(true)
	table := mock.tables["bind_select"]
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableName = "bind_select"
	stats.TableCnt = rows
	for _, column := range []string{"a", "b", "c"} {
		stats.NdvMap[column] = 10
		stats.SizeMap[column] = uint64(rows * 8)
	}
	stats.NdvMap["a"] = 1000
	stats.NdvMap["b"] = 100
	stats.NdvMap["c"] = 10
	statsCache.Set(table.TblId, stats)
	ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: mock,
		statsCache:          statsCache,
	}}
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	return queryPlan
}

type rollupAlgorithmStatsCompilerContext struct {
	*fixedStatsCompilerContext
	algorithm string
}

func (ctx *rollupAlgorithmStatsCompilerContext) ResolveVariable(
	name string, isSystemVar, isGlobalVar bool,
) (interface{}, error) {
	if name == rollupAlgorithmVariable {
		return ctx.algorithm, nil
	}
	return ctx.fixedStatsCompilerContext.ResolveVariable(name, isSystemVar, isGlobalVar)
}

func buildAutoRollupPlanWithAlgorithm(
	t *testing.T, rows float64, grouping, algorithm string,
) *Plan {
	t.Helper()
	rt := moruntime.ServiceRuntime("")
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	rt.SetGlobalVariables("optimizer_hints", "determineShuffle=2")

	mock := NewMockCompilerContext(true)
	table := mock.tables["bind_select"]
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableName = "bind_select"
	stats.TableCnt = rows
	for _, column := range []string{"a", "b", "c"} {
		stats.NdvMap[column] = 10
		stats.SizeMap[column] = uint64(rows * 8)
	}
	stats.NdvMap["a"] = 1000
	stats.NdvMap["b"] = 100
	stats.NdvMap["c"] = 10
	statsCache.Set(table.TblId, stats)
	ctx := &rollupAlgorithmStatsCompilerContext{
		fixedStatsCompilerContext: &fixedStatsCompilerContext{
			statsCacheCompilerContext: &statsCacheCompilerContext{
				MockCompilerContext: mock,
				statsCache:          statsCache,
			},
		},
		algorithm: algorithm,
	}
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, fmt.Sprintf(
		"select %s, count(*) from select_test.bind_select group by %s with rollup",
		grouping, grouping), 1)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	return queryPlan
}

func planHasSortRollup(query *Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_AGG && IsSortRollupOption(node.ExtraOptions) {
			return true
		}
	}
	return false
}
