// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// This file pins the three planner regressions of issue #30938 review round 16
// (all P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780).
//
// F1 (retained-twin shortcut overreached into genuine window calls): the
// round-15 same-name twin guard in decimalBoundaryAggregateParentDomain fired
// for ANY call whose first argument is a ColRef{aggregateTag,k} with a
// same-name ctx.aggregates[k], but that shape is not unique to retained
// TIME_WINDOW copies - a genuine window-over-grouped aggregation such as
// AVG(AVG(c)) OVER () carries exactly it (window_binder.go resolves the outer
// call's argument to the inner aggregate). The parent engine applies the
// registered outer rule twice: AvgReturnType((10,2)) = (14,6) for the inner
// call and AvgReturnType((14,6)) = (18,10) for the outer, so the review
// witness COALESCE(MAX(MAX(a*b)) OVER (), AVG(AVG(c)) OVER ()) binds the
// parent merge integral max(65, 8) + scale 10 = DECIMAL(75,10) where the
// shortcut degraded the outer arm to (14,6) and the merge to (71,6) - a
// value-visible divergence (0.6666665000 vs scale-6 rounding for the
// review's NULL-product rows). The shortcut is a TIME_WINDOW-only
// conformance device: retained twins are minted exclusively into ctx.times
// by BindTimeWindowFunc and are resolved only through the timeTag site, so
// the guard is now armed only on that route.
//
// F2 (add/subtract parent domain typed by a carrier-envelope mirror):
// decimalBoundaryParentAddSub hardcoded DECIMAL128(38, s) for every D128
// addition, where the shared typing owner's decimalAddSubReturnType derives
// the domain-based min(maxIntegerDigits + scale + 1, min(oid.Width, 65))
// (list_operator.go). Over the review witness operands x, y DECIMAL(25,25)
// the mirror recovered (38,25) where the parent derives (26,25); the retry
// funnel's downstream multiply and value-merge mirrors amplified the
// divergence to a parent-common DECIMAL(84,65) - above the 76-digit carrier
// - and re-armed the merge family's checked binding error for a statement
// the pre-change engine binds at DECIMAL(75,65): COALESCE(a*b, (x+y)*z,
// CAST(0 AS DECIMAL(65,65))) over a DECIMAL(65,55), b DECIMAL(20,0), x, y
// DECIMAL(25,25), z DECIMAL(5,5). The recovery now delegates to the shared
// authoritative publication function.DecimalAddSubPublication, the same
// exported owner the storage-boundary envelope already uses (design §4.1,
// revision 40 F-OUTPUT-DIVERGENCE-PROVENANCE: no re-implemented mirror
// remains to drift).
//
// F3 (shadow charge read consumer ordinals in the binding-tag namespace):
// cteExportedSlotWideDecimalProvenance interpreted a mirror ColRef.RelPos as
// a CHILD ORDINAL, but reuseMultiReferenceCTEs runs from build.go before
// remapAllColRefs, where RelPos carries the pre-remap BINDING TAG; the
// ordinal walk then declined every tagged result-projection mirror (an
// ORDER BY producer or a nested derived-table chain), so the admission
// estimate and reservation omitted the Decimal256 shadow column that
// finalization (build_ddl.go ensureShadowColumn) later appends to the
// producer and its SINK - the committed reservation could exceed the charged
// bytes, the exact gap approved design decision log 50
// (F-SHADOW-ADMISSION-ACCOUNTING) closes. The mirror descent now resolves
// the reference's owner node through builder.tag2NodeID (the same ownership
// table appendNode maintains and the binder's other pre-remap resolutions
// use) and continues from the owner's output column. Every expected tag and
// byte count below derives from the registered rules and the estimator
// owners, never from observed candidate behavior.

// registerReviewRound16MockTables adds the grouped-window and mixed-domain
// fixtures beside the round-15 sliding-witness fixture.
func registerReviewRound16MockTables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	registerReviewRound15MockTables(t, mock)
	twg, err := buildTestCreateTableStmt(mock,
		"create table twg30938(g int, a decimal(38,0), b decimal(38,0), c decimal(10,2))")
	require.NoError(t, err)
	mock.ctxt.tables["twg30938"] = twg
	mock.ctxt.objects["twg30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "twg30938"}
	tmix, err := buildTestCreateTableStmt(mock,
		"create table tmix30938(a decimal(65,55), b decimal(20,0), "+
			"x decimal(25,25), y decimal(25,25), z decimal(5,5))")
	require.NoError(t, err)
	mock.ctxt.tables["tmix30938"] = tmix
	mock.ctxt.objects["tmix30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "tmix30938"}
	registerModBoundaryTable(t, mock, "create table m30938r(a decimal(38,0), b decimal(38,0))")
}

func buildReviewRound16Statement(t *testing.T, sql string) (*pb.Plan, error) {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerReviewRound16MockTables(t, mock)
	return runOneStmt(mock, t, sql)
}

func TestDecimalGroupedWindowAggregateParentDomainReviewRound16Issue30938(t *testing.T) {
	t.Run("groupedWindowMergeWitness", func(t *testing.T) {
		// The review witness: a genuine window-over-grouped aggregation.
		// The parent applies the registered AVG rule at each nesting level -
		// AvgReturnType((10,2)) = DECIMAL128(14,6) for the inner call and
		// AvgReturnType((14,6)) = DECIMAL128(18,10) for the outer window call
		// - and the parent merge over (65,0)+(18,10) binds integral 65 +
		// scale 10 = (75,10). The retained-twin shortcut must stay on the
		// TIME_WINDOW route so this genuine nested call keeps both
		// applications.
		built, err := buildReviewRound16Statement(t,
			"select coalesce(max(max(a*b)) over (), avg(avg(c)) over ()) as product "+
				"from twg30938 group by g")
		require.NoError(t, err,
			"the merge over the genuine grouped-window outputs must recover the parent domain")
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 75, 10,
			"the parent merge over (65,0)+(18,10) binds (75,10), never the shortcut's (71,6)")
	})
	t.Run("groupedWindowAvgOnlyControl", func(t *testing.T) {
		// The genuine nested window aggregate's own publication applies the
		// registered rule at each level on both engines; no widened
		// multiplication participates, so the retry funnel never fires and
		// the repair cannot alter this shape.
		built, err := buildReviewRound16Statement(t,
			"select avg(avg(c)) over () as av from twg30938 group by g")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		require.Equal(t, int32(types.T_decimal128), head[0].Typ.Id)
		require.Equal(t, int32(18), head[0].Typ.Width)
		require.Equal(t, int32(10), head[0].Typ.Scale)
	})
	t.Run("groupedWindowMaxOnlyControl", func(t *testing.T) {
		// Value-selecting forms publish the selected argument's domain at
		// every level (identity rule), so the widened product surfaces
		// unchanged on both the pre- and post-repair engine.
		built, err := buildReviewRound16Statement(t,
			"select max(max(a*b)) over () as product from twg30938 group by g")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 76, 0,
			"the value-selecting nested window keeps the widened publication")
	})
	t.Run("timeWindowNonSlidingTwinControl", func(t *testing.T) {
		// The retained non-sliding TIME_WINDOW twin is the one route the
		// shortcut exists for: the descent already applied the ordinary AVG
		// rule once, so the twin resolves (14,6) and the parent merge over
		// (65,0)+(14,6) keeps binding (71,6).
		built, err := buildReviewRound16Statement(t,
			"select coalesce(max(a * b), avg(c)) as product "+
				"from twm30938 interval(ts, 2, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 71, 6,
			"the non-sliding retained twin keeps the single-application parent merge")
	})
	t.Run("timeWindowSlidingControl", func(t *testing.T) {
		// The sliding route's round-15 repair (avg_tw_result applies its own
		// registered AvgTwResultReturnType over the cache surface) is
		// independent of the twin shortcut and must keep binding (73,8).
		built, err := buildReviewRound16Statement(t,
			"select coalesce(max(a * b), avg(c)) as product "+
				"from twm30938 interval(ts, 2, minute) sliding(1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 73, 8,
			"the sliding route keeps the avg_tw_result parent merge")
	})
	t.Run("plainRouteControl", func(t *testing.T) {
		built, err := buildReviewRound16Statement(t,
			"select coalesce(max(a * b), avg(c)) as product from twm30938")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 71, 6,
			"the plain route recovers the same ordinary-AVG parent merge")
	})
}

func TestDecimalAddSubParentDomainPublicationReviewRound16Issue30938(t *testing.T) {
	t.Run("threeArmMergeWitness", func(t *testing.T) {
		// The review witness: the parent derives x+y = DECIMAL(26,25) and
		// (x+y)*z = DECIMAL(31,25) through the shared typing owner, so the
		// parent domains of the three arms - a*b at (65,55), (x+y)*z at
		// (31,25) and the authored cast at (65,65) - converge to integral
		// max(10, 6, 0) + scale 65 = DECIMAL(75,65), which the changed
		// engine's decimal merge overloads admit. The carrier-envelope
		// mirror instead recovered (38,25) -> (44,25) -> (84,65), above the
		// carrier, and re-armed the merge family's checked error for this
		// parent-supported statement.
		built, err := buildReviewRound16Statement(t,
			"select coalesce(a*b, (x+y)*z, cast(0 as decimal(65,65))) as product "+
				"from tmix30938")
		require.NoError(t, err,
			"the parent-supported three-arm merge must bind through the "+
				"authoritative add/subtract publication")
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 75, 65,
			"the parent merge over (65,55)+(31,25)+(65,65) binds (75,65)")
	})
	t.Run("twoArmNoAddControl", func(t *testing.T) {
		// The same merge without an add/subtract arm recovers (65,55) and
		// (65,65) on both the pre- and post-repair engine: integral
		// max(10, 0) + scale 65 = (75,65).
		built, err := buildReviewRound16Statement(t,
			"select coalesce(a*b, cast(0 as decimal(65,65))) as product from tmix30938")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 75, 65,
			"the add-free merge never consulted the add/subtract recovery")
	})
	t.Run("addChainNoMergeControl", func(t *testing.T) {
		// Outside the retry funnel the shared typing owner already typed the
		// chain on both engines: x+y = (26,25), (x+y)*z = (31,25).
		built, err := buildReviewRound16Statement(t,
			"select (x+y)*z as product from tmix30938")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		require.Equal(t, int32(types.T_decimal128), head[0].Typ.Id)
		require.Equal(t, int32(31), head[0].Typ.Width)
		require.Equal(t, int32(25), head[0].Typ.Scale,
			"the first-pass add/multiply chain keeps the owner's publication")
	})
	t.Run("noRetryMergeControl", func(t *testing.T) {
		// Without a widened multiplication the funnel never fires: the merge
		// over (31,25)+(65,65) binds integral max(6, 0) + scale 65 = (71,65)
		// on the first pass, identically on both engines.
		built, err := buildReviewRound16Statement(t,
			"select coalesce((x+y)*z, cast(0 as decimal(65,65))) as product from tmix30938")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 71, 65,
			"the add-carrying merge without a retry trigger stays byte-identical")
	})
}

// reviewRound16ReuseDrive binds one two-reader shared-CTE statement through
// the real binder (pre-remap binding tags intact, exactly the namespace
// reuseMultiReferenceCTEs runs in from build.go) and returns the builder and
// bound root ready for the admission call. The consumers sit in UNION ALL
// branches so the drain witness and the unfiltered-consumer exactness gate
// hold; the producer filter's selectivity makes the share profitable under
// the mock's default statistics.
func reviewRound16ReuseDrive(t *testing.T, sql string, memLimit, spillLimit int64) (*QueryBuilder, int32) {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, "create table m30938r(a decimal(38,0), b decimal(38,0))")
	proc.Base.Lim.Size = memLimit
	proc.Base.Lim.SpillSize = spillLimit
	ctx := mock.CurrentContext()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	rootID, err := builder.bindSelect(sel, bindCtx, true)
	require.NoError(t, err)
	builder.finalizeCTEIdentity(rootID)
	builder.parseOptimizeHints()
	require.False(t, builder.sharedComputationDisabled())
	require.Len(t, builder.cteRefs, 1)
	require.Len(t, builder.cteRefs[0].occurrences, 2)
	return builder, rootID
}

// reviewRound16SinkCount counts the committed share's SINK/SINK_SCAN nodes.
func reviewRound16SinkCount(builder *QueryBuilder) (sinks, sinkScans int) {
	for _, n := range builder.qry.Nodes {
		if n == nil {
			continue
		}
		switch n.NodeType {
		case pb.Node_SINK:
			sinks++
		case pb.Node_SINK_SCAN:
			sinkScans++
		}
	}
	return sinks, sinkScans
}

func TestDecimalSharedProducerTaggedMirrorShadowChargeReviewRound16Issue30938(t *testing.T) {
	// The mock's default statistics give the filtered producer 50 estimated
	// rows at row size 100, so the materialization estimate is
	// 50 * max(100, declared) = 5000 with or without the allowance; the
	// discriminating reservation component is the spill framing: one
	// nullable Decimal256 column frames 118 bytes per record on top of the
	// 69-byte record fixed cost, so the shadow-inclusive reservation is
	// 5000 + 50 * (69 + 2 * 118) = 20250 against the uncharged
	// 5000 + 50 * (69 + 118) = 14350 (estimator owners in cte_reuse.go).
	const (
		commitMem        = float64(5000)
		chargedSpill     = float64(20250)
		spillBetween     = int64(17000) // above 14350, below 20250
		bigBudget        = int64(1) << 30
		bigSpillBudget   = int64(1) << 40
		orderByMirrorSQL = "with c as (select a*b as v from m30938r where a > 5 order by a) " +
			"select sum(v) from c union all select sum(v) from c"
		nestedMirrorSQL = "with c as (select v from (select a*b as v from m30938r where a > 5) d) " +
			"select sum(v) from c union all select sum(v) from c"
		plainSQL = "with c as (select a*b as v from m30938r where a > 5) " +
			"select sum(v) from c union all select sum(v) from c"
		unprofitableSQL = "with c as (select a*b as v from m30938r order by a) " +
			"select sum(v) from c union all select sum(v) from c"
	)

	t.Run("orderByMirrorReservationIncludesEventualShadow", func(t *testing.T) {
		// The review witness shape: the tagged result-projection mirror
		// under the ORDER BY resolves to the producing multiplication
		// through the binding-tag owner, so the admission charges one
		// shadow allowance before the share commits.
		builder, rootID := reviewRound16ReuseDrive(t, orderByMirrorSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types)+1,
			"the tagged ORDER BY mirror carries widened-multiplication provenance")
		builder.reuseMultiReferenceCTEs(rootID)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, chargedSpill, builder.sharedMaterializationSpillBytes,
			"the reservation charges the shadow column finalization appends")
	})
	t.Run("nestedMirrorReservationIncludesEventualShadow", func(t *testing.T) {
		builder, rootID := reviewRound16ReuseDrive(t, nestedMirrorSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types)+1,
			"the tagged nested bare-column mirror chain resolves to the multiplication")
		builder.reuseMultiReferenceCTEs(rootID)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, chargedSpill, builder.sharedMaterializationSpillBytes)
	})
	t.Run("plainProducerChargesOnBothEngines", func(t *testing.T) {
		// The direct-projection producer carries its provenance in the
		// exported expression itself: the ordinal walk already resolved it,
		// so this control passes identically on the pre- and post-repair
		// engine and pins the unaffected path.
		builder, rootID := reviewRound16ReuseDrive(t, plainSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types)+1)
		builder.reuseMultiReferenceCTEs(rootID)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, chargedSpill, builder.sharedMaterializationSpillBytes)
	})
	t.Run("nearCapBudgetDeclinesAndLeavesCountersUnchanged", func(t *testing.T) {
		// Between the uncharged 14350 and the charged 20250 spill figures
		// the shadow allowance alone decides admission: the expanded
		// reservation exceeds the cap, the share declines to the historical
		// inline plan, and every cumulative counter stays byte-identical
		// (approved design decision log 50's declined-reservation oracle).
		builder, rootID := reviewRound16ReuseDrive(t, orderByMirrorSQL, bigBudget, spillBetween)
		nodeCount := len(builder.qry.Nodes)
		newRoot := builder.reuseMultiReferenceCTEs(rootID)
		require.Equal(t, rootID, newRoot, "the declined share keeps the inline plan")
		require.Len(t, builder.qry.Nodes, nodeCount,
			"the declined share appends no producer or consumer nodes")
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Zero(t, sinks)
		require.Zero(t, sinkScans)
		require.Zero(t, builder.sharedMaterializationMemoryBytes,
			"a declined reservation leaves the memory counter byte-identical")
		require.Zero(t, builder.sharedMaterializationSpillBytes,
			"a declined reservation leaves the spill counter byte-identical")
	})
	t.Run("unprofitableShapeStaysInline", func(t *testing.T) {
		// Without the selective filter the share is not profitable under
		// the mock's default statistics: the profitability gate declines on
		// both engines, so the witness's filter only exercises the
		// admission path and changes no gate semantics.
		builder, rootID := reviewRound16ReuseDrive(t, unprofitableSQL, bigBudget, bigSpillBudget)
		newRoot := builder.reuseMultiReferenceCTEs(rootID)
		require.Equal(t, rootID, newRoot)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Zero(t, sinks)
		require.Zero(t, sinkScans)
		require.Zero(t, builder.sharedMaterializationMemoryBytes)
		require.Zero(t, builder.sharedMaterializationSpillBytes)
	})
	t.Run("finalizationAppendsTheChargedShadow", func(t *testing.T) {
		// The eventual shadow the reservation must cover: finalization
		// (build_ddl.go ensureShadowColumn) appends one Decimal256 seam
		// column to the producer's SINK and to every consumer SINK_SCAN for
		// this committed tagged-mirror shape. This pinning passes on the
		// pre- and post-repair engine alike - finalization is unchanged;
		// the repair charges what it appends.
		built, err := buildReviewRound16Statement(t, orderByMirrorSQL)
		require.NoError(t, err)
		query := built.GetQuery()
		require.NotNil(t, query)
		sinks, sinkScans := 0, 0
		for _, n := range query.Nodes {
			if n == nil {
				continue
			}
			switch n.NodeType {
			case pb.Node_SINK:
				sinks++
				require.Len(t, n.ProjectList, 2,
					"the producer SINK carries the exported slot plus its shadow")
				require.Equal(t, int32(types.T_decimal256), n.ProjectList[1].Typ.Id)
			case pb.Node_SINK_SCAN:
				sinkScans++
				require.Len(t, n.ProjectList, 2,
					"each consumer SINK_SCAN carries the exported slot plus its shadow")
				require.Equal(t, int32(types.T_decimal256), n.ProjectList[1].Typ.Id)
			}
		}
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
	})
}
