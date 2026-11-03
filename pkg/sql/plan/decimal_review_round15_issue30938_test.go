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
	"github.com/stretchr/testify/require"
)

// This file pins the two planner regressions of issue #30938 review round 15
// (both P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780).
//
// F1 (avg_tw_result typed by the wrong registered rule): a value merge over
// a SLIDING TIME_WINDOW pending aggregate output recovered the AVG operand's
// parent domain through decimalBoundaryAggregateParentDomain's combined
// "avg"/"avg_tw_result" case, which applied the ordinary AvgReturnType to
// the sliding cache's VARCHAR surface - the registered ordinary rule's
// default branch publishes FLOAT64 (aggexec/sumavg2.go), so the retried
// merge bound the lossy FLOAT64 where the parent binds its own registered
// AvgTwResultReturnType (aggexec/avg_tw_result2.go) over the same cache
// surface: DECIMAL128(18, 8) for a DECIMAL(10,2) argument (cache scale
// clamp(12, 2, 8) = 8, aggexec/avg_tw_cache2.go). The review witness
// COALESCE(MAX(a*b), AVG(c)) over DECIMAL(38,0) operands then binds the
// parent merge integral max(65, 10) + scale 8 = DECIMAL(73,8); the repaired
// recovery reuses AvgTwResultReturnType over the resolved cache domain.
//
// F2 (shadow columns uncharged at shared-materialization admission):
// build_ddl.go's finalization appends one Decimal256 shadow column per
// exported slot whose producing expression carries widened-multiplication
// provenance, but cte_reuse.go's admission estimated and reserved only the
// original output schema, so a near-cap plan could be admitted on bytes the
// runtime reservation (sql/internal/materialized/source.go) later exceeds.
// Approved design decision log entry 50 (F-SHADOW-ADMISSION-ACCOUNTING)
// requires the reservation computed from the SHADOW-EXPANDED schema before
// the share commits: one fixed-width Decimal256 payload (32 bytes) per
// qualifying exported slot - an occurrence-independent conservative bound,
// since a slot shadows at most once per statement no matter how many
// consumers admit it. Every expected tag and byte count below derives from
// the registered rules and the estimator owners, never from observed
// candidate behavior.

// registerReviewRound15MockTables adds the sliding-witness fixture carrying
// the DECIMAL(10,2) avg operand beside the window fixtures.
func registerReviewRound15MockTables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	registerWindowMockTables(t, mock)
	twm, err := buildTestCreateTableStmt(mock,
		"create table twm30938(ts timestamp, a decimal(38,0), b decimal(38,0), c decimal(10,2))")
	require.NoError(t, err)
	mock.ctxt.tables["twm30938"] = twm
	mock.ctxt.objects["twm30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "twm30938"}
}

func buildReviewRound15Statement(t *testing.T, sql string) (*pb.Plan, error) {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerReviewRound15MockTables(t, mock)
	return runOneStmt(mock, t, sql)
}

func TestDecimalTimeWindowAvgResultParentDomainReviewRound15Issue30938(t *testing.T) {
	t.Run("slidingMixedMergeWitness", func(t *testing.T) {
		// The review witness: the sliding route republishes AVG(c) through
		// avg_tw_result over the VARCHAR(., 8) cache; the parent's
		// registered result rule binds DECIMAL128(18, 8), and the parent
		// merge over (65,0)+(18,8) binds integral 65 + scale 8 = (73,8).
		built, err := buildReviewRound15Statement(t,
			"select coalesce(max(a * b), avg(c)) as product "+
				"from twm30938 interval(ts, 2, minute) sliding(1, minute)")
		require.NoError(t, err,
			"the merge over the retained sliding outputs must recover the parent domain")
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 73, 8,
			"the parent merge over (65,0)+(18,8) binds (73,8), never FLOAT64")
	})
	t.Run("slidingMaxOnlyControl", func(t *testing.T) {
		built, err := buildReviewRound15Statement(t,
			"select max(a * b) as product from twm30938 "+
				"interval(ts, 2, minute) sliding(1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 76, 0,
			"the retained sliding MAX keeps the widened publication")
	})
	t.Run("slidingAvgOnlyControl", func(t *testing.T) {
		// avg_tw_result's first-pass publication is the registered
		// DECIMAL128(18, 8) on both engines; the repair touches only the
		// parent-domain recovery and cannot alter this shape.
		built, err := buildReviewRound15Statement(t,
			"select avg(c) as av from twm30938 "+
				"interval(ts, 2, minute) sliding(1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		require.Equal(t, int32(types.T_decimal128), head[0].Typ.Id)
		require.Equal(t, int32(18), head[0].Typ.Width)
		require.Equal(t, int32(8), head[0].Typ.Scale)
	})
	t.Run("nonSlidingAvgMergeControl", func(t *testing.T) {
		// Without SLIDING the cache remap never fires (having_binder.go's
		// timeTag && sliding gate): the retained AVG resolves through the
		// ordinary rule AvgReturnType((10,2)) = DECIMAL128(14,6), and the
		// parent merge over (65,0)+(14,6) binds (71,6) on both engines.
		built, err := buildReviewRound15Statement(t,
			"select coalesce(max(a * b), avg(c)) as product "+
				"from twm30938 interval(ts, 2, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 71, 6,
			"the non-sliding route keeps the ordinary AVG rule's parent merge")
	})
	t.Run("plainRouteControl", func(t *testing.T) {
		built, err := buildReviewRound15Statement(t,
			"select coalesce(max(a * b), avg(c)) as product from twm30938")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 71, 6,
			"the plain route recovers the same ordinary-AVG parent merge")
	})
	t.Run("genuineWideSlidingDeclines", func(t *testing.T) {
		// A genuine 76-digit literal carries no multiplication provenance:
		// the recovery declines and the family's original checked outcome
		// re-arms verbatim on both pre- and post-repair engines; the split
		// avg_tw_result case never turns the decline into an admission.
		_, err := buildReviewRound15Statement(t,
			"select coalesce("+
				"max(9999999999999999999999999999999999999999999999999999999999999999999999999999), "+
				"avg(c)) as product from twm30938 interval(ts, 2, minute) sliding(1, minute)")
		require.Error(t, err,
			"a genuine-wide merge without provenance never retries")
		require.Contains(t, err.Error(),
			"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]",
			"the declined merge re-arms the family's original checked outcome "+
				"(the second operand is the sliding AVG's DECIMAL128(18,8) surface)")
	})
}

// round15WideProductExpr builds a bound `a * b` node published at the
// widened (76,0) - the syntactic provenance marker
// rule.CarriesWideDecimalMultiplicationProvenance matches.
func round15WideProductExpr() *pb.Expr {
	decimal38 := pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0}
	return &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "*"},
			Args: []*pb.Expr{
				{Typ: decimal38, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}}},
				{Typ: decimal38, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 1}}},
			},
		}},
	}
}

func round15UuidExpr() *pb.Expr {
	return &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Width: 36, Scale: 0},
		Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "uuid"}}},
	}
}

func round15SumExpr(arg *pb.Expr) *pb.Expr {
	return &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0},
		Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "sum"}, Args: []*pb.Expr{arg}}},
	}
}

func round15ColExpr(relPos, colPos int32) *pb.Expr {
	return &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: relPos, ColPos: colPos}},
	}
}

func round15ProducerBuilder(nodes ...*pb.Node) *QueryBuilder {
	return &QueryBuilder{qry: &Query{Nodes: nodes}}
}

func TestDecimalSharedProducerShadowReservationReviewRound15Issue30938(t *testing.T) {
	decimal76 := pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0}
	varchar36 := pb.Type{Id: int32(types.T_varchar), Width: 36, Scale: 0}

	t.Run("oneShadowByteDelta", func(t *testing.T) {
		// Decision 50's one-shadow oracle: one qualifying exported slot
		// adds exactly one fixed-width Decimal256 payload (32 bytes) plus
		// its nullable share to the charged schema; the uuid slot carries
		// no provenance and stays uncharged.
		scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}
		producer := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			ProjectList: []*pb.Expr{round15WideProductExpr(), round15UuidExpr()}}
		builder := round15ProducerBuilder(scan, producer)
		exported := []pb.Type{decimal76, varchar36}
		expanded := builder.cteShadowExpandedStorageTypes(1, exported, exported)
		require.Len(t, expanded, 3,
			"one provenance-carrying exported slot charges one shadow allowance")
		require.Equal(t, int32(types.T_decimal256), expanded[2].Id)
		require.Equal(t, int32(76), expanded[2].Width)
		require.Equal(t, int32(0), expanded[2].Scale)
		require.False(t, expanded[2].NotNullable)

		declaredOrig, ok := materializedDeclaredRowSize(exported)
		require.True(t, ok)
		declaredExpanded, ok := materializedDeclaredRowSize(expanded)
		require.True(t, ok)
		require.Equal(t, float64(33), declaredExpanded-declaredOrig,
			"the allowance is one 32-byte Decimal256 payload plus its nullable share")

		stats := &pb.Stats{Outcnt: 1000, Rowsize: 1}
		estOrig, _, ok := cteEstimatedMaterializedBytes(stats, exported)
		require.True(t, ok)
		estExpanded, _, ok := cteEstimatedMaterializedBytes(stats, expanded)
		require.True(t, ok)
		require.Equal(t, 1000*declaredExpanded, estExpanded)
		require.Equal(t, float64(33000), estExpanded-estOrig,
			"the reservation charges the shadow payload for every estimated row")
	})
	t.Run("multipleShadowsPerSlotNeverPerConsumer", func(t *testing.T) {
		// Two qualifying slots charge two allowances; the count derives
		// from the producer's own exported slots, so a narrowed demanded
		// subset (occurrence-dependent) never shrinks it - a slot shadows
		// at most once per statement no matter how many consumers admit it
		// (decision 50; the emission side is pinned by
		// TestDecimalSharedProducerAllColumnsReferencedIssue30938).
		scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}
		producer := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			ProjectList: []*pb.Expr{
				round15WideProductExpr(), round15WideProductExpr(), round15UuidExpr(),
			}}
		builder := round15ProducerBuilder(scan, producer)
		exported := []pb.Type{decimal76, decimal76, varchar36}
		require.Len(t, builder.cteShadowExpandedStorageTypes(1, exported, exported), 5,
			"two provenance-carrying exported slots charge two allowances")
		require.Len(t, builder.cteShadowExpandedStorageTypes(1, exported, exported[:1]), 3,
			"the allowance counts exported slots, never the demanded subset")
	})
	t.Run("mirrorAndRowChangingBoundaryResolution", func(t *testing.T) {
		// At the reuse stage the plan is pre-remap: a projection column's
		// RelPos names the feeding node's binding tag, resolved through
		// the builder's tag map (review round 16, F3), so the hand-built
		// mirrors below reference tag values registered on the builder.
		scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}
		inner := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			BindingTags: []int32{100},
			ProjectList: []*pb.Expr{round15WideProductExpr(), round15UuidExpr()}}
		mirror := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 2, Children: []int32{1},
			ProjectList: []*pb.Expr{round15ColExpr(100, 0), round15ColExpr(100, 1)}}
		builder := round15ProducerBuilder(scan, inner, mirror)
		builder.tag2NodeID = map[int32]int32{100: 1}
		exported := []pb.Type{decimal76, varchar36}
		require.Len(t, builder.cteShadowExpandedStorageTypes(2, exported, exported), 3,
			"a bare-column mirror resolves to the producing multiplication")

		agg := &pb.Node{NodeType: pb.Node_AGG, NodeId: 1, Children: []int32{0},
			BindingTags: []int32{101},
			GroupBy:     []*pb.Expr{round15ColExpr(0, 0)},
			AggList:     []*pb.Expr{round15SumExpr(round15WideProductExpr())}}
		aggRoot := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 2, Children: []int32{1},
			ProjectList: []*pb.Expr{round15ColExpr(101, 1)}}
		aggBuilder := round15ProducerBuilder(scan, agg, aggRoot)
		aggBuilder.tag2NodeID = map[int32]int32{101: 1}
		require.Len(t, aggBuilder.cteShadowExpandedStorageTypes(2, exported[:1], exported[:1]), 2,
			"a bare column over a row-changing boundary conservatively charges the branch")

		leafRoot := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			ProjectList: []*pb.Expr{round15ColExpr(102, 0)}}
		leafBuilder := round15ProducerBuilder(scan, leafRoot)
		leafBuilder.tag2NodeID = map[int32]int32{102: 0}
		require.Len(t, leafBuilder.cteShadowExpandedStorageTypes(1, exported[:1], exported[:1]), 1,
			"a raw scan column never carries expression provenance")
	})
	t.Run("cleanProducerChargedNothing", func(t *testing.T) {
		scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}
		producer := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			BindingTags: []int32{103},
			ProjectList: []*pb.Expr{round15ColExpr(103, 0), round15UuidExpr()}}
		builder := round15ProducerBuilder(scan, producer)
		builder.tag2NodeID = map[int32]int32{103: 0}
		exported := []pb.Type{decimal76, varchar36}
		require.Equal(t, exported, builder.cteShadowExpandedStorageTypes(1, exported, exported),
			"statements without widened multiplication keep byte-identical accounting")
	})
	t.Run("cumulativeProducersAndNearCapRejection", func(t *testing.T) {
		// Decision 50's cumulative and declined-reservation oracles over
		// the expanded schema: two producers sum into the cumulative
		// counters; the reservation that exceeds the remaining cap
		// declines and leaves every counter byte-identical, and at the
		// margin the shadow allowance alone decides the outcome.
		exported := []pb.Type{decimal76}
		expanded := append(exported[:0:0], exported...)
		expanded = append(expanded, decimal76)
		stats := &pb.Stats{Outcnt: 1024, Rowsize: 1}
		estExpanded, _, ok := cteEstimatedMaterializedBytes(stats, expanded)
		require.True(t, ok)
		require.Equal(t, float64(1024*66), estExpanded)
		estOrig, _, ok := cteEstimatedMaterializedBytes(stats, exported)
		require.True(t, ok)
		require.Equal(t, float64(1024*33), estOrig)

		spillExpanded, ok := estimatedSharedMaterializationSpillBytes(estExpanded, 1024, expanded)
		require.True(t, ok)
		spillOrig, ok := estimatedSharedMaterializationSpillBytes(estOrig, 1024, exported)
		require.True(t, ok)
		require.Equal(t, estExpanded-estOrig+
			1024*float64(sharedMaterializationSpillVectorFixedBytes+
				sharedMaterializationSpillVectorMetadataBytes+
				sharedMaterializationSpillGroupingBytes+
				sharedMaterializationSpillNullableBytes),
			spillExpanded-spillOrig,
			"the spill framing charges the shadow column's per-record vectors")

		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		proc := mock.CurrentContext().GetProcess()
		require.NotNil(t, proc)
		require.NotNil(t, proc.Base)
		proc.Base.Lim.Size = 200000
		proc.Base.Lim.SpillSize = 1 << 40
		builder := &QueryBuilder{compCtx: mock.CurrentContext()}
		require.True(t, builder.reserveSharedMaterialization(estExpanded, 1024, expanded))
		require.True(t, builder.reserveSharedMaterialization(estExpanded, 1024, expanded))
		require.Equal(t, 2*estExpanded, builder.sharedMaterializationMemoryBytes,
			"cumulative producers sum the shadow-expanded charges")
		require.Equal(t, 2*spillExpanded, builder.sharedMaterializationSpillBytes)
		require.False(t, builder.reserveSharedMaterialization(estExpanded, 1024, expanded),
			"the reservation exceeding the remaining cap declines")
		require.Equal(t, 2*estExpanded, builder.sharedMaterializationMemoryBytes,
			"a declined reservation leaves every counter byte-identical")
		require.Equal(t, 2*spillExpanded, builder.sharedMaterializationSpillBytes)

		proc.Base.Lim.Size = 50000
		marginal := &QueryBuilder{compCtx: mock.CurrentContext()}
		require.False(t, marginal.reserveSharedMaterialization(estExpanded, 1024, expanded),
			"the shadow-expanded charge exceeds the margin cap")
		unexpanded := &QueryBuilder{compCtx: mock.CurrentContext()}
		require.True(t, unexpanded.reserveSharedMaterialization(estOrig, 1024, exported),
			"the shadow allowance alone decides the outcome at the margin")
	})
	t.Run("noAdmittingReaderControl", func(t *testing.T) {
		// The allowance is reader-independent by construction (the charge
		// sees only the producer's exported slots): a producer whose
		// readers never admit still reserves the bound, while finalization
		// emits no shadow - pinned by
		// TestDecimalSharedProducerValueReadersUnaffectedIssue30938 - so
		// the emitted schema never exceeds the charged schema.
		scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}
		producer := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 1, Children: []int32{0},
			ProjectList: []*pb.Expr{round15WideProductExpr(), round15UuidExpr()}}
		builder := round15ProducerBuilder(scan, producer)
		exported := []pb.Type{decimal76, varchar36}
		expanded := builder.cteShadowExpandedStorageTypes(1, exported, exported)
		require.Len(t, expanded, len(exported)+1,
			"the conservative bound is charged with no admitting reader in the inputs")
		require.Equal(t, exported, expanded[:len(exported)],
			"the charged schema covers the emitted schema as a prefix")
	})
}
