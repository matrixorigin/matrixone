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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/stretchr/testify/require"
)

// This file pins the two planner regressions of issue #30938 review round 17
// (both P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780).
//
// F1 (authored fixed-domain casts collided in the decision-ledger content
// key): writeDecimalBoundaryObservedType (rule/constant_fold.go) unwrapped
// EVERY cast when encoding a decision argument's observed pre-coercion type,
// including SQL-authored casts, so two coercive decisions whose operands
// differ only behind authored fixed-domain casts keyed identically — the
// registry's plain map assignment (process/decimal_boundary_registry.go)
// then kept only the later entry, breaking the recorded invariant that
// distinct decisions never share a key (design decision log 52). Over the
// review witness t(a DECIMAL(38,30), b DECIMAL(38,30), d DECIMAL(1,0)):
//   - the parent publishes a*b as DECIMAL(65,30), ROUND(.,0) reserves one
//     carry digit (integerDigits+1 = 36), and the merges converge to
//     persisted DECIMAL(36,0) over the (34,0) default and DECIMAL(37,0)
//     over the (37,0) default;
//   - the changed engine publishes a*b as DECIMAL(76,30) and ROUND as
//     (47,0), so both merges align their default arm to (47,0) — the
//     bound argument is the binder's implicit alignment cast (overload 0)
//     over the authored cast, and unwrapping both layers encoded the same
//     observed type (the raw d column) for the (34,0) and (37,0)
//     decisions. The later record overwrote the earlier, the surviving
//     entry's PreTypes then mismatched the earlier decision's examined
//     operand at the decisionOwnedCastOperand ownership check
//     (build_ddl.go), the binder's alignment cast was misclassified
//     decision-foreign, and the first-recorded column persisted the (47,0)
//     alignment envelope instead of the parent's (36,0)/(37,0).
// The key now unwraps only the binder's implicit alignment casts (overload
// 0 without the syntax-explicit mark) and preserves every authored
// fixed-domain cast — the same authored-cast predicate the boundary
// resolver already uses (base_binder.go) — so the two decisions key
// distinctly and both persist parent-identical schemas.
//
// F2 (shadow charge ignored provenance carried through scalar export
// references): cteExportedSlotWideDecimalProvenance (cte_reuse.go) returned
// false for any exported slot whose producing expression was not a bare
// column and carried no syntactic widened multiplication, without resolving
// the expression's column operands through the producer lineage. Over the
// review witness
//
//	with c as (select coalesce(d.v,d.v) as v from
//	           (select a*b as v from m30938r where a > 5) d)
//	select sum(v) from c union all select sum(v) from c
//
// the COALESCE's operands are tagged column references into the derived
// table whose projection holds the widened product, so the reservation
// charged zero shadow allowances, while the post-remap finalization
// (build_ddl.go's ensureShadowColumn, reached from the admitting SUM
// readers) resolves the same operands, admits the divergent product seam
// inside the replay-safe COALESCE, and appends one Decimal256 shadow column
// to the producer, its SINK and every SINK_SCAN reader — the committed
// reservation covered fewer columns than execution emits, the exact gap
// approved design decision log 50 (F-SHADOW-ADMISSION-ACCOUNTING: "the
// finalization's appends stay strictly inside the charged schema and its
// emission can never exceed it") closes. The charge now resolves provenance
// through the scalar expression's column operands using the same pre-remap
// binding-tag ownership table the mirror descent uses, bounded by a visited
// set, restricted to the replay-safe fragment finalization can replicate
// (literal/column leaves under decimal arithmetic, decimal-to-decimal casts
// and coalesce — every other form appends no shadow there and charges
// nothing here). Every expected tag and byte count below derives from the
// registered rules and the estimator owners, never from observed candidate
// behavior.

const (
	reviewRound17Expr34 = "coalesce(round(a*b, 0), cast(d as decimal(34,0)))"
	reviewRound17Expr37 = "coalesce(round(a*b, 0), cast(d as decimal(37,0)))"
	reviewRound17Table  = "create table t30938r17(a decimal(38,30), b decimal(38,30), d decimal(1,0))"
)

// buildReviewRound17Mock returns a mock optimizer with the review witness
// table registered.
func buildReviewRound17Mock(t *testing.T) *MockOptimizer {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, reviewRound17Table)
	return mock
}

// ddlPlanHeadExprsN returns the n output expressions of a CTAS DDL plan's
// query, the slots RefreshPreparedCTASInferredColumns and the storage
// boundary evaluate.
func ddlPlanHeadExprsN(t *testing.T, ddlPlan *pb.Plan, n int) []*pb.Expr {
	t.Helper()
	query := ddlPlan.GetDdl().Query
	require.NotNil(t, query)
	head := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.Len(t, head, n)
	return head
}

// requireReviewRound17Merge asserts the bound shape of one witness merge and
// returns it: the ROUND arm publishes the candidate's (47,0) and stays
// uncast under the same-type alignment skip, and the default arm is the
// binder's implicit alignment cast (47,0) over the SQL-authored fixed-domain
// cast of the declared width.
func requireReviewRound17Merge(t *testing.T, head *pb.Expr, authoredWidth int32) *pb.Expr {
	t.Helper()
	merge := requireDecimalOverload(t, head, function.COALESCE)
	require.Len(t, merge.Args, 2)
	requireDecimalOverload(t, merge.Args[0], function.ROUND)
	requireDecimalTyp(t, merge.Args[0].Typ, 47, 0,
		"the ROUND arm publishes the widened carrier's (47,0) and needs no alignment")
	alignment := requireDecimalOverload(t, merge.Args[1], function.CAST)
	require.False(t, alignment.SyntaxExplicitCast,
		"the outer cast on the default arm is the binder's implicit alignment layer")
	requireDecimalTyp(t, merge.Args[1].Typ, 47, 0)
	authored := requireDecimalOverload(t, alignment.Args[0], function.CAST)
	_, authoredOverload := function.DecodeOverloadID(authored.Func.GetObj())
	require.True(t, authored.SyntaxExplicitCast || authoredOverload != 0,
		"the inner cast on the default arm is the SQL-authored fixed-domain cast "+
			"(syntax-explicit in the direct route; the prepared route binds it with "+
			"the flag cleared under a non-zero overload - both forms are the "+
			"production key's authored-cast barrier)")
	requireAuthoredCastTyp(t, alignment.Args[0].Typ, authoredWidth)
	return head
}

// requireAuthoredCastTyp pins one SQL-authored fixed-domain cast's own
// declared type: a fixed-domain cast keeps its declared domain in the narrow
// carrier the declaration maps to (DECIMAL(34,0)/(37,0) declare DECIMAL128),
// never the envelope's DECIMAL256 alignment layer above it (revision 40's
// fixed-domain barrier; design section 5's "SQL-authored casts keep their
// declared typed envelope").
func requireAuthoredCastTyp(t *testing.T, typ pb.Type, width int32, msgAndArgs ...any) {
	t.Helper()
	message := append([]any{typ.String()}, msgAndArgs...)
	require.Equal(t, int32(types.T_decimal128), typ.Id, message...)
	require.Equal(t, width, typ.Width, message...)
	require.Equal(t, int32(0), typ.Scale, message...)
}

// requireReviewRound17LedgerEntry asserts the claimed generation ledger
// holds one witness merge's decision under its own content key with the
// authored cast's declared domain as the consumed pre type.
func requireReviewRound17LedgerEntry(t *testing.T, retained map[string]any, merge *pb.Expr, authoredWidth int32) {
	t.Helper()
	payload, found := retained[rule.DecimalBoundaryDecisionLedgerKey(merge)]
	require.True(t, found, "each witness merge's decision must survive under its own key")
	entry, ok := payload.(*rule.DecimalBoundaryDecisionEntry)
	require.True(t, ok)
	require.Equal(t, 2, entry.ArgCount)
	require.True(t, entry.Provenance, "the merge decision carries widened-multiplication provenance")
	requireDecimalTyp(t, entry.PreTypes[0], 47, 0)
	requireAuthoredCastTyp(t, entry.PreTypes[1], authoredWidth,
		"the ledger records the authored cast's own declared type as the consumed pre type")
	requireDecimalTyp(t, entry.AlignedTypes[0], 47, 0)
	requireDecimalTyp(t, entry.AlignedTypes[1], 47, 0)
	requireDecimalTyp(t, entry.ResultType, 47, 0)
}

// requireReviewRound17Schemas pins the parent-identical persisted schemas of
// the two witness expressions in both column orders across a built CTAS
// record (decision log 52's content keying keeps both decisions live for the
// ownership checks).
func requireReviewRound17Schemas(t *testing.T, cols []*pb.ColDef, firstWidth, secondWidth int32) {
	t.Helper()
	// A built CTAS TableDef appends the hidden __mo_fake_pk_col after the
	// projected columns; views carry only the projection. Assert the two
	// witness columns and require any trailing column to be the fake pk.
	require.GreaterOrEqual(t, len(cols), 2)
	requireDecimalTyp(t, cols[0].Typ, firstWidth, 0,
		"the first column persists the parent's own tag, never the (47,0) alignment envelope")
	requireDecimalTyp(t, cols[1].Typ, secondWidth, 0,
		"the second column persists the parent's own tag, never the (47,0) alignment envelope")
	for _, extra := range cols[2:] {
		require.Equal(t, "__mo_fake_pk_col", extra.Name,
			"only the hidden fake primary key may trail the two witness columns")
	}
}

func TestDecimalAuthoredCastDecisionKeyCollisionReviewRound17Issue30938(t *testing.T) {
	t.Run("ctasWitnessCast34First", func(t *testing.T) {
		mock := buildReviewRound17Mock(t)
		built, err := runOneStmt(mock, t,
			"create table r17ctas1 as select "+reviewRound17Expr34+" as c1, "+
				reviewRound17Expr37+" as c2 from t30938r17")
		require.NoError(t, err)
		requireReviewRound17Schemas(t,
			built.GetDdl().GetCreateTable().GetTableDef().GetCols(), 36, 37)
	})
	t.Run("ctasWitnessCast37First", func(t *testing.T) {
		mock := buildReviewRound17Mock(t)
		built, err := runOneStmt(mock, t,
			"create table r17ctas2 as select "+reviewRound17Expr37+" as c1, "+
				reviewRound17Expr34+" as c2 from t30938r17")
		require.NoError(t, err)
		requireReviewRound17Schemas(t,
			built.GetDdl().GetCreateTable().GetTableDef().GetCols(), 37, 36)
	})
	// requireReviewRound17ViewReplay exercises the design's replay locus for
	// one persisted view definition (section 6 M1/M4): DescribeViewColumns
	// (the description entry) and RegenerateViewDefinition (the
	// regeneration/replay entry) rebind the stored SQL through the same
	// inferViewColumns record assembly and must re-derive the parent's own
	// tags byte-for-byte, and an executable read through the view presents
	// the rebound plan's own carrier metadata exactly as the inline form
	// does (section 6 M2 - no reconciliation mechanism exists at the view
	// seam, so a repair that narrowed the executable side would violate M2).
	requireReviewRound17ViewReplay := func(t *testing.T, mock *MockOptimizer,
		viewName, createSQL string, firstWidth, secondWidth int32) {
		t.Helper()
		definition := fmt.Sprintf(`{"Stmt":%q,"DefaultDatabase":"tpch"}`, createSQL)
		described, err := DescribeViewColumns(&mock.ctxt, definition)
		require.NoError(t, err)
		requireReviewRound17Schemas(t, described, firstWidth, secondWidth)
		regenerated, err := RegenerateViewDefinition(&mock.ctxt, definition)
		require.NoError(t, err)
		requireReviewRound17Schemas(t, regenerated.TableDef.Cols, firstWidth, secondWidth)
		// Executable leg: register the persisted record and read through the
		// view. The rebound merge keeps its widened (47,0) publication - the
		// carrier result-set metadata section 6 M2 assigns to reads.
		created, err := runOneStmt(mock, t, createSQL)
		require.NoError(t, err)
		registered := DeepCopyTableDef(created.GetDdl().GetCreateView().GetTableDef(), true)
		registered.TableType = catalog.SystemViewRel
		registered.ViewSql = &pb.ViewDef{View: definition}
		mock.ctxt.tables[viewName] = registered
		mock.ctxt.objects[viewName] = &ObjectRef{SchemaName: "tpch", ObjName: viewName}
		replayed, err := runOneStmt(mock, t, "select c1, c2 from "+viewName)
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, replayed)
		require.Len(t, head, 2)
		requireDecimalTyp(t, head[0].Typ, 47, 0,
			"a read through the view presents the rebound plan's carrier metadata (section 6 M2)")
		requireDecimalTyp(t, head[1].Typ, 47, 0,
			"a read through the view presents the rebound plan's carrier metadata (section 6 M2)")
	}
	t.Run("viewRecordAndReplayCast34First", func(t *testing.T) {
		mock := buildReviewRound17Mock(t)
		createSQL := "create view r17v1 as select " + reviewRound17Expr34 + " as c1, " +
			reviewRound17Expr37 + " as c2 from t30938r17"
		created, err := runOneStmt(mock, t, createSQL)
		require.NoError(t, err)
		requireReviewRound17Schemas(t,
			created.GetDdl().GetCreateView().GetTableDef().GetCols(), 36, 37)
		requireReviewRound17ViewReplay(t, mock, "r17v1", createSQL, 36, 37)
	})
	t.Run("viewRecordAndReplayCast37First", func(t *testing.T) {
		mock := buildReviewRound17Mock(t)
		createSQL := "create view r17v2 as select " + reviewRound17Expr37 + " as c1, " +
			reviewRound17Expr34 + " as c2 from t30938r17"
		created, err := runOneStmt(mock, t, createSQL)
		require.NoError(t, err)
		requireReviewRound17Schemas(t,
			created.GetDdl().GetCreateView().GetTableDef().GetCols(), 37, 36)
		requireReviewRound17ViewReplay(t, mock, "r17v2", createSQL, 37, 36)
	})
	t.Run("preparedWitnessCast34First", func(t *testing.T) {
		original, _, retained := prepareDecimalBoundaryFixtureFor(t,
			map[string]string{"t30938r17": reviewRound17Table},
			"create table r17pctas1 as select "+reviewRound17Expr34+" as c1, "+
				reviewRound17Expr37+" as c2 from t30938r17")
		requireReviewRound17Schemas(t,
			original.GetDdl().GetCreateTable().GetTableDef().GetCols(), 36, 37)
		head := ddlPlanHeadExprsN(t, original, 2)
		merge34 := requireReviewRound17Merge(t, head[0], 34)
		merge37 := requireReviewRound17Merge(t, head[1], 37)
		require.NotEqual(t, rule.DecimalBoundaryDecisionLedgerKey(merge34),
			rule.DecimalBoundaryDecisionLedgerKey(merge37),
			"the authored fixed-domain casts keep the twin decisions' content keys distinct")
		requireReviewRound17LedgerEntry(t, retained, merge34, 34)
		requireReviewRound17LedgerEntry(t, retained, merge37, 37)
	})
	t.Run("preparedWitnessCast37First", func(t *testing.T) {
		original, _, retained := prepareDecimalBoundaryFixtureFor(t,
			map[string]string{"t30938r17": reviewRound17Table},
			"create table r17pctas2 as select "+reviewRound17Expr37+" as c1, "+
				reviewRound17Expr34+" as c2 from t30938r17")
		requireReviewRound17Schemas(t,
			original.GetDdl().GetCreateTable().GetTableDef().GetCols(), 37, 36)
		head := ddlPlanHeadExprsN(t, original, 2)
		merge37 := requireReviewRound17Merge(t, head[0], 37)
		merge34 := requireReviewRound17Merge(t, head[1], 34)
		require.NotEqual(t, rule.DecimalBoundaryDecisionLedgerKey(merge37),
			rule.DecimalBoundaryDecisionLedgerKey(merge34),
			"the authored fixed-domain casts keep the twin decisions' content keys distinct")
		requireReviewRound17LedgerEntry(t, retained, merge37, 37)
		requireReviewRound17LedgerEntry(t, retained, merge34, 34)
	})
	t.Run("singleExpressionControls", func(t *testing.T) {
		// Each expression alone records the only entry under its key, so the
		// ownership check passes on the pre- and post-repair engine alike:
		// the collision, not the single-expression recovery, is the defect.
		mock := buildReviewRound17Mock(t)
		only34, err := runOneStmt(mock, t,
			"create table r17ctl1 as select "+reviewRound17Expr34+" as c1 from t30938r17")
		require.NoError(t, err)
		requireDecimalTyp(t,
			only34.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].Typ, 36, 0)
		only37, err := runOneStmt(mock, t,
			"create table r17ctl2 as select "+reviewRound17Expr37+" as c1 from t30938r17")
		require.NoError(t, err)
		requireDecimalTyp(t,
			only37.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].Typ, 37, 0)
	})
}

func TestDecimalSharedProducerScalarExportShadowChargeReviewRound17Issue30938(t *testing.T) {
	// The estimator figures are the round-16 owners': the mock's default
	// statistics give the filtered producer 50 estimated rows at row size
	// 100, so the materialization estimate is 50 * max(100, declared) = 5000
	// with or without the allowance; the discriminating reservation
	// component is the spill framing — one nullable column frames 118 bytes
	// per record on top of the 69-byte record fixed cost, so the
	// shadow-inclusive reservation is 5000 + 50 * (69 + 2 * 118) = 20250
	// against the uncharged 5000 + 50 * (69 + 118) = 14350 (estimator
	// owners in cte_reuse.go).
	const (
		commitMem       = float64(5000)
		chargedSpill    = float64(20250)
		unchargedSpill  = float64(14350)
		spillBetween    = int64(17000) // above 14350, below 20250
		bigBudget       = int64(1) << 30
		bigSpillBudget  = int64(1) << 40
		scalarExportSQL = "with c as (select coalesce(d.v, d.v) as v from " +
			"(select a*b as v from m30938r where a > 5) d) " +
			"select sum(v) from c union all select sum(v) from c"
		scalarControlSQL = "with c as (select coalesce(d.v, d.v) as v from " +
			"(select a as v from m30938r where a > 5) d) " +
			"select sum(v) from c union all select sum(v) from c"
	)

	t.Run("scalarExportReservationIncludesEventualShadow", func(t *testing.T) {
		// The review witness shape: the exported COALESCE carries no
		// syntactic multiplication, but its column operands resolve through
		// the producer lineage to the widened product, so the admission
		// charges one shadow allowance before the share commits.
		builder, rootID := reviewRound16ReuseDrive(t, scalarExportSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types)+1,
			"the scalar export's column operands carry the product's provenance through the lineage")
		builder.reuseMultiReferenceCTEs(rootID)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, chargedSpill, builder.sharedMaterializationSpillBytes,
			"the reservation charges the shadow column finalization appends")
	})
	t.Run("scalarExportNearCapBudgetDeclinesAndLeavesCountersUnchanged", func(t *testing.T) {
		// Between the uncharged 14350 and the charged 20250 spill figures
		// the shadow allowance alone decides admission: the expanded
		// reservation exceeds the cap, the share declines to the historical
		// inline plan, and every cumulative counter stays byte-identical
		// (approved design decision log 50's declined-reservation oracle).
		builder, rootID := reviewRound16ReuseDrive(t, scalarExportSQL, bigBudget, spillBetween)
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
	t.Run("scalarExportNoProvenanceControl", func(t *testing.T) {
		// The identical scalar shape over a plain column export carries no
		// multiplication lineage anywhere: the operand resolution charges
		// nothing, and the share commits uncharged on both engines.
		builder, rootID := reviewRound16ReuseDrive(t, scalarControlSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types),
			"a scalar export without multiplication lineage charges no allowance")
		builder.reuseMultiReferenceCTEs(rootID)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, unchargedSpill, builder.sharedMaterializationSpillBytes)
	})
	t.Run("scalarExportFinalizationAppendsTheChargedShadow", func(t *testing.T) {
		// The eventual shadow the reservation must cover: finalization
		// (build_ddl.go ensureShadowColumn, reached from the two admitting
		// SUM readers) resolves the COALESCE's operands, admits the
		// divergent product seam inside the replay-safe expression, and
		// appends one Decimal256 shadow column to the producer's SINK and
		// to every consumer SINK_SCAN. This pinning passes on the pre- and
		// post-repair engine alike — finalization is unchanged; the repair
		// charges what it appends, so the charged schema (one allowance
		// above) covers every emitted shadow (one column here), exactly
		// decision log 50's cover invariant.
		built, err := buildReviewRound16Statement(t, scalarExportSQL)
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
