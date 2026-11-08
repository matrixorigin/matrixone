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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// This file pins the two planner regressions of issue #30938 review round 18
// (both P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780).
//
// F1 (join input ordinals ignored when resolving producer domains):
// resolveRelationalOutput (build_ddl.go) iterated the owning node's children
// first-resolved-wins, ignoring ColRef.RelPos. When the OWNER is a JOIN (the
// execute-time rebind anchors its lineage owner to the node whose OnList
// expressions it is about to rebind - ResetParamRefRule.MatchNode, and the
// walker applies the rule to OnList expressions), a post-remap OnList column
// reference addresses the join's inputs directly: RelPos selects the side
// (0 left, 1 right, matching Children order, query_builder's join remap) and
// ColPos the projection slot - exactly the indexing the projection-refresh
// owner's resolveColumnType closure implements for JOIN/APPLY (design
// decision log 69 (iv): "JOIN/APPLY and the generic fallback ->
// childProjectLists[relPos][colPos]"). The generic loop instead resolved a
// right-side reference through the LEFT child's projection, so a prepared
// CTAS whose JOIN ON carries COALESCE(right.v, ?) over unequal left/right
// product domains recovered the LEFT product's parent domain for right.v.
// Over the witness below (LIMIT preserves the derived projections under
// opt_misc.go's canRemoveProject guard): the left product publishes (76,30)
// (parent (65,30)), the right product publishes (76,0) (parent (65,0)), and
// the binding is DECIMAL(20,1). The parent's own common of [(65,0),(20,1)]
// is (66,1), which admits the right product's 10^60 value; the wrong-side
// resolution recovered [(65,30),(20,1)] -> (65,30), whose 35 integer digits
// reject 10^60 with the parent's checked out-of-range cast family at
// evaluation - a regression against the parent engine, which binds (66,1)
// natively. The mirror order (merge over the LEFT input) resolved correctly
// by accident because the generic loop visits the left child first; it is
// pinned as the RelPos-0 control. Existing materialized retry tests use
// identical domains on both join inputs and cannot distinguish this error.
//
// F2 (intermediate scalar producers gated out of the shadow charge): the
// round-17 repair gated EVERY scalar producing expression the recursive
// charge walk reaches on decimalShadowReplaySafe. The finalization gates
// only the OUTER resolved producing expression (build_ddl.go's
// ensureShadowColumn), then its admission follows intermediate scalar
// producers gate-free (admit/admitProducerSlot resolve the scalar's column
// operands through the producer lineage without re-testing replay safety).
// Over the witness below - export COALESCE(e.v,e.v) over a retained
// intermediate projection ABS(d.v) (NOT in the replay-safe fragment) over
// the widened product - the charge walk reached ABS over a column
// reference, found neither syntactic multiplication nor replay-safe
// membership, and charged zero allowances, while the finalization admitted
// the divergent product seam through the replay-safe outer COALESCE and
// appended one Decimal256 shadow to the producer's SINK and every SINK_SCAN
// - the emitted schema again exceeded the charged schema, the exact gap
// approved design decision log 50 (F-SHADOW-ADMISSION-ACCOUNTING) closes.
// Replica eligibility is now separated from provenance reachability: the
// walk gates the FIRST scalar of the export's own mirror chain (the
// expression the finalization gates) and follows every recursively reached
// intermediate gate-free, retaining the visited-set cycle bound. Every
// expected tag and byte count below derives from the registered rules and
// the estimator owners, never from observed candidate behavior.

const (
	reviewRound18ScaledTable = "create table t30938r18s(a decimal(38,30), b decimal(38,30))"
	reviewRound18PlainTable  = "create table t30938r18p(c decimal(38,0), d decimal(38,0))"

	// Order A (the review witness): the merge sits over the RIGHT join
	// input; the right product publishes (76,0) (parent (65,0)) while the
	// left product publishes (76,30) (parent (65,30)).
	reviewRound18JoinRightSQL = "create table r18out1 as select x.v as lv, y.v as rv from " +
		"(select a*b as v from t30938r18s limit 100) x join " +
		"(select c*d as v from t30938r18p limit 100) y on coalesce(y.v, ?) = x.v"
	// Order B (mirror control): the merge sits over the LEFT join input;
	// the generic loop's left-first visit resolves it correctly on every
	// engine, pinning that the ordinal repair keeps RelPos 0 intact.
	reviewRound18JoinLeftSQL = "create table r18out2 as select x.v as lv, y.v as rv from " +
		"(select c*d as v from t30938r18p limit 100) x join " +
		"(select a*b as v from t30938r18s limit 100) y on coalesce(x.v, ?) = y.v"

	// The round-18 F2 witness: the replay-safe outer export COALESCE(e.v,e.v)
	// reaches the widened product through the retained intermediate
	// projection ABS(d.v), which is not in the replay-safe fragment.
	reviewRound18AbsExportSQL = "with c as (select coalesce(e.v, e.v) as v from " +
		"(select abs(d.v) as v from (select a*b as v from m30938r where a > 5) d limit 100) e) " +
		"select sum(v) from c union all select sum(v) from c"
)

// reviewRound18JoinTables registers the unequal-domain join fixtures: the
// scaled operands publish (76,30) (parent (65,30)) and the plain operands
// publish (76,0) (parent (65,0)).
func reviewRound18JoinTables() map[string]string {
	return map[string]string{
		"t30938r18s": reviewRound18ScaledTable,
		"t30938r18p": reviewRound18PlainTable,
	}
}

// requireReviewRound18JoinOnCoalesce locates the filled plan's JOIN node and
// returns the coalesce merge carried in its ON list. The equality's
// comparison alignment may wrap the merge in a cast, so the search descends
// through function arguments.
func requireReviewRound18JoinOnCoalesce(t *testing.T, filled *pb.Plan) *pb.Expr {
	t.Helper()
	return requireReviewRound18QueryOnCoalesce(t, filled.GetDdl().GetQuery())
}

// requireReviewRound18QueryOnCoalesce is the Plan_Query twin of
// requireReviewRound18JoinOnCoalesce: the internal executor's CTAS follow-up
// reparse is an INSERT ... SELECT, whose prepared plan is a Plan_Query
// carrying the merge in its JOIN ON list (design revision 42 entry 88 (ii)).
func requireReviewRound18QueryOnCoalesce(t *testing.T, query *pb.Query) *pb.Expr {
	t.Helper()
	require.NotNil(t, query)
	var find func(expr *pb.Expr) *pb.Expr
	find = func(expr *pb.Expr) *pb.Expr {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		if id, _ := function.DecodeOverloadID(fn.Func.GetObj()); id == function.COALESCE {
			return expr
		}
		for _, arg := range fn.Args {
			if found := find(arg); found != nil {
				return found
			}
		}
		return nil
	}
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != pb.Node_JOIN || len(node.OnList) == 0 {
			continue
		}
		for _, onExpr := range node.OnList {
			if found := find(onExpr); found != nil {
				return found
			}
		}
	}
	t.Fatalf("the filled plan carries no JOIN ON coalesce merge")
	return nil
}

func TestDecimalJoinOnMergeInputOrdinalReviewRound18Issue30938(t *testing.T) {
	binding := decimal128Binding("0.5", 20, 1)

	t.Run("rightSideOrdinalWitness", func(t *testing.T) {
		// The review witness: COALESCE(y.v, ?) sits in the JOIN ON list with
		// y.v the RIGHT input's product. The surface family common of
		// [(76,0),(20,1)] needs 77 digits, so the execute-time rebind retries
		// over the parent-published domains; honoring the reference's RelPos
		// recovers the right product's parent (65,0) and the parent's own
		// common of [(65,0),(20,1)] is (66,1) - which admits the right
		// product's 10^60 value at evaluation. Resolving through the left
		// child instead recovers (65,30), whose 35 integer digits reject
		// 10^60 - the candidate regression the parent engine never commits.
		original, stmt, retained := prepareDecimalBoundaryFixtureFor(t,
			reviewRound18JoinTables(), reviewRound18JoinRightSQL)
		cols := original.GetDdl().GetCreateTable().TableDef.Cols
		requireDecimalTyp(t, cols[0].Typ, 65, 30,
			"the left product's snapshot tag is its parent envelope on both engines")
		requireDecimalTyp(t, cols[1].Typ, 65, 0,
			"the right product's snapshot tag is its parent envelope on both engines")

		filled, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
		merge := requireReviewRound18JoinOnCoalesce(t, filled)
		requireDecimalTyp(t, merge.Typ, 66, 1,
			"the ON merge binds the parent's common of [(65,0),(20,1)] when RelPos selects the right input")
		mergeFn := merge.GetF()
		require.Len(t, mergeFn.Args, 2)
		requireDecimalTyp(t, mergeFn.Args[0].Typ, 66, 1,
			"the product arm aligns into the (66,1) admission domain, which admits 10^60")
		requireDecimalTyp(t, mergeFn.Args[1].Typ, 66, 1,
			"the binding arm aligns into the (66,1) admission domain")
		requireDecimalTyp(t, tag, 65, 30,
			"the ON merge's common does not disturb the first column's refreshed envelope")
		requireDecimalTyp(t, filled.GetDdl().GetCreateTable().TableDef.Cols[1].Typ, 65, 0,
			"the ON merge's common does not disturb the second column's refreshed envelope")
	})

	t.Run("leftSideOrdinalMirrorControl", func(t *testing.T) {
		// The mirror input order: COALESCE(x.v, ?) over the LEFT input's
		// product resolves through the left child on the pre- and post-repair
		// engine alike (the generic loop visits the left child first), so the
		// parent's (66,1) common holds on both - the ordinal repair must keep
		// RelPos 0 byte-identical.
		original, stmt, retained := prepareDecimalBoundaryFixtureFor(t,
			reviewRound18JoinTables(), reviewRound18JoinLeftSQL)
		cols := original.GetDdl().GetCreateTable().TableDef.Cols
		requireDecimalTyp(t, cols[0].Typ, 65, 0)
		requireDecimalTyp(t, cols[1].Typ, 65, 30)

		filled, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
		merge := requireReviewRound18JoinOnCoalesce(t, filled)
		requireDecimalTyp(t, merge.Typ, 66, 1,
			"the left-input merge keeps the parent's (66,1) common on every engine")
		mergeFn := merge.GetF()
		require.Len(t, mergeFn.Args, 2)
		requireDecimalTyp(t, mergeFn.Args[0].Typ, 66, 1,
			"the product arm aligns into the (66,1) admission domain on every engine")
		requireDecimalTyp(t, tag, 65, 0)
		requireDecimalTyp(t, filled.GetDdl().GetCreateTable().TableDef.Cols[1].Typ, 65, 30)
	})
}

func TestDecimalSharedProducerIntermediateScalarShadowChargeReviewRound18Issue30938(t *testing.T) {
	// The estimator figures are the round-17 owners' unchanged: the mock's
	// default statistics give the filtered producer 50 estimated rows at row
	// size 100 (the extra ABS projection preserves row estimates and the
	// LIMIT 100 stays above the 50-row estimate), so the materialization
	// estimate is 50 * max(100, declared) = 5000 with or without the
	// allowance; the discriminating reservation component is the spill
	// framing - one nullable column frames 118 bytes per record on top of
	// the 69-byte record fixed cost, so the shadow-inclusive reservation is
	// 5000 + 50 * (69 + 2 * 118) = 20250 against the uncharged
	// 5000 + 50 * (69 + 118) = 14350 (estimator owners in cte_reuse.go).
	const (
		commitMem      = float64(5000)
		chargedSpill   = float64(20250)
		unchargedSpill = float64(14350)
		spillBetween   = int64(17000) // above 14350, below 20250
		bigBudget      = int64(1) << 30
		bigSpillBudget = int64(1) << 40
	)

	t.Run("intermediateScalarReservationIncludesEventualShadow", func(t *testing.T) {
		// The review witness shape: the replay-safe outer export reaches the
		// widened product through the non-replay-safe intermediate ABS
		// projection. The finalization admits that seam (the shadow exists -
		// pinned below), so the reservation must charge one shadow allowance
		// before the share commits (design decision log 50's cover
		// invariant).
		builder, rootID := reviewRound16ReuseDrive(t, reviewRound18AbsExportSQL, bigBudget, bigSpillBudget)
		occ := builder.cteRefs[0].occurrences[0]
		expanded := builder.cteShadowExpandedStorageTypes(occ.rootID, occ.types, occ.types)
		require.Len(t, expanded, len(occ.types)+1,
			"the charge follows the intermediate scalar to the product exactly as finalization does")
		builder.reuseMultiReferenceCTEs(rootID)
		sinks, sinkScans := reviewRound16SinkCount(builder)
		require.Equal(t, 1, sinks)
		require.Equal(t, 2, sinkScans)
		require.Equal(t, commitMem, builder.sharedMaterializationMemoryBytes)
		require.Equal(t, chargedSpill, builder.sharedMaterializationSpillBytes,
			"the reservation charges the shadow column finalization appends")
	})

	t.Run("intermediateScalarNearCapBudgetDeclinesAndLeavesCountersUnchanged", func(t *testing.T) {
		// Between the uncharged 14350 and the charged 20250 spill figures
		// the shadow allowance alone decides admission: the expanded
		// reservation exceeds the cap, the share declines to the historical
		// inline plan, and every cumulative counter stays byte-identical
		// (approved design decision log 50's declined-reservation oracle).
		builder, rootID := reviewRound16ReuseDrive(t, reviewRound18AbsExportSQL, bigBudget, spillBetween)
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

	t.Run("intermediateScalarFinalizationAppendsTheChargedShadow", func(t *testing.T) {
		// The eventual shadow the reservation must cover: finalization
		// (build_ddl.go ensureShadowColumn, reached from the two admitting
		// SUM readers) gates only the outer replay-safe COALESCE, follows
		// the intermediate ABS producer gate-free, admits the divergent
		// product seam, and appends one Decimal256 shadow column to the
		// producer's SINK and to every consumer SINK_SCAN. This pinning
		// passes on the pre- and post-repair engine alike - finalization is
		// unchanged; the repair charges what it appends, so the charged
		// schema (one allowance above) covers every emitted shadow (one
		// column here), exactly decision log 50's cover invariant.
		built, err := buildReviewRound16Statement(t, reviewRound18AbsExportSQL)
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

// The internal-route harness below pins design revision 42 entry 88's
// approved repair (F-INTERNAL-FOLLOWUP-FILL-LINEAGE): a prepared CTAS runs a
// SECOND, independent execute-time fill when populateCreatedTable reparses
// the retained CreateAsSelectSql - an INSERT ... SELECT whose SELECT text is
// the formatted AsSource AST - through the internal executor. Before the
// repair that fill rode the no-process preserving variant, so the lineage
// gate (utils.go:6905) stayed closed and the review witness's ON merge
// declined with the planner's ERROR 20203 where the parent engine commits
// only its own checked evaluation rejection. The repair threads the
// executor's own per-execution process, an entered generation and the
// attached session's div_precision_increment through a new
// preserving-and-process wrapper, so the fourth fill window behaves exactly
// as the frontend routes while the INSERT's positional write roots keep
// their prepare-time destination-facing contract.
const (
	// The write target's declared columns mirror the snapshot tags the CTAS
	// frontend fill persists for the review witness ((65,30) and (65,0) on
	// both engines).
	reviewRound18WriteTable = "create table r18wout(lv decimal(65,30), rv decimal(65,0))"

	// The production follow-up shape (build_ddl.go's retained
	// CreateAsSelectSql is `insert into <target> select ... from (<source>)
	// as __mo_ctas_source`): the ON merge is the round-18 review witness,
	// and position 0 sits in the write-position projection so the leg also
	// exercises the preserving half of the wrapper's contract.
	reviewRound18InsertRightSQL = "insert into r18wout(lv, rv) select x.v, ? from " +
		"(select a*b as v from t30938r18s limit 100) x join " +
		"(select c*d as v from t30938r18p limit 100) y on coalesce(y.v, ?) = x.v"
)

func reviewRound18InsertTables() map[string]string {
	tables := reviewRound18JoinTables()
	tables["r18wout"] = reviewRound18WriteTable
	return tables
}

// prepareReviewRound18InsertFixture builds one prepared INSERT ... SELECT
// snapshot over the round-18 join fixtures and the r18wout write target.
// Unlike the CTAS harness no ledger claim is made: the internal route's fill
// consumes no cross-route ledger (design revision 42 entry 88 (iv)(e)) - the
// plan's own widened provenance opens the lineage gate.
func prepareReviewRound18InsertFixture(t *testing.T, innerSQL string) *pb.Plan {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for name, ddl := range reviewRound18InsertTables() {
		source, err := buildTestCreateTableStmt(mock, ddl)
		require.NoError(t, err)
		if name == "r18wout" {
			// The plan-side CREATE TABLE builder appends only the hidden
			// fake primary key column; the persisted catalog def the
			// writer resolves against additionally carries the hidden
			// rowid column every real table has. Mirror that def (the
			// convention of delete_runtime_filter_sql_test.go) or the
			// insert binder's dedup/multi-update path miscounts columns.
			if _, ok := source.Name2ColIndex[catalog.Row_ID]; !ok {
				pos := len(source.Cols)
				if source.Name2ColIndex == nil {
					source.Name2ColIndex = make(map[string]int32, pos+1)
					for i, col := range source.Cols {
						source.Name2ColIndex[col.Name] = int32(i)
					}
				}
				source.Name2ColIndex[catalog.Row_ID] = int32(pos)
				source.Cols = append(source.Cols, &pb.ColDef{
					ColId:      uint64(pos),
					Name:       catalog.Row_ID,
					OriginName: catalog.Row_ID,
					Typ:        pb.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true},
					Hidden:     true,
					Default:    &pb.Default{},
				})
			}
		}
		mock.ctxt.tables[name] = source
		mock.ctxt.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name}
	}
	prepared, err := runOneStmt(mock, t,
		"prepare boundary_fixture from '"+strings.ReplaceAll(innerSQL, "'", "''")+"'")
	require.NoError(t, err)
	return prepared.GetDcl().GetPrepare().Plan
}

// requireReviewRound18RowImage locates the INSERT writer - the modern
// dedup/multi-update path emits a MULTI_UPDATE node whose first update
// context carries the target table - and the final row-image projection
// feeding it, asserting the writer's target identity.
func requireReviewRound18RowImage(t *testing.T, query *pb.Query) (*pb.Node, *pb.Node) {
	t.Helper()
	require.NotNil(t, query)
	var insertNode *pb.Node
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == pb.Node_MULTI_UPDATE {
			insertNode = node
		}
	}
	require.NotNil(t, insertNode, "the prepared plan carries its INSERT writer")
	require.NotEmpty(t, insertNode.UpdateCtxList,
		"the writer's first update context describes the base target")
	require.Equal(t, "r18wout", insertNode.UpdateCtxList[0].TableDef.Name,
		"the writer's target is the registered fixture table")
	require.NotEmpty(t, insertNode.Children)
	rowImage := query.Nodes[insertNode.Children[0]]
	require.NotNil(t, rowImage)
	require.NotEmpty(t, rowImage.ProjectList,
		"the writer consumes the row-image projection positionally")
	return insertNode, rowImage
}

func TestDecimalInternalRoutePreservingWrapperReviewRound18Issue30938(t *testing.T) {
	binding := decimal128Binding("0.5", 20, 1)

	t.Run("insertRootBindsParentCommonAndPreservesWriteRoots", func(t *testing.T) {
		// Entry 88 (vi)(a): the new preserving-and-process wrapper over the
		// INSERT-root follow-up shape. The threaded process installs the
		// lineage view, so the ON merge's retry resolves the right input's
		// parent-published (65,0) through the F1 ordinal repair and
		// converges the parent's own common (66,1) - the type whose 46
		// integer digits admit the comparison alignment that raises the
		// parent's checked 20301 at evaluation. The same fill must leave
		// the writer's positional contract intact.
		original := prepareReviewRound18InsertFixture(t, reviewRound18InsertRightSQL)
		originalQuery := original.GetQuery()
		require.NotNil(t, originalQuery)
		preserve := preparedDMLWriteExpressions(originalQuery)
		require.NotEmpty(t, preserve,
			"the write-position parameter makes the row image a preserved write root")
		_, originalImage := requireReviewRound18RowImage(t, originalQuery)

		execProc := newPlanTestProcess(t)
		t.Cleanup(execProc.Free)
		release := process.EnterDecimalBoundaryGeneration(execProc)
		filled, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWritesAndProcess(
			context.Background(), execProc, original, []any{binding, binding},
			function.DefaultDivPrecisionIncrement)
		release()
		require.NoError(t, err,
			"the with-process wrapper heals the ON merge exactly as the frontend routes do")
		query := filled.GetQuery()
		require.NotNil(t, query)

		merge := requireReviewRound18QueryOnCoalesce(t, query)
		requireDecimalTyp(t, merge.Typ, 66, 1,
			"the internal-route fill binds the parent's common of [(65,0),(20,1)]")
		mergeFn := merge.GetF()
		require.Len(t, mergeFn.Args, 2)
		requireDecimalTyp(t, mergeFn.Args[0].Typ, 66, 1,
			"the product arm aligns into the (66,1) admission domain")
		requireDecimalTyp(t, mergeFn.Args[1].Typ, 66, 1,
			"the binding arm aligns into the (66,1) admission domain")

		// The preserved write roots: the writer stands with its target, the
		// positional row image is byte-identical (it carries only param-free
		// column references into the assignment projection below, whose
		// destination-facing casts are likewise param-free), and the
		// write-position marker itself survives byte-identical -
		// applyExprPreservingRoot returns a bare parameter root unchanged
		// (visit_plan_rule.go's Expr_P case) so the positional writer's
		// ParamExpressionExecutor supplies the value with the prepare-time
		// assignment domain.
		_, filledImage := requireReviewRound18RowImage(t, query)
		require.Equal(t, originalImage.ProjectList, filledImage.ProjectList,
			"the positional row image is byte-identical after the fill")
		require.NotEmpty(t, originalImage.Children)
		require.NotEmpty(t, filledImage.Children)
		originalAssign := originalQuery.Nodes[originalImage.Children[0]]
		filledAssign := query.Nodes[filledImage.Children[0]]
		require.NotNil(t, originalAssign)
		require.NotNil(t, filledAssign)
		require.Equal(t, originalAssign.ProjectList, filledAssign.ProjectList,
			"the destination-facing assignment casts are byte-identical after the fill")
		// The write-position parameter's projection root is the preserved
		// write root preparedDMLWriteExpressions records for this shape: it
		// is the only PROJECT member carrying a parameter (the ON-clause
		// marker lives in the join's OnList, and the fill consumes it).
		findWriteRoot := func(q *pb.Query) *pb.Expr {
			for _, node := range q.Nodes {
				if node == nil || node.NodeType != pb.Node_PROJECT {
					continue
				}
				for _, expr := range node.ProjectList {
					if preparedExprContainsParam(expr) {
						return expr
					}
				}
			}
			return nil
		}
		originalRoot := findWriteRoot(originalQuery)
		require.NotNil(t, originalRoot,
			"fixture sanity: the write position carries a parameter root")
		filledRoot := findWriteRoot(query)
		require.NotNil(t, filledRoot,
			"the write-position root stays parameterized for the positional writer")
		require.Equal(t, originalRoot, filledRoot,
			"the preserved write root is byte-identical to its prepare-time form")
	})

	t.Run("insertRootProcessLessAblationDeclines", func(t *testing.T) {
		// Entry 88 (vi)(b): the same harness through the pre-existing
		// no-process preserving variant keeps declining with the exact
		// checked fill error - the lineage gate stays closed without the
		// process, proving the gate load-bearing on this route. This is the
		// live candidate divergence captured pre-repair at execute r18p1
		// (design revision 42 entry 88 (ii)).
		original := prepareReviewRound18InsertFixture(t, reviewRound18InsertRightSQL)
		_, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
			context.Background(), original, []any{binding, binding})
		require.Error(t, err)
		require.Contains(t, err.Error(),
			"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]",
			"the lineage-withheld decline is the exact checked fill error")
	})

	t.Run("divisionWitnessIncrementThroughWrapper", func(t *testing.T) {
		// Entry 88 (iv)(d): the wrapper's divIncrement reaches the lineage
		// scope, so parent-domain recovery consumes the statement's own
		// div_precision_increment on this route exactly as on the frontend
		// channels (review round 13, F1). The witness is the round-13
		// division recovery shape driven through the new wrapper.
		tables := []string{"create table r13div(a decimal(38,0), b decimal(38,0), c decimal(38,0), d decimal(38,0))"}
		innerSQL := "create table r13out as select coalesce(a*b, (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div"

		t.Run("nondefault increment recovers the session quotient", func(t *testing.T) {
			original, _, _ := prepareReviewRound13Fixture(t, tables, 10, innerSQL)
			execProc := newPlanTestProcess(t)
			t.Cleanup(execProc.Free)
			release := process.EnterDecimalBoundaryGeneration(execProc)
			filled, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWritesAndProcess(
				context.Background(), execProc, original, []any{binding}, 10)
			release()
			require.NoError(t, err)
			requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 75, 10,
				"the wrapper threads the increment-10 recovery exactly as the frontend channel does")
		})

		t.Run("default increment control", func(t *testing.T) {
			original, _, _ := prepareReviewRound13Fixture(t, tables,
				int64(function.DefaultDivPrecisionIncrement), innerSQL)
			execProc := newPlanTestProcess(t)
			t.Cleanup(execProc.Free)
			release := process.EnterDecimalBoundaryGeneration(execProc)
			filled, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWritesAndProcess(
				context.Background(), execProc, original, []any{binding},
				function.DefaultDivPrecisionIncrement)
			release()
			require.NoError(t, err)
			requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 69, 4,
				"the default increment keeps the parent's (69,4) through the wrapper")
		})
	})
}
