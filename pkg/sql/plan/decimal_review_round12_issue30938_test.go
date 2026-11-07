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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// This file pins the three planner regressions of issue #30938 review round
// 12 (pre-pr-review-1793644897654.json, all P2) against approved design
// revision 41 (sha256 ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780):
//
//	F1: widened-multiplication provenance is decided by the actual
//	    publication versus its parent publication under the authoritative
//	    operand-domain rules (decimalMultiplyDomain with the legacy
//	    DECIMAL64 18->19 and released DECIMAL128 38->39 coefficient
//	    adjustments, integer integral widths, BIT 20 and BOOL 1), never by
//	    the raw declared widths of two decimal operands. DECIMAL(38,0) x
//	    DECIMAL(27,0) diverges through the 38->39 adjustment (domains
//	    39+27=66 publishing (66,0) against the parent's (65,0)), and a
//	    divergent decimal-times-integer pair such as DECIMAL(47,0) x BIGINT
//	    (domains 47+19=66) resolves through the same channel, so the
//	    derived-table merge below binds the parent's DECIMAL(76,11) exactly
//	    as the pre-change engine does instead of failing the statement.
//	F2: after a retried set-operation common, each branch aligns its LIVE
//	    export type against the retried target exactly as the base engine
//	    aligns its live exports; the reconstructed parent domains choose
//	    the common type and never replace the live comparison, so a branch
//	    whose parent domain equals the common but whose live export widened
//	    still carries its alignment cast (and an out-of-domain value is
//	    rejected there), in either branch order.
//	F3: AVG/SUM admission demand propagates through dependent-program
//	    result edges: a scalar dependent call inside an admitted argument
//	    resolves its callee root's producer slot through the authoritative
//	    channel and enforces each divergent seam below the callee's
//	    row-changing operators, so a depth-two negative-outlier MAX/MIN
//	    cannot discard a row the parent rejected at computation. Bare
//	    dependent selectors keep the round-11 boundary (no admission
//	    without an AVG/SUM reader), and correlation type equalities hold
//	    through the projection refresh.
//
// Every expected tag below is derived from the shared typing owners
// (decimalMultiplyReturnType/decimalMultiplyDomain, the coalesce merge
// rule and AvgReturnType/SumReturnType), never from observed candidate
// behavior.

// buildReviewRound12Plan registers the round-11 fixtures plus the round-12
// provenance/set-operation tables and runs one statement through the public
// planner.
func buildReviewRound12Plan(t *testing.T, sql string) *pb.Plan {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerReviewRound11Tables(t, mock)
	registerReviewRound12Tables(t, mock)
	built, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	return built
}

func registerReviewRound12Tables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	for _, ddl := range []string{
		"create table m30938coef(a decimal(38,0), c decimal(27,0))",
		"create table m30938int(a decimal(47,0), bi bigint)",
		"create table m30938pair(a decimal(18,0), c decimal(47,0), bi bigint)",
		"create table m30938set(a decimal(38,0), b decimal(38,1), c decimal(38,0))",
	} {
		registerModBoundaryTable(t, mock, ddl)
	}
}

// findReviewRound12SetOp returns the first node of the given set-operation
// type in the query.
func findReviewRound12SetOp(query *pb.Query, nodeType pb.Node_NodeType) *pb.Node {
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == nodeType {
			return node
		}
	}
	return nil
}

// requireSetBranchesAligned pins every branch export of one set-operation
// node as an engine-owned cast to the retried common target.
func requireSetBranchesAligned(t *testing.T, query *pb.Query, setNode *pb.Node, width, scale int32, msgAndArgs ...any) {
	t.Helper()
	require.NotEmpty(t, setNode.Children, msgAndArgs...)
	for _, childID := range setNode.Children {
		child := query.Nodes[childID]
		require.NotNil(t, child, msgAndArgs...)
		require.NotEmpty(t, child.ProjectList, msgAndArgs...)
		requireEngineCastTo(t, child.ProjectList[0], width, scale, msgAndArgs...)
	}
}

// TestDecimalMultiplyCoefficientProvenanceReviewRound12Issue30938 is F1:
// provenance follows the authoritative operand domains, so a
// coefficient-adjusted pair (38,0)x(27,0) and a decimal-times-integer pair
// (47,0)xBIGINT resolve their parent domains through the derived-column
// channel and the outer value merge binds the parent's DECIMAL(76,11)
// instead of failing on the widened (66,0) publication. The nearest
// boundary controls - (18,0)x(47,0) at exactly 65 and the narrow
// (18,0)xBIGINT route - publish identically on both engines and keep their
// uncast publications, and the CTAS record of the divergent pair persists
// the parent's (65,0) tag.
func TestDecimalMultiplyCoefficientProvenanceReviewRound12Issue30938(t *testing.T) {
	for _, cell := range []struct {
		name string
		from string
		expr string
	}{
		{"coefficient adjusted pair", "m30938coef", "a*c"},
		{"decimal times integer pair", "m30938int", "a*bi"},
	} {
		t.Run(cell.name+" derived merge binds the parent common", func(t *testing.T) {
			built := buildReviewRound12Plan(t,
				"select coalesce(v, cast(0 as decimal(20,11))) as m from (select "+
					cell.expr+" as v from "+cell.from+") q")
			merged := findReviewRound11Call(built.GetQuery(), "coalesce")
			require.NotNil(t, merged, "the statement carries one COALESCE call")
			requireDecimalTyp(t, merged.Typ, 76, 11,
				"F1: the retried merge binds the parent's DECIMAL(76,11) common")
			product := findReviewRound11Call(built.GetQuery(), "*")
			require.NotNil(t, product, "the statement carries one multiplication")
			requireDecimalTyp(t, product.Typ, 66, 0,
				"F1: the divergent pair publishes the widened (66,0)")
		})
	}
	t.Run("boundary pair publishes identically on both engines", func(t *testing.T) {
		// DECIMAL(18,0) x DECIMAL(47,0): the adjusted domains sum to
		// exactly 65, so both engines publish (65,0) and the merge binds
		// without any retry.
		built := buildReviewRound12Plan(t,
			"select coalesce(v, cast(0 as decimal(20,11))) as m from (select a*c as v from m30938pair) q")
		merged := findReviewRound11Call(built.GetQuery(), "coalesce")
		require.NotNil(t, merged, "the control carries one COALESCE call")
		requireDecimalTyp(t, merged.Typ, 76, 11,
			"F1 control: the non-divergent pair binds the same common without a retry")
		product := findReviewRound11Call(built.GetQuery(), "*")
		require.NotNil(t, product, "the control carries one multiplication")
		requireDecimalTyp(t, product.Typ, 65, 0,
			"F1 control: (18,0)x(47,0) publishes (65,0) on both engines")
	})
	t.Run("narrow decimal times integer route publishes identically", func(t *testing.T) {
		// DECIMAL(18,0) x BIGINT: domains 18+19=37 stay on the narrow
		// checked-DECIMAL128 route; both engines publish (37,0).
		built := buildReviewRound12Plan(t,
			"select a*bi as v from m30938pair")
		product := findReviewRound11Call(built.GetQuery(), "*")
		require.NotNil(t, product, "the control carries one multiplication")
		requireModBoundaryTyp(t, product.Typ, types.T_decimal128, 37, 0,
			"F1 control: the narrow decimal-integer product keeps its released publication")
	})
	t.Run("ctas persists the parent tag of the divergent pair", func(t *testing.T) {
		ctas := buildReviewRound12Plan(t,
			"create table r12f1 as select a*c as v from m30938coef")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal256, 65, 0,
			"F1: the CTAS record persists the parent's (65,0) tag of the divergent pair")
	})
	t.Run("derived select publishes the widened product", func(t *testing.T) {
		built := buildReviewRound12Plan(t,
			"select a*c as v from m30938coef")
		product := findReviewRound11Call(built.GetQuery(), "*")
		require.NotNil(t, product, "the statement carries one multiplication")
		requireDecimalTyp(t, product.Typ, 66, 0,
			"F1: provenance repair never narrows the widened publication")
	})
}

// TestDecimalSetOperationRetryAlignmentReviewRound12Issue30938 is F2: the
// retried set-operation common is chosen from the reconstructed parent
// domains (COALESCE((65,1),(66,1)) -> (66,1) over the branch-0 parents and
// (65,0) for branch a*c, common (66,1)), and every branch aligns its LIVE
// export against that target: the branch-0 COALESCE publishes (76,1) live
// and must carry the alignment cast even though its parent domain equals
// the common. Both branch orders pin the same oracle; the no-retry control
// keeps its uncast branch exports.
func TestDecimalSetOperationRetryAlignmentReviewRound12Issue30938(t *testing.T) {
	lit := strings.Repeat("9", 65) + ".9" // DECIMAL(66,1) literal
	mergeBranch := "select coalesce(a*b, " + lit + ") as v from m30938set"
	productBranch := "select a*c as v from m30938set"
	for _, cell := range []struct {
		name string
		sql  string
	}{
		{"merge branch first", mergeBranch + " union all " + productBranch},
		{"product branch first", productBranch + " union all " + mergeBranch},
	} {
		t.Run(cell.name, func(t *testing.T) {
			built := buildReviewRound12Plan(t, cell.sql)
			query := built.GetQuery()
			setNode := findReviewRound12SetOp(query, pb.Node_UNION_ALL)
			require.NotNil(t, setNode, "the statement carries one UNION ALL node")
			requireSetBranchesAligned(t, query, setNode, 66, 1,
				"F2: every branch aligns its live export to the retried (66,1) common")
			require.NotEmpty(t, query.Steps, "the query carries a root step")
			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.NotNil(t, root, "the query root resolves")
			require.NotEmpty(t, root.ProjectList, "the query root projects")
			requireDecimalTyp(t, root.ProjectList[0].Typ, 66, 1,
				"F2: the union output publishes the retried (66,1) common in either branch order")
		})
	}
	t.Run("no retry control keeps uncast branches", func(t *testing.T) {
		built := buildReviewRound12Plan(t, productBranch+" union all "+productBranch)
		query := built.GetQuery()
		setNode := findReviewRound12SetOp(query, pb.Node_UNION_ALL)
		require.NotNil(t, setNode, "the control carries one UNION ALL node")
		require.Len(t, setNode.Children, 2, "the control carries two branches")
		for _, childID := range setNode.Children {
			child := query.Nodes[childID]
			head := child.ProjectList[0].GetF()
			require.NotNil(t, head, "F2 control: a non-retried union keeps the raw branch export")
			require.Equal(t, "*", head.Func.GetObjName(),
				"F2 control: a non-retried union keeps the raw branch export")
		}
	})
}

// TestDecimalDependentResultAdmissionReviewRound12Issue30938 is F3: an
// admitted AVG/SUM argument's scalar dependent call resolves its callee
// root's producer slot through the authoritative channel and admits each
// divergent seam below the callee's row-changing operators, so the
// depth-two MAX/MIN cannot discard a product row the parent rejected at
// computation. The bare dependent selector keeps the round-11 boundary (no
// admission without an AVG/SUM reader), the narrow control stays uncast,
// and the finalized plans keep the correlation type equalities.
func TestDecimalDependentResultAdmissionReviewRound12Issue30938(t *testing.T) {
	depthTwo := func(selector string) string {
		return "(select (select " + selector + "(a*b) from m30938w where m30938w.d < m30938i.i) " +
			"from m30938w as w2 limit 1)"
	}
	for _, selector := range []string{"max", "min"} {
		t.Run("sum over depth-two "+selector, func(t *testing.T) {
			built := buildReviewRound12Plan(t,
				"select sum("+depthTwo(selector)+") from m30938i")
			query := built.GetQuery()
			require.NoError(t, query.ValidateCorrelation(),
				"the finalized plan keeps the correlation type equalities")
			call := findReviewRound11Agg(query, selector)
			require.NotNil(t, call, "the statement carries one %s call", selector)
			product := requireEngineCastTo(t, call.GetF().Args[0], 65, 0,
				"F3: the dependent callee's selector admits its product seam below the operator")
			require.Equal(t, "*", product.GetF().Func.GetObjName(),
				"F3: the admission cast wraps the product itself")
			sum := findReviewRound11Agg(query, "sum")
			require.NotNil(t, sum, "the statement carries one SUM call")
			requireDecimalTyp(t, sum.Typ, 65, 0,
				"F3: the outer SUM rebinds over the admitted parent domain")
			argument := requireEngineCastTo(t, sum.GetF().Args[0], 65, 0,
				"F3: the dependent argument admits against the parent product domain")
			require.NotNil(t, argument.GetDependent(),
				"F3: the admitted expression is the scalar dependent call itself")
		})
	}
	t.Run("bare dependent selector admits nothing", func(t *testing.T) {
		built := buildReviewRound12Plan(t,
			"select "+depthTwo("max")+" as v from m30938i")
		query := built.GetQuery()
		require.NoError(t, query.ValidateCorrelation(),
			"the finalized plan keeps the correlation type equalities")
		call := findReviewRound11Agg(query, "max")
		require.NotNil(t, call, "the statement carries one MAX call")
		head := call.GetF().Args[0].GetF()
		require.NotNil(t, head,
			"F3 boundary: the bare dependent selector's argument stays the published product")
		require.Equal(t, "*", head.Func.GetObjName(),
			"F3 boundary: without an AVG/SUM reader the selector interior admits nothing")
	})
	t.Run("narrow dependent control stays uncast", func(t *testing.T) {
		built := buildReviewRound12Plan(t,
			"select sum((select (select max(x.c*x.mi) from m30938i as x where x.i > m30938i.i) "+
				"from m30938i as y limit 1)) from m30938i")
		query := built.GetQuery()
		require.NoError(t, query.ValidateCorrelation(),
			"the finalized plan keeps the correlation type equalities")
		call := findReviewRound11Agg(query, "max")
		require.NotNil(t, call, "the control carries one MAX call")
		head := call.GetF().Args[0].GetF()
		require.NotNil(t, head,
			"F3 control: a product both engines publish identically stays uncast")
		require.Equal(t, "*", head.Func.GetObjName(),
			"F3 control: a product both engines publish identically stays uncast")
	})
}
