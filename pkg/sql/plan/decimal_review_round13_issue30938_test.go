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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// This file pins the two planner regressions of issue #30938 review round
// 13 (both P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780):
//
//	F1: prepared domain recovery consumes the statement's session
//	    div_precision_increment. Execute-time lineage scopes are created
//	    without a QueryBuilder, so the division mirror
//	    (decimalBoundaryParentDivision) fell back to the engine default 4:
//	    at increment 10 the retried COALESCE common over a quotient arm
//	    narrowed to the parent's increment-4 domain DECIMAL(69,4) instead of
//	    the parent's own DECIMAL(75,10), and the alignment cast narrowed the
//	    stored quotient digits before any schema refresh could see them.
//	F2: the multiplication storage boundary applies the shared coefficient
//	    adjustments according to the recovered carrier. A width-reducing
//	    wrapper whose parent publication reaches width 38 on the decimal256
//	    carrier (TRUNCATE over a (38,27)x(38,27) product keeps Decimal256
//	    and yields Decimal256(38,0)) had its envelope width unconditionally
//	    adjusted 38->39 - the released DECIMAL128 coefficient rule - so the
//	    outer product persisted DECIMAL(40,0) where the parent persists
//	    DECIMAL(39,0).
//
// Every expected tag below is derived from the shared typing owners
// (decimalDivisionReturnType, decimalMultiplyDomain and the coalesce merge
// contract), never from observed candidate behavior.

// prepareReviewRound13Fixture builds one prepared-CTAS snapshot for innerSQL
// under one session div_precision_increment (the wrapper of
// div_precision_increment_test.go exposed through the round-11 optimizer
// shim) and claims the completed generation's decision ledger exactly as
// prepareDecimalBoundaryFixtureFor does at the default setting.
func prepareReviewRound13Fixture(t *testing.T, tables []string, increment int64, innerSQL string) (*pb.Plan, tree.Statement, map[string]any) {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, ddl := range tables {
		registerModBoundaryTable(t, mock, ddl)
	}
	stmt, err := mysql.ParseOne(context.Background(), innerSQL, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	compiler := divPrecisionCompilerContext{CompilerContext: &mock.ctxt, increment: increment}
	opt := reviewRound11Optimizer{ctx: compiler}
	prepared, err := runOneStmt(opt, t,
		"prepare boundary_fixture from '"+strings.ReplaceAll(innerSQL, "'", "''")+"'")
	require.NoError(t, err)
	retained, ok := mock.ctxt.GetProcess().ClaimDecimalBoundaryEvidence()
	require.True(t, ok, "a prepared-CTAS generation must publish its decision ledger")
	require.NotEmpty(t, retained)
	return prepared.GetDcl().GetPrepare().Plan, stmt, retained
}

// executeReviewRound13Binding drives one EXECUTE of a prepared fixture
// through the entry-68 execute generation exactly as
// executePreparedBoundaryBinding does, threading the execute session's
// div_precision_increment through the same channel the production routes
// use, and refreshes the persisted tag with that same increment.
func executeReviewRound13Binding(t *testing.T, increment int32, original *pb.Plan, stmt tree.Statement, retained map[string]any, values []any) (*pb.Plan, map[string]any, pb.Type) {
	t.Helper()
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	release := process.EnterDecimalBoundaryGeneration(execProc)
	filled, _, err := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, original, values, increment)
	merged := make(map[string]any, len(retained)+len(execProc.DecimalBoundaryDecisions()))
	for key, entry := range retained {
		merged[key] = entry
	}
	for key, entry := range execProc.DecimalBoundaryDecisions() {
		merged[key] = entry
	}
	release()
	require.NoError(t, err)
	require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, merged, increment))
	return filled, merged, filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ
}

// TestDecimalDivisionIncrementPreparedRecoveryReviewRound13Issue30938 is F1:
// the prepared COALESCE recovery over a quotient arm consumes the execute
// session's div_precision_increment, so the retried common and the persisted
// tag equal the parent's own merge at that setting.
func TestDecimalDivisionIncrementPreparedRecoveryReviewRound13Issue30938(t *testing.T) {
	tables := []string{"create table r13div(a decimal(38,0), b decimal(38,0), c decimal(38,0), d decimal(38,0))"}
	innerSQL := "create table r13out as select coalesce(a*b, (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div"
	binding := decimal128Binding("0.5", 20, 1)

	t.Run("nondefault increment recovers the session quotient", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables, 10, innerSQL)
		filled, _, tag := executeReviewRound13Binding(t, 10, original, stmt, retained, []any{binding})
		requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 75, 10,
			"F1: the rebound merge converges the parent's increment-10 common")
		requireDecimalTyp(t, tag, 75, 10,
			"F1: the refreshed record persists the parent's increment-10 tag")
	})

	t.Run("cached execution repetition", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables, 10, innerSQL)
		for repetition := 0; repetition < 2; repetition++ {
			filled, _, tag := executeReviewRound13Binding(t, 10, original, stmt, retained, []any{binding})
			requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 75, 10,
				"execute %d converges the parent's increment-10 common", repetition)
			requireDecimalTyp(t, tag, 75, 10,
				"execute %d persists the parent's increment-10 tag", repetition)
		}
	})

	t.Run("default increment control", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables, 4, innerSQL)
		_, _, tag := executeReviewRound13Binding(t, function.DefaultDivPrecisionIncrement, original, stmt, retained, []any{binding})
		requireDecimalTyp(t, tag, 69, 4,
			"F1 control: the default increment keeps the parent's (69,4)")
	})
}

// TestDecimalDivisionIncrementSetOperationRetryReviewRound13Issue30938 is
// F1's set-operation twin: the execute-time retry of a retried union common
// resolves the quotient branch's parent domain through the lineage channel,
// which must consume the same session increment as the publication did. The
// quotient carries the parameter so the branch type specializes at execute
// and the set-operation reconciliation re-runs the retry there.
func TestDecimalDivisionIncrementSetOperationRetryReviewRound13Issue30938(t *testing.T) {
	tables := []string{"create table r13div(a decimal(38,0), b decimal(38,0), c decimal(38,0), d decimal(38,0))"}
	innerSQL := "create table r13uout as select a*b as v from r13div " +
		"union all select (c*d)/? from r13div"
	binding := decimal128Binding("3", 1, 0)

	t.Run("nondefault increment set operation retry", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables, 10, innerSQL)
		filled, _, tag := executeReviewRound13Binding(t, 10, original, stmt, retained, []any{binding})
		query, filledNode := ddlUnionNode(t, filled, pb.Node_UNION_ALL)
		requireDecimalTyp(t, query.Nodes[filledNode.NodeId].ProjectList[0].Typ, 75, 10,
			"F1: the retried union converges the parent's increment-10 common")
		requireUnionBranchCastsOn(t, query, filledNode, 75, 10)
		requireDecimalTyp(t, tag, 75, 10)
	})

	t.Run("default increment set operation control", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables, 4, innerSQL)
		filled, _, tag := executeReviewRound13Binding(t, function.DefaultDivPrecisionIncrement, original, stmt, retained, []any{binding})
		query, filledNode := ddlUnionNode(t, filled, pb.Node_UNION_ALL)
		requireDecimalTyp(t, query.Nodes[filledNode.NodeId].ProjectList[0].Typ, 69, 4,
			"F1 control: the default increment keeps the parent's (69,4)")
		requireUnionBranchCastsOn(t, query, filledNode, 69, 4)
		requireDecimalTyp(t, tag, 69, 4)
	})
}

// ddlUnionNode is unionNode over a CTAS DDL plan's carried query (the slot
// ddlPlanHeadExpr reads).
func ddlUnionNode(t *testing.T, ddlPlan *pb.Plan, nodeType pb.Node_NodeType) (*pb.Query, *pb.Node) {
	t.Helper()
	query := ddlPlan.GetDdl().GetQuery()
	require.NotNil(t, query)
	var found *pb.Node
	for _, node := range query.Nodes {
		if node.NodeType == nodeType {
			require.Nil(t, found, "exactly one %s node expected", nodeType)
			found = node
		}
	}
	require.NotNil(t, found, "a %s node is required", nodeType)
	return query, found
}

// requireUnionBranchCastsOn is requireUnionBranchCasts over an explicitly
// resolved query (a CTAS DDL plan carries its query off the top level).
func requireUnionBranchCastsOn(t *testing.T, query *pb.Query, node *pb.Node, width, scale int32) {
	t.Helper()
	for _, childID := range node.Children {
		branch := query.Nodes[childID]
		require.Equal(t, pb.Node_PROJECT, branch.NodeType, "set branches are PROJECT nodes")
		require.Len(t, branch.ProjectList, 1)
		requireDecimalOverload(t, branch.ProjectList[0], function.CAST)
		requireDecimalTyp(t, branch.ProjectList[0].Typ, width, scale,
			"each branch aligns to the parent's common target")
	}
}

// TestDecimalMultiplyEnvelopeCarrierAdjustmentReviewRound13Issue30938 is F2:
// the divergent-envelope branch of the multiplication storage boundary
// applies the coefficient adjustment only when the recovered parent-side
// carrier is DECIMAL128. TRUNCATE(a*b, 0) over DECIMAL(38,27) operands
// reaches width 38 on the decimal256 carrier, so no adjustment applies and
// the outer product persists the parent's DECIMAL(39,0); the adjacent-width
// controls reaching 37 and 39 persist (38,0) and (40,0) on every engine.
func TestDecimalMultiplyEnvelopeCarrierAdjustmentReviewRound13Issue30938(t *testing.T) {
	tables := []string{
		"create table r13t38(a decimal(38,27), b decimal(38,27), c decimal(1,0))",
		"create table r13t37(a decimal(38,28), b decimal(38,28), c decimal(1,0))",
		"create table r13t39(a decimal(38,26), b decimal(38,26), c decimal(1,0))",
	}
	cells := []struct {
		name  string
		from  string
		width int32
	}{
		{"wrapper reaching 38", "r13t38", 39},
		{"wrapper reaching 37 control", "r13t37", 38},
		{"wrapper reaching 39 control", "r13t39", 40},
	}
	for _, cell := range cells {
		t.Run(cell.name+" ctas", func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			mock := NewMockOptimizer(false, proc)
			for _, ddl := range tables {
				registerModBoundaryTable(t, mock, ddl)
			}
			ctas, err := runOneStmt(mock, t,
				"create table r13f2 as select truncate(a*b,0)*c as v from "+cell.from)
			require.NoError(t, err)
			requireDecimalTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
				cell.width, 0,
				"F2: the CTAS record persists the parent's own tag for "+cell.name)
		})
		t.Run(cell.name+" view", func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			mock := NewMockOptimizer(false, proc)
			for _, ddl := range tables {
				registerModBoundaryTable(t, mock, ddl)
			}
			view, err := runOneStmt(mock, t,
				"create view r13f2v as select truncate(a*b,0)*c as v from "+cell.from)
			require.NoError(t, err)
			requireDecimalTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
				cell.width, 0,
				"F2: the view record persists the parent's own tag for "+cell.name)
		})
	}

	t.Run("wrapper reaching 38 prepared ctas", func(t *testing.T) {
		original, stmt, retained := prepareReviewRound13Fixture(t, tables,
			int64(function.DefaultDivPrecisionIncrement),
			"create table r13f2p as select truncate(a*b,0)*c as v from r13t38")
		_, _, tag := executeReviewRound13Binding(t, function.DefaultDivPrecisionIncrement,
			original, stmt, retained, nil)
		requireDecimalTyp(t, tag, 39, 0,
			"F2: the prepared refresh persists the parent's (39,0)")
	})
}

// prepareReviewRound13SelectFixture builds one prepared-SELECT snapshot for
// innerSQL under one session div_precision_increment, mirroring
// prepareReviewRound13Fixture for non-DDL inner statements.
func prepareReviewRound13SelectFixture(t *testing.T, tables []string, increment int64, innerSQL string) *pb.Plan {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, ddl := range tables {
		registerModBoundaryTable(t, mock, ddl)
	}
	compiler := divPrecisionCompilerContext{CompilerContext: &mock.ctxt, increment: increment}
	opt := reviewRound11Optimizer{ctx: compiler}
	prepared, err := runOneStmt(opt, t,
		"prepare boundary_fixture from '"+strings.ReplaceAll(innerSQL, "'", "''")+"'")
	require.NoError(t, err)
	return prepared.GetDcl().GetPrepare().Plan
}

// fillReviewRound13Select drives one EXECUTE of a prepared SELECT fixture
// through the entry-68 fill exactly as executeReviewRound13Binding does,
// returning the fill outcome for the subtest to classify.
func fillReviewRound13Select(t *testing.T, increment int32, original *pb.Plan, values []any) (*pb.Query, error) {
	t.Helper()
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	release := process.EnterDecimalBoundaryGeneration(execProc)
	filled, _, err := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, original, values, increment)
	release()
	if err != nil {
		return nil, err
	}
	return filled.GetQuery(), nil
}

// queryHeadExpr resolves the root node's first projection expression.
func queryHeadExpr(t *testing.T, query *pb.Query) *pb.Expr {
	t.Helper()
	require.NotNil(t, query)
	require.NotEmpty(t, query.Steps)
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotNil(t, root)
	require.NotEmpty(t, root.ProjectList)
	return root.ProjectList[0]
}

// decimalSourceTextBinding mirrors a SQL EXECUTE ... USING user variable
// assigned a decimal literal (SET @h = 0.5 types the variable decimal(1,1)):
// the value travels as text carrying its assignment-time source type.
func decimalSourceTextBinding(value string, width, scale int32) ParamValue {
	return ParamValue{Value: value,
		SourceType:          types.New(types.T_decimal64, width, scale),
		HasSourceType:       true,
		EnableNumericPrefix: true}
}

// varcharSourceTextBinding mirrors a SQL EXECUTE ... USING user variable
// assigned a string literal (SET @h = '0.5').
func varcharSourceTextBinding(value string) ParamValue {
	return ParamValue{Value: value,
		SourceType:          types.T_varchar.ToType(),
		HasSourceType:       true,
		EnableNumericPrefix: true}
}

// TestDecimalPreparedTextMarkerExactCommonReviewRound13Issue30938 pins the
// execute-time exact common of a text-transported parameter merged with a
// widened multiplication arm (issue #30938, design decision log 56's
// outcome contract): the widened (76,0) product surface must not overflow
// the prepared exact common where the parent's own domains still fit. The
// retry resolves each decimal operand's parent-published domain, so the
// merge converges the parent's own common at the session increment, exactly
// the computation the pre-change engine performs over the same statement. A
// genuine-wide operand without multiplication provenance and a merge whose
// parent common itself exceeds the carrier keep the pre-existing checked
// rejection on every engine.
func TestDecimalPreparedTextMarkerExactCommonReviewRound13Issue30938(t *testing.T) {
	tables := []string{"create table r13div(a decimal(38,0), b decimal(38,0), c decimal(38,0), d decimal(38,0), h decimal(65,65))"}
	innerSQL := "select coalesce(a*b, (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div"

	t.Run("decimal source typed route control", func(t *testing.T) {
		original := prepareReviewRound13SelectFixture(t, tables, 10, innerSQL)
		query, err := fillReviewRound13Select(t, 10, original,
			[]any{decimalSourceTextBinding("0.5", 1, 1)})
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, query).Typ, 75, 10,
			"the typed source route converges the parent's increment-10 domain")
	})

	t.Run("string user variable recovers the session quotient", func(t *testing.T) {
		original := prepareReviewRound13SelectFixture(t, tables, 10, innerSQL)
		query, err := fillReviewRound13Select(t, 10, original,
			[]any{varcharSourceTextBinding("0.5")})
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, query).Typ, 75, 10,
			"the exact common converges the parent's increment-10 domain")
	})

	t.Run("default increment control", func(t *testing.T) {
		original := prepareReviewRound13SelectFixture(t, tables,
			int64(function.DefaultDivPrecisionIncrement), innerSQL)
		query, err := fillReviewRound13Select(t, function.DefaultDivPrecisionIncrement, original,
			[]any{varcharSourceTextBinding("0.5")})
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, query).Typ, 69, 4,
			"the exact common converges the parent's increment-4 domain")
	})

	// The wide and high-scale arms below ride non-foldable forms on
	// purpose: the prepare-time provisional merge over a text marker
	// re-types a pure literal arm as TEXT (the engine's pre-existing
	// MySQL marker inference), and a merge whose decimal peers all
	// folded away never enters the execute-time exact path on any
	// engine. Only an arm bound as a DECIMAL function result or column
	// keeps its peer domain, so only that shape reaches the retry the
	// controls classify.
	t.Run("genuine wide peer keeps the checked rejection", func(t *testing.T) {
		original := prepareReviewRound13SelectFixture(t, tables, 10,
			"select coalesce(coalesce("+strings.Repeat("9", 76)+", a), (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div")
		_, err := fillReviewRound13Select(t, 10, original,
			[]any{varcharSourceTextBinding("0.5")})
		require.ErrorContains(t, err, "prepared common type exceeds exact precision")
	})

	t.Run("parent common overflow keeps the checked rejection", func(t *testing.T) {
		original := prepareReviewRound13SelectFixture(t, tables, 10,
			"select coalesce(a*b, h, ?) as v from r13div")
		_, err := fillReviewRound13Select(t, 10, original,
			[]any{varcharSourceTextBinding("0.5")})
		require.ErrorContains(t, err, "prepared common type exceeds exact precision")
	})
}

// executeReviewRound13PreparedSelect binds one prepared SELECT through the
// SQL EXECUTE source-binding route (the live EXECUTE ... USING path) under
// one session div_precision_increment, returning the bound plan or the
// binding error for the subtest to classify.
func executeReviewRound13PreparedSelect(t *testing.T, tables []string, increment int64, innerSQL string, binding PreparedSourceBinding, value any) (*pb.Plan, error) {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, ddl := range tables {
		registerModBoundaryTable(t, mock, ddl)
	}
	compiler := divPrecisionCompilerContext{CompilerContext: &mock.ctxt, increment: increment}
	stmt, err := mysql.ParseOne(context.Background(), innerSQL, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	bound, err := BuildPreparedExecutionPlan(compiler, stmt,
		[]PreparedSourceBinding{binding}, []any{value})
	if err != nil {
		return nil, err
	}
	return bound.Plan, nil
}

// decimalUserVariableExecute mirrors a live SQL EXECUTE ... USING user
// variable assigned a decimal literal: the source binding carries the
// assignment-time type and the value travels as text with the decimal
// prepare-param kind.
func decimalUserVariableExecute(value string, width, scale int32) (PreparedSourceBinding, ParamValue) {
	source := types.New(types.T_decimal64, width, scale)
	return PreparedSourceBinding{Position: 0, Type: source},
		ParamValue{Value: value,
			SourceType:          source,
			HasSourceType:       true,
			PrepareParamKind:    vector.PrepareParamDecimal,
			EnableNumericPrefix: true}
}

// varcharUserVariableExecute mirrors a live SQL EXECUTE ... USING user
// variable assigned a string literal.
func varcharUserVariableExecute(value string) (PreparedSourceBinding, ParamValue) {
	return PreparedSourceBinding{Position: 0, Type: types.T_varchar.ToType()},
		varcharSourceTextBinding(value)
}

// TestDecimalPreparedExecuteExactCommonReviewRound13Issue30938 is
// TestDecimalPreparedTextMarkerExactCommonReviewRound13Issue30938's live
// twin: the same merge bound through the source-binding route every SQL
// EXECUTE ... USING takes. The retry reads the statement's own decision
// ledger and session increment through the armed source-binding state, so
// the exact common converges the parent's domain on this route exactly as
// on the fill route.
func TestDecimalPreparedExecuteExactCommonReviewRound13Issue30938(t *testing.T) {
	tables := []string{"create table r13div(a decimal(38,0), b decimal(38,0), c decimal(38,0), d decimal(38,0), h decimal(65,65))"}
	innerSQL := "select coalesce(a*b, (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div"
	decimalBinding, decimalValue := decimalUserVariableExecute("0.5", 1, 1)
	varcharBinding, varcharValue := varcharUserVariableExecute("0.5")

	t.Run("decimal user variable recovers the session quotient", func(t *testing.T) {
		bound, err := executeReviewRound13PreparedSelect(t, tables, 10, innerSQL, decimalBinding, decimalValue)
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, bound.GetQuery()).Typ, 75, 10,
			"the exact common converges the parent's increment-10 domain")
	})

	t.Run("string user variable recovers the session quotient", func(t *testing.T) {
		bound, err := executeReviewRound13PreparedSelect(t, tables, 10, innerSQL, varcharBinding, varcharValue)
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, bound.GetQuery()).Typ, 75, 10,
			"the exact common converges the parent's increment-10 domain")
	})

	t.Run("default increment control", func(t *testing.T) {
		bound, err := executeReviewRound13PreparedSelect(t, tables,
			int64(function.DefaultDivPrecisionIncrement), innerSQL, decimalBinding, decimalValue)
		require.NoError(t, err)
		requireDecimalTyp(t, queryHeadExpr(t, bound.GetQuery()).Typ, 69, 4,
			"the exact common converges the parent's increment-4 domain")
	})

	t.Run("genuine wide peer keeps the checked rejection", func(t *testing.T) {
		_, err := executeReviewRound13PreparedSelect(t, tables, 10,
			"select coalesce(coalesce("+strings.Repeat("9", 76)+", a), (c*d)/cast(3 as decimal(1,0)), ?) as v from r13div",
			varcharBinding, varcharValue)
		require.ErrorContains(t, err, "prepared common type exceeds exact precision")
	})

	t.Run("parent common overflow keeps the checked rejection", func(t *testing.T) {
		_, err := executeReviewRound13PreparedSelect(t, tables, 10,
			"select coalesce(a*b, h, ?) as v from r13div",
			varcharBinding, varcharValue)
		require.ErrorContains(t, err, "prepared common type exceeds exact precision")
	})
}
