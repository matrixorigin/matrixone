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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// evalDecimalMultiplyExpr binds one SELECT expression through the public
// planner and evaluates it with the vectorized executor, so both the resolved
// result metadata and the produced value follow the execution owner a session
// would hit.
func evalDecimalMultiplyExpr(t *testing.T, sql string) (*pb.Expr, *vector.Vector, error) {
	t.Helper()
	stmt, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
	require.NoError(t, err)
	var expr *pb.Expr
	for _, node := range stmt.GetQuery().Nodes {
		if node.NodeType == pb.Node_PROJECT {
			require.Len(t, node.ProjectList, 1)
			expr = node.ProjectList[0]
		}
	}
	require.NotNil(t, expr)

	proc := newPlanTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	// The caller reads the returned vector after this function returns, so
	// the executor is released with the test rather than here.
	t.Cleanup(executor.Free)
	result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	return expr, result, err
}

// TestDecimalMultiplicationPhysicalCarrierPrecisionPublicPath reproduces issue
// #30938 end to end: products that fit the Decimal256 physical carrier must be
// returned with the carrier precision instead of failing the 65-digit result
// check. Each expected value is calculated independently of the engine.
func TestDecimalMultiplicationPhysicalCarrierPrecisionPublicPath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantScale int32
		wantNull  bool
		want      string
	}{
		{
			name:      "issue 30938 38x38 literals",
			sql:       "select 99999999999999999999999999999999999999 * 99999999999999999999999999999999999999",
			wantWidth: 76,
			wantScale: 0,
			want:      "9999999999999999999999999999999999999800000000000000000000000000000000000001",
		},
		{
			name:      "issue 30938 declared 65 times two",
			sql:       "select cast(repeat('9',65) as decimal(65,0)) * 2",
			wantWidth: 66,
			wantScale: 0,
			want:      "199999999999999999999999999999999999999999999999999999999999999998",
		},
		{
			name:      "declared 65 times one",
			sql:       "select cast(repeat('9',65) as decimal(65,0)) * 1",
			wantWidth: 66,
			wantScale: 0,
			want:      "99999999999999999999999999999999999999999999999999999999999999999",
		},
		{
			name:      "negative times positive",
			sql:       "select cast('-99999999999999999999999999999999999999' as decimal(38,0)) * cast('99999999999999999999999999999999999999' as decimal(38,0))",
			wantWidth: 76,
			wantScale: 0,
			want:      "-9999999999999999999999999999999999999800000000000000000000000000000000000001",
		},
		{
			name:      "null operand",
			sql:       "select cast(null as decimal(38,0)) * 99999999999999999999999999999999999999",
			wantWidth: 76,
			wantScale: 0,
			wantNull:  true,
		},
		{
			name:      "scaled product fills the carrier coefficient",
			sql:       "select cast('111111111111111111111111111111111111.11' as decimal(38,2)) * cast('111111111111111111111111111111111111.11' as decimal(38,2))",
			wantWidth: 76,
			wantScale: 4,
			want:      "12345679012345679012345679012345679012098765432098765432098765432098765.4321",
		},
		{
			// A product that fits the target domain still casts cleanly;
			// this guards the explicit-cast contract rather than the issue.
			name:      "in range cast of a wide product",
			sql:       "select cast(99999999999999999999999999999 * 99999999999999999999999999999 as decimal(58,0))",
			wantWidth: 58,
			wantScale: 0,
			want:      "9999999999999999999999999999800000000000000000000000000001",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, result, err := evalDecimalMultiplyExpr(t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
			require.Equal(t, tc.wantWidth, expr.Typ.Width)
			require.Equal(t, tc.wantScale, expr.Typ.Scale)
			if tc.wantNull {
				require.True(t, result.IsNull(0))
				return
			}
			require.False(t, result.IsNull(0))
			expected, parseErr := types.ParseDecimal256(tc.want, tc.wantWidth, tc.wantScale)
			require.NoError(t, parseErr)
			require.Equal(t, expected, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0))
		})
	}
}

// TestDecimalMultiplicationBoundaryFailuresPublicPath preserves the unhappy
// paths around the issue #30938 repair: values past the physical carrier
// still raise the kernel overflow even after multiplication publishes the
// carrier precision.
func TestDecimalMultiplicationBoundaryFailuresPublicPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "physical carrier overflow",
			sql:     "select cast(repeat('9',65) as decimal(65,0)) * cast(repeat('9',65) as decimal(65,0))",
			wantErr: "Mul overflow",
		},
		{
			name:    "negative physical carrier overflow",
			sql:     "select cast('-99999999999999999999999999999999999999999999999999999999999999999' as decimal(65,0)) * cast(repeat('9',65) as decimal(65,0))",
			wantErr: "Mul overflow",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := evalDecimalMultiplyExpr(t, tc.sql)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange) || moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
				"error: %v", err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestDecimalMultiplicationDeclaredCastClipsWideProduct pins the plain SELECT
// cast contract around issue #30938: a product inside the physical carrier
// multiplies exactly, and a non-strict cast into a narrower declared domain
// saturates to the domain endpoint, matching MySQL's clipping semantics.
// Strict write admission (INSERT ... SELECT, CTAS) rejects the same values;
// that vector is covered by the BVT case file for this issue.
func TestDecimalMultiplicationDeclaredCastClipsWideProduct(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		want      string
	}{
		{
			name:      "declared 65 cast clips a 76-digit product",
			sql:       "select cast(99999999999999999999999999999999999999 * 99999999999999999999999999999999999999 as decimal(65,0))",
			wantWidth: 65,
			want:      strings.Repeat("9", 65),
		},
		{
			name:      "declared 65 cast clips a 66-digit product",
			sql:       "select cast(cast(repeat('9',65) as decimal(65,0)) * 2 as decimal(65,0))",
			wantWidth: 65,
			want:      strings.Repeat("9", 65),
		},
		{
			name:      "declared 38 cast clips a 76-digit product",
			sql:       "select cast(99999999999999999999999999999999999999 * 99999999999999999999999999999999999999 as decimal(38,0))",
			wantWidth: 38,
			want:      strings.Repeat("9", 38),
		},
		{
			name:      "negative product clips to the negative endpoint",
			sql:       "select cast(cast('-99999999999999999999999999999999999999' as decimal(38,0)) * cast('99999999999999999999999999999999999999' as decimal(38,0)) as decimal(65,0))",
			wantWidth: 65,
			want:      "-" + strings.Repeat("9", 65),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, result, err := evalDecimalMultiplyExpr(t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.wantWidth, expr.Typ.Width)
			require.False(t, result.IsNull(0))
			if tc.wantWidth > 38 {
				require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
				expected, parseErr := types.ParseDecimal256(tc.want, tc.wantWidth, 0)
				require.NoError(t, parseErr)
				require.Equal(t, expected, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0))
			} else {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				expected, parseErr := types.ParseDecimal128(tc.want, tc.wantWidth, 0)
				require.NoError(t, parseErr)
				require.Equal(t, expected, vector.GetFixedAtWithTypeCheck[types.Decimal128](result, 0))
			}
		})
	}
}

// requireDecimalTyp pins one DECIMAL256 plan type's declared width and scale.
func requireDecimalTyp(t *testing.T, typ pb.Type, width, scale int32, msgAndArgs ...any) {
	t.Helper()
	message := append([]any{typ.String()}, msgAndArgs...)
	require.Equal(t, int32(types.T_decimal256), typ.Id, message...)
	require.Equal(t, width, typ.Width, message...)
	require.Equal(t, scale, typ.Scale, message...)
}

// prepareDecimalBoundaryFixture drives one prepared-CTAS fixture through the
// public planner over an injected (a, b DECIMAL(38,10), d DECIMAL(39,1))
// source and claims the completed generation's published decision ledger
// exactly as the production prepare-time claim does
// (mysql_cmd_executor.go's claimPreparedDecimalBoundaryEvidence). It returns
// the PREPARE snapshot plan, the parsed inner CTAS statement and the claimed
// carrier (issue #30938, design decision log 66).
func prepareDecimalBoundaryFixture(t *testing.T, innerSQL string) (*pb.Plan, tree.Statement, map[string]any) {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	source, err := buildTestCreateTableStmt(mock,
		"create table t30938(a decimal(38,10), b decimal(38,10), d decimal(39,1))")
	require.NoError(t, err)
	mock.ctxt.tables["t30938"] = source
	mock.ctxt.objects["t30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "t30938"}
	stmt, err := mysql.ParseOne(context.Background(), innerSQL, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	prepared, err := runOneStmt(mock, t,
		"prepare boundary_fixture from '"+strings.ReplaceAll(innerSQL, "'", "''")+"'")
	require.NoError(t, err)
	retained, ok := mock.ctxt.GetProcess().ClaimDecimalBoundaryEvidence()
	require.True(t, ok, "a prepared-CTAS generation must publish its decision ledger")
	require.NotEmpty(t, retained)
	return prepared.GetDcl().GetPrepare().Plan, stmt, retained
}

// ddlPlanHeadExpr returns the single output expression of a CTAS DDL plan's
// query, the same slot RefreshPreparedCTASInferredColumns evaluates.
func ddlPlanHeadExpr(t *testing.T, ddlPlan *pb.Plan) *pb.Expr {
	t.Helper()
	query := ddlPlan.GetDdl().Query
	require.NotNil(t, query)
	head := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.Len(t, head, 1)
	return head[0]
}

// requireDecimalOverload asserts the bound overload identity of a function
// expression node.
func requireDecimalOverload(t *testing.T, expr *pb.Expr, want int) *pb.Function {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.NotNil(t, fn.Func)
	id, _ := function.DecodeOverloadID(fn.Func.GetObj())
	require.Equal(t, int64(want), int64(id))
	return fn
}

// refreshBoundaryWithParam specializes a prepared-CTAS snapshot with one
// integer parameter and runs the execute-time inferred-column refresh with
// the supplied carrier, returning the persisted target column's type.
func refreshBoundaryWithParam(t *testing.T, original *pb.Plan, stmt tree.Statement, retained map[string]any) pb.Type {
	t.Helper()
	value := ParamValue{Value: "0", SourceType: types.T_int64.ToType(),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamInteger}
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{value})
	require.NoError(t, err)
	require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, retained, function.DefaultDivPrecisionIncrement))
	return filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ
}

// TestPreparedCTASDecimalBoundaryEvidenceOracles is design revision 27's
// digits-parameter semantics (public contract semantics[4]): over the
// prepared-CTAS fixture TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), ?) with
// nullable (38,10) operands and d DECIMAL(39,1), parameter specialization
// through a function-headed output expression does not respecialize declared
// CTAS output types on either engine — the observable contract is the
// parent's snapshot tag DECIMAL(54,1) at every execute locus. The snapshot
// still records exactly what the binder decided: the unrefined DECIMAL(65,1)
// head with the digits argument bound as an integer cast over the still-open
// parameter marker, and the merge's content-keyed decision entry. (Replaces
// the superseded decision-log-66 (53,0)/(64,0) re-derivation oracles, which
// described a design rejected before revision 20's approval.)
func TestPreparedCTASDecimalBoundaryEvidenceOracles(t *testing.T) {
	innerSQL := "create table t30938_out as select truncate(coalesce(truncate(a*b, 0), d), ?) as v from t30938"
	original, stmt, retained := prepareDecimalBoundaryFixture(t, innerSQL)

	// Never-ran oracle: the snapshot carries the unrefined head type and the
	// parent's storage-envelope tag, exactly as the parent's own PREPARE does.
	requireDecimalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 54, 1)
	head := ddlPlanHeadExpr(t, original)
	requireDecimalTyp(t, head.Typ, 65, 1)

	// The head's digits argument is an integer cast over the still-open
	// parameter marker at PREPARE, holding refineDecimalRoundingReturnType at
	// its early return.
	truncate := requireDecimalOverload(t, head, function.TRUNCATE)
	require.Len(t, truncate.Args, 2)
	digitsCast := truncate.Args[1].GetF()
	require.NotNil(t, digitsCast, "the digits argument binds as an integer cast")
	require.NotNil(t, digitsCast.Args[0].GetP(), "the cast's operand stays a parameter marker")

	// The merge keeps exactly one decision-owned alignment cast into
	// DECIMAL(65,1), on the refined inner TRUNCATE arm (64,0); the column-d
	// arm already matches the family's OID and scale, so the binder's
	// same-scale alignment skip leaves it uncast at (39,1) (review note P3).
	merge := truncate.Args[0]
	coalesce := requireDecimalOverload(t, merge, function.COALESCE)
	require.Len(t, coalesce.Args, 2)
	leftCast := requireDecimalOverload(t, coalesce.Args[0], function.CAST)
	requireDecimalTyp(t, coalesce.Args[0].Typ, 65, 1)
	require.NotNil(t, coalesce.Args[1].GetCol(), "the d arm stays an uncast column under the same-scale skip")
	requireDecimalTyp(t, coalesce.Args[1].Typ, 39, 1)
	requireDecimalTyp(t, leftCast.Args[0].Typ, 64, 0)

	// The published generation recorded exactly this merge decision under its
	// content key: the binder consumed (64,0) and (39,1), aligned the TRUNCATE
	// arm to (65,1) and left the same-scale d arm uncast — an uncast position
	// records its pre type verbatim — with widened-multiplication provenance.
	payload, found := retained[rule.DecimalBoundaryDecisionLedgerKey(merge)]
	require.True(t, found, "the claimed ledger must hold the merge's decision entry")
	entry, ok := payload.(*rule.DecimalBoundaryDecisionEntry)
	require.True(t, ok)
	require.Equal(t, 2, entry.ArgCount)
	require.True(t, entry.Provenance, "the merge decision carries widened-multiplication provenance")
	requireDecimalTyp(t, entry.PreTypes[0], 64, 0)
	requireDecimalTyp(t, entry.PreTypes[1], 39, 1)
	requireDecimalTyp(t, entry.AlignedTypes[0], 65, 1)
	requireDecimalTyp(t, entry.AlignedTypes[1], 39, 1)
	requireDecimalTyp(t, entry.ResultType, 65, 1)

	// EXECUTE ?=0 specializes the digits literal, but the function-headed
	// output expression does not respecialize the declared head type on
	// either engine; the execute plan still carries the prepare-time tag.
	value := ParamValue{Value: "0", SourceType: types.T_int64.ToType(),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamInteger}
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{value})
	require.NoError(t, err)
	runtimeHead := ddlPlanHeadExpr(t, filled)
	requireDecimalTyp(t, runtimeHead.Typ, 65, 1, "the head type is not respecialized at EXECUTE")
	digits, ok := runtimeHead.GetF().Args[1].GetF().Args[0].GetLit().GetValue().(*pb.Literal_I64Val)
	require.True(t, ok, "the digits marker must bind as an I64 literal inside the cast")
	require.Equal(t, int64(0), digits.I64Val)
	requireDecimalTyp(t, filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 54, 1,
		"before the refresh the execute plan carries the snapshot tag (the never-ran oracle)")

	// Consumption oracle: the refresh persists the parent's snapshot tag,
	// identical to the parent's own execute at every execute locus.
	require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, retained, function.DefaultDivPrecisionIncrement))
	requireDecimalTyp(t, filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 54, 1,
		"the refresh persists the parent's DECIMAL(54,1)")

	// Empty-ledger channel: the same parent-parity tag persists, exactly as
	// the parent's own execute behaves on a fully current cluster.
	requireDecimalTyp(t, refreshBoundaryWithParam(t, original, stmt, nil), 54, 1,
		"the parent's snapshot tag persists on the ledger-free channel too")
}

// TestPreparedCTASDecimalBoundaryExplicitCastControl is design revision 27's
// SQL-authored explicit-cast control (public contract semantics[4]): a
// SQL-authored CAST(d AS DECIMAL(65,1)) is user semantics — the boundary
// stores the authored DECIMAL(65,1) at PREPARE and persists it at EXECUTE on
// every ledger channel, exactly as authored. (Replaces the superseded
// decision-log-66 (64,0) envelope oracle.)
func TestPreparedCTASDecimalBoundaryExplicitCastControl(t *testing.T) {
	innerSQL := "create table t30938_out as select truncate(coalesce(truncate(a*b, 0), cast(d as decimal(65,1))), ?) as v from t30938"
	original, stmt, retained := prepareDecimalBoundaryFixture(t, innerSQL)

	requireDecimalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 65, 1,
		"the authored cast's envelope keeps the snapshot tag at DECIMAL(65,1)")
	requireDecimalTyp(t, ddlPlanHeadExpr(t, original).Typ, 65, 1)

	requireDecimalTyp(t, refreshBoundaryWithParam(t, original, stmt, retained), 65, 1,
		"with the carrier the authored control persists the authored DECIMAL(65,1)")
	requireDecimalTyp(t, refreshBoundaryWithParam(t, original, stmt, nil), 65, 1,
		"without the carrier the authored control persists the same DECIMAL(65,1)")
}

// TestPreparedCTASDecimalBoundaryNoParameterControl is the decision-log-66
// (iii) no-parameter control: the literal-digits head refines at PREPARE
// itself, so prepare-time evaluation alone persists the parent's DECIMAL(53,0)
// and the execute-time refresh branch is never reached (it requires
// numParams > 0).
func TestPreparedCTASDecimalBoundaryNoParameterControl(t *testing.T) {
	innerSQL := "create table t30938_out as select truncate(coalesce(truncate(a*b, 0), d), 0) as v from t30938"
	original, stmt, _ := prepareDecimalBoundaryFixture(t, innerSQL)
	require.Equal(t, 0, tree.ParameterCount(stmt))

	// The head refined at PREPARE (literal digits), and the snapshot already
	// persists the parent's tag.
	requireDecimalTyp(t, ddlPlanHeadExpr(t, original).Typ, 64, 0)
	requireDecimalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 53, 0,
		"prepare-time evaluation alone persists the parent's DECIMAL(53,0)")
}

// prepareDecimalBoundaryFixtureFor generalizes prepareDecimalBoundaryFixture:
// it registers the supplied mock tables, builds one prepared-CTAS fixture
// through the public planner and claims the generation's published decision
// ledger exactly as the production prepare-time claim does.
func prepareDecimalBoundaryFixtureFor(t *testing.T, tables map[string]string, innerSQL string) (*pb.Plan, tree.Statement, map[string]any) {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for name, ddl := range tables {
		source, err := buildTestCreateTableStmt(mock, ddl)
		require.NoError(t, err)
		mock.ctxt.tables[name] = source
		mock.ctxt.objects[name] = &ObjectRef{SchemaName: "tpch", ObjName: name}
	}
	stmt, err := mysql.ParseOne(context.Background(), innerSQL, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	prepared, err := runOneStmt(mock, t,
		"prepare boundary_fixture from '"+strings.ReplaceAll(innerSQL, "'", "''")+"'")
	require.NoError(t, err)
	retained, ok := mock.ctxt.GetProcess().ClaimDecimalBoundaryEvidence()
	require.True(t, ok, "a prepared-CTAS generation must publish its decision ledger")
	require.NotEmpty(t, retained)
	return prepared.GetDcl().GetPrepare().Plan, stmt, retained
}

// decimal128Binding authors one DECIMAL(20, s) text-protocol execute binding.
func decimal128Binding(value string, width, scale int32) ParamValue {
	return ParamValue{Value: value, SourceType: types.New(types.T_decimal128, width, scale),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal}
}

// executePreparedBoundaryBinding drives one EXECUTE of a prepared-CTAS
// fixture through the entry-68 execute generation: the fill runs against the
// live execute process (so lineage attaches and rebound decisions record
// into the execute-scope registry), the refresh consumes the retained
// carrier overlaid by the execute entries exactly as the production routes
// merge them, and the persisted tag is returned with the live ledger.
func executePreparedBoundaryBinding(t *testing.T, original *pb.Plan, stmt tree.Statement, retained map[string]any, value ParamValue) (*pb.Plan, map[string]any, pb.Type) {
	t.Helper()
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	release := process.EnterDecimalBoundaryGeneration(execProc)
	filled, _, err := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, original, []any{value}, function.DefaultDivPrecisionIncrement)
	merged := make(map[string]any, len(retained)+len(execProc.DecimalBoundaryDecisions()))
	for key, entry := range retained {
		merged[key] = entry
	}
	for key, entry := range execProc.DecimalBoundaryDecisions() {
		merged[key] = entry
	}
	release()
	require.NoError(t, err)
	require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, merged, function.DefaultDivPrecisionIncrement))
	return filled, merged, filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ
}

// ablatePreparedBoundaryFill drives the same EXECUTE binding through the
// lineage-withheld channel (the plain package fill carries no execute
// process, so no generation and no lineage view attach), returning the fill
// outcome the production routes would have produced before the repair.
func ablatePreparedBoundaryFill(original *pb.Plan, value ParamValue) (*pb.Plan, error) {
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), original, []any{value})
	return filled, err
}

// registerSetOperationMockTables installs the mixed-scale set-operation
// fixtures of design cell (xx): s30938x carries (a, b DECIMAL(38,0)) and
// d DECIMAL(65,1), s30938y carries (a, b DECIMAL(35,0)) for the scale-0
// control.
func registerSetOperationMockTables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	sx, err := buildTestCreateTableStmt(mock,
		"create table s30938x(a decimal(38,0), b decimal(38,0), d decimal(65,1))")
	require.NoError(t, err)
	mock.ctxt.tables["s30938x"] = sx
	mock.ctxt.objects["s30938x"] = &ObjectRef{SchemaName: "tpch", ObjName: "s30938x"}
	sy, err := buildTestCreateTableStmt(mock,
		"create table s30938y(a decimal(35,0), b decimal(35,0))")
	require.NoError(t, err)
	mock.ctxt.tables["s30938y"] = sy
	mock.ctxt.objects["s30938y"] = &ObjectRef{SchemaName: "tpch", ObjName: "s30938y"}
}

// buildStatementWithOpenLedger runs one statement through the public planner
// inside a caller-owned decimal-boundary generation so the retried decision
// records the set-operation owners synthesize stay observable in the live
// registry after the build returns.
func buildStatementWithOpenLedger(t *testing.T, sql string) (*pb.Plan, map[string]any, error) {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerSetOperationMockTables(t, mock)
	release := process.EnterDecimalBoundaryGeneration(proc)
	built, err := runOneStmt(mock, t, sql)
	ledger, _ := proc.ClaimDecimalBoundaryEvidence()
	if ledger == nil {
		ledger = map[string]any{}
		for key, entry := range proc.DecimalBoundaryDecisions() {
			ledger[key] = entry
		}
	}
	release()
	return built, ledger, err
}

// unionNode returns the single Node_UNION_ALL/Node_UNION/INTERSECT/MINUS node
// of a statement plan.
func unionNode(t *testing.T, built *pb.Plan, nodeType pb.Node_NodeType) *pb.Node {
	t.Helper()
	query := built.GetQuery()
	require.NotNil(t, query)
	var found *pb.Node
	for _, node := range query.Nodes {
		if node.NodeType == nodeType {
			require.Nil(t, found, "exactly one %s node expected", nodeType)
			found = node
		}
	}
	require.NotNil(t, found, "a %s node is required", nodeType)
	return found
}

// requireRetriedCoalesceDecision locates the content-keyed retried merge
// decision the entry-73 set-operation retry records and asserts its exact
// parent vector, alignment and result.
func requireRetriedCoalesceDecision(t *testing.T, ledger map[string]any, pre [][2]int32, alignedType, resultType [2]int32) *rule.DecimalBoundaryDecisionEntry {
	t.Helper()
	var retried *rule.DecimalBoundaryDecisionEntry
	for key, payload := range ledger {
		entry, ok := payload.(*rule.DecimalBoundaryDecisionEntry)
		if !ok || entry.Name != "coalesce" {
			continue
		}
		require.True(t, entry.Retried, "ledger coalesce entry %s must be flagged retried", key)
		require.True(t, entry.Provenance, "ledger coalesce entry %s must carry provenance", key)
		require.Nil(t, retried, "at most one retried coalesce entry per statement")
		retried = entry
	}
	require.NotNil(t, retried, "the retried binding must be recorded content-keyed")
	require.Equal(t, len(pre), retried.ArgCount)
	for i, want := range pre {
		requireDecimalTyp(t, retried.PreTypes[i], want[0], want[1], "retried parent vector position", i)
		requireDecimalTyp(t, retried.AlignedTypes[i], alignedType[0], alignedType[1],
			"every retried position aligns to the parent's common", i)
	}
	requireDecimalTyp(t, retried.ResultType, resultType[0], resultType[1],
		"the retried binding authors the parent's own common type")
	return retried
}

// requireNoRetriedCoalesceDecision asserts that no statement produced a
// retried merge decision: the first-pass binding converged (or failed)
// without engaging the entry-73 retry.
func requireNoRetriedCoalesceDecision(t *testing.T, ledger map[string]any) {
	t.Helper()
	for key, payload := range ledger {
		if entry, ok := payload.(*rule.DecimalBoundaryDecisionEntry); ok && entry.Name == "coalesce" {
			require.False(t, entry.Retried, "no retried coalesce decision expected, got %s", key)
		}
	}
}

// requireUnionBranchCasts asserts that every branch PROJECT export of column 0
// is an implicit alignment cast to the wanted type (the parent-authored
// per-branch alignment view of entry 73 (iii)).
func requireUnionBranchCasts(t *testing.T, built *pb.Plan, node *pb.Node, width, scale int32) {
	t.Helper()
	query := built.GetQuery()
	for _, childID := range node.Children {
		branch := query.Nodes[childID]
		require.Equal(t, pb.Node_PROJECT, branch.NodeType, "set branches are PROJECT nodes")
		require.Len(t, branch.ProjectList, 1)
		requireDecimalOverload(t, branch.ProjectList[0], function.CAST)
		requireDecimalTyp(t, branch.ProjectList[0].Typ, width, scale,
			"each branch aligns to the parent's common target")
	}
}

// TestSetOperationCommonTypeRetryMixedScaleIssue30938 is design revision 27
// cell (xx)'s initial-route witness: over s(a, b DECIMAL(38,0);
// d DECIMAL(65,1)) all holding 1, the widened (76,0) product arm against the
// (65,1) sibling forces a carrier-overflowing 77-width common that the
// parent's own engine binds as (66,1) (the (65,0)/(65,1) common: integral 65
// + scale 1). The initial common-type owner retries its failed lookup over
// the parent-published branch vector, binds the parent's own (66,1) with
// parent-authored per-branch alignment and records the retried binding
// content-keyed; the reversed order must bind identically.
func TestSetOperationCommonTypeRetryMixedScaleIssue30938(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		pre  [][2]int32
	}{
		{
			name: "product arm first",
			sql:  "select a*b as v from s30938x union all select d from s30938x",
			pre:  [][2]int32{{65, 0}, {65, 1}},
		},
		{
			name: "product arm second",
			sql:  "select d as v from s30938x union all select a*b from s30938x",
			pre:  [][2]int32{{65, 1}, {65, 0}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, ledger, err := buildStatementWithOpenLedger(t, tc.sql)
			require.NoError(t, err, "the retried binding must rescue the overflow the base engine rejects")
			node := unionNode(t, built, pb.Node_UNION_ALL)
			requireDecimalTyp(t, built.GetQuery().Nodes[node.Children[0]].ProjectList[0].Typ, 66, 1,
				"the aligned branch export carries the parent's common type")
			requireDecimalTyp(t, node.ProjectList[0].Typ, 66, 1)
			requireUnionBranchCasts(t, built, node, 66, 1)
			requireRetriedCoalesceDecision(t, ledger, tc.pre, [2]int32{66, 1}, [2]int32{66, 1})
		})
	}
}

// TestSetOperationCommonTypeRetryCTASPersistsParentTag pins cell (xx)'s
// storage surface: CREATE TABLE AS over the mixed-scale counterexample
// persists the parent's own DECIMAL(66,1) declared tag, never the widened
// derivation's overflow.
func TestSetOperationCommonTypeRetryCTASPersistsParentTag(t *testing.T) {
	built, ledger, err := buildStatementWithOpenLedger(t,
		"create table s30938out as select a*b as v from s30938x union all select d from s30938x")
	require.NoError(t, err)
	cols := built.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.NotEmpty(t, cols)
	requireDecimalTyp(t, cols[0].Typ, 66, 1, "CTAS persists the parent's own (66,1)")
	requireRetriedCoalesceDecision(t, ledger, [][2]int32{{65, 0}, {65, 1}}, [2]int32{66, 1}, [2]int32{66, 1})
}

// TestSetOperationCommonTypeRetryScaleZeroControl is cell (xx)'s MZ control:
// the scale-0 (76,0)/(70,0) union binds through the ordinary first-pass
// common (76,0) without the retry, so its resolution path is
// retry-invariant and records no retried decision.
func TestSetOperationCommonTypeRetryScaleZeroControl(t *testing.T) {
	built, ledger, err := buildStatementWithOpenLedger(t,
		"select a*b as v from s30938y union all select a*b from s30938x")
	require.NoError(t, err, "the scale-0 control bound before the repair and must bind after")
	node := unionNode(t, built, pb.Node_UNION_ALL)
	union := built.GetQuery().Nodes[node.NodeId]
	requireDecimalTyp(t, union.ProjectList[0].Typ, 76, 0, "the scale-0 common stays (76,0)")
	query := built.GetQuery()
	var castBranches, directBranches int
	for _, childID := range node.Children {
		branch := query.Nodes[childID]
		if branch.ProjectList[0].GetF() != nil {
			id, _ := function.DecodeOverloadID(branch.ProjectList[0].GetF().Func.GetObj())
			if id == function.CAST {
				castBranches++
				continue
			}
		}
		directBranches++
	}
	require.Equal(t, 1, castBranches, "only the narrower branch gains the published alignment")
	require.Equal(t, 1, directBranches, "the wider branch export stays the direct product")
	requireNoRetriedCoalesceDecision(t, ledger)
}

// TestSetOperationCommonTypeRetryAuthoredCastControl is cell (xx)'s MP
// control: SQL-authored CAST(a*b AS DECIMAL(65,0)) makes the export carry the
// parent's own domain, so the owner combines (66,1) on its first lookup and
// binds identically before and after the repair, keeping the authored target.
func TestSetOperationCommonTypeRetryAuthoredCastControl(t *testing.T) {
	built, ledger, err := buildStatementWithOpenLedger(t,
		"select cast(a*b as decimal(65,0)) as v from s30938x union all select d from s30938x")
	require.NoError(t, err, "the authored-cast control binds today with its authored targets")
	node := unionNode(t, built, pb.Node_UNION_ALL)
	requireDecimalTyp(t, built.GetQuery().Nodes[node.NodeId].ProjectList[0].Typ, 66, 1,
		"the parent-domain exports combine to the parent's (66,1)")
	requireNoRetriedCoalesceDecision(t, ledger)
}

// TestSetOperationCommonTypeRetryGenuineWideControl is cell (xx)'s MW
// control: a 76-digit integer-literal branch against a scaled 9.9 sibling
// needs a genuine 77-width common with NO widened-multiplication provenance,
// so the retry defers and the owner re-arms its original outcome
// byte-identically — before and after the repair.
func TestSetOperationCommonTypeRetryGenuineWideControl(t *testing.T) {
	_, ledger, err := buildStatementWithOpenLedger(t,
		"select "+strings.Repeat("9", 76)+" as v union all select 9.9")
	require.Error(t, err, "the genuine-wide 77 control must keep the parent's checked rejection")
	require.Equal(t, "SQL parser error: the 0 column cann't cast to a same type", err.Error(),
		"the re-armed outcome is byte-identical to the base engine's")
	requireNoRetriedCoalesceDecision(t, ledger)
}

// TestSetOperationCommonTypeRetryPreparedParameterArm is cell (xx)'s
// EXECUTE-SPECIALIZED witness: a prepared union of a retained widened-product
// arm and a parameter arm specializes at EXECUTE through the prepared
// owner's retry under the entry-68 execute generation. The PREPARE snapshot
// keeps the provisional VARCHAR(65535) output; an EXECUTE binding typed
// DECIMAL(65,1) converges the owner's common to the parent's (66,1) with the
// retried decision recorded content-keyed, and the retry-withheld ablation
// (no execute generation) re-arms the pre-existing checked invalid-argument
// family (decision log 73, review note P3: the prepared route's ablation
// differs from the initial route's parse error).
func TestSetOperationCommonTypeRetryPreparedParameterArm(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerSetOperationMockTables(t, mock)
	prepared, err := runOneStmt(mock, t,
		"prepare p30938 from 'select a*b as v from s30938x union all select ? as v'")
	require.NoError(t, err)
	snapshot := prepared.GetDcl().GetPrepare().Plan
	require.NotNil(t, snapshot)
	snapshotNode := unionNode(t, snapshot, pb.Node_UNION_ALL)
	snapshotOut := snapshot.GetQuery().Nodes[snapshotNode.NodeId].ProjectList[0]
	require.Equal(t, int32(types.T_varchar), snapshotOut.Typ.Id,
		"the parameter-carrying snapshot keeps the provisional VARCHAR output")
	require.Equal(t, int32(65535), snapshotOut.Typ.Width)

	binding := ParamValue{Value: "1.0", SourceType: types.New(types.T_decimal256, 65, 1),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal}

	// Two independent executes each form their own execute-scope entries and
	// bind the identical parent common (the cached-route repetition of cell
	// (xx)).
	for repetition := 0; repetition < 2; repetition++ {
		execProc := newPlanTestProcess(t)
		t.Cleanup(execProc.Free)
		release := process.EnterDecimalBoundaryGeneration(execProc)
		filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecializationAndProcess(
			context.Background(), execProc, snapshot, []any{binding}, function.DefaultDivPrecisionIncrement)
		ledger := map[string]any{}
		for key, entry := range execProc.DecimalBoundaryDecisions() {
			ledger[key] = entry
		}
		release()
		require.NoError(t, fillErr)
		require.True(t, specialized)
		filledNode := unionNode(t, filled, pb.Node_UNION_ALL)
		requireDecimalTyp(t, filled.GetQuery().Nodes[filledNode.NodeId].ProjectList[0].Typ, 66, 1,
			"execute %d converges the parent's (66,1)", repetition)
		requireUnionBranchCasts(t, filled, filledNode, 66, 1)
		requireRetriedCoalesceDecision(t, ledger, [][2]int32{{65, 0}, {65, 1}}, [2]int32{66, 1}, [2]int32{66, 1})
	}

	// Retry-withheld ablation: without the execute-time generation, the
	// prepared owner re-arms its pre-existing checked invalid-argument
	// outcome, proving the retry rescues exactly this failure.
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	_, _, fillErr := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, snapshot, []any{binding}, function.DefaultDivPrecisionIncrement)
	require.Error(t, fillErr, "withholding the retry must re-arm the prepared owner's outcome")
	require.Contains(t, fillErr.Error(), "invalid argument function coalesce, bad value [DECIMAL256 DECIMAL256]")
}

// TestSetOperationCommonTypeRetryPreparedCTASParameterArm is the CTAS twin
// of TestSetOperationCommonTypeRetryPreparedParameterArm (issue #30938,
// design decision log 73): the same widened-product arm unioned with a
// parameter arm, PREPAREd as a CREATE TABLE AS statement. The execute fill
// must converge the parent's (66,1) through the same owner retry.
func TestSetOperationCommonTypeRetryPreparedCTASParameterArm(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerSetOperationMockTables(t, mock)
	prepared, err := runOneStmt(mock, t,
		"prepare p30938ctas from 'create table s30938pout as select a*b as v from s30938x union all select ?'")
	require.NoError(t, err)
	snapshot := prepared.GetDcl().GetPrepare().Plan
	require.NotNil(t, snapshot)
	binding := ParamValue{Value: "1.0", SourceType: types.New(types.T_decimal256, 65, 1),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal}
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	release := process.EnterDecimalBoundaryGeneration(execProc)
	filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, snapshot, []any{binding}, function.DefaultDivPrecisionIncrement)
	ledger := map[string]any{}
	for key, entry := range execProc.DecimalBoundaryDecisions() {
		ledger[key] = entry
	}
	release()
	require.NoError(t, fillErr)
	require.True(t, specialized)
	filledNode := ctasUnionNode(t, filled, pb.Node_UNION_ALL)
	requireDecimalTyp(t, filled.GetDdl().GetQuery().Nodes[filledNode.NodeId].ProjectList[0].Typ, 66, 1,
		"the CTAS execute converges the parent's (66,1)")
	requireRetriedCoalesceDecision(t, ledger, [][2]int32{{65, 0}, {65, 1}}, [2]int32{66, 1}, [2]int32{66, 1})
}

// requireVarcharProvisionalTyp asserts the engine-independent provisional
// VARCHAR(65535) snapshot/tag type of a parameter-arm merge that cannot bind
// at PREPARE (issue #30938, design revision 27 cells (xv)-(xx)).
func requireVarcharProvisionalTyp(t *testing.T, typ pb.Type, msgAndArgs ...any) {
	t.Helper()
	message := append([]any{typ.String()}, msgAndArgs...)
	require.Equal(t, int32(types.T_varchar), typ.Id, message...)
	require.Equal(t, int32(65535), typ.Width, message...)
}

// ctasUnionNode returns the single set-operation node of a CTAS DDL plan's
// embedded query.
func ctasUnionNode(t *testing.T, ddlPlan *pb.Plan, nodeType pb.Node_NodeType) *pb.Node {
	t.Helper()
	query := ddlPlan.GetDdl().GetQuery()
	require.NotNil(t, query)
	var found *pb.Node
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == nodeType {
			require.Nil(t, found, "exactly one %s node expected", nodeType)
			found = node
		}
	}
	require.NotNil(t, found, "a %s node is required", nodeType)
	return found
}

// requireLedgerEntry returns the typed decision entry pinned under one exact
// content key: name#argc plus each position's <pre-width:>aligned pair.
func requireLedgerEntry(t *testing.T, ledger map[string]any, key string) *rule.DecimalBoundaryDecisionEntry {
	t.Helper()
	payload, found := ledger[key]
	require.True(t, found, "ledger must hold decision key %s", key)
	entry, ok := payload.(*rule.DecimalBoundaryDecisionEntry)
	require.True(t, ok, "ledger key %s must hold a decision entry", key)
	return entry
}

// requireDerivedDomainKey asserts that the ledger holds one exact
// binder-channel key recorded by the addBinding lineage walk: a typed
// derived-domain entry carrying the parent's published domain and widened
// provenance. These b#<rootTag>#<col> markers are PREPARE-time retained
// evidence the execute-time resolution reuses read-only.
func requireDerivedDomainKey(t *testing.T, ledger map[string]any, key string, width, scale int32) {
	t.Helper()
	payload, found := ledger[key]
	require.True(t, found, "ledger must hold binder-channel key %s", key)
	domain, ok := payload.(*rule.DecimalBoundaryDerivedDomain)
	require.True(t, ok, "ledger key %s must hold a derived-domain entry", key)
	require.True(t, domain.Provenance, "ledger key %s must carry widened-multiplication provenance", key)
	requireDecimalTyp(t, domain.Parent, width, scale)
}

// requireLedgerKeys asserts that the ledger holds exactly the pinned content
// keys, no more and no fewer, each carrying a typed decision entry.
func requireLedgerKeys(t *testing.T, ledger map[string]any, keys ...string) {
	t.Helper()
	require.Len(t, ledger, len(keys), "unexpected ledger key inventory")
	for _, key := range keys {
		requireLedgerEntry(t, ledger, key)
	}
}

// requireRetriedBoundaryEntry pins one execute-formed retried merge decision:
// the entry-72/73 retry markers plus the exact published common type.
func requireRetriedBoundaryEntry(t *testing.T, ledger map[string]any, key string, width, scale int32) {
	t.Helper()
	entry := requireLedgerEntry(t, ledger, key)
	require.True(t, entry.Retried, "the widened-arm rebind must be recorded retried")
	require.True(t, entry.Provenance, "the widened-arm rebind must carry widened-multiplication provenance")
	requireDecimalTyp(t, entry.ResultType, width, scale)
}

// refreshBoundaryWithBinding specializes the snapshot through the
// lineage-withheld channel (the plain package fill attaches no execute
// process) and refreshes with the supplied carrier.
func refreshBoundaryWithBinding(t *testing.T, original *pb.Plan, stmt tree.Statement, carrier map[string]any, value ParamValue) pb.Type {
	t.Helper()
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, []any{value})
	require.NoError(t, err)
	require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, carrier, function.DefaultDivPrecisionIncrement))
	return filled.GetDdl().GetCreateTable().TableDef.Cols[0].Typ
}

// requireExactBoundaryFillError asserts the lineage-withheld fill re-arms
// exactly the pinned checked outcome, byte-identically.
func requireExactBoundaryFillError(t *testing.T, original *pb.Plan, value ParamValue, want string) {
	t.Helper()
	_, err := ablatePreparedBoundaryFill(original, value)
	require.Error(t, err)
	require.Equal(t, want, err.Error(), "the lineage-withheld fill must re-arm the original outcome byte-identically")
}

// setOperandTables registers the (38,0)/(38,0) and (35,0) operand tables the
// set-operation cells (xviii)-(xx) build over.
func setOperandTables() map[string]string {
	return map[string]string{
		"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))",
		"t30938c2": "create table t30938c2(a decimal(38,0), b decimal(38,0))",
		"t30938c3": "create table t30938c3(a decimal(35,0), b decimal(35,0))",
	}
}

// TestPreparedCTASDecimalBoundaryValueArmBinding is design revision 27 cell
// (xv): a parameter marker in the coalesce VALUE arm leaves the snapshot at
// the engine-independent provisional VARCHAR(65535); each EXECUTE's rebound
// merge refreshes the parent's per-binding declared tag through the merged
// ledger, the ledger-free envelope diverges exactly where the parent would,
// and binary-NULL / untyped-text bindings leave the snapshot untouched on
// every channel.
func TestPreparedCTASDecimalBoundaryValueArmBinding(t *testing.T) {
	innerSQL := "create table t30938_out as select coalesce(truncate(a*b, 0), ?) as v from t30938"
	original, stmt, retained := prepareDecimalBoundaryFixture(t, innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ,
		"the value-arm merge cannot bind at PREPARE, so the snapshot stays provisional")
	prepareKeys := []string{
		"420ac26p#2|34:64:0>34:64:0|71:0:0>34:64:0",
		"jaivnbbf#2|34:76:12>34:76:12|23:0:0>23:0:0",
		"nodcnpc#2|33:38:10>34:39:10|33:38:10>34:39:10",
	}
	requireLedgerKeys(t, retained, prepareKeys...)

	// EXECUTE ?=1.25 typed DECIMAL(20,6): the rebound merge converges (70,6);
	// the widened truncate envelope alone re-derives the parent's (59,6), so
	// both ledger channels persist it.
	binding125 := decimal128Binding("1.25", 20, 6)
	filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding125)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 70, 6, "the rebound merge converges (70,6)")
	requireDecimalTyp(t, tag, 59, 6)
	requireLedgerKeys(t, merged, append(append([]string{}, prepareKeys...),
		"420ac26p#2|34:64:0>34:70:6|33:20:6>34:70:6")...)
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding125), 59, 6,
		"the widened (76,12) truncate envelope re-derives (59,6) without lineage")

	// EXECUTE ?=0.5 typed DECIMAL(20,1): the merged ledger persists the
	// parent's (54,1) where the envelope-only channel reconstructs (65,1).
	binding05 := decimal128Binding("0.5", 20, 1)
	filled, merged, tag = executePreparedBoundaryBinding(t, original, stmt, retained, binding05)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 65, 1, "the rebound merge converges (65,1)")
	requireDecimalTyp(t, tag, 54, 1)
	requireLedgerKeys(t, merged, append(append([]string{}, prepareKeys...),
		"420ac26p#2|34:64:0>34:65:1|33:20:1>34:65:1")...)
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding05), 65, 1,
		"without the retained ledger the declared envelope persists, a genuinely different value")

	// Binary-NULL and untyped-text bindings bind no decimal domain: the
	// snapshot tag survives the refresh on both ledger channels.
	nullBinding := ParamValue{Value: nil, IsBinaryProtocol: true}
	_, _, tag = executePreparedBoundaryBinding(t, original, stmt, retained, nullBinding)
	requireVarcharProvisionalTyp(t, tag, "a binary-NULL binding never respecializes the snapshot")
	requireVarcharProvisionalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, nullBinding))
	textBinding := ParamValue{Value: "0.5", SourceType: types.New(types.T_varchar, 65535, 0),
		HasSourceType: true, PrepareParamKind: vector.PrepareParamNone}
	_, _, tag = executePreparedBoundaryBinding(t, original, stmt, retained, textBinding)
	requireVarcharProvisionalTyp(t, tag, "an untyped text binding never respecializes the snapshot")
	requireVarcharProvisionalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, textBinding))
}

// TestPreparedCTASDecimalBoundaryAggregateArmSetRetry is design revision 27
// cell (xvi): the rebound coalesce references an aggregate producer through
// the plan-producer lineage view, so the execute-time rebind retries the
// failed merge over the parent-published (65,0) aggregate domain and the
// snapshot's authored-cast tag persists.
func TestPreparedCTASDecimalBoundaryAggregateArmSetRetry(t *testing.T) {
	innerSQL := "create table t30938_out as select cast(coalesce(max(a*b), ?) as decimal(65,1)) as v from t30938c1"
	tables := map[string]string{"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))"}
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, tables, innerSQL)

	requireDecimalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 65, 1,
		"the authored cast pins the snapshot tag")
	requireDecimalTyp(t, ddlPlanHeadExpr(t, original).Typ, 65, 1, "the authored cast pins the snapshot head")
	requireLedgerKeys(t, retained,
		"420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0",
		"7ib8fim8#1|34:76:0>34:76:0",
		"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")

	binding := decimal128Binding("0.5", 20, 1)
	filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 65, 1,
		"parameter specialization does not respecialize the authored-cast head")
	requireDecimalTyp(t, tag, 65, 1, "the snapshot's authored-cast tag persists")
	requireRetriedBoundaryEntry(t, merged,
		"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// TestPreparedCTASDecimalBoundaryAggregateArmPinnedTwin is cell (xvi)'s
// authored-cast twin: the pinned (65,0) arm keeps the merge first-pass
// convergent, fills on every channel, and persists its own envelope.
func TestPreparedCTASDecimalBoundaryAggregateArmPinnedTwin(t *testing.T) {
	innerSQL := "create table t30938_out as select cast(coalesce(cast(max(a*b) as decimal(65,0)), ?) as decimal(65,1)) as v from t30938c1"
	tables := map[string]string{"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))"}
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, tables, innerSQL)

	requireDecimalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ, 65, 1)
	// The merge's first arm is the SQL-authored fixed-domain cast
	// CAST(MAX(a*b) AS DECIMAL(65,0)): per decision log 52's content keying as
	// repaired in review round 17 (F1), an authored cast contributes its own
	// declared type as the observed pre type (34:65:0), never its operand's
	// unwrapped (76,0); the aligned component stays the arm's published
	// (65,0). The parameter arm keeps its text-leaf observed type through the
	// transparent binder alignment cast.
	requireLedgerKeys(t, retained,
		"420ac26p#2|34:65:0>34:65:0|71:0:0>34:65:0",
		"7ib8fim8#1|34:76:0>34:76:0",
		"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")

	binding := decimal128Binding("0.5", 20, 1)
	_, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 65, 1, "the authored twin persists its envelope with the carrier")
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding), 65, 1,
		"the authored twin re-derives its own envelope without the carrier")
}

// TestPreparedCTASDecimalBoundaryMaterializedSetArmRetry is design revision
// 27 cell (xvii): the value-unstable shared producer c materializes once
// (both join inputs scan the one published step), and the execute-time
// rebind resolves the consumer reference across the scan/step/sink/producer
// chain to the parent's (65,0) product domain.
func TestPreparedCTASDecimalBoundaryMaterializedSetArmRetry(t *testing.T) {
	innerSQL := "create table t30938_out as with c as (select a*b as v, uuid() as r from t30938c1) " +
		"select coalesce(x.v, ?) as w from c x join c y on x.v = y.v"
	tables := map[string]string{"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))"}
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, tables, innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
	require.Len(t, retained, 4, "two typed entries plus the two binder-channel markers")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
	requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
	requireDerivedDomainKey(t, retained, "b#12#0", 65, 0)
	requireDerivedDomainKey(t, retained, "b#14#0", 65, 0)

	query := original.GetDdl().GetQuery()
	require.Equal(t, []int32{2, 8}, query.Steps, "the producer publishes one step beside the query root")
	sinkScans := 0
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == pb.Node_SINK_SCAN {
			sinkScans++
			require.Equal(t, []int32{0}, node.SourceStep, "both consumers read the one materialized step")
		}
	}
	require.Equal(t, 2, sinkScans, "both join inputs scan the materialized producer")

	binding := decimal128Binding("0.5", 20, 1)
	filled, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 66, 1, "the rebound merge converges (66,1)")
	requireDecimalTyp(t, tag, 66, 1)
	requireLiveBoundaryFillErrorReplacedByRetry(t, original, binding)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// requireLiveBoundaryFillErrorReplacedByRetry documents the positive route:
// the live fill succeeds through the retried lineage (the negative re-arm
// contract is asserted separately by requireExactBoundaryFillError).
func requireLiveBoundaryFillErrorReplacedByRetry(t *testing.T, original *pb.Plan, value ParamValue) {
	t.Helper()
	execProc := newPlanTestProcess(t)
	t.Cleanup(execProc.Free)
	release := process.EnterDecimalBoundaryGeneration(execProc)
	_, _, err := FillValuesOfParamsInPlanWithSpecializationAndProcess(
		context.Background(), execProc, original, []any{value}, function.DefaultDivPrecisionIncrement)
	release()
	require.NoError(t, err)
}

// TestPreparedCTASDecimalBoundaryMaterializedSetArmPinnedTwin is cell
// (xvii)'s authored-cast twin: materialization plus an authored (65,0) arm
// keep the merge first-pass convergent on every channel.
func TestPreparedCTASDecimalBoundaryMaterializedSetArmPinnedTwin(t *testing.T) {
	innerSQL := "create table t30938_out as with c as (select cast(a*b as decimal(65,0)) as v, uuid() as r from t30938c1) " +
		"select coalesce(x.v, ?) as w from c x join c y on x.v = y.v"
	tables := map[string]string{"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))"}
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, tables, innerSQL)

	requireLedgerKeys(t, retained,
		"420ac26p#2|34:65:0>34:65:0|71:0:0>34:65:0",
		"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")

	binding := decimal128Binding("0.5", 20, 1)
	_, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 66, 1, "the authored twin persists its envelope with the carrier")
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding), 66, 1,
		"the authored twin re-derives its own envelope without the carrier")
}

// TestPreparedCTASDecimalBoundaryRecursiveBoundedCycle is cell (xvii)'s
// bounded-cycle control: a recursive CTE exporting the widened product
// resolves across its RECURSIVE_CTE/SINK/SINK_SCAN step ring without hanging,
// the execute-time rebind converges (66,1), and the withheld carrier re-arms.
func TestPreparedCTASDecimalBoundaryRecursiveBoundedCycle(t *testing.T) {
	innerSQL := "create table t30938_out as with recursive c(n, v) as " +
		"(select 1, a*b from t30938c1 union all select n+1, v from c where n < 3) " +
		"select coalesce(v, ?) as w from c"
	tables := map[string]string{"t30938c1": "create table t30938c1(a decimal(38,0), b decimal(38,0))"}
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, tables, innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
	require.Len(t, retained, 4, "the recursive fixture publishes two typed entries plus two binder-channel markers")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
	requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
	_, found := retained["b#6#1"]
	require.True(t, found, "the binder-channel marker records the recursive bind")
	requireDerivedDomainKey(t, retained, "b#18#1", 65, 0)

	binding := decimal128Binding("0.5", 20, 1)
	_, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 66, 1)
	requireRetriedBoundaryEntry(t, merged,
		"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
	require.Len(t, merged, 5)
	_, found = merged["b#6#1"]
	require.True(t, found)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// TestPreparedCTASDecimalBoundarySetOperationArmRetry is design revision 27
// cell (xviii): the rebound coalesce references a SET-OPERATION output; the
// execute-time rebind resolves both branch producers through the same
// complete producer-resolution owner that infers declared storage tags, so
// the merge retry binds the parent's (66,1) family common.
func TestPreparedCTASDecimalBoundarySetOperationArmRetry(t *testing.T) {
	innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
		"(select a*b as v from t30938c1 union all select a*b as v from t30938c2) u"
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
	require.Len(t, retained, 3, "two typed entries plus the binder-channel marker")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
	requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
	requireDerivedDomainKey(t, retained, "b#26#0", 65, 0)

	union := ctasUnionNode(t, original, pb.Node_UNION_ALL)
	require.Len(t, union.Children, 2)
	for _, childID := range union.Children {
		branch := original.GetDdl().GetQuery().Nodes[childID]
		require.Equal(t, pb.Node_PROJECT, branch.NodeType, "set branches are PROJECT nodes")
		require.Len(t, branch.ProjectList, 1)
		requireDecimalTyp(t, branch.ProjectList[0].Typ, 76, 0,
			"each widened product branch exports (76,0)")
	}

	binding := decimal128Binding("0.5", 20, 1)
	_, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 66, 1)
	requireRetriedBoundaryEntry(t, merged,
		"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// TestPreparedCTASDecimalBoundarySetOperationArmIntersectVariant routes cell
// (xviii)'s shape through INTERSECT: all six set-operation variants share
// the same owners, so the converge/re-arm oracles hold unchanged.
func TestPreparedCTASDecimalBoundarySetOperationArmIntersectVariant(t *testing.T) {
	innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
		"(select a*b as v from t30938c1 intersect select a*b as v from t30938c2) u"
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
	require.Len(t, retained, 3, "two typed entries plus the binder-channel marker")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
	requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
	requireDerivedDomainKey(t, retained, "b#26#0", 65, 0)
	ctasUnionNode(t, original, pb.Node_INTERSECT)

	binding := decimal128Binding("0.5", 20, 1)
	filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 66, 1, "the rebound merge converges (66,1)")
	requireDecimalTyp(t, tag, 66, 1)
	requireRetriedBoundaryEntry(t, merged,
		"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// TestPreparedCTASDecimalBoundarySetOperationArmPinnedTwin is cell (xviii)'s
// authored-cast twin: both branches pinned to (65,0) keep the merge
// first-pass convergent on every channel.
func TestPreparedCTASDecimalBoundarySetOperationArmPinnedTwin(t *testing.T) {
	innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
		"(select cast(a*b as decimal(65,0)) as v from t30938c1 union all select cast(a*b as decimal(65,0)) as v from t30938c2) u"
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

	requireLedgerKeys(t, retained,
		"420ac26p#2|34:65:0>34:65:0|71:0:0>34:65:0",
		"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")

	binding := decimal128Binding("0.5", 20, 1)
	_, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 66, 1, "the authored twin persists its envelope with the carrier")
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding), 66, 1,
		"the authored twin re-derives its own envelope without the carrier")
}

// TestPreparedCTASDecimalBoundarySetOperationArmGenuineWide is cell
// (xviii)'s genuine-wide control: one branch coalesces the widened product
// with a genuine 66-digit literal, so the parent domain is (66,0) (not the
// widened (76,0)); the retry binds the parent's (67,1) family common and the
// withheld carrier still re-arms.
func TestPreparedCTASDecimalBoundarySetOperationArmGenuineWide(t *testing.T) {
	literal := strings.Repeat("9", 66)
	innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
		"(select a*b as v from t30938c1 union all select coalesce(a*b, " + literal + ") as v from t30938c2) u"
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

	requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
	require.Len(t, retained, 4, "three typed entries plus the binder-channel marker")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|61:66:0>34:66:0")
	requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
	requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
	requireDerivedDomainKey(t, retained, "b#26#0", 66, 0)

	binding := decimal128Binding("0.5", 20, 1)
	filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 67, 1, "the genuine (66,0) arm converges (67,1)")
	requireDecimalTyp(t, tag, 67, 1, "the parent domain (66,0), never the widened (76,0)")
	requireRetriedBoundaryEntry(t, merged,
		"420ac26p#2|34:76:0>34:67:1|33:20:1>34:67:1", 67, 1)
	requireExactBoundaryFillError(t, original, binding,
		"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
}

// TestPreparedCTASDecimalBoundarySetOperationModArmRetry is cell (xxi) (a)'s
// MOD set-operation success witness (design revision 28, F-MOD-PARENT-DOMAIN,
// decision log 74 (vii)), replacing cell (xviii)'s withdrawn out-of-case
// decline oracle: the modulo branch is a supported decimal-producing consumer
// inside a branch, so it resolves through the engine's shared typing rules —
// the `%` branch's parent domain derives through DecimalModReturnType over
// the reconstructed parent operand domains (the product's (65,0) against the
// integer literal's digit domain, yielding the parent's (65,0)), the union
// combines to the parent's (65,0), and the rebound merge binds the parent's
// (66,1), persisting (66,1) with parent-identical values. Rejecting this
// supported shape is a compatibility defect, so the decline family stands
// ONLY as the lineage-withheld ablation reproducing the exact checked fill
// error. The unequal-widths twin routes the modulo branch through the
// engine-owned alignment cast, which the prepared-route ownership predicate
// unwraps before the same MOD resolution.
func TestPreparedCTASDecimalBoundarySetOperationModArmRetry(t *testing.T) {
	for name, tail := range map[string]struct {
		sibling string
		keys    []string
	}{
		"equal-widths": {"t30938c2", []string{
			"420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0",
			"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0",
			"tlgotmo#2|34:76:0>34:76:0|23:0:0>34:76:0",
		}},
		"unequal-widths": {"t30938c3", []string{
			"420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0",
			"nodcnpc#2|33:35:0>34:35:0|33:35:0>34:35:0",
			"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0",
			"tlgotmo#2|34:70:0>34:70:0|23:0:0>34:76:0",
		}},
	} {
		t.Run(name, func(t *testing.T) {
			innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
				"(select a*b as v from t30938c1 union all select (a*b) % 1 as v from " + tail.sibling + ") u"
			original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)
			requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
			require.Len(t, retained, len(tail.keys)+1, "typed entries plus the binder-channel marker")
			for _, key := range tail.keys {
				requireLedgerEntry(t, retained, key)
			}
			requireDerivedDomainKey(t, retained, "b#26#0", 65, 0)

			binding := decimal128Binding("0.5", 20, 1)
			filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
			requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 66, 1,
				"the rebound merge over the MOD branch converges the parent's (66,1)")
			requireDecimalTyp(t, tag, 66, 1,
				"the MOD branch persists the parent's (66,1), never a decline")
			requireRetriedBoundaryEntry(t, merged,
				"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
			requireExactBoundaryFillError(t, original, binding,
				"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
		})
	}
}

// TestPreparedCTASDecimalBoundarySetOperationMixedWidthOrders is design
// revision 27 cell (xix): the engine's own alignment cast wraps the narrower
// branch export to (76,0); the execute-time rebind looks through that
// coercion in BOTH branch orders and converges the parent's (66,1), with the
// recovered per-branch view recorded content-keyed.
func TestPreparedCTASDecimalBoundarySetOperationMixedWidthOrders(t *testing.T) {
	for name, innerSQL := range map[string]string{
		"wide-first": "create table t30938_out as select coalesce(u.v, ?) as w from " +
			"(select a*b as v from t30938c1 union all select a*b as v from t30938c3) u",
		"wide-last": "create table t30938_out as select coalesce(u.v, ?) as w from " +
			"(select a*b as v from t30938c3 union all select a*b as v from t30938c1) u",
	} {
		t.Run(name, func(t *testing.T) {
			original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

			requireVarcharProvisionalTyp(t, original.GetDdl().GetCreateTable().TableDef.Cols[0].Typ)
			require.Len(t, retained, 4, "three typed entries plus the binder-channel marker")
			requireLedgerEntry(t, retained, "420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0")
			requireLedgerEntry(t, retained, "nodcnpc#2|33:35:0>34:35:0|33:35:0>34:35:0")
			requireLedgerEntry(t, retained, "nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")
			requireDerivedDomainKey(t, retained, "b#26#0", 65, 0)

			union := ctasUnionNode(t, original, pb.Node_UNION_ALL)
			require.Len(t, union.Children, 2)
			coerced := 0
			for _, childID := range union.Children {
				branch := original.GetDdl().GetQuery().Nodes[childID]
				require.Equal(t, pb.Node_PROJECT, branch.NodeType)
				require.Len(t, branch.ProjectList, 1)
				export := branch.ProjectList[0]
				requireDecimalTyp(t, export.Typ, 76, 0)
				if fn := export.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" {
					coerced++
					require.False(t, fn.GetSyntaxExplicitCast(), "the alignment cast is engine-owned")
					requireDecimalOverload(t, export, function.CAST)
					requireDecimalTyp(t, fn.Args[0].Typ, 70, 0,
						"the narrower (35,0) product aligns to the common (76,0)")
				}
			}
			require.Equal(t, 1, coerced, "exactly the narrower branch carries the engine alignment cast")

			binding := decimal128Binding("0.5", 20, 1)
			filled, merged, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
			requireDecimalTyp(t, ddlPlanHeadExpr(t, filled).Typ, 66, 1, "the rebound merge converges (66,1)")
			requireDecimalTyp(t, tag, 66, 1)
			requireRetriedBoundaryEntry(t, merged,
				"420ac26p#2|34:76:0>34:66:1|33:20:1>34:66:1", 66, 1)
			requireExactBoundaryFillError(t, original, binding,
				"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]")
		})
	}
}

// TestPreparedCTASDecimalBoundarySetOperationMixedWidthPinnedTwin is cell
// (xix)'s authored-cast twin: SQL-authored casts keep their fixed targets,
// the unwrap declines them, and both ledger channels persist the same
// envelope-derived (66,1).
func TestPreparedCTASDecimalBoundarySetOperationMixedWidthPinnedTwin(t *testing.T) {
	innerSQL := "create table t30938_out as select coalesce(u.v, ?) as w from " +
		"(select cast(a*b as decimal(65,0)) as v from t30938c1 union all select cast(a*b as decimal(65,0)) as v from t30938c3) u"
	original, stmt, retained := prepareDecimalBoundaryFixtureFor(t, setOperandTables(), innerSQL)

	requireLedgerKeys(t, retained,
		"420ac26p#2|34:65:0>34:65:0|71:0:0>34:65:0",
		"nodcnpc#2|33:35:0>34:35:0|33:35:0>34:35:0",
		"nodcnpc#2|33:38:0>34:39:0|33:38:0>34:39:0")

	binding := decimal128Binding("0.5", 20, 1)
	_, _, tag := executePreparedBoundaryBinding(t, original, stmt, retained, binding)
	requireDecimalTyp(t, tag, 66, 1, "the authored twin persists its envelope with the carrier")
	requireDecimalTyp(t, refreshBoundaryWithBinding(t, original, stmt, nil, binding), 66, 1,
		"the authored twin re-derives its own envelope without the carrier")
}

// TestUnwrapPreparedSetOperationCoercionGuardsIssue30938 is cell (xix)'s
// white-box oracle on the branch coercion predicate: the engine's
// same-family alignment cast unwraps to its operand, SQL-authored casts and
// target-type mismatches decline untouched.
func TestUnwrapPreparedSetOperationCoercionGuardsIssue30938(t *testing.T) {
	built, _, err := buildStatementWithOpenLedger(t,
		"select a*b as v from s30938x union all select a*b as v from s30938y")
	require.NoError(t, err)
	query := built.GetQuery()
	union := unionNode(t, built, pb.Node_UNION_ALL)
	coerced := 0
	for _, childID := range union.Children {
		branch := query.Nodes[childID]
		require.Equal(t, pb.Node_PROJECT, branch.NodeType)
		require.Len(t, branch.ProjectList, 1)
		export := branch.ProjectList[0]
		fn := export.GetF()
		if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" {
			requireDecimalTyp(t, export.Typ, 76, 0)
			continue
		}
		coerced++
		require.False(t, fn.GetSyntaxExplicitCast(), "the alignment cast is engine-owned")
		requireDecimalOverload(t, export, function.CAST)
		_, oIndex := function.DecodeOverloadID(fn.Func.GetObj())
		require.Equal(t, int32(0), oIndex, "the engine binds its own cast overload identity")
		requireDecimalTyp(t, export.Typ, 76, 0)
		requireDecimalTyp(t, fn.Args[0].Typ, 70, 0)
		require.Same(t, fn.Args[0], unwrapPreparedSetOperationCoercion(query, childID, 0, export.Typ, export),
			"the unwrap recovers the engine cast's operand")
		mismatched := export.Typ
		mismatched.Width = 65
		require.Same(t, export, unwrapPreparedSetOperationCoercion(query, childID, 0, mismatched, export),
			"a target-type mismatch declines the unwrap")
	}
	require.Equal(t, 1, coerced, "exactly the narrower branch carries the engine alignment cast")

	built, _, err = buildStatementWithOpenLedger(t, "select cast(a*b as decimal(65,0)) as v from s30938x "+
		"union all select cast(a*b as decimal(65,0)) as v from s30938y")
	require.NoError(t, err)
	query = built.GetQuery()
	union = unionNode(t, built, pb.Node_UNION_ALL)
	for _, childID := range union.Children {
		branch := query.Nodes[childID]
		export := branch.ProjectList[0]
		fn := export.GetF()
		require.NotNil(t, fn)
		require.Equal(t, "cast", fn.Func.GetObjName())
		_, oIndex := function.DecodeOverloadID(fn.Func.GetObj())
		require.Equal(t, int32(1), oIndex, "the SQL-authored cast binds its own overload identity")
		requireDecimalTyp(t, export.Typ, 65, 0, "the authored cast keeps its fixed target")
		require.Same(t, export, unwrapPreparedSetOperationCoercion(query, childID, 0, export.Typ, export),
			"authored casts are user semantics, never unwrapped")
	}
}

// registerWindowMockTables installs the fixtures of the issue-30938
// projection-refresh regressions: w30938 carries an integer partition key
// beside the (a, b DECIMAL(38,0)) operands, and tws30938 adds the timestamp
// column the time-window statements bucket on.
func registerWindowMockTables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	wnp, err := buildTestCreateTableStmt(mock,
		"create table w30938(id int primary key, a decimal(38,0), b decimal(38,0))")
	require.NoError(t, err)
	mock.ctxt.tables["w30938"] = wnp
	mock.ctxt.objects["w30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "w30938"}
	tws, err := buildTestCreateTableStmt(mock,
		"create table tws30938(ts timestamp, a decimal(38,0), b decimal(38,0))")
	require.NoError(t, err)
	mock.ctxt.tables["tws30938"] = tws
	mock.ctxt.objects["tws30938"] = &ObjectRef{SchemaName: "tpch", ObjName: "tws30938"}
}

// buildWindowFixtureStatement runs one statement through the public planner
// over the window fixtures, so the initial-build finalization (including the
// execute-time projection refresh of refreshPreparedPlanProjectionTypes)
// exercises the statement exactly as the regression witnesses did.
func buildWindowFixtureStatement(t *testing.T, sql string) (*pb.Plan, error) {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerWindowMockTables(t, mock)
	return runOneStmt(mock, t, sql)
}

// requireWindowOrderKeyIntegrity asserts the physical contract the
// partition/sort compare builders rely on: colexec/partition's
// generateCompares builds the physical comparator from Expr.Typ and runs it
// over the child slot's vector, so every bare-column order key of a
// PARTITION/SORT node must carry the type of the child projection slot its
// coordinates read. appendWindowNode installs the window spec's PartitionBy
// expressions as the PARTITION node's OrderBy entries (the same *plan.Expr
// pointers, remapped partition-child-local), so a refresh resolving those
// expressions under the WINDOW node's own child view re-types the shared
// object with a sibling column's type and the execution-time comparator
// panics over an INT vector built from a decimal type (the win_part_wide
// panic witness). The owner's own refresh (the Node_SORT/Node_PARTITION
// case of refreshPreparedPlanProjectionTypes) re-syncs the shared objects
// under the correct view instead (issue #30938).
func requireWindowOrderKeyIntegrity(t *testing.T, label string, built *pb.Plan) {
	t.Helper()
	query := built.GetQuery()
	require.NotNil(t, query)
	nodes := query.GetNodes()
	for i, node := range nodes {
		switch node.GetNodeType() {
		case pb.Node_PARTITION, pb.Node_SORT:
		default:
			continue
		}
		if len(node.GetChildren()) != 1 {
			continue
		}
		child := nodes[node.GetChildren()[0]]
		for j, orderBy := range node.GetOrderBy() {
			col := orderBy.GetExpr().GetCol()
			if col == nil || col.GetRelPos() != 0 {
				continue
			}
			pos := col.GetColPos()
			require.Less(t, int(pos), len(child.GetProjectList()),
				"%s node=%d order key %d must read its child projection", label, i, j)
			want := child.GetProjectList()[pos].GetTyp()
			got := orderBy.GetExpr().GetTyp()
			require.Equal(t, want.GetId(), got.GetId(),
				"%s node=%d order key %d (%s) id: comparator type must equal the child slot type",
				label, i, j, col.GetName())
			require.Equal(t, want.GetWidth(), got.GetWidth(),
				"%s node=%d order key %d (%s) width", label, i, j, col.GetName())
			require.Equal(t, want.GetScale(), got.GetScale(),
				"%s node=%d order key %d (%s) scale", label, i, j, col.GetName())
		}
	}
	for i, node := range nodes {
		if node.GetNodeType() != pb.Node_WINDOW || len(node.GetChildren()) != 1 {
			continue
		}
		child := nodes[node.GetChildren()[0]]
		if child.GetNodeType() != pb.Node_PARTITION || len(child.GetChildren()) != 1 {
			continue
		}
		// partition_topn's TopN pushdown rewrites the partition node with an
		// explicit projection and rule-authored order keys, then remaps the
		// window spec's PartitionBy expressions through the partition node's
		// output view (query_builder.go's partitionTopNWindowNodes remap), so
		// the shared-pointer convention holds for the regular shape while the
		// TopN shape's spec keys are distinct exprs reading the partition
		// node's own projection slots. Each view is asserted through its
		// owner, so the key type always matches the slot its coordinates read.
		topN := child.GetLimit() != nil && child.GetPartitionByCount() > 0
		view := nodes[child.GetChildren()[0]].GetProjectList()
		if topN {
			view = child.GetProjectList()
		}
		for s, specExpr := range node.GetWinSpecList() {
			spec := specExpr.GetW()
			if spec == nil {
				continue
			}
			for k, key := range spec.GetPartitionBy() {
				col := key.GetCol()
				if col == nil || col.GetRelPos() != 0 {
					continue
				}
				if !topN {
					shared := false
					for _, orderBy := range child.GetOrderBy() {
						if orderBy.GetExpr() == key {
							shared = true
							break
						}
					}
					require.True(t, shared,
						"%s window node=%d spec[%d] partition key %d must be installed as the "+
							"PARTITION node's order key (the shared-pointer convention)", label, i, s, k)
				}
				pos := col.GetColPos()
				require.Less(t, int(pos), len(view),
					"%s window node=%d spec[%d] partition key %d must read its owner's view",
					label, i, s, k)
				want := view[pos].GetTyp()
				got := key.GetTyp()
				require.Equal(t, want.GetId(), got.GetId(),
					"%s window node=%d spec[%d] partition key %d (%s) id: the key must carry "+
						"the type of the slot its coordinates read", label, i, s, k, col.GetName())
				require.Equal(t, want.GetWidth(), got.GetWidth(),
					"%s window node=%d spec[%d] partition key %d (%s) width", label, i, s, k, col.GetName())
				require.Equal(t, want.GetScale(), got.GetScale(),
					"%s window node=%d spec[%d] partition key %d (%s) scale", label, i, s, k, col.GetName())
			}
		}
	}
}

// TestWindowSpecSharedPartitionKeyIntegrityIssue30938 pins the
// shared-partition-key contract across the discriminating window forms: the
// panic witness MAX(a*b) OVER (PARTITION BY id) carried its integer key as
// {33,38,0} after the WINDOW-view refresh overwrote the shared object with a
// decimal sibling's type, the AVG form exercises the envelope re-sync
// ({34,65,4} -> {34,65,0}), the ORDER BY forms cover the sort keys, and the
// TopN forms keep the limit-pushdown shape (PARTITION node carrying the
// limit and one partition key) with the same integrity contract.
func TestWindowSpecSharedPartitionKeyIntegrityIssue30938(t *testing.T) {
	forms := []struct {
		label string
		sql   string
		topN  bool
	}{
		{"panicWitness", "select max(a * b) over (partition by id) as mx from w30938", false},
		{"controlNoMultiply", "select max(a) over (partition by id) as mx from w30938", false},
		{"avgEnvelope", "select avg(a * b) over (partition by id) as av from w30938", false},
		{"sumOrder", "select sum(a * b) over (partition by id order by a) as sm from w30938", false},
		{"rowNumberOrder", "select row_number() over (partition by id order by a) as rn from w30938", false},
		{"multiKey", "select max(a * b) over (partition by id, a) as mx from w30938", false},
		{"exprKey", "select max(a * b) over (partition by id + 1) as mx from w30938", false},
		{"castKey", "select max(a * b) over (partition by cast(id as decimal(10,0))) as mx from w30938", false},
		{"topNFilter",
			"select * from (select row_number() over (partition by id order by a) as rn, b " +
				"from w30938) q where rn <= 2", true},
		{"topNLimit",
			"select rn from (select row_number() over (partition by id order by a) as rn " +
				"from w30938) q where rn <= 3", true},
	}
	for _, form := range forms {
		t.Run(form.label, func(t *testing.T) {
			built, err := buildWindowFixtureStatement(t, form.sql)
			require.NoError(t, err)
			requireWindowOrderKeyIntegrity(t, form.label, built)
			if form.topN {
				var partition *pb.Node
				for _, node := range built.GetQuery().GetNodes() {
					if node.GetNodeType() == pb.Node_PARTITION {
						require.Nil(t, partition, "exactly one PARTITION node expected")
						partition = node
					}
				}
				require.NotNil(t, partition)
				require.NotNil(t, partition.GetLimit(),
					"the TopN window plan keeps its limit pushdown on the PARTITION node")
				require.Equal(t, int32(1), partition.GetPartitionByCount(),
					"the TopN partition pass keeps exactly one partition key")
			}
		})
	}
}

// TestTimeWindowInternalFormRefreshRebindIssue30938 pins the internal-form
// convention the time-window functions share with tmrank: resetDateFunction
// saves mo_win_truncate's three-argument (value, count, unit) form into the
// plan, and the rebind paths (prepared re-binding and the projection
// refresh, both allowInternalFunctionArgs) re-resolve that saved form with
// refreshed types. Rejecting the internal form's arity there failed the
// f6_tw_fill statement at initial-build finalization with "truncate function
// need two args" once the timestamp column's cached scale refreshed to the
// producer's truth (base_binder.go's mo_win_truncate case). Both statements
// must build and persist the parent's envelope-derived tag: the widened
// product's MAX keeps the expression metadata widened, while the CTAS
// storage boundary persists DECIMAL(65,0) exactly as the parent does.
func TestTimeWindowInternalFormRefreshRebindIssue30938(t *testing.T) {
	forms := []struct {
		label string
		sql   string
	}{
		{"intervalOnly",
			"create table twfit30938 as select max(a * b) as p from tws30938 interval(ts, 1, minute)"},
		{"slidingFill",
			"create table twfill30938 as select max(a * b) as p from tws30938 " +
				"where ts > '2023-12-31 23:55:00' and ts < '2024-01-01 00:10:00' " +
				"interval(ts, 2, minute) sliding(1, minute) fill(prev)"},
	}
	for _, form := range forms {
		t.Run(form.label, func(t *testing.T) {
			built, err := buildWindowFixtureStatement(t, form.sql)
			require.NoError(t, err,
				"the saved three-argument internal form must re-resolve under the refresh")
			var pCol *pb.ColDef
			for _, col := range built.GetDdl().GetCreateTable().GetTableDef().GetCols() {
				if col.GetHidden() {
					continue
				}
				require.Nil(t, pCol, "exactly one visible column expected")
				pCol = col
			}
			require.NotNil(t, pCol)
			require.Equal(t, "p", pCol.GetName())
			requireDecimalTyp(t, pCol.GetTyp(), 65, 0,
				"the time-window aggregate persists the parent's storage-envelope tag")
		})
	}
}
