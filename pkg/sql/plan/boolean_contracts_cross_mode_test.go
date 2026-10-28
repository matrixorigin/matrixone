// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// nativeModeOptimizer builds the enum/SET mock planner around a session whose
// MatrixOneNativeMode flag is set, mirroring a native-mode connection: the
// BindContext and ddlExpressionContext carriers both resolve the D11 scalar
// from this process exactly like production sessions.
func nativeModeOptimizer(t *testing.T) (*MockOptimizer, *process.Process) {
	t.Helper()
	proc := newPlanTestProcess(t)
	proc.GetSessionInfo().MatrixOneNativeMode = true
	setPlanTestGlobalVariable(t, proc.GetService(), moruntime.InternalSQLExecutor,
		executor.NewMemExecutor(func(string) (executor.Result, error) {
			return executor.Result{}, nil
		}))
	mock := newMySQLSpecialOrderMock(t)
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	return mock, proc
}

// mysqlModeOptimizer is the default MySQL-mode connection control.
func mysqlModeOptimizer(t *testing.T) (*MockOptimizer, *process.Process) {
	t.Helper()
	proc := newPlanTestProcess(t)
	setPlanTestGlobalVariable(t, proc.GetService(), moruntime.InternalSQLExecutor,
		executor.NewMemExecutor(func(string) (executor.Result, error) {
			return executor.Result{}, nil
		}))
	mock := newMySQLSpecialOrderMock(t)
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	return mock, proc
}

// TestNativeModeSelectKeepsDisplayLabelTruthPath pins the native-mode parity
// of the SELECT binding path after the review repair: with the recovery gated
// off, the display-wrapped operand keeps the pre-contract shape (the label is
// what a boolean context parses), which is exactly what base bound and
// executed. The raw ENUM storage must not surface on a native-mode statement.
func TestNativeModeSelectKeepsDisplayLabelTruthPath(t *testing.T) {
	mock, _ := nativeModeOptimizer(t)

	// IF over the display label binds and keeps the label-typed condition.
	p, err := runOneStmt(mock, t, "select if(e, 1, 0) from enum_order_t")
	require.NoError(t, err)
	cond, ok := iffCondition(p)
	require.True(t, ok)
	require.NotEqual(t, int32(types.T_enum), cond.Typ.Id,
		"native mode keeps the display-typed condition exactly as base")

	// A bare WHERE predicate over the display label binds the base cast: the
	// BOOL cast source is the display type, never the raw ENUM storage.
	p, err = runOneStmt(mock, t, "select id from enum_order_t where e")
	require.NoError(t, err)
	source, ok := firstFilterBoolCastSource(p)
	require.True(t, ok)
	require.NotEqual(t, int32(types.T_enum), source.Id,
		"native mode keeps the display-label predicate source exactly as base")

	// CASE WHEN over the display label binds (the legacy string admission)
	// instead of rejecting as though a raw ENUM condition had been offered.
	p, err = runOneStmt(mock, t, "select case when e then 1 else 0 end from enum_order_t")
	require.NoError(t, err)
	require.NotNil(t, findPlanFunctionExpr(p, moEnumCastIndexToValueFun),
		"the display wrapper survives on a native-mode statement")

	// MySQL mode keeps the repaired contract on the same statements: the raw
	// ENUM storage decides truth.
	mysql := newMySQLSpecialOrderMock(t)
	p, err = runOneStmt(mysql, t, "select id from enum_order_t where e")
	require.NoError(t, err)
	source, ok = firstFilterBoolCastSource(p)
	require.True(t, ok)
	require.Equal(t, int32(types.T_enum), source.Id,
		"MySQL mode still keeps the storage-truth predicate after the review repair")
}

// buildOneGeneratedExpr resolves the FIRST generated column of the given DDL
// through the COMPLETE production path: parse, plan build, the generated-
// column binder, both authoring-gate applications and the assignment wrapper
// ordering review finding 6 repaired.
func buildOneGeneratedExpr(
	t *testing.T,
	opt *MockOptimizer,
	sql string,
) (*plan.GeneratedCol, error) {
	t.Helper()
	p, err := buildSingleStmt(opt, t, sql)
	if err != nil {
		return nil, err
	}
	createTable := p.GetDdl().GetCreateTable()
	if createTable == nil {
		return nil, fmt.Errorf("expected a CREATE TABLE ddl plan for %q", sql)
	}
	for _, col := range createTable.GetTableDef().Cols {
		if col.GeneratedCol != nil {
			return col.GeneratedCol, nil
		}
	}
	return nil, fmt.Errorf("no generated column was authored for %q", sql)
}

// TestNativeModeDDLBooleanParity proves the generated-column binder keeps the
// pre-contract admission on native-mode sessions for every raw-ENUM boolean
// shape the contract added - the explicit BOOL-target cast, CASE WHEN, AND,
// NOT and IS TRUE - while the same definitions author successfully on a
// MySQL-mode session and persist the storage-truth contract.
func TestNativeModeDDLBooleanParity(t *testing.T) {
	nativeOpt, _ := nativeModeOptimizer(t)
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"assignment-bool-target", "create table tg (e enum('a','b'), g bool generated always as (e))"},
		{"case-when", "create table tg (e enum('a','b'), g int generated always as (case when e then 1 else 0 end))"},
		{"and-operator", "create table tg (e enum('a','b'), g int generated always as (e and 1))"},
		{"not-operator", "create table tg (e enum('a','b'), g int generated always as (not e))"},
		{"is-true", "create table tg (e enum('a','b'), g int generated always as (e is true))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildOneGeneratedExpr(t, nativeOpt, tc.sql)
			require.Error(t, err, tc.sql,
				"a native-mode session keeps the pre-contract DDL admission for a raw ENUM boolean operand")

			mysqlOpt, _ := mysqlModeOptimizer(t)
			generated, err := buildOneGeneratedExpr(t, mysqlOpt, tc.sql)
			require.NoError(t, err, tc.sql,
				"a MySQL-mode session authors the same definition through the real wrapper path")
			require.NotNil(t, generated)
			features, walkErr := planpkgRequiredRemoteFeatures(generated.Expr)
			require.NoError(t, walkErr, tc.sql)
			require.True(t, features.JSONBooleanConditionContracts, tc.sql,
				"the persisted expression carries the conditional boolean contract for the feature gates")
		})
	}
}

// TestGeneratedColumnAssignmentCastRaisesPersistedFloor pins the review
// finding about the authoring-gate ordering: for g BOOL AS (e) the RAW
// expression is only an ENUM reference, so the pre-wrapper authoring check
// computes no requirement; the assignment wrapper is what binds the new
// ENUM->BOOL cast. The final stored expression must be validated too, so a
// below-floor session cannot author the definition through the wrapper, while
// the plain numeric control keeps authoring below the floor.
func TestGeneratedColumnAssignmentCastRaisesPersistedFloor(t *testing.T) {
	opt, proc := mysqlModeOptimizer(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	// Below the minted floor in every dimension, the assignment wrapper must
	// refuse BEFORE anything is persisted.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion131))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion131))
	_, err := buildOneGeneratedExpr(t, opt,
		"create table tg (e enum('a','b'), g bool generated always as (e))")
	require.Error(t, err,
		"the wrapped ENUM->BOOL cast must not be authored below the minted floor")
	require.ErrorContains(t, err, "protocol version 135")

	// The plain numeric control raises no requirement: the raw-ENUM ordinal
	// cast is the OLD contract, so it keeps authoring even at the old floor.
	numericGenerated, err := buildOneGeneratedExpr(t, opt,
		"create table tg (e enum('a','b'), g int generated always as (e + 0))")
	require.NoError(t, err,
		"the unchanged numeric generated-column contract keeps authoring at the old floor")
	require.NotNil(t, numericGenerated)
	features, walkErr := planpkgRequiredRemoteFeatures(numericGenerated.Expr)
	require.NoError(t, walkErr)
	require.False(t, features.JSONBooleanConditionContracts,
		"no conditional boolean contract rides the numeric control")

	// At or above the minted floor, the same BOOL-target definition authors
	// and persists exactly the wrapped contract node.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion135))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion135))
	generated, err := buildOneGeneratedExpr(t, opt,
		"create table tg (e enum('a','b'), g bool generated always as (e))")
	require.NoError(t, err)
	require.NotNil(t, generated)
	features, walkErr = planpkgRequiredRemoteFeatures(generated.Expr)
	require.NoError(t, walkErr)
	require.True(t, features.JSONBooleanConditionContracts)
	version, versionErr := RequiredPersistedExpressionProtocolVersion(generated.Expr)
	require.NoError(t, versionErr)
	require.Equal(t, int64(defines.MORPCVersion135), version)
}

// TestCrossModeConsumptionGateRequiresMySQLMode pins the same-version
// cross-mode boundary of an already-bound expression: the persisted JSON
// conditional (and the ENUM storage-truth cast) evaluate only on a MySQL-mode
// session, so a native-mode session cannot rebind or execute the catalog
// copy; the protocol dimension stays insufficient at the same binary version.
func TestCrossModeConsumptionGateRequiresMySQLMode(t *testing.T) {
	jsonColumn := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_json)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	cased, err := BindFuncExprImplByPlanExpr(context.Background(), "case", []*plan.Expr{
		jsonColumn,
		MakePlan2Int64ConstExprWithType(1),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)

	mysqlProc := newPlanTestProcess(t)
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, cased),
		"a MySQL-mode session evaluates the authored expression")

	nativeProc := newPlanTestProcess(t)
	nativeProc.GetSessionInfo().MatrixOneNativeMode = true
	err = RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, cased)
	require.Error(t, err,
		"a native-mode session must fail closed on the same-version cross-mode rebind")
	require.ErrorContains(t, err, "native-mode session")

	// A plain numeric expression never carries the contracts and stays
	// consumable in both session modes.
	numeric := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	numericIf, err := BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{
		numeric,
		MakePlan2Int64ConstExprWithType(1),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, numericIf))
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, numericIf))
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, nil))

	// The ENUM storage-truth cast is covered by the same contract surface.
	enumStorage := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_enum), Enumvalues: "low,mid,high"},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	enumCast, err := makePlan2CastExpr(context.Background(), enumStorage, plan.Type{Id: int32(types.T_bool)})
	require.NoError(t, err)
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, enumCast))
	err = RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, enumCast)
	require.Error(t, err)
	require.ErrorContains(t, err, "native-mode session")

	// SET storage conditions are exempt from the conditional boolean
	// contracts (design revision 8, finding F8): the persisted binder binds
	// the raw uint64 storage type and the pre-existing numeric kernels
	// evaluate them identically in every receiver and session mode, so they
	// stay consumable in BOTH session modes - a native refusal here would lock
	// native sessions out of SET persisted definitions base already serves.
	setStorage := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_uint64), Enumvalues: "'a','b'"},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	setIf, err := BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{
		setStorage,
		MakePlan2Int64ConstExprWithType(1),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, setIf),
		"a native-mode session keeps consuming SET IF conditions")
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, setIf))
	setCast, err := makePlan2CastExpr(context.Background(), setStorage, plan.Type{Id: int32(types.T_bool)})
	require.NoError(t, err)
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, setCast),
		"a native-mode session keeps consuming CAST(SET AS BOOL)")
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, setCast))

	// Whole-TableDef owner carrying only a SET persisted expression: the
	// shared walk must not report the contract anywhere in the TableDef, so a
	// native scan binding of an existing catalog definition stays open.
	setTableDef := &plan.TableDef{Cols: []*plan.ColDef{{Default: &plan.Default{Expr: setIf}}}}
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, setTableDef))
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, setTableDef))
}

// TestGroupingSetArmsActivateDisplayAndStorageJointly pins the D6 joint
// activation at the planner seam (review repair, finding 1): the marker that
// activates a grouping-set arm's visible ENUM key must activate the hidden
// storage sibling together, otherwise the rollup replacement nulls the
// storage vector on every arm and grouped boolean truth degrades into the
// synthetic reading. The (e)-shape arm therefore carries [true,true] flags -
// pre-fix this arm kept a flag-false sibling - and the arm without the ENUM
// key keeps both slots inactive.
func TestGroupingSetArmsActivateDisplayAndStorageJointly(t *testing.T) {
	p, err := runOneStmt(newMySQLSpecialOrderMock(t), t,
		"select e, if(e, 1, 0) as c from enum_empty_label_t group by grouping sets ((e), ())")
	require.NoError(t, err)
	joint, synthetic := false, false
	for _, node := range p.GetQuery().GetNodes() {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		if len(node.GroupingFlag) != 2 {
			continue
		}
		if node.GroupingFlag[0] && node.GroupingFlag[1] {
			joint = true
		}
		if !node.GroupingFlag[0] && !node.GroupingFlag[1] {
			synthetic = true
		}
	}
	require.True(t, joint,
		"an arm with a visible ENUM key must activate its hidden storage sibling together")
	require.True(t, synthetic,
		"the arm without the ENUM key keeps both the display and storage slots inactive")
}

// planpkgRequiredRemoteFeatures is the shared feature walk the persisted
// gates use; the alias keeps the call sites close to the production seam.
func planpkgRequiredRemoteFeatures(expr any) (plan.RemoteExpressionFeatures, error) {
	return plan.RequiredRemoteExpressionFeatures(expr)
}

// registerBoolFlagTable installs the tiny BOOL-column table the native-mode
// DML assignment tests update on a mode-specific optimizer. The primary key
// is required by the UPDATE binder's constraint walk, and the trailing
// Row_ID column matches every update-capable catalog shape the UPDATE binder
// sizes its final insert-columns against.
func registerBoolFlagTable(t *testing.T, opt *MockOptimizer) {
	t.Helper()
	opt.ctxt.objects["bool_flag_t"] = &plan.ObjectRef{Obj: 9100, ObjName: "bool_flag_t"}
	opt.ctxt.tables["bool_flag_t"] = &plan.TableDef{
		TblId: 9100,
		Name:  "bool_flag_t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "flag", Typ: plan.Type{Id: int32(types.T_bool)}},
			{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid), Width: 16}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
}

// TestNativeModeDMLAssignmentCastKeepsBaseRejection pins the r6 finding: the
// shared DML assignment-cast construction resolves through the builder
// context, and that context must carry the statement's D11 scalar. A
// native-mode UPDATE / ODKU assigning a BIT literal to a BOOL column keeps
// the pre-contract base plan: the raw payload stays a binary literal under a
// pending BOOL cast (base executes that cast on the raw byte and rejects the
// statement at runtime), with no numeric-truth fold - while the
// MySQL-compatible session folds the literal to its numeric truth. The
// binding itself succeeds on both modes; the base runtime rejection of the
// native shape is proven by the executed BVT cells (native UPDATE / ODKU
// error with the connection kept alive), not by the plan-level mock.
func TestNativeModeDMLAssignmentCastKeepsBaseRejection(t *testing.T) {
	t.Run("native update keeps the base assignment shape", func(t *testing.T) {
		opt, _ := nativeModeOptimizer(t)
		registerBoolFlagTable(t, opt)
		p, err := runOneStmt(opt, t, "update bool_flag_t set flag = b'1' where id = 1")
		require.NoError(t, err,
			"the raw assignment shape binds exactly as base binds it; only its runtime execution rejects the raw payload")
		require.False(t, planContainsTrueBoolConst(p),
			"native mode must not fold the BIT-literal assignment; the raw payload reaches the pending BOOL cast")
		require.True(t, planContainsBinaryLiteralBoolCast(p),
			"the native assignment must keep base's pending cast of the raw binary-introducer literal to BOOL")
	})
	t.Run("native odku keeps the base assignment shape", func(t *testing.T) {
		opt, _ := nativeModeOptimizer(t)
		registerBoolFlagTable(t, opt)
		p, err := runOneStmt(opt, t,
			"insert into bool_flag_t (id, flag) values (1, false) "+
				"on duplicate key update flag = b'1'")
		require.NoError(t, err,
			"the raw ON DUPLICATE KEY UPDATE assignment shape binds exactly as base binds it")
		arms := odkuBoolUpdateArms(p)
		require.Len(t, arms, 1,
			"the single flag assignment must arm the modern ODKU dedup join exactly once")
		// The plan-wide BOOL constants are ODKU dedup markers, not the folded
		// literal; the assignment's actual arm is the discriminator.
		require.Nil(t, arms[0].GetLit(),
			"native mode must not fold the ODKU BIT-literal assignment to a constant")
		require.True(t, isBinaryLiteralBoolCast(arms[0]),
			"the native ODKU arm must keep base's pending cast of the raw binary-introducer literal to BOOL")
	})
	t.Run("mysql update folds the literal", func(t *testing.T) {
		opt, _ := mysqlModeOptimizer(t)
		registerBoolFlagTable(t, opt)
		p, err := runOneStmt(opt, t, "update bool_flag_t set flag = b'1' where id = 1")
		require.NoError(t, err,
			"the MySQL-compatible session keeps the numeric-truth assignment fold")
		require.True(t, planContainsTrueBoolConst(p),
			"the folded assignment must project the numeric truth of b'1' as a BOOL constant")
	})
	t.Run("mysql odku folds the literal", func(t *testing.T) {
		opt, _ := mysqlModeOptimizer(t)
		registerBoolFlagTable(t, opt)
		p, err := runOneStmt(opt, t,
			"insert into bool_flag_t (id, flag) values (1, false) "+
				"on duplicate key update flag = b'1'")
		require.NoError(t, err,
			"the MySQL-compatible session keeps the numeric-truth assignment fold for ODKU")
		arms := odkuBoolUpdateArms(p)
		require.Len(t, arms, 1)
		require.NotNil(t, arms[0].GetLit(),
			"the MySQL-compatible session folds the ODKU assignment to its numeric truth")
		require.True(t, arms[0].GetLit().GetBval(),
			"the folded ODKU arm must be the numeric truth of b'1': true")
	})
}

// planContainsTrueBoolConst reports whether any projected expression tree
// carries the folded true constant (the assignment's numeric-truth result of
// b'1').
func planContainsTrueBoolConst(p *Plan) bool {
	var found bool
	var walk func(expr *plan.Expr)
	walk = func(expr *plan.Expr) {
		if expr == nil || found {
			return
		}
		if lit := expr.GetLit(); lit != nil && !lit.GetIsnull() {
			if expr.Typ.Id == int32(types.T_bool) && lit.GetBval() {
				found = true
				return
			}
		}
		for _, child := range expr.GetF().GetArgs() {
			walk(child)
		}
	}
	if query := p.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			for _, expr := range node.ProjectList {
				walk(expr)
			}
			for _, expr := range node.OnUpdateExprs {
				walk(expr)
			}
			for _, expr := range node.FilterList {
				walk(expr)
			}
		}
	}
	return found
}

// planContainsBinaryLiteralBoolCast reports whether the plan carries base's
// pre-contract assignment shape: a pending cast to BOOL whose operand is the
// raw binary-introducer literal payload (a T_varchar constant marked as a
// BIT/HEX literal). Base binds exactly this shape for BOOL-target BIT-literal
// assignments and rejects it at runtime; the native-mode statement must keep
// it instead of the folded BOOL constant.
func planContainsBinaryLiteralBoolCast(p *Plan) bool {
	var found bool
	var walk func(expr *plan.Expr)
	walk = func(expr *plan.Expr) {
		if expr == nil || found {
			return
		}
		if isBinaryLiteralBoolCast(expr) {
			found = true
			return
		}
		for _, child := range expr.GetF().GetArgs() {
			walk(child)
		}
	}
	if query := p.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			for _, expr := range node.ProjectList {
				walk(expr)
			}
			for _, expr := range node.OnUpdateExprs {
				walk(expr)
			}
			for _, expr := range node.FilterList {
				walk(expr)
			}
			for _, expr := range node.OnList {
				walk(expr)
			}
		}
	}
	return found
}

// isBinaryLiteralBoolCast reports whether expr is a pending BOOL cast whose
// operand is the raw binary-introducer literal payload - the assignment shape
// base binds and executes.
func isBinaryLiteralBoolCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.GetFunc().GetObjName() != "cast" || len(fn.Args) != 2 ||
		fn.Args[1].Typ.Id != int32(types.T_bool) {
		return false
	}
	source := fn.Args[0]
	if source.Typ.Id != int32(types.T_varchar) {
		return false
	}
	lit := source.GetLit()
	return lit != nil && lit.GetIsBin()
}

// odkuBoolUpdateArms returns the BOOL-typed update arms the modern ODKU
// dedup join carries in its context - the actual ON DUPLICATE KEY UPDATE
// assignment expressions. The plan-wide BOOL constants are dedup markers, so
// the arm is the only reliable fold discriminator.
func odkuBoolUpdateArms(p *Plan) []*plan.Expr {
	var arms []*plan.Expr
	if query := p.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			if dj := node.GetDedupJoinCtx(); dj != nil {
				for _, expr := range dj.GetUpdateColExprList() {
					if expr != nil && expr.Typ.Id == int32(types.T_bool) {
						arms = append(arms, expr)
					}
				}
			}
		}
	}
	return arms
}

// TestNativeModeExplicitBoolCastKeepsDisplayLabelTruth pins the G1 native
// control: without the contract admission the explicit CAST ... AS BOOL keeps
// the pre-contract base bind - the pending cast converts the projected
// display label, with no storage recovery and no zone fail-closure (the
// ambiguous declaration also binds, exactly as base does).
func TestNativeModeExplicitBoolCastKeepsDisplayLabelTruth(t *testing.T) {
	opt, _ := nativeModeOptimizer(t)
	p, err := runOneStmt(opt,
		t, "select cast(v as bool) from (select e as v from enum_order_t group by e) d")
	require.NoError(t, err,
		"native mode keeps the base display bind for the explicit BOOL cast")
	source, ok := firstPlanBoolCastSource(p)
	require.True(t, ok, "the pending BOOL cast still binds a source")
	require.Equal(t, int32(types.T_varchar), source.Id,
		"the cast source stays the projected display label, not ENUM storage")

	_, err = runOneStmt(opt,
		t, "select cast(v as bool) from (select e as v from enum_empty_label_t group by e) d")
	require.NoError(t, err,
		"native mode raises no zone fail-closure for the explicit BOOL cast")
}
