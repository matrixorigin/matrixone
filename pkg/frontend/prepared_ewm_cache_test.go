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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// ewmWindowPlan wraps one ewm function expression in the real plan shape:
// a WinSpecList expression whose window wrapper carries the call.
func ewmWindowPlan(call *plan.Expr) *plan.Plan {
	return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{WinSpecList: []*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_W{W: &plan.WindowSpec{WindowFunc: call}},
		}}}},
	}}}
}

func ewmCall(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name},
			Args: args,
		}},
	}
}

func ewmColumn(t *testing.T) *plan.Expr {
	t.Helper()
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
}

func ewmParam(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
	}
}

// TestShouldCachePrepareCompileRejectsEwmParameter pins the §10 prepared
// cache gate: the ewm decay and policy positions compile into the
// immutable per-execution configuration bytes, so a prepared marker there
// must rebuild the configuration at every EXECUTE — the statement is not
// a prepare-compile-cache candidate. A marker confined to the value
// column is configuration-independent and stays cacheable.
func TestShouldCachePrepareCompileRejectsEwmParameter(t *testing.T) {
	// the real plan shape: the prepared decay marker inside the window
	prepared := ewmWindowPlan(ewmCall(plan2.NameEwmMean, ewmColumn(t), ewmParam(0)))
	require.True(t, plan2.PreparedPlanHasEwmParams(prepared))
	require.False(t, shouldCachePrepareCompile(prepared))

	// a policy-position marker gates the same way
	prepared = ewmWindowPlan(ewmCall(plan2.NameEwmMean, ewmColumn(t),
		plan2.MakePlan2Float64ConstExprWithType(0.5), ewmParam(1)))
	require.True(t, plan2.PreparedPlanHasEwmParams(prepared))
	require.False(t, shouldCachePrepareCompile(prepared))

	// the cov family reads its decay after the second value column
	prepared = ewmWindowPlan(ewmCall(plan2.NameEwmCov, ewmColumn(t),
		ewmColumn(t), ewmParam(0)))
	require.True(t, plan2.PreparedPlanHasEwmParams(prepared))
	require.False(t, shouldCachePrepareCompile(prepared))

	// literal decay and policies cache normally
	prepared = ewmWindowPlan(ewmCall(plan2.NameEwmMean, ewmColumn(t),
		plan2.MakePlan2Float64ConstExprWithType(0.5)))
	require.False(t, plan2.PreparedPlanHasEwmParams(prepared))
	require.True(t, shouldCachePrepareCompile(prepared))

	// a marker in the value column does not admit the gate
	prepared = ewmWindowPlan(ewmCall(plan2.NameEwmMean, ewmParam(0),
		plan2.MakePlan2Float64ConstExprWithType(0.25)))
	require.False(t, plan2.PreparedPlanHasEwmParams(prepared))
	require.True(t, shouldCachePrepareCompile(prepared))
}

// TestPreparedEwmDisablesMixedRuntimeSpecializationCache: an EXECUTE-time
// decay value must not reuse a compile and must not be admitted by the
// mixed runtime-specialization cache even when another parameter (the
// ABS(?) projection) would otherwise specialize per value (§10 prepared
// stability, the percentile precedent's mixed shape; decay transition
// values re-execute rather than being cached). The per-execution decay
// and policy VALUE enforcement this sequence depends on is proven one level down: the
// α/h/policy VALUE rules at the compile configuration boundary
// (TestConstructEwmConfig) and the end-to-end EXECUTE sequence — valid α,
// different valid α, out-of-range α failing that execution alone, then
// valid again — through real SQL in
// test/distributed/cases/window/ewm_statistics.sql (the prepare/execute
// blocks).
func TestPreparedEwmDisablesMixedRuntimeSpecializationCache(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 29945,
		"select ewmmean(n, abs(?)) over (order by n), abs(?) from (select 1 as n union all select 3) t")
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); prepared.Close() })
	prepared.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("0.25"), false, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("-2"), false, cw.proc.Mp()))
	prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_DOUBLE), 0, byte(defines.MYSQL_TYPE_LONGLONG), 0}
	execute := func(alpha string) {
		require.NoError(t, vector.SetStringAt(prepared.params, 0, alpha, cw.proc.Mp()))
		comp, p, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
		if owned && stmt != nil {
			stmt.Free()
		}
		require.NoError(t, err)
		require.NotNil(t, p)
		require.True(t, prepared.hasEwmParams)
		require.Nil(t, comp, "a value-bound ewm decay must not reuse a compile")
		require.Nil(t, cw.runtimeCacheTarget, "mixed ABS specialization must not admit the ewm compile")
	}
	// successive EXECUTEs of the same statement with different decay values
	for _, alpha := range []string{"0.25", "0.75"} {
		execute(alpha)
	}
}

// TestPreparedEwmMarkerDecayKeepsRowModeOrderLatitude: a bare prepared
// decay marker binds without an event-time verdict, so PREPARE keeps the
// row-mode ORDER BY latitude; the EXECUTE that resolves the marker to a
// numeric alpha compiles the call as row decay with the same latitude
// (review finding: the binder must not classify an unresolved alpha
// marker as the event-time tag).
func TestPreparedEwmMarkerDecayKeepsRowModeOrderLatitude(t *testing.T) {
	for _, sql := range []string{
		"select ewmmean(n, ?) over (order by n desc) from (select 1 as n union all select 3) t",
		"select ewmmean(n, ?) over (order by n desc, n) from (select 1 as n union all select 3) t",
	} {
		t.Run("", func(t *testing.T) {
			ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 29946, sql)
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); prepared.Close() })
			prepared.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("0.5"), false, cw.proc.Mp()))
			prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_DOUBLE), 0}
			comp, p, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
			if owned && stmt != nil {
				stmt.Free()
			}
			require.NoError(t, err,
				"a numeric-alpha EXECUTE compiles the row-mode call with the same ORDER BY latitude")
			require.True(t, prepared.hasEwmParams,
				"a decay-position marker must mark the statement ewm-parameterized")
			require.NotNil(t, p)
			require.Nil(t, comp, "a value-bound ewm decay must not reuse a compile")
		})
	}
}

// TestEwmWindowResultMetadataStaysNullable pins the protocol export of the
// ewm window output nullability: setColFlag derives the wire NOT_NULL_FLAG
// from the plan column declaration exactly (mysql_protocol.go), and every
// ewm output stays nullable no matter how non-NULL its arguments are — the
// §5 C-gate (minPeriods) and the zero-variance correlation rule publish
// NULL, so the plan-side owner (function.DeduceNotNullable, pinned in
// pkg/sql/plan) must feed NotNullable=false and the protocol export must
// keep clearing the flag for such columns.
func TestEwmWindowResultMetadataStaysNullable(t *testing.T) {
	nullable := &MysqlColumn{}
	setColFlag(nullable, &plan.ColDef{
		Typ: plan.Type{Id: int32(types.T_float64), NotNullable: false},
	})
	require.Zero(t, nullable.flag&uint16(defines.NOT_NULL_FLAG),
		"an ewm window output column never exports NOT NULL from its arguments")

	strict := &MysqlColumn{}
	setColFlag(strict, &plan.ColDef{
		NotNull: true,
		Typ:     plan.Type{Id: int32(types.T_int64), NotNullable: true},
	})
	require.NotZero(t, strict.flag&uint16(defines.NOT_NULL_FLAG),
		"the export keeps setting the flag for declared NOT NULL columns")
}

// TestPreparedEwmRealTimeConfigIsExcludedFromBothCaches (tenth review
// round, finding 1): SECOND(CURRENT_TIMESTAMP()) binds as a per-execution
// constant policy and constructEwmConfig evaluates the surviving tree
// with the EXECUTING statement's clock, so the statement is
// execution-dependent: the plan must flag it (PreparedPlanHasEwmParams),
// the prepare-compile cache must not be a candidate
// (shouldCachePrepareCompile), and the executing EXECUTE must not hand
// back a cached compile — while the literal-only twin stays cacheable
// under the same gates (§10 prepared-stability contract).
func TestPreparedEwmRealTimeConfigIsExcludedFromBothCaches(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 29960,
		"select ewmmean(n, 0.5, second(current_timestamp()) = 1) over (order by n) from (select 1 as n union all select 3) t")
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); prepared.Close() })

	realTimePlan := prepared.PreparePlan.GetDcl().GetPrepare().Plan
	require.True(t, plan2.PreparedPlanHasEwmParams(realTimePlan),
		"a real-time decay/policy configuration must flag the statement execution-dependent")
	require.False(t, shouldCachePrepareCompile(realTimePlan),
		"the prepare-compile cache must not freeze an execution-evaluated ewm configuration")

	comp, p, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
	if owned && stmt != nil {
		stmt.Free()
	}
	require.NoError(t, err)
	require.NotNil(t, p)
	require.True(t, prepared.hasEwmParams,
		"a real-time decay/policy configuration must mark the statement ewm-parameterized")
	require.Nil(t, comp,
		"a clock-evaluated ewm configuration must rebuild on every EXECUTE")
	require.Nil(t, cw.runtimeCacheTarget,
		"the runtime-specialization cache must not admit a clock-dependent configuration")

	// the literal-only twin keeps the caches: no execution dependency
	litSes, literal, litCw, litExecCtx := newPreparedExecuteEnvForSQL(t, 29961,
		"select ewmmean(n, 0.5, false) over (order by n) from (select 1 as n union all select 3) t")
	t.Cleanup(func() { litCw.proc.SetPrepareParams(nil); literal.Close() })
	literalPlan := literal.PreparePlan.GetDcl().GetPrepare().Plan
	require.False(t, plan2.PreparedPlanHasEwmParams(literalPlan),
		"literal-only configuration is not execution-dependent")
	require.True(t, shouldCachePrepareCompile(literalPlan),
		"literal-only ewm configuration stays a prepare-compile-cache candidate")
	litComp, litP, litStmt, _, litOwned, litErr := initExecuteStmtParam(
		litExecCtx, litSes, litCw, nil, literal.Name)
	if litOwned && litStmt != nil {
		litStmt.Free()
	}
	require.NoError(t, litErr)
	require.NotNil(t, litP)
	require.False(t, literal.hasEwmParams)
	if litComp != nil {
		// the env's execute path may or may not materialize the cached
		// compile depending on scheduling; the gate above owns cacheability
		litComp.Release()
	}
}
