// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// TestEwmBinderRejections pins the §7 bind-time rules of the ewm window
// statistics: the missing-decay family, the required ORDER BY, the
// explicit-frame rejection, the event-time ORDER BY contracts, the
// key-type rule and the constancy of every decay/policy position.
func TestEwmBinderRejections(t *testing.T) {
	run := func(sql string) error {
		_, err := runOneStmt(
			NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
		return err
	}
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			"missing decay",
			"select ewmmean(n_nationkey) over (order by n_nationkey) from nation",
			"ewm statistics require a declared decay (alpha or 'halflife' half-life)",
		},
		{
			"cov missing decay",
			"select ewmcov(n_nationkey, n_regionkey) over (order by n_nationkey) from nation",
			"ewm statistics require a declared decay (alpha or 'halflife' half-life)",
		},
		{
			"order by required",
			"select ewmmean(n_nationkey, 0.5) over (partition by n_regionkey) from nation",
			"ewm statistics require an ORDER BY in the OVER clause",
		},
		{
			"explicit frame rejected",
			"select ewmmean(n_nationkey, 0.5) over (" +
				"order by n_nationkey rows between 1 preceding and 1 following) from nation",
			"ewm statistics do not support explicit window frames",
		},
		{
			"event-time multi order by",
			"select ewmmean(n_nationkey, 'halflife', 10) over " +
				"(order by n_nationkey, n_regionkey) from nation",
			"event-time ewm statistics require exactly one ORDER BY expression",
		},
		{
			"event-time descending order",
			"select ewmmean(n_nationkey, 'halflife', 10) over " +
				"(order by n_nationkey desc) from nation",
			"event-time ewm statistics require ascending ORDER BY",
		},
		{
			"event-time key type",
			"select ewmmean(n_nationkey, 'halflife', 10) over (order by n_name) from nation",
			"event-time ewm statistics require a numeric or temporal ORDER BY key",
		},
		{
			"non-constant decay",
			"select ewmmean(n_nationkey, n_regionkey) over (order by n_nationkey) from nation",
			"ewm statistics require constant decay and policy arguments",
		},
		{
			"non-constant policy",
			"select ewmmean(n_nationkey, 0.5, n_regionkey > 0) over (order by n_nationkey) from nation",
			"ewm statistics require constant decay and policy arguments",
		},
		{
			"alpha integer literal above one",
			"select ewmmean(n_nationkey, 3) over (order by n_nationkey) from nation",
			"ewm statistics require alpha in (0,1]",
		},
		{
			"alpha literal zero",
			"select ewmmean(n_nationkey, 0) over (order by n_nationkey) from nation",
			"ewm statistics require alpha in (0,1]",
		},
		{
			"halflife tag missing h",
			"select ewmmean(n_nationkey, 'halflife') over (order by n_nationkey) from nation",
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"halflife zero h",
			"select ewmmean(n_nationkey, 'halflife', 0) over (order by n_nationkey) from nation",
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"halflife non-numeric h",
			"select ewmmean(n_nationkey, 'halflife', 'x') over (order by n_nationkey) from nation",
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"unknown string decay tag arity",
			"select ewmmean(n_nationkey, 'span') over (order by n_nationkey) from nation",
			"invalid argument function ewmmean",
		},
		{
			"unknown string decay tag",
			"select ewmmean(n_nationkey, 'span', 5) over (order by n_nationkey) from nation",
			"ewm statistics require a declared decay (alpha or 'halflife' half-life)",
		},
		{
			"halflife tag in policy position",
			"select ewmmean(n_nationkey, 0.5, 'halflife') over (order by n_nationkey) from nation",
			"ewm halflife decay requires a strictly positive half-life constant",
		},
		{
			"cov halflife tag missing h",
			"select ewmcov(n_nationkey, n_regionkey, 'halflife') over (order by n_nationkey) from nation",
			"ewm halflife decay requires a strictly positive half-life constant",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := run(tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.want))
		})
	}
}

// TestEwmBinderAcceptances pins the passing shapes at bind: every decay
// form and the full policy stack resolve, the result type is float64 and
// the registry recognizes the five names as ewm window functions.
func TestEwmBinderAcceptances(t *testing.T) {
	for _, sql := range []string{
		"select ewmmean(n_nationkey, 0.5) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, 0.5, false) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, 0.5, false, true) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, 0.5, false, true, 2) over (order by n_nationkey) from nation",
		"select ewmvar(n_nationkey, 0.5, false, true, 2) over (order by n_nationkey) from nation",
		"select ewmstd(n_nationkey, 0.5, false, true, 2) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, 'halflife', 3600) over (order by n_nationkey asc) from nation",
		"select ewmcorr(n_nationkey, n_regionkey, 'halflife', 60) over (order by n_nationkey) from nation",
		"select ewmcov(n_nationkey, n_regionkey, 2.0/(3.0+1)) over (order by n_nationkey) from nation",
		"select ewmcorr(n_nationkey, n_regionkey, 0.5, true) over " +
			"(partition by n_regionkey order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, 1) over (order by n_nationkey) from nation",
	} {
		t.Run("", func(t *testing.T) {
			logicPlan, err := runOneStmt(
				NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			require.NotEmpty(t, logicPlan.GetQuery().Nodes)
		})
	}
}

// TestEwmRegistryNames pins the registry facts the dispatch keys on.
func TestEwmRegistryNames(t *testing.T) {
	for _, name := range []string{"ewmmean", "ewmvar", "ewmstd", "ewmcov", "ewmcorr"} {
		require.True(t, function.GetFunctionIsWinFunByName(name), name)
		require.True(t, function.GetFunctionIsWinValueFunByName(name), name)
		require.True(t, function.GetFunctionIsWinEwmFunByName(name), name)
		fid, ok := function.EwmFunctionIdByName(name)
		require.True(t, ok, name)
		require.GreaterOrEqual(t, fid, int32(function.EWM_MEAN))
	}
	require.False(t, function.GetFunctionIsWinEwmFunByName("rank"))
	require.False(t, function.GetFunctionIsWinEwmFunByName("ewmean"))
	_, ok := function.EwmFunctionIdByName("rank")
	require.False(t, ok)
}

// TestEwmBinderMarkerDecayDefersEventTimeShape: a prepare-time decay
// marker resolves per execution, so the binder cannot classify it as the
// event-time tag: the row-mode ORDER BY latitude (any direction, any
// number of keys) survives PREPARE, and the literal 'halflife' tag keeps
// its bind-time event-time contracts.
func TestEwmBinderMarkerDecayDefersEventTimeShape(t *testing.T) {
	// the PREPARE binding path: markers bind as prepare-time parameter
	// expressions instead of the execute statement's ?? rejection
	runOnePreparedStmt := func(opt Optimizer, t testing.TB, sql string) (*Plan, error) {
		stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer stmts[0].Free()
		return BuildPlan(opt.CurrentContext(), stmts[0], true)
	}
	for _, sql := range []string{
		"select ewmmean(n_nationkey, ?) over (order by n_nationkey desc) from nation",
		"select ewmmean(n_nationkey, ?) over (order by n_nationkey, n_regionkey) from nation",
		"select ewmvar(n_nationkey, ?) over (partition by n_regionkey order by n_nationkey desc) from nation",
	} {
		t.Run("", func(t *testing.T) {
			plan, err := runOnePreparedStmt(
				NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err,
				"an unresolved decay marker must keep the row-mode ORDER BY latitude at bind")
			require.NotEmpty(t, plan.GetQuery().Nodes)
		})
	}
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{
			"literal tag stays bind-checked",
			"select ewmmean(n_nationkey, 'halflife', 10) over (order by n_nationkey desc) from nation",
			"event-time ewm statistics require ascending ORDER BY",
		},
		{
			"literal tag stays bind-checked for multiple keys",
			"select ewmmean(n_nationkey, 'halflife', 10) over " +
				"(order by n_nationkey, n_regionkey) from nation",
			"event-time ewm statistics require exactly one ORDER BY expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(
				NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.want))
		})
	}
}

// boundEwmWindowFn binds one statement and returns the window function
// expressions of its window nodes, exactly as the framework stored them.
func boundEwmWindowFn(t testing.TB, sql string) []*planpb.Function {
	logicPlan, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
	require.NoError(t, err)
	var fns []*planpb.Function
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, w := range node.WinSpecList {
			fn := w.GetW().WindowFunc.GetF()
			require.NotNil(t, fn, "the bound window spec must hold the function expression")
			fns = append(fns, fn)
		}
	}
	require.NotEmpty(t, fns, "the built plan must contain the bound window function")
	return fns
}

// TestEwmDomainlessNullSamplesBindAsFloat64 (ninth review round): a
// bare NULL sample binds as the domainless NULL literal type, and the
// shared checker gives it the same float64 transport as every typed
// sample (§4 position 1) — the NULL-policy rows of §5/§6 then publish
// instead of failing the dispatch's sample-transport check.
func TestEwmDomainlessNullSamplesBindAsFloat64(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		idx  int
	}{
		{"mean x-null", "select ewmmean(null, 1, false) over (order by n_nationkey) from nation", 0},
		{"var x-null", "select ewmvar(null, 1, false) over (order by n_nationkey) from nation", 0},
		{"std x-null", "select ewmstd(null, 1, false) over (order by n_nationkey) from nation", 0},
		{"cov x-null", "select ewmcov(null, n_regionkey, 1, false) over (order by n_nationkey) from nation", 0},
		{"cov y-null", "select ewmcov(n_regionkey, null, 1, false) over (order by n_nationkey) from nation", 1},
		{"corr x-null", "select ewmcorr(null, n_regionkey, 1, false) over (order by n_nationkey) from nation", 0},
		{"corr y-null", "select ewmcorr(n_regionkey, null, 1, false) over (order by n_nationkey) from nation", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fns := boundEwmWindowFn(t, tc.sql)
			require.Greater(t, len(fns[0].Args), tc.idx)
			require.Equal(t, int32(types.T_float64), fns[0].Args[tc.idx].Typ.Id,
				"a domainless NULL sample must travel with the float64 NULL transport")
		})
	}
	// the resolution-level view through the shared checker: the NULL
	// literal position receives the float64 cast target like any typed
	// sample, for the mean family and both pair-sample positions
	ctx := context.Background()
	anyType := types.T_any.ToType()
	intType := types.T_int64.ToType()
	floatType := types.T_float64.ToType()
	for _, tc := range []struct {
		label  string
		name   string
		inputs []types.Type
		idx    int
	}{
		{"mean", "ewmmean", []types.Type{anyType, intType}, 0},
		{"var", "ewmvar", []types.Type{anyType, intType}, 0},
		{"std", "ewmstd", []types.Type{anyType, intType}, 0},
		{"cov x", "ewmcov", []types.Type{anyType, floatType, intType}, 0},
		{"cov y", "ewmcov", []types.Type{floatType, anyType, intType}, 1},
		{"corr x", "ewmcorr", []types.Type{anyType, floatType, intType}, 0},
		{"corr y", "ewmcorr", []types.Type{floatType, anyType, intType}, 1},
	} {
		t.Run(tc.label+"/resolve", func(t *testing.T) {
			resolved, err := function.GetFunctionByName(ctx, tc.name, tc.inputs)
			require.NoError(t, err)
			targets, cast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, cast, tc.label)
			require.Greater(t, len(targets), tc.idx)
			require.Equal(t, types.T_float64, targets[tc.idx].Oid, tc.label)
		})
	}
}

// TestEwmWindowOutputsRemainNullable (ninth review round): the ewm
// results synthesize NULL from non-NULL arguments — the §5 C-gate (the
// minPeriods publication gate) and the zero-variance correlation rule —
// and an unseen account emits NULL everywhere, so the window output
// nullability must never be inferred NOT NULL from its inputs: the
// shared owner (function.DeduceNotNullable) drives the WIN-node
// materialization column (query_builder.go rewriteWindowFunction) and,
// through it, the exported protocol column flag and CTAS nullability.
func TestEwmWindowOutputsRemainNullable(t *testing.T) {
	for _, sql := range []string{
		// the C-gate: minPeriods=2 gates the first rows of a 25-row nation
		// partition, every argument a non-NULL constant
		"select ewmmean(1e0, 1, true, false, 2) over (order by n_nationkey) from nation",
		// the zero-variance correlation rule over constant pairs
		"select ewmcorr(1e0, 1e0, 1) over (order by n_nationkey) from nation",
	} {
		t.Run("", func(t *testing.T) {
			logicPlan, err := runOneStmt(
				NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			seen := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if len(node.WinSpecList) == 0 {
					continue
				}
				for _, col := range node.ProjectList {
					ref := col.GetCol()
					if ref == nil || ref.RelPos != -1 {
						continue
					}
					require.Equal(t, int32(types.T_float64), col.Typ.Id)
					require.False(t, col.Typ.NotNullable,
						"the window output stays nullable: the C-gate and the zero-variance correlation publish NULL")
					seen++
				}
			}
			require.Equal(t, 1, seen,
				"the window materialization column must be present exactly once")
		})
	}
	// the shared nullability owner: every registered ewm name resolves
	// to a nullable result for all-non-NULL typed arguments
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	intType := types.T_int64.ToType()
	for _, tc := range []struct {
		name   string
		inputs []types.Type
	}{
		{"ewmmean", []types.Type{types.T_float64.ToType(), types.T_float64.ToType(), boolType, boolType, intType}},
		{"ewmvar", []types.Type{types.T_float64.ToType(), types.T_float64.ToType(), boolType, boolType, intType}},
		{"ewmstd", []types.Type{types.T_float64.ToType(), types.T_float64.ToType(), boolType, boolType, intType}},
		{"ewmcov", []types.Type{types.T_float64.ToType(), types.T_float64.ToType(), types.T_float64.ToType(), boolType, boolType, intType}},
		{"ewmcorr", []types.Type{types.T_float64.ToType(), types.T_float64.ToType(), types.T_float64.ToType(), boolType, boolType, intType}},
	} {
		t.Run(tc.name+"/owner", func(t *testing.T) {
			resolved, err := function.GetFunctionByName(ctx, tc.name, tc.inputs)
			require.NoError(t, err)
			args := make([]*Expr, len(tc.inputs))
			for i, typ := range tc.inputs {
				args[i] = &Expr{Typ: Type{Id: int32(typ.Oid), NotNullable: true}}
			}
			require.False(t, function.DeduceNotNullable(resolved.GetEncodedOverloadID(), args),
				tc.name+" can synthesize NULL from non-NULL arguments (§5 C-gate, zero-variance corr)")
		})
	}
}

// TestEwmMarkerDecayBindsDecimal256HalfLife (ninth review round): the
// tag-form half-life position behind an unresolved decay marker shares
// the numeric classification of the literal tag form (§4 position h;
// §8 limitation 3 — decimal256 reaches the configuration evaluator
// unchanged through the shared numeric domain), so the complete marker
// tag form, including the DECIMAL256 spelling CAST(1 AS DECIMAL(65,0))
// and the full policy suffix, binds at PREPARE.
func TestEwmMarkerDecayBindsDecimal256HalfLife(t *testing.T) {
	runOnePreparedStmt := func(opt Optimizer, t testing.TB, sql string) (*Plan, error) {
		stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer stmts[0].Free()
		return BuildPlan(opt.CurrentContext(), stmts[0], true)
	}
	for _, sql := range []string{
		"select ewmmean(n_nationkey, ?, cast(1 as decimal(65,0))) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, ?, cast(1 as decimal(65,0)), false) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, ?, cast(1 as decimal(65,0)), false, true) over (order by n_nationkey) from nation",
		"select ewmmean(n_nationkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey) from nation",
		"select ewmvar(n_nationkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey) from nation",
		"select ewmstd(n_nationkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey) from nation",
		"select ewmcov(n_nationkey, n_regionkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey) from nation",
		"select ewmcorr(n_nationkey, n_regionkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey) from nation",
		// the unresolved decay marker keeps the row-mode ORDER BY latitude
		"select ewmmean(n_nationkey, ?, cast(1 as decimal(65,0)), false, true, 2) over (order by n_nationkey desc) from nation",
	} {
		t.Run("", func(t *testing.T) {
			p, err := runOnePreparedStmt(
				NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err,
				"a DECIMAL256 half-life behind a decay marker binds the complete tag form at PREPARE")
			require.NotEmpty(t, p.GetQuery().Nodes)
		})
	}
	// the shared checker: decimal256 is inside the marker-form half-life
	// domain, alone and with the complete policy suffix, for the mean
	// and covariance families
	ctx := context.Background()
	floatType := types.T_float64.ToType()
	markerType := types.T_text.ToType()
	boolType := types.T_bool.ToType()
	intType := types.T_int64.ToType()
	decimal256 := types.New(types.T_decimal256, 65, 0)
	for _, tc := range []struct {
		label  string
		name   string
		inputs []types.Type
	}{
		{"ewmmean tag only", "ewmmean", []types.Type{floatType, markerType, decimal256}},
		{"ewmmean full suffix", "ewmmean", []types.Type{floatType, markerType, decimal256, boolType, boolType, intType}},
		{"ewmcov tag only", "ewmcov", []types.Type{floatType, floatType, markerType, decimal256}},
		{"ewmcov full suffix", "ewmcov", []types.Type{floatType, floatType, markerType, decimal256, boolType, boolType, intType}},
		{"ewmcorr full suffix", "ewmcorr", []types.Type{floatType, floatType, markerType, decimal256, boolType, boolType, intType}},
	} {
		t.Run(tc.label+"/resolve", func(t *testing.T) {
			_, err := function.GetFunctionByName(ctx, tc.name, tc.inputs)
			require.NoError(t, err,
				"the numeric half-life domain is shared by the literal and marker decay forms")
		})
	}
	// the literal 'halflife' tag twin already classifies decimal256
	_, err := function.GetFunctionByName(ctx, "ewmmean",
		[]types.Type{floatType, types.T_varchar.ToType(), decimal256})
	require.NoError(t, err)
	// an unclassifiable half-life position stays rejected behind a marker
	_, err = function.GetFunctionByName(ctx, "ewmmean",
		[]types.Type{floatType, markerType, types.T_varchar.ToType()})
	require.Error(t, err)
}

// TestEwmRealTimeConfigClassifiesExecutionDependent (tenth review round):
// the binder admits a per-execution constant in a decay or policy
// position — a real-time function like SECOND(CURRENT_TIMESTAMP()) —
// and every plan-tier fold rejects real-time trees (fold candidacy runs
// with currentExecution=false), so the tree survives into the prepare
// plan for constructEwmConfig to evaluate with the EXECUTING
// statement's clock. The prepared caches must therefore classify such a
// statement as execution-dependent: PreparedPlanHasEwmParams reports it
// (the flags gate both the prepare-compile cache and the
// runtime-specialization cache), while a literal-only configuration and
// a deterministic constant expression stay cacheable.
func TestEwmRealTimeConfigClassifiesExecutionDependent(t *testing.T) {
	bindPlan := func(sql string) *planpb.Plan {
		logicPlan, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err)
		return logicPlan
	}

	// a real-time policy position: the clock-dependent tree survives
	// plan binding unfolded (no plan-tier literal carries the clock)
	realTimePolicy := "select ewmmean(n_nationkey, 0.5, second(current_timestamp()) = 1) over (order by n_nationkey) from nation"
	flaggedPolicy := bindPlan(realTimePolicy)
	fns := boundEwmWindowFn(t, realTimePolicy)
	require.Len(t, fns[0].Args, 3)
	require.NotNil(t, fns[0].Args[2].GetF(),
		"a real-time configuration expression must survive plan binding as a function tree: "+
			"a folded literal would freeze the PREPARE clock in the plan itself")
	require.Nil(t, fns[0].Args[2].GetLit(),
		"a clock-dependent policy must not fold into a plan literal")
	require.True(t, PreparedPlanHasEwmParams(flaggedPolicy),
		"a real-time decay or policy position must flag the prepared plan execution-dependent")

	// a real-time decay position classifies through the same walk
	flaggedDecay := bindPlan("select ewmmean(n_nationkey, cast(second(current_timestamp()) as double) / 100.0) over (order by n_nationkey) from nation")
	require.True(t, PreparedPlanHasEwmParams(flaggedDecay),
		"a real-time decay position must flag the prepared plan execution-dependent")

	// a real-time half-life behind the literal 'halflife' tag
	flaggedHalfLife := bindPlan("select ewmmean(n_nationkey, 'halflife', second(current_timestamp())) over (order by n_nationkey) from nation")
	require.True(t, PreparedPlanHasEwmParams(flaggedHalfLife),
		"a real-time half-life position must flag the prepared plan execution-dependent")

	// a literal-only configuration stays cacheable
	literal := bindPlan("select ewmmean(n_nationkey, 0.5, false) over (order by n_nationkey) from nation")
	require.False(t, PreparedPlanHasEwmParams(literal),
		"literal-only ewm configuration is not execution-dependent")

	// a deterministic constant expression folds to a plan literal and
	// does not flag: the flag owns execution dependency, not function trees
	deterministic := bindPlan("select ewmmean(n_nationkey, 0.5, 1 = 1) over (order by n_nationkey) from nation")
	require.False(t, PreparedPlanHasEwmParams(deterministic),
		"a foldable deterministic configuration is not execution-dependent")
}
