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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestPublicationViewBindingMarkerSkipsBinderFolds proves the round-12
// no-evaluation contract of the defines.PublicationViewBinding marker at the
// binder level: an ordinary bind evaluates constant expressions of a stored
// definition, so validating a view that calls a side-effect builtin could
// change global runtime state from inside validation. Under the marker the
// binder's constant-fold stages are skipped, a constant expression stays an
// un-evaluated function expression with its literal operands, and no builtin
// of a validated view can execute while its publication scope is checked.
func TestPublicationViewBindingMarkerSkipsBinderFolds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	bindConstSum := func(ctx context.Context) (*plan.Expr, error) {
		literal := func(value int64) *plan.Expr {
			return makePlan2Int64ConstExprWithType(value)
		}
		// The two pre-bind literal normalizations are part of choosing the
		// right overload of the call; the fold stages behind them are what
		// evaluates constant expressions of the bound definition.
		return bindFuncExprAndConstFoldInternal(
			ctx, proc, "+", []*plan.Expr{literal(2), literal(3)}, false, true, nil)
	}

	t.Run("ordinary binding folds constant operands into one literal", func(t *testing.T) {
		expr, err := bindConstSum(context.Background())
		require.NoError(t, err)
		require.NotNil(t, expr.GetLit(), "the ordinary binder evaluated 2 + 3")
		require.Equal(t, int64(5), expr.GetLit().GetI64Val())
	})

	t.Run("publication view binding evaluates nothing", func(t *testing.T) {
		expr, err := bindConstSum(defines.WithPublicationViewBinding(context.Background()))
		require.NoError(t, err)
		require.NotNil(t, expr.GetF(),
			"the marker keeps the addition an un-evaluated function expression")
		require.Equal(t, "+", expr.GetF().GetFunc().GetObjName())
		require.Len(t, expr.GetF().Args, 2)
		require.Equal(t, int64(2), expr.GetF().Args[0].GetLit().GetI64Val())
		require.Equal(t, int64(3), expr.GetF().Args[1].GetLit().GetI64Val())
	})
}

// TestPublicationViewBindingMarkerRefusesUdfBinding proves the marker's
// user-defined function rule at the real binder: the binder selects a
// user-defined function only after builtin resolution by name, arity and
// argument types declined the call, so a veto here is exactly the finding the
// round-12 review required - it reports what the binder itself chose to bind,
// and a view calling a builtin that merely shares the catalog row with a
// different-signature user-defined function never reaches this arm and stays
// publishable.
func TestPublicationViewBindingMarkerRefusesUdfBinding(t *testing.T) {
	udf := &function.Udf{
		Body:     "0 || 1",
		Language: string(tree.SQL),
	}

	t.Run("a user-defined function selected by the binder vetoes binding", func(t *testing.T) {
		binder := NewDefaultBinder(
			defines.WithPublicationViewBinding(context.Background()),
			nil, nil, plan.Type{}, nil)
		_, err := bindFuncExprImplUdf(&binder.baseBinder, "udf27767", udf, nil, nil, 0)
		var veto *UdfDisallowedInPublicationViewBinding
		require.ErrorAs(t, err, &veto)
		require.Equal(t, "udf27767", veto.Name)
		require.EqualError(t, veto,
			"user-defined function 'udf27767' cannot bind in publication view binding")
	})

	t.Run("the same function binds outside the marker", func(t *testing.T) {
		binder := NewDefaultBinder(context.Background(), nil, nil, plan.Type{}, nil)
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "udf27767", udf, nil, nil, 0)
		require.NoError(t, err)
		require.NotNil(t, expr, "ordinary binding keeps resolving user-defined functions")
	})
}

// publicationFunctionFilters returns the last un-evaluated top-level
// function expression of a query's filter lists.
func publicationFunctionFilters(query *Query) (result *plan.Expr) {
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			if filter.GetF() != nil {
				result = filter
			}
		}
	}
	return result
}

// exprTreeContainsCall reports whether the expression tree still carries the
// un-evaluated round-14 witness call.
func exprTreeContainsCall(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.GetFunc().GetObjName() == "enable_fault_injection" {
			return true
		}
		for _, arg := range fn.GetArgs() {
			if exprTreeContainsCall(arg) {
				return true
			}
		}
		return false
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprTreeContainsCall(item) {
				return true
			}
		}
	}
	return false
}

// TestPublicationViewBindingMarkerStopsAtMetadataBoundary proves the round-13
// boundary of the defines.PublicationViewBinding marker at the whole build: an
// ordinary build of a SELECT binds and then runs the optimizer rewrite
// pipeline - foldTableScanFilters folds WHERE predicates into evaluated
// constants, executing every foldable function in them, and the limit
// pushdown moves the folded LIMIT into the scan - while the marked build,
// the publication validation probe, returns the bound tree with the
// predicate and the LIMIT still un-evaluated function calls and with the
// limit uncompensated, so no function of a stored definition executes during
// validation.
//
// The side-effect witness is enable_fault_injection() - the exact builtin of
// the review's finding - whose ordinary compile executes it and flips the
// process-wide fault-injection switch during planning, while the marked
// build leaves the switch untouched and the call un-evaluated.
func TestPublicationViewBindingMarkerStopsAtMetadataBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	build := func(t *testing.T, ctx context.Context, sql string) *Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		require.NoError(t, err)
		return bound
	}
	markedCtx := defines.WithPublicationViewBinding(context.Background())
	callFilters := func(query *Query) (unfoldedFilters int) {
		for _, node := range query.Nodes {
			for _, filter := range node.FilterList {
				if filter.GetF() != nil {
					unfoldedFilters++
				}
			}
		}
		return unfoldedFilters
	}
	limitStates := func(query *Query) (foldedLimits, unfoldedLimits, scanPushed int) {
		for _, node := range query.Nodes {
			if node.GetLimit().GetF() != nil {
				unfoldedLimits++
			} else if node.GetLimit() != nil {
				foldedLimits++
			}
			if node.NodeType == plan.Node_TABLE_SCAN && node.GetLimit() != nil {
				scanPushed++
			}
		}
		return foldedLimits, unfoldedLimits, scanPushed
	}

	t.Run("ordinary build folds the WHERE predicate and the definition LIMIT", func(t *testing.T) {
		const predicate = "select n_name from tpch.nation where concat('a', 'b') = 'ab' limit 0"
		const limited = "select * from (select n_name from tpch.nation limit 1 + 1) as d limit 0"

		baseline := build(t, context.Background(), predicate)
		require.Zero(t, callFilters(baseline.GetQuery()),
			"the ordinary optimizer folded the constant predicate to a literal filter")

		baseline = build(t, context.Background(), limited)
		folded, unfoldedLimits, scanPushed := limitStates(baseline.GetQuery())
		require.Zero(t, unfoldedLimits, "the ordinary limit fold evaluated the stored 1 + 1")
		require.NotZero(t, folded,
			"the folded literal limits stop in the plan")
		require.NotZero(t, scanPushed,
			"the optimizer's pushdownLimitToTableScan moved the folded limit into the scan")
	})

	t.Run("publication view binding keeps the WHERE predicate and the definition LIMIT un-evaluated", func(t *testing.T) {
		const predicate = "select n_name from tpch.nation where concat('a', 'b') = 'ab' limit 0"
		const limited = "select * from (select n_name from tpch.nation limit 1 + 1) as d limit 0"

		bound := build(t, markedCtx, predicate)
		require.Equal(t, 1, callFilters(bound.GetQuery()),
			"the WHERE predicate stays an un-evaluated function call: the optimizer never ran")
		require.Equal(t, "concat",
			publicationFunctionFilters(bound.GetQuery()).GetF().GetArgs()[0].GetF().GetFunc().GetObjName(),
			"the predicate's own operand call is un-evaluated too")

		bound = build(t, markedCtx, limited)
		_, unfoldedLimits, scanPushed := limitStates(bound.GetQuery())
		require.Equal(t, 1, unfoldedLimits,
			"the stored definition's LIMIT stays an un-evaluated function call")
		require.Zero(t, scanPushed,
			"the bound plan was returned in front of the optimizer's limit pushdown")
	})

	t.Run("ordinary build executes the predicate's side-effect builtin", func(t *testing.T) {
		previous := fault.Status()
		t.Cleanup(func() {
			if previous {
				fault.Enable()
			} else {
				fault.Disable()
			}
		})
		require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")

		// The ordinary compile executes enable_fault_injection() while folding
		// the constant predicate, exactly the runtime-state change the
		// publication validator must never perform.
		build(t, context.Background(),
			"select n_name from tpch.nation where enable_fault_injection() limit 0")
		require.True(t, fault.Status(),
			"the ordinary bind and optimizer executed the side-effect builtin")
	})

	t.Run("publication view binding executes nothing of the definition", func(t *testing.T) {
		require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")
		t.Cleanup(func() { fault.Disable() })

		bound := build(t, markedCtx,
			"select n_name from tpch.nation where enable_fault_injection() limit 0")
		require.False(t, fault.Status(), "the marked build never executed the builtin")
		require.Equal(t, "enable_fault_injection",
			publicationFunctionFilters(bound.GetQuery()).GetF().GetFunc().GetObjName(),
			"the side-effect call stays un-evaluated in the bound predicate")
	})
}

// TestPublicationViewBindingMarkerSkipsComparisonFolds proves the round-14
// gate of the defines.PublicationViewBinding marker at the two executor-backed
// comparison folders the review named: foldFloatComparisonConstants and
// foldDecimalStringComparisonConstants run ConstantFold on non-literal
// constant operands in front of the resolver, and a cast wrapped around a
// side-effect builtin satisfies each folder's type guard - CAST(
// enable_fault_injection() AS CHAR) against a DECIMAL column, CAST(
// enable_fault_injection() AS DOUBLE) against a FLOAT column - so an ordinary
// bind of such a predicate executes the builtin. Under the marker no operand
// executes: both folders are skipped, and the fold each returns an
// expression of the input type, so the resolver still sees the same argument
// types while the value materialization returns to the subscriber's own
// compile.
func TestPublicationViewBindingMarkerSkipsComparisonFolds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	buildProbe := func(t *testing.T, ctx context.Context, sql string) *Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		require.NoError(t, err)
		return bound
	}
	resetDisabled := func(t *testing.T) {
		t.Helper()
		previous := fault.Status()
		t.Cleanup(func() {
			if previous {
				fault.Enable()
			} else {
				fault.Disable()
			}
		})
		require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")
	}
	floatColumn := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: 0, ColPos: 0, Name: "f",
			}},
		}
	}
	// A registered, executable cast candidate bound from real SQL under the
	// marker (the ordinary binder fold would already evaluate it); the
	// extraction is probe-shaped, so it stops at the metadata boundary and
	// the expression stays un-evaluated and registered.
	castDoubleFromSql := func(t *testing.T) *plan.Expr {
		t.Helper()
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()),
			"select cast(enable_fault_injection() as double) as x limit 0")
		for _, node := range bound.GetQuery().Nodes {
			for _, project := range node.ProjectList {
				if call := project.GetF(); call != nil && call.GetFunc().GetObjName() == "cast" {
					return DeepCopyExpr(project)
				}
			}
		}
		t.Fatalf("the marked projection binds the cast-wrapped call")
		return nil
	}

	t.Run("ordinary decimal-string comparison executes the cast-wrapped builtin", func(t *testing.T) {
		resetDisabled(t)
		buildProbe(t, context.Background(),
			"select p_name from tpch.part where p_retailprice = cast(enable_fault_injection() as char) limit 0")
		require.True(t, fault.Status(),
			"the decimal-string comparison folder executed the cast and its builtin")
	})

	t.Run("publication view binding executes nothing of the decimal comparison", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()),
			"select p_name from tpch.part where p_retailprice = cast(enable_fault_injection() as char) limit 0")
		require.False(t, fault.Status(), "the marked build never executed the cast chain")
		comparison := publicationFunctionFilters(bound.GetQuery())
		require.NotNil(t, comparison)
		require.Equal(t, "cast",
			comparison.GetF().GetArgs()[1].GetF().GetFunc().GetObjName(),
			"the cast-wrapped call stays un-evaluated in the bound comparison")
		containsCall := exprTreeContainsCall(comparison)
		require.True(t, containsCall,
			"the builtin stays un-evaluated somewhere in the bound comparison")
	})

	t.Run("ordinary float comparison executes the cast-wrapped builtin", func(t *testing.T) {
		resetDisabled(t)
		candidate := castDoubleFromSql(t)
		require.False(t, fault.Status(), "the marked extraction executed nothing")
		_, err := bindFuncExprAndConstFoldInternal(context.Background(), proc, ">",
			[]*plan.Expr{floatColumn(), candidate}, false, true, nil)
		require.NoError(t, err)
		require.True(t, fault.Status(),
			"the float comparison folder executed the cast and its builtin")
	})

	t.Run("publication view binding executes nothing of the float comparison", func(t *testing.T) {
		resetDisabled(t)
		candidate := castDoubleFromSql(t)
		bound, err := bindFuncExprAndConstFoldInternal(
			defines.WithPublicationViewBinding(context.Background()), proc, ">",
			[]*plan.Expr{floatColumn(), candidate}, false, true, nil)
		require.NoError(t, err)
		require.False(t, fault.Status(), "the marked fold executed nothing")
		require.Equal(t, "cast",
			bound.GetF().GetArgs()[1].GetF().GetFunc().GetObjName(),
			"the cast-wrapped call stays un-evaluated in the bound comparison")
	})
}

// TestPublicationViewBindingMarkerSkipsStringLengthInference proves the
// marker at the binder's string-length source inference:
// annotateStringLengthSource evaluates the constant length argument of the
// left/right/repeat/lpad/rpad/substring family through
// EvaluateConstantExpression right after the resolver, so a stored
// definition like LEFT('abc', CAST(enable_fault_injection() AS SIGNED))
// executes the builtin while binding. Under the marker the inference is
// skipped, and because it only writes the additive
// PreparedNumeric.StringDomainSource witness without mutating the bound
// expression's own type, the binding verdict and overload selection stay
// exactly what an ordinary build starts from; the subscriber's ordinary
// compile infers and folds as usual.
func TestPublicationViewBindingMarkerSkipsStringLengthInference(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	buildProbe := func(t *testing.T, ctx context.Context, sql string) *Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		require.NoError(t, err)
		return bound
	}
	resetDisabled := func(t *testing.T) {
		t.Helper()
		previous := fault.Status()
		t.Cleanup(func() {
			if previous {
				fault.Enable()
			} else {
				fault.Disable()
			}
		})
		require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")
	}
	leftFunction := func(t *testing.T, query *Query) *plan.Expr {
		t.Helper()
		for _, node := range query.Nodes {
			for _, project := range node.ProjectList {
				if call := project.GetF(); call != nil &&
					strings.EqualFold(call.GetFunc().GetObjName(), "left") {
					return DeepCopyExpr(project)
				}
			}
		}
		t.Fatalf("the projection binds the left call")
		return nil
	}

	t.Run("ordinary left with a cast-wrapped length executes the builtin", func(t *testing.T) {
		resetDisabled(t)
		buildProbe(t, context.Background(),
			"select left('abc', cast(enable_fault_injection() as signed)) as s limit 0")
		require.True(t, fault.Status(),
			"the ordinary bind executed the constant length and its builtin")
	})

	t.Run("publication view binding executes nothing of the length inference", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()),
			"select left('abc', cast(enable_fault_injection() as signed)) as s limit 0")
		require.False(t, fault.Status(), "the marked build never executed the length chain")
		left := leftFunction(t, bound.GetQuery())
		require.True(t, exprTreeContainsCall(left),
			"the length argument keeps its un-evaluated call chain")
		require.Nil(t, left.GetPreparedNumeric(),
			"the skipped inference writes no string-length witness")
	})

	t.Run("publication view binding keeps a computed literal length un-executed", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()),
			"select left(p_name, 1 + 1) as s2 from tpch.part limit 0")
		require.False(t, fault.Status(), "a literal length executes nothing")
		left := leftFunction(t, bound.GetQuery())
		length := left.GetF().GetArgs()[1]
		require.NotNil(t, length.GetF(),
			"the sum length still binds as a call under the marker, got: %s", length.String())
		require.Equal(t, "+", length.GetF().GetFunc().GetObjName(),
			"the sum length stays un-evaluated at binding")
		require.Nil(t, left.GetPreparedNumeric(),
			"the skipped inference writes no string-length witness")
	})

	t.Run("ordinary computed length on a column still records the witness", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, context.Background(),
			"select left(p_name, 1 + 1) as s2 from tpch.part limit 0")
		require.False(t, fault.Status(), "the control has no builtin to execute")
		left := leftFunction(t, bound.GetQuery())
		require.NotNil(t, left.GetPreparedNumeric(),
			"the gate removed only the marked inference: ordinary builds still annotate")
		require.NotNil(t, left.GetPreparedNumeric().GetStringDomainSource(),
			"the ordinary inference records the string-length witness")
	})
}

// TestPublicationViewBindingMarkerRefusesCorrelatedAggregateSpineProof proves
// the marker at the binder's correlated aggregate empty-input analysis. The
// analysis (aggregateSpineOriginalEmpty, reached from flattenSubqueries while
// binding a scalar correlated CTE projection) evaluates substituted constant
// expressions of the stored definition through ConstantFold - the round-16
// review's counterexample shape is (WITH x AS (SELECT COUNT(*) AS c FROM
// base i WHERE i.id = o.id) SELECT c + CAST(enable_fault_injection() AS
// SIGNED) FROM x) as a projected scalar - and the spine guard
// containsVolatileFunction does not exclude unflagged builtins. Under the
// marker the analysis refuses to prove (its pre-existing nil outcome), so the
// correlated shape still binds as pure metadata while the ordinary build
// executes the cast; both facts are asserted, because a false rejection or a
// silent execution would each re-open the review's gap.
func TestPublicationViewBindingMarkerRefusesCorrelatedAggregateSpineProof(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	buildProbe := func(t *testing.T, ctx context.Context, sql string) *Plan {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		require.NoError(t, err)
		return bound
	}
	resetDisabled := func(t *testing.T) {
		t.Helper()
		previous := fault.Status()
		t.Cleanup(func() {
			if previous {
				fault.Enable()
			} else {
				fault.Disable()
			}
		})
		require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")
	}
	sideEffectSQL := "select o.o_orderkey, (with x as (select count(*) as c from tpch.orders i where i.o_orderkey = o.o_orderkey) select c + cast(enable_fault_injection() as signed) from x) as n from tpch.orders o limit 0"
	controlSQL := "select o.o_orderkey, (with x as (select count(*) as c from tpch.orders i where i.o_orderkey = o.o_orderkey) select c + 1 - 1 from x) as n from tpch.orders o limit 0"

	t.Run("the ordinary correlated CTE executes the cast", func(t *testing.T) {
		resetDisabled(t)
		buildProbe(t, context.Background(), sideEffectSQL)
		require.True(t, fault.Status(),
			"the ordinary bind's aggregate-spine analysis executed the substituted cast and its builtin")
	})

	t.Run("publication view binding proves nothing and executes nothing", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()), sideEffectSQL)
		require.False(t, fault.Status(),
			"the marked correlated CTE binds without evaluating its stored expression")
		require.NotNil(t, bound.GetQuery(),
			"the refusal is the proof's own unproven outcome: the correlated shape still binds as metadata")
	})

	t.Run("the arithmetic control still binds under the marker", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()), controlSQL)
		require.False(t, fault.Status(), "the control holds no builtin")
		require.NotNil(t, bound.GetQuery(),
			"the unproven spine leaves the correlated control binding successfully")
	})

	t.Run("a marked optimized build keeps compiling with the proof refused", func(t *testing.T) {
		resetDisabled(t)
		bound := buildProbe(t, defines.WithPublicationViewBinding(context.Background()),
			"select o.o_orderkey, (with x as (select count(*) as c from tpch.orders i where i.o_orderkey = o.o_orderkey) select c + 1 - 1 from x) as n from tpch.orders o")
		require.False(t, fault.Status(), "the control holds no builtin")
		require.NotNil(t, bound.GetQuery(),
			"a non-probe-shaped marked build - the nested catalog readers' shape - still passes the full compile with the analysis refused")
	})
}

// TestPublicationViewBindingMarkerGatesEagerFrameBoundEvaluation proves the
// marker at the binder's eager window-frame positions, which the round-13
// review named directly: makeWindowFrameConstValue and the interval
// normalization tail of resetWindowIntervalExpr execute the bound frame
// expression at bind time, so the gate returns the bound expression
// unchanged while the ordinary path evaluates it to a constant.
func TestPublicationViewBindingMarkerGatesEagerFrameBoundEvaluation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	plusCall := func(t *testing.T) *plan.Expr {
		t.Helper()
		// Binding through the real function registry produces a call the
		// ordinary executor can evaluate, unlike a hand-crafted overload id.
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*plan.Expr{
			makePlan2Int64ConstExprWithType(1),
			makePlan2Int64ConstExprWithType(2),
		})
		require.NoError(t, err)
		return bound
	}
	typ := func() *plan.Type {
		number := makePlan2Int64ConstExprWithType(1)
		return &number.Typ
	}

	t.Run("ordinary frame-bound binding evaluates the expression", func(t *testing.T) {
		bindCall := func(tree.Expr, int32, bool) (*plan.Expr, error) { return plusCall(t), nil }
		folded, err := makeWindowFrameConstValue(bindCall, proc, context.Background(), nil, typ())
		require.NoError(t, err)
		require.NotNil(t, folded.GetLit(), "the ordinary bind evaluated the frame bound")
		require.Equal(t, int64(3), folded.GetLit().GetI64Val())
	})

	t.Run("publication frame-bound binding evaluates nothing", func(t *testing.T) {
		bindCall := func(tree.Expr, int32, bool) (*plan.Expr, error) { return plusCall(t), nil }
		gated, err := makeWindowFrameConstValue(bindCall, proc,
			defines.WithPublicationViewBinding(context.Background()), nil, typ())
		require.NoError(t, err)
		require.NotNil(t, gated.GetF(),
			"the gate returns a bound expression instead of evaluating the frame bound")
		require.Equal(t, "+",
			gated.GetF().GetArgs()[0].GetF().GetFunc().GetObjName(),
			"the frame bound's call stays un-evaluated inside its cast wrapper")
	})

	intervalList := func(t *testing.T) *plan.Expr {
		t.Helper()
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_interval)},
			Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
				plusCall(t),
				makePlan2StringConstExprWithType("day"),
			}}},
		}
	}

	t.Run("ordinary interval-bound binding evaluates the value", func(t *testing.T) {
		normalized, err := resetWindowIntervalExpr(context.Background(), proc, intervalList(t))
		require.NoError(t, err)
		value := normalized.GetList().List[0]
		require.NotNil(t, value.GetLit(), "the ordinary bind evaluated the interval value")
		require.Equal(t, int64(3), value.GetLit().GetI64Val())
	})

	t.Run("publication interval-bound binding evaluates nothing", func(t *testing.T) {
		gated, err := resetWindowIntervalExpr(
			defines.WithPublicationViewBinding(context.Background()), proc, intervalList(t))
		require.NoError(t, err)
		require.NotNil(t, gated.GetList().List[0].GetF(),
			"the gate keeps the interval value un-evaluated")
	})
}

// TestPublicationViewBindingMarkerPreservesConstantExpressionSRIDs proves the
// round-19 compatibility contract of the defines.PublicationViewBinding
// marker at the geometry SRID decode. Ordinary binding folds a
// constant-expression SRID (4325 + 1) through the binder's own arithmetic
// constant fold before the geometry constructors decode it, so a view whose
// SRID is a constant expression is a valid, creatable definition. The
// marked probe returns in front of those fold stages, so before this round
// the same valid definition reached the decode still unfolded and was
// misclassified as broken ('the SRID argument of a geometry constructor
// must be a constant integer'). The probe now folds the SRID argument
// through the same executor-backed constant fold ordinary binding applies
// to the same expression, but only after a shape walk proves the argument
// is built entirely from literals and the very operator slice the ordinary
// fold covers - so every verdict matches ordinary binding, no side-effect
// builtin of a stored definition can execute during validation, and the
// verdicts that were already rejections (fraction division, non-arithmetic
// wrappers, out-of-range values) stay rejections.
//
// Round 20 extends the same contract to the implicit-cast shape: a string
// operand inside the arithmetic ('4325' + 1) makes the resolver wrap the
// operands in conversion nodes whose trailing argument is target-type
// metadata (Expr_T), ordinary binding folds that whole tree to the same
// literal, and the probe must reach the identical verdict (the walk accepts
// exactly that structural metadata leaf). The explicit CAST root keeps the
// ordinary build's own rejection in both contexts: CAST syntax bypasses
// the fold switch in ordinary binding, and the probe mirrors that boundary
// by refusing to fold a cast root, so loosening the walk for conversion
// metadata cannot admit what ordinary binding itself never admits.
func TestPublicationViewBindingMarkerPreservesConstantExpressionSRIDs(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// buildSRIDProjection parses real SQL, builds it through the real binder
	// (mock catalog, real function registry) under ctx, and returns the one
	// projection expression whose type carries a defined SRID width - the
	// geometry carrying the constant-expression SRID (the optimizer may
	// fold the whole constructor into a literal, its geometry type carries
	// the encoded SRID either way).
	buildSRIDProjection := func(t *testing.T, ctx context.Context, sql string) (*plan.Expr, error) {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		t.Cleanup(stmt.Free)
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		if err != nil {
			return nil, err
		}
		matches := make([]*plan.Expr, 0, 1)
		for _, node := range bound.GetQuery().Nodes {
			for _, project := range node.ProjectList {
				if _, defined := decodeGeometrySRIDWidth(project.Typ.Width); defined {
					matches = append(matches, DeepCopyExpr(project))
				}
			}
		}
		if len(matches) != 1 {
			return nil, fmt.Errorf("expected exactly one SRID-carrying projection, got %d", len(matches))
		}
		return matches[0], nil
	}

	// assertSRID requires the projection's type to carry exactly the
	// literal-4326 SRID, the same geometry typing ordinary binding gives
	// the definition.
	assertSRID := func(t *testing.T, expr *plan.Expr) {
		t.Helper()
		srid, defined := decodeGeometrySRIDWidth(expr.Typ.Width)
		require.True(t, defined,
			"the bound geometry type records a defined SRID, got: %s", expr.String())
		require.Equal(t, uint32(4326), srid,
			"the arithmetic SRID decodes to the literal twin's own value")
	}

	bindsWithSRID4326 := func(t *testing.T, ctx context.Context, sql string) {
		t.Helper()
		expr, err := buildSRIDProjection(t, ctx, sql)
		require.NoError(t, err)
		assertSRID(t, expr)
	}

	rejectsWith := func(t *testing.T, ctx context.Context, sql, messagePart string) {
		t.Helper()
		_, err := buildSRIDProjection(t, ctx, sql)
		require.Error(t, err)
		require.ErrorContains(t, err, messagePart)
	}

	const geometrySQL = "select st_geomfromtext('POINT(1 2)', 4325 + 1) as g limit 0"
	const literalTwinSQL = "select st_geomfromtext('POINT(1 2)', 4326) as g limit 0"
	const setterSQL = "select st_srid(st_geomfromtext('POINT(1 2)', 0), 4325 + 1) as s limit 0"
	const setterTwinSQL = "select st_srid(st_geomfromtext('POINT(1 2)', 0), 4326) as s limit 0"
	// The implicit-cast shape: a string operand inside otherwise whitelisted
	// arithmetic makes the resolver build conversion nodes whose trailing
	// argument is target-type metadata (Expr_T), not SQL. Ordinary binding
	// folds the whole tree to the same 4326, so the probe has to meet the
	// same literal.
	const implicitCastSQL = "select st_geomfromtext('POINT(1 2)', '4325' + 1) as g limit 0"
	const implicitCastSetterSQL = "select st_srid(st_geomfromtext('POINT(1 2)', 0), '4325' + 1) as s limit 0"
	markedCtx := defines.WithPublicationViewBinding(context.Background())

	t.Run("ordinary binding folds the arithmetic SRID like the literal twin", func(t *testing.T) {
		literalTwin, err := buildSRIDProjection(t, context.Background(), literalTwinSQL)
		require.NoError(t, err)
		bindsWithSRID4326(t, context.Background(), geometrySQL)
		arithmetic, err := buildSRIDProjection(t, context.Background(), geometrySQL)
		require.NoError(t, err)
		require.Equal(t, literalTwin.Typ.Width, arithmetic.Typ.Width,
			"the folded arithmetic SRID carries the literal twin's exact type shape")
	})

	t.Run("publication binding preserves the arithmetic SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, markedCtx, geometrySQL)
	})

	t.Run("publication binding preserves the ST_SRID setter through the marker", func(t *testing.T) {
		setterTwin, err := buildSRIDProjection(t, context.Background(), setterTwinSQL)
		require.NoError(t, err)
		require.Equal(t, uint32(4326), func() uint32 {
			srid, _ := decodeGeometrySRIDWidth(setterTwin.Typ.Width)
			return srid
		}())
		bindsWithSRID4326(t, context.Background(), setterSQL)
		bindsWithSRID4326(t, markedCtx, setterSQL)
	})

	t.Run("publication binding preserves the implicit-cast arithmetic SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), implicitCastSQL)
		bindsWithSRID4326(t, markedCtx, implicitCastSQL)
	})

	t.Run("publication binding preserves the implicit-cast ST_SRID setter through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), implicitCastSetterSQL)
		bindsWithSRID4326(t, markedCtx, implicitCastSetterSQL)
	})

	// The nested pure-function shape: the argument is whitelisted arithmetic
	// whose subtree is a pure deterministic builtin. Ordinary binding folds
	// the '+' root at the post-bind switch and the fold recursion folds the
	// ABS child into its literal, so an ordinary session creates this view as
	// a valid, subscribable definition and the probe has to meet the same
	// literal - the walk must admit exactly the deterministic builtins the
	// fold recursion itself would evaluate, and nothing else.
	const nestedAbsSQL = "select st_geomfromtext('POINT(1 2)', abs(4325) + 1) as g limit 0"
	const nestedAbsSetterSQL = "select st_srid(st_geomfromtext('POINT(1 2)', 0), abs(4325) + 1) as s limit 0"

	t.Run("publication binding preserves the nested pure-function arithmetic SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), nestedAbsSQL)
		bindsWithSRID4326(t, markedCtx, nestedAbsSQL)
	})

	t.Run("publication binding preserves the nested pure-function ST_SRID setter through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), nestedAbsSetterSQL)
		bindsWithSRID4326(t, markedCtx, nestedAbsSetterSQL)
	})

	// The mo-bot-local round-14 follow-up review exported counterexamples for
	// the guarded fold's remaining ordinary-SQL gaps: a comparison operator
	// inside IFF, a date-part extraction (YEAR), and the JSON readers
	// (JSON_EXTRACT, JSON_EXTRACT_STRING) are pure in-process value
	// transformations of constant operands, so an ordinary bind folds each
	// constant expression into the same literal 4326 and the stored view is
	// valid, creatable and subscribable, while the marked probe rejected the
	// same definitions with the constant-integer verdict - the nested
	// registrations were missing from the curated proven-pure classification.
	// The completed classification admits them, and these
	// ordinary-versus-marked agreements regress if its membership shrinks
	// below the audited categories or a future registration silently starts
	// carrying an excluded flag.
	const iffComparisonSQL = "select st_geomfromtext('POINT(1 2)', cast(iff(1 = 1, 4326, 0) as signed) + 0) as g limit 0"
	const yearSQL = "select st_geomfromtext('POINT(1 2)', year('4326-05-17') + 0) as g limit 0"
	const jsonExtractSQL = `select st_geomfromtext('POINT(1 2)', cast(json_extract('{"srid": 4326}', '$.srid') as signed) + 0) as g limit 0`
	const jsonExtractStringSQL = `select st_geomfromtext('POINT(1 2)', json_extract_string('{"srid": "4326"}', '$.srid') + 0) as g limit 0`

	t.Run("publication binding preserves the IFF-comparison SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), iffComparisonSQL)
		bindsWithSRID4326(t, markedCtx, iffComparisonSQL)
	})

	t.Run("publication binding preserves the YEAR date-part SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), yearSQL)
		bindsWithSRID4326(t, markedCtx, yearSQL)
	})

	t.Run("publication binding preserves the JSON-extract SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), jsonExtractSQL)
		bindsWithSRID4326(t, markedCtx, jsonExtractSQL)
	})

	t.Run("publication binding preserves the JSON-extract-string SRID through the marker", func(t *testing.T) {
		bindsWithSRID4326(t, context.Background(), jsonExtractStringSQL)
		bindsWithSRID4326(t, markedCtx, jsonExtractStringSQL)
	})

	t.Run("both bindings keep the explicit CAST root verdict", func(t *testing.T) {
		// An explicit CAST binds through the dedicated CastExpr path and
		// ordinary binding never folds it (the ground-truth live verdict for
		// these shapes is the constant-integer rejection), so the probe's
		// new fold must not start admitting them either.
		for _, sql := range []string{
			"select st_geomfromtext('POINT(1 2)', cast('4326' as signed)) as g limit 0",
			"select st_geomfromtext('POINT(1 2)', cast(4325 + 1 as signed)) as g limit 0",
		} {
			for _, ctx := range []context.Context{context.Background(), markedCtx} {
				rejectsWith(t, ctx, sql,
					"the SRID argument of a geometry constructor must be a constant integer")
			}
		}
	})

	t.Run("both bindings reject a fraction-division SRID identically", func(t *testing.T) {
		const sql = "select st_geomfromtext('POINT(1 2)', 4326 / 1) as g limit 0"
		for _, ctx := range []context.Context{context.Background(), markedCtx} {
			rejectsWith(t, ctx, sql,
				"the SRID argument of a geometry constructor must be a constant integer")
		}
	})

	t.Run("both bindings reject a divisor-free integer division identically", func(t *testing.T) {
		const sql = "select st_geomfromtext('POINT(1 2)', 4326 div 0) as g limit 0"
		for _, ctx := range []context.Context{context.Background(), markedCtx} {
			rejectsWith(t, ctx, sql,
				"the SRID argument of a geometry constructor must be a constant integer")
		}
	})

	t.Run("both bindings reject non-arithmetic wrappers identically", func(t *testing.T) {
		for _, sql := range []string{
			"select st_geomfromtext('POINT(1 2)', abs(4325)) as g limit 0",
			"select st_geomfromtext('POINT(1 2)', concat('43', '26')) as g limit 0",
		} {
			for _, ctx := range []context.Context{context.Background(), markedCtx} {
				rejectsWith(t, ctx, sql,
					"the SRID argument of a geometry constructor must be a constant integer")
			}
		}
	})

	t.Run("both bindings keep the SRID range verdict of the folded value", func(t *testing.T) {
		for _, sql := range []string{
			"select st_geomfromtext('POINT(1 2)', 2147483646 + 1) as g limit 0",
			"select st_geomfromtext('POINT(1 2)', ~4294967294) as g limit 0",
			"select st_geomfromtext('POINT(1 2)', 4326 - 8652) as g limit 0",
		} {
			for _, ctx := range []context.Context{context.Background(), markedCtx} {
				rejectsWith(t, ctx, sql, "SRID should be between 0 and")
			}
		}
	})

	t.Run("nothing executes when the SRID expression wraps a side-effect builtin", func(t *testing.T) {
		resetSRIDFixture := func(t *testing.T) {
			t.Helper()
			previous := fault.Status()
			t.Cleanup(func() {
				if previous {
					fault.Enable()
				} else {
					fault.Disable()
				}
			})
			require.False(t, fault.Disable(), "the fixture starts with fault injection disabled")
		}
		const sql = "select st_geomfromtext('POINT(1 2)', cast(enable_fault_injection() as signed) + 4325) as g limit 0"

		t.Run("the ordinary bind executes the wrapped builtin while folding", func(t *testing.T) {
			resetSRIDFixture(t)
			expr, err := buildSRIDProjection(t, context.Background(), sql)
			require.NoError(t, err)
			require.True(t, expr.Typ.Width > 0,
				"the ordinary fold let the geometry decode record a defined SRID")
			require.True(t, fault.Status(),
				"the ordinary arithmetic fold executed the cast-wrapped builtin")
		})

		t.Run("the marked probe neither executes the builtin nor admits it", func(t *testing.T) {
			resetSRIDFixture(t)
			rejectsWith(t, markedCtx, sql,
				"the SRID argument of a geometry constructor must be a constant integer")
			require.False(t, fault.Status(),
				"the guarded probe executed nothing of the wrapped definition")
		})

		const implicitCastWitnessSQL = "select st_geomfromtext('POINT(1 2)', '4325' + cast(enable_fault_injection() as signed)) as g limit 0"

		t.Run("the ordinary bind executes the implicit-cast wrapping builtin while folding", func(t *testing.T) {
			resetSRIDFixture(t)
			expr, err := buildSRIDProjection(t, context.Background(), implicitCastWitnessSQL)
			require.NoError(t, err)
			require.True(t, expr.Typ.Width > 0,
				"the ordinary fold let the geometry decode record a defined SRID")
			require.True(t, fault.Status(),
				"the ordinary fold executed the builtin nested inside implicit-cast arithmetic")
		})

		t.Run("the marked probe neither executes the implicit-cast builtin nor admits it", func(t *testing.T) {
			resetSRIDFixture(t)
			rejectsWith(t, markedCtx, implicitCastWitnessSQL,
				"the SRID argument of a geometry constructor must be a constant integer")
			require.False(t, fault.Status(),
				"the guarded probe executed nothing inside the implicit-cast arithmetic")
		})

		// The internal-machinery control keeps its rejection exactly where the
		// new deterministic-builtin admission would otherwise start folding:
		// the builtin sits inside ordinary whitelisted arithmetic, and only
		// the function-class veto keeps the guarded probe from evaluating it.
		const arithmeticWitnessSQL = "select st_geomfromtext('POINT(1 2)', 1 + cast(enable_fault_injection() as signed) + 4325) as g limit 0"

		t.Run("the ordinary bind executes the builtin nested in plain arithmetic while folding", func(t *testing.T) {
			resetSRIDFixture(t)
			expr, err := buildSRIDProjection(t, context.Background(), arithmeticWitnessSQL)
			require.NoError(t, err)
			require.True(t, expr.Typ.Width > 0,
				"the ordinary fold let the geometry decode record a defined SRID")
			require.True(t, fault.Status(),
				"the ordinary arithmetic fold executed the builtin nested in plain arithmetic")
		})

		t.Run("the marked probe neither executes the arithmetically nested builtin nor admits it", func(t *testing.T) {
			resetSRIDFixture(t)
			rejectsWith(t, markedCtx, arithmeticWitnessSQL,
				"the SRID argument of a geometry constructor must be a constant integer")
			require.False(t, fault.Status(),
				"the guarded probe executed nothing of the arithmetically nested builtin")
		})

		// The python UDF machinery is the sharp edge of the internal-class
		// veto: a stored definition can name python_user_defined_function
		// itself, so the binder resolves the call through the builtin
		// registry and never reaches the catalog-UDF veto above, and the
		// overloads carry no volatile flag - only the class bit keeps the
		// guarded probe away from UdfService.Run. The counting fake proves
		// the boundary at the service itself: the ordinary bind executes the
		// machinery through its own fold (and still rejects the service-less
		// definition with the constant-integer verdict), while the marked
		// probe must neither call the service nor admit the definition.
		pyUdfJSON := `{"udf":{"body":"{\\\"import\\\":false,\\\"handler\\\":\\\"h\\\",\\\"body\\\":\\\"pass\\\"}","language":"python","rettype":"int","db":"d","modified_time":""}}`
		pyUdfWitnessSQL := "select st_geomfromtext('POINT(1 2)', 1 + python_user_defined_function('" +
			pyUdfJSON + "', 0)) as g limit 0"

		t.Run("the ordinary bind executes the python UDF machinery through its own fold", func(t *testing.T) {
			udfService := &countingUdfService{}
			proc.Base.UdfService = udfService
			defer func() { proc.Base.UdfService = nil }()

			_, err := buildSRIDProjection(t, context.Background(), pyUdfWitnessSQL)
			require.Error(t, err, "the service-less definition cannot produce a constant integer")
			require.ErrorContains(t, err,
				"the SRID argument of a geometry constructor must be a constant integer")
			require.Positive(t, udfService.runs,
				"the ordinary fold executes the wrapped python UDF machinery, exactly its own admission contract")
		})

		t.Run("the marked probe neither calls the UDF service nor admits the definition", func(t *testing.T) {
			udfService := &countingUdfService{}
			proc.Base.UdfService = udfService
			defer func() { proc.Base.UdfService = nil }()

			rejectsWith(t, markedCtx, pyUdfWitnessSQL,
				"the SRID argument of a geometry constructor must be a constant integer")
			require.Zero(t, udfService.runs,
				"the guarded probe executed nothing of the internal UDF machinery")
		})

		// The mo-bot-local review's executable counterexample, rebuilt on the
		// registry's deterministic in-process LLM: llm_chat is registered
		// STRICT without the internal-machinery class bit and without a
		// volatile or real-time-related overload, so a negative-only
		// admission passes it and the guarded probe fold would execute the
		// LLM machinery during this metadata-only validation - the echo
		// model answers the prompt with "4326", and only evaluating that
		// node can turn this tree into the constant-integer SRID that the
		// geometry decode accepts. Reaching the rejection verdict therefore
		// proves the wrapper executed nothing: the tree stays un-folded and
		// the decode reports its own constant-integer error.
		const llmWitnessSQL = "select st_geomfromtext('POINT(1 2)', cast(llm_chat('', '', 'echo', '', '" +
			`[{"role":"user","content":"4326"}]') as signed) + 0) as g limit 0`

		t.Run("the ordinary bind still evaluates the LLM witness while folding", func(t *testing.T) {
			// Ordinary planning keeps its own semantics: its post-bind fold
			// switch executes every unflagged builtin of a constant tree, so
			// the witness still decodes its SRID 4326 outside the probe.
			bindsWithSRID4326(t, context.Background(), llmWitnessSQL)
		})

		t.Run("the marked probe neither executes the LLM builtin nor admits the definition", func(t *testing.T) {
			rejectsWith(t, markedCtx, llmWitnessSQL,
				"the SRID argument of a geometry constructor must be a constant integer")
		})
	})
}

// countingUdfService records every UdfService.Run call the fold machinery
// performs during a bind, so a test can prove an internal-class builtin never
// reaches the UDF service during a publication binding probe.
type countingUdfService struct {
	runs int
}

func (s *countingUdfService) Language() string { return "python" }

func (s *countingUdfService) Run(
	_ context.Context, _ *udf.Request, _ udf.PkgReader,
) (*udf.Response, error) {
	s.runs++
	return nil, moerr.NewInternalErrorNoCtx("udf service unavailable in binding tests")
}

// TestPublicationViewBindingMarkerKeepsProbeLiteralStatements binds the
// probe's own literal shape with and without the marker: the probe is
// `select * from db.view limit 0` and only its literal clauses are bound,
// so the gated paths never change what the probe itself can prove.
func TestPublicationViewBindingMarkerKeepsProbeLiteralStatements(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select n_nationkey from tpch.nation limit 0", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)

	for _, ctx := range []context.Context{
		context.Background(),
		defines.WithPublicationViewBinding(context.Background()),
	} {
		compiler := NewMockCompilerContext(true, proc)
		compiler.SetContext(ctx)
		bound, err := BuildPlan(compiler, stmt.(*tree.Select), false)
		require.NoError(t, err)
		limitSeen := false
		for _, node := range bound.GetQuery().Nodes {
			if node.GetLimit() != nil {
				require.NotNil(t, node.GetLimit().GetLit(),
					"the literal LIMIT 0 of the probe binds unchanged as a literal")
				require.Equal(t, uint64(0), node.GetLimit().GetLit().GetU64Val())
				limitSeen = true
			}
		}
		require.True(t, limitSeen, "the probe's bound plan carries its LIMIT clause")
	}
}
