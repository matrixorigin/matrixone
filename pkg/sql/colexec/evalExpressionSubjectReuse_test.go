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

package colexec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func orLikeTestLiteral(value string) *plan.Expr {
	return &plan.Expr{Typ: types.New(types.T_varchar, int32(len(value)), 0).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}}}
}

func orLikeTestColumn(position int32) *plan.Expr {
	return &plan.Expr{Typ: types.New(types.T_varchar, 128, 0).PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: position}}}
}

func orLikeTestSubject(t testing.TB, proc *process.Process, nullSubstitution bool) *plan.Expr {
	t.Helper()
	input := orLikeTestColumn(0)
	if nullSubstitution {
		input = bindTestFunction(t, proc, "case", bindTestFunction(t, proc, "isnull", input), orLikeTestLiteral(""), proto.Clone(input).(*plan.Expr))
	}
	return bindTestFunction(t, proc, "lower", bindTestFunction(t, proc, "concat", input, orLikeTestLiteral(" ")))
}

func orLikeTestExpression(t testing.TB, proc *process.Process, subject *plan.Expr, terms int) *plan.Expr {
	t.Helper()
	patterns := []string{"%error%", "%timeout%", "%retry%"}
	leaves := make([]*plan.Expr, terms)
	for i := range leaves {
		leaves[i] = bindTestFunction(t, proc, "like", proto.Clone(subject).(*plan.Expr), orLikeTestLiteral(patterns[i%len(patterns)]))
	}
	return bindTestFunction(t, proc, "or", leaves...)
}

func orLikeTestRaw(t testing.TB, proc *process.Process, expression *plan.Expr) ExpressionExecutor {
	t.Helper()
	executor, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	return executor
}

func orLikeTestFilterExecutor(proc *process.Process, expression *plan.Expr) (ExpressionExecutor, error) {
	executor, err := NewExpressionExecutor(proc, expression)
	if err != nil {
		return nil, err
	}
	return ReuseFilterExpressionExecutor(expression, executor, orLikeMinFirstBatchRows), nil
}

func TestORLikeSubjectReuseExplicitFilterBoundary(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, true), 3)
	generic, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(generic.Free)
	require.IsType(t, &FunctionExpressionExecutor{}, generic)
	for _, build := range []func() ([]ExpressionExecutor, error){
		func() ([]ExpressionExecutor, error) {
			return NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expression})
		},
		func() ([]ExpressionExecutor, error) {
			return NewOwnedConstantFilterExecutors(proc, []*plan.Expr{expression})
		},
		func() ([]ExpressionExecutor, error) {
			return NewJoinBuildExpressionExecutors(proc, []*plan.Expr{expression}, nil, &DeferredJoinDiagnostic{})
		},
	} {
		executors, err := build()
		require.NoError(t, err)
		t.Cleanup(executors[0].Free)
		require.IsType(t, &FunctionExpressionExecutor{}, executors[0])
	}
	expressions := []*plan.Expr{expression, proto.Clone(expression).(*plan.Expr)}
	executors, err := NewExpressionExecutorsFromPlanExpressions(proc, expressions)
	require.NoError(t, err)
	for i, executor := range executors {
		executors[i] = ReuseFilterExpressionExecutor(expressions[i], executor, orLikeMinFirstBatchRows)
		t.Cleanup(executors[i].Free)
		require.IsType(t, &memoRootExpressionExecutor{}, executors[i])
	}
	require.NotSame(t, executors[0].(*memoRootExpressionExecutor).states[0], executors[1].(*memoRootExpressionExecutor).states[0])
	// A complete existing planner memo owner remains opaque at the new boundary.
	memo := proto.Clone(expression).(*plan.Expr)
	memo.AuxId = -1
	plannerOwned, err := NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{memo})
	require.NoError(t, err)
	t.Cleanup(plannerOwned[0].Free)
	originalOwner := plannerOwned[0]
	plannerOwned[0] = ReuseFilterExpressionExecutor(memo, plannerOwned[0], orLikeMinFirstBatchRows)
	require.Same(t, originalOwner, plannerOwned[0])
	owner := plannerOwned[0].(*memoRootExpressionExecutor)
	require.IsType(t, &memoExpressionExecutor{}, owner.executor)
	require.Len(t, owner.states, 1)
	require.IsType(t, &FunctionExpressionExecutor{}, owner.states[0].executor)
}

func orLikeTestBatch(t testing.TB, proc *process.Process, values []string, nulls []uint64) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nulls, proc.Mp())
	input.SetRowCount(len(values))
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	return input
}

func TestORLikeSubjectReuseInvocation(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	for _, nullSubstitution := range []bool{false, true} {
		t.Run(fmt.Sprintf("null_substitution=%t", nullSubstitution), func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, nullSubstitution), 3)
			executor, err := orLikeTestFilterExecutor(proc, expression)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			root, ok := executor.(*memoRootExpressionExecutor)
			require.True(t, ok, "eligible expression must have an OR-local owner")
			require.Len(t, root.states, 1)
			require.Equal(t, 3, root.states[0].refs)
			or := root.executor.(*FunctionExpressionExecutor)
			for _, leaf := range or.parameterExecutor {
				ref := leaf.(*FunctionExpressionExecutor).parameterExecutor[0].(*memoExpressionExecutor)
				require.Same(t, root.states[0], ref.state)
			}
			canonical := root.states[0].executor.(*FunctionExpressionExecutor)
			eval := canonical.evalFn
			calls := 0
			canonical.evalFn = func(parameters []*vector.Vector, result vector.FunctionResultWrapper, p *process.Process, length int, selection *function.FunctionSelectList) error {
				calls++
				return eval(parameters, result, p, length, selection)
			}
			_, err = DebugShowExecutor(executor)
			require.NoError(t, err)
			first := orLikeTestBatch(t, proc, []string{"ERROR", "ordinary", "TİMEOUT", "", "terror"}, []uint64{3})
			result, err := executor.Eval(proc, []*batch.Batch{first}, nil)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			for row, want := range []bool{true, false, true, false, true} {
				if row == 3 && !nullSubstitution {
					require.True(t, result.IsNull(uint64(row)))
				} else {
					require.False(t, result.IsNull(uint64(row)))
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
				}
			}
			second := orLikeTestBatch(t, proc, []string{"ordinary", "RETRY"}, nil)
			mask := []bool{true, false}
			result, err = executor.Eval(proc, []*batch.Batch{second}, mask)
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.False(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
			require.True(t, result.IsNull(1))
			mask[0], mask[1] = false, true
			result, err = executor.Eval(proc, []*batch.Batch{second}, mask)
			require.NoError(t, err)
			require.Equal(t, 3, calls)
			require.True(t, result.IsNull(0))
			require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 1))
			_, err = executor.Eval(proc, []*batch.Batch{second}, []bool{false, false})
			require.NoError(t, err)
			require.Equal(t, 3, calls, "inactive invocation must not demand the subject")
			_, err = DebugShowExecutor(executor)
			require.NoError(t, err)
		})
	}
}

func TestORLikeSubjectReuseAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		terms  int
		mutate func(*plan.Expr)
		want   bool
	}{
		{name: "two", terms: 2, want: true},
		{name: "eight", terms: 8, want: true},
		{name: "nine", terms: 9},
		{name: "different_column", terms: 3, mutate: func(e *plan.Expr) { e.GetF().Args[1].GetF().Args[0].GetF().Args[0].GetF().Args[0].GetCol().ColPos = 1 }},
		{name: "different_type_metadata", terms: 3, mutate: func(e *plan.Expr) { e.GetF().Args[1].GetF().Args[0].Typ.NotNullable = true }},
		{name: "different_positive_aux", terms: 3, mutate: func(e *plan.Expr) { e.GetF().Args[1].GetF().Args[0].AuxId = 1 }},
		{name: "existing_memo", terms: 3, mutate: func(e *plan.Expr) { e.GetF().Args[1].GetF().Args[0].AuxId = -1 }},
		{name: "literal_provenance_tree", terms: 3, mutate: func(e *plan.Expr) {
			e.GetF().Args[0].GetF().Args[0].GetF().Args[0].GetF().Args[1].GetLit().Src = orLikeTestLiteral("hidden")
		}},
		{name: "large_literal", terms: 3, mutate: func(e *plan.Expr) {
			e.GetF().Args[0].GetF().Args[1] = orLikeTestLiteral(strings.Repeat("a", orLikeMaxLiteralBytes+1))
		}},
		{name: "pattern_column", terms: 3, mutate: func(e *plan.Expr) { e.GetF().Args[0].GetF().Args[1] = orLikeTestColumn(0) }},
		{name: "parameter", terms: 3, mutate: func(e *plan.Expr) {
			e.GetF().Args[0].GetF().Args[0].GetF().Args[0].GetF().Args[0] = &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, false), test.terms)
			if test.mutate != nil {
				test.mutate(expression)
			}
			raw, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
			require.NoError(t, err)
			executor := reuseORLikeSubjects(expression, raw)
			t.Cleanup(executor.Free)
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, test.want, shared)
			if !test.want {
				require.Same(t, raw, executor)
			}
		})
	}
}

func TestORLikeSubjectReuseNecessaryMismatchDoesNotReplaceIdentity(t *testing.T) {
	for _, test := range []struct {
		name             string
		nullSubstitution bool
		second           func(testing.TB, *process.Process, *plan.Expr) *plan.Expr
	}{
		{"function_id", false, func(t testing.TB, p *process.Process, first *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "upper", proto.Clone(first.GetF().Args[0]).(*plan.Expr))
		}},
		{"function_arity", false, func(t testing.TB, p *process.Process, first *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "lower", bindTestFunction(t, p, "concat", orLikeTestColumn(0), orLikeTestLiteral(" "), orLikeTestLiteral(" ")))
		}},
		{"inner_literal_identity", false, func(t testing.TB, p *process.Process, first *plan.Expr) *plan.Expr {
			second := proto.Clone(first).(*plan.Expr)
			second.GetF().Args[0].GetF().Args[1].GetLit().Value = &plan.Literal_Sval{Sval: "_"}
			return second
		}},
		{"marker_identity", true, func(t testing.TB, p *process.Process, first *plan.Expr) *plan.Expr {
			second := proto.Clone(first).(*plan.Expr)
			second.GetF().Args[0].GetF().Args[0].PreparedNumeric = &plan.PreparedNumericMetadata{IfnullCommonValue: true}
			return second
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			first := orLikeTestSubject(t, proc, test.nullSubstitution)
			expression := orLikeTestExpression(t, proc, first, 3)
			expression.GetF().Args[1].GetF().Args[0] = test.second(t, proc, first)
			raw := orLikeTestRaw(t, proc, expression)
			require.Same(t, raw, reuseORLikeSubjects(expression, raw), "necessary checks must not replace full typed identity")
		})
	}
}

func TestORLikeSubjectReuseBudgetIsAtomic(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	eligible := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, false), 3)
	arguments := make([]*plan.Expr, 1, 1+orLikeDiscoveryBudget)
	arguments[0] = eligible
	for range orLikeDiscoveryBudget {
		arguments = append(arguments, makePlan2BoolConstExprWithType(true))
	}
	expression := bindTestFunction(t, proc, "and", arguments...)
	raw := orLikeTestRaw(t, proc, expression)
	first := raw.(*FunctionExpressionExecutor).parameterExecutor[0]
	require.Same(t, raw, reuseORLikeSubjects(expression, raw))
	require.Same(t, first, raw.(*FunctionExpressionExecutor).parameterExecutor[0], "a later overflow must not commit an earlier candidate")
}

func TestORLikeSubjectReuseNestedOwnerBudgetBoundary(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprintf("visits%d", orLikeDiscoveryBudget+extra), func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			eligible := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, false), 3)
			arguments := []*plan.Expr{eligible}
			// AND root + OR root + three (LIKE, pattern, four subject nodes)
			// consume 20 visits; the rest independently tests the whole-pass cap.
			for range orLikeDiscoveryBudget - 20 + extra {
				arguments = append(arguments, makePlan2BoolConstExprWithType(true))
			}
			expression := bindTestFunction(t, proc, "and", arguments...)
			raw, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
			require.NoError(t, err)
			originalOR := raw.(*FunctionExpressionExecutor).parameterExecutor[0]
			executor := reuseORLikeSubjects(expression, raw)
			t.Cleanup(executor.Free)
			require.Same(t, raw, executor, "nested replacement must retain the outer owner")
			child := raw.(*FunctionExpressionExecutor).parameterExecutor[0]
			if extra == 0 {
				owner, ok := child.(*memoRootExpressionExecutor)
				require.True(t, ok)
				require.Same(t, originalOR, owner.executor)
			} else {
				require.Same(t, originalOR, child, "late exhaustion must not commit an earlier group")
			}
			input := orLikeTestBatch(t, proc, []string{"ERROR", "ordinary"}, nil)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](result))
		})
	}
}

func TestORLikeSubjectReuseNullLiteralPayloadBounds(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
		edit func(*plan.Literal)
		want bool
	}{
		{name: "bytes4096", size: 2040, want: true},
		{name: "bytes4098", size: 2041},
		{name: "canonical_nil", edit: func(l *plan.Literal) { l.Value = nil }, want: true},
		{name: "hidden_source", edit: func(l *plan.Literal) { l.Src = orLikeTestLiteral("hidden") }},
		{name: "non_string_payload", edit: func(l *plan.Literal) { l.Value = &plan.Literal_I64Val{I64Val: 1} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			payload := orLikeTestLiteral(strings.Repeat("x", test.size))
			payload.GetLit().Isnull = true
			if test.edit != nil {
				test.edit(payload.GetLit())
			}
			subject := bindTestFunction(t, proc, "concat", orLikeTestColumn(0), payload)
			expression := orLikeTestExpression(t, proc, subject, 2)
			raw, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
			require.NoError(t, err)
			executor := reuseORLikeSubjects(expression, raw)
			t.Cleanup(executor.Free)
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, test.want, shared)
			input := orLikeTestBatch(t, proc, []string{"ERROR", "ordinary"}, nil)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.True(t, result.IsNull(0))
			require.True(t, result.IsNull(1))
		})
	}
}

func TestORLikeSubjectReuseSubjectBounds(t *testing.T) {
	for _, test := range []struct {
		name string
		make func(testing.TB, *process.Process) *plan.Expr
		want bool
	}{
		{"nodes64", func(t testing.TB, p *process.Process) *plan.Expr { return orLikeTestWideSubject(t, p, 64) }, true},
		{"nodes65", func(t testing.TB, p *process.Process) *plan.Expr { return orLikeTestWideSubject(t, p, 65) }, false},
		{"depth16", func(t testing.TB, p *process.Process) *plan.Expr { return orLikeTestDeepSubject(t, p, 16) }, true},
		{"depth17", func(t testing.TB, p *process.Process) *plan.Expr { return orLikeTestDeepSubject(t, p, 17) }, false},
		{"literal_bytes4096", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "concat", orLikeTestColumn(0), orLikeTestLiteral(strings.Repeat("a", 2040)))
		}, true},
		{"literal_bytes4098", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "concat", orLikeTestColumn(0), orLikeTestLiteral(strings.Repeat("a", 2041)))
		}, false},
		{"identity_bytes", func(t testing.TB, p *process.Process) *plan.Expr {
			c := orLikeTestColumn(0)
			c.GetCol().Name = strings.Repeat("a", orLikeMaxIdentityBytes)
			return bindTestFunction(t, p, "lower", c)
		}, false},
		{"constant", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "lower", orLikeTestLiteral("ERROR"))
		}, false},
		{"non_allowlisted_pure", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "upper", orLikeTestColumn(0))
		}, false},
		{"cast", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "lower", bindTestCast(t.(*testing.T), p, 0, orLikeTestColumn(0), types.T_text.ToType()))
		}, false},
		{"coalesce", func(t testing.TB, p *process.Process) *plan.Expr {
			return bindTestFunction(t, p, "lower", bindTestFunction(t, p, "coalesce", orLikeTestColumn(0), orLikeTestLiteral("fallback")))
		}, true},
		{"iff", func(t testing.TB, p *process.Process) *plan.Expr {
			c := orLikeTestColumn(0)
			return bindTestFunction(t, p, "lower", bindTestFunction(t, p, "if", bindTestFunction(t, p, "isnull", c), orLikeTestLiteral(""), proto.Clone(c).(*plan.Expr)))
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			expression := orLikeTestExpression(t, proc, test.make(t, proc), 2)
			raw, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
			require.NoError(t, err)
			executor := reuseORLikeSubjects(expression, raw)
			t.Cleanup(executor.Free)
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, test.want, shared)
		})
	}
}

func TestORLikeSubjectReuseCoalesceStringDomains(t *testing.T) {
	for _, test := range []struct {
		name   string
		oid    types.T
		legacy bool
	}{
		{"varchar", types.T_varchar, false},
		{"canonical_char", types.T_char, false},
		{"legacy_char", types.T_char, true},
		{"text", types.T_text, false},
		{"blob", types.T_blob, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			oid := test.oid
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			column := orLikeTestColumn(0)
			column.Typ = oid.ToType().PlanType()
			coalesce := bindTestFunction(t, proc, "coalesce", column, proto.Clone(column).(*plan.Expr))
			if test.legacy {
				// Exercise the still-registered CHAR kernel, rather than pretending
				// that the current public binder selects this retained identity.
				coalesce.GetF().Func.Obj = function.EncodeOverloadID(function.COALESCE, 1)
				coalesce.Typ = types.T_char.ToType().PlanType()
			}
			_, ordinal := function.DecodeOverloadID(coalesce.GetF().Func.Obj)
			t.Logf("registered %s COALESCE ordinal: %d", oid, ordinal)
			expression := orLikeTestExpression(t, proc, bindTestFunction(t, proc, "lower", coalesce), 3)
			raw, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			executor := raw
			t.Cleanup(func() { executor.Free() })
			calls := 0
			for _, leaf := range raw.(*FunctionExpressionExecutor).parameterExecutor {
				lower := leaf.(*FunctionExpressionExecutor).parameterExecutor[0].(*FunctionExpressionExecutor)
				eval := lower.evalFn
				lower.evalFn = func(parameters []*vector.Vector, result vector.FunctionResultWrapper, p *process.Process, length int, selection *function.FunctionSelectList) error {
					calls++
					return eval(parameters, result, p, length, selection)
				}
			}
			executor = ReuseFilterExpressionExecutor(expression, raw, orLikeMinFirstBatchRows)
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, oid != types.T_blob, shared, "only registered text-family COALESCE overloads qualify")
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(oid.ToType())
			values := []string{"ERROR", "ordinary", "TİMEOUT", "RETRY", ""}
			if oid == types.T_blob {
				// LOWER preserves bytes in the binary domain. Keep the fallback
				// control's match oracle independent of Unicode text folding.
				values = []string{"error", "ordinary", "timeout", "retry", ""}
			}
			for row, value := range values {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), row == 4, proc.Mp()))
			}
			input.SetRowCount(len(values))
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			wantCalls := 1
			if !shared {
				wantCalls = 3
			}
			require.Equal(t, wantCalls, calls)
			for row, want := range []bool{true, false, true, true} {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
			}
			require.True(t, result.IsNull(4))
		})
	}
}

func TestORLikeSubjectReuseRejectsMislabeledCoalesce(t *testing.T) {
	for _, oid := range []types.T{types.T_blob, types.T_decimal64} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			column := orLikeTestColumn(0)
			coalesce := bindTestFunction(t, proc, "coalesce", column, proto.Clone(column).(*plan.Expr))
			registered, err := function.GetFunctionByName(proc.Ctx, "coalesce", []types.Type{oid.ToType(), oid.ToType()})
			require.NoError(t, err)
			coalesce.GetF().Func.Obj = registered.GetEncodedOverloadID()
			expression := orLikeTestExpression(t, proc, bindTestFunction(t, proc, "lower", coalesce), 3)
			raw, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			t.Cleanup(raw.Free)
			// The synthetic identity/type mismatch proves defensive admission only;
			// never execute a numeric kernel with these string vectors.
			require.Same(t, raw, ReuseFilterExpressionExecutor(expression, raw, orLikeMinFirstBatchRows))
		})
	}
}

func TestORLikeSubjectReusePreservesLazyFolding(t *testing.T) {
	for _, test := range []struct {
		name   string
		make   func(testing.TB, *process.Process, *plan.Expr) *plan.Expr
		shared bool
		folded bool
	}{
		{"constant_prefix", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "coalesce", orLikeTestLiteral("error"), col)
		}, false, true},
		{"null_then_constant", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			null := orLikeTestLiteral("")
			null.GetLit().Isnull = true
			return bindTestFunction(t, p, "coalesce", null, orLikeTestLiteral("error"), col)
		}, false, true},
		{"constant_function_prefix", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "coalesce", bindTestFunction(t, p, "lower", orLikeTestLiteral("ERROR")), col)
		}, false, true},
		{"outer_lower", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "lower", bindTestFunction(t, p, "coalesce", orLikeTestLiteral("ERROR"), col))
		}, false, true},
		{"outer_concat", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "concat", bindTestFunction(t, p, "coalesce", orLikeTestLiteral("error"), col), orLikeTestLiteral(" "))
		}, false, true},
		{"nested_constant_prefix", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			inner := bindTestFunction(t, p, "coalesce", orLikeTestLiteral("error"), col)
			return bindTestFunction(t, p, "coalesce", inner, proto.Clone(col).(*plan.Expr))
		}, false, true},
		{"live_first", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "coalesce", col, orLikeTestLiteral("ordinary"))
		}, true, false},
		{"null_then_live", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			null := orLikeTestLiteral("")
			null.GetLit().Isnull = true
			return bindTestFunction(t, p, "coalesce", null, col, orLikeTestLiteral("ordinary"))
		}, true, false},
		{"constant_function_null_prefix", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			null := orLikeTestLiteral("")
			null.GetLit().Isnull = true
			return bindTestFunction(t, p, "coalesce", bindTestFunction(t, p, "lower", null), col)
		}, false, false},
		{"constant_branch_and_live_sibling", func(t testing.TB, p *process.Process, col *plan.Expr) *plan.Expr {
			return bindTestFunction(t, p, "concat", bindTestFunction(t, p, "coalesce", orLikeTestLiteral(""), col), proto.Clone(col).(*plan.Expr))
		}, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			expression := orLikeTestExpression(t, proc, test.make(t, proc, orLikeTestColumn(0)), 3)
			generic, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			t.Cleanup(generic.Free)
			raw, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			executor := raw
			t.Cleanup(func() { executor.Free() })
			executor = ReuseFilterExpressionExecutor(expression, raw, orLikeMinFirstBatchRows)
			input := orLikeTestBatch(t, proc, []string{"error", "ordinary", "timeout", "retry"}, nil)
			for invocation := 0; invocation < 2; invocation++ {
				control, err := generic.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, test.folded, control.IsConst(), "generic folding is the independent constness oracle")
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, control.IsConst(), result.IsConst(), "sharing must preserve the existing const-bool path")
				for row := 0; row < input.RowCount(); row++ {
					require.False(t, result.IsNull(uint64(row)))
					want := test.folded || row != 1
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](control, row))
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
				}
			}
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, test.shared, shared)
			if !shared {
				require.Same(t, raw, executor)
			}
		})
	}
}

func orLikeTestWideSubject(t testing.TB, proc *process.Process, nodes int) *plan.Expr {
	arguments := []*plan.Expr{orLikeTestColumn(0)}
	for len(arguments) < nodes-1 {
		arguments = append(arguments, orLikeTestLiteral(""))
	}
	return bindTestFunction(t, proc, "concat", arguments...)
}

func orLikeTestDeepSubject(t testing.TB, proc *process.Process, nodes int) *plan.Expr {
	subject := orLikeTestColumn(0)
	for range nodes - 1 {
		subject = bindTestFunction(t, proc, "lower", subject)
	}
	return subject
}

func TestORLikeSubjectReuseMarkerAndOwnerBarriers(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*testing.T, *plan.Expr)
		want bool
	}{
		{"canonical_marker", func(t *testing.T, s *plan.Expr) {
			s.GetF().Args[0].GetF().Args[0].PreparedNumeric = &plan.PreparedNumericMetadata{IfnullCommonValue: true}
		}, true},
		{"additional_marker", func(t *testing.T, s *plan.Expr) {
			s.GetF().Args[0].GetF().Args[0].PreparedNumeric = &plan.PreparedNumericMetadata{IfnullCommonValue: true, ProjectedCommonValue: true}
		}, false},
		{"hidden_marker_expression", func(t *testing.T, s *plan.Expr) {
			s.GetF().Args[0].GetF().Args[0].PreparedNumeric = &plan.PreparedNumericMetadata{IfnullCommonValue: true, StringDomainSource: orLikeTestLiteral("hidden")}
		}, false},
		{"unknown_marker_field", func(t *testing.T, s *plan.Expr) {
			m := &plan.PreparedNumericMetadata{}
			require.NoError(t, proto.Unmarshal([]byte{96, 1, 248, 7, 1}, m))
			s.GetF().Args[0].GetF().Args[0].PreparedNumeric = m
		}, false},
		{"marker_on_lower", func(t *testing.T, s *plan.Expr) {
			s.PreparedNumeric = &plan.PreparedNumericMetadata{IfnullCommonValue: true}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			subject := orLikeTestSubject(t, proc, true)
			test.edit(t, subject)
			expression := orLikeTestExpression(t, proc, subject, 3)
			raw, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
			require.NoError(t, err)
			executor := reuseORLikeSubjects(expression, raw)
			t.Cleanup(executor.Free)
			_, shared := executor.(*memoRootExpressionExecutor)
			require.Equal(t, test.want, shared)
		})
	}
	t.Run("diagnostic_owner_construction", func(t *testing.T) {
		proc := testutil.NewProcess(t, testutil.WithFileService(nil))
		checkExpressionStorageAfterCleanup(t, proc)
		expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, true), 3)
		executors, activation, err := NewJoinProbeExpressionExecutors(proc, []*plan.Expr{expression}, nil, &DeferredJoinDiagnostic{})
		require.NoError(t, err)
		t.Cleanup(executors[0].Free)
		require.Empty(t, activation)
		_, shared := executors[0].(*memoRootExpressionExecutor)
		require.False(t, shared)
	})
}

func TestORLikeSubjectReuseAccountFailureAndBuildRollback(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(4 << 10)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, account.Seal().Used)
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, true), 3)
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, selection)
	require.NoError(t, err)
	// Exercise selected-vector ownership through the same postpass directly;
	// the static-filter public API deliberately has no allocation override.
	executor = reuseORLikeSubjects(expression, executor)
	t.Cleanup(executor.Free)
	root := executor.(*memoRootExpressionExecutor)
	canonical := root.states[0].executor.(*FunctionExpressionExecutor)
	small := orLikeTestBatch(t, proc, []string{"ERROR", "ordinary"}, nil)
	large := orLikeTestBatch(t, proc, []string{strings.Repeat("A", 2048), strings.Repeat("A", 2048), strings.Repeat("A", 2048), strings.Repeat("A", 2048)}, nil)
	_, err = executor.Eval(proc, []*batch.Batch{large}, []bool{true, false, true, false})
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, root.states[0].cached)
	result, err := executor.Eval(proc, []*batch.Batch{small}, nil)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](result))
	require.Same(t, selection, canonical.resultVector.GetResultVector().AllocationAccountSelection())
	invalid := orLikeTestLiteral("invalid")
	invalid.GetLit().StringSource = 257
	before := proc.Mp().CurrNB()
	failed, err := NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expression, invalid})
	require.Nil(t, failed)
	require.ErrorContains(t, err, "invalid literal string source")
	require.Equal(t, before, proc.Mp().CurrNB())
}

func TestORLikeSubjectReuseIndependentCASEArms(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	subject := orLikeTestSubject(t, proc, false)
	makeArm := func(first, second string) *plan.Expr {
		return bindTestFunction(t, proc, "or", bindTestFunction(t, proc, "like", proto.Clone(subject).(*plan.Expr), orLikeTestLiteral(first)), bindTestFunction(t, proc, "like", proto.Clone(subject).(*plan.Expr), orLikeTestLiteral(second)))
	}
	flag := &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
	expression := bindTestFunction(t, proc, "case", flag, makeArm("%a%", "%x%"), makeArm("%b%", "%y%"))
	executor, err := orLikeTestFilterExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	root := executor.(*FunctionExpressionExecutor)
	left := root.parameterExecutor[1].(*memoRootExpressionExecutor)
	right := root.parameterExecutor[2].(*memoRootExpressionExecutor)
	require.NotSame(t, left.states[0], right.states[0])
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = testutil.MakeVarcharVector([]string{"a", "b"}, nil, proc.Mp())
	input.Vecs[1] = testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false})
	input.SetRowCount(2)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, vector.MustFixedColNoTypeCheck[bool](result))
}

func TestORLikeSubjectReuseErrorRecoveryAndTransfer(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	expression := orLikeTestExpression(t, proc, orLikeTestSubject(t, proc, true), 3)
	executor, err := orLikeTestFilterExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor != nil {
			executor.Free()
		}
	})
	root := executor.(*memoRootExpressionExecutor)
	subject := root.states[0].executor.(*FunctionExpressionExecutor)
	realEval := subject.evalFn
	failure := moerr.NewInvalidInputNoCtx("injected subject failure")
	subject.evalFn = func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *function.FunctionSelectList) error {
		return failure
	}
	input := orLikeTestBatch(t, proc, []string{"error", "ordinary"}, nil)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, root.states[0].cached)
	subject.evalFn = realEval
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](result))
	late := root.executor.(*FunctionExpressionExecutor).parameterExecutor[1].(*FunctionExpressionExecutor)
	lateEval := late.evalFn
	late.evalFn = func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *function.FunctionSelectList) error {
		return failure
	}
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, failure)
	late.evalFn = lateEval
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	executor.ResetForNextQuery()
	owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { owned.Free(proc.Mp()) })
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](owned))
	writable, err := GetWritableResultFromExpression(proc, expression, []*batch.Batch{input})
	require.NoError(t, err)
	t.Cleanup(func() { writable.Free(proc.Mp()) })
	executor.Free()
	executor = nil
	// Both transferred results remain readable after their executors are freed.
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](owned))
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](writable))
}

func BenchmarkORLikeSubjectReuse(b *testing.B) {
	for _, value := range []struct{ name, text string }{{"short", "ordinary ERROR"}, {"long", strings.Repeat("ordinary text ", 128) + " TIMEOUT"}, {"unicode", strings.Repeat("中文TİMEOUT ", 64)}} {
		for _, terms := range []int{2, 3, 8} {
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/terms%d/reuse%t", value.name, terms, reuse), func(b *testing.B) {
					proc := testutil.NewProcess(b, testutil.WithFileService(nil))
					expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), terms)
					executor, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
					require.NoError(b, err)
					if reuse {
						executor.Free()
						executor, err = orLikeTestFilterExecutor(proc, expression)
						require.NoError(b, err)
						_, shared := executor.(*memoRootExpressionExecutor)
						require.True(b, shared)
					}
					defer executor.Free()
					input := orLikeTestBatch(b, proc, []string{value.text, value.text, value.text, value.text}, nil)
					batches := []*batch.Batch{input}
					_, err = executor.Eval(proc, batches, nil)
					require.NoError(b, err)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := executor.Eval(proc, batches, nil); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}

func BenchmarkORLikeSubjectReuseConstruction(b *testing.B) {
	for _, name := range []string{"lower", "constant", "arithmetic", "different_subjects"} {
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/public%t", name, reuse), func(b *testing.B) {
				proc := testutil.NewProcess(b, testutil.WithFileService(nil))
				var expression *plan.Expr
				switch name {
				case "lower":
					expression = bindTestFunction(b, proc, "lower", orLikeTestColumn(0))
				case "constant":
					expression = orLikeTestLiteral("constant")
				case "arithmetic":
					column := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
					expression = bindTestFunction(b, proc, "+", column, makePlan2Int64ConstExprWithType(1))
				case "different_subjects":
					expression = orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, false), 3)
					expression.GetF().Args[1].GetF().Args[0].GetF().Args[0].GetF().Args[0].GetCol().ColPos = 1
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var executor ExpressionExecutor
					var err error
					if reuse {
						executor, err = NewExpressionExecutor(proc, expression)
					} else {
						executor, err = orLikeBenchmarkLegacyExecutor(proc, expression)
					}
					if err != nil {
						b.Fatal(err)
					}
					executor.Free()
				}
				b.StopTimer()
			})
		}
	}
}

func BenchmarkORLikeSubjectReuseBatch(b *testing.B) {
	for _, rows := range []int{256, 8192} {
		for _, partial := range []bool{false, true} {
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows%d/partial%t/reuse%t", rows, partial, reuse), func(b *testing.B) {
					proc := testutil.NewProcess(b, testutil.WithFileService(nil))
					expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), 3)
					executor, err := newExpressionExecutorWithAllocation(proc, expression, nil, &expressionExecutorBuildContext{})
					require.NoError(b, err)
					if reuse {
						executor.Free()
						executor, err = orLikeTestFilterExecutor(proc, expression)
						require.NoError(b, err)
					}
					defer executor.Free()
					values := make([]string, rows)
					for i := range values {
						values[i] = strings.Repeat("ordinary 中文 ", 32) + " TIMEOUT"
					}
					input := orLikeTestBatch(b, proc, values, nil)
					batches := []*batch.Batch{input}
					var mask []bool
					if partial {
						mask = make([]bool, rows)
						for i := range mask {
							mask[i] = i%2 == 0
						}
					}
					_, err = executor.Eval(proc, batches, mask)
					require.NoError(b, err)
					b.ReportAllocs()
					b.SetBytes(int64(len(values[0]) * rows))
					b.ResetTimer()
					for range b.N {
						if _, err := executor.Eval(proc, batches, mask); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}

func BenchmarkORLikeSubjectReuseWritable(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprintf("public%t", reuse), func(b *testing.B) {
			proc := testutil.NewProcess(b, testutil.WithFileService(nil))
			expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), 3)
			values := make([]string, 256)
			for i := range values {
				values[i] = "ordinary ERROR"
			}
			input := orLikeTestBatch(b, proc, values, nil)
			batches := []*batch.Batch{input}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := orLikeBenchmarkWritable(proc, expression, batches, reuse)
				if err != nil {
					b.Fatal(err)
				}
				result.Free(proc.Mp())
			}
			b.StopTimer()
		})
	}
}

func BenchmarkORLikeSubjectReuseWritableSmall(b *testing.B) {
	for _, rows := range []int{1, 16} {
		for _, matches := range []bool{false, true} {
			for _, reuse := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows%d/matches%t/public%t", rows, matches, reuse), func(b *testing.B) {
					proc := testutil.NewProcess(b, testutil.WithFileService(nil))
					expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), 3)
					probe, err := NewExpressionExecutor(proc, expression)
					require.NoError(b, err)
					_, shared := probe.(*memoRootExpressionExecutor)
					require.False(b, shared, "generic constructor must bypass subject reuse")
					probe.Free()
					text := "ordinary plain"
					if matches {
						text = "ordinary ERROR"
					}
					values := make([]string, rows)
					for i := range values {
						values[i] = text
					}
					input := orLikeTestBatch(b, proc, values, nil)
					batches := []*batch.Batch{input}
					result, err := orLikeBenchmarkWritable(proc, expression, batches, reuse)
					require.NoError(b, err)
					actual := vector.MustFixedColNoTypeCheck[bool](result)
					require.Len(b, actual, rows)
					for _, value := range actual {
						require.Equal(b, matches, value)
					}
					result.Free(proc.Mp())
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						result, err := orLikeBenchmarkWritable(proc, expression, batches, reuse)
						if err != nil {
							b.Fatal(err)
						}
						result.Free(proc.Mp())
					}
					b.StopTimer()
				})
			}
		}
	}
}

// Compare the real public writable API against the original generic construction
// and result-transfer path. Neither path opts in to persistent-filter reuse.
func orLikeBenchmarkWritable(proc *process.Process, expression *plan.Expr, batches []*batch.Batch, publicAPI bool) (*vector.Vector, error) {
	if publicAPI {
		return GetWritableResultFromExpression(proc, expression, batches)
	}
	executor, err := orLikeBenchmarkLegacyExecutor(proc, expression)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	result, err := executor.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if !modifyResultOwnerToOuter(executor) {
		return result.Dup(proc.Mp())
	}
	return result, nil
}

// Preserve the exact pre-feature generic collation, build, and planner-memo finish.
func orLikeBenchmarkLegacyExecutor(proc *process.Process, expression *plan.Expr) (ExpressionExecutor, error) {
	if err := plan.RequireLegacyCollations(expression); err != nil {
		return nil, err
	}
	build := &expressionExecutorBuildContext{}
	executor, err := newExpressionExecutorWithAllocation(proc, expression, nil, build)
	if err != nil || len(build.states) == 0 {
		return executor, err
	}
	return &memoRootExpressionExecutor{executor: executor, states: build.states}, nil
}

func BenchmarkORLikeSubjectReuseFilterSetup(b *testing.B) {
	for _, admitted := range []bool{false, true} {
		b.Run(fmt.Sprintf("filter%t", admitted), func(b *testing.B) {
			proc := testutil.NewProcess(b, testutil.WithFileService(nil))
			expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), 3)
			expressions := []*plan.Expr{expression}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var executors []ExpressionExecutor
				var err error
				executors, err = NewExpressionExecutorsFromPlanExpressions(proc, expressions)
				if admitted && err == nil {
					executors[0] = ReuseFilterExpressionExecutor(expression, executors[0], orLikeMinFirstBatchRows)
				}
				if err != nil {
					b.Fatal(err)
				}
				executors[0].Free()
			}
			b.StopTimer()
		})
	}
}

func BenchmarkORLikeSubjectReuseReadonlySmall(b *testing.B) {
	for _, rows := range []int{1, 16} {
		for _, matches := range []bool{false, true} {
			for _, publicAPI := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows%d/matches%t/public%t", rows, matches, publicAPI), func(b *testing.B) {
					proc := testutil.NewProcess(b, testutil.WithFileService(nil))
					expression := orLikeTestExpression(b, proc, orLikeTestSubject(b, proc, true), 3)
					text := "ordinary plain"
					if matches {
						text = "ordinary ERROR"
					}
					values := make([]string, rows)
					for i := range values {
						values[i] = text
					}
					input := orLikeTestBatch(b, proc, values, nil)
					batches := []*batch.Batch{input}
					result, free, err := orLikeBenchmarkReadonly(proc, expression, batches, publicAPI)
					require.NoError(b, err)
					actual := vector.MustFixedColNoTypeCheck[bool](result)
					require.Len(b, actual, rows)
					for _, value := range actual {
						require.Equal(b, matches, value)
					}
					free()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						_, free, err := orLikeBenchmarkReadonly(proc, expression, batches, publicAPI)
						if err != nil {
							b.Fatal(err)
						}
						free()
					}
					b.StopTimer()
				})
			}
		}
	}
}

func orLikeBenchmarkReadonly(proc *process.Process, expression *plan.Expr, batches []*batch.Batch, publicAPI bool) (*vector.Vector, func(), error) {
	if publicAPI {
		return GetReadonlyResultFromExpression(proc, expression, batches)
	}
	executor, err := orLikeBenchmarkLegacyExecutor(proc, expression)
	if err != nil {
		return nil, nil, err
	}
	result, err := executor.Eval(proc, batches, nil)
	if err != nil {
		executor.Free()
		return nil, nil, err
	}
	return result, executor.Free, nil
}
