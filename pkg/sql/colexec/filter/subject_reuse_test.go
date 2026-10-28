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

package filter

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func orLikeOperatorBind(t testing.TB, proc *process.Process, name string, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		var err error
		argTypes[i], err = types.TypeFromPlan(arg.Typ)
		require.NoError(t, err)
	}
	resolved, err := function.GetFunctionByName(proc.Ctx, name, argTypes)
	require.NoError(t, err)
	typ := resolved.GetReturnType()
	resultType := typ.PlanType()
	resultType.NotNullable = typ.GetNotNull()
	return &plan.Expr{Typ: resultType, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: name}, Args: args}}}
}
func orLikeOperatorLiteral(text string) *plan.Expr {
	return &plan.Expr{Typ: types.New(types.T_varchar, int32(len(text)), 0).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}}}
}
func orLikeOperatorExpression(t testing.TB, proc *process.Process) *plan.Expr {
	col := &plan.Expr{Typ: types.New(types.T_varchar, 128, 0).PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	subject := orLikeOperatorBind(t, proc, "lower", orLikeOperatorBind(t, proc, "concat", col, orLikeOperatorLiteral(" ")))
	leaves := make([]*plan.Expr, 3)
	for i, p := range []string{"%error%", "%timeout%", "%retry%"} {
		leaves[i] = orLikeOperatorBind(t, proc, "like", proto.Clone(subject).(*plan.Expr), orLikeOperatorLiteral(p))
	}
	return orLikeOperatorBind(t, proc, "or", leaves...)
}
func orLikeOperatorOne(t testing.TB, proc *process.Process) *plan.Expr {
	col := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
	zero := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
	return orLikeOperatorBind(t, proc, "<=", col, zero)
}

// The caller owns this batch. Filter fixtures transfer that ownership to MockOperator.
func orLikeOperatorBatch(t testing.TB, proc *process.Process, rows int, mode string) (*batch.Batch, []int64) {
	values := make([]string, rows)
	ids := make([]int64, rows)
	var nulls []uint64
	want := make([]int64, 0, rows)
	for i := range values {
		ids[i] = int64(i)
		switch mode {
		case "pattern":
			values[i] = []string{"ERROR", "ordinary", "TİMEOUT", "RETRY", ""}[i%5]
			if i%5 == 4 {
				nulls = append(nulls, uint64(i))
			}
			if i%5 == 0 || i%5 == 2 || i%5 == 3 {
				want = append(want, int64(i))
			}
		case "match":
			values[i] = "ordinary ERROR"
			want = append(want, int64(i))
		case "miss":
			values[i] = "ordinary plain"
		default:
			t.Fatalf("unknown mode %s", mode)
		}
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nulls, proc.Mp())
	input.Vecs[1] = testutil.MakeInt64Vector(ids, nil, proc.Mp())
	input.SetRowCount(rows)
	return input, want
}
func orLikeOperatorShared(executor colexec.ExpressionExecutor) bool {
	return fmt.Sprintf("%T", executor) == "*colexec.memoRootExpressionExecutor"
}
func orLikeOperatorIDs(t testing.TB, input *batch.Batch, want []int64) {
	require.NotNil(t, input)
	require.Equal(t, len(want), input.RowCount())
	if len(want) > 0 {
		require.Equal(t, want, vector.MustFixedColNoTypeCheck[int64](input.Vecs[1]))
	}
}

func TestORLikeSubjectReuseFilterLifetime(t *testing.T) {
	for _, rows := range []int{1, 16, 1023, 1024} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			proc := testutil.NewProc(t)
			expr := orLikeOperatorExpression(t, proc)
			arg := &Filter{FilterExprs: []*plan.Expr{expr}}
			first, want := orLikeOperatorBatch(t, proc, rows, "pattern")
			second, wantSecond := orLikeOperatorBatch(t, proc, 1024, "match")
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
			arg.AppendChild(child)
			t.Cleanup(func() { arg.Free(proc, false, nil); child.Free(proc, false, nil); require.Zero(t, proc.Mp().CurrNB()) })
			require.NoError(t, arg.Prepare(proc))
			require.False(t, orLikeOperatorShared(arg.ctr.executors[0]))
			require.False(t, arg.ctr.subjectReuseAdmissionDone)
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			orLikeOperatorIDs(t, result.Batch, want)
			owner := arg.ctr.executors[0]
			require.Equal(t, rows >= 1024, orLikeOperatorShared(owner))
			require.Same(t, owner, arg.ctr.allExecutors[0])
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			result, err = vm.Exec(arg, proc)
			require.NoError(t, err)
			orLikeOperatorIDs(t, result.Batch, wantSecond)
			require.Same(t, owner, arg.ctr.executors[0])
			arg.Reset(proc, false, nil)
			child.Free(proc, false, nil)
			next, wantNext := orLikeOperatorBatch(t, proc, 1024, "pattern")
			child.WithBatchs([]*batch.Batch{next})
			require.NoError(t, arg.Prepare(proc))
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			require.Same(t, owner, arg.ctr.executors[0])
			result, err = vm.Exec(arg, proc)
			require.NoError(t, err)
			orLikeOperatorIDs(t, result.Batch, wantNext)
			require.Same(t, owner, arg.ctr.executors[0])
			arg.Free(proc, false, nil)
			require.False(t, arg.ctr.subjectReuseAdmissionDone)
			child.Free(proc, false, nil)
			arg = &Filter{FilterExprs: []*plan.Expr{expr}}
			last, wantLast := orLikeOperatorBatch(t, proc, 1024, "match")
			child.WithBatchs([]*batch.Batch{last})
			arg.AppendChild(child)
			require.NoError(t, arg.Prepare(proc))
			result, err = vm.Exec(arg, proc)
			require.NoError(t, err)
			orLikeOperatorIDs(t, result.Batch, wantLast)
			require.True(t, orLikeOperatorShared(arg.ctr.executors[0]))
		})
	}
}
func TestORLikeSubjectReuseFilterEmptyBeforeDecision(t *testing.T) {
	proc := testutil.NewProc(t)
	expr := orLikeOperatorExpression(t, proc)
	arg := &Filter{FilterExprs: []*plan.Expr{expr}}
	empty, _ := orLikeOperatorBatch(t, proc, 0, "match")
	large, want := orLikeOperatorBatch(t, proc, 1024, "pattern")
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{empty, large})
	arg.AppendChild(child)
	t.Cleanup(func() { arg.Free(proc, false, nil); child.Free(proc, false, nil); require.Zero(t, proc.Mp().CurrNB()) })
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.False(t, arg.ctr.subjectReuseAdmissionDone)
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	orLikeOperatorIDs(t, result.Batch, want)
	require.True(t, orLikeOperatorShared(arg.ctr.executors[0]))
}
func TestORLikeSubjectReuseFilterPrecedingShrinkAndExclusions(t *testing.T) {
	for _, kind := range []string{"runtime_shrink", "static_shrink", "earlier_admitted", "runtime_excluded", "owned_excluded", "unreached_empty", "unreached_error"} {
		t.Run(kind, func(t *testing.T) {
			proc := testutil.NewProc(t)
			expr := orLikeOperatorExpression(t, proc)
			arg := &Filter{FilterExprs: []*plan.Expr{expr}}
			target := 0
			switch kind {
			case "runtime_shrink":
				arg.RuntimeFilterExprs = []*plan.Expr{orLikeOperatorOne(t, proc)}
			case "static_shrink":
				arg.FilterExprs = []*plan.Expr{orLikeOperatorOne(t, proc), expr}
				target = 1
			case "earlier_admitted":
				arg.FilterExprs = []*plan.Expr{proto.Clone(expr).(*plan.Expr), orLikeOperatorOne(t, proc), expr}
				target = 2
			case "runtime_excluded":
				arg.RuntimeFilterExprs = []*plan.Expr{proto.Clone(expr).(*plan.Expr)}
			case "owned_excluded":
				arg.OwnsConstantCastWarnings = true
			case "unreached_empty":
				arg.FilterExprs = []*plan.Expr{{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}, expr}
				target = 1
			case "unreached_error":
				arg.FilterExprs = []*plan.Expr{orLikeOperatorLiteral("bad"), expr}
				target = 1
			}
			input, want := orLikeOperatorBatch(t, proc, 8192, "match")
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
			arg.AppendChild(child)
			t.Cleanup(func() { arg.Free(proc, false, nil); child.Free(proc, false, nil); require.Zero(t, proc.Mp().CurrNB()) })
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			if kind == "unreached_error" {
				require.ErrorContains(t, err, "filter condition is not boolean")
			} else {
				require.NoError(t, err)
				if kind == "runtime_shrink" || kind == "static_shrink" || kind == "earlier_admitted" {
					want = []int64{0}
				}
				if kind == "unreached_empty" {
					want = nil
				}
				orLikeOperatorIDs(t, result.Batch, want)
			}
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			shared := kind == "runtime_excluded"
			require.Equal(t, shared, orLikeOperatorShared(arg.ctr.executors[target]))
			require.Same(t, arg.ctr.executors[target], arg.ctr.allExecutors[len(arg.ctr.runtimeExecutors)+target])
			if kind == "earlier_admitted" {
				require.True(t, orLikeOperatorShared(arg.ctr.executors[0]))
			}
			for _, executor := range arg.ctr.runtimeExecutors {
				require.False(t, orLikeOperatorShared(executor))
			}
			arg.Reset(proc, err != nil, err)
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			require.Equal(t, shared, orLikeOperatorShared(arg.ctr.executors[target]))
		})
	}
}
func BenchmarkORLikeSubjectReuseFilterFirstBatch(b *testing.B) {
	for _, rows := range []int{1, 16, 1023, 1024, 8192} {
		for _, mode := range []string{"match", "miss"} {
			for _, baseline := range []bool{true, false} {
				b.Run(fmt.Sprintf("rows%d/mode%s/shrink%t/baseline%t", rows, mode, rows == 8192, baseline), func(b *testing.B) {
					proc := testutil.NewProc(b)
					expr := orLikeOperatorExpression(b, proc)
					one := orLikeOperatorOne(b, proc)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						input, want := orLikeOperatorBatch(b, proc, rows, mode)
						child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
						arg := &Filter{FilterExprs: []*plan.Expr{expr}}
						if rows == 8192 {
							arg.RuntimeFilterExprs = []*plan.Expr{one}
							if mode == "match" {
								want = []int64{0}
							} else {
								want = nil
							}
						}
						arg.AppendChild(child)
						b.StartTimer()
						err := arg.Prepare(proc)
						if err != nil {
							b.Fatal(err)
						}
						if baseline {
							arg.ctr.subjectReuseAdmissionDone = true
						}
						result, err := vm.Exec(arg, proc)
						if err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						orLikeOperatorIDs(b, result.Batch, want)
						require.Equal(b, !baseline && rows >= 1024 && rows != 8192, orLikeOperatorShared(arg.ctr.executors[0]))
						b.StartTimer()
						arg.Free(proc, false, nil)
						child.Free(proc, false, nil)
					}
					b.StopTimer()
					require.Zero(b, proc.Mp().CurrNB())
				})
			}
		}
	}
}
