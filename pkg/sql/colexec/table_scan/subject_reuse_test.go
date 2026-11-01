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

package table_scan

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

func orLikeScanArgument(expression *plan.Expr) *TableScan {
	return &TableScan{Attrs: []string{"text", "id", "payload"}, Types: []plan.Type{types.New(types.T_varchar, 128, 0).PlanType(), types.T_int64.ToType().PlanType(), types.T_text.ToType().PlanType()}, FilterExprs: []*plan.Expr{expression}}
}
func orLikeScanBatch(t testing.TB, proc *process.Process, rows int, mode string, late bool) (*batch.Batch, []int64) {
	input, want := orLikeOperatorBatch(t, proc, rows, mode)
	payload := vector.NewVec(types.T_text.ToType())
	input.Vecs = append(input.Vecs, payload)
	if !late {
		for range rows {
			require.NoError(t, vector.AppendBytes(payload, []byte("payload"), false, proc.Mp()))
		}
	}
	return input, want
}
func orLikeScanEval(t testing.TB, proc *process.Process, arg *TableScan, input *batch.Batch, late bool, want []int64) {
	before := input.RowCount()
	if late {
		require.NotNil(t, arg.ctr.readerFilter)
		result, err := arg.ctr.readerFilter(input, []int{0, 1})
		require.NoError(t, err)
		require.Zero(t, input.Vecs[2].Length())
		if len(want) == before {
			require.True(t, result.All)
		} else {
			require.False(t, result.All)
			if len(want) == 0 {
				require.Empty(t, result.Sels)
			} else {
				require.Equal(t, want, result.Sels)
			}
		}
	} else {
		_, err := arg.evalFilter(proc, input, nil)
		require.NoError(t, err)
	}
	orLikeOperatorIDs(t, input, want)
}
func TestORLikeSubjectReuseTableScanLifetime(t *testing.T) {
	for _, rows := range []int{1, 16, 1023, 1024} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			proc := testutil.NewProc(t)
			expr := orLikeOperatorExpression(t, proc)
			arg := orLikeScanArgument(expr)
			batches := make([]*batch.Batch, 0, 4)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				for _, b := range batches {
					b.Clean(proc.Mp())
				}
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.Prepare(proc))
			require.False(t, orLikeOperatorShared(arg.ctr.filterExecutors[0]))
			require.False(t, arg.ctr.subjectReuseAdmissionDone)
			first, want := orLikeScanBatch(t, proc, rows, "pattern", true)
			batches = append(batches, first)
			orLikeScanEval(t, proc, arg, first, true, want)
			owner := arg.ctr.filterExecutors[0]
			require.Equal(t, rows >= 1024, orLikeOperatorShared(owner))
			require.Same(t, owner, arg.ctr.allFilterExecutors[0])
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			next, wantNext := orLikeScanBatch(t, proc, 1024, "match", false)
			batches = append(batches, next)
			orLikeScanEval(t, proc, arg, next, false, wantNext)
			require.Same(t, owner, arg.ctr.filterExecutors[0])
			arg.Reset(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			require.Same(t, owner, arg.ctr.filterExecutors[0])
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			afterReset, wantReset := orLikeScanBatch(t, proc, 1024, "pattern", true)
			batches = append(batches, afterReset)
			orLikeScanEval(t, proc, arg, afterReset, true, wantReset)
			require.Same(t, owner, arg.ctr.filterExecutors[0])
			arg.Free(proc, false, nil)
			require.False(t, arg.ctr.subjectReuseAdmissionDone)
			arg = orLikeScanArgument(expr)
			require.NoError(t, arg.Prepare(proc))
			fresh, wantFresh := orLikeScanBatch(t, proc, 1024, "match", true)
			batches = append(batches, fresh)
			orLikeScanEval(t, proc, arg, fresh, true, wantFresh)
			require.True(t, orLikeOperatorShared(arg.ctr.filterExecutors[0]))
		})
	}
}
func TestORLikeSubjectReuseTableScanEmptyBeforeDecision(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := orLikeScanArgument(orLikeOperatorExpression(t, proc))
	batches := make([]*batch.Batch, 0, 2)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		for _, b := range batches {
			b.Clean(proc.Mp())
		}
		require.Zero(t, proc.Mp().CurrNB())
	})
	require.NoError(t, arg.Prepare(proc))
	empty, wantEmpty := orLikeScanBatch(t, proc, 0, "pattern", true)
	batches = append(batches, empty)
	orLikeScanEval(t, proc, arg, empty, true, wantEmpty)
	require.False(t, arg.ctr.subjectReuseAdmissionDone)
	large, want := orLikeScanBatch(t, proc, 1024, "pattern", true)
	batches = append(batches, large)
	orLikeScanEval(t, proc, arg, large, true, want)
	require.True(t, orLikeOperatorShared(arg.ctr.filterExecutors[0]))
}
func TestORLikeSubjectReuseTableScanPrecedingShrinkAndExclusions(t *testing.T) {
	for _, kind := range []string{"runtime_shrink", "static_shrink", "earlier_admitted", "runtime_excluded", "unreached_empty", "unreached_error"} {
		t.Run(kind, func(t *testing.T) {
			proc := testutil.NewProc(t)
			expr := orLikeOperatorExpression(t, proc)
			arg := orLikeScanArgument(expr)
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
			case "unreached_empty":
				arg.FilterExprs = []*plan.Expr{{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}, expr}
				target = 1
			case "unreached_error":
				arg.FilterExprs = []*plan.Expr{orLikeOperatorLiteral("bad"), expr}
				target = 1
			}
			input, want := orLikeScanBatch(t, proc, 8192, "match", true)
			t.Cleanup(func() { arg.Free(proc, false, nil); input.Clean(proc.Mp()); require.Zero(t, proc.Mp().CurrNB()) })
			require.NoError(t, arg.Prepare(proc))
			if kind == "unreached_error" {
				_, err := arg.ctr.readerFilter(input, []int{0, 1})
				require.ErrorContains(t, err, "filter condition is not boolean")
			} else {
				if kind == "runtime_shrink" || kind == "static_shrink" || kind == "earlier_admitted" {
					want = []int64{0}
				}
				if kind == "unreached_empty" {
					want = nil
				}
				orLikeScanEval(t, proc, arg, input, true, want)
			}
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			shared := kind == "runtime_excluded"
			require.Equal(t, shared, orLikeOperatorShared(arg.ctr.filterExecutors[target]))
			require.Same(t, arg.ctr.filterExecutors[target], arg.ctr.allFilterExecutors[len(arg.ctr.runtimeFilterExecutors)+target])
			if kind == "earlier_admitted" {
				require.True(t, orLikeOperatorShared(arg.ctr.filterExecutors[0]))
			}
			for _, executor := range arg.ctr.runtimeFilterExecutors {
				require.False(t, orLikeOperatorShared(executor))
			}
			arg.Reset(proc, false, nil)
			require.True(t, arg.ctr.subjectReuseAdmissionDone)
			require.Equal(t, shared, orLikeOperatorShared(arg.ctr.filterExecutors[target]))
		})
	}
}
func BenchmarkORLikeSubjectReuseTableScanFirstBatch(b *testing.B) {
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
						input, want := orLikeScanBatch(b, proc, rows, mode, true)
						arg := orLikeScanArgument(expr)
						if rows == 8192 {
							arg.RuntimeFilterExprs = []*plan.Expr{one}
							if mode == "match" {
								want = []int64{0}
							} else {
								want = nil
							}
						}
						b.StartTimer()
						err := arg.Prepare(proc)
						if err != nil {
							b.Fatal(err)
						}
						if baseline {
							arg.ctr.subjectReuseAdmissionDone = true
						}
						_, err = arg.ctr.readerFilter(input, []int{0, 1})
						if err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						orLikeOperatorIDs(b, input, want)
						require.Equal(b, !baseline && rows >= 1024 && rows != 8192, orLikeOperatorShared(arg.ctr.filterExecutors[0]))
						b.StartTimer()
						arg.Free(proc, false, nil)
						input.Clean(proc.Mp())
					}
					b.StopTimer()
					require.Zero(b, proc.Mp().CurrNB())
				})
			}
		}
	}
}
