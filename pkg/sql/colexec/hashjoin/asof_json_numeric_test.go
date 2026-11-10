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

package hashjoin

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func jsonNumericAsofFn(t testing.TB, name string, args ...*plan.Expr) *plan.Expr {
	ts := make([]types.Type, len(args))
	for i, e := range args {
		ts[i] = types.New(types.T(e.Typ.Id), e.Typ.Width, e.Typ.Scale)
	}
	fn, err := function.GetFunctionByName(context.Background(), name, ts)
	require.NoError(t, err)
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name}, Args: args}}}
}
func TestAsofJSONNumericResidualMatches(t *testing.T) {
	for _, constant := range []bool{false, true} {
		for _, c := range []struct {
			name   string
			values []string
			times  []types.Timestamp
			want   types.Timestamp
		}{
			{"nearest_false", []string{"1", "2"}, []types.Timestamp{8, 9}, 8},
			{"nearest_null", []string{"1", "null"}, []types.Timestamp{8, 9}, 8},
			{"nearest_string", []string{"1", `"1"`}, []types.Timestamp{8, 9}, 8},
			{"nearest_fraction", []string{"1", "1.5"}, []types.Timestamp{8, 9}, 8},
			{"tie_false_first", []string{"2", "1"}, []types.Timestamp{9, 9}, 9},
			{"tie_true_first", []string{"1", "2"}, []types.Timestamp{9, 9}, 9},
			{"tie_both_true", []string{"1", "1"}, []types.Timestamp{9, 9}, 9},
		} {
			t.Run(fmt.Sprintf("constant=%v/%s", constant, c.name), func(t *testing.T) {
				key, tm, num, js := types.T_int32.ToType(), types.T_timestamp.ToType(), types.T_int64.ToType(), types.T_json.ToType()
				eq := [][]*plan.Expr{{newExpr(0, key)}, {newExpr(0, key)}}
				if constant {
					for side := range eq {
						eq[side][0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
					}
				}
				tc := newTestCase(t, []types.Type{key, key}, []colexec.ResultPos{colexec.NewResultPos(1, 1)}, eq)
				defer func() {
					tc.arg.Reset(tc.proc, false, nil)
					tc.barg.Reset(tc.proc, false, nil)
					tc.arg.Free(tc.proc, false, nil)
					tc.barg.Free(tc.proc, false, nil)
					tc.proc.Free()
					tc.cancel()
					require.Zero(t, tc.proc.Mp().CurrNB())
					require.Zero(t, tc.arg.allocationAccount.Snapshot().Used)
				}()
				tc.arg.JoinType = plan.Node_ASOF_LEFT
				tc.arg.AsofBuildLeft = false
				tc.arg.AsofRightCol = 1
				tc.arg.LeftTypes = []types.Type{key, tm, num}
				tc.arg.RightTypes = []types.Type{key, tm, js}
				l, r := newExpr(2, num), newExpr(2, js)
				l.GetCol().RelPos = 0
				r.GetCol().RelPos = 1
				tc.arg.NonEqCond = jsonNumericAsofFn(t, "and", makeAsofCondition(t, tm, ">="), jsonNumericAsofFn(t, "=", l, r))
				left := batch.NewWithSize(3)
				left.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, tc.proc.Mp())
				left.Vecs[1] = vector.NewVec(tm)
				require.NoError(t, vector.AppendFixedList(left.Vecs[1], []types.Timestamp{10}, nil, tc.proc.Mp()))
				left.Vecs[2] = vector.NewVec(num)
				require.NoError(t, vector.AppendFixedList(left.Vecs[2], []int64{1}, nil, tc.proc.Mp()))
				left.SetRowCount(1)
				right := batch.NewWithSize(3)
				right.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, tc.proc.Mp())
				right.Vecs[1] = vector.NewVec(tm)
				require.NoError(t, vector.AppendFixedList(right.Vecs[1], c.times, nil, tc.proc.Mp()))
				right.Vecs[2] = vector.NewVec(js)
				for _, s := range c.values {
					j, err := types.ParseStringToByteJson(s)
					require.NoError(t, err)
					b, err := types.EncodeJson(j)
					require.NoError(t, err)
					require.NoError(t, vector.AppendBytes(right.Vecs[2], b, false, tc.proc.Mp()))
				}
				right.SetRowCount(2)
				resetHashBuildChildrenWithBatch(tc.barg, right)
				resetChildrenWithBatch(tc.arg, left)
				require.NoError(t, tc.arg.Prepare(tc.proc))
				require.NoError(t, tc.barg.Prepare(tc.proc))
				_, err := vm.Exec(tc.barg, tc.proc)
				require.NoError(t, err)
				out, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				require.NotNil(t, out.Batch)
				require.Equal(t, 1, out.Batch.RowCount())
				require.Empty(t, tc.arg.ctr.asofIndexes)
				require.False(t, out.Batch.Vecs[0].GetNulls().Contains(0), "earlier qualifying candidate must survive")
				require.Equal(t, c.want, vector.MustFixedColWithTypeCheck[types.Timestamp](out.Batch.Vecs[0])[0])
			})
		}
	}
}

// A direct selection fixture exercises repeated probes and failure/reuse without
// a service or a second retained copy of build rows. Inputs have explicit owners.
func newJSONNumericAsofScan(t testing.TB, constant bool, values []string, times []types.Timestamp) (*HashJoin, *process.Process, []int32) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	key, tm, num, js := types.T_int32.ToType(), types.T_timestamp.ToType(), types.T_int64.ToType(), types.T_json.ToType()
	eq := [][]*plan.Expr{{newExpr(0, key)}, {newExpr(0, key)}}
	if constant {
		for side := range eq {
			eq[side][0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
		}
	}
	l, r := newExpr(2, num), newExpr(2, js)
	l.GetCol().RelPos = 0
	r.GetCol().RelPos = 1
	arg := &HashJoin{JoinType: plan.Node_ASOF_LEFT, LeftTypes: []types.Type{key, tm, num}, RightTypes: []types.Type{key, tm, js}, EqConds: eq, AsofRightCol: 1, NonEqCond: jsonNumericAsofFn(t, "and", jsonNumericAsofFn(t, ">=", func() *plan.Expr { e := newExpr(1, tm); e.GetCol().RelPos = 0; return e }(), func() *plan.Expr { e := newExpr(1, tm); e.GetCol().RelPos = 1; return e }()), jsonNumericAsofFn(t, "=", l, r))}
	account := installTestAllocation(t, arg)
	var left, right *batch.Batch
	t.Cleanup(func() {
		arg.ctr.leftBat = nil
		arg.ctr.rightBats = nil
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		if left != nil {
			left.Clean(proc.Mp())
		}
		if right != nil {
			right.Clean(proc.Mp())
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
		require.Zero(t, account.Snapshot().Used)
	})
	require.NoError(t, arg.Prepare(proc))
	left = batch.NewWithSize(3)
	left.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	left.Vecs[1] = vector.NewVec(tm)
	require.NoError(t, vector.AppendFixed(left.Vecs[1], types.Timestamp(100000), false, proc.Mp()))
	left.Vecs[2] = vector.NewVec(num)
	require.NoError(t, vector.AppendFixed(left.Vecs[2], int64(1), false, proc.Mp()))
	left.SetRowCount(1)
	right = batch.NewWithSize(3)
	right.Vecs[0] = testutil.MakeInt32Vector(make([]int32, len(values)), nil, proc.Mp())
	right.Vecs[1] = vector.NewVec(tm)
	require.NoError(t, vector.AppendFixedList(right.Vecs[1], times, nil, proc.Mp()))
	right.Vecs[2] = vector.NewVec(js)
	for _, s := range values {
		j, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(j)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(right.Vecs[2], encoded, false, proc.Mp()))
	}
	right.SetRowCount(len(values))
	arg.ctr.leftBat = left
	arg.ctr.rightBats = []*batch.Batch{right}
	arg.ctr.joinBats[0], arg.ctr.cfs1 = colexec.NewJoinBatch(left, proc.Mp())
	arg.ctr.joinBats[1], arg.ctr.cfs2 = colexec.NewJoinBatch(right, proc.Mp())
	sels := make([]int32, len(values))
	for i := range sels {
		sels[i] = int32(i)
	}
	return arg, proc, sels
}

func TestAsofJSONNumericRepeatedGroups(t *testing.T) {
	for _, order := range []string{"ascending", "descending", "unordered"} {
		t.Run(order, func(t *testing.T) {
			n := asofLinearOnlyGroupSizeLimit + 1
			values := make([]string, n)
			times := make([]types.Timestamp, n)
			for i := range values {
				values[i] = "2"
				times[i] = types.Timestamp(i + 1)
			}
			values[0] = "1"
			arg, proc, sels := newJSONNumericAsofScan(t, false, values, times)
			switch order {
			case "descending":
				slices.Reverse(sels)
			case "unordered":
				sels[0], sels[n/2] = sels[n/2], sels[0]
			}
			for range asofIndexPromotionScans(int32(n), int32(n)) + 2 {
				best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
				require.NoError(t, err)
				require.True(t, found)
				require.Zero(t, best)
			}
			require.Empty(t, arg.ctr.asofIndexes)
			// Stable build ordinal wins, even when traversal is reversed.
			arg.ctr.rightBats[0].Vecs[2].CleanOnlyData()
			for range n {
				j, err := types.ParseStringToByteJson("1")
				require.NoError(t, err)
				encoded, err := types.EncodeJson(j)
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(arg.ctr.rightBats[0].Vecs[2], encoded, false, proc.Mp()))
			}
			arg.ctr.rightBats[0].Vecs[1].CleanOnlyData()
			require.NoError(t, vector.AppendFixedList(arg.ctr.rightBats[0].Vecs[1], make([]types.Timestamp, n), nil, proc.Mp()))
			best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
			require.NoError(t, err)
			require.True(t, found)
			require.Zero(t, best)
		})
	}
}

type jsonNumericAsofObservedExecutor struct {
	colexec.ExpressionExecutor
	before func() error
}

func (e *jsonNumericAsofObservedExecutor) Eval(proc *process.Process, bats []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	if err := e.before(); err != nil {
		return nil, err
	}
	return e.ExpressionExecutor.Eval(proc, bats, selectList)
}

func TestAsofJSONNumericScanCancelAndReuse(t *testing.T) {
	const n = 257
	vals := make([]string, n)
	for i := range vals {
		vals[i] = "1"
	}
	arg, proc, sels := newJSONNumericAsofScan(t, true, vals, make([]types.Timestamp, n))
	originalCtx := proc.Ctx
	ctx, cancel := context.WithCancel(originalCtx)
	defer cancel()
	proc.Ctx = ctx
	calls := 0
	arg.ctr.nonEqCondExec = &jsonNumericAsofObservedExecutor{ExpressionExecutor: arg.ctr.nonEqCondExec, before: func() error {
		calls++
		if calls == 1 {
			cancel()
		}
		return nil
	}}
	_, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, found)
	require.Equal(t, 256, calls, "scan must observe cancellation after entering residual evaluation")
	proc.Ctx = originalCtx
	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, best)
	// Skipped candidates must also count toward the cancellation interval.
	for i := range vector.MustFixedColWithTypeCheck[types.Timestamp](arg.ctr.rightBats[0].Vecs[1]) {
		vector.MustFixedColWithTypeCheck[types.Timestamp](arg.ctr.rightBats[0].Vecs[1])[i] = 100001
	}
	ctx, cancel = context.WithCancel(originalCtx)
	cancel()
	proc.Ctx = ctx
	_, _, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.ErrorIs(t, err, context.Canceled)
	proc.Ctx = originalCtx
}

func TestAsofJSONNumericScanErrorsAndEmpty(t *testing.T) {
	arg, proc, sels := newJSONNumericAsofScan(t, true, []string{"1", "18446744073709551615"}, []types.Timestamp{1, 2})
	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.NoError(t, err)
	require.True(t, found, "a wider nonmatching JSON number must not hide the earlier match")
	require.Zero(t, best)

	// A genuine residual evaluation error must still propagate after a match.
	executor := arg.ctr.nonEqCondExec
	calls := 0
	evalErr := moerr.NewInternalErrorNoCtx("residual evaluation failed")
	arg.ctr.nonEqCondExec = &jsonNumericAsofObservedExecutor{ExpressionExecutor: executor, before: func() error {
		calls++
		if calls == 2 {
			return evalErr
		}
		return nil
	}}
	_, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.ErrorIs(t, err, evalErr)
	require.False(t, found, "evaluation errors cannot become NULL extension after an earlier match")
	require.Equal(t, 2, calls)
	arg.ctr.nonEqCondExec = executor
	best, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels[:1])
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, best)
	_, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, nil)
	require.NoError(t, err)
	require.False(t, found)
	arg.ctr.leftBat.Vecs[1].GetNulls().Add(0)
	_, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.NoError(t, err)
	require.False(t, found)
	arg.AsofBuildLeft = true
	require.ErrorContains(t, arg.Prepare(proc), "requires build-right")
	arg.AsofBuildLeft = false
	require.NoError(t, arg.Prepare(proc))
	require.True(t, arg.ctr.asofResidualEquality)
}

func TestAsofJSONNumericPartialPrepareFailure(t *testing.T) {
	source, proc, _ := newJSONNumericAsofScan(t, true, []string{"1"}, []types.Timestamp{1})
	valid := source.NonEqCond
	broken := jsonNumericAsofFn(t, "and", valid, &plan.Expr{Typ: types.T_bool.ToType().PlanType()})
	arg := &HashJoin{JoinType: plan.Node_ASOF_LEFT, LeftTypes: source.LeftTypes, RightTypes: source.RightTypes, EqConds: source.EqConds, AsofRightCol: 1, NonEqCond: broken}
	account := installTestAllocation(t, arg)
	defer func() { arg.Free(proc, false, nil); require.Zero(t, account.Snapshot().Used) }()
	require.Error(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.eqCondExecs)
	require.Nil(t, arg.ctr.nonEqCondExec)
	require.Zero(t, account.Snapshot().Used)
	arg.NonEqCond = valid
	require.NoError(t, arg.Prepare(proc))
	require.True(t, arg.ctr.asofResidualEquality)
	arg.Reset(proc, false, nil)
	require.False(t, arg.ctr.asofResidualEquality)
	require.Zero(t, account.Snapshot().Used)
	arg.NonEqCond = valid.GetF().Args[0]
	require.NoError(t, arg.Prepare(proc))
	require.False(t, arg.ctr.asofResidualEquality)
}

func BenchmarkAsofJSONNumericEligibility(b *testing.B) {
	for _, constant := range []bool{false, true} {
		for _, n := range []int{32, 1024} {
			b.Run(fmt.Sprintf("constant=%v/rows=%d", constant, n), func(b *testing.B) {
				vals := make([]string, n)
				times := make([]types.Timestamp, n)
				for i := range vals {
					vals[i] = "2"
					times[i] = types.Timestamp(i)
				}
				vals[0] = "1"
				arg, proc, sels := newJSONNumericAsofScan(b, constant, vals, times)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_, _, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(n), "candidates/op")
			})
		}
	}
}

func TestAsofConstantGroupBroadcastBudgetFailure(t *testing.T) {
	key, tm, num, js := types.T_int32.ToType(), types.T_timestamp.ToType(), types.T_int64.ToType(), types.T_json.ToType()
	constant := func() *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
	}
	tc := newTestCase(t, []types.Type{key, key}, []colexec.ResultPos{colexec.NewResultPos(1, 1)}, [][]*plan.Expr{{constant()}, {constant()}})
	tc.proc.Base.Lim.Size = 64 << 10
	budget, err := tc.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	old := tc.arg.allocationAccount
	require.NoError(t, tc.arg.ClearAllocationAccount(old))
	require.NoError(t, tc.barg.ClearAllocationAccount(old))
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<24, budget)
	require.NoError(t, err)
	require.NoError(t, tc.arg.SetAllocationAccount(account))
	require.NoError(t, tc.barg.SetAllocationAccount(account))
	tc.arg.JoinType = plan.Node_ASOF_LEFT
	tc.arg.LeftTypes = []types.Type{key, tm, num}
	tc.arg.RightTypes = []types.Type{key, tm, js}
	tc.arg.AsofRightCol = 1
	l, r := newExpr(2, num), newExpr(2, js)
	l.GetCol().RelPos = 0
	r.GetCol().RelPos = 1
	tc.arg.NonEqCond = jsonNumericAsofFn(t, "and", makeAsofCondition(t, tm, ">="), jsonNumericAsofFn(t, "=", l, r))
	// Sixteen moderate JSON strings exceed the small retained-build budget.
	// The single constant key must not be interpreted as one retained row.
	right := batch.NewWithSize(3)
	right.Vecs[0] = testutil.MakeInt32Vector(make([]int32, 16), nil, tc.proc.Mp())
	right.Vecs[1] = vector.NewVec(tm)
	require.NoError(t, vector.AppendFixedList(right.Vecs[1], make([]types.Timestamp, 16), nil, tc.proc.Mp()))
	right.Vecs[2] = vector.NewVec(js)
	j, err := types.ParseStringToByteJson(`"` + strings.Repeat("x", 8192) + `"`)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(j)
	require.NoError(t, err)
	for range 16 {
		require.NoError(t, vector.AppendBytes(right.Vecs[2], encoded, false, tc.proc.Mp()))
	}
	right.SetRowCount(16)
	resetHashBuildChildrenWithBatch(tc.barg, right)
	var buildErr error
	defer func() {
		tc.arg.Reset(tc.proc, true, buildErr)
		tc.barg.Reset(tc.proc, true, buildErr)
		tc.arg.Free(tc.proc, true, buildErr)
		tc.barg.Free(tc.proc, true, buildErr)
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, buildErr = vm.Exec(tc.barg, tc.proc)
	require.Error(t, buildErr)
	require.True(t, moerr.IsMoErrCode(buildErr, moerr.ErrOOM), buildErr)
	require.Zero(t, tc.barg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"])
	dependency, err := message.ReceiveJoinMapResult(tc.barg.JoinMapTag, false, 0, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, dependency.IsBuildError())
	require.Equal(t, buildErr.Error(), dependency.BuildError().AsMoErr().Error())
}

func TestAsofJSONNumericMultipleBatches(t *testing.T) {
	n := colexec.DefaultBatchSize + 1
	values := make([]string, n)
	for i := range values {
		values[i] = "2"
	}
	values[n-1] = "1"
	arg, proc, sels := newJSONNumericAsofScan(t, true, values, make([]types.Timestamp, n))
	whole := arg.ctr.rightBats[0]
	first, err := whole.Window(0, colexec.DefaultBatchSize)
	require.NoError(t, err)
	second, err := whole.Window(colexec.DefaultBatchSize, n)
	require.NoError(t, err)
	defer first.Clean(proc.Mp())
	defer second.Clean(proc.Mp())
	arg.ctr.rightBats = []*batch.Batch{first, second}
	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, sels)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int32(n-1), best)
	require.Empty(t, arg.ctr.asofIndexes)
}
