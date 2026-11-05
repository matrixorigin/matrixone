// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func membershipCondition(t testing.TB, typ types.Type) *plan.Expr {
	t.Helper()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
	require.NoError(t, err)
	args := make([]*plan.Expr, 2)
	for i := range args {
		args[i] = &plan.Expr{
			Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: int32(i), ColPos: 0}},
		}
	}
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: "="}, Args: args,
	}}}
}

func newMembershipOperators(t testing.TB, mp *mpool.MPool) (*process.Process, *LoopJoin, *hashbuild.HashBuild) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	proc.SetMessageBoard(message.NewMessageBoard())
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.AllocatedBytes())
	})
	join := &LoopJoin{
		NonEqCond: membershipCondition(t, types.T_int32.ToType()),
		JoinType:  plan.Node_MARK, JoinMapTag: 1,
		LeftTypes:  []types.Type{types.T_int32.ToType()},
		RightTypes: []types.Type{types.T_int32.ToType()},
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)},
	}
	build := &hashbuild.HashBuild{NeedBatches: true, JoinMapTag: 1, JoinMapRefCnt: 1}
	join.AppendChild(colexec.NewMockOperator())
	build.AppendChild(colexec.NewMockOperator())
	t.Cleanup(func() {
		join.Free(proc, false, nil) // also test direct Free without Reset
		build.Free(proc, false, nil)
		join.GetChildren(0).Free(proc, false, nil)
		build.GetChildren(0).Free(proc, false, nil)
	})
	return proc, join, build
}

func membershipInputs(t testing.TB, proc *process.Process, join *LoopJoin, build *hashbuild.HashBuild, left, right []int32, leftNull, rightNull []uint64) {
	t.Helper()
	l := makeInt32LoopJoinBatch(proc.Mp(), left)
	join.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{l})
	l.Vecs[0].GetNulls().Add(leftNull...)
	r := makeInt32LoopJoinBatch(proc.Mp(), right)
	build.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{r})
	r.Vecs[0].GetNulls().Add(rightNull...)
	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))
	_, err := vm.Exec(build, proc)
	require.NoError(t, err)
}

func membershipResults(t testing.TB, proc *process.Process, join *LoopJoin) [][2]bool {
	t.Helper()
	var output [][2]bool
	for {
		res, err := vm.Exec(join, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		for i := range res.Batch.RowCount() {
			v := res.Batch.Vecs[1]
			output = append(output, [2]bool{vector.GetFixedAtNoTypeCheck[bool](v, i), v.GetNulls().Contains(uint64(i)) || v.IsConstNull()})
		}
	}
	return output
}

func TestMarkMembershipEvaluatorEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		right []int32
		nulls []uint64
		want  [][2]bool
	}{
		{"empty", nil, nil, [][2]bool{{false, false}, {false, false}, {false, false}}},
		{"duplicates", []int32{1, 1}, nil, [][2]bool{{true, false}, {false, false}, {false, true}}},
		{"match-and-null", []int32{1, 0}, []uint64{1}, [][2]bool{{true, false}, {false, true}, {false, true}}},
		{"all-null", []int32{0}, []uint64{0}, [][2]bool{{false, true}, {false, true}, {false, true}}},
	} {
		for _, indexed := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/evaluator", true: "/index"}[indexed], func(t *testing.T) {
				proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
				membershipInputs(t, proc, join, build, []int32{1, 2, 0}, tc.right, []uint64{2}, tc.nulls)
				if !indexed {
					join.ctr.markProbeCol = -1
				}
				require.Equal(t, tc.want, membershipResults(t, proc, join))
				if len(tc.right) > 0 && indexed {
					require.Positive(t, join.ctr.mp.Size())
					require.Nil(t, join.ctr.joinBat, "index path must not materialize pairwise evaluation batches")
				}
			})
		}
	}
}

func TestMarkMembershipResetAndBudgetFallback(t *testing.T) {
	mp, err := mpool.NewMPool("mark-fallback", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc, join, build := newMembershipOperators(t, mp)
	membershipInputs(t, proc, join, build, []int32{1, 2}, []int32{1, 0}, nil, []uint64{1})
	blocker, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	jm, err := message.ReceiveJoinMap(1, false, 0, proc.GetMessageBoard(), proc.Ctx)
	require.NoError(t, err)
	index, err := jm.GetMembershipIndex(proc.Ctx, 0, types.T_int32.ToType())
	require.NoError(t, err)
	require.Nil(t, index)
	// Release the competing allocation before the evaluator allocates its
	// mandatory result storage. The disabled index and RHS remain published.
	mp.Free(blocker)
	blocker = nil
	got := membershipResults(t, proc, join)
	require.Equal(t, [][2]bool{{true, false}, {false, true}}, got)
	require.Zero(t, join.ctr.mp.Size())
	require.NotNil(t, join.ctr.joinBat)
	join.Reset(proc, false, nil)
	build.Reset(proc, false, nil)
	join.GetChildren(0).Free(proc, false, nil)
	build.GetChildren(0).Free(proc, false, nil)
	proc.SetMessageBoard(proc.GetMessageBoard().Reset())
	membershipInputs(t, proc, join, build, []int32{1, 2}, []int32{2}, nil, nil)
	require.Equal(t, [][2]bool{{false, false}, {true, false}}, membershipResults(t, proc, join))
	require.Positive(t, join.ctr.mp.Size(), "new generation must retry indexing and clear old NULL facts")
}

func TestMarkMembershipOutputAllocationErrorIsNotFallback(t *testing.T) {
	mp, err := mpool.NewMPool("mark-output", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc, join, build := newMembershipOperators(t, mp)
	membershipInputs(t, proc, join, build, []int32{1}, []int32{1}, nil, nil)
	jm, err := message.ReceiveJoinMap(1, false, 0, proc.GetMessageBoard(), proc.Ctx)
	require.NoError(t, err)
	index, err := jm.GetMembershipIndex(proc.Ctx, 0, types.T_int32.ToType())
	require.NoError(t, err)
	require.NotNil(t, index)
	join.ResultCols = []colexec.ResultPos{colexec.NewResultPos(-1, 0)}
	join.ctr.resBat = batch.NewWithSize(1)
	join.ctr.resBat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
	blocker, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	res, err := vm.Exec(join, proc)
	require.ErrorContains(t, err, "mpool out of space")
	require.Nil(t, res.Batch)
	require.Nil(t, join.ctr.joinBat, "mandatory output allocation failure must not invoke the evaluator")
	require.Positive(t, jm.Size(), "published index remains owned by the JoinMap")
}

func TestMarkMembershipEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*LoopJoin)
		want   bool
	}{
		{"columns", func(*LoopJoin) {}, true},
		{"commuted", func(j *LoopJoin) { f := j.NonEqCond.GetF(); f.Args[0], f.Args[1] = f.Args[1], f.Args[0] }, true},
		{"not-mark", func(j *LoopJoin) { j.JoinType = plan.Node_ANTI }, false},
		{"nil", func(j *LoopJoin) { j.NonEqCond = nil }, false},
		{"same-side", func(j *LoopJoin) { j.NonEqCond.GetF().Args[1].GetCol().RelPos = 0 }, false},
		{"type-mismatch", func(j *LoopJoin) { j.NonEqCond.GetF().Args[1].Typ.Id = int32(types.T_int64) }, false},
		{"constant", func(j *LoopJoin) { j.NonEqCond.GetF().Args[0].Expr = &plan.Expr_Lit{Lit: &plan.Literal{}} }, false},
		{"function-even-if-not-null", func(j *LoopJoin) {
			j.NonEqCond.GetF().Args[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, Expr: &plan.Expr_F{F: &plan.Function{}}}
		}, false},
		{"residual", func(j *LoopJoin) { j.NonEqCond.GetF().Func.Obj = int64(function.AND) << 32 }, false},
		{"float", func(j *LoopJoin) { j.NonEqCond = membershipCondition(t, types.T_float64.ToType()) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			join := &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: membershipCondition(t, types.T_int32.ToType())}
			tc.change(join)
			join.prepareMembership()
			require.Equal(t, tc.want, join.ctr.markProbeCol >= 0)
		})
	}
}

type membershipEvalSpy struct {
	colexec.ExpressionExecutor
	calls     int
	err       error
	errAtCall int
	cancel    context.CancelFunc
}

func (spy *membershipEvalSpy) Eval(proc *process.Process, batches []*batch.Batch, selection []bool) (*vector.Vector, error) {
	spy.calls++
	if spy.cancel != nil {
		spy.cancel()
	}
	if spy.err != nil && (spy.errAtCall == 0 || spy.calls >= spy.errAtCall) {
		return nil, spy.err
	}
	return spy.ExpressionExecutor.Eval(proc, batches, selection)
}

func TestMarkMembershipPreservesEvaluatorEarlyMatch(t *testing.T) {
	proc, join, _ := newMembershipOperators(t, mpool.MustNewZero())
	key := join.NonEqCond.GetF().Args[1]
	coalesce, err := function.GetFunctionByName(proc.Ctx, "coalesce", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_int32, coalesce.GetReturnType().Oid)
	join.NonEqCond.GetF().Args[1] = &plan.Expr{Typ: key.Typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: coalesce.GetEncodedOverloadID(), ObjName: "coalesce"}, Args: []*plan.Expr{key, key},
	}}}
	join.NonEqCond.GetF().Args[1].Typ.NotNullable = true
	join.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{makeInt32LoopJoinBatch(proc.Mp(), []int32{1})})
	require.NoError(t, join.Prepare(proc))
	require.Equal(t, -1, join.ctr.markProbeCol)
	// Preserve two RHS batches to expose the early-match boundary directly.
	unpublished := make([]*batch.Batch, 0, 2)
	t.Cleanup(func() {
		for _, bat := range unpublished {
			bat.Clean(proc.Mp())
		}
	})
	unpublished = append(unpublished, makeInt32LoopJoinBatch(proc.Mp(), []int32{1}))
	unpublished = append(unpublished, makeInt32LoopJoinBatch(proc.Mp(), []int32{2}))
	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, unpublished, proc.Mp())
	jm.IncRef(1)
	message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, Tag: 1}, proc.GetMessageBoard())
	unpublished = nil // message/consumer lifecycle owns both batches now
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr, err: errors.New("later batch must not be evaluated"), errAtCall: 2}
	join.ctr.expr = spy
	require.Equal(t, [][2]bool{{true, false}}, membershipResults(t, proc, join))
	require.Equal(t, 1, spy.calls)
	require.Zero(t, jm.Size())
}

func TestMarkMembershipUnrelatedErrorAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated-error", true: "cancel-during-evaluator"}[cancel], func(t *testing.T) {
			proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
			membershipInputs(t, proc, join, build, []int32{2, 3}, []int32{1}, nil, nil)
			join.ctr.markProbeCol = -1
			spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
			join.ctr.expr = spy
			want := errors.New("injected evaluator failure")
			if cancel {
				var stop context.CancelFunc
				proc.Ctx, stop = context.WithCancel(proc.Ctx)
				defer stop()
				spy.cancel = stop
				want = context.Canceled
			} else {
				spy.err = want
			}
			_, err := vm.Exec(join, proc)
			require.ErrorIs(t, err, want)
			require.Equal(t, 1, spy.calls)
		})
	}
}

func TestMarkMembershipAvoidsEvaluatorAndEmptyProbeBuild(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "indexed", true: "empty-probe"}[empty], func(t *testing.T) {
			proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
			left := []int32{1, 2}
			if empty {
				left = nil
			}
			membershipInputs(t, proc, join, build, left, []int32{1}, nil, nil)
			spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr, err: errors.New("must not evaluate")}
			join.ctr.expr = spy
			result := membershipResults(t, proc, join)
			require.Len(t, result, len(left))
			require.Zero(t, spy.calls)
			if empty {
				require.Zero(t, join.ctr.mp.Size())
			}
		})
	}
}
