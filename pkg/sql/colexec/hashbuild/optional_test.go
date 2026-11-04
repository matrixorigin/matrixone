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

package hashbuild

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestOptionalIndexBudgetFallback(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		cap                                 int64
		hash, optional, indexed, expression bool
	}{
		{"batch only", 2 << 20, false, false, false, false},
		{"optional constrained", 2 << 20, true, true, false, false},
		{"required constrained", 2 << 20, true, false, false, false},
		{"optional admitted", 32 << 20, true, true, true, false},
		{"unknown expression bound", 32 << 20, true, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := newExpr(0, types.T_int32.ToType())
			if tc.expression {
				fn, err := function.GetFunctionByName(context.Background(), "abs", []types.Type{types.T_int32.ToType()})
				require.NoError(t, err)
				key = &plan.Expr{Typ: key.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "abs"}, Args: []*plan.Expr{key}}}}
			}
			mp, err := mpool.NewMPool(tc.name, tc.cap, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			proc.SetMessageBoard(message.NewMessageBoard())
			sourceMP := mpool.MustNewZero()
			bat := batch.NewWithSize(1)
			values := make([]int32, 100000)
			for i := range values {
				values[i] = int32(i)
			}
			bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, sourceMP)
			bat.SetRowCount(len(values))
			b := &HashBuild{NeedHashMap: tc.hash, NeedBatches: true, OptionalIndex: tc.optional, Conditions: []*plan.Expr{key}, JoinMapTag: 173, JoinMapRefCnt: 1}
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
			b.AppendChild(child)
			t.Cleanup(func() {
				b.Free(proc, false, nil)
				proc.GetMessageBoard().Reset()
				bat.Clean(sourceMP)
				proc.Free()
				require.Zero(t, mp.CurrNB())
				require.Zero(t, sourceMP.CurrNB())
			})
			require.NoError(t, b.Prepare(proc))
			_, err = b.Call(proc)
			if tc.hash && !tc.optional {
				require.Error(t, err)
				require.NotEqual(t, SendSucceed, b.ctr.state)
				return
			}
			require.NoError(t, err)
			t.Logf("retained bytes=%d cap=%d indexed=%v", mp.CurrNB(), tc.cap, tc.indexed)
			jm, err := message.ReceiveJoinMap(173, false, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			require.NotNil(t, jm)
			require.Equal(t, tc.indexed, jm.NewIterator() != nil)
			require.Equal(t, int64(len(values)), jm.GetRowCount())
			rows := 0
			for _, b := range jm.GetBatches() {
				for _, v := range vector.MustFixedColNoTypeCheck[int32](b.Vecs[0]) {
					require.Equal(t, int32(rows), v)
					rows++
				}
			}
			require.Equal(t, len(values), rows, "fallback must retain every build row")
			jm.Free()
			b.Reset(proc, false, nil)
			require.False(t, b.ctr.hashmapBuilder.hasNullKey)
			if tc.optional && !tc.expression {
				proc.GetMessageBoard().Reset()
				small := batch.NewWithSize(1)
				small.Vecs[0] = testutil.MakeInt32Vector([]int32{9}, nil, sourceMP)
				small.SetRowCount(1)
				t.Cleanup(func() { small.Clean(sourceMP) })
				b.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{small}), 0)
				require.NoError(t, b.Prepare(proc))
				_, err = b.Call(proc)
				require.NoError(t, err)
				next, err := message.ReceiveJoinMap(173, false, 0, proc.GetMessageBoard(), proc.Ctx)
				require.NoError(t, err)
				require.NotNil(t, next)
				require.Equal(t, int64(1), next.GetRowCount())
				require.NotNil(t, next.NewIterator(), "fallback must not disable indexing in the next generation")
				next.Free()
			}
		})
	}
}

type reviewEvalHook struct {
	colexec.ExpressionExecutor
	hook func() error
}

func (e reviewEvalHook) Eval(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	if err := e.hook(); err != nil {
		return nil, err
	}
	return e.ExpressionExecutor.Eval(p, b, s)
}

func TestOptionalIndexPartialFailure(t *testing.T) {
	for _, mode := range []string{"oom", "pool limit", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			mp, err := mpool.NewMPool("optional failure", 32<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			proc.SetMessageBoard(message.NewMessageBoard())
			ctx, cancel := context.WithCancel(proc.Ctx)
			proc.Ctx = ctx
			defer cancel()
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
			bat.SetRowCount(2)
			b := &HashBuild{NeedHashMap: true, NeedBatches: true, OptionalIndex: true, Conditions: []*plan.Expr{newExpr(0, types.T_int32.ToType())}, JoinMapTag: 175, JoinMapRefCnt: 1}
			b.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))
			t.Cleanup(func() {
				b.Free(proc, true, nil)
				bat.Clean(proc.Mp())
				proc.GetMessageBoard().Reset()
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, b.Prepare(proc))
			sentinel := moerr.NewInvalidInputNoCtx("injected expression failure")
			b.ctr.hashmapBuilder.executors[0] = reviewEvalHook{b.ctr.hashmapBuilder.executors[0], func() error {
				require.NotNil(t, b.ctr.hashmapBuilder.IntHashMap, "failure after index allocation")
				if mode == "cancel" {
					cancel()
					return nil
				}
				if mode == "oom" {
					return moerr.NewOOMNoCtx()
				}
				if mode == "pool limit" {
					_, err := mp.Alloc(int(mp.Cap()), true)
					require.Error(t, err)
					return err
				}
				return sentinel
			}}
			_, err = b.Call(proc)
			if mode == "oom" || mode == "pool limit" {
				require.NoError(t, err)
				jm, e := message.ReceiveJoinMap(175, false, 0, proc.GetMessageBoard(), proc.Ctx)
				require.NoError(t, e)
				require.Nil(t, jm.NewIterator())
				require.Equal(t, int64(2), jm.GetRowCount())
				jm.Free()
			} else {
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, sentinel)
				}
				require.NotEqual(t, SendSucceed, b.ctr.state)
			}
		})
	}
}

func TestHashmapCancellationBetweenBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	defer cancel()
	var hb HashmapBuilder
	t.Cleanup(func() { hb.Free(proc); proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	for j := 0; j < 2; j++ {
		bat := batch.NewWithSize(1)
		values := make([]int32, colexec.DefaultBatchSize)
		for i := range values {
			values[i] = int32(j*len(values) + i)
		}
		bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		bat.SetRowCount(len(values))
		require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
		hb.InputBatchRowCount += len(values)
		bat.Clean(proc.Mp())
	}
	calls := 0
	hb.executors[0] = reviewEvalHook{hb.executors[0], func() error {
		calls++
		if calls == 2 {
			require.GreaterOrEqual(t, hb.GetGroupCount(), uint64(hashmap.UnitLimit))
			cancel()
		}
		return nil
	}}
	require.ErrorIs(t, hb.BuildHashmap(false, false, false, proc), context.Canceled)
	require.Equal(t, 2, calls)
	require.Less(t, hb.GetGroupCount(), uint64(hb.InputBatchRowCount), "no insertion after cancellation")
}
