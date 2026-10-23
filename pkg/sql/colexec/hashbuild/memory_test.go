// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
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
	"testing"
)

type observedBuildBudget struct {
	rscthrottler.RSCThrottler
	acquire func(int64) bool
	held    int64
}

func (b *observedBuildBudget) Acquire(n int64) (int64, bool) {
	if !b.acquire(n) {
		return b.held, false
	}
	b.held += n
	return b.held, true
}
func (b *observedBuildBudget) Release(n int64) int64 { b.held -= n; return b.held }
func (b *observedBuildBudget) Available() int64      { return 0 }

func TestBuildReservesBeforeCopyAndHashAllocation(t *testing.T) {
	for _, deny := range []int{1, 2, 0} {
		name := map[int]string{1: "copy denied", 2: "hash denied", 0: "map owns reservation"}[deny]
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.SetMessageBoard(message.NewMessageBoard())
			source := batch.NewOffHeapWithSize(1)
			source.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
			source.SetRowCount(2)
			defer source.Clean(proc.Mp())
			build := &HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, CanSpill: true, SpillExpectedScopes: []uint32{1}, Conditions: []*plan.Expr{newExpr(0, types.T_int32.ToType())}, JoinMapTag: 91, SpillThreshold: 1 << 30}
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
			require.NoError(t, build.Prepare(proc))
			defer build.Free(proc, false, nil)
			sourceBytes := proc.Mp().AllocatedBytes()
			calls := 0
			budget := &observedBuildBudget{}
			budget.acquire = func(n int64) bool {
				calls++
				require.Positive(t, n)
				require.Nil(t, build.ctr.hashmapBuilder.IntHashMap, "no hash table can exist before its reservation")
				require.Nil(t, build.ctr.hashmapBuilder.StrHashMap)
				if calls == 1 {
					require.Zero(t, build.ctr.hashmapBuilder.Batches.RowCount(), "copy cannot precede Acquire")
					require.Equal(t, sourceBytes, proc.Mp().AllocatedBytes())
				} else {
					require.Equal(t, 2, build.ctr.hashmapBuilder.Batches.RowCount())
					require.Equal(t, build.ctr.hashmapBuilder.Batches.Allocated(), build.ctr.hashmapBuilder.Batches.RetainedBytes())
				}
				return calls != deny
			}
			build.ctr.memoryThrottler = budget
			_, err := build.Call(proc)
			require.NoError(t, err)
			require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](source.Vecs[0]))
			require.Zero(t, build.ctr.memoryReserved)
			build.ctr.hashmapBuilder.Batches.TrackAllocation()
			require.Zero(t, build.ctr.hashmapBuilder.Batches.RetainedBytes(), "spill retirement or JoinMap transfer detaches retained storage")
			if deny != 0 {
				require.True(t, build.SpillGroup.IsSpilled())
				require.Zero(t, budget.held, "re-spill must retire old copied storage and its D reservation")
			} else {
				require.Positive(t, budget.held, "published map owns its transferred D reservation")
				build.Free(proc, false, nil)
				require.Positive(t, budget.held, "producer Free cannot release the consumer's map reservation")
			}
			proc.GetMessageBoard().Reset()
			require.Zero(t, budget.held, "terminal group and queued message must release exactly once")
		})
	}
}

func TestControlledBuildResetRetiresExpressionCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.Base.ImportMemory = &process.ImportMemory{}
	proc.SetMessageBoard(message.NewMessageBoard())
	source := batch.NewOffHeapWithSize(1)
	source.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	source.SetRowCount(2)
	defer source.Clean(proc.Mp())
	fn, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
	require.NoError(t, err)
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{newExpr(0, types.T_int32.ToType()), newExpr(0, types.T_int32.ToType())}}}}
	build := &HashBuild{NeedHashMap: true, Conditions: []*plan.Expr{expr}, JoinMapTag: 91}
	defer build.Free(proc, false, nil)
	baseline := proc.Mp().AllocatedBytes()
	for range 2 {
		require.NoError(t, build.Prepare(proc))
		_, err := build.ctr.hashmapBuilder.executors[0].Eval(proc, []*batch.Batch{source}, nil)
		require.NoError(t, err)
		require.Greater(t, proc.Mp().AllocatedBytes(), baseline)
		build.Reset(proc, false, nil)
		require.Nil(t, build.ctr.hashmapBuilder.executors)
		require.Equal(t, baseline, proc.Mp().AllocatedBytes())
	}
}

func TestLegacyExpressionBuildSpillsOnlyUnderPressure(t *testing.T) {
	for _, mode := range []string{"memory-available", "copy-budget-denied", "threshold-reached"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.SetMessageBoard(message.NewMessageBoard())
			defer proc.GetMessageBoard().Reset()
			require.Nil(t, proc.Base.ImportMemory)
			input := batch.NewOffHeapWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector([]int64{-7}, nil, proc.Mp())
			input.SetRowCount(1)
			defer input.Clean(proc.Mp())
			fn, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "abs"},
				Args: []*plan.Expr{newExpr(0, types.T_int64.ToType())},
			}}}
			require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{key}), "ordinary supported functions need not have an import capacity contract")
			build := &HashBuild{NeedHashMap: true, NeedBatches: true, IsShuffle: true, CanSpill: true,
				Conditions: []*plan.Expr{key}, JoinMapTag: 702, JoinMapRefCnt: 1,
				SpillThreshold: 1 << 30, RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: 701}}
			if mode == "threshold-reached" {
				build.SpillThreshold = 1
			}
			build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, build.Prepare(proc))
			defer build.Free(proc, false, nil)
			budget := &observedBuildBudget{acquire: func(int64) bool { return mode != "copy-budget-denied" }}
			build.ctr.memoryThrottler = budget
			_, err = build.Call(proc)
			require.NoError(t, err)
			jm, err := message.ReceiveJoinMap(702, true, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			require.NotNil(t, jm)
			require.Equal(t, mode != "memory-available", jm.IsSpilled())
			if !jm.IsSpilled() {
				require.Equal(t, int64(1), jm.GetRowCount())
				require.Equal(t, int64(-7), vector.GetFixedAtNoTypeCheck[int64](jm.GetBatches()[0].Vecs[0], 0))
			}
			jm.Free()
			require.Zero(t, budget.held, "consumer cleanup retires the transferred copy reservation once")
		})
	}
}
