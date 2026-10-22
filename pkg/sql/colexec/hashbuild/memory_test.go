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
				}
				return calls != deny
			}
			build.ctr.memoryThrottler = budget
			_, err := build.Call(proc)
			require.NoError(t, err)
			require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](source.Vecs[0]))
			require.Zero(t, build.ctr.memoryReserved)
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
