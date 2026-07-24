// Copyright 2021-2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestDupOperator(t *testing.T) {
	dupOperator(
		insert.NewPartitionInsert(
			&insert.Insert{},
			1,
		),
		0,
		0,
	)

	dupOperator(
		deletion.NewPartitionDelete(
			&deletion.Deletion{},
			1,
		),
		0,
		0,
	)
}

func TestDupHashBuildPreservesNullTracking(t *testing.T) {
	source := hashbuild.NewArgument()
	defer source.Release()
	source.TrackNullKeys = true

	duplicated := dupOperator(source, 0, 1).(*hashbuild.HashBuild)
	defer duplicated.Release()
	require.True(t, duplicated.TrackNullKeys)
}

func TestDupOperatorMergeTop(t *testing.T) {
	op := mergetop.NewArgument()
	op.Limit = plan2.MakePlan2Int64ConstExprWithType(10)
	op.Fs = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MergeTop")
	}
	dupOp := result.(*mergetop.MergeTop)
	if dupOp.Limit != op.Limit {
		t.Errorf("Limit mismatch")
	}
}

func TestDupOperatorMergeOrder(t *testing.T) {
	op := mergeorder.NewArgument()
	op.OrderBySpecs = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC}}
	op.SpillThreshold = 1234
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MergeOrder")
	}
	dupOp := result.(*mergeorder.MergeOrder)
	if len(dupOp.OrderBySpecs) != len(op.OrderBySpecs) {
		t.Errorf("OrderBySpecs length mismatch: got %d, want %d", len(dupOp.OrderBySpecs), len(op.OrderBySpecs))
	}
	if dupOp.SpillThreshold != op.SpillThreshold {
		t.Errorf("SpillThreshold mismatch: got %d, want %d", dupOp.SpillThreshold, op.SpillThreshold)
	}
}

func TestDupOperatorPartitionMultiUpdate(t *testing.T) {
	innerOp := multi_update.NewArgument()
	op := multi_update.NewPartitionMultiUpdate(innerOp, 1)
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for PartitionMultiUpdate")
	}
}

func TestDupOperatorMultiUpdateCountDeleteAffectRows(t *testing.T) {
	op := multi_update.NewArgument()
	op.Action = multi_update.UpdateWriteTable
	op.IsOnduplicateKeyUpdate = true
	op.CountDeleteAffectRows = true
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MultiUpdate")
	}
	dupOp := result.(*multi_update.MultiUpdate)
	if !dupOp.CountDeleteAffectRows {
		t.Error("CountDeleteAffectRows not preserved by dupOperator")
	}
	if dupOp.Action != op.Action {
		t.Errorf("Action mismatch: got %v, want %v", dupOp.Action, op.Action)
	}
	if dupOp.IsOnduplicateKeyUpdate != op.IsOnduplicateKeyUpdate {
		t.Errorf("IsOnduplicateKeyUpdate mismatch: got %v, want %v",
			dupOp.IsOnduplicateKeyUpdate, op.IsOnduplicateKeyUpdate)
	}
}

func TestDupOperatorDispatchRecCTE(t *testing.T) {
	op := dispatch.NewArgument()
	op.RecCTE = true
	op.RecSink = true
	op.IsSink = true
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for Dispatch")
	}
	dupOp := result.(*dispatch.Dispatch)
	if dupOp.RecCTE != op.RecCTE {
		t.Errorf("RecCTE mismatch: got %v, want %v", dupOp.RecCTE, op.RecCTE)
	}
}

func TestConstructTimeWindowUsesRegularSumForCountCache(t *testing.T) {
	arg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 1},
		},
	}
	ts := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{arg},
					},
				},
			},
		},
		GroupBy:   []*plan.Expr{ts},
		Timestamp: ts,
		Interval:  makeTimeWindowIntervalExpr(1, "second"),
	}

	timeWin := constructTimeWindow(context.Background(), node, nil)
	require.Len(t, timeWin.Aggs, 1)
	require.Equal(t, int64(function.AggSumOverloadID), timeWin.Aggs[0].GetAggID())
	require.Equal(t, types.T_int64, timeWin.Types[0].Oid)
}

func TestConstructTimeWindowUsesRegularSumForPartialSum(t *testing.T) {
	arg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 1},
		},
	}
	ts := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0},
		},
	}
	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_uint64)},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{arg},
					},
				},
			},
		},
		GroupBy:   []*plan.Expr{ts},
		Timestamp: ts,
		Interval:  makeTimeWindowIntervalExpr(1, "second"),
	}

	timeWin := constructTimeWindow(context.Background(), node, nil)
	require.Len(t, timeWin.Aggs, 1)
	require.Equal(t, int64(function.AggSumOverloadID), timeWin.Aggs[0].GetAggID())
	require.Equal(t, types.T_decimal128, timeWin.Types[0].Oid)
}

func TestDupOperatorLoopJoinMarkPos(t *testing.T) {
	op := loopjoin.NewArgument()
	op.MarkPos = 3
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for LoopJoin")
	}
	dupOp := result.(*loopjoin.LoopJoin)
	if dupOp.MarkPos != op.MarkPos {
		t.Errorf("MarkPos mismatch: got %d, want %d", dupOp.MarkPos, op.MarkPos)
	}
}

func TestDupOperatorShuffleSharesPoolAcrossWorkers(t *testing.T) {
	op := shuffle.NewArgument()
	op.BucketNum = 4
	op.DrainAllBuckets = true

	dupCtx := newOperatorDupContext()
	dup1 := dupOperatorWithContext(op, 0, 2, dupCtx).(*shuffle.Shuffle)
	dup2 := dupOperatorWithContext(op, 1, 2, dupCtx).(*shuffle.Shuffle)

	require.Nil(t, op.GetShufflePool(), "duplicating must not mutate the reusable template")
	require.Same(t, dup1.GetShufflePool(), dup2.GetShufflePool())
	nextGeneration := dupOperatorWithContext(op, 0, 2, newOperatorDupContext()).(*shuffle.Shuffle)
	require.NotSame(t, dup1.GetShufflePool(), nextGeneration.GetShufflePool())
	require.Equal(t, int32(0), dup1.CurrentShuffleIdx)
	require.Equal(t, int32(1), dup2.CurrentShuffleIdx)
	require.True(t, dup1.DrainAllBuckets)
	require.True(t, dup2.DrainAllBuckets)
}

func TestDupOperatorAssignsSharedShuffleConsumerIndex(t *testing.T) {
	hashBuild := hashbuild.NewArgument()
	hashBuild.IsShuffle = true
	hashBuild.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(hashBuild, 2, 4).(*hashbuild.HashBuild).ShuffleIdx)

	hashJoin := hashjoin.NewArgument()
	hashJoin.IsShuffle = true
	hashJoin.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(hashJoin, 2, 4).(*hashjoin.HashJoin).ShuffleIdx)

	dedupJoin := dedupjoin.NewArgument()
	dedupJoin.IsShuffle = true
	dedupJoin.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(dedupJoin, 2, 4).(*dedupjoin.DedupJoin).ShuffleIdx)

	rightDedupJoin := rightdedupjoin.NewArgument()
	rightDedupJoin.IsShuffle = true
	rightDedupJoin.ShuffleIdx = -1
	require.Equal(t, int32(2), dupOperator(rightDedupJoin, 2, 4).(*rightdedupjoin.RightDedupJoin).ShuffleIdx)
}

func TestConstructShuffleOperatorForJoinSupportsColumnsAndExpressions(t *testing.T) {
	left := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 3}},
	}
	right := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "serial_full"},
		}},
	}
	node := &plan.Node{
		OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{left, right}}}}},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			ShuffleColIdx: 0,
			ShuffleType:   plan.ShuffleType_Range,
		}},
		RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: 42}},
	}

	leftShuffle := constructShuffleOperatorForJoin(4, node, true)
	require.Equal(t, int32(3), leftShuffle.ShuffleColIdx)
	require.Nil(t, leftShuffle.ShuffleExpr)
	require.Equal(t, int32(42), leftShuffle.RuntimeFilterSpec.Tag)

	rightShuffle := constructShuffleOperatorForJoin(4, node, false)
	require.Equal(t, right.Typ.Id, rightShuffle.ShuffleExpr.Typ.Id)
	require.Equal(t, "serial_full", rightShuffle.ShuffleExpr.GetF().Func.ObjName)
	require.Nil(t, rightShuffle.RuntimeFilterSpec)
}

func makeTimeWindowIntervalExpr(value int64, unit string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_I64Val{I64Val: value},
							},
						},
					},
					{
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_Sval{Sval: unit},
							},
						},
					},
				},
			},
		},
	}
}
