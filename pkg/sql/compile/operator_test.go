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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffleV2"
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

	dup1 := dupOperator(op, 0, 2).(*shuffle.Shuffle)
	dup2 := dupOperator(op, 1, 2).(*shuffle.Shuffle)

	require.NotNil(t, op.GetShufflePool())
	require.Same(t, op.GetShufflePool(), dup1.GetShufflePool())
	require.Same(t, op.GetShufflePool(), dup2.GetShufflePool())
}

func TestDupOperatorShuffleV2SharesPoolAcrossWorkers(t *testing.T) {
	op := shuffleV2.NewArgument()
	op.BucketNum = 4

	dup1 := dupOperator(op, 0, 2).(*shuffleV2.ShuffleV2)
	dup2 := dupOperator(op, 1, 2).(*shuffleV2.ShuffleV2)

	require.NotNil(t, op.GetShufflePool())
	require.Same(t, op.GetShufflePool(), dup1.GetShufflePool())
	require.Same(t, op.GetShufflePool(), dup2.GetShufflePool())
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
