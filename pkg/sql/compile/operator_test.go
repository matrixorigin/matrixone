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
	"testing"

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
	result := dupOperator(op, 0, 1)
	if result == nil {
		t.Fatal("dupOperator returned nil for MergeOrder")
	}
	dupOp := result.(*mergeorder.MergeOrder)
	if len(dupOp.OrderBySpecs) != len(op.OrderBySpecs) {
		t.Errorf("OrderBySpecs length mismatch: got %d, want %d", len(dupOp.OrderBySpecs), len(op.OrderBySpecs))
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
