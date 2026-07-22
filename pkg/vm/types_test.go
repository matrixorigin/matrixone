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

package vm

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testOperator struct {
	OperatorBase
	name      string
	callCount int
	callErr   error
	result    CallResult
	projected *batch.Batch
}

func (op *testOperator) Free(*process.Process, bool, error) {}

func (op *testOperator) Reset(*process.Process, bool, error) {}

func (op *testOperator) String(buf *bytes.Buffer) {
	buf.WriteString(op.name)
}

func (op *testOperator) OpType() OpType { return Mock }

func (op *testOperator) Prepare(*process.Process) error { return nil }

func (op *testOperator) Call(*process.Process) (CallResult, error) {
	op.callCount++
	if op.callErr != nil {
		return op.result, op.callErr
	}
	return op.result, nil
}

func (op *testOperator) Release() {}

func (op *testOperator) GetOperatorBase() *OperatorBase { return &op.OperatorBase }

func (op *testOperator) ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if op.projected != nil {
		return op.projected, nil
	}
	return input, nil
}

func TestOperatorTypeAndBaseAccessors(t *testing.T) {
	require.Equal(t, "IcebergWrite", IcebergWrite.String())
	require.Equal(t, "Unknown", OpType(9999).String())

	info := &OperatorInfo{
		Idx:         7,
		IsFirst:     true,
		IsLast:      false,
		CnAddr:      "cn-a",
		OperatorID:  11,
		ParallelID:  13,
		MaxParallel: 17,
	}
	var base OperatorBase
	base.SetInfo(info)
	require.Equal(t, 7, base.GetIdx())
	require.True(t, base.GetIsFirst())
	require.False(t, base.GetIsLast())
	require.Equal(t, "cn-a", base.GetCnAddr())
	require.Equal(t, int32(11), base.GetOperatorID())
	require.Equal(t, int32(13), base.GetParalleID())
	require.Equal(t, int32(17), base.GetMaxParallel())

	base.SetCnAddr("cn-b")
	base.SetOperatorID(19)
	base.SetParalleID(23)
	base.SetMaxParallel(29)
	base.SetIdx(31)
	base.SetIsFirst(false)
	base.SetIsLast(true)
	base.SetAnalyzeControl(37, true)
	require.Equal(t, "cn-b", base.GetCnAddr())
	require.Equal(t, int32(19), base.GetOperatorID())
	require.Equal(t, int32(23), base.GetParalleID())
	require.Equal(t, int32(29), base.GetMaxParallel())
	require.Equal(t, 37, base.GetIdx())
	require.True(t, base.GetIsFirst())
	require.True(t, base.GetIsLast())

	addr := base.OperatorInfo.GetAddress()
	require.Equal(t, "cn-b", addr.CnAddr)
	require.Equal(t, int32(19), addr.OperatorID)
	require.Equal(t, int32(23), addr.ParallelID)
}

func TestOperatorToStrMapCompleteness(t *testing.T) {
	const reservedShuffleV2 = Shuffle + 1

	for op := Top; op < OpTypeEnd; op++ {
		if op == reservedShuffleV2 {
			continue
		}
		if op.String() == "Unknown" {
			t.Errorf("missing operator name for %d", op)
		}
	}
}

func TestOperatorChildrenAndTraversal(t *testing.T) {
	root := &testOperator{name: "root"}
	left := &testOperator{name: "left"}
	right := &testOperator{name: "right"}
	leaf := &testOperator{name: "leaf"}

	root.AppendChild(left)
	root.AppendChild(right)
	right.AppendChild(leaf)
	require.Equal(t, 2, root.NumChildren())
	require.Same(t, left, root.GetChildren(0))
	require.Same(t, right, root.GetChildren(1))

	replacement := &testOperator{name: "replacement"}
	root.SetChild(replacement, 0)
	require.Same(t, replacement, root.GetChildren(0))

	var visits []string
	err := HandleAllOp(root, func(parentOp Operator, op Operator) error {
		if parentOp == nil {
			visits = append(visits, "nil/"+op.(*testOperator).name)
			return nil
		}
		visits = append(visits, parentOp.(*testOperator).name+"/"+op.(*testOperator).name)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"root/replacement", "right/leaf", "root/right", "nil/root"}, visits)

	var leaves []string
	err = HandleLeafOp(nil, root, func(parentOp Operator, leafOp Operator) error {
		leaves = append(leaves, parentOp.(*testOperator).name+"/"+leafOp.(*testOperator).name)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"root/replacement", "right/leaf"}, leaves)

	require.Same(t, replacement, GetLeafOp(root))
	require.Same(t, root, GetLeafOpParent(nil, root))
	require.NoError(t, HandleAllOp(nil, func(parentOp Operator, op Operator) error {
		t.Fatalf("nil root should not invoke handler")
		return nil
	}))
	require.NoError(t, HandleLeafOp(nil, nil, func(parentOp Operator, op Operator) error {
		t.Fatalf("nil root should not invoke handler")
		return nil
	}))

	root.ResetChildren()
	require.Equal(t, 0, root.NumChildren())
	root.SetChildren([]Operator{leaf})
	require.Equal(t, 1, root.NumChildren())
	require.Same(t, leaf, root.GetChildren(0))
}

func TestExecCancelAndProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proc := &process.Process{Ctx: ctx}
	op := &testOperator{
		OperatorBase: OperatorBase{OpAnalyzer: process.NewTempAnalyzer()},
		result:       NewCallResult(),
	}
	result, err := Exec(op, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, ExecStop, result.Status)
	require.Equal(t, 0, op.callCount)

	input := batch.NewWithSize(0)
	input.SetRowCount(2)
	projected := batch.NewWithSize(0)
	projected.SetRowCount(1)
	proc = &process.Process{Ctx: context.Background()}
	op = &testOperator{
		OperatorBase: OperatorBase{OpAnalyzer: process.NewTempAnalyzer()},
		result:       CallResult{Status: ExecNext, Batch: input},
		projected:    projected,
	}
	result, err = Exec(op, proc)
	require.NoError(t, err)
	require.Same(t, projected, result.Batch)
	require.Equal(t, 1, op.callCount)
	require.Equal(t, int64(1), op.GetOperatorBase().OpAnalyzer.GetOpStats().OutputRows)

	callErr := errors.New("call failed")
	op = &testOperator{
		OperatorBase: OperatorBase{OpAnalyzer: process.NewTempAnalyzer()},
		callErr:      callErr,
		result:       CallResult{Status: ExecHasMore},
	}
	result, err = Exec(op, proc)
	require.ErrorIs(t, err, callErr)
	require.Equal(t, ExecHasMore, result.Status)
}

func TestChildrenCallRecordsInputAfterChildExec(t *testing.T) {
	childBatch := batch.NewWithSize(0)
	childBatch.SetRowCount(3)
	proc := &process.Process{Ctx: context.Background()}
	child := &testOperator{
		OperatorBase: OperatorBase{OpAnalyzer: process.NewTempAnalyzer()},
		result:       CallResult{Status: ExecNext, Batch: childBatch},
	}
	parentAnalyzer := process.NewTempAnalyzer()

	result, err := ChildrenCall(child, proc, parentAnalyzer)
	require.NoError(t, err)
	require.Same(t, childBatch, result.Batch)
	require.Equal(t, int64(3), parentAnalyzer.GetOpStats().InputRows)
}
