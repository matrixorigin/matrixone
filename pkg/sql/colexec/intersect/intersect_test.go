// Copyright 2022 Matrix Origin
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

package intersect

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type intersectTestCase struct {
	proc   *process.Process
	arg    *Argument
	cancel context.CancelFunc
}

func TestIntersect(t *testing.T) {
	proc := testutil.NewProcess()
	// [2 rows + 2 row, 3 columns] intersect [1 row + 1 rows, 3 columns]
	/*
		{1, 2, 3}	    {1, 2, 3}
		{1, 2, 3} intersect {4, 5, 6} ==> {1, 2, 3}
		{3, 4, 5}
		{3, 4, 5}
	*/
	var end vm.CallResult
	c, ctx := newIntersectTestCase(proc)

	setProcForTest(ctx, proc)
	err := c.arg.Prepare(c.proc)
	require.NoError(t, err)
	cnt := 0
	end, err = c.arg.Call(c.proc)
	require.NoError(t, err)
	result := end.Batch
	if result != nil && !result.IsEmpty() {
		cnt += result.RowCount()
		require.Equal(t, 3, len(result.Vecs)) // 3 column
	}
	require.Equal(t, 1, cnt) // 1 row
	c.proc.Reg.MergeReceivers[0].Ch <- nil
	c.proc.Reg.MergeReceivers[1].Ch <- nil

	c.arg.Reset(c.proc, false, nil)

	setProcForTest(ctx, proc)
	err = c.arg.Prepare(c.proc)
	require.NoError(t, err)
	cnt = 0
	end, err = c.arg.Call(c.proc)
	require.NoError(t, err)
	result = end.Batch
	if result != nil && !result.IsEmpty() {
		cnt += result.RowCount()
		require.Equal(t, 3, len(result.Vecs)) // 3 column
	}
	require.Equal(t, 1, cnt) // 1 row
	c.proc.Reg.MergeReceivers[0].Ch <- nil
	c.proc.Reg.MergeReceivers[1].Ch <- nil
	c.arg.Free(c.proc, false, nil)
	proc.FreeVectors()
	require.Equal(t, int64(0), c.proc.Mp().CurrNB())
}

func newIntersectTestCase(proc *process.Process) (intersectTestCase, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	arg := new(Argument)
	arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
		Idx:     0,
		IsFirst: false,
		IsLast:  false,
	}
	return intersectTestCase{
		proc:   proc,
		arg:    arg,
		cancel: cancel,
	}, ctx
}

func setProcForTest(ctx context.Context, proc *process.Process) {
	leftBatches := []*batch.Batch{
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{2, 2}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
			}, nil),
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{4, 4}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{5, 5}),
			}, nil),
	}

	rightBatches := []*batch.Batch{
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{2}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{3}),
			}, nil),
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{4}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{5}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{6}),
			}, nil),
	}

	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	{
		c := make(chan *batch.Batch, len(leftBatches)+10)
		for i := range leftBatches {
			c <- leftBatches[i]
		}
		c <- nil
		proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  c,
		}
	}
	{
		c := make(chan *batch.Batch, len(rightBatches)+10)
		for i := range rightBatches {
			c <- rightBatches[i]
		}
		c <- nil
		proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  c,
		}
	}
}
