// Copyright 2021 Matrix Origin
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

package minus

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type minusTestCase struct {
	proc   *process.Process
	arg    *Argument
	cancel context.CancelFunc
}

func TestMinus(t *testing.T) {
	proc := testutil.NewProcess()
	// [2 rows + 2 row, 3 columns] minus [1 row + 1 rows, 3 columns]
	/*
		{1, 2, 3}	{1, 2, 3}
		{1, 2, 3} minus {4, 5, 6} ==> {3, 4, 5}
		{3, 4, 5}
		{3, 4, 5}
	*/
	c := newMinusTestCase(
		proc,
		[]*batch.Batch{
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
		},
		[]*batch.Batch{
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
		},
	)

	err := Prepare(c.proc, c.arg)
	require.NoError(t, err)
	cnt := 0
	var end process.ExecStatus
	for {
		end, err = Call(0, c.proc, c.arg, false, false)
		if end == process.ExecStop {
			break
		}
		require.NoError(t, err)
		result := c.proc.InputBatch()
		if result != nil && !result.IsEmpty() {
			cnt += result.Length()
			require.Equal(t, 3, len(result.Vecs))
			c.proc.InputBatch().Clean(c.proc.Mp())
		}
	}
	c.arg.Free(c.proc, false)
	require.Equal(t, 1, cnt) // 1 row
	require.Equal(t, int64(0), c.proc.Mp().CurrNB())
}

func newMinusTestCase(proc *process.Process, leftBatches, rightBatches []*batch.Batch) minusTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	{
		c := make(chan *batch.Batch, len(leftBatches)+5)
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
		c := make(chan *batch.Batch, len(rightBatches)+5)
		for i := range rightBatches {
			c <- rightBatches[i]
		}
		c <- nil
		proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  c,
		}
	}
	proc.Reg.MergeReceivers[0].Ch <- nil
	proc.Reg.MergeReceivers[1].Ch <- nil
	arg := new(Argument)
	return minusTestCase{
		proc:   proc,
		arg:    arg,
		cancel: cancel,
	}
}
