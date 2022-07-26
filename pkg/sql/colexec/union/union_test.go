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

package union

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

type unionTestCase struct {
	proc   *process.Process
	arg    *Argument
	cancel context.CancelFunc
}

func TestUnion(t *testing.T) {
	proc := testutil.NewProcess()
	// [4 rows + 3 rows, 2 columns] union [5 rows + 5 rows, 2 columns]
	c := newUnionTestCase(
		proc,
		[]*batch.Batch{
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(4, types.T_int64.ToType(), proc.Mp, true, nil),
					testutil.NewVector(4, types.T_int64.ToType(), proc.Mp, true, nil),
				}, nil),

			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(3, types.T_int64.ToType(), proc.Mp, true, nil),
					testutil.NewVector(3, types.T_int64.ToType(), proc.Mp, true, nil),
				}, nil),
		},

		[]*batch.Batch{
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(5, types.T_int64.ToType(), proc.Mp, true, nil),
					testutil.NewVector(5, types.T_int64.ToType(), proc.Mp, true, nil),
				}, nil),

			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.NewVector(5, types.T_int64.ToType(), proc.Mp, true, nil),
					testutil.NewVector(5, types.T_int64.ToType(), proc.Mp, true, nil),
				}, nil),
		},
	)

	err := Prepare(c.proc, c.arg)
	require.NoError(t, err)
	_, err = Call(0, c.proc, c.arg)
	{
		result := c.arg.ctr.bat
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Vecs))               // 2 columns
		require.Equal(t, 17, vector.Length(result.Vecs[0])) // 17 = (4+3+5+5) rows
	}
	c.proc.Reg.InputBatch.Clean(c.proc.Mp) // clean the final result
	require.Equal(t, int64(0), mheap.Size(c.proc.Mp))
}

func newUnionTestCase(proc *process.Process, leftBatches, rightBatches []*batch.Batch) unionTestCase {
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	{
		c := make(chan *batch.Batch, len(leftBatches)+1)
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
		c := make(chan *batch.Batch, len(rightBatches)+1)
		for i := range rightBatches {
			c <- rightBatches[i]
		}
		c <- nil
		proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  c,
		}
	}
	arg := &Argument{}
	return unionTestCase{
		proc:   proc,
		arg:    arg,
		cancel: cancel,
	}
}
