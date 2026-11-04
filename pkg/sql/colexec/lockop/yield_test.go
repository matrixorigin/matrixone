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

package lockop

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type yieldingLockInput struct {
	*colexec.MockOperator
	calls int
}

func (input *yieldingLockInput) Call(proc *process.Process) (vm.CallResult, error) {
	input.calls++
	if input.calls == 1 || input.calls == 3 {
		return vm.CallResult{Status: vm.ExecYield}, nil
	}
	return input.MockOperator.Call(proc)
}

func TestLockOpYieldPreservesRetryDrain(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "stream"
		if retry {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			runLockOpTest(t, func(proc *process.Process) {
				makeBatch := func(key int32) *batch.Batch {
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = testutil.MakeInt32Vector([]int32{key}, nil, proc.Mp())
					bat.SetRowCount(1)
					return bat
				}
				first, second := makeBatch(1), makeBatch(2)
				defer first.Clean(proc.Mp())
				defer second.Clean(proc.Mp())
				input := &yieldingLockInput{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})}
				arg := NewArgumentByEngine(nil)
				defer arg.Release()
				defer arg.Free(proc, false, nil)
				arg.AddLockTarget(1, nil, 0, types.T_int32.ToType(), -1, -1, nil, false)
				arg.AppendChild(input)
				require.NoError(t, arg.Prepare(proc))
				arg.ctr.hasNewVersionInRange = testFunc

				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecYield, result.Status)
				require.Nil(t, result.Batch)
				require.Zero(t, arg.ctr.lockCount)
				require.False(t, proc.GetTxnOperator().HasLockTable(uint64(1)))

				result, err = vm.Exec(arg, proc)
				require.NoError(t, err)
				require.Same(t, first, result.Batch)
				require.Equal(t, int64(1), arg.ctr.lockCount)
				if retry {
					arg.ctr.retryError = moerr.NewTxnNeedRetryNoCtx()
				}
				pending := arg.ctr.retryError

				result, err = vm.Exec(arg, proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecYield, result.Status)
				require.Nil(t, result.Batch)
				require.Equal(t, pending, arg.ctr.retryError)
				require.Equal(t, int64(1), arg.ctr.lockCount)
				require.Equal(t, 3, input.calls)

				result, err = vm.Exec(arg, proc)
				if retry {
					require.ErrorIs(t, err, pending)
					require.Nil(t, result.Batch, "retry drains stale batches without forwarding them")
				} else {
					require.NoError(t, err)
					require.Same(t, second, result.Batch)
					result, err = vm.Exec(arg, proc)
					require.NoError(t, err)
					require.Nil(t, result.Batch)
				}
				require.Equal(t, int64(2), arg.ctr.lockCount)
				require.Equal(t, 5, input.calls)
			})
		})
	}
}
