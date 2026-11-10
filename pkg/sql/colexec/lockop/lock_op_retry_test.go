// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lockRetryErrorChild struct {
	*colexec.MockOperator
	err error
}

func (child *lockRetryErrorChild) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := child.MockOperator.Call(proc)
	if err == nil && result.Batch == nil {
		err = child.err
	}
	return result, err
}

func TestLockOpRetryBeforeOutput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      uint64
		retry      bool
		definition bool
		childError error
		cancel     bool
	}{
		{name: "streaming without conflict", limit: 1},
		{name: "retry without limit", retry: true},
		{name: "retry with limit", limit: 1, retry: true},
		{name: "definition change overrides row retry", limit: 1, retry: true, definition: true},
		{name: "child error while draining", limit: 1, retry: true, childError: assert.AnError},
		{name: "cancellation while draining", limit: 1, retry: true, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runLockOpTest(t, func(proc *process.Process) {
				const tableID = uint64(1)
				callerCtx := proc.Ctx
				queryCtx, cancel := context.WithCancel(callerCtx)
				defer cancel()
				proc.Ctx = queryCtx
				batches := make([]*batch.Batch, 0, 3)
				for _, key := range []int32{0, 1} {
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = testutil.MakeInt32Vector([]int32{key}, nil, proc.Mp())
					bat.SetRowCount(1)
					defer bat.Clean(proc.Mp())
					batches = append(batches, bat)
					if key == 0 {
						batches = append(batches, batch.EmptyBatch)
					}
				}
				pulled, ended := 0, false
				child := &lockRetryErrorChild{MockOperator: colexec.NewMockOperator().WithBatchs(batches).
					WithBatchCallback(func(index int) {
						pulled++
						if tc.cancel && index == 1 {
							cancel()
						}
					}).
					WithEndOfDataCallback(func() { ended = true }), err: tc.childError}
				arg := NewArgumentByEngine(nil)
				defer arg.Release()
				defer arg.Free(proc, false, nil)
				arg.AddLockTarget(tableID, nil, 0, types.T_int32.ToType(), -1, -1, nil, false)
				arg.AppendChild(child)
				require.NoError(t, arg.Prepare(proc))
				arg.ctr.hasNewVersionInRange = testFunc

				// Model a committed row version, followed optionally by a DDL
				// fence on the second batch. Real lockservice still owns both keys.
				runtime.SetupServiceRuntimeTestingContext(proc.GetService())
				testingContext := runtime.MustGetTestingContext(proc.GetService())
				testingContext.SetBeforeLockFunc(func([]byte, uint64) {})
				defer testingContext.SetBeforeLockFunc(nil)
				locks := 0
				testingContext.SetAdjustLockResultFunc(func(_ []byte, _ uint64, result *lock.Result) {
					locks++
					if tc.retry {
						result.HasConflict = true
						result.HasPrevCommit = true
						result.Timestamp = proc.GetTxnOperator().Txn().SnapshotTS.Next()
						if tc.definition && locks == 2 {
							result.TableDefChanged = true
						}
					}
				})
				defer testingContext.SetAdjustLockResultFunc(nil)

				var parent vm.Operator = arg
				if tc.limit != 0 {
					lim := limit.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(tc.limit))
					defer lim.Release()
					defer lim.Free(proc, false, nil)
					lim.AppendChild(arg)
					require.NoError(t, lim.Prepare(proc))
					parent = lim
				}
				emitted := 0
				out := output.NewArgument().WithBlock(true).WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
					emitted += bat.RowCount()
					return nil
				})
				defer out.Release()
				defer out.Free(proc, false, nil)
				out.AppendChild(parent)
				require.NoError(t, out.Prepare(proc))
				_, err := vm.Exec(out, proc)
				if !tc.retry {
					require.NoError(t, err)
					require.Equal(t, 1, emitted)
					require.Equal(t, 1, pulled)
					require.False(t, ended, "uncontended input must remain streaming")
					return
				}
				if tc.cancel {
					require.ErrorIs(t, err, context.Canceled)
				} else if tc.childError != nil {
					require.ErrorIs(t, err, tc.childError)
				} else if tc.definition {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)
				} else {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry), err)
				}
				require.Zero(t, emitted, "an obsolete batch must not reach output")
				lockedKeys := []int32{0, 1}
				if tc.cancel {
					require.False(t, ended)
					require.Equal(t, 2, pulled, "stop pulling as soon as the process is canceled")
					require.Equal(t, 1, locks)
					lockedKeys = lockedKeys[:1]
				} else {
					require.True(t, ended)
					require.Equal(t, len(batches), pulled, "finish locking the input before retrying")
					require.Equal(t, 2, locks)
				}
				for _, key := range lockedKeys {
					arg.ctr.parker.Reset()
					arg.ctr.parker.EncodeInt32(key)
					holder, found, err := proc.GetLockService().GetLockHolder(callerCtx, tableID,
						arg.ctr.parker.Bytes(), lock.LockOptions{Mode: lock.LockMode_Exclusive})
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, proc.GetTxnOperator().Txn().ID, holder.TxnID)
				}
			}, client.WithTimestampWaiter(immediateLockTimestampWaiter{}))
		})
	}
}
