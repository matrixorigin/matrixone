// Copyright 2023 Matrix Origin
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

package lockop

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var testFunc = func(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	vec *vector.Vector,
	from, to timestamp.Timestamp) (bool, error) {
	return false, nil
}

var (
	sid = ""
)

func TestCallLockOpWithNoConflict(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)

			vec := result.Batch.GetVector(1)
			values := vector.MustFixedColWithTypeCheck[types.TS](vec)
			assert.Equal(t, 3, len(values))
			for _, v := range values {
				assert.Equal(t, types.TS{}, v)
			}
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflict(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)

				vec := result.Batch.GetVector(1)
				values := vector.MustFixedColWithTypeCheck[types.TS](vec)
				assert.Equal(t, 3, len(values))
				for _, v := range values {
					assert.Equal(t, types.BuildTS(math.MaxInt64, 1), v)
				}
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflictWithRefreshNotEnabled(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommit(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()

			// txn01 commit
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))

			// txn02 abort
			_, err = proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommitLessMe(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()

			// txn01 commit
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64 - 1}))

			// txn02 abort
			_, err = proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				proc.GetTxnOperator().TxnRef().SnapshotTS = timestamp.Timestamp{PhysicalTime: math.MaxInt64}

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.NoError(t, err)
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestLockWithHasNewVersionInLockedTS(t *testing.T) {
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	stopper := stopper.NewStopper("")
	stopper.RunTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 100):
				tw.NotifyLatestCommitTS(timestamp.Timestamp{PhysicalTime: time.Now().UTC().UnixNano()})
			}
		}
	})
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = func(
				proc *process.Process,
				rel engine.Relation,
				analyzer process.Analyzer,
				tableID uint64,
				eng engine.Engine,
				vec *vector.Vector,
				from, to timestamp.Timestamp) (bool, error) {
				return true, nil
			}

			_, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.Error(t, arg.ctr.retryError)
			require.True(t, moerr.IsMoErrCode(arg.ctr.retryError, moerr.ErrTxnNeedRetry))
		},
		client.WithTimestampWaiter(tw),
	)
	stopper.Stop()
}

func runLockNonBlockingOpTest(
	t *testing.T,
	tables []uint64,
	values [][]int32,
	fn func(*process.Process, *LockOp),
	opts ...client.TxnClientCreateOption) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			bat := batch.NewWithSize(len(tables) * 2)
			bat.SetRowCount(len(tables) * 2)

			defer func() {
				bat.Clean(proc.Mp())
			}()

			offset := int32(0)
			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgumentByEngine(nil)
			arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			for idx, table := range tables {
				arg.AddLockTarget(table, offset, pkType, offset+1, nil, true)

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, values[idx], nil, proc.Mp())
				bat.Vecs[offset] = vec

				vec = vector.NewVec(tsType)
				bat.Vecs[offset+1] = vec
				offset += 2
			}
			resetChildren(arg, bat)

			fn(proc, arg)
			arg.Free(proc, false, nil)
		},
		opts...)
}

func runLockOpTest(
	t *testing.T,
	fn func(*process.Process),
	opts ...client.TxnClientCreateOption) {
	defer leaktest.AfterTest(t)()
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("s1", rt)

			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"s1"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					// TODO: remove
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					s, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)

					opts = append(opts, client.WithLockService(services[0]))
					c := client.NewTxnClient(sid, s, opts...)
					c.Resume()
					defer func() {
						assert.NoError(t, c.Close())
					}()
					txnOp, err := c.New(ctx, timestamp.Timestamp{})
					require.NoError(t, err)

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						c,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil)
					require.Equal(t, int64(0), proc.Mp().CurrNB())
					defer func() {
						require.Equal(t, int64(0), proc.Mp().CurrNB())
					}()
					fn(proc)
				},
				nil,
			)
		},
	)
}

func resetChildren(arg *LockOp, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
