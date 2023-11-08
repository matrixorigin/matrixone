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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testFunc = func(
	proc *process.Process,
	tableID uint64,
	eng engine.Engine,
	vec *vector.Vector,
	from, to timestamp.Timestamp) (bool, error) {
	return false, nil
}

func TestCallLockOpWithNoConflict(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = testFunc
			result, err := arg.Call(proc)
			require.NoError(t, err)

			vec := result.Batch.GetVector(1)
			values := vector.MustFixedCol[types.TS](vec)
			assert.Equal(t, 3, len(values))
			for _, v := range values {
				assert.Equal(t, types.TS{}, v)
			}
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflict(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = testFunc

			arg.rt.parker.Reset()
			arg.rt.parker.EncodeInt32(0)
			conflictRow := arg.rt.parker.Bytes()
			_, err := proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				result, err := arg.Call(proc)
				require.NoError(t, err)

				vec := result.Batch.GetVector(1)
				values := vector.MustFixedCol[types.TS](vec)
				assert.Equal(t, 3, len(values))
				for _, v := range values {
					assert.Equal(t, types.BuildTS(math.MaxInt64, 1), v)
				}
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflictWithRefreshNotEnabled(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = testFunc

			arg.rt.parker.Reset()
			arg.rt.parker.EncodeInt32(0)
			conflictRow := arg.rt.parker.Bytes()
			_, err := proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &Argument{
					info: &vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				}
				arg2.rt = &state{}
				arg2.rt.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.rt.hasNewVersionInRange = testFunc
				valueScan := arg.children[0].(*value_scan.Argument)
				resetChildren(arg2, valueScan.Batchs[0])
				defer arg2.rt.parker.FreeMem()

				_, err = arg2.Call(proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = arg2.Call(proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommit(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = testFunc

			arg.rt.parker.Reset()
			arg.rt.parker.EncodeInt32(0)
			conflictRow := arg.rt.parker.Bytes()

			// txn01 commit
			_, err := proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))

			// txn02 abort
			_, err = proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &Argument{
					info: &vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					}}
				arg2.rt = &state{}
				arg2.rt.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.rt.hasNewVersionInRange = testFunc
				valueScan := arg.children[0].(*value_scan.Argument)
				resetChildren(arg2, valueScan.Batchs[0])
				defer arg2.rt.parker.FreeMem()

				_, err = arg2.Call(proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = arg2.Call(proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommitLessMe(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = testFunc

			arg.rt.parker.Reset()
			arg.rt.parker.EncodeInt32(0)
			conflictRow := arg.rt.parker.Bytes()

			// txn01 commit
			_, err := proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64 - 1}))

			// txn02 abort
			_, err = proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &Argument{
					info: &vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					}}
				arg2.rt = &state{}
				arg2.rt.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.rt.hasNewVersionInRange = testFunc
				valueScan := arg.children[0].(*value_scan.Argument)
				resetChildren(arg2, valueScan.Batchs[0])
				defer arg2.rt.parker.FreeMem()

				proc.TxnOperator.TxnRef().SnapshotTS = timestamp.Timestamp{PhysicalTime: math.MaxInt64}

				_, err = arg2.Call(proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = arg2.Call(proc)
				require.NoError(t, err)
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestLockWithBlocking(t *testing.T) {
	values := [][]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
	runLockBlockingOpTest(
		t,
		1,
		values,
		nil,
		func(
			proc *process.Process,
			arg *Argument,
			idx int,
			isFirst, isLast bool) (bool, error) {
			arg.rt.hasNewVersionInRange = testFunc
			arg.info = &vm.OperatorInfo{
				Idx:     idx,
				IsFirst: isFirst,
				IsLast:  isLast,
			}
			end, err := arg.Call(proc)
			require.NoError(t, err)
			if end.Batch != nil {
				end.Batch.Clean(proc.GetMPool())
			}
			if end.Status == vm.ExecStop {
				if arg.rt.parker != nil {
					arg.rt.parker.FreeMem()
				}
			}
			return end.Status == vm.ExecStop, nil
		},
		func(arg *Argument, proc *process.Process) {
			arg.Free(proc, false, nil)
			proc.FreeVectors()
		},
	)
}

func TestLockWithBlockingWithConflict(t *testing.T) {
	values := [][]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
	runLockBlockingOpTest(
		t,
		1,
		values,
		func(proc *process.Process) {
			parker := types.NewPacker(proc.Mp())
			defer parker.FreeMem()

			parker.Reset()
			parker.EncodeInt32(1)
			conflictRow := parker.Bytes()

			_, err := proc.LockService.Lock(
				proc.Ctx,
				1,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			go func() {
				require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
				require.NoError(t, proc.LockService.Unlock(
					proc.Ctx,
					[]byte("txn01"),
					timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			}()
		},
		func(
			proc *process.Process,
			arg *Argument,
			idx int,
			isFirst, isLast bool) (bool, error) {
			arg.rt.hasNewVersionInRange = testFunc
			arg.info = &vm.OperatorInfo{
				Idx:     idx,
				IsFirst: isFirst,
				IsLast:  isLast,
			}
			ok, err := arg.Call(proc)
			return ok.Status == vm.ExecStop, err
		},
		func(arg *Argument, proc *process.Process) {
			require.True(t, moerr.IsMoErrCode(arg.rt.retryError, moerr.ErrTxnNeedRetry))
			for _, bat := range arg.rt.cachedBatches {
				bat.Clean(proc.Mp())
			}
			arg.Free(proc, false, nil)
			proc.FreeVectors()
		},
	)
}

func TestLockWithHasNewVersionInLockedTS(t *testing.T) {
	tw := client.NewTimestampWaiter()
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
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, arg.Prepare(proc))
			arg.rt.hasNewVersionInRange = func(
				proc *process.Process,
				tableID uint64,
				eng engine.Engine,
				vec *vector.Vector,
				from, to timestamp.Timestamp) (bool, error) {
				return true, nil
			}

			_, err := arg.Call(proc)
			require.NoError(t, err)
			require.Error(t, arg.rt.retryError)
			require.True(t, moerr.IsMoErrCode(arg.rt.retryError, moerr.ErrTxnNeedRetry))
		},
		client.WithTimestampWaiter(tw),
	)
	stopper.Stop()
}

func runLockNonBlockingOpTest(
	t *testing.T,
	tables []uint64,
	values [][]int32,
	fn func(*process.Process, *Argument),
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
			arg := NewArgument(nil)
			arg.info = &vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			for idx, table := range tables {
				arg.AddLockTarget(table, offset, pkType, offset+1)

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

func runLockBlockingOpTest(
	t *testing.T,
	table uint64,
	values [][]int32,
	beforeFunc func(proc *process.Process),
	fn func(proc *process.Process, arg *Argument, idx int, isFirst, isLast bool) (bool, error),
	checkFunc func(*Argument, *process.Process),
	opts ...client.TxnClientCreateOption) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			if beforeFunc != nil {
				beforeFunc(proc)
			}

			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgument(nil).SetBlock(true).AddLockTarget(table, 0, pkType, 1)

			var batches []*batch.Batch
			var batches2 []*batch.Batch
			for _, vs := range values {
				bat := batch.NewWithSize(2)
				bat.SetRowCount(2)

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, vs, nil, proc.Mp())
				bat.Vecs[0] = vec

				vec = vector.NewVec(tsType)
				bat.Vecs[1] = vec

				batches = append(batches, bat)
				batches2 = append(batches2, bat)
			}
			require.NoError(t, arg.Prepare(proc))
			arg.rt.batchFetchFunc = func(process.Analyze) (*batch.Batch, bool, error) {
				if len(batches) == 0 {
					return nil, true, nil
				}
				bat := batches[0]
				batches = batches[1:]
				return bat, false, nil
			}

			var err error
			var end bool
			for {
				end, err = fn(proc, arg, 1, false, false)
				if err != nil || end {
					break
				}
			}
			for _, bat := range batches2 {
				bat.Clean(proc.Mp())
			}
			checkFunc(arg, proc)
		},
		opts...)
}

func runLockOpTest(
	t *testing.T,
	fn func(*process.Process),
	opts ...client.TxnClientCreateOption) {
	defer leaktest.AfterTest(t)()
	lockservice.RunLockServicesForTest(
		zap.DebugLevel,
		[]string{"s1"},
		time.Second,
		func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
			runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.LockService, services[0])

			// TODO: remove
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			s, err := rpc.NewSender(rpc.Config{}, runtime.ProcessLevelRuntime())
			require.NoError(t, err)

			opts = append(opts, client.WithLockService(services[0]))
			c := client.NewTxnClient(s, opts...)
			c.Resume()
			defer func() {
				assert.NoError(t, c.Close())
			}()
			txnOp, err := c.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)

			proc := process.New(
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
}

func resetChildren(arg *Argument, bat *batch.Batch) {
	if len(arg.children) == 0 {
		arg.AppendChild(&value_scan.Argument{
			Batchs: []*batch.Batch{bat},
		})

	} else {
		arg.children = arg.children[:0]
		arg.AppendChild(&value_scan.Argument{
			Batchs: []*batch.Batch{bat},
		})
	}
}
