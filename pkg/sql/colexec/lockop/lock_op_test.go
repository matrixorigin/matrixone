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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCallLockOpWithNoConflict(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, Prepare(proc, arg))
			_, err := Call(0, proc, arg, false, false)
			require.NoError(t, err)

			vec := proc.InputBatch().GetVector(1)
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
			require.NoError(t, Prepare(proc, arg))

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
				_, err = Call(0, proc, arg, false, false)
				require.NoError(t, err)

				vec := proc.InputBatch().GetVector(1)
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
			require.NoError(t, Prepare(proc, arg))

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
				arg2 := &Argument{}
				arg2.rt = &state{}
				arg2.rt.retryError = nil
				arg2.targets = arg.targets
				Prepare(proc, arg2)
				defer arg2.rt.parker.FreeMem()

				_, err = Call(0, proc, arg2, false, false)
				assert.NoError(t, err)

				proc.SetInputBatch(nil)
				_, err = Call(0, proc, arg2, false, false)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.LockService, 1, conflictRow, 1))
			require.NoError(t, proc.LockService.Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
	)
}

func TestLockWithBlocking(t *testing.T) {
	var downstreamBatches []*batch.Batch
	values := [][]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
	n := 0
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
			end, err := Call(idx, proc, arg, isFirst, isLast)
			require.NoError(t, err)
			if arg.rt.step == stepLock {
				require.Equal(t, batch.EmptyBatch, proc.InputBatch())
			} else if arg.rt.step == stepDownstream {
				if n > 0 {
					downstreamBatches = append(downstreamBatches, proc.InputBatch())
				}
				n++
			} else {
				if !end {
					downstreamBatches = append(downstreamBatches, proc.InputBatch())
				} else {
					require.Equal(t, 3, len(downstreamBatches))
					for i, bat := range downstreamBatches {
						require.Equal(t, values[i], vector.MustFixedCol[int32](bat.GetVector(0)))
						bat.Clean(proc.Mp())
					}
				}
			}
			return end, nil
		},
		func(a *Argument) {
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
			return Call(idx, proc, arg, isFirst, isLast)
		},
		func(arg *Argument) {
			require.True(t, moerr.IsMoErrCode(arg.rt.retryError, moerr.ErrTxnNeedRetry))
			require.Empty(t, arg.rt.cachedBatches)
		},
	)
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
			bat.Zs = make([]int64, len(tables)*2)

			defer func() {
				bat.Clean(proc.Mp())
			}()

			offset := int32(0)
			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgument()
			for idx, table := range tables {
				arg.AddLockTarget(table, offset, pkType, offset+1)

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, values[idx], nil, proc.Mp())
				bat.Vecs[offset] = vec

				vec = vector.NewVec(tsType)
				bat.Vecs[offset+1] = vec
				offset += 2
			}
			proc.SetInputBatch(bat)
			fn(proc, arg)
			arg.Free(proc, false)
		},
		opts...)
}

func runLockBlockingOpTest(
	t *testing.T,
	table uint64,
	values [][]int32,
	beforeFunc func(proc *process.Process),
	fn func(proc *process.Process, arg *Argument, idx int, isFirst, isLast bool) (bool, error),
	checkFunc func(*Argument),
	opts ...client.TxnClientCreateOption) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			if beforeFunc != nil {
				beforeFunc(proc)
			}

			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgument().SetBlock(true).AddLockTarget(table, 0, pkType, 1)

			var batches []*batch.Batch
			for _, vs := range values {
				bat := batch.NewWithSize(2)
				bat.Zs = make([]int64, 2)

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, vs, nil, proc.Mp())
				bat.Vecs[0] = vec

				vec = vector.NewVec(tsType)
				bat.Vecs[1] = vec

				batches = append(batches, bat)
			}
			require.NoError(t, Prepare(proc, arg))
			arg.rt.batchFetchFunc = func(process.Analyze) (*batch.Batch, bool, error) {
				if len(batches) == 0 {
					return nil, true, nil
				}
				bat := batches[0]
				batches = batches[1:]
				return bat, false, nil
			}

			i := 0
			sum := len(batches) - 1
			var err error
			var end bool
			for {
				end, err = fn(proc, arg, i, i == 0, i == sum)
				if err != nil || end {
					break
				}
				i++
			}
			checkFunc(arg)
			arg.Free(proc, false)
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
			// TODO: remove
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			s, err := rpc.NewSender(rpc.Config{}, runtime.ProcessLevelRuntime())
			require.NoError(t, err)

			c := client.NewTxnClient(s, opts...)
			defer func() {
				assert.NoError(t, c.Close())
			}()
			txnOp, err := c.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)

			txnOp.TxnRef().LockService = services[0].GetConfig().ServiceID

			proc := process.New(
				ctx,
				mpool.MustNewZero(),
				c,
				txnOp,
				nil,
				services[0],
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
