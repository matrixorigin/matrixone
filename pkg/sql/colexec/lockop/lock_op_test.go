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
	runLockOpTest(
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
	)
}

func TestCallLockOpWithConflict(t *testing.T) {
	runLockOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *Argument) {
			require.NoError(t, Prepare(proc, arg))

			arg.parker.Reset()
			arg.parker.EncodeInt32(0)
			conflictRow := arg.parker.Bytes()
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
	)
}

func runLockOpTest(
	t *testing.T,
	tables []uint64,
	values [][]int32,
	fn func(*process.Process, *Argument)) {
	defer leaktest.AfterTest(t)()
	lockservice.RunLockServicesForTest(
		zap.DebugLevel,
		[]string{"s1"},
		time.Second,
		func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			s, err := rpc.NewSender(rpc.Config{}, runtime.ProcessLevelRuntime())
			require.NoError(t, err)

			c := client.NewTxnClient(runtime.ProcessLevelRuntime(), s)
			defer func() {
				assert.NoError(t, c.Close())
			}()
			txnOp, err := c.New()
			require.NoError(t, err)

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

			bat := &batch.Batch{
				Zs:   make([]int64, len(tables)*2),
				Vecs: make([]*vector.Vector, 0, len(tables)*2),
			}
			defer func() {
				for _, vec := range bat.Vecs {
					vec.Free(proc.Mp())
				}
			}()

			offset := int32(0)
			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgument()
			for idx, table := range tables {
				arg.AddLockTarget(table, offset, pkType, offset+1)
				offset += 2

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, values[idx], nil, proc.Mp())
				bat.Vecs = append(bat.Vecs, vec)

				vec = vector.NewVec(tsType)
				bat.Vecs = append(bat.Vecs, vec)
			}
			proc.SetInputBatch(bat)
			fn(proc, arg)
			arg.Free(proc, false)
		},
		nil,
	)
}
