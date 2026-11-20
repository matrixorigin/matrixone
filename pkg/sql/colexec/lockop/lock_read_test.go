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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLockReadFastPipelineRowLock(t *testing.T) {
	const tableID = 10
	runLockNonBlockingOpTest(t, []uint64{tableID}, [][]int32{{7}}, func(proc *process.Process, op *LockOp) {
		root := resource.NewRoot(resource.ConnExternal)
		proc.Ctx = resource.ContextWithLockReadRoot(proc.Ctx, root)
		defer func() { require.NoError(t, proc.GetTxnOperator().Rollback(proc.Ctx)) }()
		require.NoError(t, op.Prepare(proc))
		op.ctr.hasNewVersionInRange = testFunc
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		require.Equal(t, []int32{7}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
		_, snapshot := root.SealWithLockRead(0)
		diagnostic := snapshot.Diagnostic(uuid.Nil, 1, false)
		require.NotZero(t, diagnostic.Seen&1)
		require.Positive(t, diagnostic.AcquisitionNS)
		require.Empty(t, diagnostic.LastIncomplete)
		require.False(t, snapshot.Qualifies(0, time.Second))
	})
}

type lockReadFrontierContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *lockReadFrontierContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type lockReadFrontierWaiter struct {
	client.TimestampWaiter
	waiting chan struct{}
}

func (w *lockReadFrontierWaiter) GetTimestamp(ctx context.Context, target timestamp.Timestamp) (timestamp.Timestamp, error) {
	// Observe the built-in waiter's Done call rather than the txn client's
	// context construction, which differs between supported branches.
	waitCtx := &lockReadFrontierContext{Context: ctx, waiting: w.waiting}
	return w.TimestampWaiter.GetTimestamp(waitCtx, target)
}

func TestLockReadLogtailFrontier(t *testing.T) {
	for _, name := range []string{"advance", "cancel"} {
		t.Run(name, func(t *testing.T) {
			waiter := &lockReadFrontierWaiter{
				TimestampWaiter: client.NewTimestampWaiter(runtime.DefaultRuntime().Logger()),
				waiting:         make(chan struct{}),
			}
			defer waiter.Close()
			client.RunTxnTests(func(txnClient client.TxnClient, _ rpc.TxnSender) {
				base, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				target := timestamp.Timestamp{PhysicalTime: 100}
				o := &resource.LockReadObservation{}
				type result struct {
					ts  timestamp.Timestamp
					err error
				}
				done := make(chan result, 1)
				received := false
				go func() {
					ts, err := waitLockReadLogtail(base, txnClient, target, o)
					done <- result{ts, err}
				}()
				defer func() {
					cancel()
					if !received {
						<-done
					}
				}()
				select {
				case <-waiter.waiting:
				case <-base.Done():
					t.Fatal("target timestamp did not wait for the CN frontier")
				}
				waitedAt := time.Now()
				select {
				case <-done:
					received = true
					t.Fatal("target was returned before advancing the CN frontier")
				default:
				}
				releasedAt := time.Now()
				if name == "cancel" {
					cancel()
				} else {
					waiter.NotifyLatestCommitTS(target)
				}
				got := <-done
				received = true
				root := resource.NewRoot(resource.ConnExternal)
				root.MergeLockRead(o)
				_, snapshot := root.SealWithLockRead(0)
				d := snapshot.Diagnostic(uuid.Nil, 1, got.err != nil)
				require.Equal(t, uint8(2), d.Seen)
				require.GreaterOrEqual(t, d.LogtailNS, uint64(releasedAt.Sub(waitedAt)))
				require.Zero(t, d.AcquisitionNS)
				if name == "cancel" {
					require.ErrorIs(t, got.err, context.Canceled)
					require.Equal(t, "logtail", d.LastIncomplete)
					require.True(t, snapshot.Qualifies(0, time.Second))
				} else {
					require.NoError(t, got.err)
					require.Equal(t, target.Next(), got.ts)
					require.Empty(t, d.LastIncomplete)
					waiter.waiting = make(chan struct{})
					ts, err := waitLockReadLogtail(base, txnClient, target, nil)
					require.NoError(t, err)
					require.Equal(t, target.Next(), ts)
					select {
					case <-waiter.waiting:
						t.Fatal("already-applied target blocked")
					default:
					}
				}
			}, client.WithTimestampWaiter(waiter))
		})
	}
}
