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

package lockservice

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// unlockAfterApplyErrorTable simulates an owner that applies an Unlock before
// the source CN loses the response.
type unlockAfterApplyErrorTable struct {
	lockTable
	err error
}

// unlockBeforeApplyErrorTable simulates a request that fails before the owner
// receives it.
type unlockBeforeApplyErrorTable struct {
	lockTable
	err error
}

type recordingUnlockTable struct {
	lockTable
	mutations []pb.ExtraMutation
}

type blockingProxyLockTable struct {
	lockTable
	bind          pb.LockTable
	started       chan struct{}
	release       chan struct{}
	err           error
	reuseExisting bool
	calls         atomic.Int32
}

func (t *blockingProxyLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	_ [][]byte,
	_ LockOptions,
	cb func(pb.Result, error),
) {
	t.calls.Add(1)
	select {
	case t.started <- struct{}{}:
	default:
	}
	// Match remoteLockTable.lock's mutex contract: the RPC wait does not retain
	// the transaction mutex, and the callback runs after reacquiring it.
	txn.Unlock()
	defer txn.Lock()
	select {
	case <-t.release:
		cb(pb.Result{NewLockAdd: !t.reuseExisting}, t.err)
	case <-ctx.Done():
		cb(pb.Result{}, ctx.Err())
	}
}

func (t *blockingProxyLockTable) unlockWithContext(
	_ context.Context,
	_ *activeTxn,
	_ *cowSlice,
	_ timestamp.Timestamp,
	_ ...pb.ExtraMutation,
) error {
	return nil
}

func closeProxyTestTxn(
	t *testing.T,
	txn *activeTxn,
	proxy *localLockTableProxy,
) {
	txn.Lock()
	defer txn.Unlock()
	require.NoError(t, txn.close(
		txn.txnID,
		timestamp.Timestamp{},
		func(pb.LockTable) (lockTable, error) { return proxy, nil },
		getLogger(""),
	))
}

func proxyReentrantWaiterCount(ops *sharedOps) int {
	n := 0
	for idx := range ops.pending {
		n += len(ops.pending[idx].reentrantWaiters)
	}
	return n
}

func TestProxySameTxnSharedReentryJoinsInFlightGeneration(t *testing.T) {
	reuse.RunReuseTests(func() {
		for _, cancelReentry := range []bool{false, true} {
			name := "success"
			if cancelReentry {
				name = "canceled-reentry"
			}
			t.Run(name, func(t *testing.T) {
				bind := pb.LockTable{
					Group:     0,
					Table:     26771,
					ServiceID: "remote",
					Valid:     true,
				}
				remote := &blockingProxyLockTable{
					bind:    bind,
					started: make(chan struct{}, 1),
					release: make(chan struct{}),
				}
				proxy := newLockTableProxy("local", "", remote, getLogger("")).(*localLockTableProxy)
				txn := newActiveTxn(
					[]byte("same-txn-reentry"),
					"same-txn-reentry",
					newFixedSlicePool(4),
					"",
				)
				rows := [][]byte{[]byte("row")}
				options := LockOptions{LockOptions: newTestRowSharedOptions()}

				firstDone := make(chan error, 1)
				go func() {
					txn.Lock()
					defer txn.Unlock()
					proxy.lock(context.Background(), txn, rows, options,
						func(_ pb.Result, err error) { firstDone <- err })
				}()
				select {
				case <-remote.started:
				case <-time.After(time.Second):
					t.Fatal("first remote lock did not start")
				}

				reentryCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reentryDone := make(chan error, 1)
				go func() {
					txn.Lock()
					defer txn.Unlock()
					proxy.lock(reentryCtx, txn, rows, options,
						func(_ pb.Result, err error) { reentryDone <- err })
				}()

				// The in-flight RPC owns the single holder generation. A re-entry may
				// subscribe to its result, but it must not append the same transaction
				// as another holder.
				require.Eventually(t, func() bool {
					proxy.mu.RLock()
					defer proxy.mu.RUnlock()
					ops := proxy.mu.holders[string(rows[0])]
					return ops != nil && proxyReentrantWaiterCount(ops) == 1
				}, time.Second, time.Millisecond)
				proxy.mu.RLock()
				ops := proxy.mu.holders[string(rows[0])]
				require.Empty(t, ops.txns)
				require.Len(t, ops.pending, 1)
				require.Same(t, txn, ops.pending[0].txn)
				require.Equal(t, 1, proxyReentrantWaiterCount(ops))
				proxy.mu.RUnlock()
				require.Equal(t, int32(1), remote.calls.Load())

				if cancelReentry {
					cancel()
					select {
					case err := <-reentryDone:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(time.Second):
						t.Fatal("same-transaction re-entry ignored cancellation")
					}
					proxy.mu.RLock()
					ops = proxy.mu.holders[string(rows[0])]
					require.Empty(t, ops.txns)
					require.Len(t, ops.pending, 1)
					require.Same(t, txn, ops.pending[0].txn)
					require.Zero(t, proxyReentrantWaiterCount(ops))
					proxy.mu.RUnlock()
				}

				close(remote.release)
				select {
				case err := <-firstDone:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("first remote lock did not finish")
				}
				if !cancelReentry {
					select {
					case err := <-reentryDone:
						require.NoError(t, err)
					case <-time.After(time.Second):
						t.Fatal("same-transaction re-entry did not join completion")
					}
				}
				require.Equal(t, int32(1), remote.calls.Load())
				closeProxyTestTxn(t, txn, proxy)
			})
		}
	})
}

func TestProxyOwnerReuseRetriesIndependentWaiterWithoutCaching(t *testing.T) {
	reuse.RunReuseTests(func() {
		bind := pb.LockTable{
			Group:     0,
			Table:     26777,
			ServiceID: "remote",
			Valid:     true,
		}
		remote := &blockingProxyLockTable{
			bind:          bind,
			started:       make(chan struct{}, 2),
			release:       make(chan struct{}),
			reuseExisting: true,
		}
		proxy := newLockTableProxy("local", "", remote, getLogger("")).(*localLockTableProxy)
		rows := [][]byte{[]byte("row")}
		options := LockOptions{LockOptions: newTestRowSharedOptions()}
		firstTxn := newActiveTxn([]byte("existing-owner"), "existing-owner", newFixedSlicePool(4), "")
		secondTxn := newActiveTxn([]byte("queued-sharer"), "queued-sharer", newFixedSlicePool(4), "")

		lockAsync := func(txn *activeTxn) <-chan error {
			done := make(chan error, 1)
			go func() {
				txn.Lock()
				defer txn.Unlock()
				proxy.lock(context.Background(), txn, rows, options,
					func(_ pb.Result, err error) { done <- err })
			}()
			return done
		}
		firstDone := lockAsync(firstTxn)
		select {
		case <-remote.started:
		case <-time.After(time.Second):
			t.Fatal("first owner request did not start")
		}
		secondDone := lockAsync(secondTxn)
		require.Eventually(t, func() bool {
			proxy.mu.RLock()
			defer proxy.mu.RUnlock()
			ops := proxy.mu.holders[string(rows[0])]
			return ops != nil && len(ops.pending) == 2
		}, time.Second, time.Millisecond)

		close(remote.release)
		for _, done := range []<-chan error{firstDone, secondDone} {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("uncacheable proxy generation did not complete")
			}
		}
		require.Equal(t, int32(2), remote.calls.Load(),
			"an independent waiter must retry under its own owner identity")
		proxy.mu.RLock()
		require.Empty(t, proxy.mu.holders)
		require.Empty(t, proxy.mu.currentHolder)
		proxy.mu.RUnlock()

		closeProxyTestTxn(t, firstTxn, proxy)
		closeProxyTestTxn(t, secondTxn, proxy)
	})
}

func TestProxyQueuedFollowerReentryWaitsForFollowerAdmission(t *testing.T) {
	reuse.RunReuseTests(func() {
		bind := pb.LockTable{
			Group:     0,
			Table:     26778,
			ServiceID: "remote",
			Valid:     true,
		}
		remote := &blockingProxyLockTable{
			bind:    bind,
			started: make(chan struct{}, 1),
			release: make(chan struct{}),
		}
		proxy := newLockTableProxy("local", "", remote, getLogger("")).(*localLockTableProxy)
		rows := [][]byte{[]byte("row")}
		options := LockOptions{LockOptions: newTestRowSharedOptions()}
		firstTxn := newActiveTxn(
			[]byte("queued-reentry-first"),
			"queued-reentry-first",
			newFixedSlicePool(4),
			"")
		followerTxn := newActiveTxn(
			[]byte("queued-reentry-follower"),
			"queued-reentry-follower",
			newFixedSlicePool(4),
			"")
		followerTxn.beforeLockAdded = func([]byte, [][]byte) error {
			return ErrTxnNotFound
		}

		lockAsync := func(txn *activeTxn) <-chan error {
			done := make(chan error, 1)
			go func() {
				txn.Lock()
				defer txn.Unlock()
				proxy.lock(
					context.Background(), txn, rows, options,
					func(_ pb.Result, err error) { done <- err })
			}()
			return done
		}

		firstDone := lockAsync(firstTxn)
		select {
		case <-remote.started:
		case <-time.After(time.Second):
			t.Fatal("first owner request did not start")
		}
		followerDone := lockAsync(followerTxn)
		require.Eventually(t, func() bool {
			proxy.mu.RLock()
			defer proxy.mu.RUnlock()
			ops := proxy.mu.holders[string(rows[0])]
			return ops != nil && len(ops.pending) == 2 &&
				ops.pending[1].waiter != nil
		}, time.Second, time.Millisecond)

		reentryDone := lockAsync(followerTxn)
		require.Eventually(t, func() bool {
			proxy.mu.RLock()
			defer proxy.mu.RUnlock()
			ops := proxy.mu.holders[string(rows[0])]
			return ops != nil && proxyReentrantWaiterCount(ops) == 1
		}, time.Second, time.Millisecond,
			"same-txn re-entry did not subscribe to follower admission")

		close(remote.release)
		select {
		case err := <-firstDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("first owner request did not finish")
		}
		for name, done := range map[string]<-chan error{
			"primary follower":  followerDone,
			"same-txn re-entry": reentryDone,
		} {
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrTxnNotFound, name)
			case <-time.After(time.Second):
				t.Fatalf("%s did not observe admission failure", name)
			}
		}
		require.Equal(t, int32(1), remote.calls.Load(),
			"a failed follower admission must not be reported as an owner success")

		proxy.mu.RLock()
		ops := proxy.mu.holders[string(rows[0])]
		require.NotNil(t, ops)
		require.Len(t, ops.txns, 1)
		require.Same(t, firstTxn, ops.txns[0])
		require.Empty(t, ops.pending)
		require.Zero(t, proxyReentrantWaiterCount(ops))
		proxy.mu.RUnlock()

		followerTxn.Lock()
		followerTxn.beforeLockAdded = nil
		followerTxn.Unlock()
		closeProxyTestTxn(t, firstTxn, proxy)
		closeProxyTestTxn(t, followerTxn, proxy)
	})
}

func TestProxyFollowerClosureCancelsBeforeOwnerCompletion(t *testing.T) {
	reuse.RunReuseTests(func() {
		bind := pb.LockTable{
			Group:     0,
			Table:     26781,
			ServiceID: "remote",
			Valid:     true,
		}
		remote := &blockingProxyLockTable{
			bind:    bind,
			started: make(chan struct{}, 1),
			release: make(chan struct{}),
		}
		proxy := newLockTableProxy("local", "", remote, getLogger("")).(*localLockTableProxy)
		rows := [][]byte{[]byte("row")}
		options := LockOptions{LockOptions: newTestRowSharedOptions()}
		firstTxn := newActiveTxn([]byte("first-holder"), "first-holder", newFixedSlicePool(4), "")
		followerTxn := newActiveTxn([]byte("closing-follower"), "closing-follower", newFixedSlicePool(4), "")
		defer reuse.Free(firstTxn, nil)
		defer reuse.Free(followerTxn, nil)

		firstDone := make(chan error, 1)
		go func() {
			firstTxn.Lock()
			var lockErr error
			proxy.lock(context.Background(), firstTxn, rows, options,
				func(_ pb.Result, err error) { lockErr = err })
			firstTxn.Unlock()
			firstDone <- lockErr
		}()
		select {
		case <-remote.started:
		case <-time.After(time.Second):
			t.Fatal("first owner request did not start")
		}

		followerDone := make(chan error, 1)
		go func() {
			followerTxn.Lock()
			defer followerTxn.Unlock()
			ctx, finish := followerTxn.beginLockOpLocked(context.Background())
			defer finish()
			proxy.lock(ctx, followerTxn, rows, options,
				func(_ pb.Result, err error) { followerDone <- err })
		}()
		require.Eventually(t, func() bool {
			proxy.mu.RLock()
			defer proxy.mu.RUnlock()
			ops := proxy.mu.holders[string(rows[0])]
			return ops != nil && len(ops.pending) == 2
		}, time.Second, time.Millisecond)

		closureDone := make(chan bool, 1)
		go func() {
			followerTxn.Lock()
			followerTxn.beginClosingLocked(getLogger(""))
			closedGeneration := followerTxn.waitAsyncLockOpsLocked(
				followerTxn.txnID, followerTxn.generation)
			followerTxn.Unlock()
			closureDone <- closedGeneration
		}()
		select {
		case err := <-followerDone:
			require.ErrorIs(t, err, ErrTxnNotFound)
		case <-time.After(time.Second):
			t.Fatal("follower retained its transaction mutex while waiting")
		}
		select {
		case closedGeneration := <-closureDone:
			require.True(t, closedGeneration)
		case <-time.After(time.Second):
			t.Fatal("transaction closure did not drain the proxy follower")
		}

		// The first physical RPC is deliberately still blocked: follower closure
		// must not depend on unrelated owner progress.
		close(remote.release)
		select {
		case err := <-firstDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("first owner request did not complete")
		}
	})
}

func TestExistingProxyBypassesCacheWhenProtocolDowngrades(t *testing.T) {
	const protocolService = "proxy-protocol-downgrade"
	moruntime.RunTest(protocolService, func(rt moruntime.Runtime) {
		value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, value)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion19)

		reuse.RunReuseTests(func() {
			bind := pb.LockTable{
				Group:     0,
				Table:     26770,
				ServiceID: "remote",
				Valid:     true,
			}
			remote := &blockingProxyLockTable{
				bind:    bind,
				started: make(chan struct{}, 2),
				release: make(chan struct{}),
			}
			close(remote.release)
			proxy := newLockTableProxy(
				"local",
				protocolService,
				remote,
				getLogger(""),
			).(*localLockTableProxy)
			rows := [][]byte{[]byte("row")}
			options := LockOptions{LockOptions: newTestRowSharedOptions()}

			for _, txnID := range [][]byte{[]byte("first"), []byte("second")} {
				txn := newActiveTxn(txnID, string(txnID), newFixedSlicePool(4), "")
				txn.Lock()
				var lockErr error
				proxy.lock(context.Background(), txn, rows, options,
					func(_ pb.Result, err error) { lockErr = err })
				require.NoError(t, lockErr)
				reuse.Free(txn, nil)
				txn.Unlock()
			}

			require.Equal(t, int32(2), remote.calls.Load(),
				"a downgraded proxy must send every new sharer to the owner")
			proxy.mu.RLock()
			require.Empty(t, proxy.mu.holders)
			proxy.mu.RUnlock()
		})
	})
}

func (t *blockingProxyLockTable) getBind() pb.LockTable {
	return t.bind
}

func (t *recordingUnlockTable) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	locks *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	t.mutations = append(t.mutations, mutations...)
	if unlocker, ok := t.lockTable.(contextUnlocker); ok {
		return unlocker.unlockWithContext(ctx, txn, locks, commitTS, mutations...)
	}
	t.lockTable.unlock(txn, locks, commitTS, mutations...)
	return nil
}

func (t *unlockAfterApplyErrorTable) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	locks *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	if unlocker, ok := t.lockTable.(contextUnlocker); ok {
		if err := unlocker.unlockWithContext(ctx, txn, locks, commitTS, mutations...); err != nil {
			return err
		}
	} else {
		t.lockTable.unlock(txn, locks, commitTS, mutations...)
	}
	return t.err
}

func (t *unlockBeforeApplyErrorTable) unlockWithContext(
	_ context.Context,
	_ *activeTxn,
	_ *cowSlice,
	_ timestamp.Timestamp,
	_ ...pb.ExtraMutation,
) error {
	return t.err
}

func TestProxySharedLockCancellationWhileFirstRemoteLockInFlight(t *testing.T) {
	reuse.RunReuseTests(func() {
		for _, remoteErr := range []error{nil, errors.New("remote lock failed")} {
			name := "remote-success"
			if remoteErr != nil {
				name = "remote-failure"
			}
			t.Run(name, func(t *testing.T) {
				bind := pb.LockTable{
					Group:     0,
					Table:     1,
					ServiceID: "remote",
					Valid:     true,
				}
				remote := &blockingProxyLockTable{
					bind:    bind,
					started: make(chan struct{}, 1),
					release: make(chan struct{}),
					err:     remoteErr,
				}
				proxy := newLockTableProxy("local", "", remote, getLogger("")).(*localLockTableProxy)
				rows := [][]byte{[]byte("row")}
				options := LockOptions{LockOptions: newTestRowSharedOptions()}
				firstTxn := newActiveTxn([]byte("first"), "first", newFixedSlicePool(4), "")
				secondTxn := newActiveTxn([]byte("second"), "second", newFixedSlicePool(4), "")

				firstDone := make(chan error, 1)
				go func() {
					firstTxn.Lock()
					defer firstTxn.Unlock()
					proxy.lock(context.Background(), firstTxn, rows, options, func(_ pb.Result, err error) {
						firstDone <- err
					})
				}()
				select {
				case <-remote.started:
				case <-time.After(time.Second):
					t.Fatal("first remote lock did not start")
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var secondCallbacks atomic.Int32
				var secondLockAdded atomic.Int32
				secondTxn.beforeLockAdded = func([]byte, [][]byte) error {
					secondLockAdded.Add(1)
					return nil
				}
				secondDone := make(chan error, 2)
				go func() {
					secondTxn.Lock()
					defer secondTxn.Unlock()
					proxy.lock(ctx, secondTxn, rows, options, func(_ pb.Result, err error) {
						secondCallbacks.Add(1)
						secondDone <- err
					})
				}()

				require.Eventually(t, func() bool {
					proxy.mu.Lock()
					defer proxy.mu.Unlock()
					ops := proxy.mu.holders[string(rows[0])]
					return ops != nil && len(ops.pending) == 2 &&
						ops.pending[1].waiter != nil
				}, time.Second, time.Millisecond, "second shared lock was not admitted")

				cancel()
				select {
				case err := <-secondDone:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("second shared lock ignored cancellation")
				}
				require.Equal(t, int32(1), secondCallbacks.Load())
				require.Zero(t, secondLockAdded.Load())
				proxy.mu.Lock()
				ops := proxy.mu.holders[string(rows[0])]
				require.Empty(t, ops.txns)
				require.Len(t, ops.pending, 1)
				require.Same(t, firstTxn, ops.pending[0].txn)
				proxy.mu.Unlock()

				close(remote.release)
				select {
				case err := <-firstDone:
					if remoteErr == nil {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, remoteErr)
					}
				case <-time.After(time.Second):
					t.Fatal("first remote lock did not finish")
				}
				require.Equal(t, int32(1), secondCallbacks.Load())
				require.Zero(t, secondLockAdded.Load())
				select {
				case err := <-secondDone:
					t.Fatalf("second callback ran more than once: %v", err)
				default:
				}
				if remoteErr != nil {
					proxy.mu.RLock()
					_, retained := proxy.mu.holders[string(rows[0])]
					proxy.mu.RUnlock()
					require.False(t, retained,
						"a failed first generation must not retain an empty row entry")
				}
				closeProxyTestTxn(t, firstTxn, proxy)
				closeProxyTestTxn(t, secondTxn, proxy)
			})
		}
	})
}

func TestProxySharedLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(4)
			// txn5 := newTestTxnID(5)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			v = s2.tableGroups.get(0, tableID)
			ltp := v.(*localLockTableProxy)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 1, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[0].txnID, txn2)
			require.Empty(t, ltp.mu.holders[string(rows[0])].pending)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 2, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[1].txnID, txn3)
			require.Empty(t, ltp.mu.holders[string(rows[0])].pending)

			_, err = s2.Lock(ctx, tableID, rows, txn4, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 3, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[2].txnID, txn4)
			require.Empty(t, ltp.mu.holders[string(rows[0])].pending)

			// require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{txn4}, nil, nil)
			// require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn4)

			// require.NoError(t, s2.Unlock(ctx, txn4, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{txn3}, nil, nil)
			// require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn3)

			// require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{}, nil, nil)
			// require.Empty(t, ltp.mu.currentHolder)

			// _, err = s1.Lock(ctx, tableID, rows, txn5, newTestRowExclusiveOptions())
			// require.NoError(t, err, err)
			// require.NoError(t, s1.Unlock(ctx, txn5, timestamp.Timestamp{}))
		},
	)
}

func TestProxySharedUnlock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(3)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))

			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{}, nil, nil)
			ltp := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			ltp.mu.RLock()
			_, retained := ltp.mu.holders[string(rows[0])]
			_, current := ltp.mu.currentHolder[string(rows[0])]
			ltp.mu.RUnlock()
			require.False(t, retained,
				"the last acknowledged unlock must reclaim the empty row entry")
			require.False(t, current)

			_, err = s1.Lock(ctx, tableID, rows, txn4, newTestRowExclusiveOptions())
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn4, timestamp.Timestamp{}))
		},
	)
}

func TestProxyUnlockCleansBookkeepingAfterContextExpiry(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			timedOutTxn := []byte("timed-out")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the table owner, then consolidate two shared holders on
			// s2 behind one remote holder.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, timedOutTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			timedOut := s2.activeTxnHolder.getActiveTxn(timedOutTxn, false, "")
			survivor := s2.activeTxnHolder.getActiveTxn(survivorTxn, false, "")
			require.NotNil(t, timedOut)
			require.NotNil(t, survivor)
			s2.unknownCommitResolver.mu.Lock()
			s2.unknownCommitResolver.mu.pending[string(timedOutTxn)] = unknownCommitTxn{
				id: timedOutTxn,
			}
			s2.unknownCommitResolver.mu.Unlock()

			// Expire the resolver context while the unknown-commit resolver is
			// waiting on the source txn mutex. It must leave both proxy and owner
			// on the old holder so a later orphan cleanup cannot release a lock
			// that the proxy still treats as held by survivor.
			timedOut.Lock()
			unlockCtx, unlockCancel := context.WithCancel(context.Background())
			defer unlockCancel()
			unlocked := make(chan error, 1)
			go func() {
				unlocked <- s2.unlockUnknownCommit(
					unlockCtx,
					timedOutTxn,
					timestamp.Timestamp{},
				)
			}()
			require.Eventually(t, func() bool {
				return s2.activeTxnHolder.hasActiveTxn(timedOutTxn)
			}, time.Second, time.Millisecond)
			unlockCancel()
			timedOut.Unlock()

			require.ErrorIs(t, <-unlocked, context.Canceled)

			// The failed handoff cannot publish survivor as the remote holder.
			// Keeping the old source txn active makes CheckActiveTxn report it as
			// live even after the frontend transaction is gone.
			proxy.mu.RLock()
			shared := proxy.mu.holders[string(row)]
			require.NotNil(t, shared)
			require.Len(t, shared.txns, 2)
			require.Same(t, timedOut, shared.txns[0])
			require.Same(t, survivor, shared.txns[1])
			require.Equal(t, timedOutTxn, proxy.mu.currentHolder[string(row)])
			proxy.mu.RUnlock()

			holder, ok, err := s1.tableGroups.get(0, tableID).getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, timedOutTxn, holder.TxnID)

			// Exercise the owner-side orphan path. The unknown-commit resolver
			// still owns the source holder, so the owner must not use
			// CannotCommit to release it before ReplaceTo has been acknowledged.
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			s1.activeTxnHolder.keepRemoteLockBindActive(s2.serviceID, owner.bind)
			s1.events.checkOrphan(checkOrphan{
				wait: waitTooLong,
				key:  row,
				lt:   owner,
				txn:  pb.WaitTxn{TxnID: []byte("owner-waiter"), CreatedOn: s1.serviceID},
			})

			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, timedOutTxn, holder.TxnID)
		},
	)
}

func TestProxyUnlockRetryIgnoresNewOwnerAfterLostLastHolderResponse(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			timedOutTxn := []byte("timed-out")
			newOwnerTxn := []byte("new-owner")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the table owner, then acquire one shared proxy holder on s2.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, timedOutTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock response lost"),
			}

			// The owner executes the first Unlock, but the source does not receive
			// the response and therefore retains the old proxy holder for retry.
			err = s2.unlockUnknownCommit(ctx, timedOutTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")

			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
			require.Empty(t, holder.TxnID)

			// A different owner-local transaction may acquire the row before the
			// source retries its already-applied Unlock.
			_, err = s1.Lock(ctx, tableID, [][]byte{row}, newOwnerTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)

			proxy.remote = remote
			require.NoError(t, s2.unlockUnknownCommit(ctx, timedOutTxn, timestamp.Timestamp{}))

			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, newOwnerTxn, holder.TxnID)
			require.NoError(t, s1.Unlock(ctx, newOwnerTxn, timestamp.Timestamp{}))
		},
	)
}

func TestProxyLostLastHolderResponseRoutesLateSharerThroughOwner(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			exclusiveTxn := []byte("exclusive")
			lateSharerTxn := []byte("late-sharer")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the owner and leave one proxied shared holder on s2.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock response lost"),
			}

			// The owner removes the last shared holder, but s2 cannot observe the
			// acknowledgement and retains it for an idempotent retry.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")
			_, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
			proxy.mu.RLock()
			_, pending := proxy.mu.pendingLastHolderUnlocks[string(row)]
			proxy.mu.RUnlock()
			require.True(t, pending)

			// An owner-local exclusive can acquire before the proxy retries.
			_, err = s1.Lock(ctx, tableID, [][]byte{row}, exclusiveTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)

			// A late proxy sharer must reach the owner and wait behind that
			// exclusive instead of using the stale local shared holder.
			proxy.remote = remote
			lateSharerDone := make(chan error, 1)
			go func() {
				_, err := s2.Lock(ctx, tableID, [][]byte{row}, lateSharerTxn, newTestRowSharedOptions())
				lateSharerDone <- err
			}()
			waitWaiters(t, s1, tableID, row, 1)

			// Converging the stale last-holder removal must preserve both the
			// exclusive holder and its queued late sharer.
			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, exclusiveTxn, holder.TxnID)
			proxy.mu.RLock()
			_, pending = proxy.mu.pendingLastHolderUnlocks[string(row)]
			proxy.mu.RUnlock()
			require.False(t, pending)

			require.NoError(t, s1.Unlock(ctx, exclusiveTxn, timestamp.Timestamp{}))
			select {
			case err := <-lateSharerDone:
				require.NoError(t, err)
			case <-ctx.Done():
				require.NoError(t, ctx.Err())
			}
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, lateSharerTxn, holder.TxnID)
			require.NoError(t, s2.Unlock(ctx, lateSharerTxn, timestamp.Timestamp{}))
		},
	)
}

func TestProxySurvivorUnlockReleasesLostHandoffHolder(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock response lost"),
			}

			// The owner applies unknown -> survivor, but s2 loses the response
			// and therefore still records unknown as its current remote holder.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, survivorTxn, holder.TxnID)
			proxy.mu.RLock()
			require.Equal(t, unknownTxn, proxy.mu.currentHolder[string(row)])
			require.Equal(t, survivorTxn, proxy.mu.pendingRemoteHolders[string(row)])
			proxy.mu.RUnlock()
			require.True(t, s1.activeTxnHolder.hasActiveTxn(survivorTxn))

			recordingRemote := &recordingUnlockTable{lockTable: remote}
			proxy.remote = recordingRemote
			// A normal survivor Unlock must not be skipped merely because the
			// proxy has not acknowledged the previous handoff. It conditionally
			// transfers the owner back to unknown, allowing the resolver retry to
			// release it instead of leaving survivor at the remote owner.
			require.NoError(t, s2.Unlock(ctx, survivorTxn, timestamp.Timestamp{}))
			require.Len(t, recordingRemote.mutations, 1)
			require.Equal(t, row, recordingRemote.mutations[0].Key)
			require.Equal(t, unknownTxn, recordingRemote.mutations[0].ReplaceTo)
			require.False(t, s1.activeTxnHolder.hasActiveTxn(survivorTxn))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			_, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
		},
	)
}

func TestProxySurvivorUnlockHandlesUnappliedHandoff(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockBeforeApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock request lost"),
			}

			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock request lost")
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			proxy.remote = remote
			require.NoError(t, s2.Unlock(ctx, survivorTxn, timestamp.Timestamp{}))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			_, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
		},
	)
}

func TestProxyRetryPreservesAppliedHandoffRepresentative(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			firstReplacementTxn := []byte("first-replacement")
			lateSharerTxn := []byte("late-sharer")
			exclusiveTxn := []byte("exclusive")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, firstReplacementTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("handoff response lost"),
			}

			// The owner has already moved unknown -> firstReplacement, but the
			// proxy still records unknown and must retry that same representative.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "handoff response lost")

			// A later local sharer must not overwrite the representative selected
			// by the unacknowledged handoff.
			proxy.remote = remote
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, lateSharerTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))

			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, firstReplacementTxn, holder.TxnID)
			proxy.mu.RLock()
			currentHolder := append([]byte(nil), proxy.mu.currentHolder[string(row)]...)
			pendingHolders := len(proxy.mu.pendingRemoteHolders)
			proxy.mu.RUnlock()
			require.Equal(t, firstReplacementTxn, currentHolder)
			require.Zero(t, pendingHolders)

			// Let the first replacement finish. The owner must now move to the
			// still-active late sharer, rather than retaining an orphan holder.
			require.NoError(t, s2.Unlock(ctx, firstReplacementTxn, timestamp.Timestamp{}))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, lateSharerTxn, holder.TxnID)

			// In the real service, an ordinary live txn is reported by the
			// frontend iterator. Keep the late sharer visible while exercising the
			// owner orphan check for the finished first replacement.
			s2.cfg.TxnIterFunc = newTestTxnIterFunc(lateSharerTxn)
			s1.activeTxnHolder.keepRemoteLockBindActive(s2.serviceID, owner.bind)
			s1.events.checkOrphan(checkOrphan{
				wait: waitTooLong,
				key:  row,
				lt:   owner,
				txn:  pb.WaitTxn{TxnID: []byte("owner-waiter"), CreatedOn: s1.serviceID},
			})

			exclusiveDone := make(chan error, 1)
			// Waiter notification is asynchronous. Give this phase its own budget
			// so setup time or temporary runner starvation cannot consume it.
			exclusiveCtx, exclusiveCancel := context.WithTimeout(context.Background(), 30*time.Second)
			exclusiveExited := make(chan struct{})
			defer func() {
				exclusiveCancel()
				select {
				case <-exclusiveExited:
				case <-time.After(5 * time.Second):
					dumpProxyHandoffWait(t, owner, proxy, row)
					t.Error("exclusive lock worker did not exit after cancellation")
				}
			}()
			go func() {
				defer close(exclusiveExited)
				_, err := s1.Lock(exclusiveCtx, tableID, [][]byte{row}, exclusiveTxn, newTestRowExclusiveOptions())
				exclusiveDone <- err
			}()
			waitWaiters(t, s1, tableID, row, 1)
			require.Never(t, func() bool {
				select {
				case err := <-exclusiveDone:
					require.NoError(t, err)
					return true
				default:
					return false
				}
			}, 100*time.Millisecond, time.Millisecond)

			// These are separate RPC/cleanup phases, not extensions of the
			// exclusive acquisition budget. Do not reuse setup's aging context.
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s2.Unlock(releaseCtx, lateSharerTxn, timestamp.Timestamp{})
			releaseCancel()
			require.NoError(t, err)
			err = waitProxyLockResult(exclusiveCtx, exclusiveDone)
			if err != nil {
				dumpProxyHandoffWait(t, owner, proxy, row)
			}
			require.NoError(t, err)
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s1.Unlock(cleanupCtx, exclusiveTxn, timestamp.Timestamp{})
			cleanupCancel()
			require.NoError(t, err)
		},
	)
}

// A completed result is authoritative even if the test goroutine resumes after
// its hang guard expired. The deadline branch only rechecks; it never waits for
// additional work or suppresses an actual Lock error.
func waitProxyLockResult(ctx context.Context, done <-chan error) error {
	var err error
	var ok bool
	select {
	case err, ok = <-done:
	case <-ctx.Done():
		select {
		case err, ok = <-done:
		default:
			return ctx.Err()
		}
	}
	if !ok {
		return errors.New("lock result channel closed without a result")
	}
	return err
}

func TestWaitProxyLockResult(t *testing.T) {
	lockErr := errors.New("lock failed")
	for _, tc := range []struct {
		name    string
		expired bool
		send    bool
		closed  bool
		result  error
		want    error
	}{
		{name: "success", send: true},
		{name: "lock error", send: true, result: lockErr, want: lockErr},
		{name: "success and deadline ready", expired: true, send: true},
		{name: "lock error and deadline ready", expired: true, send: true, result: lockErr, want: lockErr},
		{name: "deadline without result", expired: true, want: context.DeadlineExceeded},
		{name: "closed without result", closed: true},
		{name: "closed without result and deadline ready", expired: true, closed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.expired {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Time{})
				defer cancel()
			}
			done := make(chan error, 1)
			if tc.send {
				done <- tc.result
			}
			if tc.closed {
				close(done)
			}
			err := waitProxyLockResult(ctx, done)
			if tc.closed {
				require.EqualError(t, err, "lock result channel closed without a result")
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

// Failure-only diagnostics must not wait on the mutex that may explain the
// hang. Snapshots are per-lock, not an atomic view of owner and proxy together.
func dumpProxyHandoffWait(t *testing.T, owner *localLockTable, proxy *localLockTableProxy, row []byte) {
	t.Helper()
	if owner.mu.TryRLock() {
		lock, found := owner.mu.store.Get(row)
		var state string
		if found {
			state = fmt.Sprintf("holders=%v", lock.holders.txns)
			lock.waiters.iter(func(w *waiter) bool {
				state += fmt.Sprintf(" waiter=%x status=%d notifications=%d", w.txn.TxnID, w.getStatus(), len(w.c))
				return true
			})
		}
		owner.mu.RUnlock()
		t.Logf("handoff owner: found=%t %s", found, state)
	} else {
		t.Log("handoff owner mutex unavailable")
	}
	if proxy.mu.TryRLock() {
		state := fmt.Sprintf("holder=%x pending=%v", proxy.mu.currentHolder[string(row)], proxy.mu.pendingRemoteHolders)
		proxy.mu.RUnlock()
		t.Logf("handoff proxy: %s", state)
	} else {
		t.Log("handoff proxy mutex unavailable")
	}
	t.Logf("handoff event queue: %d", len(owner.events.eventC))
	stack := make([]byte, 2<<20)
	n := runtime.Stack(stack, true)
	t.Logf("handoff goroutines (truncated=%t):\n%s", n == len(stack), stack[:n])
}

func TestProxyAmbiguousDirectLockForcesTxnIDUnlock(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26767)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)

			client := &loseNthLockResponseClient{Client: origin.remote.client}
			client.dropAt.Store(2)
			origin.remote.client = client

			representative := []byte("proxy-direct-representative")
			txnID := []byte("proxy-direct-ambiguous")
			_, err = origin.Lock(
				ctx, table, newTestRows(1), representative,
				newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = origin.Lock(
				ctx, table, newTestRows(1), txnID,
				newTestRowSharedOptions())
			require.NoError(t, err)

			// Exclusive requests bypass the singleton Shared proxy. The owner
			// commits row 3 for txnID, then the wrapper drops that response.
			_, err = origin.Lock(
				ctx, table, newTestRows(3), txnID,
				newTestRowExclusiveOptions())
			require.Error(t, err)

			proxy := origin.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			require.False(t, proxy.isRemoteHolderLocked(string(newTestRows(1)[0]), txnID),
				"txnID must remain only a local proxy sharer")
			proxy.mu.RUnlock()

			// Its only retained probe key belongs to the local proxy cache and
			// would normally make unlock skip the owner RPC. Direct-remote
			// ownership forces the table-level txnID unlock instead.
			require.NoError(t, origin.Unlock(ctx, txnID, timestamp.Timestamp{}))
			probeTxn := []byte("proxy-direct-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, newTestRows(3), probeTxn, probe)
			require.NoError(t, err, "ambiguous direct ownership leaked behind the proxy")
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))
			require.NoError(t, origin.Unlock(ctx, representative, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 2
			c.MaxFixedSliceSize = 4
		},
	)
}

func TestProxyDirectSharedOverlapDoesNotSkipOwnerHolder(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26772)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)
			origin.cfg.EnableRemoteLocalProxy = true

			rows := newTestRows(1, 2)
			representative := []byte("proxy-overlap-representative")
			txnID := []byte("proxy-overlap-direct-holder")
			shared := newTestRowSharedOptions()
			_, err = origin.Lock(ctx, table, rows[:1], representative, shared)
			require.NoError(t, err)
			_, err = origin.Lock(ctx, table, rows[:1], txnID, shared)
			require.NoError(t, err)

			// A multi-row Shared request bypasses the singleton proxy. The owner now
			// records txnID as a real holder of row 1 as well as row 2, even though
			// the proxy still regards it as a local non-representative for row 1.
			_, err = origin.Lock(ctx, table, rows, txnID, shared)
			require.NoError(t, err)
			require.NoError(t, origin.Unlock(ctx, txnID, timestamp.Timestamp{}))
			require.NoError(t, origin.Unlock(ctx, representative, timestamp.Timestamp{}))

			probeTxn := []byte("proxy-overlap-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, rows, probeTxn, probe)
			require.NoError(t, err,
				"direct holder on the proxy-overlap key leaked at the owner")
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestDirectSharedOverlapThenProxyCleansLocalHolder(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26776)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)
			origin.cfg.EnableRemoteLocalProxy = true

			txnID := []byte("direct-then-proxy-overlap")
			// The multi-row request bypasses the singleton proxy. Put the later
			// proxy key second so a compact direct route cannot cover it by chance.
			_, err = origin.Lock(
				ctx, table, newTestRows(3, 1), txnID,
				newTestRowSharedOptions())
			require.NoError(t, err)
			// The owner already has row 1 for this transaction and therefore reports
			// NewLockAdd=false. The proxy cannot infer the effective owner mode from
			// that re-entry and must leave it on the direct cleanup route.
			_, err = origin.Lock(
				ctx, table, newTestRows(1), txnID,
				newTestRowSharedOptions())
			require.NoError(t, err)
			proxy := origin.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			_, holderPublished := proxy.mu.holders[string(newTestRows(1)[0])]
			_, currentPublished := proxy.mu.currentHolder[string(newTestRows(1)[0])]
			proxy.mu.RUnlock()
			require.False(t, holderPublished,
				"reused owner-side ownership must not seed the Shared proxy cache")
			require.False(t, currentPublished)

			require.NoError(t, origin.Unlock(ctx, txnID, timestamp.Timestamp{}))
			proxy.mu.RLock()
			_, holderRetained := proxy.mu.holders[string(newTestRows(1)[0])]
			_, currentRetained := proxy.mu.currentHolder[string(newTestRows(1)[0])]
			proxy.mu.RUnlock()
			require.False(t, holderRetained,
				"transaction close must remove the local proxy holder")
			require.False(t, currentRetained,
				"transaction close must remove the proxy's remote representative")

			probeTxn := []byte("direct-then-proxy-exclusive-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, newTestRows(1), probeTxn, probe)
			require.NoError(t, err,
				"table-scoped unlock must release the overlapping owner lock")
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestDirectExclusiveThenProxySharedDoesNotBypassOwnerMode(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26778)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)
			origin.cfg.EnableRemoteLocalProxy = true

			holderTxn := []byte("direct-exclusive-owner")
			row := newTestRows(1)
			_, err = origin.Lock(
				ctx, table, row, holderTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			_, err = origin.Lock(
				ctx, table, row, holderTxn, newTestRowSharedOptions())
			require.NoError(t, err,
				"same-transaction weaker re-entry remains successful")

			proxy := origin.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			require.Empty(t, proxy.mu.holders,
				"an effective Exclusive owner cannot become a Shared proxy representative")
			require.Empty(t, proxy.mu.currentHolder)
			proxy.mu.RUnlock()

			blockedTxn := []byte("shared-probe-behind-exclusive")
			shared := newTestRowSharedOptions()
			shared.Policy = pb.WaitPolicy_FastFail
			_, err = origin.Lock(ctx, table, row, blockedTxn, shared)
			require.Error(t, err,
				"another Shared transaction must still consult the Exclusive owner")
			require.NoError(t, origin.Unlock(ctx, blockedTxn, timestamp.Timestamp{}))
			require.NoError(t, origin.Unlock(ctx, holderTxn, timestamp.Timestamp{}))

			probeTxn := []byte("exclusive-cleanup-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, row, probeTxn, probe)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestProxyHandoffPreservesEveryTableOnSameOwner(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"proxy-owner", "proxy-origin"},
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			tables := []uint64{26781, 26782}
			rows := [][]byte{[]byte("first-table-row"), []byte("second-table-row")}
			for idx, table := range tables {
				seed := []byte(fmt.Sprintf("proxy-seed-%d", idx))
				_, err := owner.Lock(ctx, table, [][]byte{rows[idx]}, seed, newTestRowSharedOptions())
				require.NoError(t, err)
				require.NoError(t, owner.Unlock(ctx, seed, timestamp.Timestamp{}))
			}

			origin.cfg.EnableRemoteLocalProxy = true
			sourceTxn := []byte("multi-table-proxy-source")
			replacementTxn := []byte("multi-table-proxy-replacement")
			for idx, table := range tables {
				_, err := origin.Lock(ctx, table, [][]byte{rows[idx]}, sourceTxn, newTestRowSharedOptions())
				require.NoError(t, err)
				_, err = origin.Lock(ctx, table, [][]byte{rows[idx]}, replacementTxn, newTestRowSharedOptions())
				require.NoError(t, err)
			}

			require.NoError(t, origin.Unlock(ctx, sourceTxn, timestamp.Timestamp{}))
			for idx, table := range tables {
				lt := owner.tableGroups.get(0, table).(*localLockTable)
				holder, ok, err := lt.getLockHolder(ctx, rows[idx])
				require.NoError(t, err)
				require.True(t, ok,
					"each proxy table needs its own owner-side handoff")
				require.Equal(t, replacementTxn, holder.TxnID)
			}
			owner.mu.RLock()
			for _, table := range tables {
				require.Equal(t, uint64(1), owner.mu.lockTableRef[0][table],
					"handoff must transfer the drain reference with physical ownership")
			}
			owner.mu.RUnlock()
			owner.checkCanMoveGroupTables()
			require.Nil(t, owner.topGroupTables(),
				"a table with a handed-off physical holder is not movable")

			require.NoError(t, origin.Unlock(ctx, replacementTxn, timestamp.Timestamp{}))
			owner.mu.RLock()
			for _, table := range tables {
				_, retained := owner.mu.lockTableRef[0][table]
				require.False(t, retained)
			}
			owner.mu.RUnlock()
			require.Equal(t, pb.Status_ServiceUnLockSucc, owner.getStatus())
		},
	)
}

func TestRemoteTableByTableUnlockFencesLateLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"table-unlock-owner", "table-unlock-origin"},
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			tables := []uint64{26783, 26784}
			rows := [][]byte{[]byte("first-owned-row"), []byte("second-owned-row")}
			for idx, table := range tables {
				seed := []byte(fmt.Sprintf("table-unlock-seed-%d", idx))
				_, err := owner.Lock(ctx, table, [][]byte{rows[idx]}, seed, newTestRowExclusiveOptions())
				require.NoError(t, err)
				require.NoError(t, owner.Unlock(ctx, seed, timestamp.Timestamp{}))
			}

			txnID := []byte("table-by-table-closing-txn")
			for idx, table := range tables {
				_, err := origin.Lock(ctx, table, [][]byte{rows[idx]}, txnID, newTestRowExclusiveOptions())
				require.NoError(t, err)
			}
			owner.mu.RLock()
			_, firstRef := owner.mu.lockTableRef[0][tables[0]]
			_, secondRef := owner.mu.lockTableRef[0][tables[1]]
			owner.mu.RUnlock()
			require.True(t, firstRef)
			require.True(t, secondRef)

			firstBind := owner.tableGroups.get(0, tables[0]).getBind()
			wrongBind := firstBind
			wrongBind.Version++
			err := owner.unlockRemoteLockTable(
				ctx, wrongBind, txnID, timestamp.Timestamp{})
			require.Error(t, err)
			ownerTxn := owner.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, ownerTxn)
			ownerTxn.RLock()
			require.False(t, ownerTxn.closing.Load(),
				"a mismatched generation must not fence or mutate the live transaction")
			ownerTxn.RUnlock()
			firstTable := owner.tableGroups.get(0, tables[0]).(*localLockTable)
			holder, ok, err := firstTable.getLockHolder(ctx, rows[0])
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, txnID, holder.TxnID)

			require.NoError(t, owner.unlockRemoteLockTable(
				ctx, firstBind, txnID, timestamp.Timestamp{}))
			ownerTxn = owner.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, ownerTxn,
				"the owner transaction must remain until its last table closes")
			ownerTxn.RLock()
			require.True(t, ownerTxn.closing.Load())
			ownerTxn.RUnlock()
			owner.mu.RLock()
			_, firstRef = owner.mu.lockTableRef[0][tables[0]]
			_, secondRef = owner.mu.lockTableRef[0][tables[1]]
			owner.mu.RUnlock()
			require.True(t, firstRef,
				"partial close must retain every drain reference until finalization")
			require.True(t, secondRef)

			lateRow := []byte("late-row")
			_, err = origin.Lock(
				ctx, tables[0], [][]byte{lateRow}, txnID, newTestRowExclusiveOptions())
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState),
				"a delayed Lock RPC cannot republish ownership during close")

			secondBind := owner.tableGroups.get(0, tables[1]).getBind()
			require.NoError(t, owner.unlockRemoteLockTable(
				ctx, secondBind, txnID, timestamp.Timestamp{}))
			require.Nil(t, owner.activeTxnHolder.getActiveTxn(txnID, false, ""))
			owner.mu.RLock()
			_, firstRef = owner.mu.lockTableRef[0][tables[0]]
			_, secondRef = owner.mu.lockTableRef[0][tables[1]]
			owner.mu.RUnlock()
			require.False(t, firstRef)
			require.False(t, secondRef)
			require.NoError(t, origin.Unlock(ctx, txnID, timestamp.Timestamp{}))

			probe := []byte("late-row-probe")
			probeOptions := newTestRowExclusiveOptions()
			probeOptions.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, tables[0], [][]byte{lateRow}, probe, probeOptions)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, probe, timestamp.Timestamp{}))
		},
	)
}

func TestOriginUnlockCancelsIntentOnlyPendingRemoteLockBeforeTxnRecycle(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"intent-owner", "intent-origin"},
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			const table = uint64(26779)
			row := []byte("blocked-intent-row")
			blockerTxn := []byte("intent-blocker")
			_, err := owner.Lock(
				ctx, table, [][]byte{row}, blockerTxn,
				newTestRowExclusiveOptions())
			require.NoError(t, err)

			pendingTxn := []byte("intent-only-remote-txn")
			lockDone := make(chan error, 1)
			go func() {
				_, lockErr := origin.Lock(
					ctx, table, [][]byte{row}, pendingTxn,
					newTestRowExclusiveOptions())
				lockDone <- lockErr
			}()
			waitWaiters(t, owner, table, row, 1)

			unlockDone := make(chan error, 1)
			go func() {
				unlockDone <- origin.Unlock(
					ctx, pendingTxn, timestamp.Timestamp{})
			}()
			select {
			case err = <-unlockDone:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("intent-only remote unlock did not finish")
			}

			lockFinished := false
			var lockErr error
			select {
			case lockErr = <-lockDone:
				lockFinished = true
			case <-time.After(500 * time.Millisecond):
			}
			// Always release the blocker before asserting so the pre-fix failure
			// cannot strand the test goroutine or lockservice teardown.
			require.NoError(t, owner.Unlock(ctx, blockerTxn, timestamp.Timestamp{}))
			if !lockFinished {
				lockErr = <-lockDone
			}
			require.True(t, lockFinished,
				"owner cleanup must cancel and drain the pending callback before recycling its transaction")
			require.Error(t, lockErr)
			require.Nil(t, origin.activeTxnHolder.getActiveTxn(pendingTxn, false, ""))
			require.Nil(t, owner.activeTxnHolder.getActiveTxn(pendingTxn, false, ""))
		},
	)
}

func TestRemoteTableUnlockNeverTouchesReboundGeneration(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"rebound-unlock-owner", "rebound-unlock-origin"},
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			const table = uint64(26785)
			oldRow := []byte("old-generation-row")
			seedTxn := []byte("rebound-seed")
			_, err := owner.Lock(ctx, table, [][]byte{oldRow}, seedTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			remoteTxnID := []byte("old-generation-remote-txn")
			_, err = origin.Lock(ctx, table, [][]byte{oldRow}, remoteTxnID, newTestRowExclusiveOptions())
			require.NoError(t, err)
			oldBind := owner.tableGroups.get(0, table).getBind()
			newBind := oldBind
			newBind.Version++
			owner.handleBindChanged(newBind)

			newRow := []byte("new-generation-row")
			newTxnID := []byte("new-generation-local-txn")
			_, err = owner.Lock(ctx, table, [][]byte{newRow}, newTxnID, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, owner.unlockRemoteLockTable(
				ctx,
				oldBind,
				remoteTxnID,
				timestamp.Timestamp{},
			))

			current := owner.tableGroups.get(0, table).(*localLockTable)
			holder, ok, err := current.getLockHolder(ctx, newRow)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, newTxnID, holder.TxnID,
				"an old-generation unlock must never mutate the rebound table")

			require.NoError(t, origin.Unlock(ctx, remoteTxnID, timestamp.Timestamp{}))
			require.NoError(t, owner.Unlock(ctx, newTxnID, timestamp.Timestamp{}))
		},
	)
}

func TestProxyOwnerSnapshotCapacitySkewFallsBackToUncached(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26789)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)

			// Model two independently valid rolling configurations. Each singleton
			// Shared request is proxy-eligible, but the origin cannot mirror all five
			// proxy memberships in its four-entry fixed slice. Owner-local snapshots
			// and table-scoped unlock make a compact route sufficient for any generation
			// that cannot be cached locally.
			origin.cfg.MaxLockRowCount = 4
			origin.cfg.MaxFixedSliceSize = 4
			origin.fsp = newFixedSlicePool(4)
			origin.activeTxnHolder.(*mapBasedTxnHolder).fsp = origin.fsp

			compatibleTxn := []byte("proxy-capacity-compatible-holder")
			_, err = owner.Lock(
				ctx, table, newTestRows(3), compatibleTxn,
				newTestRowSharedOptions())
			require.NoError(t, err)

			holderTxn := []byte("proxy-capacity-holder")
			rows := newTestRows(1, 2, 3, 4, 5)
			for _, row := range rows {
				_, err = origin.Lock(
					ctx, table, [][]byte{row}, holderTxn,
					newTestRowSharedOptions())
				require.NoError(t, err,
					"a negotiated owner snapshot must not turn proxy bookkeeping pressure into a lock failure")
			}

			ownerTxn := owner.activeTxnHolder.getActiveTxn(holderTxn, false, "")
			require.NotNil(t, ownerTxn)
			ownerTxn.RLock()
			ownerKeys := ownerTxn.lockHolders[0].tableKeys[table].slice()
			ownerTxn.RUnlock()
			require.Equal(t, rows, ownerKeys.all())
			ownerKeys.unref()

			originTxn := origin.activeTxnHolder.getActiveTxn(holderTxn, false, "")
			require.NotNil(t, originTxn)
			originTxn.RLock()
			originKeys := originTxn.lockHolders[0].tableKeys[table].slice()
			originTxn.RUnlock()
			require.LessOrEqual(t, originKeys.len(), 4)
			originKeys.unref()

			require.NoError(t, origin.Unlock(ctx, holderTxn, timestamp.Timestamp{}))
			require.NoError(t, owner.Unlock(ctx, compatibleTxn, timestamp.Timestamp{}))

			probeTxn := []byte("proxy-capacity-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, rows, probeTxn, probe)
			require.NoError(t, err,
				"table-scoped unlock must release cached and uncached owner rows")
			require.NoError(t, owner.Unlock(ctx, probeTxn, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestProxyCachedFollowerCapacityFallsBackToOwner(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26791)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)

			origin.cfg.MaxLockRowCount = 4
			origin.cfg.MaxFixedSliceSize = 4
			origin.fsp = newFixedSlicePool(4)
			origin.activeTxnHolder.(*mapBasedTxnHolder).fsp = origin.fsp

			rows := newTestRows(1, 2, 3, 4, 5)
			representatives := make([][]byte, 0, len(rows))
			for idx, row := range rows {
				txnID := []byte(fmt.Sprintf("proxy-capacity-representative-%d", idx))
				representatives = append(representatives, txnID)
				_, err = origin.Lock(
					ctx, table, [][]byte{row}, txnID,
					newTestRowSharedOptions())
				require.NoError(t, err)
			}

			followerTxnID := []byte("proxy-capacity-cached-follower")
			for _, row := range rows {
				_, err = origin.Lock(
					ctx, table, [][]byte{row}, followerTxnID,
					newTestRowSharedOptions())
				require.NoError(t, err,
					"a full proxy ledger must fall back to a direct owner acquisition")
			}

			originTxn := origin.activeTxnHolder.getActiveTxn(
				followerTxnID, false, "")
			require.NotNil(t, originTxn)
			originTxn.RLock()
			originKeys := originTxn.lockHolders[0].tableKeys[table].slice()
			originTxn.RUnlock()
			require.Equal(t, rows[:4], originKeys.all())
			originKeys.unref()

			ownerTxn := owner.activeTxnHolder.getActiveTxn(
				followerTxnID, false, "")
			require.NotNil(t, ownerTxn,
				"the overflow key must be owned directly under the follower identity")
			ownerTxn.RLock()
			ownerKeys := ownerTxn.lockHolders[0].tableKeys[table].slice()
			ownerTxn.RUnlock()
			require.Equal(t, rows[4:], ownerKeys.all())
			ownerKeys.unref()

			proxy := origin.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			overflowGeneration := proxy.mu.holders[string(rows[4])]
			require.NotNil(t, overflowGeneration)
			_, overflowAdmitted := overflowGeneration.admissionState(originTxn)
			require.False(t, overflowAdmitted,
				"a direct fallback must not become a proxy handoff target")
			proxy.mu.RUnlock()

			require.NoError(t, origin.Unlock(
				ctx, followerTxnID, timestamp.Timestamp{}))
			for _, txnID := range representatives {
				require.NoError(t, origin.Unlock(
					ctx, txnID, timestamp.Timestamp{}))
			}

			probeTxnID := []byte("proxy-capacity-cached-follower-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, rows, probeTxnID, probe)
			require.NoError(t, err,
				"direct fallback and cached memberships must share one complete cleanup path")
			require.NoError(t, owner.Unlock(
				ctx, probeTxnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestProxyPendingFollowerIsNotHandoffEligible(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"owner", "origin"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26790)
			row := newTestRows(1)
			_, err := owner.getLockTableWithCreate(
				ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)

			representativeTxnID := []byte("proxy-admitted-representative")
			followerTxnID := []byte("proxy-pending-follower")
			_, err = origin.Lock(
				ctx, table, row, representativeTxnID,
				newTestRowSharedOptions())
			require.NoError(t, err)

			followerTxn := origin.activeTxnHolder.getActiveTxn(
				followerTxnID, true, "")
			bookkeepingStarted := make(chan struct{})
			releaseBookkeeping := make(chan struct{})
			var signalOnce sync.Once
			followerTxn.Lock()
			followerTxn.beforeLockAdded = func([]byte, [][]byte) error {
				signalOnce.Do(func() { close(bookkeepingStarted) })
				<-releaseBookkeeping
				return ErrTxnNotFound
			}
			followerTxn.Unlock()

			followerDone := make(chan error, 1)
			go func() {
				_, lockErr := origin.Lock(
					ctx, table, row, followerTxnID,
					newTestRowSharedOptions())
				followerDone <- lockErr
			}()
			select {
			case <-bookkeepingStarted:
			case <-ctx.Done():
				t.Fatalf("follower did not reach transaction bookkeeping: %v", ctx.Err())
			}

			unlockDone := make(chan error, 1)
			go func() {
				unlockDone <- origin.Unlock(
					ctx, representativeTxnID, timestamp.Timestamp{})
			}()

			// Give the source Unlock a deterministic opportunity to expose an
			// uncommitted follower. A correct implementation keeps it behind the
			// proxy admission point until bookkeeping either commits or rolls back.
			var earlyUnlockErr error
			unlockFinishedEarly := false
			select {
			case earlyUnlockErr = <-unlockDone:
				unlockFinishedEarly = true
			case <-time.After(100 * time.Millisecond):
			}
			close(releaseBookkeeping)

			select {
			case lockErr := <-followerDone:
				require.ErrorIs(t, lockErr, ErrTxnNotFound)
			case <-ctx.Done():
				t.Fatalf("follower bookkeeping did not finish: %v", ctx.Err())
			}
			if unlockFinishedEarly {
				require.NoError(t, earlyUnlockErr)
			} else {
				select {
				case unlockErr := <-unlockDone:
					require.NoError(t, unlockErr)
				case <-ctx.Done():
					t.Fatalf("representative unlock did not finish: %v", ctx.Err())
				}
			}
			followerTxn.Lock()
			followerTxn.beforeLockAdded = nil
			followerTxn.Unlock()
			require.NoError(t, origin.Unlock(
				ctx, followerTxnID, timestamp.Timestamp{}))

			proxy := origin.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			_, retainedHolder := proxy.mu.holders[string(row[0])]
			_, retainedRepresentative := proxy.mu.currentHolder[string(row[0])]
			proxy.mu.RUnlock()
			require.False(t, retainedHolder,
				"a failed follower admission must not retain proxy membership")
			require.False(t, retainedRepresentative,
				"a failed follower admission must not become the remote representative")

			probeTxnID := []byte("proxy-pending-follower-probe")
			probe := newTestRowExclusiveOptions()
			probe.Policy = pb.WaitPolicy_FastFail
			_, err = owner.Lock(ctx, table, row, probeTxnID, probe)
			require.NoError(t, err,
				"failed follower bookkeeping must not strand its identity at the owner")
			require.NoError(t, owner.Unlock(
				ctx, probeTxnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 8
			c.MaxFixedSliceSize = 8
		},
	)
}
