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
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestForwardLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			_, err := l2.getLockTableWithCreate(context.Background(), 0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			txn1 := []byte("txn1")
			row1 := []byte{1}

			_, err = l1.Lock(ctx, tableID, [][]byte{row1}, txn1, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			txn := l2.activeTxnHolder.getActiveTxn(txn1, false, "")
			require.NotNil(t, txn)
			require.Equal(t, l1.serviceID, txn.remoteService)
			require.True(t, l2.activeTxnHolder.hasRemoteLockBind(l1.serviceID, l2.tableGroups.get(0, tableID).getBind(), time.Second))
		},
	)
}

func TestForwardLockUsesEffectiveLockDeadline(t *testing.T) {
	holderTxn := []byte("holder")
	waiterTxn := []byte("waiter")
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			origin := services[0]
			owner := services[1]
			const tableID = uint64(11)
			_, err := owner.getLockTableWithCreate(context.Background(), 0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			options := newTestRowExclusiveOptions()
			options.ForwardTo = "s2"
			options.LockWaitTimeout = 1
			// Forwarded txns are normally created by the frontend txn lifecycle on
			// the origin CN. Register both here so owner-side orphan detection sees
			// the same liveness state as production while the waiter is blocked.
			origin.activeTxnHolder.getActiveTxn(holderTxn, true, "")
			origin.activeTxnHolder.getActiveTxn(waiterTxn, true, "")

			// A deadline-less background context previously reached morpc.Send
			// unchanged and panicked. Service entry must inject and propagate the
			// effective lock deadline to the forwarded RPC.
			_, err = origin.Lock(context.Background(), tableID, [][]byte{{1}}, holderTxn, options)
			require.NoError(t, err)

			start := time.Now()
			_, err = origin.Lock(context.Background(), tableID, [][]byte{{1}}, waiterTxn, options)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout), "unexpected error: %v", err)
			require.GreaterOrEqual(t, time.Since(start), time.Second)
			require.Less(t, time.Since(start), 3*time.Second)
		},
		func(c *Config) {
			c.TxnIterFunc = func(fn func([]byte) bool) {
				if fn(holderTxn) {
					fn(waiterTxn)
				}
			}
		},
	)
}

func TestForwardLockDeadlineCancelsLockTableAllocationWait(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			const (
				group   = uint32(0)
				tableID = uint64(24917)
			)
			waitC := make(chan struct{})
			s.mu.Lock()
			if s.mu.allocating[group] == nil {
				s.mu.allocating[group] = make(map[uint64]chan struct{})
			}
			s.mu.allocating[group][tableID] = waitC
			s.mu.Unlock()
			defer func() {
				s.mu.Lock()
				delete(s.mu.allocating[group], tableID)
				s.mu.Unlock()
				close(waitC)
			}()

			options := newTestRowExclusiveOptions()
			options.LockWaitTimeout = 60
			options.LockWaitDeadline = time.Now().Add(100 * time.Millisecond).UnixNano()
			resultC := make(chan error, 1)
			go func() {
				_, err := s.forwardLock(
					context.Background(),
					tableID,
					[][]byte{{1}},
					[]byte("forward-bind-waiter"),
					options)
				resultC <- err
			}()

			select {
			case err := <-resultC:
				require.ErrorIs(t, err, ErrLockTimeout)
			case <-time.After(2 * time.Second):
				require.Fail(t, "forwarded lock budget did not cancel the allocation wait")
			}
		},
	)
}

func TestNewLockRPCContextPreservesEarlierParentDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelParent()
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)

	rpcCtx, cancelRPC := newLockRPCContext(parent, pb.LockOptions{
		LockWaitTimeout:  60,
		LockWaitDeadline: time.Now().Add(time.Second).UnixNano(),
	})
	defer cancelRPC()
	rpcDeadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, rpcDeadline)
}

func TestDeadLockWithForward(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			// The receiving CN is the logical coordinator, as in CN setup.
			l2.cfg.ForwardedLockAdmission = func(id []byte) bool { return string(id) == string(txn2) }
			l1.cfg.TxnIterFunc = func(visit func([]byte) bool) { visit(txn1) }
			l2.cfg.TxnIterFunc = func(visit func([]byte) bool) { visit(txn2) }
			row1 := []byte{1}
			row2 := []byte{2}

			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			_, err := l1.Lock(ctx, 1, [][]byte{row2}, txn2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   l2.serviceID,
			})
			require.NoError(t, err)

			holder, held, err := l1.GetLockHolder(ctx, 1, row2, newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.True(t, held)
			require.Equal(t, txn2, holder.TxnID)

			lockErrors := make(chan error, 2)
			unlockErrors := make(chan error, 2)
			for _, participant := range []struct {
				service *service
				txn     []byte
				row     []byte
			}{{l1, txn1, row2}, {l2, txn2, row1}} {
				go func() {
					_, err := participant.service.Lock(ctx, 1, [][]byte{participant.row},
						participant.txn, newTestRowExclusiveOptions())
					lockErrors <- err
					unlockErrors <- participant.service.Unlock(ctx, participant.txn, timestamp.Timestamp{})
				}()
			}
			first, second := <-lockErrors, <-lockErrors
			require.NoError(t, <-unlockErrors)
			require.NoError(t, <-unlockErrors)
			firstDeadlock := moerr.IsMoErrCode(first, moerr.ErrDeadLockDetected)
			secondDeadlock := moerr.IsMoErrCode(second, moerr.ErrDeadLockDetected)
			require.True(t, (firstDeadlock || secondDeadlock) &&
				(first == nil || firstDeadlock) && (second == nil || secondDeadlock),
				"expected a detected deadlock, got %v and %v", first, second)
		},
	)
}

func TestLocalRollbackRechecksRouteAfterForwardedLock(t *testing.T) {
	type rollbackKey struct{}
	immediateEntered := make(chan struct{}, 1)
	resumeImmediate := make(chan struct{})
	resume := sync.OnceFunc(func() { close(resumeImmediate) })
	runLockServiceTests(t, []string{"coordinator", "mirror", "owner"},
		func(_ *lockTableAllocator, services []*service) {
			defer resume()
			coordinator, mirror, owner := services[0], services[1], services[2]
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			const localTable, remoteTable = 1050, 1051
			row := []byte{1}
			rows := [][]byte{row}
			id := []byte("forward-vs-local-rollback")
			coordinator.cfg.ForwardedLockAdmission = func([]byte) bool { return true }
			coordinator.cfg.EnableRemoteLocalProxy = true

			_, err := coordinator.Lock(ctx, localTable, rows, id, newTestRowExclusiveOptions())
			require.NoError(t, err)
			_, err = owner.Lock(ctx, remoteTable, rows, []byte("seed"), newTestRowExclusiveOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, []byte("seed"), timestamp.Timestamp{}))
			table, err := coordinator.getLockTableWithCreate(ctx, 0, remoteTable, rows, pb.Sharding_None)
			require.NoError(t, err)
			proxy := table.(*localLockTableProxy)

			immediate := make(chan error, 1)
			go func() {
				immediate <- coordinator.unlockTransaction(context.WithValue(ctx, rollbackKey{}, true),
					id, timestamp.Timestamp{}, localRollbackCleanup)
			}()
			select {
			case <-immediateEntered:
			case <-ctx.Done():
				t.Fatal("rollback did not reach the close boundary")
			}

			options := newTestRowSharedOptions()
			options.ForwardTo = coordinator.serviceID
			_, err = mirror.Lock(ctx, remoteTable, rows, id, options)
			require.NoError(t, err)

			// A rollback must see the remote route under its transaction mutex
			// and exit without trying to acquire the proxy mutex.
			proxy.mu.Lock()
			resume()
			select {
			case err := <-immediate:
				proxy.mu.Unlock()
				require.ErrorIs(t, err, ErrLockTimeout)
			case <-time.After(time.Second):
				proxy.mu.Unlock()
				t.Fatal("foreground rollback entered proxy cleanup")
			}

			resolved := make(chan struct{})
			pending, err := coordinator.ResolveRollbackCleanup(id, func() { close(resolved) })
			require.NoError(t, err)
			require.True(t, pending)
			select {
			case <-resolved:
			case <-ctx.Done():
				t.Fatal("resolver did not release forwarded lock")
			}
			_, held, err := owner.GetLockHolder(ctx, remoteTable, row, options)
			require.NoError(t, err)
			require.False(t, held)
		},
		WithWait(func(ctx context.Context) error {
			if ctx.Value(rollbackKey{}) == nil {
				return nil
			}
			immediateEntered <- struct{}{}
			select {
			case <-resumeImmediate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	)
}

func TestForwardLockCreatesMissingAndRejectsReplacedTable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replace  bool
		sharding pb.Sharding
	}{
		{name: "missing route"},
		{name: "cold by-row route", sharding: pb.Sharding_ByRow},
		{name: "replaced instance", replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runLockServiceTests(t, []string{"worker", "owner"}, func(_ *lockTableAllocator, nodes []*service) {
				worker, owner := nodes[0], nodes[1]
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const table = 1811
				rows := newTestRows(1)
				opts := newTestRowExclusiveOptions()
				opts.Sharding = tc.sharding
				old, err := owner.getLockTableWithCreate(ctx, 0, table, rows, opts.Sharding)
				require.NoError(t, err)
				opts.ForwardTo = owner.serviceID
				id := []byte("forwarded")
				result := make(chan error, 1)
				remove := func() {
					removed, err := owner.CloseRemoteLockTable(0, old.getBind().Table, old.getBind().Version)
					require.NoError(t, err)
					require.True(t, removed)
				}

				entered, release := make(chan struct{}), make(chan struct{})
				resume := sync.OnceFunc(func() { close(release) })
				defer resume()
				if tc.replace {
					owner.option.beforeRemoteLockBindCheck = sync.OnceFunc(func() {
						close(entered)
						<-release
					})
				} else {
					remove()
				}
				go func() {
					_, err := worker.Lock(ctx, table, rows, id, opts)
					result <- err
				}()
				if tc.replace {
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("forwarded request did not reach the bind check")
					}
					remove()
					current, err := owner.getLockTableWithCreate(ctx, 0, table, rows, opts.Sharding)
					require.NoError(t, err)
					require.NotSame(t, old, current)
					require.True(t, old.getBind().Equal(current.getBind()))
					resume()
				}
				select {
				case err := <-result:
					if tc.replace {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged), "%v", err)
					} else {
						require.NoError(t, err)
					}
				case <-ctx.Done():
					t.Fatal("forwarded request did not return")
				}
				if tc.replace {
					require.False(t, owner.activeTxnHolder.hasActiveTxn(id))
					_, held, err := owner.GetLockHolder(ctx, table, rows[0], opts)
					require.NoError(t, err)
					require.False(t, held)
					_, err = worker.Lock(ctx, table, rows, id, opts)
					require.NoError(t, err)
				}
				holder, held, err := owner.GetLockHolder(ctx, table, rows[0], opts)
				require.NoError(t, err)
				require.True(t, held)
				require.Equal(t, id, holder.TxnID)
				if tc.sharding == pb.Sharding_ByRow {
					bind := owner.tableGroups.get(0, ShardingByRow(rows[0])).getBind()
					require.Equal(t, uint64(table), bind.OriginTable)
					require.Equal(t, ShardingByRow(rows[0]), bind.Table)
				}
				require.NoError(t, owner.Unlock(ctx, id, timestamp.Timestamp{}))
			})
		})
	}
}

func TestForwardLockColdBindingObservesCoordinatorFence(t *testing.T) {
	runLockServiceTests(t, []string{"s1", "s2"}, func(alloc *lockTableAllocator, services []*service) {
		coordinator, mirror := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const held, cold = 263, 264
		id := []byte("mirror-cold-bind-fence")
		rows := [][]byte{{1}}
		opts := newTestRowExclusiveOptions()
		_, err := coordinator.Lock(ctx, held, rows, id, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), id, timestamp.Timestamp{})
		txn := coordinator.activeTxnHolder.getActiveTxn(id, false, "")

		gate := make(chan struct{})
		mirror.mu.Lock()
		mirror.mu.allocating[0] = map[uint64]chan struct{}{cold: gate}
		mirror.mu.Unlock()
		defer func() {
			mirror.mu.Lock()
			delete(mirror.mu.allocating[0], cold)
			close(gate)
			mirror.mu.Unlock()
		}()

		bind := coordinator.tableGroups.get(0, held).getBind()
		bind.Version++
		alloc.mu.Lock()
		release := sync.OnceFunc(alloc.mu.Unlock)
		defer release()
		opts.ForwardTo = coordinator.serviceID
		done := make(chan error, 1)
		go func() {
			_, err := mirror.Lock(ctx, cold, rows, id, opts)
			done <- err
		}()
		require.Eventually(t, func() bool { return txn.bindFence.Load() != nil }, time.Second, time.Millisecond)
		coordinator.handleBindChanged(bind)
		select {
		case err := <-done:
			require.True(t, moerr.IsLockTableBindChangedForFencedTxn(err), "%v", err)
		case <-ctx.Done():
			t.Fatal("cold forwarded lock did not observe coordinator fence")
		}
		require.Equal(t, gate, mirror.getAllocatingC(0, cold, false))
		require.Nil(t, mirror.tableGroups.get(0, cold))
		require.Nil(t, coordinator.tableGroups.get(0, cold))

		// Already-fenced transactions also fail with both waits gated.
		_, err = mirror.Lock(ctx, cold, rows, id, opts)
		require.True(t, moerr.IsLockTableBindChangedForFencedTxn(err), "%v", err)

		// An unfenced transaction resolves the mirror route and only then
		// acquires a real lock with the same binding used for heartbeat.
		release()
		control := []byte("mirror-cold-bind-control")
		result, err := mirror.Lock(ctx, cold, rows, control, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), control, timestamp.Timestamp{})
		require.Equal(t, mirror.tableGroups.get(0, cold).getBind(), result.LockedOn)
		require.Equal(t, coordinator.tableGroups.get(0, cold).getBind(), result.LockedOn)
		require.True(t, coordinator.activeTxnHolder.hasRemoteLockBind(mirror.serviceID, result.LockedOn, time.Second))
	})
}

func TestForwardBindSaturationPreservesCoordinatorUnlock(t *testing.T) {
	runLockServiceTests(t, []string{"s1", "s2"}, func(alloc *lockTableAllocator, services []*service) {
		coordinator, mirror := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows := [][]byte{{1}}
		opts := newTestRowExclusiveOptions()
		holder := []byte("unlock-during-cold-resolution")
		const held, cold, mirrorWarm = 265, 266, 267
		lock, err := coordinator.Lock(ctx, held, rows, holder, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), holder, timestamp.Timestamp{})
		_, err = mirror.getLockTableWithCreate(ctx, opts.Group, held, rows, opts.Sharding)
		require.NoError(t, err)
		_, err = mirror.getLockTableWithCreate(ctx, opts.Group, mirrorWarm, rows, opts.Sharding)
		require.NoError(t, err)
		require.Nil(t, coordinator.tableGroups.get(opts.Group, mirrorWarm))

		alloc.mu.Lock()
		release := sync.OnceFunc(alloc.mu.Unlock)
		defer release()
		opts.ForwardTo = coordinator.serviceID

		resolveID := []byte("resolve-cold-mirror")
		warmID := []byte("forward-cold-coordinator")
		mismatchID := []byte("refresh-mismatched-coordinator")
		start := func(call func() error, slots int32) <-chan error {
			t.Helper()
			done := make(chan error, 1)
			go func() { done <- call() }()
			require.Eventually(t, func() bool {
				return coordinator.forwardBindResolvers.Load() == slots
			}, time.Second, time.Millisecond)
			return done
		}

		resolve := start(func() error {
			_, err := mirror.Lock(ctx, cold, rows, resolveID, opts)
			return err
		}, 1)
		forward := start(func() error {
			_, err := mirror.Lock(ctx, mirrorWarm, rows, warmID, opts)
			return err
		}, 2)
		stale := lock.LockedOn
		stale.Version++
		mismatch := start(func() error {
			req := acquireRequest()
			defer releaseRequest(req)
			req.Method = pb.Method_ForwardLock
			req.LockTable = stale
			req.Lock = pb.LockRequest{TxnID: mismatchID, ServiceID: mirror.serviceID, Rows: rows, Options: opts}
			resp, err := mirror.remote.client.Send(ctx, req)
			if resp != nil {
				releaseResponse(resp)
			}
			return err
		}, maxForwardBindResolvers)

		overload := make(chan error, defaultHandleWorkers)
		for i := range defaultHandleWorkers {
			go func() {
				id := []byte(fmt.Sprintf("over-capacity-%d", i))
				_, err := mirror.Lock(ctx, mirrorWarm, rows, id, opts)
				overload <- err
			}()
		}
		for range defaultHandleWorkers {
			require.True(t, moerr.IsMoErrCode(<-overload, moerr.ErrForwardBindBusy))
		}

		warmControl := []byte("warm-control")
		warmRow := []byte{2}
		result, err := mirror.Lock(ctx, held, [][]byte{warmRow}, warmControl, opts)
		require.NoError(t, err)
		require.Equal(t, lock.LockedOn, result.LockedOn)
		defer coordinator.Unlock(context.Background(), warmControl, timestamp.Timestamp{})
		owner, ok, err := coordinator.GetLockHolder(ctx, held, warmRow, opts)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, warmControl, owner.TxnID)
		req := acquireRequest()
		defer releaseRequest(req)
		req.Method = pb.Method_Unlock
		req.LockTable = lock.LockedOn
		req.Unlock.TxnID = holder
		resp, err := mirror.remote.client.Send(ctx, req)
		require.NoError(t, err)
		releaseResponse(resp)
		require.False(t, coordinator.activeTxnHolder.hasActiveTxn(holder))
		require.EqualValues(t, maxForwardBindResolvers, coordinator.forwardBindResolvers.Load())

		release()
		require.NoError(t, <-resolve)
		require.NoError(t, <-forward)
		require.NoError(t, <-mismatch)
		require.Zero(t, coordinator.forwardBindResolvers.Load())
		require.False(t, coordinator.activeTxnHolder.hasActiveTxn(mismatchID))
		require.NoError(t, coordinator.Unlock(ctx, resolveID, timestamp.Timestamp{}))
		require.NoError(t, coordinator.Unlock(ctx, warmID, timestamp.Timestamp{}))
	})
}

func TestForwardLockRemovedBindingPreservesFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("fenced=%t", fenced), func(t *testing.T) {
			runLockServiceTests(t, []string{"s1", "s2"}, func(_ *lockTableAllocator, services []*service) {
				coordinator, mirror := services[0], services[1]
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const held, target = 268, 269
				id := []byte("forward-removed-binding")
				rows := [][]byte{{1}}
				opts := newTestRowExclusiveOptions()
				_, err := coordinator.getLockTableWithCreate(ctx, opts.Group, target, rows, opts.Sharding)
				require.NoError(t, err)
				l, err := mirror.getLockTableWithCreate(ctx, opts.Group, target, rows, opts.Sharding)
				require.NoError(t, err)
				if fenced {
					_, err = coordinator.Lock(ctx, held, rows, id, opts)
					require.NoError(t, err)
					defer coordinator.Unlock(context.Background(), id, timestamp.Timestamp{})
				}
				bind := l.getBind()
				coordinator.option.beforeRemoteLockBindCheck = sync.OnceFunc(func() {
					coordinator.observeAllocatorVersion("forward-test", bind.Version+1)
				})
				opts.ForwardTo = coordinator.serviceID
				_, err = mirror.Lock(ctx, target, rows, id, opts)
				if fenced {
					require.True(t, moerr.IsFencedTxnLockRejected(err), "%v", err)
				} else {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound), "%v", err)
					require.False(t, coordinator.activeTxnHolder.hasActiveTxn(id))
				}
				require.Nil(t, coordinator.tableGroups.get(opts.Group, target))
				require.False(t, coordinator.activeTxnHolder.hasRemoteLockBind(mirror.serviceID, bind, time.Second))
			})
		})
	}
}

type forwardTestClient struct {
	Client
	send func(context.Context, *pb.Request) (*pb.Response, error)
}

func (c *forwardTestClient) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	return c.send(ctx, req)
}

func TestColdForwardBindDistinguishesLookupDeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"lookup deadline", nil},
		{"caller deadline", context.DeadlineExceeded},
		{"lock wait deadline", ErrLockTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tc.cause != nil {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeoutCause(ctx, time.Second, tc.cause)
					defer cancel()
				}
				mirror := &service{tableGroups: &lockTableHolders{holders: make(map[uint32]*lockTableHolder)}}
				mirror.remote.client = &forwardTestClient{send: func(rpcCtx context.Context, req *pb.Request) (*pb.Response, error) {
					require.Equal(t, pb.Method_ResolveForwardBind, req.Method)
					<-rpcCtx.Done()
					return nil, rpcCtx.Err()
				}}
				opts := pb.LockOptions{ForwardTo: getServiceIdentifier("coordinator", 1)}
				_, err := mirror.getForwardBinding(ctx, 1, nil, []byte("txn"), opts)
				if tc.cause != nil {
					require.ErrorIs(t, err, tc.cause)
					require.False(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindLookupTimeout))
					return
				}
				require.NoError(t, ctx.Err())
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindLookupTimeout), "%v", err)
				var reply pb.Response
				reply.WrapError(err)
				require.True(t, moerr.IsMoErrCode(reply.UnwrapError(), moerr.ErrLockTableBindLookupTimeout))
			})
		})
	}
}

func TestWarmForwardBindLookupDeadlineSurvivesRPC(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(t, []string{"s1", "s2"}, 30*time.Second, func(alloc *lockTableAllocator, services []*service) {
		coordinator, mirror := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 2*defaultRPCTimeout+5*time.Second)
		defer cancel()
		const table = 276
		rows := [][]byte{{1}}
		local, err := coordinator.getLockTableWithCreate(ctx, 0, table, rows, pb.Sharding_None)
		require.NoError(t, err)
		cached, err := mirror.getLockTableWithCreate(ctx, 0, table, rows, pb.Sharding_None)
		require.NoError(t, err)
		bind := local.getBind()
		require.Equal(t, bind, cached.getBind())
		require.Equal(t, 1, coordinator.tableGroups.removeWithFilter(func(id uint64, _ lockTable) bool {
			return id == table
		}, closeReasonKeeperFailed))

		opts := newTestRowExclusiveOptions()
		opts.ForwardTo = coordinator.serviceID
		alloc.mu.Lock()
		release := sync.OnceFunc(alloc.mu.Unlock)
		defer release()
		_, err = mirror.Lock(ctx, table, rows, []byte("warm-forward-lookup-timeout"), opts)
		release()
		require.NoError(t, ctx.Err())
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindLookupTimeout), "%v", err)

		control := []byte("warm-forward-lookup-control")
		result, err := mirror.Lock(ctx, table, rows, control, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), control, timestamp.Timestamp{})
		require.Equal(t, bind, result.LockedOn)
	}, nil)
}

func TestForwardLockStaleMirrorUsesCoordinatorBinding(t *testing.T) {
	runLockServiceTests(t, []string{"s1", "s2"}, func(alloc *lockTableAllocator, services []*service) {
		coordinator, mirror := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		const held, target = 271, 272
		id, control := []byte("fenced-after-rebind"), []byte("healthy-after-rebind")
		rows := [][]byte{{1}}
		opts := newTestRowExclusiveOptions()
		_, err := coordinator.Lock(ctx, held, rows, id, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), id, timestamp.Timestamp{})
		old, err := mirror.getLockTableWithCreate(ctx, 0, target, rows, pb.Sharding_None)
		require.NoError(t, err)
		oldBind := old.getBind()
		newBind := oldBind
		newBind.ServiceID = coordinator.serviceID
		newBind.Version++
		alloc.mu.Lock()
		alloc.mu.lockTables[0][target] = newBind
		alloc.mu.Unlock()

		client := mirror.remote.client
		mirror.remote.client = &forwardTestClient{Client: client, send: func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			if req.Method == pb.Method_GetBind {
				t.Error("mirror queried the allocator after the coordinator returned a binding")
			}
			return client.Send(ctx, req)
		}}
		opts.ForwardTo = coordinator.serviceID
		_, err = mirror.Lock(ctx, target, rows, id, opts)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged), "%v", err)
		require.False(t, moerr.IsLockTableBindChangedForFencedTxn(err))
		require.Equal(t, newBind, mirror.tableGroups.get(0, target).getBind())
		heldBind := coordinator.tableGroups.get(0, held).getBind()
		heldBind.Version++
		coordinator.handleBindChanged(heldBind)
		_, err = mirror.Lock(ctx, target, rows, id, opts)
		require.True(t, moerr.IsLockTableBindChangedForFencedTxn(err), "%v", err)

		result, err := mirror.Lock(ctx, target, rows, control, opts)
		require.NoError(t, err)
		defer coordinator.Unlock(context.Background(), control, timestamp.Timestamp{})
		require.Equal(t, newBind, result.LockedOn)
		holder, found, err := coordinator.GetLockHolder(ctx, target, rows[0], opts)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, control, holder.TxnID)
	})
}

func TestForwardLockRefreshesAllocatorSnapshotAfterColdResolve(t *testing.T) {
	runLockServiceTests(t, []string{"s1", "s2"}, func(_ *lockTableAllocator, services []*service) {
		coordinator, mirror := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		initial := allocatorState{id: "initial", version: 100}
		resolved := allocatorState{id: "resolved", version: 200}
		refreshed := allocatorState{id: "refreshed", version: 300}
		mirror.lastAllocatorState.Store(&initial)
		const table = 273
		bind := pb.LockTable{Table: table, OriginTable: table, ServiceID: coordinator.serviceID,
			AllocatorID: resolved.id, Version: resolved.version, Valid: true}
		newBind := bind
		newBind.AllocatorID, newBind.Version = refreshed.id, refreshed.version
		client := mirror.remote.client
		mirror.remote.client = &forwardTestClient{Client: client, send: func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			switch req.Method {
			case pb.Method_ResolveForwardBind:
				resp := acquireResponse()
				resp.GetBind = pb.GetBindResponse{LockTable: bind, AllocatorID: resolved.id, AllocatorVersion: resolved.version}
				return resp, nil
			case pb.Method_ForwardLock:
				require.Equal(t, resolved, mirror.allocatorStateSnapshot())
				require.Equal(t, bind, req.LockTable)
				resp := acquireResponse()
				resp.NewBind = &newBind
				resp.GetBind = pb.GetBindResponse{AllocatorID: refreshed.id, AllocatorVersion: refreshed.version}
				return resp, nil
			default:
				return client.Send(ctx, req)
			}
		}}
		opts := newTestRowExclusiveOptions()
		opts.ForwardTo = coordinator.serviceID
		_, err := mirror.Lock(ctx, table, [][]byte{{1}}, []byte("cold-allocator-transition"), opts)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged), "%v", err)
		require.Equal(t, refreshed, mirror.allocatorStateSnapshot())
		require.Equal(t, newBind, mirror.tableGroups.get(0, table).getBind())
	})
}
