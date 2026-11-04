// Copyright 2026 Matrix Origin
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

type retiredRouteTestClient struct {
	*idleRouteTestClient
	unreachableOwner string
	discoveryErr     error
	unlockCalls      atomic.Int64
	discoveryCalls   atomic.Int64
	retried          chan struct{}
}

func (c *retiredRouteTestClient) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if req.Method == pb.Method_Unlock && req.LockTable.ServiceID == c.unreachableOwner {
		if c.unlockCalls.Add(1) == 2 {
			close(c.retried)
		}
		return nil, moerr.NewBackendCannotConnectNoCtx("retired owner unavailable")
	}
	return c.idleRouteTestClient.Send(ctx, req)
}

func (c *retiredRouteTestClient) confirmLockServiceOwner(context.Context, string) (bool, error) {
	c.discoveryCalls.Add(1)
	return false, c.discoveryErr
}

func TestRetiredRemoteOwnerUnlockDoesNotRequireReplacementDiscovery(t *testing.T) {
	for _, discovery := range []string{"absent", "error"} {
		for _, api := range []string{"bounded", "ordinary"} {
			t.Run(discovery+"/"+api, func(t *testing.T) {
				runLockServiceTests(t, []string{"old-owner", "new-owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
					owner, replacement, source := services[0], services[1], services[2]
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					for _, svc := range services {
						require.NoError(t, svc.remote.keeper.Close())
					}
					keeper := source.remote.keeper.(*lockTableKeeper)
					owners := []string{"old-owner", "survivor"}
					transport := &retiredRouteTestClient{
						idleRouteTestClient: &idleRouteTestClient{Client: source.remote.client, owners: &owners},
						unreachableOwner:    owner.serviceID, retried: make(chan struct{}),
					}
					if discovery == "error" {
						transport.discoveryErr = errors.New("replacement discovery unavailable")
					}
					source.remote.client, keeper.client = transport, transport
					keeper.keepIdleRemoteRoutes(ctx)
					options := newTestRowExclusiveOptions()
					const table = uint64(94001)
					old := alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
					for index, txnID := range []string{"closing", "other-consumer"} {
						_, err := source.Lock(ctx, table, newTestRows(byte(index+1)), []byte(txnID), options)
						require.NoError(t, err)
					}
					owners = []string{"survivor"}
					keeper.keepIdleRemoteRoutes(ctx)
					require.Equal(t, old, source.tableGroups.get(old.Group, table).getBind(), "membership alone must preserve the active old generation")
					alloc.disableGroupTables([]pb.LockTable{old}, alloc.getServiceBinds(owner.serviceID))
					newBind := alloc.Get(replacement.serviceID, options.Group, table, table, options.Sharding)
					require.NotEqual(t, source.serviceID, newBind.ServiceID)
					require.True(t, old.Changed(newBind))
					before := transport.getBindCalls.Load()
					callCtx, stop := context.WithCancel(ctx)
					defer stop()
					if api == "ordinary" {
						stop() // Ordinary Unlock must complete even after caller cancellation.
					}
					done, exited := make(chan error, 1), make(chan struct{})
					go func() {
						defer close(exited)
						if api == "ordinary" {
							done <- source.Unlock(callCtx, []byte("closing"), timestamp.Timestamp{})
						} else {
							done <- source.UnlockWithContext(callCtx, []byte("closing"), timestamp.Timestamp{})
						}
					}()
					defer func() {
						stop()
						// Bound a broken ordinary Unlock independently of its ignored
						// caller context, before fixture cleanup waits for operations.
						source.lifecycle.cancel()
						<-exited
					}()
					select {
					case err := <-done:
						require.NoError(t, err)
					case <-transport.retried:
						t.Fatal("Unlock retried a binding already superseded by TN")
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					require.Equal(t, int64(1), transport.unlockCalls.Load())
					require.Equal(t, before+1, transport.getBindCalls.Load(), "cleanup must use the real accepted TN binding")
					require.Zero(t, transport.discoveryCalls.Load(), "old-generation cleanup must not consult replacement discovery")
					require.Nil(t, source.tableGroups.get(old.Group, table), "unconfirmed replacement must leave the route cold")
					require.Nil(t, source.activeTxnHolder.getActiveTxn([]byte("closing"), false, ""))
					other := source.activeTxnHolder.getActiveTxn([]byte("other-consumer"), false, "")
					require.NotNil(t, other)
					other.Lock()
					fenced := other.bindChanged
					other.Unlock()
					require.True(t, fenced, "TN's accepted change must fence the other old-generation consumer")
					require.Empty(t, source.collectRemoteLockBinds(nil))
					source.mu.RLock()
					ref := source.mu.remoteBindRefs[makeRemoteBindKey(old)]
					source.mu.RUnlock()
					require.Equal(t, uint64(1), ref.refs)
					require.True(t, ref.invalidated)
					require.NoError(t, source.UnlockWithContext(ctx, []byte("other-consumer"), timestamp.Timestamp{}))
					source.mu.RLock()
					remaining := len(source.mu.remoteBindRefs)
					source.mu.RUnlock()
					require.Zero(t, remaining, "the last cleanup must release the drain reference")
				})
			})
		}
	}
}

func TestRetiredRemoteBindPreservesReplacementRoute(t *testing.T) {
	runLockServiceTests(t, []string{"old-owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		for _, svc := range services {
			require.NoError(t, svc.remote.keeper.Close())
		}
		for index, name := range []string{"already-published", "newer-published"} {
			t.Run(name, func(t *testing.T) {
				options := newTestRowExclusiveOptions()
				table := uint64(94010 + index)
				old := alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
				_, err := source.Lock(ctx, table, newTestRows(1), []byte("old-consumer"), options)
				require.NoError(t, err)
				proposed := old
				proposed.ServiceID = source.serviceID
				proposed.Version++
				current := proposed
				if index != 0 {
					current.Version++
				}
				// A concurrent publication completes while the old TN reply is
				// delayed. A new transaction already uses that replacement.
				source.handleBindChanged(current)
				_, err = source.Lock(ctx, table, newTestRows(2), []byte("current-consumer"), options)
				require.NoError(t, err)
				state := source.allocatorStateSnapshot()
				err = source.handleBindChangedFromAllocator(ctx, "test-unchanged-bind", current, current, state, state)
				require.ErrorIs(t, err, ErrLockTableBindChanged, "an unchanged TN bind cannot authorize retirement")
				require.NoError(t, source.handleBindChangedFromAllocator(ctx, "test-late-changed-bind", old, proposed, state, state))
				require.Equal(t, current, source.tableGroups.get(options.Group, table).getBind())
				txn := source.activeTxnHolder.getActiveTxn([]byte("current-consumer"), false, "")
				require.NotNil(t, txn)
				txn.Lock()
				fenced := txn.bindChanged
				txn.Unlock()
				require.False(t, fenced, "retiring the old generation must preserve replacement consumers")
				require.NoError(t, source.UnlockWithContext(ctx, []byte("old-consumer"), timestamp.Timestamp{}))
				require.NoError(t, source.UnlockWithContext(ctx, []byte("current-consumer"), timestamp.Timestamp{}))
			})
		}
	})
}
