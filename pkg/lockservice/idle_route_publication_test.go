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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestIdleRemoteRoutePublicationAfterMemberAddition(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		transport := source.remote.client.(*client)
		inventory := transport.cluster
		var ownerInfo metadata.CNService
		require.NoError(t, clusterservice.GetCNServiceRawWithContext(ctx, inventory,
			clusterservice.NewServiceIDSelector("owner"), func(cn metadata.CNService) bool {
				ownerInfo = cn
				return false
			}))
		require.NotEmpty(t, ownerInfo.LockServiceAddress)
		// Internal lock ownership does not require public SQL admission.
		ownerInfo.WorkState = metadata.WorkState_Drained
		owners := []string{"survivor"}
		counted := &idleRouteTestClient{Client: transport, owners: &owners}
		source.remote.client, keeper.client = counted, counted
		keeper.keepIdleRemoteRoutes(ctx)
		defer func() { transport.cluster = inventory }()

		for index, refresh := range []bool{false, true} {
			name := "inventory-already-current"
			if refresh {
				name = "inventory-needs-refresh"
			}
			t.Run(name, func(t *testing.T) {
				view := &refreshOnDemandCluster{MOCluster: inventory, after: ownerInfo}
				if !refresh {
					view.before = ownerInfo
				}
				transport.cluster = view
				options := newTestRowExclusiveOptions()
				table := uint64(93000 + index)
				alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
				before := counted.getBindCalls.Load()
				_, err := source.Lock(ctx, table, newTestRows(1), []byte(name), options)
				require.NoError(t, err, "a stale keeper snapshot must not reject a routable TN binding")
				require.NoError(t, source.Unlock(ctx, []byte(name), timestamp.Timestamp{}))
				require.Equal(t, before+1, counted.getBindCalls.Load(), "the first Lock must succeed without a GetBind retry")
				require.Equal(t, refresh, view.refreshed)
				require.Equal(t, owner.serviceID, source.tableGroups.get(options.Group, table).getBind().ServiceID)
			})
		}
		// Legacy publication confirms the owner. An accepted TN replacement
		// can instead finish old-generation cleanup and leave its route cold;
		// the subsequent cold publication still confirms the joined owner.
		for index := range 2 {
			bind := pb.LockTable{Table: uint64(93010 + index), ServiceID: owner.serviceID, Version: 1, Valid: true}
			if index == 0 {
				source.handleBindChanged(bind)
			} else {
				state := source.allocatorStateSnapshot()
				old := bind
				old.ServiceID = source.serviceID
				require.NoError(t, source.handleBindChangedFromAllocator(ctx, "test-member-add", old, bind, state, state))
				require.Nil(t, source.tableGroups.get(0, bind.Table))
				_, err := source.publishLockTableBindFromAllocator(ctx, "test-cold-member-add", 0, bind.Table, bind, state, state)
				require.NoError(t, err)
			}
			current := source.tableGroups.get(0, bind.Table)
			require.NotNil(t, current)
			require.Equal(t, bind, current.getBind())
		}
		// The owner can join and leave entirely between keeper rounds. Even
		// though its snapshot is unchanged, published idle routes need a scan.
		transport.cluster = inventory
		inventory.RemoveCN("owner")
		keeper.keepIdleRemoteRoutes(ctx)
		for _, table := range []uint64{93000, 93001, 93010, 93011} {
			require.Nil(t, source.tableGroups.get(0, table), "a brief join/leave must not strand a route outside the keeper worklist")
		}
		require.False(t, source.idleRouteScanRequested.Load())
	})
}

func TestIdleRemoteRoutePublicationConfirmationFailures(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		transport := source.remote.client.(*client)
		inventory := transport.cluster
		defer func() { transport.cluster = inventory }()
		var ownerInfo metadata.CNService
		require.NoError(t, clusterservice.GetCNServiceRawWithContext(ctx, inventory,
			clusterservice.NewServiceIDSelector("owner"), func(cn metadata.CNService) bool {
				ownerInfo = cn
				return false
			}))
		owners := []string{"survivor"}
		counted := &idleRouteTestClient{Client: transport, owners: &owners}
		source.remote.client, keeper.client = counted, counted
		keeper.keepIdleRemoteRoutes(ctx)
		refreshErr := errors.New("discovery refresh failed")
		for index, mode := range []string{"refresh-error", "caller-cancel", "snapshot-changed", "still-absent"} {
			t.Run(mode, func(t *testing.T) {
				view := &refreshOnDemandCluster{
					MOCluster: inventory, after: ownerInfo,
					refreshing: make(chan struct{}), refreshDone: make(chan struct{}),
				}
				if mode == "refresh-error" {
					view.refreshErr = refreshErr
				}
				if mode == "still-absent" {
					view.after = metadata.CNService{}
				}
				transport.cluster = view
				options := newTestRowExclusiveOptions()
				table := uint64(93100 + index)
				alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
				callCtx, stop := context.WithCancel(ctx)
				defer stop()
				done, exited := make(chan error, 1), make(chan struct{})
				go func() {
					defer close(exited)
					_, err := source.Lock(callCtx, table, newTestRows(1), []byte(mode), options)
					done <- err
				}()
				defer func() {
					stop()
					<-exited
				}()
				select {
				case <-view.refreshing:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// Discovery must not retain the locks needed by cached work,
				// authoritative allocator transitions, or service shutdown.
				require.True(t, source.bindChangeMu.TryLock())
				source.bindChangeMu.Unlock()
				require.True(t, source.allocatorVersionMu.TryLock())
				source.allocatorVersionMu.Unlock()
				require.True(t, source.lifecycle.TryLock())
				source.lifecycle.Unlock()
				if mode == "caller-cancel" {
					stop() // Refresh must terminate without releasing refreshDone.
				} else {
					if mode == "snapshot-changed" {
						owners = []string{"survivor", "unrelated"}
						keeper.keepIdleRemoteRoutes(ctx)
					}
					close(view.refreshDone)
				}
				err := <-done
				switch mode {
				case "refresh-error":
					require.ErrorIs(t, err, refreshErr)
				case "caller-cancel":
					require.ErrorIs(t, err, context.Canceled)
				default:
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
				}
				require.Nil(t, source.tableGroups.get(options.Group, table))
				require.Nil(t, source.activeTxnHolder.getActiveTxn([]byte(mode), false, ""), "failed confirmation must precede transaction admission")
				require.False(t, source.idleRouteScanRequested.Load())
			})
		}
	})
}
