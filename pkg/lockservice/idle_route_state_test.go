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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestIdleRemoteRoutesPreserveAdmissionAndCleanup(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		owners := []string{"owner", "survivor"}
		keeper.client = &idleRouteTestClient{Client: keeper.client, owners: &owners}
		keeper.keepIdleRemoteRoutes(ctx)
		options := newTestRowExclusiveOptions()
		const table = uint64(92001)
		old := alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
		source.tableGroups.set(old.Group, old.Table, source.createLockTableByBind(old))

		// The owner callback is reached after the source has published its bind
		// reference, while the first Lock RPC has not completed yet.
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		owner.option.beforeRemoteLockBindCheck = func() {
			close(entered)
			<-release
		}
		defer func() { owner.option.beforeRemoteLockBindCheck = nil }()
		done := make(chan error, 1)
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			_, err := source.Lock(ctx, table, newTestRows(1), []byte("in-flight"), options)
			done <- err
		}()
		defer func() {
			releaseOnce.Do(func() { close(release) })
			cancel()
			<-exited
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		owners = []string{"survivor"}
		keeper.keepIdleRemoteRoutes(ctx)
		require.Equal(t, old, source.tableGroups.get(old.Group, table).getBind())
		require.Len(t, keeper.idleRemoteRoutes, 1, "a referenced route must remain pending instead of being evicted")
		txn := source.activeTxnHolder.getActiveTxn([]byte("in-flight"), false, "")
		require.NotNil(t, txn)
		txn.Lock()
		fenced := txn.bindChanged
		txn.Unlock()
		require.False(t, fenced, "inventory absence must not fence an admitted transaction")
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, <-done)
		require.NoError(t, source.Unlock(ctx, []byte("in-flight"), timestamp.Timestamp{}))
		keeper.keepIdleRemoteRoutes(ctx)
		require.Nil(t, source.tableGroups.get(old.Group, table), "cleanup must make the pending idle route eligible without a second membership change")
		require.Empty(t, keeper.idleRemoteRoutes)
	})
}

func TestIdleRemoteRouteSnapshotSignals(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		transport := source.remote.client.(*client)
		reader := &idleRouteTestClient{Client: keeper.client}
		keeper.client = reader
		old := alloc.Get(owner.serviceID, 0, 92002, 92002, pb.Sharding_None)
		source.tableGroups.set(0, old.Table, source.createLockTableByBind(old))
		keeper.keepIdleRemoteRoutes(ctx)

		var ownerInfo metadata.CNService
		require.NoError(t, clusterservice.GetCNServiceRawWithContext(ctx, transport.cluster,
			clusterservice.NewServiceIDSelector("owner"), func(cn metadata.CNService) bool {
				ownerInfo = cn
				return false
			}))
		ownerInfo.WorkState = metadata.WorkState_Drained
		transport.cluster.UpdateCN(ownerInfo)
		keeper.keepIdleRemoteRoutes(ctx)
		require.Equal(t, old, source.tableGroups.get(0, old.Table).getBind(), "a non-Working CN remains a valid internal lock owner")

		reader.inventoryErr = errors.New("inventory unavailable")
		transport.cluster.RemoveCN("owner")
		keeper.keepIdleRemoteRoutes(ctx)
		require.Equal(t, old, source.tableGroups.get(0, old.Table).getBind(), "a failed snapshot must not publish partial membership or evict routes")
		reader.inventoryErr = nil
		canceled, stop := context.WithCancel(ctx)
		stop()
		keeper.keepIdleRemoteRoutes(canceled)
		require.Equal(t, old, source.tableGroups.get(0, old.Table).getBind())
		keeper.keepIdleRemoteRoutes(ctx)
		require.Nil(t, source.tableGroups.get(0, old.Table))

		// Rejoining is an update to current inventory, not a permanent owner
		// tombstone. TN still decides whether this exact binding can be used.
		transport.cluster.AddCN(ownerInfo)
		keeper.keepIdleRemoteRoutes(ctx)
		source.handleBindChanged(old)
		require.Equal(t, old, source.tableGroups.get(0, old.Table).getBind())
	})
}

func TestIdleRemoteRouteEvictionBatchAndGeneration(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(_ *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		owners := []string{"owner", "survivor"}
		keeper.client = &idleRouteTestClient{Client: keeper.client, owners: &owners}
		keeper.keepIdleRemoteRoutes(ctx)
		const routeCount = idleRemoteRouteEvictionBatchSize*2 + 1
		for i := range routeCount {
			bind := pb.LockTable{Table: uint64(i + 1), OriginTable: uint64(i + 1), ServiceID: owner.serviceID, Version: 1, Valid: true}
			source.tableGroups.set(0, bind.Table, source.createLockTableByBind(bind))
		}
		owners = []string{"survivor"}
		keeper.keepIdleRemoteRoutes(ctx)
		require.Len(t, keeper.idleRemoteRoutes, routeCount-idleRemoteRouteEvictionBatchSize)
		remaining := 0
		source.tableGroups.iter(func(_ uint64, _ lockTable) bool { remaining++; return true })
		require.Equal(t, routeCount-idleRemoteRouteEvictionBatchSize, remaining)

		stale := keeper.idleRemoteRoutes[0]
		replacement := stale
		replacement.ServiceID = source.serviceID
		replacement.Version++
		source.handleBindChanged(replacement)
		for len(keeper.idleRemoteRoutes) > 0 {
			require.NoError(t, ctx.Err())
			keeper.keepIdleRemoteRoutes(ctx)
		}
		require.Equal(t, replacement, source.tableGroups.get(0, replacement.Table).getBind(), "an old queued candidate must not evict a replacement generation")
		version := source.tableGroups.getVersion()
		keeper.keepIdleRemoteRoutes(ctx)
		require.Equal(t, version, source.tableGroups.getVersion(), "stable membership must leave the route cache untouched")
	})
}

func TestIdleRemoteRouteRejectsDelayedPublication(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		owners := []string{"owner", "survivor"}
		returned, release := make(chan struct{}), make(chan struct{})
		var unblock sync.Once
		var blocked atomic.Bool
		reader := &idleRouteTestClient{Client: source.remote.client, owners: &owners}
		reader.afterGetBind = func(ctx context.Context) error {
			if !blocked.CompareAndSwap(false, true) {
				return nil
			}
			close(returned)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		source.remote.client, keeper.client = reader, reader
		keeper.keepIdleRemoteRoutes(ctx)
		options := newTestRowExclusiveOptions()
		const table = uint64(92003)
		old := alloc.Get(owner.serviceID, 0, table, table, options.Sharding)
		done, exited := make(chan error, 1), make(chan struct{})
		go func() {
			defer close(exited)
			_, err := source.Lock(ctx, table, newTestRows(1), []byte("delayed"), options)
			done <- err
		}()
		defer func() {
			unblock.Do(func() { close(release) })
			cancel()
			<-exited
		}()
		select {
		case <-returned:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		owners = []string{"survivor"}
		keeper.keepIdleRemoteRoutes(ctx)
		alloc.disableGroupTables([]pb.LockTable{old}, alloc.getServiceBinds(owner.serviceID))
		reader.Client.(*client).cluster.RemoveCN("owner")
		unblock.Do(func() { close(release) })
		require.True(t, moerr.IsMoErrCode(<-done, moerr.ErrLockTableBindChanged))
		require.Nil(t, source.tableGroups.get(0, table), "a late GetBind reply must not repopulate the departed route")
		require.Nil(t, source.activeTxnHolder.getActiveTxn([]byte("delayed"), false, ""), "stale publication must be rejected before admitting transaction state")

		// Both other publication paths share the same membership guard.
		source.handleBindChanged(old)
		require.Nil(t, source.tableGroups.get(0, table))
		state := source.allocatorStateSnapshot()
		err := source.handleBindChangedFromAllocator(ctx, "test-late-refresh", old, old, state, state)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
		require.Nil(t, source.tableGroups.get(0, table))

		_, err = source.Lock(ctx, table, newTestRows(1), []byte("delayed"), options)
		require.NoError(t, err, "pre-admission rejection must leave the same transaction ID eligible for a fresh binding")
		require.NoError(t, source.Unlock(ctx, []byte("delayed"), timestamp.Timestamp{}))
	})
}

func BenchmarkIdleRemoteRoutesStable(b *testing.B) {
	for _, count := range []int{0, 10000, 100000} {
		b.Run(fmt.Sprintf("cached-routes-%d", count), func(b *testing.B) {
			// Stable rounds must be independent of route-cache cardinality.
			// No background services or RPC clients are started for this benchmark.
			holder := &lockTableHolder{tables: make(map[uint64]lockTable, count)}
			for i := range count {
				holder.tables[uint64(i)] = &remoteLockTable{bind: pb.LockTable{ServiceID: "owner"}}
			}
			tables := &lockTableHolders{holders: map[uint32]*lockTableHolder{0: holder}}
			owners := []string{"owner", "survivor"}
			svc := &service{serviceID: "survivor", tableGroups: tables}
			keeper := &lockTableKeeper{
				serviceID: "survivor", service: svc, groupTables: tables,
				client: &idleRouteTestClient{owners: &owners},
			}
			ctx := context.Background()
			keeper.keepIdleRemoteRoutes(ctx)
			keeper.keepIdleRemoteRoutes(ctx) // allocate both reusable snapshot buffers
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				keeper.keepIdleRemoteRoutes(ctx)
			}
		})
	}
}
