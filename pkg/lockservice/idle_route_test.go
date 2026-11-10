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
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

type idleRouteTestClient struct {
	Client
	owners       *[]string
	inventoryErr error
	getBindCalls atomic.Int64
	afterGetBind func(context.Context) error
}

func (c *idleRouteTestClient) cachedLockServiceOwners(ctx context.Context, scratch []string) ([]string, error) {
	if c.inventoryErr != nil {
		return nil, c.inventoryErr
	}
	if c.owners != nil {
		return append(scratch[:0], (*c.owners)...), ctx.Err()
	}
	return c.Client.(interface {
		cachedLockServiceOwners(context.Context, []string) ([]string, error)
	}).cachedLockServiceOwners(ctx, scratch)
}

func (c *idleRouteTestClient) confirmLockServiceOwner(ctx context.Context, serviceID string) (bool, error) {
	return c.Client.(interface {
		confirmLockServiceOwner(context.Context, string) (bool, error)
	}).confirmLockServiceOwner(ctx, serviceID)
}

func (c *idleRouteTestClient) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if req.Method == pb.Method_GetBind {
		c.getBindCalls.Add(1)
	}
	resp, err := c.Client.Send(ctx, req)
	if err == nil && req.Method == pb.Method_GetBind && c.afterGetBind != nil {
		if err := c.afterGetBind(ctx); err != nil {
			releaseResponse(resp)
			return nil, err
		}
	}
	return resp, err
}

func TestIdleRemoteRoutesAfterMemberRemoval(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "survivor"}, func(alloc *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Drive the real keeper round explicitly, without a timer or sleeps.
		require.NoError(t, owner.remote.keeper.Close())
		require.NoError(t, source.remote.keeper.Close())
		keeper := source.remote.keeper.(*lockTableKeeper)
		transport := source.remote.client.(*client)
		counted := &idleRouteTestClient{Client: source.remote.client}
		source.remote.client, keeper.client = counted, counted
		options := newTestRowExclusiveOptions()
		const table = uint64(29447)
		old := alloc.Get(owner.serviceID, options.Group, table, table, options.Sharding)
		_, err := source.Lock(ctx, table, newTestRows(1), []byte("warm"), options)
		require.NoError(t, err)
		require.NoError(t, source.Unlock(ctx, []byte("warm"), timestamp.Timestamp{}))
		require.Empty(t, source.collectRemoteLockBinds(nil))
		require.Equal(t, old, source.tableGroups.get(old.Group, table).getBind())

		keeper.doKeepRemoteLock(ctx, nil, nil)
		warmCalls := counted.getBindCalls.Load()
		for range 10 {
			_, err := source.Lock(ctx, table, newTestRows(1), []byte("steady"), options)
			require.NoError(t, err)
			require.NoError(t, source.Unlock(ctx, []byte("steady"), timestamp.Timestamp{}))
			keeper.doKeepRemoteLock(ctx, nil, nil)
		}
		require.Equal(t, warmCalls, counted.getBindCalls.Load(), "stable cache hits and idle keeper rounds must not send GetBind")

		// Model the already-completed retirement: TN invalidates the binding,
		// then the existing local membership snapshot loses the old CN.
		alloc.disableGroupTables([]pb.LockTable{old}, alloc.getServiceBinds(owner.serviceID))
		transport.cluster.RemoveCN("owner")
		keeper.doKeepRemoteLock(ctx, nil, nil)
		require.Nil(t, source.tableGroups.get(old.Group, table), "the background keeper must evict the idle route before a new transaction uses it")
		require.Equal(t, warmCalls, counted.getBindCalls.Load(), "membership reconciliation must not send GetBind")

		_, err = source.Lock(ctx, table, newTestRows(1), []byte("first-after-scale"), options)
		require.NoError(t, err, "first new transaction after retirement must acquire the valid binding directly")
		require.NoError(t, source.Unlock(ctx, []byte("first-after-scale"), timestamp.Timestamp{}))
		require.Equal(t, warmCalls+1, counted.getBindCalls.Load(), "only the ordinary cache miss should fetch a binding")
		require.Equal(t, source.serviceID, source.tableGroups.get(old.Group, table).getBind().ServiceID)
	})
}
