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

package mongodb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func TestClusterRemoteClientRetirerCoversEveryCNLifecycleScope(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retirement ClientRetirement
		connection Connection
	}{
		{
			name: "alter generation",
			retirement: ClientRetirement{
				AccountID: 7, ConnectionID: 9, VersionExclusive: 4,
			},
			connection: Connection{AccountID: 7, ConnectionID: 9, Version: 3},
		},
		{
			name:       "drop connection",
			retirement: ClientRetirement{AccountID: 7, ConnectionID: 9},
			connection: Connection{AccountID: 7, ConnectionID: 9, Version: 3},
		},
		{
			name:       "drop account",
			retirement: ClientRetirement{AccountID: 7},
			connection: Connection{AccountID: 7, ConnectionID: 9, Version: 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := clusterservice.NewMOCluster(
				"", nil, time.Second, clusterservice.WithDisableRefresh(),
				clusterservice.WithServices([]metadata.CNService{
					{ServiceID: "self", QueryAddress: "self-addr"},
					{ServiceID: "remote-1", QueryAddress: "remote-1-addr"},
					{ServiceID: "remote-2", QueryAddress: "remote-2-addr"},
					{ServiceID: "remote-empty"},
				}, nil),
			)
			defer cluster.Close()

			pools := map[string]*ClientPool{
				"self-addr":     NewClientPool(&fakeFactory{}),
				"remote-1-addr": NewClientPool(&fakeFactory{}),
				"remote-2-addr": NewClientPool(&fakeFactory{}),
			}
			for _, pool := range pools {
				t.Cleanup(func() { require.NoError(t, pool.Close(context.Background())) })
			}
			targets := make(map[string]*fakeClient, len(pools))
			unaffected := make(map[string]*fakeClient, len(pools))
			for address, pool := range pools {
				targets[address] = seedIdleClient(t, pool, tc.connection)
				unaffected[address] = seedIdleClient(t, pool, Connection{AccountID: 8, ConnectionID: 9, Version: 3})
			}

			require.NoError(t, tc.retirement.Apply(pools["self-addr"]))
			client := &retirementQueryClient{serviceID: "self", pools: pools}
			ClusterRemoteClientRetirer{
				Cluster: cluster, QueryClient: client, Timeout: time.Second,
			}.Retire(t.Context(), tc.retirement)

			for address := range pools {
				require.Equal(t, 1, targets[address].count(), address)
				require.Zero(t, unaffected[address].count(), address)
			}
			require.ElementsMatch(t, []string{"remote-1-addr", "remote-2-addr"}, client.addresses())
			require.Equal(t, 2, client.releasedCount())
		})
	}
}

func seedIdleClient(t *testing.T, pool *ClientPool, connection Connection) *fakeClient {
	t.Helper()
	lease, err := pool.Acquire(t.Context(), connection, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	client := lease.Client().(*fakeClient)
	require.NoError(t, lease.Release(t.Context()))
	return client
}

type retirementQueryClient struct {
	serviceID string
	pools     map[string]*ClientPool
	mu        sync.Mutex
	sent      []string
	released  int
}

func (c *retirementQueryClient) ServiceID() string { return c.serviceID }

func (*retirementQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *retirementQueryClient) SendMessage(
	ctx context.Context, address string, req *query.Request,
) (*query.Response, error) {
	c.mu.Lock()
	c.sent = append(c.sent, address)
	c.mu.Unlock()
	payload := req.GetMongoDBClientRetireRequest()
	err := (ClientRetirement{
		AccountID: payload.AccountID, ConnectionID: payload.ConnectionID,
		VersionExclusive: payload.VersionExclusive,
	}).Apply(c.pools[address])
	return &query.Response{CmdMethod: req.CmdMethod, MongoDBClientRetireResponse: query.MongoDBClientRetireResponse{Success: err == nil}}, err
}

func (c *retirementQueryClient) Release(*query.Response) {
	c.mu.Lock()
	c.released++
	c.mu.Unlock()
}

func (c *retirementQueryClient) addresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

func (c *retirementQueryClient) releasedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.released
}
