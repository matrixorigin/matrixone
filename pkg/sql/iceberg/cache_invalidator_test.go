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

package iceberg

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func TestClusterRemoteCacheInvalidatorSendsBestEffortToOtherCNs(t *testing.T) {
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "self", QueryAddress: "self-addr"},
			{ServiceID: "remote-1", QueryAddress: "remote-1-addr"},
			{ServiceID: "remote-2", QueryAddress: "remote-2-addr"},
			{ServiceID: "remote-empty"},
		}, nil),
	)
	defer cluster.Close()
	client := &fakeQueryMessageClient{serviceID: "self", failAddress: "remote-2-addr"}
	err := ClusterRemoteCacheInvalidator{
		Cluster:     cluster,
		QueryClient: client,
		Timeout:     time.Second,
	}.InvalidateIcebergTable(context.Background(), api.AppendRequest{
		CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	}, api.CommitResult{SnapshotID: 200, MetadataLocationHash: "hash-200", CommitID: "commit-200"})
	require.NoError(t, err)
	addresses, requests, released := client.snapshot()
	require.ElementsMatch(t, []string{"remote-1-addr", "remote-2-addr"}, addresses)
	require.NotEmpty(t, requests)
	require.Equal(t, query.CmdMethod_IcebergCacheInvalidate, requests[0].CmdMethod)
	payload := requests[0].IcebergCacheInvalidateRequest
	require.Equal(t, uint32(7), payload.AccountID)
	require.Equal(t, uint64(42), payload.CatalogID)
	require.Equal(t, api.NamespaceCacheKey(api.Namespace{"sales"}), payload.Namespace)
	require.Equal(t, "orders", payload.Table)
	require.Equal(t, int64(200), payload.SnapshotID)
	require.Equal(t, "hash-200", payload.MetadataLocationHash)
	require.Equal(t, "commit-200", payload.CommitID)
	require.Equal(t, 1, released)
}

func TestClusterRemoteCacheInvalidatorBlockedPeerDoesNotSuppressHealthyPeer(t *testing.T) {
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "self", QueryAddress: "self-addr"},
			{ServiceID: "remote-1", QueryAddress: "remote-1-addr"},
			{ServiceID: "remote-2", QueryAddress: "remote-2-addr"},
		}, nil),
	)
	defer cluster.Close()
	client := &fakeQueryMessageClient{serviceID: "self", blockAddress: "remote-1-addr"}

	err := (ClusterRemoteCacheInvalidator{
		Cluster:     cluster,
		QueryClient: client,
		Timeout:     100 * time.Millisecond,
	}).InvalidateIcebergTable(context.Background(), api.AppendRequest{}, api.CommitResult{})

	require.NoError(t, err)
	addresses, _, released := client.snapshot()
	require.ElementsMatch(t, []string{"remote-1-addr", "remote-2-addr"}, addresses)
	require.Equal(t, 1, released, "the healthy peer must complete before the shared deadline")
}

func TestClusterRemoteCacheInvalidatorIncludesDrainingCN(t *testing.T) {
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "self", QueryAddress: "self-addr", WorkState: metadata.WorkState_Working},
			{ServiceID: "draining", QueryAddress: "draining-addr", WorkState: metadata.WorkState_Draining},
			{ServiceID: "no-query-address", WorkState: metadata.WorkState_Draining},
		}, nil),
	)
	defer cluster.Close()
	client := &fakeQueryMessageClient{serviceID: "self"}

	err := (ClusterRemoteCacheInvalidator{
		Cluster:     cluster,
		QueryClient: client,
		Timeout:     time.Second,
	}).InvalidateIcebergTable(context.Background(), api.AppendRequest{}, api.CommitResult{})

	require.NoError(t, err)
	addresses, _, released := client.snapshot()
	require.Equal(t, []string{"draining-addr"}, addresses)
	require.Equal(t, 1, released)
}

func TestClusterRemoteCacheInvalidatorOutlivesCommittedStatementContext(t *testing.T) {
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "self", QueryAddress: "self-addr"},
			{ServiceID: "remote", QueryAddress: "remote-addr"},
		}, nil),
	)
	defer cluster.Close()
	client := &fakeQueryMessageClient{serviceID: "self"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (ClusterRemoteCacheInvalidator{
		Cluster:     cluster,
		QueryClient: client,
		Timeout:     time.Second,
	}).InvalidateIcebergTable(ctx, api.AppendRequest{}, api.CommitResult{})

	require.NoError(t, err)
	addresses, _, released := client.snapshot()
	require.Equal(t, []string{"remote-addr"}, addresses)
	require.Equal(t, 1, released)
}

type fakeQueryMessageClient struct {
	mu           sync.Mutex
	serviceID    string
	failAddress  string
	blockAddress string
	addresses    []string
	requests     []*query.Request
	released     int
}

func (c *fakeQueryMessageClient) ServiceID() string {
	return c.serviceID
}

func (c *fakeQueryMessageClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *fakeQueryMessageClient) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	c.mu.Lock()
	c.addresses = append(c.addresses, address)
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	if address == c.blockAddress {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	if address == c.failAddress {
		return nil, api.NewError(api.ErrCatalogUnavailable, "remote unavailable", nil)
	}
	return &query.Response{CmdMethod: req.CmdMethod}, nil
}

func (c *fakeQueryMessageClient) Release(resp *query.Response) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.released++
}

func (c *fakeQueryMessageClient) snapshot() ([]string, []*query.Request, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.addresses...), append([]*query.Request(nil), c.requests...), c.released
}
