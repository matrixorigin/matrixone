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
	require.Equal(t, []string{"remote-1-addr", "remote-2-addr"}, client.addresses)
	require.Equal(t, query.CmdMethod_IcebergCacheInvalidate, client.requests[0].CmdMethod)
	payload := client.requests[0].IcebergCacheInvalidateRequest
	require.Equal(t, uint32(7), payload.AccountID)
	require.Equal(t, uint64(42), payload.CatalogID)
	require.Equal(t, api.NamespaceCacheKey(api.Namespace{"sales"}), payload.Namespace)
	require.Equal(t, "orders", payload.Table)
	require.Equal(t, int64(200), payload.SnapshotID)
	require.Equal(t, "hash-200", payload.MetadataLocationHash)
	require.Equal(t, "commit-200", payload.CommitID)
	require.Equal(t, 1, client.released)
}

func TestClusterRemoteCacheInvalidatorBoundsWholeBroadcast(t *testing.T) {
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
	client := &fakeQueryMessageClient{serviceID: "self", blockUntilCancel: true}

	err := (ClusterRemoteCacheInvalidator{
		Cluster:     cluster,
		QueryClient: client,
		Timeout:     20 * time.Millisecond,
	}).InvalidateIcebergTable(context.Background(), api.AppendRequest{}, api.CommitResult{})

	require.NoError(t, err)
	require.Equal(t, []string{"remote-1-addr"}, client.addresses,
		"one broadcast timeout must stop sends to later CNs")
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
	require.Equal(t, []string{"remote-addr"}, client.addresses)
	require.Equal(t, 1, client.released)
}

type fakeQueryMessageClient struct {
	serviceID        string
	failAddress      string
	addresses        []string
	requests         []*query.Request
	released         int
	blockUntilCancel bool
}

func (c *fakeQueryMessageClient) ServiceID() string {
	return c.serviceID
}

func (c *fakeQueryMessageClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *fakeQueryMessageClient) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	c.addresses = append(c.addresses, address)
	c.requests = append(c.requests, req)
	if c.blockUntilCancel {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	if address == c.failAddress {
		return nil, api.NewError(api.ErrCatalogUnavailable, "remote unavailable", nil)
	}
	return &query.Response{CmdMethod: req.CmdMethod}, nil
}

func (c *fakeQueryMessageClient) Release(resp *query.Response) {
	c.released++
}
