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

package cnservice

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func TestServiceIcebergCacheInvalidatorReachesRemoteCNAfterCommit(t *testing.T) {
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "cn-a", QueryAddress: "cn-a-query"},
			{ServiceID: "cn-b", QueryAddress: "cn-b-query"},
		}, nil),
	)
	defer cluster.Close()

	local := &icebergAssemblyTableCache{}
	remote := &icebergAssemblyQueryClient{serviceID: "cn-a"}
	s := &service{moCluster: cluster, queryClient: remote}
	invalidator := s.newIcebergMetadataCacheInvalidator(local)
	req := icebergapi.AppendRequest{
		CatalogRequest: icebergapi.CatalogRequest{
			Catalog: model.Catalog{AccountID: 7, CatalogID: 42},
		},
		Namespace: icebergapi.Namespace{"sales", "audit"},
		Table:     "orders",
	}
	result := icebergapi.CommitResult{
		SnapshotID:           200,
		MetadataLocationHash: "metadata-200",
		CommitID:             "commit-200",
	}

	require.NoError(t, invalidator.InvalidateIcebergTable(context.Background(), req, result))
	require.Equal(t, 1, local.calls)
	require.Equal(t, uint32(7), local.accountID)
	require.Equal(t, uint64(42), local.catalogID)
	require.Equal(t, icebergapi.NamespaceCacheKey(req.Namespace), local.namespace)
	require.Equal(t, "orders", local.table)

	require.Equal(t, []string{"cn-b-query"}, remote.addresses)
	require.Len(t, remote.requests, 1)
	payload := remote.requests[0].IcebergCacheInvalidateRequest
	require.Equal(t, query.CmdMethod_IcebergCacheInvalidate, remote.requests[0].CmdMethod)
	require.Equal(t, uint32(7), payload.AccountID)
	require.Equal(t, uint64(42), payload.CatalogID)
	require.Equal(t, icebergapi.NamespaceCacheKey(req.Namespace), payload.Namespace)
	require.Equal(t, "orders", payload.Table)
	require.Equal(t, int64(200), payload.SnapshotID)
	require.Equal(t, "metadata-200", payload.MetadataLocationHash)
	require.Equal(t, "commit-200", payload.CommitID)
	require.Equal(t, 1, remote.released)
}

type icebergAssemblyTableCache struct {
	calls     int
	accountID uint32
	catalogID uint64
	namespace string
	table     string
}

func (c *icebergAssemblyTableCache) InvalidateTable(accountID uint32, catalogID uint64, namespace, table string) int {
	c.calls++
	c.accountID = accountID
	c.catalogID = catalogID
	c.namespace = namespace
	c.table = table
	return 1
}

type icebergAssemblyQueryClient struct {
	serviceID string
	addresses []string
	requests  []*query.Request
	released  int
}

func (c *icebergAssemblyQueryClient) ServiceID() string {
	return c.serviceID
}

func (c *icebergAssemblyQueryClient) SendMessage(_ context.Context, address string, req *query.Request) (*query.Response, error) {
	c.addresses = append(c.addresses, address)
	c.requests = append(c.requests, req)
	return &query.Response{CmdMethod: req.CmdMethod}, nil
}

func (c *icebergAssemblyQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *icebergAssemblyQueryClient) Release(*query.Response) {
	c.released++
}

func (*icebergAssemblyQueryClient) Close() error {
	return nil
}
