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

package maintenance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestCatalogMetadataLoaderLoadsAndParsesMetadata(t *testing.T) {
	current := int64(2)
	metadataJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/sales/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 2,
		"refs": {"main": {"snapshot-id": 2, "type": "branch"}},
		"snapshots": [
			{"snapshot-id": 1, "timestamp-ms": 1000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-1.avro"},
			{"snapshot-id": 2, "parent-snapshot-id": 1, "timestamp-ms": 2000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-2.avro"}
		]
	}`)
	var got api.LoadTableRequest
	client := &catalog.MockClient{LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		got = req
		return &api.LoadTableResponse{
			Namespace:        req.Namespace,
			TableName:        req.Table,
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.json",
			MetadataJSON:     metadataJSON,
		}, nil
	}}
	meta, err := (CatalogMetadataLoader{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
	}).LoadMaintenanceTableMetadata(context.Background(), Request{
		Namespace: "sales.orders_ns",
		Table:     "orders",
		TargetRef: "dev",
	})
	require.NoError(t, err)
	require.Equal(t, api.Namespace{"sales", "orders_ns"}, got.Namespace)
	require.Equal(t, "orders", got.Table)
	require.Equal(t, "all", got.Snapshots)
	require.Equal(t, current, *meta.CurrentSnapshotID)
	require.Equal(t, "main", meta.Refs["main"].Name)
	require.NotEmpty(t, meta.MetadataLocationHash)
	require.NotContains(t, meta.MetadataLocationRed, "warehouse")
}

func TestCatalogMetadataLoaderRequestsAllSnapshotsForExpire(t *testing.T) {
	metadataJSON := []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/sales/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 2,
		"refs": {"main": {"snapshot-id": 2, "type": "branch"}},
		"snapshots": [
			{"snapshot-id": 1, "timestamp-ms": 1000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-1.avro"},
			{"snapshot-id": 2, "parent-snapshot-id": 1, "timestamp-ms": 2000, "manifest-list": "s3://warehouse/sales/orders/metadata/snap-2.avro"}
		]
	}`)
	var got api.LoadTableRequest
	client := &catalog.MockClient{LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		got = req
		return &api.LoadTableResponse{
			Namespace:        req.Namespace,
			TableName:        req.Table,
			MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.json",
			MetadataJSON:     metadataJSON,
		}, nil
	}}
	_, err := (CatalogMetadataLoader{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
	}).LoadMaintenanceTableMetadata(context.Background(), Request{
		Operation: OperationExpireSnapshots,
		Namespace: "sales.orders_ns",
		Table:     "orders",
		TargetRef: "dev",
	})
	require.NoError(t, err)
	require.Equal(t, "all", got.Snapshots)
}

func TestCatalogMetadataLoaderRequiresMetadataJSON(t *testing.T) {
	_, err := (CatalogMetadataLoader{Client: &catalog.MockClient{}}).LoadMaintenanceTableMetadata(context.Background(), Request{
		Namespace: "sales",
		Table:     "orders",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata JSON")
}
