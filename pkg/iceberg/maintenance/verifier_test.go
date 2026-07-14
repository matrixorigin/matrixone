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

func TestCatalogCommitVerifierChecksTargetSnapshot(t *testing.T) {
	var loadReq api.LoadTableRequest
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
	}
	result, ok, err := (CatalogCommitVerifier{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
	}).VerifyCommittedMaintenance(context.Background(), Request{
		AccountID: 7,
		CatalogID: 42,
		Namespace: "sales",
		Table:     "orders",
		TargetRef: "main",
		Operation: OperationRewriteManifests,
	}, maintenanceCommitPlan(), api.CommitResult{
		SnapshotID:           4,
		CommitID:             "commit-4",
		MetadataLocationHash: api.PathHash("s3://warehouse/orders/metadata/v4.json"),
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Verified)
	require.Equal(t, "commit-4", result.CommitID)
	require.Equal(t, api.Namespace{"sales"}, loadReq.Namespace)
	require.Equal(t, "orders", loadReq.Table)
	require.Equal(t, "all", loadReq.Snapshots)
}

func TestCatalogCommitVerifierReturnsUnverifiedOnSnapshotMismatch(t *testing.T) {
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
	}
	result, ok, err := (CatalogCommitVerifier{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
	}).VerifyCommittedMaintenance(context.Background(), Request{
		AccountID: 7,
		CatalogID: 42,
		Namespace: "sales",
		Table:     "orders",
		TargetRef: "main",
		Operation: OperationRewriteManifests,
	}, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 5, CommitID: "commit-5"})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, result.Verified)
	require.Equal(t, "commit-5", result.CommitID)
}

func TestCatalogCommitVerifierReturnsUnverifiedOnMetadataHashMismatch(t *testing.T) {
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
	}
	result, ok, err := (CatalogCommitVerifier{
		Client:  client,
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
	}).VerifyCommittedMaintenance(context.Background(), Request{
		AccountID: 7,
		CatalogID: 42,
		Namespace: "sales",
		Table:     "orders",
		TargetRef: "main",
		Operation: OperationRewriteManifests,
	}, maintenanceCommitPlan(), api.CommitResult{
		SnapshotID:           4,
		CommitID:             "commit-4",
		MetadataLocationHash: "different",
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, result.Verified)
}

func TestCatalogFactoryCommitVerifierCreatesClientForResolvedCatalog(t *testing.T) {
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     expireMetadataJSON(),
			}, nil
		},
	}
	factory := &verifierCatalogFactory{client: client}
	result, ok, err := (CatalogFactoryCommitVerifier{CatalogFactory: factory}).VerifyCommittedMaintenance(context.Background(), Request{
		AccountID: 7,
		CatalogID: 42,
		Catalog: model.Catalog{
			AccountID: 7,
			CatalogID: 42,
			Name:      "ksa_gold",
			Type:      "rest",
			URI:       "https://catalog.example.com",
		},
		Namespace: "sales",
		Table:     "orders",
		TargetRef: "main",
		Operation: OperationRewriteManifests,
	}, maintenanceCommitPlan(), api.CommitResult{
		SnapshotID:           4,
		CommitID:             "commit-4",
		MetadataLocationHash: api.PathHash("s3://warehouse/orders/metadata/v4.json"),
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Verified)
	require.Equal(t, uint64(42), factory.catalog.CatalogID)
	require.Equal(t, "ksa_gold", factory.catalog.Name)
}

func TestCatalogCommitVerifierValidationEdges(t *testing.T) {
	result, ok, err := (CatalogCommitVerifier{}).VerifyCommittedMaintenance(context.Background(), Request{
		Operation: OperationRewriteManifests,
	}, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, int64(4), result.SnapshotID)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))

	result, ok, err = (CatalogCommitVerifier{Client: &catalog.MockClient{}}).VerifyCommittedMaintenance(context.Background(), Request{
		Operation: OperationRewriteManifests,
		Namespace: "sales",
		Table:     "orders",
	}, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 0, CommitID: "unknown"})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "unknown", result.CommitID)
}

func TestCatalogFactoryCommitVerifierValidationEdges(t *testing.T) {
	req := Request{
		Operation: OperationRewriteManifests,
		Catalog:   model.Catalog{AccountID: 7, CatalogID: 42, Name: "ksa_gold"},
		Namespace: "sales",
		Table:     "orders",
	}
	_, ok, err := (CatalogFactoryCommitVerifier{}).VerifyCommittedMaintenance(context.Background(), req, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))

	req.Catalog.CatalogID = 0
	_, ok, err = (CatalogFactoryCommitVerifier{
		CatalogFactory: &verifierCatalogFactory{client: &catalog.MockClient{}},
	}).VerifyCommittedMaintenance(context.Background(), req, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Contains(t, err.Error(), "resolved catalog")

	req.Catalog.CatalogID = 42
	_, ok, err = (CatalogFactoryCommitVerifier{
		CatalogFactory: &verifierCatalogFactory{err: api.NewError(api.ErrCatalogUnavailable, "catalog unavailable", nil)},
	}).VerifyCommittedMaintenance(context.Background(), req, maintenanceCommitPlan(), api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Contains(t, err.Error(), string(api.ErrCatalogUnavailable))
}

type verifierCatalogFactory struct {
	catalog model.Catalog
	client  api.CatalogClient
	err     error
}

func (f *verifierCatalogFactory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	f.catalog = catalog
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}
