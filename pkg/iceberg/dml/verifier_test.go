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

package dml

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestCatalogCommitVerifierChecksTargetRefSnapshot(t *testing.T) {
	var loadReq api.LoadTableRequest
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			loadReq = req
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     dmlVerifierMetadataJSON(),
			}, nil
		},
	}
	req, materialized := dmlVerifierRequest("branch:publish")
	result, ok, err := (CatalogCommitVerifier{Client: client}).VerifyDMLCommit(context.Background(), req, materialized, &api.CommitResult{
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
	require.Equal(t, "publish", loadReq.Snapshots)
}

func TestCatalogCommitVerifierReturnsUnverifiedOnSnapshotMismatch(t *testing.T) {
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace:        req.Namespace,
				TableName:        req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     dmlVerifierMetadataJSON(),
			}, nil
		},
	}
	req, materialized := dmlVerifierRequest("main")
	result, ok, err := (CatalogCommitVerifier{Client: client}).VerifyDMLCommit(context.Background(), req, materialized, &api.CommitResult{
		SnapshotID: 5,
		CommitID:   "commit-5",
	})
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
				MetadataJSON:     dmlVerifierMetadataJSON(),
			}, nil
		},
	}
	req, materialized := dmlVerifierRequest("main")
	result, ok, err := (CatalogCommitVerifier{Client: client}).VerifyDMLCommit(context.Background(), req, materialized, &api.CommitResult{
		SnapshotID:           4,
		CommitID:             "commit-4",
		MetadataLocationHash: "different",
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, result.Verified)
}

func TestCatalogCommitVerifierRejectsMissingClientAndMetadata(t *testing.T) {
	req, materialized := dmlVerifierRequest("main")
	result, ok, err := (CatalogCommitVerifier{}).VerifyDMLCommit(context.Background(), req, materialized, &api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, int64(4), result.SnapshotID)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))

	result, ok, err = (CatalogCommitVerifier{Client: &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return nil, nil
		},
	}}).VerifyDMLCommit(context.Background(), req, materialized, &api.CommitResult{SnapshotID: 4})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, int64(4), result.SnapshotID)
	require.Contains(t, err.Error(), string(api.ErrMetadataInvalid))

	result, ok, err = (CatalogCommitVerifier{Client: &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				Namespace: req.Namespace, TableName: req.Table,
				MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
				MetadataJSON:     dmlVerifierMetadataJSON(),
			}, nil
		},
	}}).VerifyDMLCommit(context.Background(), req, materialized, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Verified)
	require.False(t, result.Unknown)
	require.Equal(t, int64(4), result.SnapshotID)
}

func TestDMLTargetSnapshotFallbacksAndErrors(t *testing.T) {
	current := int64(4)
	meta := &api.TableMetadata{
		CurrentSnapshotID: &current,
		Snapshots: []api.Snapshot{
			{SnapshotID: 3, TimestampMS: 100},
			{SnapshotID: 4, TimestampMS: 200},
		},
	}
	snapshot, err := dmlTargetSnapshot(meta, "")
	require.NoError(t, err)
	require.Equal(t, int64(4), snapshot.SnapshotID)

	meta.Refs = map[string]api.SnapshotRef{"publish": {SnapshotID: 3, Type: "branch"}}
	snapshot, err = dmlTargetSnapshot(meta, "publish")
	require.NoError(t, err)
	require.Equal(t, int64(3), snapshot.SnapshotID)

	_, err = dmlTargetSnapshot(meta, "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "target ref")
	_, err = dmlTargetSnapshot(&api.TableMetadata{
		Refs:      map[string]api.SnapshotRef{"publish": {SnapshotID: 99}},
		Snapshots: []api.Snapshot{{SnapshotID: 3}},
	}, "publish")
	require.Error(t, err)
	require.Contains(t, err.Error(), "target snapshot")
	_, err = dmlTargetSnapshot(nil, "main")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires table snapshots")
}

func TestDMLTargetRefPrefersMaterializedAttemptThenBaseThenMain(t *testing.T) {
	req, materialized := dmlVerifierRequest("branch:base")
	materialized.Attempt.TargetRef = "attempt-ref"
	require.Equal(t, "attempt-ref", dmlTargetRef(req, materialized))
	materialized.Attempt.TargetRef = " "
	require.Equal(t, "branch:base", dmlTargetRef(req, materialized))
	req.Stream.Base.TargetRef = " "
	require.Equal(t, "main", dmlTargetRef(req, nil))
}

func dmlVerifierRequest(ref string) (CommitWorkflowRequest, *ManifestMaterializeResult) {
	stream := ActionStream{
		Operation: OperationOverwrite,
		Base: CommitBase{
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
			TargetRef:      ref,
			BaseSnapshotID: 3,
			IdempotencyKey: "stmt-dml",
		},
		Actions: []Action{{
			Kind: ActionAppendData,
			File: api.DataFile{
				FilePath:        "s3://warehouse/orders/data/part-1.parquet",
				FileFormat:      "parquet",
				RecordCount:     1,
				FileSizeInBytes: 16,
			},
		}},
	}
	intent, err := BuildCommitIntent(stream)
	if err != nil {
		panic(err)
	}
	materialized, err := BuildManifestCommitAttempt(context.Background(), withDMLTestManifestMetadata(ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       4,
		SequenceNumber:   4,
		DataManifestPath: "s3://warehouse/orders/metadata/data-4.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/snap-4.avro",
	}, 0))
	if err != nil {
		panic(err)
	}
	return withDMLTestWorkflowMetadata(CommitWorkflowRequest{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{
			AccountID: 7,
			CatalogID: 42,
			Name:      "ksa_gold",
		}},
		Stream:           stream,
		SnapshotID:       4,
		SequenceNumber:   4,
		DataManifestPath: "s3://warehouse/orders/metadata/data-4.avro",
		ManifestListPath: materialized.AttemptManifestListPath(),
		TableLocation:    "s3://warehouse/orders",
	}, 0), materialized
}

func dmlVerifierMetadataJSON() []byte {
	return []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 4,
		"refs": {
			"main": {"snapshot-id": 4, "type": "branch"},
			"publish": {"snapshot-id": 4, "type": "branch"}
		},
		"snapshots": [
			{"snapshot-id": 3, "timestamp-ms": 1767312000000, "manifest-list": "s3://warehouse/orders/metadata/snap-3.avro"},
			{"snapshot-id": 4, "parent-snapshot-id": 3, "timestamp-ms": 1767398400000, "manifest-list": "s3://warehouse/orders/metadata/snap-4.avro", "summary": {"idempotency-key": "stmt-dml"}}
		]
	}`)
}
