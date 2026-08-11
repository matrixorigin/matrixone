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

package write

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
)

func TestCatalogCommitVerifierResolvesUnknownFromAttempt(t *testing.T) {
	var loadReq api.LoadTableRequest
	client := &catalog.MockClient{LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		loadReq = req
		return &api.LoadTableResponse{
			Namespace: req.Namespace, TableName: req.Table,
			MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
			MetadataJSON:     appendVerifierMetadataJSON("stmt-append"),
		}, nil
	}}
	req, attempt := appendVerifierInput()

	result, ok, err := (CatalogCommitVerifier{Client: client}).VerifyCommit(context.Background(), req, attempt, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, result.Verified)
	require.False(t, result.Unknown)
	require.Equal(t, int64(4), result.SnapshotID)
	require.Equal(t, "main", loadReq.Snapshots)
}

func TestCatalogCommitVerifierRejectsDifferentIdempotencyIdentity(t *testing.T) {
	client := &catalog.MockClient{LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
		return &api.LoadTableResponse{
			Namespace: req.Namespace, TableName: req.Table,
			MetadataLocation: "s3://warehouse/orders/metadata/v4.json",
			MetadataJSON:     appendVerifierMetadataJSON("another-statement"),
		}, nil
	}}
	req, attempt := appendVerifierInput()

	result, ok, err := (CatalogCommitVerifier{Client: client}).VerifyCommit(context.Background(), req, attempt, &api.CommitResult{Unknown: true})
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, result.Unknown)
}

func appendVerifierInput() (api.AppendRequest, *api.CommitAttempt) {
	snapshot := api.Snapshot{
		SnapshotID:   4,
		ManifestList: "s3://warehouse/orders/metadata/snap-4.avro",
		Summary:      map[string]string{"idempotency-key": "stmt-append"},
	}
	return api.AppendRequest{
			Namespace: api.Namespace{"sales"}, Table: "orders", TargetRef: "main",
			IdempotencyKey: "stmt-append",
		}, &api.CommitAttempt{
			TargetRef: "main", IdempotencyKey: "stmt-append",
			Updates: []api.CommitUpdate{{Type: "add-snapshot", Snapshot: &snapshot}},
		}
}

func appendVerifierMetadataJSON(idempotencyKey string) []byte {
	return []byte(`{
		"format-version": 2,
		"table-uuid": "uuid-1",
		"location": "s3://warehouse/orders",
		"current-schema-id": 1,
		"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"current-snapshot-id": 4,
		"refs": {"main": {"snapshot-id": 4, "type": "branch"}},
		"snapshots": [{"snapshot-id": 4, "timestamp-ms": 1767398400000, "manifest-list": "s3://warehouse/orders/metadata/snap-4.avro", "summary": {"idempotency-key": "` + idempotencyKey + `"}}]
	}`)
}
