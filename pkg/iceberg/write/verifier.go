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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

// CatalogCommitVerifier resolves an unknown append result from the immutable
// commit attempt. REST failures intentionally carry no snapshot ID because the
// response may have been lost before it was decoded; using the response as the
// expected identity would therefore make the recovery path impossible.
type CatalogCommitVerifier struct {
	Client api.CatalogClient
}

func (v CatalogCommitVerifier) VerifyCommit(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, result *api.CommitResult) (*api.CommitResult, bool, error) {
	if v.Client == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg append commit verifier requires a catalog client", map[string]string{"table": req.Table})
	}
	expected, ok := appendExpectedSnapshot(attempt)
	if !ok {
		return result, false, api.NewError(api.ErrMetadataInvalid, "Iceberg append commit verifier has no expected snapshot", map[string]string{"table": req.Table})
	}
	if result != nil && result.SnapshotID > 0 && result.SnapshotID != expected.SnapshotID {
		return result, false, nil
	}
	targetRef := strings.TrimSpace(attempt.TargetRef)
	if targetRef == "" {
		targetRef = strings.TrimSpace(req.TargetRef)
	}
	if targetRef == "" {
		targetRef = "main"
	}
	resp, err := v.Client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: req.CatalogRequest,
		Namespace:      append(api.Namespace(nil), req.Namespace...),
		Table:          req.Table,
		Snapshots:      targetRef,
	})
	if err != nil {
		return result, false, err
	}
	if resp == nil || len(resp.MetadataJSON) == 0 {
		return result, false, api.NewError(api.ErrMetadataInvalid, "Iceberg append commit verifier did not receive table metadata", map[string]string{"table": req.Table})
	}
	meta, err := metadata.ParseTableMetadata(resp.MetadataJSON, resp.MetadataLocation)
	if err != nil {
		return result, false, err
	}
	snapshot, err := appendTargetSnapshot(meta, targetRef)
	if err != nil {
		return result, false, err
	}
	if snapshot.SnapshotID != expected.SnapshotID || strings.TrimSpace(snapshot.ManifestList) != strings.TrimSpace(expected.ManifestList) {
		return result, false, nil
	}
	if key := strings.TrimSpace(attempt.IdempotencyKey); key != "" && strings.TrimSpace(snapshot.Summary["idempotency-key"]) != key {
		return result, false, nil
	}
	if result != nil && strings.TrimSpace(result.MetadataLocationHash) != "" && result.MetadataLocationHash != meta.MetadataLocationHash {
		return result, false, nil
	}
	verified := api.CommitResult{}
	if result != nil {
		verified = *result
	}
	verified.SnapshotID = expected.SnapshotID
	verified.MetadataLocation = meta.MetadataLocation
	verified.MetadataLocationHash = meta.MetadataLocationHash
	verified.Unknown = false
	verified.Verified = true
	return &verified, true, nil
}

func appendExpectedSnapshot(attempt *api.CommitAttempt) (api.Snapshot, bool) {
	if attempt == nil {
		return api.Snapshot{}, false
	}
	for _, update := range attempt.Updates {
		if update.Type == "add-snapshot" && update.Snapshot != nil && update.Snapshot.SnapshotID > 0 {
			return *update.Snapshot, true
		}
	}
	return api.Snapshot{}, false
}

func appendTargetSnapshot(meta *api.TableMetadata, targetRef string) (api.Snapshot, error) {
	if meta == nil {
		return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg append commit verifier requires table metadata", nil)
	}
	if ref, ok := meta.Refs[targetRef]; ok && ref.SnapshotID > 0 {
		for _, snapshot := range meta.Snapshots {
			if snapshot.SnapshotID == ref.SnapshotID {
				return snapshot, nil
			}
		}
	}
	if targetRef == "main" && meta.CurrentSnapshotID != nil {
		for _, snapshot := range meta.Snapshots {
			if snapshot.SnapshotID == *meta.CurrentSnapshotID {
				return snapshot, nil
			}
		}
	}
	return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg append commit verifier target snapshot is not present in table metadata", map[string]string{
		"ref": targetRef,
		"expected_snapshot_id": strconv.FormatInt(func() int64 {
			if ref, ok := meta.Refs[targetRef]; ok {
				return ref.SnapshotID
			}
			return 0
		}(), 10),
	})
}

var _ CommitVerifier = CatalogCommitVerifier{}
