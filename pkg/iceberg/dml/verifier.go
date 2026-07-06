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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type CatalogCommitVerifier struct {
	Client  api.CatalogClient
	Catalog api.CatalogRequest
}

func (v CatalogCommitVerifier) VerifyDMLCommit(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error) {
	if v.Client == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg DML commit verifier requires a catalog client", map[string]string{
			"table": req.Stream.Base.Table,
		})
	}
	if result == nil || result.SnapshotID <= 0 {
		return result, false, nil
	}
	catalogReq := req.Catalog
	if catalogReq.Catalog.CatalogID == 0 && catalogReq.Catalog.Name == "" {
		catalogReq = v.Catalog
	}
	targetRef := dmlTargetRef(req, materialized)
	resp, err := v.Client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: catalogReq,
		Namespace:      append(api.Namespace(nil), req.Stream.Base.Namespace...),
		Table:          req.Stream.Base.Table,
		Snapshots:      targetRef,
	})
	if err != nil {
		return result, false, err
	}
	if resp == nil || len(resp.MetadataJSON) == 0 {
		return result, false, api.NewError(api.ErrMetadataInvalid, "Iceberg DML commit verifier did not receive table metadata", map[string]string{
			"table": req.Stream.Base.Table,
		})
	}
	meta, err := metadata.ParseTableMetadata(resp.MetadataJSON, resp.MetadataLocation)
	if err != nil {
		return result, false, err
	}
	snapshot, err := dmlTargetSnapshot(meta, targetRef)
	if err != nil {
		return result, false, err
	}
	if snapshot.SnapshotID != result.SnapshotID {
		return result, false, nil
	}
	if strings.TrimSpace(result.MetadataLocationHash) != "" && meta.MetadataLocationHash != result.MetadataLocationHash {
		return result, false, nil
	}
	verified := *result
	verified.Verified = true
	return &verified, true, nil
}

func dmlTargetRef(req CommitWorkflowRequest, materialized *ManifestMaterializeResult) string {
	if materialized != nil && materialized.Attempt != nil {
		if ref := strings.TrimSpace(materialized.Attempt.TargetRef); ref != "" {
			return ref
		}
	}
	if ref := strings.TrimSpace(req.Stream.Base.TargetRef); ref != "" {
		return ref
	}
	return "main"
}

func dmlTargetSnapshot(meta *api.TableMetadata, targetRef string) (api.Snapshot, error) {
	if meta == nil || len(meta.Snapshots) == 0 {
		return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML commit verifier requires table snapshots", nil)
	}
	ref := strings.TrimSpace(targetRef)
	if ref == "" {
		ref = "main"
	}
	if meta.Refs != nil {
		if snapshotRef, ok := meta.Refs[ref]; ok && snapshotRef.SnapshotID != 0 {
			return dmlSnapshotByID(meta, snapshotRef.SnapshotID)
		}
	}
	if ref == "main" && metadata.HasCurrentSnapshot(meta) {
		return dmlSnapshotByID(meta, *meta.CurrentSnapshotID)
	}
	return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML commit verifier target ref is not present in table metadata", map[string]string{
		"ref": ref,
	})
}

func dmlSnapshotByID(meta *api.TableMetadata, snapshotID int64) (api.Snapshot, error) {
	for _, snapshot := range meta.Snapshots {
		if snapshot.SnapshotID == snapshotID {
			return snapshot, nil
		}
	}
	return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML commit verifier target snapshot is not present in table metadata", map[string]string{
		"snapshot_id": strconv.FormatInt(snapshotID, 10),
	})
}

var _ CommitVerifier = CatalogCommitVerifier{}
