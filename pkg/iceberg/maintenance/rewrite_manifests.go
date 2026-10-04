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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type RewriteManifestsPlanner struct {
	Catalog api.CatalogRequest
	Loader  MaintenanceTableMetadataLoader
}

func (p RewriteManifestsPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationRewriteManifests {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg rewrite-manifests planner only supports rewrite_manifests", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests planner requires a metadata loader", nil)
	}
	meta, err := p.Loader.LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return nil, err
	}
	snapshot, err := maintenanceTargetSnapshot(meta, req.TargetRef, req.SnapshotBefore)
	if err != nil {
		return nil, err
	}
	targetRef := firstNonEmptyString(req.TargetRef, "main")
	summary := map[string]string{
		"operation":       string(OperationRewriteManifests),
		"engine":          "matrixone",
		"idempotency-key": firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"base-snapshot":   strconv.FormatInt(snapshot.SnapshotID, 10),
	}
	updates := []api.CommitUpdate{
		{
			Type:     "rewrite-manifests",
			FilePath: snapshot.ManifestList,
			Payload: map[string]string{
				"snapshot_id":   strconv.FormatInt(snapshot.SnapshotID, 10),
				"manifest_list": snapshot.ManifestList,
			},
		},
		{Type: "set-snapshot-summary", Payload: cloneStringMap(summary)},
	}
	return &CommitPlan{
		Catalog: p.Catalog,
		Attempt: &api.CommitAttempt{
			Requirements: []api.CommitRequirement{{
				Type:       "assert-ref-snapshot-id",
				Ref:        targetRef,
				SnapshotID: snapshot.SnapshotID,
			}},
			Updates:        updates,
			Summary:        summary,
			IdempotencyKey: firstNonEmptyString(req.IdempotencyKey, req.JobID),
			BaseSnapshotID: snapshot.SnapshotID,
			TargetRef:      targetRef,
		},
		RewrittenFileCount: 0,
	}, nil
}

func maintenanceTargetSnapshot(meta *api.TableMetadata, targetRef, snapshotBefore string) (api.Snapshot, error) {
	if meta == nil || len(meta.Snapshots) == 0 {
		return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance requires table snapshots", nil)
	}
	if raw := strings.TrimSpace(snapshotBefore); raw != "" {
		snapshotID, err := maintenanceBaseSnapshotID(raw, 0)
		if err != nil {
			return api.Snapshot{}, err
		}
		return snapshotByID(meta, snapshotID)
	}
	ref := strings.TrimSpace(targetRef)
	if ref == "" {
		ref = "main"
	}
	if meta.Refs != nil {
		if snapshotRef, ok := meta.Refs[ref]; ok && snapshotRef.SnapshotID != 0 {
			return snapshotByID(meta, snapshotRef.SnapshotID)
		}
	}
	currentID, err := currentSnapshotID(meta)
	if err != nil {
		return api.Snapshot{}, err
	}
	if ref == "main" {
		return snapshotByID(meta, currentID)
	}
	return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance target ref is not present in table metadata", map[string]string{
		"ref": ref,
	})
}

func snapshotByID(meta *api.TableMetadata, snapshotID int64) (api.Snapshot, error) {
	for _, snapshot := range meta.Snapshots {
		if snapshot.SnapshotID == snapshotID {
			if strings.TrimSpace(snapshot.ManifestList) == "" {
				return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance target snapshot is missing manifest list", map[string]string{
					"snapshot_id": strconv.FormatInt(snapshotID, 10),
				})
			}
			return snapshot, nil
		}
	}
	return api.Snapshot{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance target snapshot is not present in table metadata", map[string]string{
		"snapshot_id": strconv.FormatInt(snapshotID, 10),
	})
}
