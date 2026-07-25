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

var rewriteDataFilesControlOptions = map[string]struct{}{
	"ref":             {},
	"ref_type":        {},
	"allow_tag_move":  {},
	"snapshot_before": {},
	"idempotency_key": {},
	"job_id":          {},
}

type RewriteDataFilesPlanner struct {
	Catalog api.CatalogRequest
	Loader  MaintenanceTableMetadataLoader
}

func (p RewriteDataFilesPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationRewriteDataFiles {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg rewrite-data-files planner only supports rewrite_data_files", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files planner requires a metadata loader", nil)
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
	payload := rewriteDataFilesPayload(req.Options, snapshot)
	summary := map[string]string{
		"operation":       string(OperationRewriteDataFiles),
		"engine":          "matrixone",
		"idempotency-key": firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"base-snapshot":   strconv.FormatInt(snapshot.SnapshotID, 10),
	}
	updates := []api.CommitUpdate{
		{
			Type:     "rewrite-data-files",
			FilePath: snapshot.ManifestList,
			Payload:  payload,
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
	}, nil
}

func rewriteDataFilesPayload(options map[string]string, snapshot api.Snapshot) map[string]string {
	payload := map[string]string{
		"snapshot_id":   strconv.FormatInt(snapshot.SnapshotID, 10),
		"manifest_list": snapshot.ManifestList,
	}
	for key, value := range options {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if normalizedKey == "" {
			continue
		}
		if _, skip := rewriteDataFilesControlOptions[normalizedKey]; skip {
			continue
		}
		if strings.TrimSpace(value) == "" {
			continue
		}
		payload[normalizedKey] = strings.TrimSpace(value)
	}
	return payload
}
