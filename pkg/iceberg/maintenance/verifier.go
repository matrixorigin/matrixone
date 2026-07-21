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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type CatalogCommitVerifier struct {
	Client  api.CatalogClient
	Catalog api.CatalogRequest
}

type CatalogFactoryCommitVerifier struct {
	CatalogFactory CatalogClientFactory
}

func (v CatalogFactoryCommitVerifier) VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
	if v.CatalogFactory == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := v.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return result, false, err
	}
	return (CatalogCommitVerifier{
		Client: client,
		Catalog: api.CatalogRequest{
			Catalog:           req.Catalog,
			ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
		},
	}).VerifyCommittedMaintenance(ctx, req, plan, result)
}

func (v CatalogCommitVerifier) VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
	if v.Client == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires a catalog client", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if plan == nil || plan.Attempt == nil {
		return result, false, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit verifier requires a commit attempt", map[string]string{
			"operation": string(req.Operation),
		})
	}
	meta, err := (CatalogMetadataLoader(v)).LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return result, false, err
	}
	expected, hasAddedSnapshot := maintenanceExpectedSnapshot(plan.Attempt)
	snapshot, err := maintenanceTargetSnapshot(meta, firstNonEmptyString(req.TargetRef, plan.Attempt.TargetRef), "")
	if err != nil {
		return result, false, err
	}
	expectedSnapshotID := plan.Attempt.BaseSnapshotID
	if hasAddedSnapshot {
		expectedSnapshotID = expected.SnapshotID
	} else if result.SnapshotID > 0 {
		expectedSnapshotID = result.SnapshotID
	} else if maintenanceHasSnapshotSummaryUpdate(plan.Attempt.Updates) {
		// Server-side rewrite extensions allocate their result snapshot inside
		// the catalog, so the attempt cannot predict its id. The idempotency and
		// operation summary checked below is the immutable result identity.
		expectedSnapshotID = snapshot.SnapshotID
	}
	if result.SnapshotID > 0 && result.SnapshotID != expectedSnapshotID {
		return result, false, nil
	}
	if snapshot.SnapshotID != expectedSnapshotID {
		return result, false, nil
	}
	if hasAddedSnapshot && expected.ManifestList != "" && strings.TrimSpace(snapshot.ManifestList) != strings.TrimSpace(expected.ManifestList) {
		return result, false, nil
	}
	if key := strings.TrimSpace(plan.Attempt.IdempotencyKey); hasAddedSnapshot && key != "" && strings.TrimSpace(snapshot.Summary["idempotency-key"]) != key {
		return result, false, nil
	}
	if !maintenanceUpdatesApplied(meta, snapshot, plan.Attempt.Updates) {
		return result, false, nil
	}
	if strings.TrimSpace(result.MetadataLocationHash) != "" && meta.MetadataLocationHash != result.MetadataLocationHash {
		return result, false, nil
	}
	result.SnapshotID = expectedSnapshotID
	result.MetadataLocation = meta.MetadataLocation
	result.MetadataLocationHash = meta.MetadataLocationHash
	result.Unknown = false
	result.Verified = true
	return result, true, nil
}

func maintenanceExpectedSnapshot(attempt *api.CommitAttempt) (api.Snapshot, bool) {
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

func maintenanceHasSnapshotSummaryUpdate(updates []api.CommitUpdate) bool {
	for _, update := range updates {
		if update.Type == "set-snapshot-summary" && len(update.Payload) > 0 {
			return true
		}
	}
	return false
}

func maintenanceUpdatesApplied(meta *api.TableMetadata, target api.Snapshot, updates []api.CommitUpdate) bool {
	if meta == nil {
		return false
	}
	hasSummaryProof := false
	for _, update := range updates {
		switch update.Type {
		case "add-snapshot":
			if update.Snapshot == nil || update.Snapshot.SnapshotID != target.SnapshotID {
				return false
			}
		case "set-snapshot-ref":
			ref, ok := meta.Refs[update.Ref]
			if !ok || ref.SnapshotID != update.SnapshotID {
				return false
			}
		case "remove-snapshot-ref":
			if _, exists := meta.Refs[update.Ref]; exists {
				return false
			}
		case "remove-snapshots":
			for _, snapshotID := range update.SnapshotIDs {
				for _, snapshot := range meta.Snapshots {
					if snapshot.SnapshotID == snapshotID {
						return false
					}
				}
			}
		case "set-snapshot-summary":
			if len(update.Payload) == 0 {
				return false
			}
			for key, value := range update.Payload {
				if target.Summary[key] != value {
					return false
				}
			}
			hasSummaryProof = true
		case "rewrite-manifests", "rewrite-data-files":
			// These extension actions choose their output paths/snapshot ids inside
			// the catalog. Require the set-snapshot-summary action from the same
			// attempt to prove the idempotent result instead of accepting an
			// unchanged base snapshot.
			if !maintenanceHasSnapshotSummaryUpdate(updates) {
				return false
			}
		default:
			return false
		}
	}
	if maintenanceHasSnapshotSummaryUpdate(updates) && !hasSummaryProof {
		return false
	}
	return true
}

var _ CommitResultVerifier = CatalogCommitVerifier{}
var _ CommitResultVerifier = CatalogFactoryCommitVerifier{}
