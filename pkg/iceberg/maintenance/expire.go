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
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

const (
	expireOptionOlderThan  = "older_than"
	expireOptionRetainLast = "retain_last"

	tablePropertyMinSnapshotsToKeep = "history.expire.min-snapshots-to-keep"
	tablePropertyMaxSnapshotAgeMS   = "history.expire.max-snapshot-age-ms"
)

type MaintenanceTableMetadataLoader interface {
	LoadMaintenanceTableMetadata(ctx context.Context, req Request) (*api.TableMetadata, error)
}

type MaintenanceTableMetadataLoaderFunc func(ctx context.Context, req Request) (*api.TableMetadata, error)

func (f MaintenanceTableMetadataLoaderFunc) LoadMaintenanceTableMetadata(ctx context.Context, req Request) (*api.TableMetadata, error) {
	return f(ctx, req)
}

type ExpireSnapshotsPlanner struct {
	Catalog           api.CatalogRequest
	Loader            MaintenanceTableMetadataLoader
	Now               func() time.Time
	DefaultRetainLast int
}

func (p ExpireSnapshotsPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationExpireSnapshots {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg expire-snapshots planner only supports expire_snapshots", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots planner requires a metadata loader", nil)
	}
	meta, err := p.Loader.LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return nil, err
	}
	if meta == nil || len(meta.Snapshots) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg expire-snapshots planner requires table snapshots", map[string]string{"table": req.Table})
	}
	currentID, err := currentSnapshotID(meta)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	olderThanMS, retainLast, err := expireOptions(req.Options, meta.Properties, now, p.DefaultRetainLast)
	if err != nil {
		return nil, err
	}
	expired := selectExpiredSnapshots(meta, olderThanMS, retainLast)
	if len(expired) == 0 {
		return &CommitPlan{
			NoOp:             true,
			NoOpSnapshotID:   currentID,
			RemovedFileCount: 0,
		}, nil
	}
	baseSnapshotID, err := maintenanceBaseSnapshotID(req.SnapshotBefore, currentID)
	if err != nil {
		return nil, err
	}
	updates := make([]api.CommitUpdate, 0, len(expired)+1)
	orphanPaths := make([]string, 0, len(expired))
	for _, snapshot := range expired {
		payload := map[string]string{
			"snapshot_id": strconv.FormatInt(snapshot.SnapshotID, 10),
			"expired_at":  strconv.FormatInt(now.UnixMilli(), 10),
		}
		if strings.TrimSpace(snapshot.ManifestList) != "" {
			payload["manifest_list"] = snapshot.ManifestList
			orphanPaths = append(orphanPaths, snapshot.ManifestList)
		}
		updates = append(updates, api.CommitUpdate{
			Type:     "remove-snapshot",
			FilePath: snapshot.ManifestList,
			Payload:  payload,
		})
	}
	summary := map[string]string{
		"operation":         string(OperationExpireSnapshots),
		"engine":            "matrixone",
		"idempotency-key":   firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"expired-snapshots": strconv.Itoa(len(expired)),
		"older-than-ms":     strconv.FormatInt(olderThanMS, 10),
		"retain-last":       strconv.Itoa(retainLast),
	}
	updates = append(updates, api.CommitUpdate{Type: "set-snapshot-summary", Payload: cloneStringMap(summary)})
	targetRef := firstNonEmptyString(req.TargetRef, "main")
	return &CommitPlan{
		Catalog: p.Catalog,
		Attempt: &api.CommitAttempt{
			Requirements: []api.CommitRequirement{{
				Type:       "assert-ref-snapshot-id",
				Ref:        targetRef,
				SnapshotID: baseSnapshotID,
			}},
			Updates:        updates,
			Summary:        summary,
			IdempotencyKey: firstNonEmptyString(req.IdempotencyKey, req.JobID),
			BaseSnapshotID: baseSnapshotID,
			TargetRef:      targetRef,
		},
		PostCommitOrphans: dedupeNonEmptyStrings(orphanPaths),
		RemovedFileCount:  uint64(len(expired)),
	}, nil
}

func expireOptions(options, properties map[string]string, now time.Time, defaultRetainLast int) (int64, int, error) {
	retainLast := defaultRetainLast
	if retainLast <= 0 {
		retainLast = 1
	}
	if propertyRetain, ok, err := intOption(properties, tablePropertyMinSnapshotsToKeep); err != nil {
		return 0, 0, err
	} else if ok && propertyRetain > retainLast {
		retainLast = propertyRetain
	}
	if optionRetain, ok, err := intOption(options, expireOptionRetainLast); err != nil {
		return 0, 0, err
	} else if ok {
		if optionRetain < retainLast {
			return 0, 0, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots retain_last is lower than table retention policy", map[string]string{
				"retain_last": strconv.Itoa(optionRetain),
				"min":         strconv.Itoa(retainLast),
			})
		}
		retainLast = optionRetain
	}
	if raw := strings.TrimSpace(options[expireOptionOlderThan]); raw != "" {
		olderThan, err := parseExpireOlderThan(raw)
		if err != nil {
			return 0, 0, err
		}
		return olderThan.UnixMilli(), retainLast, nil
	}
	if maxAgeMS, ok, err := int64Option(properties, tablePropertyMaxSnapshotAgeMS); err != nil {
		return 0, 0, err
	} else if ok && maxAgeMS > 0 {
		return now.Add(-time.Duration(maxAgeMS) * time.Millisecond).UnixMilli(), retainLast, nil
	}
	return 0, 0, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots requires older_than or table max snapshot age property", nil)
}

func parseExpireOlderThan(raw string) (time.Time, error) {
	raw = strings.Trim(strings.TrimSpace(raw), `'"`)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02"} {
		if ts, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return ts.UTC(), nil
		}
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	return time.Time{}, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots older_than is invalid", map[string]string{"older_than": raw})
}

func selectExpiredSnapshots(meta *api.TableMetadata, olderThanMS int64, retainLast int) []api.Snapshot {
	protected := protectedSnapshots(meta, retainLast)
	expired := make([]api.Snapshot, 0)
	for _, snapshot := range meta.Snapshots {
		if snapshot.SnapshotID == 0 || snapshot.TimestampMS >= olderThanMS {
			continue
		}
		if protected[snapshot.SnapshotID] {
			continue
		}
		expired = append(expired, snapshot)
	}
	return expired
}

func protectedSnapshots(meta *api.TableMetadata, retainLast int) map[int64]bool {
	protected := make(map[int64]bool)
	if meta == nil {
		return protected
	}
	if meta.CurrentSnapshotID != nil {
		protected[*meta.CurrentSnapshotID] = true
	}
	byID := make(map[int64]api.Snapshot, len(meta.Snapshots))
	for _, snapshot := range meta.Snapshots {
		byID[snapshot.SnapshotID] = snapshot
	}
	for _, ref := range meta.Refs {
		if ref.SnapshotID != 0 {
			protected[ref.SnapshotID] = true
			limit := ref.MinSnapshotsToKeep
			if limit < 1 {
				limit = 1
			}
			protectSnapshotLineage(protected, byID, ref.SnapshotID, limit)
		}
	}
	if retainLast > 0 {
		newest := append([]api.Snapshot(nil), meta.Snapshots...)
		sortSnapshotsNewestFirst(newest)
		for i, snapshot := range newest {
			if i >= retainLast {
				break
			}
			protected[snapshot.SnapshotID] = true
		}
	}
	return protected
}

func protectSnapshotLineage(protected map[int64]bool, byID map[int64]api.Snapshot, snapshotID int64, limit int) {
	for i := 0; i < limit && snapshotID != 0; i++ {
		snapshot, ok := byID[snapshotID]
		if !ok {
			return
		}
		protected[snapshotID] = true
		if snapshot.ParentSnapshotID == nil {
			return
		}
		snapshotID = *snapshot.ParentSnapshotID
	}
}

func sortSnapshotsNewestFirst(snapshots []api.Snapshot) {
	for i := 1; i < len(snapshots); i++ {
		for j := i; j > 0 && snapshots[j-1].TimestampMS < snapshots[j].TimestampMS; j-- {
			snapshots[j-1], snapshots[j] = snapshots[j], snapshots[j-1]
		}
	}
}

func currentSnapshotID(meta *api.TableMetadata) (int64, error) {
	if !metadata.HasCurrentSnapshot(meta) {
		return 0, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance requires a current snapshot", nil)
	}
	return *meta.CurrentSnapshotID, nil
}

func maintenanceBaseSnapshotID(snapshotBefore string, currentID int64) (int64, error) {
	raw := strings.TrimSpace(snapshotBefore)
	if raw == "" {
		return currentID, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance snapshot_before must be a positive snapshot id", map[string]string{"snapshot_before": raw})
	}
	return value, nil
}

func intOption(options map[string]string, key string) (int, bool, error) {
	value, ok, err := int64Option(options, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if value < 0 || value > int64(^uint(0)>>1) {
		return 0, true, api.NewError(api.ErrConfigInvalid, "Iceberg integer option is out of range", map[string]string{"option": key})
	}
	return int(value), true, nil
}

func int64Option(options map[string]string, key string) (int64, bool, error) {
	raw := strings.TrimSpace(options[key])
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, true, api.NewError(api.ErrConfigInvalid, "Iceberg integer option must be non-negative", map[string]string{"option": key, "value": raw})
	}
	return value, true, nil
}

func dedupeNonEmptyStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
