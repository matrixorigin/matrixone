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
	"sort"
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
	tablePropertyMaxRefAgeMS        = "history.expire.max-ref-age-ms"
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
	Metadata          api.MetadataFacade
	ObjectReader      api.ObjectReader
	Now               func() time.Time
	DefaultRetainLast int
	PlanningMaxMemory int64
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
	defaultSnapshotAgeMS, defaultRefAgeMS, err := expireRetentionAges(meta.Properties)
	if err != nil {
		return nil, err
	}
	protected, expiredRefs, err := protectedSnapshots(meta, now.UnixMilli(), retainLast, defaultSnapshotAgeMS, defaultRefAgeMS)
	if err != nil {
		return nil, err
	}
	expired := selectExpiredSnapshots(meta, olderThanMS, protected)
	if len(expired) == 0 && len(expiredRefs) == 0 {
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
	updates := make([]api.CommitUpdate, 0, len(expiredRefs)+1)
	for _, refName := range expiredRefs {
		updates = append(updates, api.CommitUpdate{Type: "remove-snapshot-ref", Ref: refName})
	}
	orphanPaths, err := p.expiredSnapshotFiles(ctx, expired)
	if err != nil {
		return nil, err
	}
	snapshotIDs := make([]int64, 0, len(expired))
	for _, snapshot := range expired {
		snapshotIDs = append(snapshotIDs, snapshot.SnapshotID)
	}
	if len(snapshotIDs) > 0 {
		updates = append(updates, api.CommitUpdate{Type: "remove-snapshots", SnapshotIDs: snapshotIDs})
	}
	summary := map[string]string{
		"operation":         string(OperationExpireSnapshots),
		"engine":            "matrixone",
		"idempotency-key":   firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"expired-snapshots": strconv.Itoa(len(expired)),
		"expired-refs":      strconv.Itoa(len(expiredRefs)),
		"older-than-ms":     strconv.FormatInt(olderThanMS, 10),
		"retain-last":       strconv.Itoa(retainLast),
	}
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
		RemovedFileCount:  uint64(len(orphanPaths)),
	}, nil
}

func (p ExpireSnapshotsPlanner) expiredSnapshotFiles(ctx context.Context, snapshots []api.Snapshot) ([]string, error) {
	paths := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if path := strings.TrimSpace(snapshot.ManifestList); path != "" {
			paths = append(paths, path)
		}
	}
	if p.ObjectReader == nil {
		return dedupeNonEmptyStrings(paths), nil
	}
	facade := p.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	seenManifestLists := make(map[string]struct{}, len(snapshots))
	seenManifests := make(map[string]struct{})
	memoryLimit := maintenanceMemoryLimit(p.PlanningMaxMemory)
	var memoryUsed int64
	for _, snapshot := range snapshots {
		manifestListPath := strings.TrimSpace(snapshot.ManifestList)
		if manifestListPath == "" {
			continue
		}
		if _, seen := seenManifestLists[manifestListPath]; seen {
			continue
		}
		seenManifestLists[manifestListPath] = struct{}{}
		manifestListData, err := readMaintenanceMetadataObject(ctx, p.ObjectReader, manifestListPath, memoryLimit-memoryUsed)
		if err != nil {
			if isMaintenancePlanningLimit(err) {
				return nil, err
			}
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg expire-snapshots failed to read manifest list", map[string]string{"manifest_list": api.RedactPath(manifestListPath)}, err)
		}
		manifests, err := readMaintenanceManifestList(
			ctx, facade, manifestListData,
			maintenanceRecordLimit(memoryLimit-memoryUsed, 512),
			memoryLimit-memoryUsed,
		)
		if err != nil {
			return nil, err
		}
		memoryBytes := metadata.ManifestListMemoryWeight(cap(manifestListData), manifests)
		if err := checkMaintenanceMemory(memoryUsed, memoryBytes, memoryLimit); err != nil {
			return nil, err
		}
		memoryUsed += memoryBytes
		for _, manifest := range manifests {
			manifestPath := strings.TrimSpace(manifest.Path)
			if manifestPath == "" {
				continue
			}
			paths = append(paths, manifestPath)
			if _, seen := seenManifests[manifestPath]; seen {
				continue
			}
			seenManifests[manifestPath] = struct{}{}
			manifestData, err := readMaintenanceMetadataObject(ctx, p.ObjectReader, manifestPath, memoryLimit-memoryUsed)
			if err != nil {
				if isMaintenancePlanningLimit(err) {
					return nil, err
				}
				return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg expire-snapshots failed to read manifest", map[string]string{"manifest": api.RedactPath(manifestPath)}, err)
			}
			entries, err := readMaintenanceManifest(
				ctx, facade, manifestData,
				maintenanceRecordLimit(memoryLimit-memoryUsed, 1024),
				memoryLimit-memoryUsed,
			)
			if err != nil {
				return nil, err
			}
			entryBytes := metadata.ManifestEntriesMemoryWeight(cap(manifestData), entries)
			if err := checkMaintenanceMemory(memoryUsed, entryBytes, memoryLimit); err != nil {
				return nil, err
			}
			// Keep a conservative cumulative charge. The output path slice retains
			// strings from decoded entries, so treating the decoded manifest as live
			// avoids a second, fragile per-field accounting scheme.
			memoryUsed += entryBytes
			for _, entry := range entries {
				if filePath := strings.TrimSpace(entry.DataFile.FilePath); filePath != "" {
					paths = append(paths, filePath)
				}
				if dvPath := strings.TrimSpace(entry.DataFile.DeletionVectorPath); dvPath != "" {
					paths = append(paths, dvPath)
				}
			}
		}
	}
	return dedupeNonEmptyStrings(paths), nil
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

func expireRetentionAges(properties map[string]string) (int64, int64, error) {
	maxSnapshotAgeMS, _, err := int64Option(properties, tablePropertyMaxSnapshotAgeMS)
	if err != nil {
		return 0, 0, err
	}
	maxRefAgeMS, _, err := int64Option(properties, tablePropertyMaxRefAgeMS)
	if err != nil {
		return 0, 0, err
	}
	return maxSnapshotAgeMS, maxRefAgeMS, nil
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

func selectExpiredSnapshots(meta *api.TableMetadata, olderThanMS int64, protected map[int64]bool) []api.Snapshot {
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

func protectedSnapshots(meta *api.TableMetadata, nowMS int64, defaultMinSnapshots int, defaultMaxSnapshotAgeMS, defaultMaxRefAgeMS int64) (map[int64]bool, []string, error) {
	protected := make(map[int64]bool)
	if meta == nil {
		return protected, nil, nil
	}
	byID := make(map[int64]api.Snapshot, len(meta.Snapshots))
	for _, snapshot := range meta.Snapshots {
		byID[snapshot.SnapshotID] = snapshot
	}
	expiredRefs := make([]string, 0)
	hasMainRef := false
	for name, ref := range meta.Refs {
		if ref.SnapshotID == 0 {
			continue
		}
		head, ok := byID[ref.SnapshotID]
		if !ok {
			return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg snapshot ref points to an unknown snapshot", map[string]string{
				"ref":         name,
				"snapshot_id": strconv.FormatInt(ref.SnapshotID, 10),
			})
		}
		if name == "main" {
			hasMainRef = true
		}
		maxRefAgeMS := ref.MaxRefAgeMS
		if maxRefAgeMS <= 0 {
			maxRefAgeMS = defaultMaxRefAgeMS
		}
		if name != "main" && snapshotOlderThanAge(head.TimestampMS, nowMS, maxRefAgeMS) {
			expiredRefs = append(expiredRefs, name)
			continue
		}
		protected[ref.SnapshotID] = true
		if name != "main" && strings.EqualFold(strings.TrimSpace(ref.Type), "tag") {
			continue
		}
		minSnapshots := ref.MinSnapshotsToKeep
		if minSnapshots <= 0 {
			minSnapshots = defaultMinSnapshots
		}
		if minSnapshots <= 0 {
			minSnapshots = 1
		}
		maxSnapshotAgeMS := ref.MaxSnapshotAgeMS
		if maxSnapshotAgeMS <= 0 {
			maxSnapshotAgeMS = defaultMaxSnapshotAgeMS
		}
		protectSnapshotLineage(protected, byID, ref.SnapshotID, minSnapshots, nowMS, maxSnapshotAgeMS)
	}
	// Older metadata may omit refs while still carrying current-snapshot-id.
	// Treat it as an implicit main branch, including retain-last/max-age lineage,
	// rather than protecting only the head snapshot.
	if meta.CurrentSnapshotID != nil {
		if !hasMainRef {
			minSnapshots := defaultMinSnapshots
			if minSnapshots <= 0 {
				minSnapshots = 1
			}
			protectSnapshotLineage(protected, byID, *meta.CurrentSnapshotID, minSnapshots, nowMS, defaultMaxSnapshotAgeMS)
		} else {
			protected[*meta.CurrentSnapshotID] = true
		}
	}
	sort.Strings(expiredRefs)
	return protected, expiredRefs, nil
}

func protectSnapshotLineage(protected map[int64]bool, byID map[int64]api.Snapshot, snapshotID int64, minSnapshots int, nowMS, maxSnapshotAgeMS int64) {
	for count := 0; snapshotID != 0; count++ {
		snapshot, ok := byID[snapshotID]
		if !ok {
			return
		}
		withinMinSnapshots := count < minSnapshots
		withinMaxAge := snapshotWithinAge(snapshot.TimestampMS, nowMS, maxSnapshotAgeMS)
		if !withinMinSnapshots && !withinMaxAge {
			return
		}
		protected[snapshotID] = true
		if snapshot.ParentSnapshotID == nil {
			return
		}
		snapshotID = *snapshot.ParentSnapshotID
	}
}

func snapshotOlderThanAge(timestampMS, nowMS, maxAgeMS int64) bool {
	return maxAgeMS > 0 && nowMS > maxAgeMS && timestampMS < nowMS-maxAgeMS
}

func snapshotWithinAge(timestampMS, nowMS, maxAgeMS int64) bool {
	return maxAgeMS > 0 && (nowMS <= maxAgeMS || timestampMS >= nowMS-maxAgeMS)
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
