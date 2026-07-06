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

package metadata

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const localPlanningMode = "client-side"

type LocalScanPlanner struct {
	Catalog                 api.CatalogClient
	Metadata                api.MetadataFacade
	ObjectReader            api.ObjectReader
	Cache                   *Cache
	RefCacheRefresher       RefCacheRefresher
	CredentialHash          string
	CredentialScope         string
	ManifestReadParallelism int
	MaxManifestFiles        int
	MaxDataFiles            int
	PlanningTimeout         time.Duration
	ServerPlanningMode      api.ServerPlanningMode
}

func (p LocalScanPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	if p.Catalog == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg local scan planner requires catalog client", nil)
	}
	if p.Metadata == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg local scan planner requires metadata facade", nil)
	}
	if p.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg local scan planner requires object reader", nil)
	}
	cfg, err := p.normalizedConfig(req)
	if err != nil {
		return nil, err
	}
	if cfg.serverPlanningMode == api.ServerPlanningRequired {
		return nil, api.NewError(api.ErrServerPlanningRequired, "Iceberg server-side planning is required by configuration", nil)
	}

	planningCtx, cancel := api.WithPlanningTimeout(ctx, cfg.planningTimeout)
	defer cancel()

	selector := normalizeSnapshotSelector(req)
	tableReq := api.LoadTableRequest{
		CatalogRequest: req.CatalogRequest,
		Namespace:      req.Namespace,
		Table:          req.Table,
		Snapshots:      loadTableSnapshots(selector),
	}
	loaded, err := CachedTableMetadataLoader{
		Catalog:          p.Catalog,
		Metadata:         p.Metadata,
		ObjectReader:     p.ObjectReader,
		Cache:            p.Cache,
		CredentialHash:   p.CredentialHash,
		ExternalRef:      selector.RefName,
		SnapshotSelector: selector,
	}.Load(planningCtx, tableReq)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}
	p.refreshRefCache(planningCtx, req, loaded.Metadata)
	profile := api.PlanningProfile{
		MetadataBytes: loaded.MetadataBytes,
		PlanningMode:  localPlanningMode,
	}
	if loaded.CacheHit {
		profile.PlanningCacheHits++
	} else {
		profile.PlanningCacheMiss++
	}

	snapshot, err := p.Metadata.ResolveSnapshot(planningCtx, loaded.Metadata, selector)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}
	if strings.TrimSpace(snapshot.ManifestList) == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg snapshot is missing manifest-list", map[string]string{
			"snapshot_id": strconv.FormatInt(snapshot.SnapshotID, 10),
		})
	}
	schema, schemaID, err := snapshotSchema(loaded.Metadata, snapshot)
	if err != nil {
		return nil, err
	}
	pruner := newScanPruner(loaded.Metadata, schema, req.PrunePredicates)
	if features, err := p.Metadata.DetectUnsupportedP0(planningCtx, loaded.Metadata, nil, nil); err != nil {
		return nil, planContextError(planningCtx, err)
	} else if len(features) > 0 {
		return nil, unsupportedFeaturesError(features)
	}

	baseKey := loaded.MetadataCacheKey
	baseKey.Kind = ""
	manifestReader := CachedManifestReader{
		Metadata:       p.Metadata,
		ObjectReader:   p.ObjectReader,
		Cache:          p.Cache,
		BaseKey:        baseKey,
		CredentialHash: p.CredentialHash,
	}
	manifestListRead, err := manifestReader.ReadManifestListWithStats(planningCtx, snapshot.ManifestList)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}
	profile.ManifestListBytes = manifestListRead.SizeBytes
	if manifestListRead.CacheHit {
		profile.PlanningCacheHits++
	} else {
		profile.PlanningCacheMiss++
	}
	if len(manifestListRead.Manifests) > cfg.maxManifestFiles {
		return nil, planningLimitExceeded("manifest_files", len(manifestListRead.Manifests), cfg.maxManifestFiles, cfg.serverPlanningMode)
	}

	selectedManifests, selectedDeleteManifests, prunedManifests, err := selectScanManifests(planningCtx, manifestListRead.Manifests, pruner, req.EnableDeleteApply)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}
	profile.ManifestsSelected = len(selectedManifests)
	profile.ManifestsPruned = prunedManifests
	if len(selectedManifests) > cfg.maxManifestFiles {
		return nil, planningLimitExceeded("selected_manifest_files", len(selectedManifests), cfg.maxManifestFiles, cfg.serverPlanningMode)
	}

	manifestsToRead := append([]api.ManifestFile(nil), selectedManifests...)
	manifestsToRead = append(manifestsToRead, selectedDeleteManifests...)
	manifestReads, err := p.readManifests(planningCtx, manifestReader, manifestsToRead, cfg.manifestReadParallelism)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}

	var files []api.DataFile
	dataTasks := make([]api.DataFileTask, 0)
	deleteEntries := make([]deleteManifestEntry, 0)
	for _, read := range manifestReads {
		profile.ManifestBytes += read.SizeBytes
		if read.CacheHit {
			profile.PlanningCacheHits++
		} else {
			profile.PlanningCacheMiss++
		}
		manifestPath := read.Manifest.Path
		for _, entry := range read.Entries {
			if err := checkPlanningContext(planningCtx); err != nil {
				return nil, err
			}
			if entry.Status == api.ManifestEntryDeleted {
				profile.DataFilesPruned++
				profile.DataFileBytesPruned += positiveFileSize(entry.DataFile)
				continue
			}
			file := normalizeDataFile(entry.DataFile)
			file.SequenceNumber = firstNonZeroInt64(file.SequenceNumber, entry.SequenceNumber, read.Manifest.SequenceNumber)
			file.FileSequenceNumber = firstNonZeroInt64(file.FileSequenceNumber, entry.FileSequence)
			file.SpecID = firstNonZeroInt(file.SpecID, read.Manifest.PartitionSpecID)
			if read.Manifest.Content == api.ManifestContentDeletes {
				if !req.EnableDeleteApply {
					return nil, ValidateP0ManifestFile(read.Manifest)
				}
				if err := ValidateP1DeleteFile(file); err != nil {
					return nil, err
				}
				deleteEntries = append(deleteEntries, deleteManifestEntry{
					manifestPath: manifestPath,
					file:         file,
				})
				continue
			}
			if err := ValidateP0DataFile(file); err != nil {
				return nil, err
			}
			if shouldPruneZeroRecordDataFile(file) {
				profile.DataFilesPruned++
				profile.DataFileBytesPruned += positiveFileSize(file)
				continue
			}
			if pruner.shouldPruneDataFile(file) {
				profile.DataFilesPruned++
				profile.DataFileBytesPruned += positiveFileSize(file)
				continue
			}
			files = append(files, file)
			profile.DataFileBytesSelected += positiveFileSize(file)
			if len(files) > cfg.maxDataFiles {
				return nil, planningLimitExceeded("data_files", len(files), cfg.maxDataFiles, cfg.serverPlanningMode)
			}
			dataTasks = append(dataTasks, api.DataFileTask{
				DataFile:        file,
				ManifestPath:    manifestPath,
				ResidualFilter:  residualFilter(req.ResidualSQL),
				CredentialScope: p.CredentialScope,
			})
		}
	}
	profile.DataFilesSelected = len(dataTasks)
	deleteTasks, err := pairDeleteTasks(dataTasks, deleteEntries, p.CredentialScope)
	if err != nil {
		return nil, err
	}
	dataTasks, err = p.applyRowGroupPlanning(planningCtx, loaded.Metadata, schema, req, dataTasks, &profile)
	if err != nil {
		return nil, planContextError(planningCtx, err)
	}
	profile.DeleteFilesSelected = len(deleteEntries)
	if features, err := p.Metadata.DetectUnsupportedP0(planningCtx, loaded.Metadata, selectedManifests, files); err != nil {
		return nil, planContextError(planningCtx, err)
	} else if len(features) > 0 {
		return nil, unsupportedFeaturesError(features)
	}

	columnMapping, err := buildColumnMapping(schema, req.ProjectionIDs)
	if err != nil {
		return nil, err
	}
	columnMapping, err = addHiddenDeleteColumnMappings(columnMapping, schema, deleteTasks)
	if err != nil {
		return nil, err
	}
	partitionSpecIDs := partitionSpecIDs(loaded.Metadata, selectedManifests)

	return &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           snapshot.SnapshotID,
			SchemaID:             schemaID,
			PartitionSpecIDs:     partitionSpecIDs,
			MetadataLocation:     loaded.Metadata.MetadataLocationRed,
			MetadataLocationHash: loaded.Metadata.MetadataLocationHash,
			ManifestList:         api.RedactPath(snapshot.ManifestList),
			ManifestListHash:     api.PathHash(snapshot.ManifestList),
			RefName:              selector.RefName,
			PlanningMode:         localPlanningMode,
		},
		DataTasks:            dataTasks,
		DeleteTasks:          deleteTasks,
		ColumnMapping:        columnMapping,
		ResidualFilter:       residualFilter(req.ResidualSQL),
		Profile:              profile,
		DeleteMaxMemoryBytes: req.DeleteMaxMemoryBytes,
		EnableDeleteSpill:    req.EnableDeleteSpill,
	}, nil
}

type RefCacheRefresher interface {
	RefreshRefCache(ctx context.Context, refs []model.RefCache) error
}

type RefCacheRefresherFunc func(ctx context.Context, refs []model.RefCache) error

func (f RefCacheRefresherFunc) RefreshRefCache(ctx context.Context, refs []model.RefCache) error {
	return f(ctx, refs)
}

func (p LocalScanPlanner) refreshRefCache(ctx context.Context, req api.ScanPlanRequest, meta *api.TableMetadata) {
	if p.RefCacheRefresher == nil || meta == nil || len(meta.Refs) == 0 {
		return
	}
	namespace := strings.Join(req.Namespace, ".")
	if req.Catalog.AccountID == 0 ||
		req.Catalog.CatalogID == 0 ||
		strings.TrimSpace(namespace) == "" ||
		strings.TrimSpace(req.Table) == "" {
		return
	}
	refs := icebergref.RefreshCache(req.Catalog.AccountID, req.Catalog.CatalogID, namespace, req.Table, meta, "catalog", time.Now())
	if len(refs) == 0 {
		return
	}
	if err := p.RefCacheRefresher.RefreshRefCache(ctx, refs); err != nil {
		logutil.Warn("Iceberg ref cache refresh failed",
			zap.Uint32("account-id", req.Catalog.AccountID),
			zap.Uint64("catalog-id", req.Catalog.CatalogID),
			zap.String("namespace", namespace),
			zap.String("table", req.Table),
			zap.Error(err))
	}
}

type localPlannerConfig struct {
	manifestReadParallelism int
	maxManifestFiles        int
	maxDataFiles            int
	planningTimeout         time.Duration
	serverPlanningMode      api.ServerPlanningMode
}

func (p LocalScanPlanner) normalizedConfig(req api.ScanPlanRequest) (localPlannerConfig, error) {
	defaults := api.DefaultConfig().Scan
	cfg := localPlannerConfig{
		manifestReadParallelism: p.ManifestReadParallelism,
		maxManifestFiles:        p.MaxManifestFiles,
		maxDataFiles:            p.MaxDataFiles,
		planningTimeout:         p.PlanningTimeout,
		serverPlanningMode:      p.ServerPlanningMode,
	}
	if cfg.manifestReadParallelism <= 0 {
		cfg.manifestReadParallelism = defaults.ManifestReadParallelism
	}
	if cfg.maxManifestFiles <= 0 {
		cfg.maxManifestFiles = defaults.MaxManifestFiles
	}
	if cfg.maxDataFiles <= 0 {
		cfg.maxDataFiles = defaults.MaxDataFiles
	}
	if cfg.planningTimeout == 0 {
		cfg.planningTimeout = defaults.PlanningTimeout
	}
	if strings.TrimSpace(string(cfg.serverPlanningMode)) == "" {
		cfg.serverPlanningMode = defaults.ServerPlanningMode
	}
	if strings.TrimSpace(req.PlanningTimeout) != "" {
		timeout, err := time.ParseDuration(strings.TrimSpace(req.PlanningTimeout))
		if err != nil {
			return localPlannerConfig{}, api.WrapError(api.ErrConfigInvalid, "Iceberg planning timeout is invalid", map[string]string{
				"planning_timeout": req.PlanningTimeout,
			}, err)
		}
		cfg.planningTimeout = timeout
	}
	return cfg, nil
}

type plannedManifestRead struct {
	Manifest  api.ManifestFile
	Entries   []api.ManifestEntry
	CacheHit  bool
	SizeBytes int64
}

func (p LocalScanPlanner) readManifests(ctx context.Context, reader CachedManifestReader, manifests []api.ManifestFile, parallelism int) ([]plannedManifestRead, error) {
	if len(manifests) == 0 {
		return nil, nil
	}
	if parallelism <= 0 {
		parallelism = api.DefaultConfig().Scan.ManifestReadParallelism
	}
	if parallelism > len(manifests) {
		parallelism = len(manifests)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan plannedManifestReadResult, len(manifests))
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				manifest := normalizeManifestFile(manifests[idx])
				read, err := reader.ReadManifestWithStats(ctx, manifest.Path)
				if err != nil {
					results <- plannedManifestReadResult{Index: idx, Err: err}
					cancel()
					continue
				}
				results <- plannedManifestReadResult{
					Index: idx,
					Read: plannedManifestRead{
						Manifest:  manifest,
						Entries:   read.Entries,
						CacheHit:  read.CacheHit,
						SizeBytes: read.SizeBytes,
					},
				}
			}
		}()
	}
	for idx := range manifests {
		if err := checkPlanningContext(ctx); err != nil {
			cancel()
			close(jobs)
			wg.Wait()
			return nil, err
		}
		select {
		case jobs <- idx:
		case <-ctx.Done():
			cancel()
			close(jobs)
			wg.Wait()
			return nil, planningContextError(ctx)
		}
	}
	close(jobs)
	wg.Wait()
	close(results)

	out := make([]plannedManifestRead, len(manifests))
	for result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		out[result.Index] = result.Read
	}
	return out, nil
}

type plannedManifestReadResult struct {
	Index int
	Read  plannedManifestRead
	Err   error
}

func normalizeSnapshotSelector(req api.ScanPlanRequest) api.SnapshotSelector {
	selector := req.Snapshot
	ref := strings.TrimSpace(selector.RefName)
	if ref == "" {
		ref = strings.TrimSpace(req.Ref)
	}
	if ref != "" {
		selector.RefName = ref
		if ref == model.DefaultRefMain {
			selector.AllowMainFallback = true
		}
	}
	return selector
}

func loadTableSnapshots(selector api.SnapshotSelector) string {
	if selector.HasSnapshotID || selector.HasTimestampMS {
		return "all"
	}
	return selector.RefName
}

func selectScanManifests(ctx context.Context, manifests []api.ManifestFile, pruner scanPruner, enableDeleteApply bool) ([]api.ManifestFile, []api.ManifestFile, int, error) {
	selectedData := make([]api.ManifestFile, 0, len(manifests))
	selectedDeletes := make([]api.ManifestFile, 0)
	pruned := 0
	for _, manifest := range manifests {
		if err := checkPlanningContext(ctx); err != nil {
			return nil, nil, 0, err
		}
		manifest = normalizeManifestFile(manifest)
		if manifest.Content == api.ManifestContentDeletes {
			if !enableDeleteApply {
				if err := ValidateP0ManifestFile(manifest); err != nil {
					return nil, nil, 0, err
				}
			}
			selectedDeletes = append(selectedDeletes, manifest)
			continue
		}
		if err := ValidateP0ManifestFile(manifest); err != nil {
			return nil, nil, 0, err
		}
		if manifest.Content != "" && manifest.Content != api.ManifestContentData {
			pruned++
			continue
		}
		if pruner.shouldPruneManifest(manifest) {
			pruned++
			continue
		}
		selectedData = append(selectedData, manifest)
	}
	return selectedData, selectedDeletes, pruned, nil
}

func snapshotSchema(meta *api.TableMetadata, snapshot api.Snapshot) (api.Schema, int, error) {
	schemaID := meta.CurrentSchemaID
	if snapshot.SchemaID != nil {
		schemaID = *snapshot.SchemaID
	}
	for _, schema := range meta.Schemas {
		if schema.SchemaID == schemaID {
			return schema, schemaID, nil
		}
	}
	return api.Schema{}, 0, api.NewError(api.ErrMetadataInvalid, "Iceberg snapshot schema id was not found", map[string]string{
		"schema_id": strconv.Itoa(schemaID),
	})
}

func buildColumnMapping(schema api.Schema, projectionIDs []int) ([]api.IcebergColumnMapping, error) {
	projected := make(map[int]bool, len(projectionIDs))
	for _, id := range projectionIDs {
		projected[id] = true
	}
	allProjected := len(projected) == 0
	out := make([]api.IcebergColumnMapping, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		moType, err := MapP0TypeToMO(field.Type, field.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, api.IcebergColumnMapping{
			FieldID:        field.ID,
			ColumnName:     field.Name,
			MOType:         moType,
			Required:       field.Required,
			Projected:      allProjected || projected[field.ID],
			ParquetFieldID: field.ID,
		})
	}
	return out, nil
}

func partitionSpecIDs(meta *api.TableMetadata, manifests []api.ManifestFile) []int {
	seen := make(map[int]bool, len(manifests))
	for _, manifest := range manifests {
		seen[manifest.PartitionSpecID] = true
	}
	if len(seen) == 0 {
		seen[meta.DefaultSpecID] = true
	}
	out := make([]int, 0, len(seen))
	for specID := range seen {
		out = append(out, specID)
	}
	sort.Ints(out)
	return out
}

func residualFilter(sql string) api.ResidualFilter {
	expr := strings.TrimSpace(sql)
	return api.ResidualFilter{
		ExpressionSQL: expr,
		AlwaysTrue:    expr == "",
	}
}

func normalizeManifestFile(manifest api.ManifestFile) api.ManifestFile {
	if manifest.ManifestPathRedacted == "" {
		manifest.ManifestPathRedacted = api.RedactPath(manifest.Path)
	}
	if manifest.ManifestPathHash == "" {
		manifest.ManifestPathHash = api.PathHash(manifest.Path)
	}
	return manifest
}

func normalizeDataFile(file api.DataFile) api.DataFile {
	if file.FilePathRedacted == "" {
		file.FilePathRedacted = api.RedactPath(file.FilePath)
	}
	if file.FilePathHash == "" {
		file.FilePathHash = api.PathHash(file.FilePath)
	}
	return file
}

func shouldPruneZeroRecordDataFile(file api.DataFile) bool {
	return file.RecordCount == 0
}

func positiveFileSize(file api.DataFile) int64 {
	if file.FileSizeInBytes > 0 {
		return file.FileSizeInBytes
	}
	return 0
}

func planningLimitExceeded(kind string, observed, limit int, mode api.ServerPlanningMode) error {
	fields := map[string]string{
		"limit_kind": kind,
		"observed":   strconv.Itoa(observed),
		"limit":      strconv.Itoa(limit),
	}
	if mode == api.ServerPlanningRequired {
		return api.NewError(api.ErrServerPlanningRequired, "Iceberg client-side planner reached a planning limit and server planning is required", fields)
	}
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg client-side planner reached a planning limit", fields)
}

func checkPlanningContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return planningContextError(ctx)
	default:
		return nil
	}
}

func planContextError(ctx context.Context, err error) error {
	if ctxErr := checkPlanningContext(ctx); ctxErr != nil {
		return ctxErr
	}
	return err
}

func planningContextError(ctx context.Context) error {
	cause := context.Cause(ctx)
	if cause == nil {
		cause = ctx.Err()
	}
	return api.WrapError(api.ErrPlanningTimeout, "Iceberg scan planning context was cancelled", nil, cause)
}
