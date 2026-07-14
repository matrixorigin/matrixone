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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

const (
	defaultRewriteDataFilesTargetSizeBytes = 128 * 1024 * 1024
	defaultRewriteDataFilesMinInputFiles   = 2
	defaultRewriteDataFilesMaxGroupFactor  = 4
)

type RewriteDataFilesSelector struct {
	Metadata     api.MetadataFacade
	ObjectReader api.ObjectReader
}

type RewriteDataFilesSelectionRequest struct {
	Snapshot api.Snapshot
	Options  map[string]string
}

type RewriteDataFileCandidate struct {
	ManifestPath string
	Entry        api.ManifestEntry
	File         api.DataFile
}

type RewriteDataFileGroup struct {
	PartitionSpecID int
	PartitionKey    string
	Candidates      []RewriteDataFileCandidate
	TotalSizeBytes  int64
	TotalRecords    int64
}

type RewriteDataFilesSelection struct {
	Groups                 []RewriteDataFileGroup
	PreservedDataManifests []RewriteDataFilesPreservedManifest
	PreservedManifests     []api.ManifestFile
	DeleteManifests        []api.ManifestFile
	ScannedManifestCount   int
	DeleteManifestCount    int
	ScannedFileCount       uint64
	CandidateFileCount     uint64
	CandidateSizeBytes     int64
}

type RewriteDataFileRewrite struct {
	Group            RewriteDataFileGroup
	ReplacementFiles []api.DataFile
}

type RewriteDataFilesPreservedManifest struct {
	Manifest api.ManifestFile
	Entries  []api.ManifestEntry
}

type RewriteDataFilesMaterializeRequest struct {
	Snapshot           api.Snapshot
	FormatVersion      int
	Schema             api.Schema
	PartitionSpecs     []api.PartitionSpec
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	SchemaID           int
	TargetRef          string
	TargetRefType      string
	TargetRefRetention api.SnapshotRef
	IdempotencyKey     string
	DataManifestPath   string
	ManifestListPath   string
	PreservedManifests []api.ManifestFile
	Summary            map[string]string
	Rewrites           []RewriteDataFileRewrite
}

type RewriteDataFilesMaterializeResult struct {
	Entries           []api.ManifestEntry
	ManifestFile      api.ManifestFile
	ManifestFiles     []api.ManifestFile
	ManifestListPath  string
	ManifestBytes     []byte
	ManifestObjects   []ObjectWrite
	ManifestListBytes []byte
	Attempt           *api.CommitAttempt
	RewrittenFiles    uint64
	AddedFiles        uint64
}

type RewriteDataFilesCompactor interface {
	CompactRewriteDataFiles(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error)
}

type RewriteDataFilesDeleteAwareCompactor interface {
	SupportsDeleteManifests() bool
}

type RewriteDataFilesCompactorFunc func(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error)

func (f RewriteDataFilesCompactorFunc) CompactRewriteDataFiles(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error) {
	return f(ctx, req)
}

type RewriteDataFilesCompactRequest struct {
	Metadata       *api.TableMetadata
	Snapshot       api.Snapshot
	Selection      RewriteDataFilesSelection
	JobID          string
	IdempotencyKey string
	Options        map[string]string
}

type RewriteDataFilesCompactResult struct {
	Rewrites    []RewriteDataFileRewrite
	Objects     []ObjectWrite
	OrphanPaths []string
}

type NativeRewriteDataFilesPlanner struct {
	Catalog    api.CatalogRequest
	Loader     MaintenanceTableMetadataLoader
	Now        func() time.Time
	Selector   RewriteDataFilesSelector
	Compactor  RewriteDataFilesCompactor
	PathPrefix string
}

type rewriteDataFilesSelectionOptions struct {
	targetFileSizeBytes uint64
	minInputFiles       uint64
	maxGroupSizeBytes   uint64
}

func (p NativeRewriteDataFilesPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationRewriteDataFiles {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg native rewrite-data-files planner only supports rewrite_data_files", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-data-files planner requires a metadata loader", nil)
	}
	if p.Compactor == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-data-files planner requires a compactor", nil)
	}
	meta, err := p.Loader.LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return nil, err
	}
	schema, err := currentMaintenanceSchema(meta)
	if err != nil {
		return nil, err
	}
	snapshot, err := maintenanceTargetSnapshot(meta, req.TargetRef, req.SnapshotBefore)
	if err != nil {
		return nil, err
	}
	selector := p.Selector
	selection, err := selector.Select(ctx, RewriteDataFilesSelectionRequest{
		Snapshot: snapshot,
		Options:  req.Options,
	})
	if err != nil {
		return nil, err
	}
	if len(selection.Groups) == 0 {
		return &CommitPlan{
			NoOp:               true,
			NoOpSnapshotID:     snapshot.SnapshotID,
			RewrittenFileCount: 0,
		}, nil
	}
	if selection.DeleteManifestCount > 0 && !rewriteDataFilesCompactorSupportsDeletes(p.Compactor) {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg native rewrite-data-files planner requires a delete-aware compactor before rewriting tables with delete manifests", map[string]string{
			"snapshot_id":      strconv.FormatInt(snapshot.SnapshotID, 10),
			"delete_manifests": strconv.Itoa(selection.DeleteManifestCount),
		})
	}
	compacted, err := p.Compactor.CompactRewriteDataFiles(ctx, RewriteDataFilesCompactRequest{
		Metadata:       meta,
		Snapshot:       snapshot,
		Selection:      *selection,
		JobID:          req.JobID,
		IdempotencyKey: req.IdempotencyKey,
		Options:        cloneOptions(req.Options),
	})
	if err != nil {
		return nil, err
	}
	if compacted == nil || len(compacted.Rewrites) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg native rewrite-data-files compactor returned no rewrites", map[string]string{
			"snapshot_id": strconv.FormatInt(snapshot.SnapshotID, 10),
		})
	}
	basePath, err := p.rewriteBasePath(meta, snapshot, req)
	if err != nil {
		return nil, err
	}
	now := maintenanceNow(p.Now)
	newSnapshotID := nextMaintenanceSnapshotID(now, meta)
	dataManifestPath := joinObjectPath(basePath, "data-manifest.avro")
	manifestListPath := joinObjectPath(basePath, "manifest-list.avro")
	preservedDataManifests, preservedObjects, err := materializeRewriteDataFilesPreservedManifests(
		meta,
		basePath,
		selection.PreservedDataManifests,
		rewriteDataFilesSourcePathSet(compacted.Rewrites),
	)
	if err != nil {
		return nil, err
	}
	materialized, err := BuildRewriteDataFilesManifestCommit(RewriteDataFilesMaterializeRequest{
		Snapshot:           snapshot,
		FormatVersion:      meta.FormatVersion,
		Schema:             schema,
		PartitionSpecs:     append([]api.PartitionSpec(nil), meta.PartitionSpecs...),
		SnapshotID:         newSnapshotID,
		SequenceNumber:     nextMaintenanceSequenceNumber(meta),
		TimestampMS:        now.UnixMilli(),
		SchemaID:           meta.CurrentSchemaID,
		TargetRef:          firstNonEmptyString(req.TargetRef, "main"),
		TargetRefType:      req.TargetRefType,
		TargetRefRetention: meta.Refs[firstNonEmptyString(req.TargetRef, "main")],
		IdempotencyKey:     firstNonEmptyString(req.IdempotencyKey, req.JobID),
		DataManifestPath:   dataManifestPath,
		ManifestListPath:   manifestListPath,
		PreservedManifests: append(
			append([]api.ManifestFile(nil), selection.PreservedManifests...),
			preservedManifestFiles(preservedDataManifests)...,
		),
		Summary: map[string]string{
			"operation":           string(OperationRewriteDataFiles),
			"engine":              "matrixone",
			"idempotency-key":     firstNonEmptyString(req.IdempotencyKey, req.JobID),
			"base-snapshot":       strconv.FormatInt(snapshot.SnapshotID, 10),
			"candidate-files":     strconv.FormatUint(selection.CandidateFileCount, 10),
			"candidate-bytes":     strconv.FormatInt(selection.CandidateSizeBytes, 10),
			"candidate-manifests": strconv.Itoa(selection.ScannedManifestCount),
		},
		Rewrites: compacted.Rewrites,
	})
	if err != nil {
		return nil, err
	}
	objects := make([]ObjectWrite, 0, len(compacted.Objects)+len(preservedObjects)+len(materialized.ManifestObjects)+1)
	objects = append(objects, compacted.Objects...)
	objects = append(objects, preservedObjects...)
	objects = append(objects, materialized.ManifestObjects...)
	objects = append(objects, ObjectWrite{Location: materialized.ManifestListPath, Payload: materialized.ManifestListBytes})
	return &CommitPlan{
		Catalog:            p.Catalog,
		Attempt:            materialized.Attempt,
		Objects:            objects,
		OrphanPaths:        dedupeNonEmptyStrings(compacted.OrphanPaths),
		PostCommitOrphans:  dedupeNonEmptyStrings(rewriteDataFilesSourcePaths(compacted.Rewrites)),
		RewrittenFileCount: materialized.RewrittenFiles,
	}, nil
}

func (p NativeRewriteDataFilesPlanner) rewriteBasePath(meta *api.TableMetadata, snapshot api.Snapshot, req Request) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(p.PathPrefix), "/")
	if base == "" && meta != nil && strings.TrimSpace(meta.Location) != "" {
		base = joinObjectPath(meta.Location, "metadata")
	}
	if base == "" {
		base = objectDir(snapshot.ManifestList)
	}
	if base == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-data-files planner requires table location, manifest list path, or path prefix", map[string]string{
			"snapshot_id": strconv.FormatInt(snapshot.SnapshotID, 10),
		})
	}
	return joinObjectPath(base, "mo-rewrite-data-files", rewriteDataFilesID(req, snapshot)), nil
}

func rewriteDataFilesCompactorSupportsDeletes(compactor RewriteDataFilesCompactor) bool {
	deleteAware, ok := compactor.(RewriteDataFilesDeleteAwareCompactor)
	return ok && deleteAware.SupportsDeleteManifests()
}

func rewriteDataFilesID(req Request, snapshot api.Snapshot) string {
	raw := firstNonEmptyString(req.IdempotencyKey, req.JobID, strconv.FormatInt(snapshot.SnapshotID, 10))
	if raw == "" {
		raw = "rewrite-data-files"
	}
	return "rw-" + api.PathHash(raw)
}

func rewriteDataFilesSourcePaths(rewrites []RewriteDataFileRewrite) []string {
	paths := make([]string, 0)
	for _, rewrite := range rewrites {
		for _, candidate := range rewrite.Group.Candidates {
			paths = append(paths, candidate.File.FilePath)
		}
	}
	return paths
}

func rewriteDataFilesSourcePathSet(rewrites []RewriteDataFileRewrite) map[string]struct{} {
	paths := rewriteDataFilesSourcePaths(rewrites)
	if len(paths) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) != "" {
			out[strings.TrimSpace(path)] = struct{}{}
		}
	}
	return out
}

func materializeRewriteDataFilesPreservedManifests(meta *api.TableMetadata, basePath string, preserved []RewriteDataFilesPreservedManifest, removedPaths map[string]struct{}) ([]api.ManifestFile, []ObjectWrite, error) {
	if len(preserved) == 0 {
		return nil, nil, nil
	}
	outManifests := make([]api.ManifestFile, 0, len(preserved))
	outObjects := make([]ObjectWrite, 0, len(preserved))
	for idx, item := range preserved {
		entries := filterRewriteDataFilesPreservedEntries(item.Entries, removedPaths)
		if len(entries) == 0 {
			continue
		}
		path := joinObjectPath(basePath, fmt.Sprintf("preserved-manifest-%05d.avro", idx+1))
		content := item.Manifest.Content
		if content == "" {
			content = api.ManifestContentData
		}
		writeOpts, err := maintenanceManifestWriteOptions(meta, item.Manifest.PartitionSpecID, content)
		if err != nil {
			return nil, nil, err
		}
		payload, err := metadata.EncodeManifest(entries, writeOpts)
		if err != nil {
			return nil, nil, err
		}
		manifest := rewriteDataFilesPreservedManifestFile(item.Manifest, path, payload, entries)
		outManifests = append(outManifests, manifest)
		outObjects = append(outObjects, ObjectWrite{Location: path, Payload: payload})
	}
	return outManifests, outObjects, nil
}

func filterRewriteDataFilesPreservedEntries(entries []api.ManifestEntry, removedPaths map[string]struct{}) []api.ManifestEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]api.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		path := strings.TrimSpace(entry.DataFile.FilePath)
		if _, remove := removedPaths[path]; remove {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func rewriteDataFilesPreservedManifestFile(original api.ManifestFile, path string, payload []byte, entries []api.ManifestEntry) api.ManifestFile {
	manifest := original
	manifest.Path = path
	manifest.Length = int64(len(payload))
	if manifest.Content == "" {
		manifest.Content = api.ManifestContentData
	}
	manifest.AddedFilesCount = rewriteDataFilesCountEntries(entries, api.ManifestEntryAdded)
	manifest.ExistingFilesCount = rewriteDataFilesCountEntries(entries, api.ManifestEntryExisting)
	manifest.DeletedFilesCount = rewriteDataFilesCountEntries(entries, api.ManifestEntryDeleted)
	manifest.AddedRowsCount = rewriteDataFilesRows(entries, api.ManifestEntryAdded)
	manifest.ExistingRowsCount = rewriteDataFilesRows(entries, api.ManifestEntryExisting)
	manifest.DeletedRowsCount = rewriteDataFilesRows(entries, api.ManifestEntryDeleted)
	manifest.AddedFilesSizeInBytes = rewriteDataFilesBytes(entries, api.ManifestEntryAdded)
	manifest.ExistingFilesSizeInBytes = rewriteDataFilesBytes(entries, api.ManifestEntryExisting)
	manifest.DeletedFilesSizeInBytes = rewriteDataFilesBytes(entries, api.ManifestEntryDeleted)
	manifest.ManifestPathRedacted = api.RedactPath(path)
	manifest.ManifestPathHash = api.PathHash(path)
	return manifest
}

func preservedManifestFiles(in []api.ManifestFile) []api.ManifestFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.ManifestFile, len(in))
	copy(out, in)
	return out
}

func (s RewriteDataFilesSelector) Select(ctx context.Context, req RewriteDataFilesSelectionRequest) (*RewriteDataFilesSelection, error) {
	if s.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files selector requires an object reader", nil)
	}
	if strings.TrimSpace(req.Snapshot.ManifestList) == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files selector requires a target manifest list", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	opts, err := parseRewriteDataFilesSelectionOptions(req.Options)
	if err != nil {
		return nil, err
	}
	facade := s.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	manifestListData, err := s.ObjectReader.Read(ctx, req.Snapshot.ManifestList, 0, -1)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-data-files selector failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
		}, err)
	}
	manifests, err := facade.ReadManifestList(ctx, manifestListData)
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]RewriteDataFileCandidate)
	selection := &RewriteDataFilesSelection{}
	for _, manifest := range manifests {
		if manifest.Content == api.ManifestContentDeletes {
			if strings.TrimSpace(manifest.Path) == "" {
				return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files selector found delete manifest without path", map[string]string{
					"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
				})
			}
			selection.PreservedManifests = append(selection.PreservedManifests, manifest)
			selection.DeleteManifests = append(selection.DeleteManifests, manifest)
			selection.DeleteManifestCount++
			continue
		}
		if manifest.Content != "" && manifest.Content != api.ManifestContentData {
			continue
		}
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files selector found data manifest without path", map[string]string{
				"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
			})
		}
		manifestData, err := s.ObjectReader.Read(ctx, manifestPath, 0, -1)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-data-files selector failed to read manifest", map[string]string{
				"manifest": api.RedactPath(manifestPath),
			}, err)
		}
		entries, err := facade.ReadManifest(ctx, manifestData)
		if err != nil {
			return nil, err
		}
		for idx := range entries {
			entries[idx].DataFile.SpecID = manifest.PartitionSpecID
		}
		selection.ScannedManifestCount++
		selection.PreservedDataManifests = append(selection.PreservedDataManifests, RewriteDataFilesPreservedManifest{
			Manifest: manifest,
			Entries:  append([]api.ManifestEntry(nil), entries...),
		})
		for _, entry := range entries {
			if entry.Status == api.ManifestEntryDeleted || entry.DataFile.Content != api.DataFileContentData {
				continue
			}
			file := entry.DataFile
			if strings.TrimSpace(file.FilePath) == "" {
				return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files selector found data file without path", map[string]string{
					"manifest": api.RedactPath(manifestPath),
				})
			}
			selection.ScannedFileCount++
			if !isRewriteDataFilesCandidate(file, opts) {
				continue
			}
			candidate := RewriteDataFileCandidate{ManifestPath: manifestPath, Entry: entry, File: file}
			groupKey := rewriteDataFilesGroupKey(file)
			grouped[groupKey] = append(grouped[groupKey], candidate)
			selection.CandidateFileCount++
			selection.CandidateSizeBytes += file.FileSizeInBytes
		}
	}
	selection.Groups = buildRewriteDataFileGroups(grouped, opts)
	return selection, nil
}

func parseRewriteDataFilesSelectionOptions(options map[string]string) (rewriteDataFilesSelectionOptions, error) {
	out := rewriteDataFilesSelectionOptions{
		targetFileSizeBytes: defaultRewriteDataFilesTargetSizeBytes,
		minInputFiles:       defaultRewriteDataFilesMinInputFiles,
	}
	if value, ok, err := UintOption(options, "target_file_size"); err != nil {
		return out, err
	} else if ok {
		if value == 0 {
			return out, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files target_file_size must be positive", map[string]string{"option": "target_file_size"})
		}
		out.targetFileSizeBytes = value
	}
	if value, ok, err := UintOption(options, "min_input_files"); err != nil {
		return out, err
	} else if ok {
		if value == 0 {
			return out, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files min_input_files must be positive", map[string]string{"option": "min_input_files"})
		}
		out.minInputFiles = value
	}
	defaultMaxGroupSize := out.targetFileSizeBytes * defaultRewriteDataFilesMaxGroupFactor
	if defaultMaxGroupSize < out.targetFileSizeBytes {
		defaultMaxGroupSize = out.targetFileSizeBytes
	}
	out.maxGroupSizeBytes = defaultMaxGroupSize
	if value, ok, err := UintOption(options, "max_group_size"); err != nil {
		return out, err
	} else if ok {
		if value == 0 {
			return out, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files max_group_size must be positive", map[string]string{"option": "max_group_size"})
		}
		out.maxGroupSizeBytes = value
	}
	if out.maxGroupSizeBytes < out.targetFileSizeBytes {
		out.maxGroupSizeBytes = out.targetFileSizeBytes
	}
	return out, nil
}

func isRewriteDataFilesCandidate(file api.DataFile, opts rewriteDataFilesSelectionOptions) bool {
	if file.FileSizeInBytes <= 0 {
		return false
	}
	if uint64(file.FileSizeInBytes) > opts.targetFileSizeBytes {
		return false
	}
	return strings.EqualFold(firstNonEmptyString(file.FileFormat, "parquet"), "parquet")
}

func buildRewriteDataFileGroups(grouped map[string][]RewriteDataFileCandidate, opts rewriteDataFilesSelectionOptions) []RewriteDataFileGroup {
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	groups := make([]RewriteDataFileGroup, 0, len(keys))
	for _, key := range keys {
		candidates := grouped[key]
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].File.FilePath < candidates[j].File.FilePath
		})
		groups = append(groups, chunkRewriteDataFileCandidates(key, candidates, opts)...)
	}
	return groups
}

func chunkRewriteDataFileCandidates(key string, candidates []RewriteDataFileCandidate, opts rewriteDataFilesSelectionOptions) []RewriteDataFileGroup {
	groups := make([]RewriteDataFileGroup, 0, 1)
	current := RewriteDataFileGroup{}
	for _, candidate := range candidates {
		nextSize := current.TotalSizeBytes + candidate.File.FileSizeInBytes
		if len(current.Candidates) > 0 && uint64(nextSize) > opts.maxGroupSizeBytes {
			if uint64(len(current.Candidates)) >= opts.minInputFiles {
				groups = append(groups, current)
			}
			current = RewriteDataFileGroup{}
		}
		addRewriteDataFileCandidate(&current, key, candidate)
	}
	if uint64(len(current.Candidates)) >= opts.minInputFiles {
		groups = append(groups, current)
	}
	return groups
}

func addRewriteDataFileCandidate(group *RewriteDataFileGroup, key string, candidate RewriteDataFileCandidate) {
	if len(group.Candidates) == 0 {
		group.PartitionSpecID = candidate.File.SpecID
		group.PartitionKey = key
	}
	group.Candidates = append(group.Candidates, candidate)
	group.TotalSizeBytes += candidate.File.FileSizeInBytes
	group.TotalRecords += candidate.File.RecordCount
}

func rewriteDataFilesGroupKey(file api.DataFile) string {
	return strconv.Itoa(file.SpecID) + "|" + rewriteDataFilesPartitionKey(file.Partition)
}

func rewriteDataFilesPartitionKey(partition map[string]any) string {
	if len(partition) == 0 {
		return ""
	}
	keys := make([]string, 0, len(partition))
	for key := range partition {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		writeRewriteDataFilesKeyPart(&out, key)
		writeRewriteDataFilesKeyPart(&out, rewriteDataFilesPartitionValue(partition[key]))
	}
	return out.String()
}

func writeRewriteDataFilesKeyPart(out *strings.Builder, value string) {
	out.WriteString(strconv.Itoa(len(value)))
	out.WriteByte(':')
	out.WriteString(value)
}

func rewriteDataFilesPartitionValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		if v {
			return "b:1"
		}
		return "b:0"
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case uint:
		return "u:" + strconv.FormatUint(uint64(v), 10)
	case uint32:
		return "u:" + strconv.FormatUint(uint64(v), 10)
	case uint64:
		return "u:" + strconv.FormatUint(v, 10)
	case string:
		return "s:" + v
	default:
		return "x:" + fmt.Sprint(v)
	}
}

func BuildRewriteDataFilesManifestCommit(req RewriteDataFilesMaterializeRequest) (*RewriteDataFilesMaterializeResult, error) {
	if req.Snapshot.SnapshotID <= 0 || req.SnapshotID <= 0 || req.SequenceNumber <= 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files materialization requires snapshot and sequence numbers", nil)
	}
	if strings.TrimSpace(req.DataManifestPath) == "" || strings.TrimSpace(req.ManifestListPath) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files manifest paths are required", nil)
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files materialization requires an idempotency key", nil)
	}
	entries, rewrittenFiles, addedFiles, err := rewriteDataFilesManifestEntries(req)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files materialization requires at least one rewrite group", nil)
	}
	entriesBySpec := rewriteDataFilesEntriesBySpec(entries)
	specIDs := make([]int, 0, len(entriesBySpec))
	for specID := range entriesBySpec {
		specIDs = append(specIDs, specID)
	}
	sort.Ints(specIDs)
	manifests := make([]api.ManifestFile, 0, len(specIDs))
	manifestObjects := make([]ObjectWrite, 0, len(specIDs))
	for _, specID := range specIDs {
		specEntries := entriesBySpec[specID]
		writeOpts, err := rewriteDataFilesManifestWriteOptions(req, specID)
		if err != nil {
			return nil, err
		}
		manifestBytes, err := metadata.EncodeManifest(specEntries, writeOpts)
		if err != nil {
			return nil, err
		}
		manifestPath := rewriteDataFilesManifestPath(req.DataManifestPath, specID, len(specIDs))
		manifest := rewriteDataFilesManifestFile(req, manifestPath, specID, manifestBytes, specEntries)
		manifests = append(manifests, manifest)
		manifestObjects = append(manifestObjects, ObjectWrite{Location: manifestPath, Payload: manifestBytes})
	}
	manifestList := append(cloneRewriteDataFilesManifests(req.PreservedManifests), manifests...)
	parentSnapshotID := req.Snapshot.SnapshotID
	manifestListBytes, err := metadata.EncodeManifestList(manifestList, metadata.ManifestListWriteOptions{
		FormatVersion:    req.FormatVersion,
		SnapshotID:       req.SnapshotID,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   req.SequenceNumber,
	})
	if err != nil {
		return nil, err
	}
	result := &RewriteDataFilesMaterializeResult{
		Entries:           entries,
		ManifestFiles:     manifests,
		ManifestListPath:  req.ManifestListPath,
		ManifestObjects:   manifestObjects,
		ManifestListBytes: manifestListBytes,
		RewrittenFiles:    rewrittenFiles,
		AddedFiles:        addedFiles,
	}
	if len(manifests) > 0 {
		result.ManifestFile = manifests[0]
		result.ManifestBytes = manifestObjects[0].Payload
	}
	result.Attempt = buildRewriteDataFilesCommitAttempt(req, manifests, rewrittenFiles, addedFiles)
	return result, nil
}

func rewriteDataFilesEntriesBySpec(entries []api.ManifestEntry) map[int][]api.ManifestEntry {
	out := make(map[int][]api.ManifestEntry)
	for _, entry := range entries {
		specID := entry.DataFile.SpecID
		out[specID] = append(out[specID], entry)
	}
	return out
}

func rewriteDataFilesManifestPath(base string, specID, specCount int) string {
	if specCount <= 1 {
		return base
	}
	ext := ""
	if idx := strings.LastIndex(base, "."); idx > strings.LastIndex(base, "/") {
		ext = base[idx:]
		base = base[:idx]
	}
	return base + "-spec-" + strconv.Itoa(specID) + ext
}

func rewriteDataFilesManifestFile(req RewriteDataFilesMaterializeRequest, path string, specID int, payload []byte, entries []api.ManifestEntry) api.ManifestFile {
	return api.ManifestFile{
		Path:                     path,
		Length:                   int64(len(payload)),
		PartitionSpecID:          specID,
		Content:                  api.ManifestContentData,
		SequenceNumber:           req.SequenceNumber,
		MinSequenceNumber:        req.SequenceNumber,
		AddedSnapshotID:          req.SnapshotID,
		AddedFilesCount:          rewriteDataFilesCountEntries(entries, api.ManifestEntryAdded),
		DeletedFilesCount:        rewriteDataFilesCountEntries(entries, api.ManifestEntryDeleted),
		AddedRowsCount:           rewriteDataFilesRows(entries, api.ManifestEntryAdded),
		DeletedRowsCount:         rewriteDataFilesRows(entries, api.ManifestEntryDeleted),
		AddedFilesSizeInBytes:    rewriteDataFilesBytes(entries, api.ManifestEntryAdded),
		DeletedFilesSizeInBytes:  rewriteDataFilesBytes(entries, api.ManifestEntryDeleted),
		ExistingFilesSizeInBytes: 0,
		ManifestPathRedacted:     api.RedactPath(path),
		ManifestPathHash:         api.PathHash(path),
	}
}

func rewriteDataFilesManifestWriteOptions(req RewriteDataFilesMaterializeRequest, specID int) (metadata.ManifestWriteOptions, error) {
	for _, spec := range req.PartitionSpecs {
		if spec.SpecID == specID {
			return metadata.ManifestWriteOptions{
				FormatVersion: req.FormatVersion,
				Schema:        req.Schema,
				PartitionSpec: spec,
				Content:       api.ManifestContentData,
			}, nil
		}
	}
	return metadata.ManifestWriteOptions{}, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files manifest partition spec is missing", map[string]string{
		"spec_id": strconv.Itoa(specID),
	})
}

func rewriteDataFilesManifestEntries(req RewriteDataFilesMaterializeRequest) ([]api.ManifestEntry, uint64, uint64, error) {
	entries := make([]api.ManifestEntry, 0)
	var rewrittenFiles uint64
	var addedFiles uint64
	for _, rewrite := range req.Rewrites {
		if len(rewrite.Group.Candidates) == 0 {
			return nil, 0, 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files rewrite group requires source files", nil)
		}
		if len(rewrite.ReplacementFiles) == 0 {
			return nil, 0, 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files rewrite group requires replacement files", nil)
		}
		for _, candidate := range rewrite.Group.Candidates {
			file := candidate.File
			file.Content = api.DataFileContentData
			if strings.TrimSpace(file.FilePath) == "" {
				return nil, 0, 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files source file path is required", nil)
			}
			sequenceNumber := candidate.Entry.SequenceNumber
			if sequenceNumber == 0 {
				sequenceNumber = file.SequenceNumber
			}
			fileSequenceNumber := candidate.Entry.FileSequence
			if fileSequenceNumber == 0 {
				fileSequenceNumber = file.FileSequenceNumber
			}
			entries = append(entries, rewriteDataFilesManifestEntryWithSequences(
				api.ManifestEntryDeleted,
				req.SnapshotID,
				sequenceNumber,
				fileSequenceNumber,
				file,
			))
			rewrittenFiles++
		}
		for _, replacement := range rewrite.ReplacementFiles {
			replacement.Content = api.DataFileContentData
			if strings.TrimSpace(replacement.FilePath) == "" {
				return nil, 0, 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files replacement file path is required", nil)
			}
			entries = append(entries, rewriteDataFilesManifestEntry(api.ManifestEntryAdded, req.SnapshotID, req.SequenceNumber, replacement))
			addedFiles++
		}
	}
	return entries, rewrittenFiles, addedFiles, nil
}

func rewriteDataFilesManifestEntry(status api.ManifestEntryStatus, snapshotID, sequenceNumber int64, file api.DataFile) api.ManifestEntry {
	return rewriteDataFilesManifestEntryWithSequences(status, snapshotID, sequenceNumber, sequenceNumber, file)
}

func rewriteDataFilesManifestEntryWithSequences(status api.ManifestEntryStatus, snapshotID, sequenceNumber, fileSequenceNumber int64, file api.DataFile) api.ManifestEntry {
	return api.ManifestEntry{
		Status:         status,
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		FileSequence:   fileSequenceNumber,
		DataFile:       file,
	}
}

func buildRewriteDataFilesCommitAttempt(req RewriteDataFilesMaterializeRequest, manifests []api.ManifestFile, rewrittenFiles, addedFiles uint64) *api.CommitAttempt {
	targetRef := firstNonEmptyString(req.TargetRef, "main")
	summary := cloneStringMap(req.Summary)
	if summary == nil {
		summary = make(map[string]string)
	}
	summary["operation"] = string(OperationRewriteDataFiles)
	summary["engine"] = "matrixone"
	summary["idempotency-key"] = req.IdempotencyKey
	summary["base-snapshot"] = strconv.FormatInt(req.Snapshot.SnapshotID, 10)
	summary["rewritten-files"] = strconv.FormatUint(rewrittenFiles, 10)
	summary["added-files"] = strconv.FormatUint(addedFiles, 10)
	updates := []api.CommitUpdate{
		api.NewAddSnapshotUpdate(api.NewCommitSnapshot(
			req.SnapshotID,
			req.Snapshot.SnapshotID,
			req.SequenceNumber,
			req.SchemaID,
			rewriteDataFilesTimestampMS(req.TimestampMS),
			req.ManifestListPath,
			summary,
		)),
		api.NewSetSnapshotRefUpdatePreservingRetention(targetRef, req.TargetRefType, req.SnapshotID, req.TargetRefRetention),
	}
	return &api.CommitAttempt{
		Requirements: []api.CommitRequirement{{
			Type:       "assert-ref-snapshot-id",
			Ref:        targetRef,
			SnapshotID: req.Snapshot.SnapshotID,
		}},
		Updates:        updates,
		ManifestFiles:  append([]api.ManifestFile(nil), manifests...),
		Summary:        summary,
		IdempotencyKey: req.IdempotencyKey,
		BaseSnapshotID: req.Snapshot.SnapshotID,
		TargetRef:      targetRef,
		TargetRefType:  req.TargetRefType,
	}
}

func rewriteDataFilesTimestampMS(timestampMS int64) int64 {
	if timestampMS > 0 {
		return timestampMS
	}
	return time.Now().UTC().UnixMilli()
}

func cloneRewriteDataFilesManifests(in []api.ManifestFile) []api.ManifestFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.ManifestFile, len(in))
	copy(out, in)
	return out
}

func rewriteDataFilesCountEntries(entries []api.ManifestEntry, status api.ManifestEntryStatus) int {
	count := 0
	for _, entry := range entries {
		if entry.Status == status {
			count++
		}
	}
	return count
}

func rewriteDataFilesRows(entries []api.ManifestEntry, status api.ManifestEntryStatus) int64 {
	var rows int64
	for _, entry := range entries {
		if entry.Status == status {
			rows += entry.DataFile.RecordCount
		}
	}
	return rows
}

func rewriteDataFilesBytes(entries []api.ManifestEntry, status api.ManifestEntryStatus) int64 {
	var bytes int64
	for _, entry := range entries {
		if entry.Status == status {
			bytes += entry.DataFile.FileSizeInBytes
		}
	}
	return bytes
}
