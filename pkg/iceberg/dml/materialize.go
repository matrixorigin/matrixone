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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type ManifestMaterializeRequest struct {
	Intent             CommitIntent
	FormatVersion      int
	Schema             api.Schema
	PartitionSpecs     []api.PartitionSpec
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	DataManifestPath   string
	DeleteManifestPath string
	ManifestListPath   string
	PreservedManifests []api.ManifestFile
	PreservedSources   []PreservedManifestSource
	// MaxMemoryBytes bounds the complete materialization working set. A zero
	// value preserves compatibility for direct library callers; MatrixOne's
	// runtime always supplies the validated iceberg.write.dml-max-memory value.
	MaxMemoryBytes     int64
	InitialMemoryBytes int64
}

type ManifestMaterializeResult struct {
	DataEntries                 []api.ManifestEntry
	DeleteEntries               []api.ManifestEntry
	DataManifest                *api.ManifestFile
	DeleteManifest              *api.ManifestFile
	DataManifests               []MaterializedManifest
	DeleteManifests             []MaterializedManifest
	RewrittenPreservedManifests []RewrittenPreservedManifest
	DataManifestBytes           []byte
	DeleteManifestBytes         []byte
	ManifestListBytes           []byte
	Attempt                     *api.CommitAttempt
}

type MaterializedManifest struct {
	Manifest      api.ManifestFile
	ManifestBytes []byte
}

type PreservedManifestSource struct {
	Manifest api.ManifestFile
	Entries  []api.ManifestEntry
}

type RewrittenPreservedManifest struct {
	OriginalPath   string
	Manifest       api.ManifestFile
	ManifestBytes  []byte
	RemovedEntries int
}

func BuildManifestCommitAttempt(ctx context.Context, req ManifestMaterializeRequest) (*ManifestMaterializeResult, error) {
	if req.SnapshotID <= 0 || req.SequenceNumber <= 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest materialization requires snapshot and sequence numbers", nil)
	}
	if strings.TrimSpace(req.ManifestListPath) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest list path is required", nil)
	}
	if strings.TrimSpace(req.Intent.IdempotencyKey) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest materialization requires an idempotency key", nil)
	}
	budget, err := newManifestMaterializeBudget(req.MaxMemoryBytes, req.InitialMemoryBytes)
	if err != nil {
		return nil, err
	}
	// Reserve before constructing the entry slices. Intent actions retain the
	// source DataFile values while materialization creates independent entry
	// structs; counting the full conservative entry weight also covers slice and
	// map/header overhead without relying on Go implementation sizes.
	if err := budget.reserve(estimateIntentEntryMemory(req.Intent)); err != nil {
		return nil, err
	}
	dataEntries, deleteEntries, err := manifestEntries(req.Intent, req.SnapshotID, req.SequenceNumber)
	if err != nil {
		return nil, err
	}
	if len(dataEntries) == 0 && len(deleteEntries) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest materialization requires at least one manifest entry", nil)
	}
	result := &ManifestMaterializeResult{
		DataEntries:   dataEntries,
		DeleteEntries: deleteEntries,
	}
	newManifests := make([]api.ManifestFile, 0, 2)
	if len(dataEntries) > 0 {
		if strings.TrimSpace(req.DataManifestPath) == "" {
			return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML data manifest path is required", nil)
		}
		manifests, err := buildDMLManifests(req, budget, req.DataManifestPath, api.ManifestContentData, req.SnapshotID, req.SequenceNumber, dataEntries)
		if err != nil {
			return nil, err
		}
		result.DataManifests = manifests
		result.DataManifest = &result.DataManifests[0].Manifest
		result.DataManifestBytes = result.DataManifests[0].ManifestBytes
		newManifests = append(newManifests, materializedManifestFiles(manifests)...)
	}
	if len(deleteEntries) > 0 {
		if strings.TrimSpace(req.DeleteManifestPath) == "" {
			return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete manifest path is required", nil)
		}
		manifests, err := buildDMLManifests(req, budget, req.DeleteManifestPath, api.ManifestContentDeletes, req.SnapshotID, req.SequenceNumber, deleteEntries)
		if err != nil {
			return nil, err
		}
		result.DeleteManifests = manifests
		result.DeleteManifest = &result.DeleteManifests[0].Manifest
		result.DeleteManifestBytes = result.DeleteManifests[0].ManifestBytes
		newManifests = append(newManifests, materializedManifestFiles(manifests)...)
	}
	if err := budget.reserve(estimatePreservedDerivationMemory(req, dataEntries)); err != nil {
		return nil, err
	}
	preservedManifests, rewrittenPreserved, err := materializePreservedManifests(req, budget, dataEntries)
	if err != nil {
		return nil, err
	}
	result.RewrittenPreservedManifests = rewrittenPreserved
	manifestList := append(preservedManifests, newManifests...)
	if err := budget.reserve(metadata.ManifestListMemoryWeight(0, manifestList)); err != nil {
		return nil, err
	}
	var parentSnapshotID *int64
	if req.Intent.BaseSnapshotID > 0 {
		parentSnapshotID = &req.Intent.BaseSnapshotID
	}
	manifestListBytes, err := budget.encodeManifestList(manifestList, metadata.ManifestListWriteOptions{
		FormatVersion:    req.FormatVersion,
		SnapshotID:       req.SnapshotID,
		ParentSnapshotID: parentSnapshotID,
		SequenceNumber:   req.SequenceNumber,
	})
	if err != nil {
		return nil, err
	}
	result.ManifestListBytes = manifestListBytes
	result.Attempt = buildDMLCommitAttempt(req, newManifests)
	return result, nil
}

func manifestEntries(intent CommitIntent, snapshotID, sequenceNumber int64) ([]api.ManifestEntry, []api.ManifestEntry, error) {
	dataEntries := make([]api.ManifestEntry, 0, len(intent.Actions))
	deleteEntries := make([]api.ManifestEntry, 0)
	for _, action := range intent.Actions {
		switch action.Kind {
		case ActionAppendData:
			file := action.File
			file.Content = api.DataFileContentData
			dataEntries = append(dataEntries, addedManifestEntry(snapshotID, sequenceNumber, file))
		case ActionAddEqualityDelete:
			file := action.DeleteFile
			file.Content = api.DataFileContentEqualityDelete
			if len(file.EqualityIDs) == 0 {
				return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete file requires equality ids", map[string]string{"path": file.FilePathHash})
			}
			deleteEntries = append(deleteEntries, addedManifestEntry(snapshotID, sequenceNumber, file))
		case ActionAddPositionDelete:
			file := action.DeleteFile
			file.Content = api.DataFileContentPositionDelete
			if strings.TrimSpace(file.ReferencedDataFile) == "" {
				return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete file requires a referenced data file", map[string]string{"path": file.FilePathHash})
			}
			deleteEntries = append(deleteEntries, addedManifestEntry(snapshotID, sequenceNumber, file))
		case ActionRewriteDataFile:
			old := action.ReplacedFile
			old.Content = api.DataFileContentData
			dataEntries = append(dataEntries, deletedManifestEntry(snapshotID, old))
			for _, replacement := range action.ReplacementFiles {
				replacement.Content = api.DataFileContentData
				dataEntries = append(dataEntries, addedManifestEntry(snapshotID, sequenceNumber, replacement))
			}
		case ActionDeleteDataFile:
			old := action.ReplacedFile
			old.Content = api.DataFileContentData
			dataEntries = append(dataEntries, deletedManifestEntry(snapshotID, old))
		default:
			return nil, nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML action kind is unsupported", map[string]string{"action": string(action.Kind)})
		}
	}
	return dataEntries, deleteEntries, nil
}

func addedManifestEntry(snapshotID, sequenceNumber int64, file api.DataFile) api.ManifestEntry {
	return api.ManifestEntry{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		FileSequence:   sequenceNumber,
		DataFile:       file,
	}
}

func deletedManifestEntry(snapshotID int64, file api.DataFile) api.ManifestEntry {
	return api.ManifestEntry{
		Status:         api.ManifestEntryDeleted,
		SnapshotID:     snapshotID,
		SequenceNumber: file.SequenceNumber,
		FileSequence:   file.FileSequenceNumber,
		DataFile:       file,
	}
}

func buildDMLManifests(req ManifestMaterializeRequest, budget *manifestMaterializeBudget, basePath string, content api.ManifestContent, snapshotID, sequenceNumber int64, entries []api.ManifestEntry) ([]MaterializedManifest, error) {
	// Grouping copies entry structs by spec while the flat entry list remains
	// live. Charge it before the map/slices are allocated.
	groupingMemory := metadata.ManifestEntriesMemoryWeight(0, entries)
	if err := budget.reserve(groupingMemory); err != nil {
		return nil, err
	}
	defer budget.release(groupingMemory)
	entriesBySpec := make(map[int][]api.ManifestEntry)
	for _, entry := range entries {
		entriesBySpec[entry.DataFile.SpecID] = append(entriesBySpec[entry.DataFile.SpecID], entry)
	}
	specIDs := make([]int, 0, len(entriesBySpec))
	for specID := range entriesBySpec {
		specIDs = append(specIDs, specID)
	}
	sort.Ints(specIDs)
	out := make([]MaterializedManifest, 0, len(specIDs))
	for _, specID := range specIDs {
		specEntries := entriesBySpec[specID]
		metrics, err := aggregateDMLManifestEntries(specEntries)
		if err != nil {
			return nil, err
		}
		if err := validateDMLManifestContent(specEntries, content); err != nil {
			return nil, err
		}
		opts, err := dmlManifestWriteOptions(req, content, specID)
		if err != nil {
			return nil, err
		}
		manifestBytes, err := budget.encodeManifest(specEntries, opts)
		if err != nil {
			return nil, err
		}
		path := dmlManifestPathForSpec(basePath, specID, len(specIDs))
		out = append(out, MaterializedManifest{
			Manifest: api.ManifestFile{
				Path:                     path,
				Length:                   int64(len(manifestBytes)),
				PartitionSpecID:          specID,
				Content:                  content,
				SequenceNumber:           sequenceNumber,
				MinSequenceNumber:        sequenceNumber,
				AddedSnapshotID:          snapshotID,
				AddedFilesCount:          metrics.addedFiles,
				ExistingFilesCount:       metrics.existingFiles,
				DeletedFilesCount:        metrics.deletedFiles,
				AddedRowsCount:           metrics.addedRows,
				ExistingRowsCount:        metrics.existingRows,
				DeletedRowsCount:         metrics.deletedRows,
				AddedFilesSizeInBytes:    metrics.addedBytes,
				ExistingFilesSizeInBytes: metrics.existingBytes,
				DeletedFilesSizeInBytes:  metrics.deletedBytes,
				ManifestPathRedacted:     api.RedactPath(path),
				ManifestPathHash:         api.PathHash(path),
			},
			ManifestBytes: manifestBytes,
		})
	}
	return out, nil
}

func dmlManifestPathForSpec(basePath string, specID, specCount int) string {
	if specCount <= 1 {
		return basePath
	}
	ext := ""
	if idx := strings.LastIndex(basePath, "."); idx > strings.LastIndex(basePath, "/") {
		ext = basePath[idx:]
		basePath = basePath[:idx]
	}
	return basePath + "-spec-" + strconv.Itoa(specID) + ext
}

func materializedManifestFiles(manifests []MaterializedManifest) []api.ManifestFile {
	out := make([]api.ManifestFile, 0, len(manifests))
	for _, manifest := range manifests {
		out = append(out, manifest.Manifest)
	}
	return out
}

func buildDMLCommitAttempt(req ManifestMaterializeRequest, manifests []api.ManifestFile) *api.CommitAttempt {
	updates := []api.CommitUpdate{
		api.NewAddSnapshotUpdate(api.NewCommitSnapshot(
			req.SnapshotID,
			req.Intent.BaseSnapshotID,
			req.SequenceNumber,
			req.Intent.BaseSchemaID,
			dmlTimestampMS(req.TimestampMS),
			req.ManifestListPath,
			req.Intent.Summary,
		)),
		api.NewSetSnapshotRefUpdatePreservingRetention(req.Intent.TargetRef, req.Intent.TargetRefType, req.SnapshotID, req.Intent.TargetRefRetention),
	}
	return &api.CommitAttempt{
		Requirements:   append([]api.CommitRequirement(nil), req.Intent.Requirements...),
		Updates:        updates,
		ManifestFiles:  append([]api.ManifestFile(nil), manifests...),
		Summary:        cloneStringMap(req.Intent.Summary),
		IdempotencyKey: req.Intent.IdempotencyKey,
		BaseSnapshotID: req.Intent.BaseSnapshotID,
		TargetRef:      req.Intent.TargetRef,
		TargetRefType:  req.Intent.TargetRefType,
	}
}

func materializePreservedManifests(req ManifestMaterializeRequest, budget *manifestMaterializeBudget, dataEntries []api.ManifestEntry) ([]api.ManifestFile, []RewrittenPreservedManifest, error) {
	deletedFiles := deletedDataFileSet(dataEntries)
	if len(deletedFiles) == 0 {
		return cloneManifestFiles(req.PreservedManifests), nil, nil
	}
	if len(req.PreservedManifests) == 0 {
		return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DML file-delete commit requires preserved data manifests", map[string]string{
			"deleted_files": strconv.Itoa(len(deletedFiles)),
		})
	}
	sources := preservedSourceByPath(req.PreservedSources)
	preserved := make([]api.ManifestFile, 0, len(req.PreservedManifests))
	rewritten := make([]RewrittenPreservedManifest, 0)
	foundDeleted := make(map[string]struct{}, len(deletedFiles))
	for idx, manifest := range req.PreservedManifests {
		if manifest.Content != "" && manifest.Content != api.ManifestContentData {
			preserved = append(preserved, manifest)
			continue
		}
		source, ok := sources[strings.TrimSpace(manifest.Path)]
		if !ok {
			return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DML file-delete commit requires entries for preserved data manifests", map[string]string{
				"manifest": api.RedactPath(manifest.Path),
			})
		}
		retainedEntries, removed := filterPreservedManifestEntries(source.Entries, deletedFiles, foundDeleted)
		if removed == 0 {
			preserved = append(preserved, manifest)
			continue
		}
		if len(retainedEntries) == 0 {
			continue
		}
		rewrite, err := buildRewrittenPreservedManifest(req, budget, manifest, retainedEntries, idx, removed)
		if err != nil {
			return nil, nil, err
		}
		rewritten = append(rewritten, rewrite)
		preserved = append(preserved, rewrite.Manifest)
	}
	if len(foundDeleted) != len(deletedFiles) {
		return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DML file-delete commit could not match all deleted files in base manifests", map[string]string{
			"missing_files": strconv.Itoa(len(deletedFiles) - len(foundDeleted)),
		})
	}
	return preserved, rewritten, nil
}

func deletedDataFileSet(entries []api.ManifestEntry) map[string]struct{} {
	out := make(map[string]struct{})
	for _, entry := range entries {
		if entry.Status != api.ManifestEntryDeleted || entry.DataFile.Content != api.DataFileContentData {
			continue
		}
		if path := strings.TrimSpace(entry.DataFile.FilePath); path != "" {
			out[path] = struct{}{}
		}
	}
	return out
}

func preservedSourceByPath(in []PreservedManifestSource) map[string]PreservedManifestSource {
	out := make(map[string]PreservedManifestSource, len(in))
	for _, source := range in {
		if path := strings.TrimSpace(source.Manifest.Path); path != "" {
			out[path] = source
		}
	}
	return out
}

func filterPreservedManifestEntries(entries []api.ManifestEntry, deletedFiles map[string]struct{}, found map[string]struct{}) ([]api.ManifestEntry, int) {
	retained := make([]api.ManifestEntry, 0, len(entries))
	var removed int
	for _, entry := range entries {
		path := strings.TrimSpace(entry.DataFile.FilePath)
		if entry.Status != api.ManifestEntryDeleted {
			if _, shouldDelete := deletedFiles[path]; shouldDelete {
				found[path] = struct{}{}
				removed++
				continue
			}
		}
		retained = append(retained, entry)
	}
	return retained, removed
}

func buildRewrittenPreservedManifest(req ManifestMaterializeRequest, budget *manifestMaterializeBudget, original api.ManifestFile, entries []api.ManifestEntry, index int, removed int) (RewrittenPreservedManifest, error) {
	path := rewrittenPreservedManifestPath(req.ManifestListPath, original.Path, index)
	content := original.Content
	if content == "" {
		content = api.ManifestContentData
	}
	specID := original.PartitionSpecID
	if inferred := manifestSpecID(entries); specID == 0 && inferred != 0 {
		specID = inferred
	}
	opts, err := dmlManifestWriteOptions(req, content, specID)
	if err != nil {
		return RewrittenPreservedManifest{}, err
	}
	metrics, err := aggregateDMLManifestEntries(entries)
	if err != nil {
		return RewrittenPreservedManifest{}, err
	}
	if err := validateDMLManifestContent(entries, content); err != nil {
		return RewrittenPreservedManifest{}, err
	}
	manifestBytes, err := budget.encodeManifest(entries, opts)
	if err != nil {
		return RewrittenPreservedManifest{}, err
	}
	manifest := original
	manifest.Path = path
	manifest.Length = int64(len(manifestBytes))
	manifest.AddedFilesCount = metrics.addedFiles
	manifest.ExistingFilesCount = metrics.existingFiles
	manifest.DeletedFilesCount = metrics.deletedFiles
	manifest.AddedRowsCount = metrics.addedRows
	manifest.ExistingRowsCount = metrics.existingRows
	manifest.DeletedRowsCount = metrics.deletedRows
	manifest.AddedFilesSizeInBytes = metrics.addedBytes
	manifest.ExistingFilesSizeInBytes = metrics.existingBytes
	manifest.DeletedFilesSizeInBytes = metrics.deletedBytes
	manifest.ManifestPathRedacted = api.RedactPath(path)
	manifest.ManifestPathHash = api.PathHash(path)
	return RewrittenPreservedManifest{
		OriginalPath:   original.Path,
		Manifest:       manifest,
		ManifestBytes:  manifestBytes,
		RemovedEntries: removed,
	}, nil
}

func dmlManifestWriteOptions(req ManifestMaterializeRequest, content api.ManifestContent, specID int) (metadata.ManifestWriteOptions, error) {
	for _, spec := range req.PartitionSpecs {
		if spec.SpecID == specID {
			return metadata.ManifestWriteOptions{
				FormatVersion: req.FormatVersion,
				Schema:        req.Schema,
				PartitionSpec: spec,
				Content:       content,
			}, nil
		}
	}
	return metadata.ManifestWriteOptions{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest partition spec is missing", map[string]string{
		"spec_id": strconv.Itoa(specID),
	})
}

func rewrittenPreservedManifestPath(manifestListPath, originalPath string, index int) string {
	base := strings.TrimRight(strings.TrimSpace(manifestListPath), "/")
	if slash := strings.LastIndex(base, "/"); slash >= 0 {
		base = base[:slash]
	}
	if base == "" {
		base = "."
	}
	return base + "/preserved-manifest-" + strconv.Itoa(index) + "-" + api.PathHash(originalPath) + ".avro"
}

func dmlTimestampMS(timestampMS int64) int64 {
	if timestampMS > 0 {
		return timestampMS
	}
	return time.Now().UnixMilli()
}

func cloneManifestFiles(in []api.ManifestFile) []api.ManifestFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.ManifestFile, len(in))
	copy(out, in)
	return out
}

func manifestSpecID(entries []api.ManifestEntry) int {
	for _, entry := range entries {
		if entry.DataFile.SpecID != 0 {
			return entry.DataFile.SpecID
		}
	}
	return 0
}

type dmlManifestMetrics struct {
	addedFiles, existingFiles, deletedFiles int
	addedRows, existingRows, deletedRows    int64
	addedBytes, existingBytes, deletedBytes int64
}

func aggregateDMLManifestEntries(entries []api.ManifestEntry) (dmlManifestMetrics, error) {
	var metrics dmlManifestMetrics
	for _, entry := range entries {
		file := entry.DataFile
		if strings.TrimSpace(file.FilePath) == "" {
			return dmlManifestMetrics{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest entry requires a file path", nil)
		}
		if file.Content < api.DataFileContentData || file.Content > api.DataFileContentEqualityDelete ||
			entry.SequenceNumber < 0 || entry.FileSequence < 0 || file.RecordCount < 0 || file.FileSizeInBytes < 0 ||
			(file.RecordCount > 0 && file.FileSizeInBytes == 0) {
			return dmlManifestMetrics{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest entry has negative sequence or file metrics", map[string]string{
				"file": api.RedactPath(file.FilePath),
			})
		}
		var count *int
		var rows, bytes *int64
		switch entry.Status {
		case api.ManifestEntryAdded:
			count, rows, bytes = &metrics.addedFiles, &metrics.addedRows, &metrics.addedBytes
		case api.ManifestEntryExisting:
			count, rows, bytes = &metrics.existingFiles, &metrics.existingRows, &metrics.existingBytes
		case api.ManifestEntryDeleted:
			count, rows, bytes = &metrics.deletedFiles, &metrics.deletedRows, &metrics.deletedBytes
		default:
			return dmlManifestMetrics{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest entry status is invalid", map[string]string{
				"file":   api.RedactPath(file.FilePath),
				"status": strconv.Itoa(int(entry.Status)),
			})
		}
		if file.RecordCount > math.MaxInt64-*rows || file.FileSizeInBytes > math.MaxInt64-*bytes {
			return dmlManifestMetrics{}, api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest aggregate metrics overflow", map[string]string{
				"file": api.RedactPath(file.FilePath),
			})
		}
		*count = *count + 1
		*rows += file.RecordCount
		*bytes += file.FileSizeInBytes
	}
	return metrics, nil
}

func validateDMLManifestContent(entries []api.ManifestEntry, content api.ManifestContent) error {
	for _, entry := range entries {
		valid := entry.DataFile.Content == api.DataFileContentData
		if content == api.ManifestContentDeletes {
			valid = entry.DataFile.Content == api.DataFileContentPositionDelete || entry.DataFile.Content == api.DataFileContentEqualityDelete
		}
		if !valid {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg DML manifest entry content does not match its manifest", map[string]string{
				"file": api.RedactPath(entry.DataFile.FilePath),
			})
		}
	}
	return nil
}

type manifestMaterializeBudget struct {
	limit int64
	used  int64
}

func newManifestMaterializeBudget(limit, initial int64) (*manifestMaterializeBudget, error) {
	if initial < 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML initial memory usage cannot be negative", nil)
	}
	budget := &manifestMaterializeBudget{limit: limit, used: initial}
	if limit > 0 && initial > limit {
		return nil, dmlMaterializeMemoryExceeded(initial, 0, limit)
	}
	return budget, nil
}

func (b *manifestMaterializeBudget) reserve(bytes int64) error {
	if b == nil || bytes <= 0 {
		return nil
	}
	if b.limit > 0 && (b.used > b.limit || bytes > b.limit-b.used) {
		return dmlMaterializeMemoryExceeded(b.used, bytes, b.limit)
	}
	b.used = saturatingMaterializeAdd(b.used, bytes)
	return nil
}

func (b *manifestMaterializeBudget) release(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
}

func (b *manifestMaterializeBudget) encodeManifest(entries []api.ManifestEntry, opts metadata.ManifestWriteOptions) ([]byte, error) {
	if b == nil || b.limit <= 0 {
		return metadata.EncodeManifest(entries, opts)
	}
	scratch := metadata.ManifestEntriesMemoryWeight(0, entries)
	remaining, err := b.remainingAfterScratch(scratch)
	if err != nil {
		return nil, err
	}
	payload, err := metadata.EncodeManifestBounded(entries, opts, remaining)
	if err != nil {
		return nil, err
	}
	if err := b.reserve(int64(cap(payload))); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *manifestMaterializeBudget) encodeManifestList(manifests []api.ManifestFile, opts metadata.ManifestListWriteOptions) ([]byte, error) {
	if b == nil || b.limit <= 0 {
		return metadata.EncodeManifestList(manifests, opts)
	}
	scratch := metadata.ManifestListMemoryWeight(0, manifests)
	remaining, err := b.remainingAfterScratch(scratch)
	if err != nil {
		return nil, err
	}
	payload, err := metadata.EncodeManifestListBounded(manifests, opts, remaining)
	if err != nil {
		return nil, err
	}
	if err := b.reserve(int64(cap(payload))); err != nil {
		return nil, err
	}
	return payload, nil
}

func (b *manifestMaterializeBudget) remainingAfterScratch(scratch int64) (int64, error) {
	if b == nil || b.limit <= 0 {
		return math.MaxInt64, nil
	}
	if scratch < 0 || b.used > b.limit || scratch >= b.limit-b.used {
		return 0, dmlMaterializeMemoryExceeded(b.used, scratch, b.limit)
	}
	return b.limit - b.used - scratch, nil
}

func estimateIntentEntryMemory(intent CommitIntent) int64 {
	var total int64
	addFile := func(file api.DataFile) {
		total = saturatingMaterializeAdd(total, metadata.ManifestEntriesMemoryWeight(0, []api.ManifestEntry{{DataFile: file}}))
	}
	for _, action := range intent.Actions {
		switch action.Kind {
		case ActionAppendData:
			addFile(action.File)
		case ActionAddEqualityDelete, ActionAddPositionDelete:
			addFile(action.DeleteFile)
		case ActionRewriteDataFile:
			addFile(action.ReplacedFile)
			for _, replacement := range action.ReplacementFiles {
				addFile(replacement)
			}
		case ActionDeleteDataFile:
			addFile(action.ReplacedFile)
		}
	}
	return total
}

func estimatePreservedDerivationMemory(req ManifestMaterializeRequest, dataEntries []api.ManifestEntry) int64 {
	// The source entries and manifests remain live while filtering creates path
	// indexes and retained-entry slices. Full weights intentionally over-count
	// aliased strings/maps; this is a safety limit, not heap profiling.
	total := metadata.ManifestListMemoryWeight(0, req.PreservedManifests)
	total = saturatingMaterializeAdd(total, metadata.ManifestEntriesMemoryWeight(0, dataEntries))
	for _, source := range req.PreservedSources {
		total = saturatingMaterializeAdd(total, metadata.ManifestEntriesMemoryWeight(0, source.Entries))
	}
	return total
}

func dmlMaterializeMemoryExceeded(used, requested, limit int64) error {
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg DML manifest materialization memory limit exceeded", map[string]string{
		"used_bytes":      strconv.FormatInt(used, 10),
		"requested_bytes": strconv.FormatInt(requested, 10),
		"limit_bytes":     strconv.FormatInt(limit, 10),
	})
}

func saturatingMaterializeAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}
