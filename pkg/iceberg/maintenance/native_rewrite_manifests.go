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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type RewriteManifestsMaterializer struct {
	Metadata       api.MetadataFacade
	ObjectReader   api.ObjectReader
	PathPrefix     string
	MaxMemoryBytes int64
}

type RewriteManifestsMaterializeRequest struct {
	Metadata       *api.TableMetadata
	Snapshot       api.Snapshot
	SnapshotID     int64
	SequenceNumber int64
	JobID          string
	IdempotencyKey string
}

type RewriteManifestsMaterializeResult struct {
	Manifests              []api.ManifestFile
	ManifestListPath       string
	Objects                []ObjectWrite
	PostCommitOrphanPaths  []string
	RewrittenManifestCount uint64
}

type NativeRewriteManifestsPlanner struct {
	Catalog      api.CatalogRequest
	Loader       MaintenanceTableMetadataLoader
	Now          func() time.Time
	Materializer RewriteManifestsMaterializer
}

func (p NativeRewriteManifestsPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationRewriteManifests {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg native rewrite-manifests planner only supports rewrite_manifests", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-manifests planner requires a metadata loader", nil)
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
	now := maintenanceNow(p.Now)
	newSnapshotID := nextMaintenanceSnapshotID(now, meta)
	sequenceNumber := nextMaintenanceSequenceNumber(meta)
	materialized, err := p.Materializer.Materialize(ctx, RewriteManifestsMaterializeRequest{
		Metadata:       meta,
		Snapshot:       snapshot,
		SnapshotID:     newSnapshotID,
		SequenceNumber: sequenceNumber,
		JobID:          req.JobID,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	summary := map[string]string{
		"operation":           string(OperationRewriteManifests),
		"engine":              "matrixone",
		"idempotency-key":     firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"base-snapshot":       strconv.FormatInt(snapshot.SnapshotID, 10),
		"rewritten-manifests": strconv.FormatUint(materialized.RewrittenManifestCount, 10),
	}
	updates := []api.CommitUpdate{
		api.NewAddSnapshotUpdate(api.NewCommitSnapshot(
			newSnapshotID,
			snapshot.SnapshotID,
			sequenceNumber,
			meta.CurrentSchemaID,
			now.UnixMilli(),
			materialized.ManifestListPath,
			summary,
		)),
		api.NewSetSnapshotRefUpdatePreservingRetention(targetRef, req.TargetRefType, newSnapshotID, meta.Refs[targetRef]),
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
			ManifestFiles:  append([]api.ManifestFile(nil), materialized.Manifests...),
			Summary:        summary,
			IdempotencyKey: firstNonEmptyString(req.IdempotencyKey, req.JobID),
			BaseSnapshotID: snapshot.SnapshotID,
			TargetRef:      targetRef,
			TargetRefType:  req.TargetRefType,
		},
		Objects:            append([]ObjectWrite(nil), materialized.Objects...),
		PostCommitOrphans:  append([]string(nil), materialized.PostCommitOrphanPaths...),
		RewrittenFileCount: materialized.RewrittenManifestCount,
	}, nil
}

func (m RewriteManifestsMaterializer) Materialize(ctx context.Context, req RewriteManifestsMaterializeRequest) (*RewriteManifestsMaterializeResult, error) {
	if m.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests materializer requires an object reader", nil)
	}
	if req.SnapshotID <= 0 || req.SequenceNumber < 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests materializer requires snapshot and sequence numbers", nil)
	}
	if _, err := currentMaintenanceSchema(req.Metadata); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Snapshot.ManifestList) == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer requires a target manifest list", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	facade := m.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	memoryLimit := maintenanceMemoryLimit(m.MaxMemoryBytes)
	var memoryUsed int64
	manifestListData, err := readMaintenanceMetadataObject(ctx, m.ObjectReader, req.Snapshot.ManifestList, memoryLimit)
	if err != nil {
		if isMaintenancePlanningLimit(err) {
			return nil, err
		}
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-manifests materializer failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
		}, err)
	}
	manifests, err := readMaintenanceManifestList(
		ctx, facade, manifestListData,
		maintenanceRecordLimit(memoryLimit, 512),
		memoryLimit,
	)
	if err != nil {
		return nil, err
	}
	memoryUsed = metadata.ManifestListMemoryWeight(cap(manifestListData), manifests)
	if err := checkMaintenanceMemory(0, memoryUsed, memoryLimit); err != nil {
		return nil, err
	}
	if len(manifests) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer found an empty manifest list", map[string]string{
			"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
		})
	}
	basePath, err := m.rewriteBasePath(req)
	if err != nil {
		return nil, err
	}
	// The result retains a second manifest slice, ObjectWrite headers and orphan
	// path headers alongside the decoded source list. Reserve those capacities
	// before allocating them; payload capacities are charged as they are encoded.
	resultCapacityMemory := saturatingMaintenanceAdd(
		metadata.ManifestListMemoryWeight(0, manifests),
		saturatingMaintenanceMul(saturatingMaintenanceAdd(int64(len(manifests)), 1), 128),
	)
	if err := reserveMaintenanceMemory(&memoryUsed, resultCapacityMemory, memoryLimit); err != nil {
		return nil, err
	}
	result := &RewriteManifestsMaterializeResult{
		Manifests:             make([]api.ManifestFile, 0, len(manifests)),
		Objects:               make([]ObjectWrite, 0, len(manifests)+1),
		PostCommitOrphanPaths: []string{req.Snapshot.ManifestList},
	}
	for idx, manifest := range manifests {
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer found manifest without path", map[string]string{
				"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
			})
		}
		manifestData, err := readMaintenanceMetadataObject(ctx, m.ObjectReader, manifestPath, memoryLimit-memoryUsed)
		if err != nil {
			if isMaintenancePlanningLimit(err) {
				return nil, err
			}
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-manifests materializer failed to read manifest", map[string]string{
				"manifest": api.RedactPath(manifestPath),
			}, err)
		}
		entries, err := readMaintenanceManifest(
			ctx, facade, manifestData,
			maintenanceRecordLimit(memoryLimit-memoryUsed, 1024),
			memoryLimit-memoryUsed,
		)
		if err != nil {
			return nil, err
		}
		entryMemory := metadata.ManifestEntriesMemoryWeight(cap(manifestData), entries)
		if err := checkMaintenanceMemory(memoryUsed, entryMemory, memoryLimit); err != nil {
			return nil, err
		}
		memoryUsed += entryMemory
		entries = liveRewriteManifestEntries(entries, manifest)
		if len(entries) == 0 {
			// Deleted entries describe historical changes, not live files. A
			// rewrite may drop a manifest that contains no live entries.
			result.PostCommitOrphanPaths = append(result.PostCommitOrphanPaths, manifestPath)
			continue
		}
		rewritePath := joinObjectPath(basePath, fmt.Sprintf("manifest-%05d.avro", idx))
		content := manifest.Content
		if content == "" {
			content = api.ManifestContentData
		}
		writeOpts, err := maintenanceManifestWriteOptions(req.Metadata, manifest.PartitionSpecID, content)
		if err != nil {
			return nil, err
		}
		encoderScratch := metadata.ManifestEntriesMemoryWeight(0, entries)
		if err := checkMaintenanceMemory(memoryUsed, encoderScratch, memoryLimit); err != nil {
			return nil, err
		}
		// hamba/avro keeps one encoded OCF block while the flushed output is
		// retained. Reserve a decoded-entry-sized scratch allowance and give only
		// the remainder to a capacity-bounded output buffer.
		rewriteBuffer := boundedMaintenanceBuffer{maxBytes: memoryLimit - memoryUsed - encoderScratch}
		err = metadata.WriteManifest(&rewriteBuffer, entries, writeOpts)
		if err != nil {
			if rewriteBuffer.exceeded {
				return nil, checkMaintenanceMemory(memoryLimit, 1, memoryLimit)
			}
			return nil, err
		}
		rewriteData := rewriteBuffer.Bytes()
		rewrittenManifest, err := rewrittenMaintenanceManifest(req, manifest, entries)
		if err != nil {
			return nil, err
		}
		rewrittenManifest.Path = rewritePath
		rewrittenManifest.Length = int64(len(rewriteData))
		rewrittenManifest.ManifestPathRedacted = api.RedactPath(rewritePath)
		rewrittenManifest.ManifestPathHash = api.PathHash(rewritePath)
		retainedOutput := int64(cap(rewriteData))
		if err := checkMaintenanceMemory(memoryUsed, retainedOutput, memoryLimit); err != nil {
			return nil, err
		}
		memoryUsed += retainedOutput
		result.Manifests = append(result.Manifests, rewrittenManifest)
		result.Objects = append(result.Objects, ObjectWrite{Location: rewritePath, Payload: rewriteData})
		result.PostCommitOrphanPaths = append(result.PostCommitOrphanPaths, manifestPath)
	}
	manifestListPath := joinObjectPath(basePath, "manifest-list.avro")
	parentSnapshotID := req.Snapshot.SnapshotID
	encoderScratch := metadata.ManifestListMemoryWeight(0, result.Manifests)
	if err := checkMaintenanceMemory(memoryUsed, encoderScratch, memoryLimit); err != nil {
		return nil, err
	}
	manifestListBuffer := boundedMaintenanceBuffer{maxBytes: memoryLimit - memoryUsed - encoderScratch}
	err = metadata.WriteManifestList(&manifestListBuffer, result.Manifests, metadata.ManifestListWriteOptions{
		FormatVersion:    req.Metadata.FormatVersion,
		SnapshotID:       req.SnapshotID,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   req.SequenceNumber,
	})
	if err != nil {
		if manifestListBuffer.exceeded {
			return nil, checkMaintenanceMemory(memoryLimit, 1, memoryLimit)
		}
		return nil, err
	}
	manifestListBytes := manifestListBuffer.Bytes()
	if err := checkMaintenanceMemory(memoryUsed, int64(cap(manifestListBytes)), memoryLimit); err != nil {
		return nil, err
	}
	memoryUsed += int64(cap(manifestListBytes))
	result.ManifestListPath = manifestListPath
	result.Objects = append(result.Objects, ObjectWrite{Location: manifestListPath, Payload: manifestListBytes})
	result.PostCommitOrphanPaths = dedupeNonEmptyStrings(result.PostCommitOrphanPaths)
	result.RewrittenManifestCount = uint64(len(result.Manifests))
	return result, nil
}

func liveRewriteManifestEntries(entries []api.ManifestEntry, manifest api.ManifestFile) []api.ManifestEntry {
	out := make([]api.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Status == api.ManifestEntryDeleted {
			continue
		}
		entry.Status = api.ManifestEntryExisting
		entry.SnapshotID = firstNonZeroMaintenanceInt64(entry.SnapshotID, manifest.AddedSnapshotID)
		entry.SequenceNumber = firstNonZeroMaintenanceInt64(entry.SequenceNumber, manifest.SequenceNumber)
		entry.FileSequence = firstNonZeroMaintenanceInt64(entry.FileSequence, manifest.SequenceNumber)
		entry.DataFile.SpecID = manifest.PartitionSpecID
		entry.DataFile.SequenceNumber = entry.SequenceNumber
		entry.DataFile.FileSequenceNumber = entry.FileSequence
		out = append(out, entry)
	}
	return out
}

func rewrittenMaintenanceManifest(req RewriteManifestsMaterializeRequest, original api.ManifestFile, entries []api.ManifestEntry) (api.ManifestFile, error) {
	manifest := original
	manifest.SequenceNumber = req.SequenceNumber
	manifest.AddedSnapshotID = req.SnapshotID
	manifest.AddedFilesCount = 0
	manifest.DeletedFilesCount = 0
	manifest.AddedRowsCount = 0
	manifest.DeletedRowsCount = 0
	manifest.AddedFilesSizeInBytes = 0
	manifest.DeletedFilesSizeInBytes = 0
	manifest.ExistingFilesCount = len(entries)
	manifest.ExistingRowsCount = 0
	manifest.ExistingFilesSizeInBytes = 0
	manifest.MinSequenceNumber = 0
	referenced := make(map[string]struct{})
	allPositionReferencesKnown := true
	for idx, entry := range entries {
		if entry.SequenceNumber < 0 || entry.FileSequence < 0 || entry.DataFile.RecordCount < 0 || entry.DataFile.FileSizeInBytes < 0 {
			return api.ManifestFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests found negative sequence or file metrics", map[string]string{
				"file": api.RedactPath(entry.DataFile.FilePath),
			})
		}
		if entry.DataFile.RecordCount > math.MaxInt64-manifest.ExistingRowsCount || entry.DataFile.FileSizeInBytes > math.MaxInt64-manifest.ExistingFilesSizeInBytes {
			return api.ManifestFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests aggregate metrics overflow", map[string]string{
				"file": api.RedactPath(entry.DataFile.FilePath),
			})
		}
		manifest.ExistingRowsCount += entry.DataFile.RecordCount
		manifest.ExistingFilesSizeInBytes += entry.DataFile.FileSizeInBytes
		if idx == 0 || entry.SequenceNumber < manifest.MinSequenceNumber {
			manifest.MinSequenceNumber = entry.SequenceNumber
		}
		if entry.DataFile.Content == api.DataFileContentPositionDelete {
			if path := strings.TrimSpace(entry.DataFile.ReferencedDataFile); path != "" {
				referenced[path] = struct{}{}
			} else {
				allPositionReferencesKnown = false
			}
		}
	}
	if allPositionReferencesKnown {
		manifest.ReferencedDataFilesCount = len(referenced)
	} else {
		// referenced_data_file is optional for position deletes; the paths may
		// live only inside the delete Parquet rows. Preserve the catalog-provided
		// aggregate when this metadata-only rewrite cannot recompute it exactly.
		manifest.ReferencedDataFilesCount = original.ReferencedDataFilesCount
	}
	return manifest, nil
}

func firstNonZeroMaintenanceInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func (m RewriteManifestsMaterializer) rewriteBasePath(req RewriteManifestsMaterializeRequest) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(m.PathPrefix), "/")
	if base == "" && req.Metadata != nil && strings.TrimSpace(req.Metadata.Location) != "" {
		base = joinObjectPath(req.Metadata.Location, "metadata")
	}
	if base == "" {
		base = objectDir(req.Snapshot.ManifestList)
	}
	if base == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests materializer requires table location, manifest list path, or path prefix", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	return joinObjectPath(base, "mo-rewrite-manifests", rewriteManifestsID(req)), nil
}

func rewriteManifestsID(req RewriteManifestsMaterializeRequest) string {
	raw := firstNonEmptyString(req.IdempotencyKey, req.JobID, strconv.FormatInt(req.Snapshot.SnapshotID, 10))
	if raw == "" {
		raw = "rewrite-manifests"
	}
	return "rw-" + api.PathHash(raw)
}

func joinObjectPath(base string, parts ...string) string {
	out := strings.TrimRight(strings.TrimSpace(base), "/")
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), "/")
		if part == "" {
			continue
		}
		if out == "" {
			out = part
		} else {
			out += "/" + part
		}
	}
	return out
}

func objectDir(location string) string {
	location = strings.TrimSpace(location)
	if location == "" {
		return ""
	}
	idx := strings.LastIndex(location, "/")
	if idx <= 0 {
		return ""
	}
	return strings.TrimRight(location[:idx], "/")
}
