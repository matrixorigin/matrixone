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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type ParquetConcatRewriteDataFilesCompactor struct {
	ObjectReader   api.ObjectReader
	Metadata       api.MetadataFacade
	OutputPrefix   string
	MaxMemoryBytes int64
}

type rewriteDataFilesDeleteFilters map[string]rewriteDataFilesFileDeleteFilter

type rewriteDataFilesFileDeleteFilter struct {
	Equality  []rewriteDataFilesEqualityDeleteFilter
	Positions map[int64]struct{}
}

type rewriteDataFilesEqualityDeleteFilter struct {
	EqualityIDs []int
	Keys        map[string]struct{}
}

const (
	// parquet-go maintains one page buffer per leaf column plus shared encode
	// scratch space. Keep those buffers deliberately small because this rewrite
	// implementation still materializes its output. A future spillable writer
	// can replace this conservative reservation and permit larger row groups.
	rewriteDataFilesPageBufferBytes = 64 << 10
	rewriteDataFilesRowsPerRowGroup = 1024
)

func (c ParquetConcatRewriteDataFilesCompactor) SupportsDeleteManifests() bool {
	return true
}

func (c ParquetConcatRewriteDataFilesCompactor) CompactRewriteDataFiles(ctx context.Context, req RewriteDataFilesCompactRequest) (*RewriteDataFilesCompactResult, error) {
	if c.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg parquet rewrite-data-files compactor requires an object reader", nil)
	}
	if req.Metadata == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg parquet rewrite-data-files compactor requires table metadata", nil)
	}
	if len(req.Selection.Groups) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor requires selected file groups", nil)
	}
	opts, err := parseRewriteDataFilesSelectionOptions(req.Options)
	if err != nil {
		return nil, err
	}
	deleteFilters, retainedBytes, err := c.buildDeleteFilters(ctx, req, req.Selection.RetainedMemoryBytes)
	if err != nil {
		return nil, err
	}
	memoryLimit := maintenanceMemoryLimit(c.MaxMemoryBytes)
	if retainedBytes >= memoryLimit {
		return nil, maintenanceMemoryExceeded(retainedBytes, memoryLimit)
	}
	out := &RewriteDataFilesCompactResult{
		Rewrites: make([]RewriteDataFileRewrite, 0, len(req.Selection.Groups)),
		Objects:  make([]ObjectWrite, 0, len(req.Selection.Groups)),
	}
	remainingMemory := uint64(memoryLimit - retainedBytes)
	remaining := opts.maxRewriteBytes
	if remainingMemory < remaining {
		remaining = remainingMemory
	}
	for idx, group := range req.Selection.Groups {
		rewrite, object, err := c.compactGroup(ctx, req, group, idx, deleteFilters, remaining, int64(remainingMemory))
		if err != nil {
			return nil, err
		}
		if uint64(len(object.Payload)) > remaining {
			return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg rewrite-data-files materialized output exceeded its byte budget", map[string]string{
				"limit": strconv.FormatUint(opts.maxRewriteBytes, 10),
			})
		}
		remaining -= uint64(len(object.Payload))
		retainedOutputBytes := uint64(cap(object.Payload))
		if retainedOutputBytes > remainingMemory {
			return nil, maintenanceMemoryExceeded(int64(retainedOutputBytes), int64(remainingMemory))
		}
		remainingMemory -= retainedOutputBytes
		out.Rewrites = append(out.Rewrites, rewrite)
		out.Objects = append(out.Objects, object)
		out.OrphanPaths = append(out.OrphanPaths, object.Location)
	}
	out.RetainedMemoryBytes = memoryLimit - int64(remainingMemory)
	return out, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) buildDeleteFilters(ctx context.Context, req RewriteDataFilesCompactRequest, initialMemory int64) (rewriteDataFilesDeleteFilters, int64, error) {
	memoryLimit := maintenanceMemoryLimit(c.MaxMemoryBytes)
	memoryUsed := initialMemory
	if err := checkMaintenanceMemory(0, memoryUsed, memoryLimit); err != nil {
		return nil, memoryUsed, err
	}
	deleteManifests, err := c.rewriteDataFilesDeleteManifests(ctx, req, &memoryUsed, memoryLimit)
	if err != nil || len(deleteManifests) == 0 {
		return nil, memoryUsed, err
	}
	candidates := rewriteDataFilesCompactionCandidates(req.Selection.Groups)
	if len(candidates) == 0 {
		return nil, memoryUsed, nil
	}
	specFieldCounts := rewriteDataFilesSpecFieldCounts(req.Metadata)
	facade := c.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	filters := make(rewriteDataFilesDeleteFilters)
	for _, manifest := range deleteManifests {
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, memoryUsed, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor found delete manifest without path", map[string]string{
				"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
			})
		}
		manifestData, err := readMaintenanceMetadataObject(ctx, c.ObjectReader, manifestPath, memoryLimit-memoryUsed)
		if err != nil {
			if isMaintenancePlanningLimit(err) {
				return nil, memoryUsed, err
			}
			return nil, memoryUsed, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg parquet rewrite-data-files compactor failed to read delete manifest", map[string]string{
				"manifest": api.RedactPath(manifestPath),
			}, err)
		}
		entries, err := readMaintenanceManifest(
			ctx, facade, manifestData,
			maintenanceRecordLimit(memoryLimit-memoryUsed, 1024),
			memoryLimit-memoryUsed,
		)
		if err != nil {
			return nil, memoryUsed, err
		}
		if err := reserveMaintenanceMemory(&memoryUsed, metadata.ManifestEntriesMemoryWeight(cap(manifestData), entries), memoryLimit); err != nil {
			return nil, memoryUsed, err
		}
		for _, entry := range entries {
			if entry.Status == api.ManifestEntryDeleted {
				continue
			}
			file := entry.DataFile
			file.SpecID = manifest.PartitionSpecID
			if err := metadata.ValidateP1DeleteFile(file); err != nil {
				return nil, memoryUsed, err
			}
			if file.RecordCount == 0 {
				continue
			}
			affected, err := rewriteDataFilesDeleteAffectedFiles(file, candidates, specFieldCounts)
			if err != nil {
				return nil, memoryUsed, err
			}
			if len(affected) == 0 {
				continue
			}
			switch file.Content {
			case api.DataFileContentPositionDelete:
				positions, err := c.readPositionDeletePositions(ctx, file, affected, &memoryUsed, memoryLimit)
				if err != nil {
					return nil, memoryUsed, err
				}
				for path, filePositions := range positions {
					filter := filters[path]
					if filter.Positions == nil {
						if err := reserveMaintenanceMemory(&memoryUsed, int64(64+len(path)), memoryLimit); err != nil {
							return nil, memoryUsed, err
						}
						filter.Positions = make(map[int64]struct{}, len(filePositions))
					}
					for pos := range filePositions {
						if _, exists := filter.Positions[pos]; !exists {
							if err := reserveMaintenanceMemory(&memoryUsed, 32, memoryLimit); err != nil {
								return nil, memoryUsed, err
							}
						}
						filter.Positions[pos] = struct{}{}
					}
					filters[path] = filter
				}
			case api.DataFileContentEqualityDelete:
				keys, err := c.readEqualityDeleteKeys(ctx, file, &memoryUsed, memoryLimit)
				if err != nil {
					return nil, memoryUsed, err
				}
				equality := rewriteDataFilesEqualityDeleteFilter{
					EqualityIDs: append([]int(nil), file.EqualityIDs...),
					Keys:        keys,
				}
				for _, path := range affected {
					if err := reserveMaintenanceMemory(&memoryUsed, int64(64+len(path)+len(file.EqualityIDs)*8), memoryLimit); err != nil {
						return nil, memoryUsed, err
					}
					filter := filters[path]
					filter.Equality = append(filter.Equality, equality)
					filters[path] = filter
				}
			}
		}
	}
	if len(filters) == 0 {
		return nil, memoryUsed, nil
	}
	return filters, memoryUsed, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) rewriteDataFilesDeleteManifests(ctx context.Context, req RewriteDataFilesCompactRequest, memoryUsed *int64, memoryLimit int64) ([]api.ManifestFile, error) {
	if len(req.Selection.DeleteManifests) > 0 {
		return append([]api.ManifestFile(nil), req.Selection.DeleteManifests...), nil
	}
	if req.Selection.DeleteManifestCount > 0 {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg parquet rewrite-data-files compactor requires delete manifest metadata before rewriting tables with delete manifests", map[string]string{
			"snapshot_id":      strconv.FormatInt(req.Snapshot.SnapshotID, 10),
			"delete_manifests": strconv.Itoa(req.Selection.DeleteManifestCount),
		})
	}
	snapshot := req.Snapshot
	if strings.TrimSpace(snapshot.ManifestList) == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor requires a manifest list", map[string]string{
			"snapshot_id": strconv.FormatInt(snapshot.SnapshotID, 10),
		})
	}
	facade := c.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	manifestListData, err := readMaintenanceMetadataObject(ctx, c.ObjectReader, snapshot.ManifestList, memoryLimit-*memoryUsed)
	if err != nil {
		if isMaintenancePlanningLimit(err) {
			return nil, err
		}
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg parquet rewrite-data-files compactor failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(snapshot.ManifestList),
		}, err)
	}
	manifests, err := readMaintenanceManifestList(
		ctx, facade, manifestListData,
		maintenanceRecordLimit(memoryLimit-*memoryUsed, 512),
		memoryLimit-*memoryUsed,
	)
	if err != nil {
		return nil, err
	}
	if err := reserveMaintenanceMemory(memoryUsed, metadata.ManifestListMemoryWeight(cap(manifestListData), manifests), memoryLimit); err != nil {
		return nil, err
	}
	deleteManifests := make([]api.ManifestFile, 0)
	for _, manifest := range manifests {
		if manifest.Content == api.ManifestContentDeletes {
			deleteManifests = append(deleteManifests, manifest)
		}
	}
	return deleteManifests, nil
}

func rewriteDataFilesCompactionCandidates(groups []RewriteDataFileGroup) map[string]api.DataFile {
	out := make(map[string]api.DataFile)
	for _, group := range groups {
		for _, candidate := range group.Candidates {
			path := strings.TrimSpace(candidate.File.FilePath)
			if path != "" {
				out[path] = candidate.File
			}
		}
	}
	return out
}

func rewriteDataFilesSpecFieldCounts(meta *api.TableMetadata) map[int]int {
	if meta == nil {
		return nil
	}
	out := make(map[int]int, len(meta.PartitionSpecs))
	for _, spec := range meta.PartitionSpecs {
		out[spec.SpecID] = len(spec.Fields)
	}
	return out
}

func rewriteDataFilesDeleteAffectedFiles(deleteFile api.DataFile, dataByPath map[string]api.DataFile, specFieldCounts map[int]int) ([]string, error) {
	fieldCount, specKnown := specFieldCounts[deleteFile.SpecID]
	if !specKnown {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files delete file references an unknown partition spec", map[string]string{
			"file":    api.RedactPath(deleteFile.FilePath),
			"spec_id": strconv.Itoa(deleteFile.SpecID),
		})
	}
	globalDelete := fieldCount == 0
	affected := make([]string, 0)
	switch deleteFile.Content {
	case api.DataFileContentPositionDelete:
		referenced := strings.TrimSpace(deleteFile.ReferencedDataFile)
		if referenced != "" {
			dataFile, ok := dataByPath[referenced]
			if ok && rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile) {
				affected = append(affected, dataFile.FilePath)
			}
			break
		}
		// referenced_data_file is optional. In that form each delete row carries
		// its target path, so preselect every sequence-compatible candidate and
		// let readPositionDeletePositions filter by the actual row paths.
		for path, dataFile := range dataByPath {
			if rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile) {
				affected = append(affected, path)
			}
		}
	case api.DataFileContentEqualityDelete:
		for path, dataFile := range dataByPath {
			if !rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile) {
				continue
			}
			if !globalDelete {
				if deleteFile.SpecID != dataFile.SpecID {
					continue
				}
				if !rewriteDataFilesSamePartitionScope(deleteFile.Partition, dataFile.Partition) {
					continue
				}
			}
			affected = append(affected, path)
		}
	}
	sort.Strings(affected)
	return affected, nil
}

func rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile api.DataFile) bool {
	if deleteFile.SequenceNumber == 0 || dataFile.SequenceNumber == 0 {
		return true
	}
	if deleteFile.Content == api.DataFileContentPositionDelete {
		return deleteFile.SequenceNumber >= dataFile.SequenceNumber
	}
	return deleteFile.SequenceNumber > dataFile.SequenceNumber
}

func rewriteDataFilesSamePartitionScope(deletePartition, dataPartition map[string]any) bool {
	if len(deletePartition) == 0 {
		return true
	}
	if len(deletePartition) != len(dataPartition) {
		return false
	}
	for key, deleteValue := range deletePartition {
		dataValue, ok := dataPartition[key]
		if !ok {
			return false
		}
		if rewriteDataFilesPartitionScopeValue(deleteValue) != rewriteDataFilesPartitionScopeValue(dataValue) {
			return false
		}
	}
	return true
}

func rewriteDataFilesPartitionScopeValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "b:" + strconv.FormatBool(v)
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int8:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int16:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case uint:
		return rewriteDataFilesPartitionScopeUint(uint64(v))
	case uint8:
		return rewriteDataFilesPartitionScopeUint(uint64(v))
	case uint16:
		return rewriteDataFilesPartitionScopeUint(uint64(v))
	case uint32:
		return rewriteDataFilesPartitionScopeUint(uint64(v))
	case uint64:
		return rewriteDataFilesPartitionScopeUint(v)
	case float32:
		return "f:" + strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64)
	case string:
		return "s:" + v
	case []byte:
		return "bytes:" + string(v)
	default:
		return fmt.Sprintf("%T:%#v", value, value)
	}
}

func rewriteDataFilesPartitionScopeUint(value uint64) string {
	if value <= math.MaxInt64 {
		return "i:" + strconv.FormatInt(int64(value), 10)
	}
	return "u:" + strconv.FormatUint(value, 10)
}

func (c ParquetConcatRewriteDataFilesCompactor) compactGroup(ctx context.Context, req RewriteDataFilesCompactRequest, group RewriteDataFileGroup, groupIndex int, deleteFilters rewriteDataFilesDeleteFilters, maxOutputBytes uint64, maxMemoryBytes int64) (RewriteDataFileRewrite, ObjectWrite, error) {
	if len(group.Candidates) == 0 {
		return RewriteDataFileRewrite{}, ObjectWrite{}, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor received an empty group", nil)
	}
	output := boundedRewriteBuffer{maxBytes: maxOutputBytes, maxMemoryBytes: uint64(maxMemoryBytes)}
	var writer *parquet.Writer
	var writerSchema *parquet.Schema
	var writerReservedBytes uint64
	var totalRows int64
	for _, candidate := range group.Candidates {
		if candidate.File.FileSizeInBytes <= 0 {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files candidate is missing file size", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			})
		}
		retainedBeforeRead, overflow := addRewriteMemory(uint64(cap(output.data)), writerReservedBytes)
		if overflow || retainedBeforeRead > uint64(maxMemoryBytes) {
			return RewriteDataFileRewrite{}, ObjectWrite{}, maintenanceMemoryExceeded(math.MaxInt64, maxMemoryBytes)
		}
		availableInputBytes := uint64(maxMemoryBytes) - retainedBeforeRead
		if uint64(candidate.File.FileSizeInBytes) > availableInputBytes {
			// Reject before the read. Checking after materialization would permit the
			// retained output + writer + next input to transiently cross the bound.
			actual := int64(math.MaxInt64)
			if retainedBeforeRead <= uint64(math.MaxInt64-candidate.File.FileSizeInBytes) {
				actual = candidate.File.FileSizeInBytes + int64(retainedBeforeRead)
			}
			return RewriteDataFileRewrite{}, ObjectWrite{}, maintenanceMemoryExceeded(actual, maxMemoryBytes)
		}
		data, err := readMaintenanceMetadataObject(ctx, c.ObjectReader, candidate.File.FilePath, int64(availableInputBytes))
		if err != nil {
			if isMaintenancePlanningLimit(err) {
				return RewriteDataFileRewrite{}, ObjectWrite{}, err
			}
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to read data file", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
		if int64(len(data)) != candidate.File.FileSizeInBytes {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files object size does not match manifest metadata", map[string]string{
				"file":          api.RedactPath(candidate.File.FilePath),
				"metadata_size": strconv.FormatInt(candidate.File.FileSizeInBytes, 10),
				"actual_size":   strconv.Itoa(len(data)),
			})
		}
		output.reservedBytes = uint64(cap(data)) + writerReservedBytes
		if err := output.checkMemory(uint64(cap(output.data))); err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, err
		}
		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor failed to open data file", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
		initializeWriter := writer == nil
		if initializeWriter {
			schema, err := rewriteDataFilesWriterSchema(req.Metadata, file.Schema())
			if err != nil {
				return RewriteDataFileRewrite{}, ObjectWrite{}, err
			}
			writerSchema = schema
			writerReservedBytes, err = rewriteDataFilesWriterMemoryReservation(schema)
			if err != nil {
				return RewriteDataFileRewrite{}, ObjectWrite{}, err
			}
		}
		decodeBytes, err := rewriteDataFilesMaxRowGroupBytes(file)
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, err
		}
		workingBytes, overflow := addRewriteMemory(uint64(cap(data)), decodeBytes)
		if !overflow {
			workingBytes, overflow = addRewriteMemory(workingBytes, writerReservedBytes)
		}
		if overflow {
			return RewriteDataFileRewrite{}, ObjectWrite{}, maintenanceMemoryExceeded(math.MaxInt64, maxMemoryBytes)
		}
		output.reservedBytes = workingBytes
		if err := output.checkMemory(uint64(cap(output.data))); err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, err
		}
		if initializeWriter {
			writer = parquet.NewWriter(
				&output,
				writerSchema,
				parquet.PageBufferSize(rewriteDataFilesPageBufferBytes),
				parquet.WriteBufferSize(0),
				parquet.MaxRowsPerRowGroup(rewriteDataFilesRowsPerRowGroup),
			)
		}
		conv, err := parquet.Convert(writerSchema, file.Schema())
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor failed to map data file schema", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
		n, err := c.copyCandidateRows(writer, conv, file, candidate, deleteFilters[candidate.File.FilePath])
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, err
		}
		output.reservedBytes = writerReservedBytes
		totalRows += n
	}
	if writer == nil {
		return RewriteDataFileRewrite{}, ObjectWrite{}, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor found no readable parquet rows", nil)
	}
	if err := writer.Close(); err != nil {
		return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to close output file", nil, err)
	}
	location, err := c.outputLocation(req, group, groupIndex)
	if err != nil {
		return RewriteDataFileRewrite{}, ObjectWrite{}, err
	}
	replacement := api.DataFile{
		Content:          api.DataFileContentData,
		FilePath:         location,
		FileFormat:       "parquet",
		Partition:        cloneRewriteDataFilesPartition(group.Candidates[0].File.Partition),
		RecordCount:      totalRows,
		FileSizeInBytes:  int64(output.Len()),
		SpecID:           group.PartitionSpecID,
		FilePathRedacted: api.RedactPath(location),
		FilePathHash:     api.PathHash(location),
	}
	return RewriteDataFileRewrite{
			Group:            group,
			ReplacementFiles: []api.DataFile{replacement},
		}, ObjectWrite{
			Location: location,
			Payload:  output.Bytes(),
		}, nil
}

type boundedRewriteBuffer struct {
	data           []byte
	maxBytes       uint64
	maxMemoryBytes uint64
	reservedBytes  uint64
}

func (b *boundedRewriteBuffer) Write(p []byte) (int, error) {
	current := uint64(len(b.data))
	if current > b.maxBytes || uint64(len(p)) > b.maxBytes-current {
		return 0, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg rewrite-data-files output exceeded its byte budget", map[string]string{
			"limit": strconv.FormatUint(b.maxBytes, 10),
		})
	}
	required := current + uint64(len(p))
	if required > uint64(math.MaxInt) {
		return 0, b.memoryExceeded()
	}
	capacity := uint64(cap(b.data))
	if required > capacity {
		capacity = required
		if doubled := uint64(cap(b.data)) * 2; doubled > capacity {
			capacity = doubled
		}
		if capacity > uint64(math.MaxInt) {
			capacity = uint64(math.MaxInt)
		}
		if b.maxMemoryBytes > 0 {
			if b.reservedBytes > b.maxMemoryBytes {
				return 0, b.memoryExceeded()
			}
			available := b.maxMemoryBytes - b.reservedBytes
			if required > available {
				return 0, b.memoryExceeded()
			}
			if capacity > available {
				capacity = available
			}
		}
	}
	if err := b.checkMemory(capacity); err != nil {
		return 0, err
	}
	if required > uint64(cap(b.data)) {
		// make+copy keeps both arrays live. reservedBytes covers input and
		// parquet writer state; add both output capacities for the true peak.
		if b.maxMemoryBytes > 0 {
			oldCapacity := uint64(cap(b.data))
			if b.reservedBytes > b.maxMemoryBytes || oldCapacity > b.maxMemoryBytes-b.reservedBytes || capacity > b.maxMemoryBytes-b.reservedBytes-oldCapacity {
				return 0, b.memoryExceeded()
			}
		}
		next := make([]byte, len(b.data), int(capacity))
		copy(next, b.data)
		b.data = next
	}
	oldLen := len(b.data)
	b.data = b.data[:int(required)]
	copy(b.data[oldLen:], p)
	return len(p), nil
}

func (b *boundedRewriteBuffer) Len() int {
	return len(b.data)
}

func (b *boundedRewriteBuffer) Bytes() []byte {
	return b.data
}

func (b *boundedRewriteBuffer) checkMemory(outputCapacity uint64) error {
	if b.maxMemoryBytes > 0 && (b.reservedBytes > b.maxMemoryBytes || outputCapacity > b.maxMemoryBytes-b.reservedBytes) {
		return b.memoryExceeded()
	}
	return nil
}

func (b *boundedRewriteBuffer) memoryExceeded() error {
	return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg rewrite-data-files materialized working set exceeded the memory budget", map[string]string{
		"limit": strconv.FormatUint(b.maxMemoryBytes, 10),
	})
}

func rewriteDataFilesWriterMemoryReservation(schema *parquet.Schema) (uint64, error) {
	if schema == nil {
		return 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files writer requires a schema", nil)
	}
	// The two extra pages cover parquet-go's shared encode/compression scratch
	// buffers. Schema/index bookkeeping is bounded by the materialized input and
	// output; this reservation covers the large reusable payload buffers.
	columns := uint64(len(schema.Columns()))
	if columns > math.MaxUint64/rewriteDataFilesPageBufferBytes-2 {
		return 0, maintenanceMemoryExceeded(math.MaxInt64, math.MaxInt64)
	}
	return (columns + 2) * rewriteDataFilesPageBufferBytes, nil
}

func rewriteDataFilesMaxRowGroupBytes(file *parquet.File) (uint64, error) {
	if file == nil || file.Metadata() == nil {
		return 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor requires Parquet metadata", nil)
	}
	var maxBytes uint64
	for _, rowGroup := range file.Metadata().RowGroups {
		if rowGroup.TotalByteSize < 0 {
			return 0, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor found a negative Parquet row-group size", nil)
		}
		if size := uint64(rowGroup.TotalByteSize); size > maxBytes {
			maxBytes = size
		}
	}
	return maxBytes, nil
}

func addRewriteMemory(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, true
	}
	return left + right, false
}

func (c ParquetConcatRewriteDataFilesCompactor) outputLocation(req RewriteDataFilesCompactRequest, group RewriteDataFileGroup, groupIndex int) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(c.OutputPrefix), "/")
	if base == "" && req.Metadata != nil && strings.TrimSpace(req.Metadata.Location) != "" {
		base = joinObjectPath(req.Metadata.Location, "data")
	}
	if base == "" {
		base = objectDir(req.Snapshot.ManifestList)
	}
	if base == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg parquet rewrite-data-files compactor requires table location, manifest list path, or output prefix", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	rewriteID := "rw-" + api.PathHash(firstNonEmptyString(req.IdempotencyKey, req.JobID, strconv.FormatInt(req.Snapshot.SnapshotID, 10)))
	fileName := "group-" + strconv.Itoa(groupIndex+1) + "-" + api.PathHash(group.PartitionKey) + ".parquet"
	return joinObjectPath(base, "mo-rewrite-data-files", rewriteID, fileName), nil
}

func rewriteDataFilesWriterSchema(meta *api.TableMetadata, fallback *parquet.Schema) (*parquet.Schema, error) {
	if meta != nil {
		schema, ok := meta.CurrentSchema()
		if ok && len(schema.Fields) > 0 {
			group := make(parquet.Group, len(schema.Fields))
			for _, field := range schema.Fields {
				node, err := rewriteDataFilesParquetNodeForField(field)
				if err != nil {
					return nil, err
				}
				group[field.Name] = parquet.FieldID(node, field.ID)
			}
			return parquet.NewSchema("iceberg", group), nil
		}
	}
	if fallback != nil {
		node, err := rewriteDataFilesPlainParquetNode(fallback)
		if err != nil {
			return nil, err
		}
		return parquet.NewSchema(firstNonEmptyString(fallback.Name(), "iceberg"), node), nil
	}
	return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor requires a current schema or fallback parquet schema", nil)
}

func rewriteDataFilesParquetNodeForField(field api.SchemaField) (parquet.Node, error) {
	var node parquet.Node
	switch field.Type.Kind {
	case api.TypeBoolean:
		node = parquet.Leaf(parquet.BooleanType)
	case api.TypeInt:
		node = parquet.Int(32)
	case api.TypeLong:
		node = parquet.Int(64)
	case api.TypeFloat:
		node = parquet.Leaf(parquet.FloatType)
	case api.TypeDouble:
		node = parquet.Leaf(parquet.DoubleType)
	case api.TypeString:
		node = parquet.Encoded(parquet.String(), &parquet.Plain)
	case api.TypeDate:
		node = parquet.Date()
	case api.TypeTimestamp:
		node = parquet.TimestampAdjusted(parquet.Microsecond, false)
	case api.TypeTimestampTZ:
		node = parquet.Timestamp(parquet.Microsecond)
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg rewrite-data-files writer type is unsupported", map[string]string{
			"field": field.Name,
			"type":  field.Type.String(),
		})
	}
	if field.Required {
		return parquet.Required(node), nil
	}
	return parquet.Optional(node), nil
}

func rewriteDataFilesPlainParquetNode(node parquet.Node) (parquet.Node, error) {
	if node == nil {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor found nil parquet schema node", nil)
	}
	var out parquet.Node
	if node.Leaf() {
		out = parquet.Leaf(node.Type())
	} else {
		group := make(parquet.Group, len(node.Fields()))
		for _, field := range node.Fields() {
			child, err := rewriteDataFilesPlainParquetNode(field)
			if err != nil {
				return nil, err
			}
			group[field.Name()] = child
		}
		out = group
	}
	if id := node.ID(); id != 0 {
		out = parquet.FieldID(out, id)
	}
	switch {
	case node.Optional():
		out = parquet.Optional(out)
	case node.Repeated():
		out = parquet.Repeated(out)
	default:
		out = parquet.Required(out)
	}
	return out, nil
}

func cloneRewriteDataFilesPartition(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

var _ RewriteDataFilesCompactor = ParquetConcatRewriteDataFilesCompactor{}

func (c ParquetConcatRewriteDataFilesCompactor) readPositionDeletePositions(ctx context.Context, deleteFile api.DataFile, affected []string, memoryUsed *int64, memoryLimit int64) (map[string]map[int64]struct{}, error) {
	deletePath := strings.TrimSpace(deleteFile.FilePath)
	if deletePath == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete file path is required before rewrite-data-files delete apply", nil)
	}
	affectedSet := make(map[string]struct{}, len(affected))
	for _, path := range affected {
		if trimmed := strings.TrimSpace(path); trimmed != "" {
			affectedSet[trimmed] = struct{}{}
		}
	}
	if len(affectedSet) == 0 {
		return nil, nil
	}
	readLimit := memoryLimit - *memoryUsed
	data, err := readMaintenanceMetadataObject(ctx, c.ObjectReader, deletePath, readLimit)
	if err != nil {
		if isMaintenancePlanningLimit(err) {
			return nil, err
		}
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read position delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
	}
	// Keep the encoded input charged while building decoded maps. This is
	// intentionally conservative; a future spillable delete index can release
	// pages incrementally and lower this charge without weakening the bound.
	if err := reserveMaintenanceMemory(memoryUsed, int64(cap(data)), memoryLimit); err != nil {
		return nil, err
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor failed to open position delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
	}
	filePathCol, ok := rewriteDataFilesColumnIndexByName(file, "file_path", "file")
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete file is missing file_path column", map[string]string{
			"delete_file": api.RedactPath(deletePath),
		})
	}
	posCol, ok := rewriteDataFilesColumnIndexByName(file, "pos", "position", "row_position")
	if !ok {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete file is missing pos column", map[string]string{
			"delete_file": api.RedactPath(deletePath),
		})
	}
	out := make(map[string]map[int64]struct{})
	for _, rowGroup := range file.RowGroups() {
		rows := rowGroup.Rows()
		buffer := make([]parquet.Row, 128)
		for {
			n, readErr := rows.ReadRows(buffer)
			for idx := 0; idx < n; idx++ {
				dataFile, ok := rewriteDataFilesRowString(buffer[idx], filePathCol)
				if !ok || strings.TrimSpace(dataFile) == "" {
					_ = rows.Close()
					return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row is missing file_path", map[string]string{
						"delete_file": api.RedactPath(deletePath),
					})
				}
				if _, ok := affectedSet[dataFile]; !ok {
					continue
				}
				pos, ok := rewriteDataFilesRowInt64(buffer[idx], posCol)
				if !ok || pos < 0 {
					_ = rows.Close()
					return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row is missing pos", map[string]string{
						"delete_file": api.RedactPath(deletePath),
					})
				}
				positions := out[dataFile]
				if positions == nil {
					if err := reserveMaintenanceMemory(memoryUsed, int64(64+len(dataFile)), memoryLimit); err != nil {
						_ = rows.Close()
						return nil, err
					}
					positions = make(map[int64]struct{})
					out[dataFile] = positions
				}
				if _, exists := positions[pos]; !exists {
					if err := reserveMaintenanceMemory(memoryUsed, 32, memoryLimit); err != nil {
						_ = rows.Close()
						return nil, err
					}
				}
				positions[pos] = struct{}{}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = rows.Close()
				return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read position delete rows", map[string]string{
					"file": api.RedactPath(deletePath),
				}, readErr)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to close position delete rows", map[string]string{
				"file": api.RedactPath(deletePath),
			}, err)
		}
	}
	return out, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) readEqualityDeleteKeys(ctx context.Context, deleteFile api.DataFile, memoryUsed *int64, memoryLimit int64) (map[string]struct{}, error) {
	deletePath := strings.TrimSpace(deleteFile.FilePath)
	if deletePath == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete file path is required before rewrite-data-files delete apply", nil)
	}
	readLimit := memoryLimit - *memoryUsed
	data, err := readMaintenanceMetadataObject(ctx, c.ObjectReader, deletePath, readLimit)
	if err != nil {
		if isMaintenancePlanningLimit(err) {
			return nil, err
		}
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read equality delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
	}
	if err := reserveMaintenanceMemory(memoryUsed, int64(cap(data)), memoryLimit); err != nil {
		return nil, err
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor failed to open equality delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
	}
	columnByID, err := rewriteDataFilesColumnIndexByFieldID(file)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for _, rowGroup := range file.RowGroups() {
		rows := rowGroup.Rows()
		buffer := make([]parquet.Row, 128)
		for {
			n, readErr := rows.ReadRows(buffer)
			for idx := 0; idx < n; idx++ {
				key, err := rewriteDataFilesEqualityKey(buffer[idx], columnByID, deleteFile.EqualityIDs)
				if err != nil {
					_ = rows.Close()
					return nil, err
				}
				if _, exists := keys[key]; !exists {
					if err := reserveMaintenanceMemory(memoryUsed, int64(48+len(key)), memoryLimit); err != nil {
						_ = rows.Close()
						return nil, err
					}
					keys[key] = struct{}{}
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = rows.Close()
				return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read equality delete rows", map[string]string{
					"file": api.RedactPath(deletePath),
				}, readErr)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to close equality delete rows", map[string]string{
				"file": api.RedactPath(deletePath),
			}, err)
		}
	}
	return keys, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) copyCandidateRows(writer *parquet.Writer, conv parquet.Conversion, file *parquet.File, candidate RewriteDataFileCandidate, filter rewriteDataFilesFileDeleteFilter) (int64, error) {
	columnByID, err := rewriteDataFilesColumnIndexByFieldID(file)
	if err != nil && len(filter.Equality) > 0 {
		return 0, err
	}
	var totalRows int64
	var rowOrdinal int64
	for _, rowGroup := range file.RowGroups() {
		rows := rowGroup.Rows()
		buffer := make([]parquet.Row, 128)
		for {
			n, readErr := rows.ReadRows(buffer)
			if n > 0 {
				kept := buffer[:0]
				for idx := 0; idx < n; idx++ {
					deleted := false
					if _, ok := filter.Positions[rowOrdinal]; ok {
						deleted = true
					}
					if !deleted && len(filter.Equality) > 0 {
						match, err := rewriteDataFilesRowDeleted(buffer[idx], columnByID, filter.Equality)
						if err != nil {
							_ = rows.Close()
							return 0, err
						}
						deleted = match
					}
					if !deleted {
						kept = append(kept, buffer[idx])
					}
					rowOrdinal++
				}
				if len(kept) > 0 {
					if _, err := conv.Convert(kept); err != nil {
						_ = rows.Close()
						return 0, api.WrapError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor failed to convert rows to target schema", map[string]string{
							"file": api.RedactPath(candidate.File.FilePath),
						}, err)
					}
					written, err := writer.WriteRows(kept)
					if err != nil {
						_ = rows.Close()
						return 0, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to copy filtered rows", map[string]string{
							"file": api.RedactPath(candidate.File.FilePath),
						}, err)
					}
					totalRows += int64(written)
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = rows.Close()
				return 0, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to read row group", map[string]string{
					"file": api.RedactPath(candidate.File.FilePath),
				}, readErr)
			}
		}
		if err := rows.Close(); err != nil {
			return 0, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to close row reader", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
	}
	return totalRows, nil
}

func rewriteDataFilesColumnIndexByFieldID(file *parquet.File) (map[int]int, error) {
	if file == nil || file.Schema() == nil {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor requires a Parquet schema", nil)
	}
	out := make(map[int]int)
	for _, path := range file.Schema().Columns() {
		leaf, ok := file.Schema().Lookup(path...)
		if !ok || leaf.Node == nil {
			continue
		}
		fieldID := leaf.Node.ID()
		if fieldID == 0 {
			continue
		}
		if _, exists := out[fieldID]; exists {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files compactor found duplicate Parquet field id", map[string]string{
				"field_id": strconv.Itoa(fieldID),
			})
		}
		out[fieldID] = leaf.ColumnIndex
	}
	return out, nil
}

func rewriteDataFilesColumnIndexByName(file *parquet.File, names ...string) (int, bool) {
	if file == nil || file.Root() == nil {
		return 0, false
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	for _, col := range file.Root().Columns() {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(col.Name()))]; ok {
			return int(col.Index()), true
		}
	}
	return 0, false
}

func rewriteDataFilesRowDeleted(row parquet.Row, columnByID map[int]int, filters []rewriteDataFilesEqualityDeleteFilter) (bool, error) {
	for _, filter := range filters {
		key, err := rewriteDataFilesEqualityKey(row, columnByID, filter.EqualityIDs)
		if err != nil {
			return false, err
		}
		if _, ok := filter.Keys[key]; ok {
			return true, nil
		}
	}
	return false, nil
}

func rewriteDataFilesEqualityKey(row parquet.Row, columnByID map[int]int, equalityIDs []int) (string, error) {
	if len(equalityIDs) == 0 {
		return "", api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files equality delete requires equality field ids", nil)
	}
	var builder strings.Builder
	for idx, fieldID := range equalityIDs {
		columnIndex, ok := columnByID[fieldID]
		if !ok {
			return "", api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files equality field id is missing from Parquet schema", map[string]string{
				"field_id": strconv.Itoa(fieldID),
			})
		}
		value, ok := rewriteDataFilesRowValue(row, columnIndex)
		if !ok {
			return "", api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-data-files equality row is missing a required column", map[string]string{
				"field_id": strconv.Itoa(fieldID),
			})
		}
		if idx > 0 {
			builder.WriteByte('|')
		}
		builder.WriteString(strconv.Itoa(fieldID))
		builder.WriteByte('=')
		builder.WriteString(rewriteDataFilesParquetValueToken(value))
	}
	return builder.String(), nil
}

func rewriteDataFilesRowValue(row parquet.Row, columnIndex int) (parquet.Value, bool) {
	var out parquet.Value
	found := false
	row.Range(func(idx int, values []parquet.Value) bool {
		if idx != columnIndex {
			return true
		}
		if len(values) != 1 {
			return false
		}
		out = values[0]
		found = true
		return false
	})
	return out, found
}

func rewriteDataFilesRowString(row parquet.Row, columnIndex int) (string, bool) {
	value, ok := rewriteDataFilesRowValue(row, columnIndex)
	if !ok || value.IsNull() {
		return "", false
	}
	switch value.Kind() {
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(value.ByteArray()), true
	default:
		return "", false
	}
}

func rewriteDataFilesRowInt64(row parquet.Row, columnIndex int) (int64, bool) {
	value, ok := rewriteDataFilesRowValue(row, columnIndex)
	if !ok || value.IsNull() {
		return 0, false
	}
	switch value.Kind() {
	case parquet.Int64:
		return value.Int64(), true
	case parquet.Int32:
		return int64(value.Int32()), true
	default:
		return 0, false
	}
}

func rewriteDataFilesParquetValueToken(value parquet.Value) string {
	return strconv.Itoa(int(value.Kind())) +
		":" + strconv.Itoa(value.DefinitionLevel()) +
		":" + hex.EncodeToString(value.Bytes())
}
