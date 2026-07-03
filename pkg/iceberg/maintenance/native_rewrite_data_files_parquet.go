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
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type ParquetConcatRewriteDataFilesCompactor struct {
	ObjectReader api.ObjectReader
	Metadata     api.MetadataFacade
	OutputPrefix string
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
	deleteFilters, err := c.buildDeleteFilters(ctx, req)
	if err != nil {
		return nil, err
	}
	out := &RewriteDataFilesCompactResult{
		Rewrites: make([]RewriteDataFileRewrite, 0, len(req.Selection.Groups)),
		Objects:  make([]ObjectWrite, 0, len(req.Selection.Groups)),
	}
	for idx, group := range req.Selection.Groups {
		rewrite, object, err := c.compactGroup(ctx, req, group, idx, deleteFilters)
		if err != nil {
			return nil, err
		}
		out.Rewrites = append(out.Rewrites, rewrite)
		out.Objects = append(out.Objects, object)
		out.OrphanPaths = append(out.OrphanPaths, object.Location)
	}
	return out, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) buildDeleteFilters(ctx context.Context, req RewriteDataFilesCompactRequest) (rewriteDataFilesDeleteFilters, error) {
	deleteManifests, err := c.rewriteDataFilesDeleteManifests(ctx, req)
	if err != nil || len(deleteManifests) == 0 {
		return nil, err
	}
	candidates := rewriteDataFilesCompactionCandidates(req.Selection.Groups)
	if len(candidates) == 0 {
		return nil, nil
	}
	facade := c.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	filters := make(rewriteDataFilesDeleteFilters)
	for _, manifest := range deleteManifests {
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor found delete manifest without path", map[string]string{
				"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
			})
		}
		manifestData, err := c.ObjectReader.Read(ctx, manifestPath, 0, -1)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg parquet rewrite-data-files compactor failed to read delete manifest", map[string]string{
				"manifest": api.RedactPath(manifestPath),
			}, err)
		}
		entries, err := facade.ReadManifest(ctx, manifestData)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Status == api.ManifestEntryDeleted {
				continue
			}
			file := entry.DataFile
			if err := metadata.ValidateP1DeleteFile(file); err != nil {
				return nil, err
			}
			affected := rewriteDataFilesDeleteAffectedFiles(file, candidates)
			if len(affected) == 0 {
				continue
			}
			switch file.Content {
			case api.DataFileContentPositionDelete:
				positions, err := c.readPositionDeletePositions(ctx, file, affected)
				if err != nil {
					return nil, err
				}
				for path, filePositions := range positions {
					filter := filters[path]
					if filter.Positions == nil {
						filter.Positions = make(map[int64]struct{}, len(filePositions))
					}
					for pos := range filePositions {
						filter.Positions[pos] = struct{}{}
					}
					filters[path] = filter
				}
			case api.DataFileContentEqualityDelete:
				keys, err := c.readEqualityDeleteKeys(ctx, file)
				if err != nil {
					return nil, err
				}
				equality := rewriteDataFilesEqualityDeleteFilter{
					EqualityIDs: append([]int(nil), file.EqualityIDs...),
					Keys:        keys,
				}
				for _, path := range affected {
					filter := filters[path]
					filter.Equality = append(filter.Equality, equality)
					filters[path] = filter
				}
			}
		}
	}
	if len(filters) == 0 {
		return nil, nil
	}
	return filters, nil
}

func (c ParquetConcatRewriteDataFilesCompactor) rewriteDataFilesDeleteManifests(ctx context.Context, req RewriteDataFilesCompactRequest) ([]api.ManifestFile, error) {
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
	manifestListData, err := c.ObjectReader.Read(ctx, snapshot.ManifestList, 0, -1)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg parquet rewrite-data-files compactor failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(snapshot.ManifestList),
		}, err)
	}
	manifests, err := facade.ReadManifestList(ctx, manifestListData)
	if err != nil {
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

func rewriteDataFilesDeleteAffectedFiles(deleteFile api.DataFile, dataByPath map[string]api.DataFile) []string {
	affected := make([]string, 0)
	switch deleteFile.Content {
	case api.DataFileContentPositionDelete:
		dataFile, ok := dataByPath[strings.TrimSpace(deleteFile.ReferencedDataFile)]
		if ok && rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile) {
			affected = append(affected, dataFile.FilePath)
		}
	case api.DataFileContentEqualityDelete:
		for path, dataFile := range dataByPath {
			if !rewriteDataFilesDeleteSequenceApplies(dataFile, deleteFile) {
				continue
			}
			if deleteFile.SpecID != dataFile.SpecID {
				continue
			}
			if !rewriteDataFilesSamePartitionScope(deleteFile.Partition, dataFile.Partition) {
				continue
			}
			affected = append(affected, path)
		}
	}
	return affected
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

func (c ParquetConcatRewriteDataFilesCompactor) compactGroup(ctx context.Context, req RewriteDataFilesCompactRequest, group RewriteDataFileGroup, groupIndex int, deleteFilters rewriteDataFilesDeleteFilters) (RewriteDataFileRewrite, ObjectWrite, error) {
	if len(group.Candidates) == 0 {
		return RewriteDataFileRewrite{}, ObjectWrite{}, api.NewError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor received an empty group", nil)
	}
	var output bytes.Buffer
	var writer *parquet.Writer
	var writerSchema *parquet.Schema
	var totalRows int64
	for _, candidate := range group.Candidates {
		data, err := c.ObjectReader.Read(ctx, candidate.File.FilePath, 0, -1)
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrObjectIO, "Iceberg parquet rewrite-data-files compactor failed to read data file", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return RewriteDataFileRewrite{}, ObjectWrite{}, api.WrapError(api.ErrMetadataInvalid, "Iceberg parquet rewrite-data-files compactor failed to open data file", map[string]string{
				"file": api.RedactPath(candidate.File.FilePath),
			}, err)
		}
		if writer == nil {
			schema, err := rewriteDataFilesWriterSchema(req.Metadata, file.Schema())
			if err != nil {
				return RewriteDataFileRewrite{}, ObjectWrite{}, err
			}
			writerSchema = schema
			writer = parquet.NewWriter(&output, schema)
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
			Payload:  append([]byte(nil), output.Bytes()...),
		}, nil
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

func (c ParquetConcatRewriteDataFilesCompactor) readPositionDeletePositions(ctx context.Context, deleteFile api.DataFile, affected []string) (map[string]map[int64]struct{}, error) {
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
	data, err := c.ObjectReader.Read(ctx, deletePath, 0, -1)
	if err != nil {
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read position delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
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
				if !ok {
					_ = rows.Close()
					return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row is missing file_path", map[string]string{
						"delete_file": api.RedactPath(deletePath),
					})
				}
				if _, ok := affectedSet[dataFile]; !ok {
					continue
				}
				pos, ok := rewriteDataFilesRowInt64(buffer[idx], posCol)
				if !ok {
					_ = rows.Close()
					return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row is missing pos", map[string]string{
						"delete_file": api.RedactPath(deletePath),
					})
				}
				positions := out[dataFile]
				if positions == nil {
					positions = make(map[int64]struct{})
					out[dataFile] = positions
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

func (c ParquetConcatRewriteDataFilesCompactor) readEqualityDeleteKeys(ctx context.Context, deleteFile api.DataFile) (map[string]struct{}, error) {
	deletePath := strings.TrimSpace(deleteFile.FilePath)
	if deletePath == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete file path is required before rewrite-data-files delete apply", nil)
	}
	data, err := c.ObjectReader.Read(ctx, deletePath, 0, -1)
	if err != nil {
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg rewrite-data-files compactor failed to read equality delete file", map[string]string{
			"file": api.RedactPath(deletePath),
		}, err)
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
				keys[key] = struct{}{}
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
		return value.String(), true
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
