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
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const (
	positionDeleteFilePathFieldID = 2147483546
	positionDeletePosFieldID      = 2147483545
	deleteWriteBatchRows          = 1024
	deleteMaxRowsPerRowGroup      = 8192
	maxDeleteWriterBoundBytes     = 4 << 10
)

// WriteMemoryBudget lets the SQL runtime and the format encoder charge the
// same hard boundary without introducing an Iceberg -> SQL dependency. The
// runtime always supplies it; direct library callers may leave it nil.
type WriteMemoryBudget interface {
	Reserve(ctx context.Context, bytes int64) error
	Release(bytes int64)
}

type PositionDeleteRow struct {
	FilePath string
	Pos      int64
}

type PositionDeleteWriteRequest struct {
	FilePath           string
	Rows               []PositionDeleteRow
	ReferencedDataFile string
	Partition          map[string]any
	SpecID             int
	DeleteSchemaID     int
	MemoryBudget       WriteMemoryBudget
}

type EqualityDeleteRow struct {
	Values map[int]any
}

type EqualityDeleteWriteRequest struct {
	FilePath       string
	Schema         api.Schema
	EqualityIDs    []int
	Rows           []EqualityDeleteRow
	Partition      map[string]any
	SpecID         int
	DeleteSchemaID int
	MemoryBudget   WriteMemoryBudget
}

type DeleteObjectWriter interface {
	WriteObject(ctx context.Context, location string, payload []byte) error
}

func reserveDeleteWriterMemory(ctx context.Context, budget WriteMemoryBudget, bytes int64) error {
	if budget == nil || bytes <= 0 {
		return nil
	}
	return budget.Reserve(ctx, bytes)
}

func releaseDeleteWriterMemory(budget WriteMemoryBudget, bytes int64) {
	if budget != nil && bytes > 0 {
		budget.Release(bytes)
	}
}

func deleteParquetScratchBytes(fields int) int64 {
	// parquet-go owns page/column buffers which are not visible through the
	// destination writer. Bounded row groups cap their lifetime; reserve a
	// conservative per-column allowance for that opaque state. A future writer
	// exposing allocator hooks could replace this estimate with exact charging.
	return saturatingDeleteAdd(2<<20, saturatingDeleteMul(int64(fields), 1<<20))
}

func retainedDeleteFileMetadataBytes(fields int) int64 {
	// Bounds are capped at 4 KiB and DataFile metadata is defensively cloned by
	// planning/commit handoffs. Keep the reservation until the coordinator ends.
	return saturatingDeleteAdd(32<<10, saturatingDeleteMul(int64(fields), 16<<10))
}

func WritePositionDeleteFile(ctx context.Context, dst io.Writer, req PositionDeleteWriteRequest) (api.DataFile, error) {
	if dst == nil {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg position delete writer requires output writer", nil)
	}
	if strings.TrimSpace(req.FilePath) == "" {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg position delete writer requires file path", nil)
	}
	rowCopyBytes := saturatingDeleteMul(int64(len(req.Rows)), 64)
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, rowCopyBytes); err != nil {
		return api.DataFile{}, err
	}
	defer releaseDeleteWriterMemory(req.MemoryBudget, rowCopyBytes)
	rows, referenced, err := normalizePositionDeleteRows(req)
	if err != nil {
		return api.DataFile{}, err
	}
	cw := &deleteCountingWriter{writer: dst}
	schema := parquet.NewSchema("file_position_delete", parquet.Group{
		"file_path": parquet.FieldID(parquet.Required(deleteStringNode()), positionDeleteFilePathFieldID),
		"pos":       parquet.FieldID(parquet.Required(parquet.Leaf(parquet.Int64Type)), positionDeletePosFieldID),
	})
	scratchBytes := deleteParquetScratchBytes(2)
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, scratchBytes); err != nil {
		return api.DataFile{}, err
	}
	defer releaseDeleteWriterMemory(req.MemoryBudget, scratchBytes)
	writer := parquet.NewGenericWriter[any](cw, schema, parquet.MaxRowsPerRowGroup(deleteMaxRowsPerRowGroup))
	writerFinished := false
	defer func() {
		if !writerFinished {
			// Reset drops parquet-go's page/row-group state without attempting to
			// publish a footer after an earlier validation or budget error.
			writer.Reset(io.Discard)
		}
	}()
	metrics := newDeleteMetrics([]api.SchemaField{
		{ID: positionDeleteFilePathFieldID, Name: "file_path", Required: true, Type: api.IcebergType{Kind: api.TypeString, Raw: string(api.TypeString)}},
		{ID: positionDeletePosFieldID, Name: "pos", Required: true, Type: api.IcebergType{Kind: api.TypeLong, Raw: string(api.TypeLong)}},
	})
	for start := 0; start < len(rows); start += deleteWriteBatchRows {
		if err := ctx.Err(); err != nil {
			return api.DataFile{}, err
		}
		end := min(start+deleteWriteBatchRows, len(rows))
		workingBytes := saturatingDeleteMul(int64(end-start), 256)
		if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, workingBytes); err != nil {
			return api.DataFile{}, err
		}
		parquetRows := make([]any, end-start)
		for idx := start; idx < end; idx++ {
			row := rows[idx]
			parquetRows[idx-start] = map[string]any{"file_path": row.FilePath, "pos": row.Pos}
			metrics.observe(ctx, positionDeleteFilePathFieldID, row.FilePath)
			metrics.observe(ctx, positionDeletePosFieldID, row.Pos)
		}
		_, writeErr := writer.Write(parquetRows)
		releaseDeleteWriterMemory(req.MemoryBudget, workingBytes)
		if writeErr != nil {
			return api.DataFile{}, api.WrapError(api.ErrObjectIO, "Iceberg position delete writer failed to encode rows", nil, writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		return api.DataFile{}, api.WrapError(api.ErrObjectIO, "Iceberg position delete writer failed to close", nil, err)
	}
	writerFinished = true
	file := deleteDataFile(req.FilePath, api.DataFileContentPositionDelete, int64(len(rows)), cw.n, req.Partition, req.SpecID, req.DeleteSchemaID, metrics)
	file.ReferencedDataFile = referenced
	return file, nil
}

func WritePositionDeleteObject(ctx context.Context, writer DeleteObjectWriter, req PositionDeleteWriteRequest) (api.DataFile, error) {
	if writer == nil {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg position delete object writer requires object writer", nil)
	}
	buf := deleteObjectBuffer{ctx: ctx, budget: req.MemoryBudget}
	defer buf.Release()
	file, err := WritePositionDeleteFile(ctx, &buf, req)
	if err != nil {
		return api.DataFile{}, err
	}
	copyBytes := int64(len(buf.Bytes()))
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, copyBytes); err != nil {
		return api.DataFile{}, err
	}
	err = writer.WriteObject(ctx, req.FilePath, buf.Bytes())
	releaseDeleteWriterMemory(req.MemoryBudget, copyBytes)
	if err != nil {
		return api.DataFile{}, err
	}
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, retainedDeleteFileMetadataBytes(2)); err != nil {
		return api.DataFile{}, err
	}
	return file, nil
}

func WriteEqualityDeleteFile(ctx context.Context, dst io.Writer, req EqualityDeleteWriteRequest) (api.DataFile, error) {
	if dst == nil {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg equality delete writer requires output writer", nil)
	}
	if strings.TrimSpace(req.FilePath) == "" {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg equality delete writer requires file path", nil)
	}
	fields, equalityIDs, err := equalityDeleteFields(req)
	if err != nil {
		return api.DataFile{}, err
	}
	if len(req.Rows) == 0 {
		return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete writer requires at least one row", map[string]string{"path": api.RedactPath(req.FilePath)})
	}
	group := make(parquet.Group, len(fields))
	for _, field := range fields {
		node, err := deleteParquetNodeForField(field)
		if err != nil {
			return api.DataFile{}, err
		}
		group[field.Name] = parquet.FieldID(node, field.ID)
	}
	cw := &deleteCountingWriter{writer: dst}
	scratchBytes := deleteParquetScratchBytes(len(fields))
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, scratchBytes); err != nil {
		return api.DataFile{}, err
	}
	defer releaseDeleteWriterMemory(req.MemoryBudget, scratchBytes)
	writer := parquet.NewGenericWriter[any](cw, parquet.NewSchema("equality_delete", group), parquet.MaxRowsPerRowGroup(deleteMaxRowsPerRowGroup))
	writerFinished := false
	defer func() {
		if !writerFinished {
			writer.Reset(io.Discard)
		}
	}()
	metrics := newDeleteMetrics(fields)
	for start := 0; start < len(req.Rows); start += deleteWriteBatchRows {
		if err := ctx.Err(); err != nil {
			return api.DataFile{}, err
		}
		end := min(start+deleteWriteBatchRows, len(req.Rows))
		perRowBytes := saturatingDeleteAdd(128, saturatingDeleteMul(int64(len(fields)), 96))
		workingBytes := saturatingDeleteMul(int64(end-start), perRowBytes)
		if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, workingBytes); err != nil {
			return api.DataFile{}, err
		}
		parquetRows := make([]any, end-start)
		for rowIdx := start; rowIdx < end; rowIdx++ {
			row := req.Rows[rowIdx]
			out := make(map[string]any, len(fields))
			for _, field := range fields {
				value, ok := row.Values[field.ID]
				if !ok || value == nil {
					if field.Required {
						releaseDeleteWriterMemory(req.MemoryBudget, workingBytes)
						return api.DataFile{}, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete row is missing required field", map[string]string{"field_id": strconv.Itoa(field.ID), "field": field.Name})
					}
					out[field.Name] = nil
					metrics.observeNull(field.ID)
					continue
				}
				canonical, err := canonicalDeleteValue(ctx, field.Type, value)
				if err != nil {
					releaseDeleteWriterMemory(req.MemoryBudget, workingBytes)
					return api.DataFile{}, err
				}
				out[field.Name] = canonical
				metrics.observe(ctx, field.ID, canonical)
			}
			parquetRows[rowIdx-start] = out
		}
		_, writeErr := writer.Write(parquetRows)
		releaseDeleteWriterMemory(req.MemoryBudget, workingBytes)
		if writeErr != nil {
			return api.DataFile{}, api.WrapError(api.ErrObjectIO, "Iceberg equality delete writer failed to encode rows", nil, writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		return api.DataFile{}, api.WrapError(api.ErrObjectIO, "Iceberg equality delete writer failed to close", nil, err)
	}
	writerFinished = true
	file := deleteDataFile(req.FilePath, api.DataFileContentEqualityDelete, int64(len(req.Rows)), cw.n, req.Partition, req.SpecID, req.DeleteSchemaID, metrics)
	file.EqualityIDs = equalityIDs
	return file, nil
}

func WriteEqualityDeleteObject(ctx context.Context, writer DeleteObjectWriter, req EqualityDeleteWriteRequest) (api.DataFile, error) {
	if writer == nil {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg equality delete object writer requires object writer", nil)
	}
	buf := deleteObjectBuffer{ctx: ctx, budget: req.MemoryBudget}
	defer buf.Release()
	file, err := WriteEqualityDeleteFile(ctx, &buf, req)
	if err != nil {
		return api.DataFile{}, err
	}
	copyBytes := int64(len(buf.Bytes()))
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, copyBytes); err != nil {
		return api.DataFile{}, err
	}
	err = writer.WriteObject(ctx, req.FilePath, buf.Bytes())
	releaseDeleteWriterMemory(req.MemoryBudget, copyBytes)
	if err != nil {
		return api.DataFile{}, err
	}
	if err := reserveDeleteWriterMemory(ctx, req.MemoryBudget, retainedDeleteFileMetadataBytes(len(req.EqualityIDs))); err != nil {
		return api.DataFile{}, err
	}
	return file, nil
}

func normalizePositionDeleteRows(req PositionDeleteWriteRequest) ([]PositionDeleteRow, string, error) {
	if len(req.Rows) == 0 {
		return nil, "", api.NewError(api.ErrMetadataInvalid, "Iceberg position delete writer requires at least one row", map[string]string{"path": api.RedactPath(req.FilePath)})
	}
	rows := append([]PositionDeleteRow(nil), req.Rows...)
	referenced := strings.TrimSpace(req.ReferencedDataFile)
	seen := make(map[string]bool)
	for idx := range rows {
		rows[idx].FilePath = strings.TrimSpace(rows[idx].FilePath)
		if rows[idx].FilePath == "" {
			return nil, "", api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row requires data file path", map[string]string{"path": api.RedactPath(req.FilePath)})
		}
		if rows[idx].Pos < 0 {
			return nil, "", api.NewError(api.ErrMetadataInvalid, "Iceberg position delete row position must be non-negative", map[string]string{"path": api.RedactPath(req.FilePath)})
		}
		seen[rows[idx].FilePath] = true
	}
	if referenced == "" && len(seen) == 1 {
		for path := range seen {
			referenced = path
		}
	}
	if referenced == "" || len(seen) != 1 || !seen[referenced] {
		return nil, "", api.NewError(api.ErrUnsupportedFeature, "Iceberg position delete writer currently supports one referenced data file per delete file", map[string]string{"path": api.RedactPath(req.FilePath)})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].FilePath != rows[j].FilePath {
			return rows[i].FilePath < rows[j].FilePath
		}
		return rows[i].Pos < rows[j].Pos
	})
	return rows, referenced, nil
}

func equalityDeleteFields(req EqualityDeleteWriteRequest) ([]api.SchemaField, []int, error) {
	if len(req.EqualityIDs) == 0 {
		return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete writer requires equality ids", map[string]string{"path": api.RedactPath(req.FilePath)})
	}
	byID := make(map[int]api.SchemaField, len(req.Schema.Fields))
	for _, field := range req.Schema.Fields {
		byID[field.ID] = field
	}
	seen := make(map[int]bool, len(req.EqualityIDs))
	fields := make([]api.SchemaField, 0, len(req.EqualityIDs))
	ids := make([]int, 0, len(req.EqualityIDs))
	for _, id := range req.EqualityIDs {
		if id <= 0 {
			return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete id must be positive", map[string]string{"field_id": strconv.Itoa(id)})
		}
		if seen[id] {
			continue
		}
		field, ok := byID[id]
		if !ok {
			return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete id is not present in table schema", map[string]string{"field_id": strconv.Itoa(id)})
		}
		if strings.TrimSpace(field.Name) == "" {
			return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg equality delete field name is empty", map[string]string{"field_id": strconv.Itoa(id)})
		}
		seen[id] = true
		fields = append(fields, field)
		ids = append(ids, id)
	}
	return fields, ids, nil
}

func deleteParquetNodeForField(field api.SchemaField) (parquet.Node, error) {
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
		node = deleteStringNode()
	case api.TypeDate:
		node = parquet.Date()
	case api.TypeTimestamp:
		node = parquet.TimestampAdjusted(parquet.Microsecond, false)
	case api.TypeTimestampTZ:
		node = parquet.Timestamp(parquet.Microsecond)
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg delete writer type is unsupported", map[string]string{"field": field.Name, "type": field.Type.String()})
	}
	if field.Required {
		return parquet.Required(node), nil
	}
	return parquet.Optional(node), nil
}

func deleteStringNode() parquet.Node {
	return parquet.Encoded(parquet.String(), &parquet.Plain)
}

func canonicalDeleteValue(ctx context.Context, typ api.IcebergType, value any) (any, error) {
	switch typ.Kind {
	case api.TypeBoolean:
		v, ok := value.(bool)
		if !ok {
			return nil, deleteTypeMismatch(typ, value)
		}
		return v, nil
	case api.TypeInt, api.TypeDate:
		switch v := value.(type) {
		case int8:
			return int32(v), nil
		case int16:
			return int32(v), nil
		case int32:
			return v, nil
		case int:
			if v < math.MinInt32 || v > math.MaxInt32 {
				return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg delete writer int value is out of range", map[string]string{"type": typ.String()})
			}
			return int32(v), nil
		default:
			return nil, deleteTypeMismatch(typ, value)
		}
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		switch v := value.(type) {
		case int8:
			return int64(v), nil
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case time.Time:
			if typ.Kind == api.TypeTimestamp || typ.Kind == api.TypeTimestampTZ {
				return v.UTC().UnixMicro(), nil
			}
			return nil, deleteTypeMismatch(typ, value)
		default:
			return nil, deleteTypeMismatch(typ, value)
		}
	case api.TypeFloat:
		v, ok := value.(float32)
		if !ok {
			return nil, deleteTypeMismatch(typ, value)
		}
		return v, nil
	case api.TypeDouble:
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		default:
			return nil, deleteTypeMismatch(typ, value)
		}
	case api.TypeString:
		v, ok := value.(string)
		if !ok {
			return nil, deleteTypeMismatch(typ, value)
		}
		return v, nil
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg delete writer type is unsupported", map[string]string{"type": typ.String()})
	}
}

func deleteTypeMismatch(typ api.IcebergType, value any) error {
	valueType := "<nil>"
	if value != nil {
		valueType = reflect.TypeOf(value).String()
	}
	return api.NewError(api.ErrMetadataInvalid, "Iceberg delete writer value type does not match field type", map[string]string{
		"type":       typ.String(),
		"value_type": valueType,
	})
}

func deleteDataFile(path string, content api.DataFileContent, rows, size int64, partition map[string]any, specID, schemaID int, metrics *deleteMetrics) api.DataFile {
	file := api.DataFile{
		Content:          content,
		FilePath:         path,
		FileFormat:       "parquet",
		Partition:        cloneAnyMap(partition),
		RecordCount:      rows,
		FileSizeInBytes:  size,
		SpecID:           specID,
		DeleteSchemaID:   schemaID,
		FilePathRedacted: api.RedactPath(path),
		FilePathHash:     api.PathHash(path),
	}
	if metrics != nil {
		file.ValueCounts = metrics.valueCounts(rows)
		file.NullValueCounts = cloneInt64Map(metrics.nullCounts)
		file.NaNValueCounts = cloneInt64Map(metrics.nanCounts)
		file.LowerBounds = cloneBytesMap(metrics.lowerBounds)
		file.UpperBounds = cloneBytesMap(metrics.upperBounds)
	}
	return file
}

type deleteMetrics struct {
	fields      map[int]api.SchemaField
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
	compare     map[int]any
	boundsOff   map[int]bool
}

func newDeleteMetrics(fields []api.SchemaField) *deleteMetrics {
	out := &deleteMetrics{
		fields:      make(map[int]api.SchemaField, len(fields)),
		nullCounts:  make(map[int]int64),
		nanCounts:   make(map[int]int64),
		lowerBounds: make(map[int][]byte),
		upperBounds: make(map[int][]byte),
		compare:     make(map[int]any),
		boundsOff:   make(map[int]bool),
	}
	for _, field := range fields {
		out.fields[field.ID] = field
	}
	return out
}

func (m *deleteMetrics) observeNull(fieldID int) {
	m.nullCounts[fieldID] = saturatingDeleteAdd(m.nullCounts[fieldID], 1)
}

func (m *deleteMetrics) observe(ctx context.Context, fieldID int, value any) {
	field, ok := m.fields[fieldID]
	if !ok || isDeleteNaN(value) {
		if isDeleteNaN(value) {
			m.nanCounts[fieldID] = saturatingDeleteAdd(m.nanCounts[fieldID], 1)
		}
		return
	}
	if m.boundsOff[fieldID] {
		return
	}
	if text, ok := value.(string); ok && len(text) > maxDeleteWriterBoundBytes {
		// Prefix truncation needs Iceberg's type-specific upper-bound rules. Until
		// those are implemented, omitting both bounds is the only pruning-safe
		// behavior and also keeps retained delete metrics bounded.
		m.boundsOff[fieldID] = true
		delete(m.compare, fieldID)
		delete(m.lowerBounds, fieldID)
		delete(m.upperBounds, fieldID)
		return
	}
	encoded, cmp, err := encodeDeleteBound(ctx, field.Type, value)
	if err != nil {
		return
	}
	current, hasCurrent := m.compare[fieldID]
	if !hasCurrent || compareDeleteMetricValue(cmp, current) < 0 {
		m.compare[fieldID] = cmp
		m.lowerBounds[fieldID] = encoded
	}
	upper := m.upperBounds[fieldID]
	if len(upper) == 0 || compareDeleteMetricValue(cmp, decodeDeleteMetricValue(field.Type, upper)) > 0 {
		m.upperBounds[fieldID] = append([]byte(nil), encoded...)
	}
}

func (m *deleteMetrics) valueCounts(rows int64) map[int]int64 {
	if len(m.fields) == 0 {
		return nil
	}
	out := make(map[int]int64, len(m.fields))
	for id := range m.fields {
		out[id] = rows
	}
	return out
}

func encodeDeleteBound(ctx context.Context, typ api.IcebergType, value any) ([]byte, any, error) {
	switch typ.Kind {
	case api.TypeBoolean:
		v := value.(bool)
		if v {
			return []byte{1}, v, nil
		}
		return []byte{0}, v, nil
	case api.TypeInt, api.TypeDate:
		v := value.(int32)
		out := make([]byte, 4)
		binary.LittleEndian.PutUint32(out, uint32(v))
		return out, int64(v), nil
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		v := value.(int64)
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, uint64(v))
		return out, v, nil
	case api.TypeFloat:
		v := value.(float32)
		out := make([]byte, 4)
		binary.LittleEndian.PutUint32(out, math.Float32bits(v))
		return out, float64(v), nil
	case api.TypeDouble:
		v := value.(float64)
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, math.Float64bits(v))
		return out, v, nil
	case api.TypeString:
		v := value.(string)
		return []byte(v), v, nil
	default:
		return nil, nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg delete writer bound type is unsupported", map[string]string{"type": typ.String()})
	}
}

func decodeDeleteMetricValue(typ api.IcebergType, data []byte) any {
	switch typ.Kind {
	case api.TypeBoolean:
		return len(data) > 0 && data[0] != 0
	case api.TypeInt, api.TypeDate:
		return int64(int32(binary.LittleEndian.Uint32(data)))
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		return int64(binary.LittleEndian.Uint64(data))
	case api.TypeFloat:
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))
	case api.TypeDouble:
		return math.Float64frombits(binary.LittleEndian.Uint64(data))
	case api.TypeString:
		return string(data)
	default:
		return nil
	}
}

func compareDeleteMetricValue(left, right any) int {
	switch l := left.(type) {
	case bool:
		r := right.(bool)
		if l == r {
			return 0
		}
		if !l {
			return -1
		}
		return 1
	case int64:
		r := right.(int64)
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
		return 0
	case float64:
		r := right.(float64)
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
		return 0
	case string:
		return strings.Compare(l, right.(string))
	default:
		return 0
	}
}

func isDeleteNaN(value any) bool {
	switch v := value.(type) {
	case float32:
		return math.IsNaN(float64(v))
	case float64:
		return math.IsNaN(v)
	default:
		return false
	}
}

type deleteCountingWriter struct {
	writer io.Writer
	n      int64
}

func (w *deleteCountingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.n = saturatingDeleteAdd(w.n, int64(n))
	return n, err
}

type deleteObjectBuffer struct {
	ctx      context.Context
	budget   WriteMemoryBudget
	data     []byte
	reserved int64
}

func (b *deleteObjectBuffer) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	if len(b.data) > math.MaxInt-len(payload) {
		return 0, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg delete-file output exceeds the addressable memory limit", nil)
	}
	required := len(b.data) + len(payload)
	if required > cap(b.data) {
		oldCapacity := cap(b.data)
		nextCapacity := required
		if oldCapacity <= math.MaxInt/2 && oldCapacity*2 > nextCapacity {
			nextCapacity = oldCapacity * 2
		}
		if nextCapacity < 64<<10 {
			nextCapacity = 64 << 10
		}
		// The old and new arrays coexist during copy, so charge the entire new
		// allocation before releasing the old reservation.
		if err := reserveDeleteWriterMemory(b.ctx, b.budget, int64(nextCapacity)); err != nil {
			return 0, err
		}
		next := make([]byte, len(b.data), nextCapacity)
		copy(next, b.data)
		b.data = next
		releaseDeleteWriterMemory(b.budget, int64(oldCapacity))
		b.reserved = int64(nextCapacity)
	}
	start := len(b.data)
	b.data = b.data[:required]
	copy(b.data[start:], payload)
	return len(payload), nil
}

func (b *deleteObjectBuffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

func (b *deleteObjectBuffer) Release() {
	if b == nil {
		return
	}
	b.data = nil
	releaseDeleteWriterMemory(b.budget, b.reserved)
	b.reserved = 0
}

func saturatingDeleteAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func saturatingDeleteMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneInt64Map(in map[int]int64) map[int]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneBytesMap(in map[int][]byte) map[int][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int][]byte, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}
