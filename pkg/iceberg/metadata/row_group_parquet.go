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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func (p LocalScanPlanner) applyRowGroupPlanning(
	ctx context.Context,
	meta *api.TableMetadata,
	schema api.Schema,
	req api.ScanPlanRequest,
	tasks []api.DataFileTask,
	profile *api.PlanningProfile,
) ([]api.DataFileTask, error) {
	if !req.EnableRowGroupPlanning || len(tasks) == 0 {
		return tasks, nil
	}
	out := make([]api.DataFileTask, 0, len(tasks))
	for _, task := range tasks {
		if err := checkPlanningContext(ctx); err != nil {
			return nil, err
		}
		footers, err := p.readParquetRowGroupFooters(ctx, schema, task.DataFile)
		if err != nil {
			return nil, err
		}
		splits := BuildRowGroupSplits(footers)
		if len(splits) == 0 {
			out = append(out, task)
			continue
		}
		selected, pruned := PruneRowGroupSplits(meta, schema, task.DataFile.SpecID, splits, req.PrunePredicates)
		if profile != nil {
			profile.RowGroupsSelected += len(selected)
			profile.RowGroupsPruned += pruned
		}
		if pruned == 0 {
			out = append(out, task)
			continue
		}
		for _, split := range selected {
			rowGroupTask := task
			rowGroupTask.RowGroups = []api.RowGroupSplit{split}
			out = append(out, rowGroupTask)
		}
	}
	return out, nil
}

func (p LocalScanPlanner) readParquetRowGroupFooters(ctx context.Context, schema api.Schema, file api.DataFile) ([]RowGroupFooter, error) {
	var reader io.ReaderAt
	size := file.FileSizeInBytes
	if size > 0 {
		reader = icebergObjectReaderAt{ctx: ctx, reader: p.ObjectReader, location: file.FilePath}
	} else {
		data, err := p.ObjectReader.Read(ctx, file.FilePath, 0, -1)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
		size = int64(len(data))
	}
	pf, err := parquet.OpenFile(reader, size)
	if err != nil {
		return nil, api.WrapError(api.ErrObjectIO, "Iceberg row-group footer read failed", map[string]string{
			"file": api.RedactPath(file.FilePath),
		}, err)
	}
	return parquetRowGroupFooters(pf, schema, file.FileSizeInBytes), nil
}

type icebergObjectReaderAt struct {
	ctx      context.Context
	reader   api.ObjectReader
	location string
}

func (r icebergObjectReaderAt) ReadAt(p []byte, off int64) (int, error) {
	data, err := r.reader.Read(r.ctx, r.location, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func parquetRowGroupFooters(file *parquet.File, schema api.Schema, fileSize int64) []RowGroupFooter {
	if file == nil {
		return nil
	}
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		return nil
	}
	fields := schemaFieldsByID(schema)
	columnFieldIDs := parquetLeafColumnFieldIDs(file.Root(), fields)
	out := make([]RowGroupFooter, 0, len(rowGroups))
	totalRows := file.NumRows()
	for idx, rowGroup := range rowGroups {
		footer := RowGroupFooter{
			Ordinal:         int32(idx),
			RowCount:        rowGroup.NumRows(),
			Bytes:           estimateIcebergParquetRowGroupBytes(fileSize, totalRows, rowGroup.NumRows(), len(rowGroups)),
			LowerBounds:     make(map[int][]byte),
			UpperBounds:     make(map[int][]byte),
			NullValueCounts: make(map[int]int64),
			ValueCounts:     make(map[int]int64),
		}
		for colIdx, chunk := range rowGroup.ColumnChunks() {
			if colIdx < 0 || colIdx >= len(columnFieldIDs) {
				continue
			}
			fieldID := columnFieldIDs[colIdx]
			if fieldID <= 0 {
				continue
			}
			field, ok := fields[fieldID]
			if !ok {
				continue
			}
			footer.ValueCounts[fieldID] = chunk.NumValues()
			lower, upper, nulls, ok := parquetChunkBounds(field.Type, chunk)
			if !ok {
				if nulls > 0 {
					footer.NullValueCounts[fieldID] = nulls
				}
				continue
			}
			footer.LowerBounds[fieldID] = lower
			footer.UpperBounds[fieldID] = upper
			footer.NullValueCounts[fieldID] = nulls
		}
		if len(footer.LowerBounds) == 0 {
			footer.LowerBounds = nil
		}
		if len(footer.UpperBounds) == 0 {
			footer.UpperBounds = nil
		}
		if len(footer.NullValueCounts) == 0 {
			footer.NullValueCounts = nil
		}
		if len(footer.ValueCounts) == 0 {
			footer.ValueCounts = nil
		}
		out = append(out, footer)
	}
	return out
}

func schemaFieldsByID(schema api.Schema) map[int]api.SchemaField {
	fields := make(map[int]api.SchemaField, len(schema.Fields))
	for _, field := range schema.Fields {
		fields[field.ID] = field
	}
	return fields
}

func parquetLeafColumnFieldIDs(root *parquet.Column, fields map[int]api.SchemaField) []int {
	if root == nil {
		return nil
	}
	cols := root.Columns()
	out := make([]int, len(cols))
	nameToFieldID := make(map[string]int, len(fields))
	ambiguous := make(map[string]bool)
	for id, field := range fields {
		key := field.Name
		if prev, ok := nameToFieldID[key]; ok && prev != id {
			ambiguous[key] = true
			continue
		}
		nameToFieldID[key] = id
	}
	for idx, col := range cols {
		if id := col.ID(); id > 0 {
			if _, ok := fields[id]; ok {
				out[idx] = id
			}
			continue
		}
		if id, ok := nameToFieldID[col.Name()]; ok && !ambiguous[col.Name()] {
			out[idx] = id
		}
	}
	return out
}

func parquetChunkBounds(fieldType api.IcebergType, chunk parquet.ColumnChunk) (lower, upper []byte, nulls int64, ok bool) {
	index, err := chunk.ColumnIndex()
	if err != nil || index == nil || index.NumPages() == 0 {
		return nil, nil, 0, false
	}
	var lowerValue, upperValue pruneValue
	var hasLower, hasUpper bool
	for pageIdx := 0; pageIdx < index.NumPages(); pageIdx++ {
		nulls += index.NullCount(pageIdx)
		if index.NullPage(pageIdx) {
			continue
		}
		pageLower, okLower := parquetValuePruneValue(fieldType, index.MinValue(pageIdx))
		pageUpper, okUpper := parquetValuePruneValue(fieldType, index.MaxValue(pageIdx))
		if !okLower || !okUpper {
			continue
		}
		if !hasLower || comparePruneValue(pageLower, lowerValue) < 0 {
			lowerValue = pageLower
			hasLower = true
		}
		if !hasUpper || comparePruneValue(pageUpper, upperValue) > 0 {
			upperValue = pageUpper
			hasUpper = true
		}
	}
	if !hasLower || !hasUpper {
		return nil, nil, nulls, false
	}
	lower, okLower := encodePruneBound(fieldType, lowerValue)
	upper, okUpper := encodePruneBound(fieldType, upperValue)
	if !okLower || !okUpper {
		return nil, nil, nulls, false
	}
	return lower, upper, nulls, true
}

func parquetValuePruneValue(fieldType api.IcebergType, value parquet.Value) (pruneValue, bool) {
	if value.IsNull() {
		return pruneValue{}, false
	}
	switch fieldType.Kind {
	case api.TypeInt, api.TypeDate:
		switch value.Kind() {
		case parquet.Int32:
			return pruneValue{kind: pruneValueInt64, i64: int64(value.Int32())}, true
		case parquet.Int64:
			v := value.Int64()
			if v < math.MinInt32 || v > math.MaxInt32 {
				return pruneValue{}, false
			}
			return pruneValue{kind: pruneValueInt64, i64: v}, true
		default:
			return pruneValue{}, false
		}
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		switch value.Kind() {
		case parquet.Int64:
			return pruneValue{kind: pruneValueInt64, i64: value.Int64()}, true
		case parquet.Int32:
			return pruneValue{kind: pruneValueInt64, i64: int64(value.Int32())}, true
		default:
			return pruneValue{}, false
		}
	case api.TypeFloat:
		var v float64
		switch value.Kind() {
		case parquet.Float:
			v = float64(value.Float())
		case parquet.Double:
			v = value.Double()
		default:
			return pruneValue{}, false
		}
		if math.IsNaN(v) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: v}, true
	case api.TypeDouble:
		var v float64
		switch value.Kind() {
		case parquet.Double:
			v = value.Double()
		case parquet.Float:
			v = float64(value.Float())
		default:
			return pruneValue{}, false
		}
		if math.IsNaN(v) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: v}, true
	case api.TypeString:
		switch value.Kind() {
		case parquet.ByteArray, parquet.FixedLenByteArray:
			return pruneValue{kind: pruneValueString, str: string(value.ByteArray())}, true
		default:
			return pruneValue{}, false
		}
	default:
		return pruneValue{}, false
	}
}

func encodePruneBound(fieldType api.IcebergType, value pruneValue) ([]byte, bool) {
	switch fieldType.Kind {
	case api.TypeInt, api.TypeDate:
		if value.kind != pruneValueInt64 || value.i64 < math.MinInt32 || value.i64 > math.MaxInt32 {
			return nil, false
		}
		out := make([]byte, 4)
		binary.LittleEndian.PutUint32(out, uint32(int32(value.i64)))
		return out, true
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		if value.kind != pruneValueInt64 {
			return nil, false
		}
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, uint64(value.i64))
		return out, true
	case api.TypeFloat:
		if value.kind != pruneValueFloat64 || math.IsNaN(value.f64) {
			return nil, false
		}
		out := make([]byte, 4)
		binary.LittleEndian.PutUint32(out, math.Float32bits(float32(value.f64)))
		return out, true
	case api.TypeDouble:
		if value.kind != pruneValueFloat64 || math.IsNaN(value.f64) {
			return nil, false
		}
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, math.Float64bits(value.f64))
		return out, true
	case api.TypeString:
		if value.kind != pruneValueString {
			return nil, false
		}
		return []byte(value.str), true
	default:
		return nil, false
	}
}

func estimateIcebergParquetRowGroupBytes(fileSize, totalRows, rowGroupRows int64, rowGroupCount int) int64 {
	if fileSize <= 0 {
		return 1
	}
	if totalRows > 0 && rowGroupRows > 0 {
		size := int64(float64(fileSize) * float64(rowGroupRows) / float64(totalRows))
		if size > 0 {
			return size
		}
		return 1
	}
	if rowGroupCount > 0 {
		size := fileSize / int64(rowGroupCount)
		if size > 0 {
			return size
		}
	}
	return 1
}
