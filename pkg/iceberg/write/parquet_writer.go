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

package write

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type DataWriterConfig struct {
	Schema        api.Schema
	PartitionSpec api.PartitionSpec
	FilePath      string
	TimeZone      *time.Location
}

type ParquetDataWriter struct {
	cfg     DataWriterConfig
	writer  *parquet.GenericWriter[any]
	out     *countingWriter
	fields  []api.SchemaField
	byName  map[string]api.SchemaField
	metrics map[int]*columnMetrics

	partitionSet bool
	partition    map[string]any
	partitionKey string
	rows         int64
	estimated    int64
	closed       bool
}

func NewParquetDataWriter(ctx context.Context, cfg DataWriterConfig, dst io.Writer) (*ParquetDataWriter, error) {
	if dst == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg Parquet writer requires an output writer", nil)
	}
	if strings.TrimSpace(cfg.FilePath) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg Parquet writer requires a file path", nil)
	}
	if cfg.TimeZone == nil {
		cfg.TimeZone = time.UTC
	}
	group := make(parquet.Group, len(cfg.Schema.Fields))
	byName := make(map[string]api.SchemaField, len(cfg.Schema.Fields))
	for _, field := range cfg.Schema.Fields {
		node, err := parquetNodeForField(ctx, field)
		if err != nil {
			return nil, err
		}
		group[field.Name] = parquet.FieldID(node, field.ID)
		byName[strings.ToLower(field.Name)] = field
	}
	cw := &countingWriter{writer: dst}
	return &ParquetDataWriter{
		cfg:     cfg,
		writer:  parquet.NewGenericWriter[any](cw, parquet.NewSchema("iceberg", group)),
		out:     cw,
		fields:  append([]api.SchemaField(nil), cfg.Schema.Fields...),
		byName:  byName,
		metrics: make(map[int]*columnMetrics, len(cfg.Schema.Fields)),
	}, nil
}

func (w *ParquetDataWriter) WriteBatch(ctx context.Context, attrs []string, bat *batch.Batch) error {
	return w.WriteRows(ctx, attrs, bat, nil)
}

func (w *ParquetDataWriter) WriteRows(ctx context.Context, attrs []string, bat *batch.Batch, rowIndexes []int) error {
	if w == nil || w.writer == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg Parquet writer is not initialized", nil)
	}
	if w.closed {
		return api.NewError(api.ErrConfigInvalid, "Iceberg Parquet writer is already closed", nil)
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(attrs) != len(bat.Vecs) {
		return moerr.NewInvalidInputf(ctx, "Iceberg Parquet writer attrs/vector mismatch: attrs=%d vectors=%d", len(attrs), len(bat.Vecs))
	}
	if len(rowIndexes) == 0 {
		rowIndexes = make([]int, bat.RowCount())
		for i := range rowIndexes {
			rowIndexes[i] = i
		}
	}
	rows := make([]any, len(rowIndexes))
	for outIdx, rowIdx := range rowIndexes {
		if rowIdx < 0 || rowIdx >= bat.RowCount() {
			return moerr.NewInvalidInputf(ctx, "Iceberg Parquet writer row index out of range: row=%d row_count=%d", rowIdx, bat.RowCount())
		}
		row := make(map[string]any, len(w.fields))
		rowValues := make(map[int]any, len(w.fields))
		seenFieldIDs := make(map[int]struct{}, len(attrs))
		for colIdx, attr := range attrs {
			field, ok := w.byName[strings.ToLower(attr)]
			if !ok {
				return api.NewError(api.ErrMetadataInvalid, "Iceberg writer column is not present in schema", map[string]string{"column": attr})
			}
			seenFieldIDs[field.ID] = struct{}{}
			value, isNull, err := vectorValue(ctx, bat.Vecs[colIdx], rowIdx, field.Type, w.cfg.TimeZone)
			if err != nil {
				return err
			}
			if isNull {
				row[field.Name] = nil
				rowValues[field.ID] = nil
				w.metric(field).observeNull()
				continue
			}
			row[field.Name] = value
			rowValues[field.ID] = value
			if err := w.metric(field).observe(ctx, field.Type, value); err != nil {
				return err
			}
			w.estimated += estimatedValueSize(value)
		}
		if err := w.fillMissingFields(row, rowValues, seenFieldIDs); err != nil {
			return err
		}
		partition, err := partitionTuple(ctx, w.cfg.PartitionSpec, w.fields, rowValues)
		if err != nil {
			return err
		}
		if err := w.observePartition(ctx, partition); err != nil {
			return err
		}
		rows[outIdx] = row
	}
	if _, err := w.writer.Write(rows); err != nil {
		return api.WrapError(api.ErrObjectIO, "Iceberg Parquet writer failed to encode rows", nil, err)
	}
	w.rows += int64(len(rowIndexes))
	return nil
}

func (w *ParquetDataWriter) fillMissingFields(
	row map[string]any,
	rowValues map[int]any,
	seenFieldIDs map[int]struct{},
) error {
	for _, field := range w.fields {
		if _, ok := seenFieldIDs[field.ID]; ok {
			continue
		}
		if field.Required {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg writer required column is missing", map[string]string{"column": field.Name})
		}
		row[field.Name] = nil
		rowValues[field.ID] = nil
		w.metric(field).observeNull()
	}
	return nil
}

func (w *ParquetDataWriter) Close(ctx context.Context) (api.DataFile, error) {
	if w == nil || w.writer == nil {
		return api.DataFile{}, api.NewError(api.ErrConfigInvalid, "Iceberg Parquet writer is not initialized", nil)
	}
	if !w.closed {
		if err := w.writer.Close(); err != nil {
			return api.DataFile{}, api.WrapError(api.ErrObjectIO, "Iceberg Parquet writer failed to close", nil, err)
		}
		w.closed = true
	}
	file := api.DataFile{
		Content:           api.DataFileContentData,
		FilePath:          w.cfg.FilePath,
		FileFormat:        "parquet",
		Partition:         clonePartition(w.partition),
		PartitionFieldIDs: partitionFieldIDs(w.cfg.PartitionSpec),
		RecordCount:       w.rows,
		FileSizeInBytes:   w.out.n,
		ValueCounts:       make(map[int]int64),
		NullValueCounts:   make(map[int]int64),
		NaNValueCounts:    make(map[int]int64),
		LowerBounds:       make(map[int][]byte),
		UpperBounds:       make(map[int][]byte),
		SpecID:            w.cfg.PartitionSpec.SpecID,
		FilePathRedacted:  api.RedactPath(w.cfg.FilePath),
		FilePathHash:      api.PathHash(w.cfg.FilePath),
	}
	for _, field := range w.fields {
		metric := w.metrics[field.ID]
		if metric == nil {
			file.ValueCounts[field.ID] = w.rows
			continue
		}
		file.ValueCounts[field.ID] = w.rows
		file.NullValueCounts[field.ID] = metric.nullCount
		if metric.nanCount > 0 {
			file.NaNValueCounts[field.ID] = metric.nanCount
		}
		if len(metric.lower) > 0 {
			file.LowerBounds[field.ID] = append([]byte(nil), metric.lower...)
		}
		if len(metric.upper) > 0 {
			file.UpperBounds[field.ID] = append([]byte(nil), metric.upper...)
		}
	}
	return file, nil
}

func partitionFieldIDs(spec api.PartitionSpec) map[string]int {
	if len(spec.Fields) == 0 {
		return nil
	}
	out := make(map[string]int, len(spec.Fields))
	for _, field := range spec.Fields {
		if field.Name != "" && field.FieldID != 0 {
			out[field.Name] = field.FieldID
		}
	}
	return out
}

func (w *ParquetDataWriter) PartitionPath() string {
	if len(w.partition) == 0 {
		return ""
	}
	parts := make([]string, 0, len(w.cfg.PartitionSpec.Fields))
	for _, field := range w.cfg.PartitionSpec.Fields {
		value, ok := w.partition[field.Name]
		if !ok {
			continue
		}
		parts = append(parts, field.Name+"="+partitionValueString(value))
	}
	return path.Join(parts...)
}

func (w *ParquetDataWriter) ApproxSize() int64 {
	if w == nil || w.out == nil {
		return 0
	}
	if w.out.n > w.estimated {
		return w.out.n
	}
	return w.estimated
}

func (w *ParquetDataWriter) metric(field api.SchemaField) *columnMetrics {
	metric := w.metrics[field.ID]
	if metric == nil {
		metric = &columnMetrics{}
		w.metrics[field.ID] = metric
	}
	return metric
}

func (w *ParquetDataWriter) observePartition(ctx context.Context, partition map[string]any) error {
	key := partitionKey(partition, w.cfg.PartitionSpec)
	if !w.partitionSet {
		w.partitionSet = true
		w.partitionKey = key
		w.partition = clonePartition(partition)
		return nil
	}
	if key != w.partitionKey {
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg writer received multiple partition tuples in one data file", map[string]string{"partition": key})
	}
	return nil
}

func parquetNodeForField(ctx context.Context, field api.SchemaField) (parquet.Node, error) {
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
	case api.TypeDecimal:
		if field.Type.Precision <= 0 || field.Type.Precision > 18 {
			return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer decimal precision is unsupported", map[string]string{"field": field.Name, "type": field.Type.String()})
		}
		node = parquet.Decimal(field.Type.Scale, field.Type.Precision, parquet.Int64Type)
	case api.TypeString:
		node = icebergStringNode()
	case api.TypeDate:
		node = parquet.Date()
	case api.TypeTimestamp:
		node = parquet.TimestampAdjusted(parquet.Microsecond, false)
	case api.TypeTimestampTZ:
		node = parquet.Timestamp(parquet.Microsecond)
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer type is unsupported", map[string]string{"field": field.Name, "type": field.Type.String()})
	}
	if field.Required {
		return parquet.Required(node), nil
	}
	return parquet.Optional(node), nil
}

func icebergStringNode() parquet.Node {
	return parquet.Encoded(parquet.String(), &parquet.Plain)
}

func vectorValue(ctx context.Context, vec *vector.Vector, row int, typ api.IcebergType, loc *time.Location) (any, bool, error) {
	if vec == nil {
		return nil, false, api.NewError(api.ErrMetadataInvalid, "Iceberg writer batch contains a nil vector", nil)
	}
	if vec.GetNulls().Contains(uint64(row)) {
		return nil, true, nil
	}
	switch typ.Kind {
	case api.TypeBoolean:
		if vec.GetType().Oid != types.T_bool {
			return nil, false, typeMismatch(ctx, typ, vec)
		}
		return vector.GetFixedAtNoTypeCheck[bool](vec, row), false, nil
	case api.TypeInt:
		return intValue(ctx, vec, row, typ)
	case api.TypeLong:
		return longValue(ctx, vec, row, typ)
	case api.TypeFloat:
		if vec.GetType().Oid != types.T_float32 {
			return nil, false, typeMismatch(ctx, typ, vec)
		}
		return vector.GetFixedAtNoTypeCheck[float32](vec, row), false, nil
	case api.TypeDouble:
		if vec.GetType().Oid != types.T_float64 {
			return nil, false, typeMismatch(ctx, typ, vec)
		}
		return vector.GetFixedAtNoTypeCheck[float64](vec, row), false, nil
	case api.TypeDecimal:
		return decimalValue(ctx, vec, row, typ)
	case api.TypeString:
		switch vec.GetType().Oid {
		case types.T_char, types.T_varchar, types.T_text:
			return string(vec.GetBytesAt(row)), false, nil
		default:
			return nil, false, typeMismatch(ctx, typ, vec)
		}
	case api.TypeDate:
		if vec.GetType().Oid != types.T_date {
			return nil, false, typeMismatch(ctx, typ, vec)
		}
		return int32(vector.GetFixedAtNoTypeCheck[types.Date](vec, row) - types.DateFromCalendar(1970, 1, 1)), false, nil
	case api.TypeTimestamp:
		switch vec.GetType().Oid {
		case types.T_datetime:
			return int64(vector.GetFixedAtNoTypeCheck[types.Datetime](vec, row) - types.DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0)), false, nil
		case types.T_timestamp:
			return int64(vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row).ToDatetime(loc) - types.DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0)), false, nil
		default:
			return nil, false, typeMismatch(ctx, typ, vec)
		}
	case api.TypeTimestampTZ:
		if vec.GetType().Oid != types.T_timestamp {
			return nil, false, typeMismatch(ctx, typ, vec)
		}
		return int64(vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row)) - int64(types.DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0)), false, nil
	default:
		return nil, false, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer type is unsupported", map[string]string{"type": typ.String()})
	}
}

func decimalValue(ctx context.Context, vec *vector.Vector, row int, typ api.IcebergType) (any, bool, error) {
	if typ.Precision <= 0 || typ.Precision > 18 {
		return nil, false, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer decimal precision is unsupported", map[string]string{"type": typ.String()})
	}
	scaleDelta := int32(typ.Scale) - vec.GetType().Scale
	switch vec.GetType().Oid {
	case types.T_decimal64:
		value := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row)
		if scaleDelta != 0 {
			var err error
			value, err = value.Scale(scaleDelta)
			if err != nil {
				return nil, false, err
			}
		}
		return int64(value), false, nil
	case types.T_decimal128:
		value := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row)
		if scaleDelta != 0 {
			var err error
			value, err = value.Scale(scaleDelta)
			if err != nil {
				return nil, false, err
			}
		}
		out, err := decimal128ToInt64(ctx, value, typ)
		if err != nil {
			return nil, false, err
		}
		return out, false, nil
	default:
		return nil, false, typeMismatch(ctx, typ, vec)
	}
}

func decimal128ToInt64(ctx context.Context, value types.Decimal128, typ api.IcebergType) (int64, error) {
	lo := value.B0_63
	hi := value.B64_127
	if hi == 0 && lo <= uint64(1)<<63-1 {
		return int64(lo), nil
	}
	if hi == ^uint64(0) && lo >= uint64(1)<<63 {
		return int64(lo), nil
	}
	return 0, moerr.NewInvalidInputf(ctx, "Iceberg writer decimal value overflows int64 storage for %s", typ.String())
}

func intValue(ctx context.Context, vec *vector.Vector, row int, typ api.IcebergType) (any, bool, error) {
	switch vec.GetType().Oid {
	case types.T_int8:
		return int32(vector.GetFixedAtNoTypeCheck[int8](vec, row)), false, nil
	case types.T_int16:
		return int32(vector.GetFixedAtNoTypeCheck[int16](vec, row)), false, nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, row), false, nil
	default:
		return nil, false, typeMismatch(ctx, typ, vec)
	}
}

func longValue(ctx context.Context, vec *vector.Vector, row int, typ api.IcebergType) (any, bool, error) {
	switch vec.GetType().Oid {
	case types.T_int8:
		return int64(vector.GetFixedAtNoTypeCheck[int8](vec, row)), false, nil
	case types.T_int16:
		return int64(vector.GetFixedAtNoTypeCheck[int16](vec, row)), false, nil
	case types.T_int32:
		return int64(vector.GetFixedAtNoTypeCheck[int32](vec, row)), false, nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, row), false, nil
	default:
		return nil, false, typeMismatch(ctx, typ, vec)
	}
}

func typeMismatch(ctx context.Context, typ api.IcebergType, vec *vector.Vector) error {
	return moerr.NewInvalidInputf(ctx, "Iceberg writer type mismatch: iceberg=%s mo=%s", typ.String(), vec.GetType().String())
}

type columnMetrics struct {
	nullCount int64
	nanCount  int64
	hasValue  bool
	lower     []byte
	upper     []byte
	compare   any
}

func (m *columnMetrics) observeNull() {
	m.nullCount++
}

func (m *columnMetrics) observe(ctx context.Context, typ api.IcebergType, value any) error {
	if isNaN(value) {
		m.nanCount++
		return nil
	}
	encoded, cmp, err := encodeBound(ctx, typ, value)
	if err != nil {
		return err
	}
	if !m.hasValue {
		m.hasValue = true
		m.compare = cmp
		m.lower = encoded
		m.upper = append([]byte(nil), encoded...)
		return nil
	}
	if compareMetricValue(cmp, m.compare) < 0 {
		m.compare = cmp
		m.lower = encoded
	}
	if compareMetricValue(cmp, decodeMetricValue(typ, m.upper)) > 0 {
		m.upper = encoded
	}
	return nil
}

func encodeBound(ctx context.Context, typ api.IcebergType, value any) ([]byte, any, error) {
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
	case api.TypeDecimal:
		v := value.(int64)
		return decimalInt64BoundBytes(v), v, nil
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
		return nil, nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer bound type is unsupported", map[string]string{"type": typ.String()})
	}
}

func decimalInt64BoundBytes(value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	start := 0
	if value >= 0 {
		for start < len(buf)-1 && buf[start] == 0x00 && buf[start+1]&0x80 == 0 {
			start++
		}
	} else {
		for start < len(buf)-1 && buf[start] == 0xff && buf[start+1]&0x80 != 0 {
			start++
		}
	}
	return append([]byte(nil), buf[start:]...)
}

func decodeMetricValue(typ api.IcebergType, data []byte) any {
	switch typ.Kind {
	case api.TypeBoolean:
		return len(data) > 0 && data[0] != 0
	case api.TypeInt, api.TypeDate:
		return int64(int32(binary.LittleEndian.Uint32(data)))
	case api.TypeLong, api.TypeTimestamp, api.TypeTimestampTZ:
		return int64(binary.LittleEndian.Uint64(data))
	case api.TypeDecimal:
		return decimalInt64FromBoundBytes(data)
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

func decimalInt64FromBoundBytes(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}
	var buf [8]byte
	if data[0]&0x80 != 0 {
		for idx := range buf {
			buf[idx] = 0xff
		}
	}
	if len(data) > len(buf) {
		data = data[len(data)-len(buf):]
	}
	copy(buf[len(buf)-len(data):], data)
	return int64(binary.BigEndian.Uint64(buf[:]))
}

func compareMetricValue(left, right any) int {
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

func isNaN(value any) bool {
	switch v := value.(type) {
	case float32:
		return math.IsNaN(float64(v))
	case float64:
		return math.IsNaN(v)
	default:
		return false
	}
}

func estimatedValueSize(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 1
	case bool:
		return 1
	case int32, float32:
		return 4
	case int64, float64:
		return 8
	case string:
		return int64(len(v))
	default:
		return int64(len(strings.TrimSpace(fmt.Sprint(v))))
	}
}

func partitionTuple(ctx context.Context, spec api.PartitionSpec, fields []api.SchemaField, values map[int]any) (map[string]any, error) {
	if len(spec.Fields) == 0 {
		return nil, nil
	}
	fieldByID := make(map[int]api.SchemaField, len(fields))
	for _, field := range fields {
		fieldByID[field.ID] = field
	}
	out := make(map[string]any, len(spec.Fields))
	for _, part := range spec.Fields {
		field, ok := fieldByID[part.SourceID]
		if !ok {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg partition source field is missing", map[string]string{"field_id": strconv.Itoa(part.SourceID)})
		}
		raw, ok := values[field.ID]
		if !ok {
			if field.Required {
				return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer partition field is missing", map[string]string{"field": field.Name})
			}
			raw = nil
		}
		value, err := transformPartitionValue(ctx, field.Type, part.Transform, raw)
		if err != nil {
			return nil, err
		}
		out[part.Name] = value
	}
	return out, nil
}

func transformPartitionValue(ctx context.Context, typ api.IcebergType, transform string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	transform = strings.ToLower(strings.TrimSpace(transform))
	if transform == "" || transform == "identity" {
		return value, nil
	}
	switch typ.Kind {
	case api.TypeDate:
		days := int64(value.(int32))
		return transformTemporal(ctx, transform, time.Unix(days*86400, 0).UTC())
	case api.TypeTimestamp, api.TypeTimestampTZ:
		return transformTemporal(ctx, transform, time.UnixMicro(value.(int64)).UTC())
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer partition transform is unsupported for field type", map[string]string{"transform": transform, "type": typ.String()})
	}
}

func transformTemporal(ctx context.Context, transform string, ts time.Time) (any, error) {
	switch transform {
	case "year":
		return int32(ts.Year() - 1970), nil
	case "month":
		return int32((ts.Year()-1970)*12 + int(ts.Month()) - 1), nil
	case "day":
		return int32(ts.Unix() / 86400), nil
	case "hour":
		return ts.Unix() / 3600, nil
	default:
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg writer partition transform is unsupported", map[string]string{"transform": transform})
	}
}

func partitionKey(values map[string]any, spec api.PartitionSpec) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(spec.Fields))
	for _, field := range spec.Fields {
		parts = append(parts, field.Name+"="+partitionValueString(values[field.Name]))
	}
	return strings.Join(parts, "/")
}

func partitionValueString(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func clonePartition(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type countingWriter struct {
	writer io.Writer
	n      int64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.n += int64(n)
	return n, err
}
