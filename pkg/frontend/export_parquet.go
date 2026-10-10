// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/parquet-go/parquet-go"
)

// ParquetWriter handles writing data to Parquet format
type ParquetWriter struct {
	ctx         context.Context
	buf         *bytes.Buffer
	writer      *parquet.GenericWriter[any]
	schema      *parquet.Schema
	columnNames []string
	columnTypes []defines.MysqlType
}

// NewParquetWriter creates a new ParquetWriter
func NewParquetWriter(ctx context.Context, mrs *MysqlResultSet) (*ParquetWriter, error) {
	if mrs == nil || len(mrs.Columns) == 0 {
		return nil, moerr.NewInternalError(ctx, "no columns for parquet export")
	}

	columnNames := make([]string, len(mrs.Columns))
	columnTypes := make([]defines.MysqlType, len(mrs.Columns))
	columnNamesSeen := make(map[string]struct{}, len(mrs.Columns))

	// Build parquet schema from column definitions using Group (map[string]Node)
	group := make(parquet.Group)
	for i, col := range mrs.Columns {
		columnNames[i] = col.Name()
		columnKey := strings.ToLower(columnNames[i])
		if _, exists := columnNamesSeen[columnKey]; exists {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate column name %q in parquet export", columnNames[i])
		}
		columnNamesSeen[columnKey] = struct{}{}
		// Get the column type from MysqlColumn
		mysqlCol, ok := col.(*MysqlColumn)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid column type")
		}
		columnTypes[i] = mysqlCol.ColumnType()
		group[columnNames[i]] = buildParquetNode(columnTypes[i], mysqlCol.Flag())
	}

	schema := parquet.NewSchema("export", group)
	buf := &bytes.Buffer{}
	// Disable parquet-go's separate write buffer so Flush makes the serialized
	// row-group bytes observable through buf.Len for split-size accounting.
	writer := parquet.NewGenericWriter[any](buf, schema, parquet.WriteBufferSize(0))

	return &ParquetWriter{
		ctx:         ctx,
		buf:         buf,
		writer:      writer,
		schema:      schema,
		columnNames: columnNames,
		columnTypes: columnTypes,
	}, nil
}

// buildParquetNode creates a parquet node from MySQL type
func buildParquetNode(typ defines.MysqlType, flag uint16) parquet.Node {
	isUnsigned := flag&uint16(defines.UNSIGNED_FLAG) != 0
	// All fields are optional (nullable) by default
	switch typ {
	case defines.MYSQL_TYPE_BOOL:
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))
	case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24:
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case defines.MYSQL_TYPE_LONG:
		// For unsigned int32, use Int64 to avoid overflow
		if isUnsigned {
			return parquet.Optional(parquet.Leaf(parquet.Int64Type))
		}
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case defines.MYSQL_TYPE_BIT:
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_LONGLONG:
		if isUnsigned {
			// Parquet INT64 cannot represent the complete BIGINT UNSIGNED
			// domain. Keep the decimal representation so LOAD can parse values
			// above math.MaxInt64 without loss.
			return parquet.Optional(parquet.String())
		}
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case defines.MYSQL_TYPE_FLOAT:
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case defines.MYSQL_TYPE_DOUBLE:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case defines.MYSQL_TYPE_TIMESTAMP:
		return parquet.Optional(parquet.TimestampAdjusted(parquet.Microsecond, true))
	case defines.MYSQL_TYPE_YEAR:
		// YEAR is represented as its canonical four-digit text so that the
		// loader can preserve the MySQL YEAR domain (including 0000).
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIME:
		// Use string representation for date/time types for simplicity and compatibility
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_DECIMAL:
		// Use string representation for decimals
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
		defines.MYSQL_TYPE_JSON, defines.MYSQL_TYPE_UUID, defines.MYSQL_TYPE_TEXT:
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_TINY_BLOB, defines.MYSQL_TYPE_BLOB,
		defines.MYSQL_TYPE_MEDIUM_BLOB, defines.MYSQL_TYPE_LONG_BLOB:
		if flag&uint16(defines.BINARY_FLAG) != 0 {
			return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
		}
		return parquet.Optional(parquet.String())
	default:
		// Default to string for unknown types
		return parquet.Optional(parquet.String())
	}
}

// WriteBatch writes a batch of data to the parquet writer
func (pw *ParquetWriter) WriteBatch(bat *batch.Batch, mp *mpool.MPool, timeZone *time.Location) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	return pw.writeBatchRange(bat, 0, bat.RowCount(), timeZone)
}

func (pw *ParquetWriter) writeBatchRange(bat *batch.Batch, start, end int, timeZone *time.Location) error {
	if bat == nil || start >= end {
		return nil
	}

	rows := make([]any, end-start)
	for i := range rows {
		row := make(map[string]any)
		rows[i] = row
	}

	if len(bat.Vecs) != len(pw.columnNames) {
		return moerr.NewInternalErrorf(pw.ctx, "parquet batch has %d vectors for %d columns", len(bat.Vecs), len(pw.columnNames))
	}

	// Convert each column
	for colIdx, vec := range bat.Vecs {
		if vec == nil {
			return moerr.NewInternalErrorf(pw.ctx, "parquet batch vector %d is nil", colIdx)
		}
		colName := pw.columnNames[colIdx]
		for rowIdx := start; rowIdx < end; rowIdx++ {
			row := rows[rowIdx-start].(map[string]any)
			if nulls := vec.GetNulls(); nulls != nil && nulls.Contains(uint64(rowIdx)) {
				row[colName] = nil
				continue
			}
			val, err := pw.vectorValueToParquet(vec, colIdx, rowIdx, timeZone)
			if err != nil {
				return err
			}
			row[colName] = val
		}
	}

	_, err := pw.writer.Write(rows)
	return err
}

// vectorValueToParquet preserves the MySQL column domain when the execution
// vector carries a compatible temporal representation. YEAR can arrive as a
// DATE vector on the SELECT projection path, but its exported value must still
// be the four-digit year rather than a calendar date.
func (pw *ParquetWriter) vectorValueToParquet(vec *vector.Vector, colIdx, rowIdx int, timeZone *time.Location) (any, error) {
	if pw.columnTypes[colIdx] == defines.MYSQL_TYPE_YEAR && vec.GetType().Oid == types.T_date {
		value := vector.GetFixedAtNoTypeCheck[types.Date](vec, rowIdx)
		return types.MoYear(value.Year()).String(), nil
	}
	return vectorValueToParquet(vec, rowIdx, timeZone)
}

// vectorValueToParquet converts a vector value to a parquet-compatible Go value
func vectorValueToParquet(vec *vector.Vector, i int, timeZone *time.Location) (any, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, i), nil
	case types.T_int8:
		return int32(vector.GetFixedAtNoTypeCheck[int8](vec, i)), nil
	case types.T_int16:
		return int32(vector.GetFixedAtNoTypeCheck[int16](vec, i)), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, i), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, i), nil
	case types.T_uint8:
		return int32(vector.GetFixedAtNoTypeCheck[uint8](vec, i)), nil
	case types.T_uint16:
		return int32(vector.GetFixedAtNoTypeCheck[uint16](vec, i)), nil
	case types.T_uint32:
		// Use int64 to avoid overflow (uint32 max > int32 max)
		return int64(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), nil
	case types.T_uint64:
		return strconv.FormatUint(vector.GetFixedAtNoTypeCheck[uint64](vec, i), 10), nil
	case types.T_bit:
		return strconv.FormatUint(vector.GetFixedAtNoTypeCheck[uint64](vec, i), 10), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, i), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, i), nil
	case types.T_char, types.T_varchar, types.T_text:
		return string(vec.GetBytesAt(i)), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return vec.GetBytesAt(i), nil
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		return val.String(), nil
	case types.T_date:
		val := vector.GetFixedAtNoTypeCheck[types.Date](vec, i)
		return val.String(), nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)
		return val, nil
	case types.T_time:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)
		return val, nil
	case types.T_timestamp:
		// The internal timestamp stores the instant as microseconds since the
		// Gregorian epoch used by MatrixOne. Parquet's adjusted UTC timestamp
		// stores microseconds since the Unix epoch, so subtract the same epoch
		// offset here. This representation is independent of the session zone.
		val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i)
		return int64(val) - int64(types.UnixMicroToTimestamp(0)), nil
	case types.T_array_float32:
		return types.BytesToArrayToString[float32](vec.GetBytesAt(i)), nil
	case types.T_array_float64:
		return types.BytesToArrayToString[float64](vec.GetBytesAt(i)), nil
	case types.T_year:
		return vector.GetFixedAtNoTypeCheck[types.MoYear](vec, i).String(), nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
		return val, nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
		return val, nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale)
		return val, nil
	case types.T_uuid:
		val := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()
		return val, nil
	case types.T_enum:
		val := vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()
		return val, nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "unsupported type for parquet export: %v", vec.GetType().Oid)
	}
}

// Close closes the parquet writer and returns the complete parquet data
func (pw *ParquetWriter) Close() ([]byte, error) {
	if err := pw.writer.Close(); err != nil {
		return nil, err
	}
	return pw.buf.Bytes(), nil
}

// Flush makes buffered row-group data visible in the output buffer. It is
// used by split-size accounting before the writer is closed for a file.
func (pw *ParquetWriter) Flush() error {
	return pw.writer.Flush()
}

// Reset resets the writer for a new file
func (pw *ParquetWriter) Reset() {
	pw.buf.Reset()
	pw.writer.Reset(pw.buf)
}

// Size returns the current buffer size in bytes
func (pw *ParquetWriter) Size() int {
	return pw.buf.Len()
}
