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

	// Build parquet schema from column definitions using Group (map[string]Node)
	group := make(parquet.Group)
	for i, col := range mrs.Columns {
		columnNames[i] = col.Name()
		// Get the column type from MysqlColumn
		mysqlCol, ok := col.(*MysqlColumn)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid column type")
		}
		columnTypes[i] = mysqlCol.ColumnType()
		group[columnNames[i]] = buildParquetNode(columnTypes[i])
	}

	schema := parquet.NewSchema("export", group)
	buf := &bytes.Buffer{}
	writer := parquet.NewGenericWriter[any](buf, schema)

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
func buildParquetNode(typ defines.MysqlType) parquet.Node {
	// All fields are optional (nullable) by default
	switch typ {
	case defines.MYSQL_TYPE_BOOL:
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))
	case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case defines.MYSQL_TYPE_LONGLONG:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case defines.MYSQL_TYPE_FLOAT:
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case defines.MYSQL_TYPE_DOUBLE:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_TIME:
		// Use string representation for date/time types for simplicity and compatibility
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_DECIMAL:
		// Use string representation for decimals
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
		defines.MYSQL_TYPE_JSON, defines.MYSQL_TYPE_UUID, defines.MYSQL_TYPE_TEXT:
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_BLOB:
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
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

	rows := make([]any, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		row := make(map[string]any)
		rows[i] = row
	}

	// Convert each column
	for colIdx, vec := range bat.Vecs {
		colName := pw.columnNames[colIdx]
		for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
			row := rows[rowIdx].(map[string]any)
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colName] = nil
				continue
			}
			val, err := vectorValueToParquet(vec, rowIdx, timeZone)
			if err != nil {
				return err
			}
			row[colName] = val
		}
	}

	_, err := pw.writer.Write(rows)
	return err
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
		return int32(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), nil
	case types.T_uint64:
		return int64(vector.GetFixedAtNoTypeCheck[uint64](vec, i)), nil
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
		if timeZone == nil {
			timeZone = time.UTC
		}
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale)
		return val, nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
		return val, nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
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

// Reset resets the writer for a new file
func (pw *ParquetWriter) Reset() {
	pw.buf.Reset()
	pw.writer.Reset(pw.buf)
}
