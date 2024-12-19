// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// extractRowFromEveryVector gets the j row from the every vector and outputs the row.
// !!!NOTE!!! use safeRefSlice before you know what you are doing.
// safeRefSlice is used to determine whether to copy the slice or not.
// types.T_json, types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink are
// stored as bytes slice.
// get every row from colSlices that holds the slice value of the vector to avoid converting the slice for every row.
func extractRowFromEveryVector(ctx context.Context, ses FeSession, dataSet *batch.Batch, j int, row []any, safeRefSlice bool, colSlices []any) error {
	var rowIndex = j
	for i, vec := range dataSet.Vecs { //col index
		rowIndexBackup := rowIndex
		if vec.IsConstNull() {
			row[i] = nil
			continue
		}
		if vec.IsConst() {
			rowIndex = 0
		}

		err := extractRowFromVector(ctx, ses, vec, i, row, rowIndex, safeRefSlice, colSlices)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, ses FeSession, vec *vector.Vector, i int, row []any, rowIndex int, safeRefSlice bool, colSlices []any) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(copyBytes(vec.GetBytesAt2(colSlices[i].([]types.Varlena), rowIndex), !safeRefSlice))
	case types.T_bool:
		row[i] = GetFixedAtNoTypeCheck[bool](vec, colSlices[i].([]bool), rowIndex)
	case types.T_bit:
		row[i] = GetFixedAtNoTypeCheck[uint64](vec, colSlices[i].([]uint64), rowIndex)
	case types.T_int8:
		row[i] = GetFixedAtNoTypeCheck[int8](vec, colSlices[i].([]int8), rowIndex)
	case types.T_uint8:
		row[i] = GetFixedAtNoTypeCheck[uint8](vec, colSlices[i].([]uint8), rowIndex)
	case types.T_int16:
		row[i] = GetFixedAtNoTypeCheck[int16](vec, colSlices[i].([]int16), rowIndex)
	case types.T_uint16:
		row[i] = GetFixedAtNoTypeCheck[uint16](vec, colSlices[i].([]uint16), rowIndex)
	case types.T_int32:
		row[i] = GetFixedAtNoTypeCheck[int32](vec, colSlices[i].([]int32), rowIndex)
	case types.T_uint32:
		row[i] = GetFixedAtNoTypeCheck[uint32](vec, colSlices[i].([]uint32), rowIndex)
	case types.T_int64:
		row[i] = GetFixedAtNoTypeCheck[int64](vec, colSlices[i].([]int64), rowIndex)
	case types.T_uint64:
		row[i] = GetFixedAtNoTypeCheck[uint64](vec, colSlices[i].([]uint64), rowIndex)
	case types.T_float32:
		row[i] = GetFixedAtNoTypeCheck[float32](vec, colSlices[i].([]float32), rowIndex)
	case types.T_float64:
		row[i] = GetFixedAtNoTypeCheck[float64](vec, colSlices[i].([]float64), rowIndex)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		row[i] = copyBytes(vec.GetBytesAt2(colSlices[i].([]types.Varlena), rowIndex), !safeRefSlice)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		row[i] = vector.GetArrayAt2[float32](vec, colSlices[i].([]types.Varlena), rowIndex)
	case types.T_array_float64:
		row[i] = vector.GetArrayAt2[float64](vec, colSlices[i].([]types.Varlena), rowIndex)
	case types.T_date:
		row[i] = GetFixedAtNoTypeCheck[types.Date](vec, colSlices[i].([]types.Date), rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = GetFixedAtNoTypeCheck[types.Datetime](vec, colSlices[i].([]types.Datetime), rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = GetFixedAtNoTypeCheck[types.Time](vec, colSlices[i].([]types.Time), rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = GetFixedAtNoTypeCheck[types.Timestamp](vec, colSlices[i].([]types.Timestamp), rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = GetFixedAtNoTypeCheck[types.Decimal64](vec, colSlices[i].([]types.Decimal64), rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = GetFixedAtNoTypeCheck[types.Decimal128](vec, colSlices[i].([]types.Decimal128), rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = GetFixedAtNoTypeCheck[types.Uuid](vec, colSlices[i].([]types.Uuid), rowIndex).String()
	case types.T_Rowid:
		row[i] = GetFixedAtNoTypeCheck[types.Rowid](vec, colSlices[i].([]types.Rowid), rowIndex)
	case types.T_Blockid:
		row[i] = GetFixedAtNoTypeCheck[types.Blockid](vec, colSlices[i].([]types.Blockid), rowIndex)
	case types.T_TS:
		row[i] = GetFixedAtNoTypeCheck[types.TS](vec, colSlices[i].([]types.TS), rowIndex)
	case types.T_enum:
		row[i] = GetFixedAtNoTypeCheck[types.Enum](vec, colSlices[i].([]types.Enum), rowIndex)
	default:
		ses.Error(ctx,
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

func GetFixedAtNoTypeCheck[T any](v *vector.Vector, slice []T, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	return slice[idx]
}

// convertBatchToSlices converts the vectors in the batch to slices.
func convertBatchToSlices(ctx context.Context, ses FeSession, dataSet *batch.Batch, colSlices []any) error {
	for i, vec := range dataSet.Vecs { //col index
		if vec.IsConstNull() {
			continue
		}

		err := convertVectorToSlice(ctx, ses, vec, i, colSlices)
		if err != nil {
			return err
		}
	}
	return nil
}

func convertVectorToSlice(ctx context.Context, ses FeSession, vec *vector.Vector, i int, colSlices []any) error {
	if vec.IsConstNull() {
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Varlena](vec)
	case types.T_bool:
		colSlices[i] = vector.ToSliceNoTypeCheck2[bool](vec)
	case types.T_bit:
		colSlices[i] = vector.ToSliceNoTypeCheck2[uint64](vec)
	case types.T_int8:
		colSlices[i] = vector.ToSliceNoTypeCheck2[int8](vec)
	case types.T_uint8:
		colSlices[i] = vector.ToSliceNoTypeCheck2[uint8](vec)
	case types.T_int16:
		colSlices[i] = vector.ToSliceNoTypeCheck2[int16](vec)
	case types.T_uint16:
		colSlices[i] = vector.ToSliceNoTypeCheck2[uint16](vec)
	case types.T_int32:
		colSlices[i] = vector.ToSliceNoTypeCheck2[int32](vec)
	case types.T_uint32:
		colSlices[i] = vector.ToSliceNoTypeCheck2[uint32](vec)
	case types.T_int64:
		colSlices[i] = vector.ToSliceNoTypeCheck2[int64](vec)
	case types.T_uint64:
		colSlices[i] = vector.ToSliceNoTypeCheck2[uint64](vec)
	case types.T_float32:
		colSlices[i] = vector.ToSliceNoTypeCheck2[float32](vec)
	case types.T_float64:
		colSlices[i] = vector.ToSliceNoTypeCheck2[float64](vec)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Varlena](vec)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Varlena](vec)
	case types.T_array_float64:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Varlena](vec)
	case types.T_date:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Date](vec)
	case types.T_datetime:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Datetime](vec)
	case types.T_time:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Time](vec)
	case types.T_timestamp:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Timestamp](vec)
	case types.T_decimal64:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Decimal64](vec)
	case types.T_decimal128:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Decimal128](vec)
	case types.T_uuid:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Uuid](vec)
	case types.T_Rowid:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Rowid](vec)
	case types.T_Blockid:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Blockid](vec)
	case types.T_TS:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.TS](vec)
	case types.T_enum:
		colSlices[i] = vector.ToSliceNoTypeCheck2[types.Enum](vec)
	default:
		ses.Error(ctx,
			"Failed to convert vector to slice, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "convertVectorToSlice : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}
