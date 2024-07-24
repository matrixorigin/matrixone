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

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
func extractRowFromEveryVector(ctx context.Context, ses FeSession, dataSet *batch.Batch, j int, row []any) error {
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

		err := extractRowFromVector(ctx, ses, vec, i, row, rowIndex)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, ses FeSession, vec *vector.Vector, i int, row []any, rowIndex int) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(copyBytes(vec.GetBytesAt(rowIndex), true))
	case types.T_bool:
		row[i] = vector.GetFixedAt[bool](vec, rowIndex)
	case types.T_bit:
		row[i] = vector.GetFixedAt[uint64](vec, rowIndex)
	case types.T_int8:
		row[i] = vector.GetFixedAt[int8](vec, rowIndex)
	case types.T_uint8:
		row[i] = vector.GetFixedAt[uint8](vec, rowIndex)
	case types.T_int16:
		row[i] = vector.GetFixedAt[int16](vec, rowIndex)
	case types.T_uint16:
		row[i] = vector.GetFixedAt[uint16](vec, rowIndex)
	case types.T_int32:
		row[i] = vector.GetFixedAt[int32](vec, rowIndex)
	case types.T_uint32:
		row[i] = vector.GetFixedAt[uint32](vec, rowIndex)
	case types.T_int64:
		row[i] = vector.GetFixedAt[int64](vec, rowIndex)
	case types.T_uint64:
		row[i] = vector.GetFixedAt[uint64](vec, rowIndex)
	case types.T_float32:
		row[i] = vector.GetFixedAt[float32](vec, rowIndex)
	case types.T_float64:
		row[i] = vector.GetFixedAt[float64](vec, rowIndex)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex), true)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		row[i] = vector.GetArrayAt[float32](vec, rowIndex)
	case types.T_array_float64:
		row[i] = vector.GetArrayAt[float64](vec, rowIndex)
	case types.T_date:
		row[i] = vector.GetFixedAt[types.Date](vec, rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Datetime](vec, rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Time](vec, rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = vector.GetFixedAt[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAt[types.Uuid](vec, rowIndex).String()
	case types.T_Rowid:
		row[i] = vector.GetFixedAt[types.Rowid](vec, rowIndex)
	case types.T_Blockid:
		row[i] = vector.GetFixedAt[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAt[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = vector.GetFixedAt[types.Enum](vec, rowIndex)
	default:
		ses.Error(ctx,
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalError(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}
