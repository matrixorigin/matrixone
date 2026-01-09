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
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// extractRowFromEveryVector gets the j row from the every vector and outputs the row.
// !!!NOTE!!! use safeRefSlice before you know what you are doing.
// safeRefSlice is used to determine whether to copy the slice or not.
// types.T_json, types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink are
// stored as bytes slice.
func extractRowFromEveryVector(ctx context.Context, ses FeSession, dataSet *batch.Batch, j int, row []any, safeRefSlice bool) error {
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

		err := extractRowFromVector(ctx, ses, vec, i, row, rowIndex, safeRefSlice)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ctx context.Context, ses FeSession, vec *vector.Vector, i int, row []any, rowIndex int, safeRefSlice bool) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(commonutil.CloneBytesIf(vec.GetBytesAt(rowIndex), !safeRefSlice))
	case types.T_bool:
		row[i] = vector.GetFixedAtNoTypeCheck[bool](vec, rowIndex)
	case types.T_bit:
		row[i] = vector.GetFixedAtNoTypeCheck[uint64](vec, rowIndex)
	case types.T_int8:
		row[i] = vector.GetFixedAtNoTypeCheck[int8](vec, rowIndex)
	case types.T_uint8:
		row[i] = vector.GetFixedAtNoTypeCheck[uint8](vec, rowIndex)
	case types.T_int16:
		row[i] = vector.GetFixedAtNoTypeCheck[int16](vec, rowIndex)
	case types.T_uint16:
		row[i] = vector.GetFixedAtNoTypeCheck[uint16](vec, rowIndex)
	case types.T_int32:
		row[i] = vector.GetFixedAtNoTypeCheck[int32](vec, rowIndex)
	case types.T_uint32:
		row[i] = vector.GetFixedAtNoTypeCheck[uint32](vec, rowIndex)
	case types.T_int64:
		row[i] = vector.GetFixedAtNoTypeCheck[int64](vec, rowIndex)
	case types.T_uint64:
		row[i] = vector.GetFixedAtNoTypeCheck[uint64](vec, rowIndex)
	case types.T_float32:
		row[i] = vector.GetFixedAtNoTypeCheck[float32](vec, rowIndex)
	case types.T_float64:
		row[i] = vector.GetFixedAtNoTypeCheck[float64](vec, rowIndex)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		row[i] = commonutil.CloneBytesIf(vec.GetBytesAt(rowIndex), !safeRefSlice)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		arr := vector.GetArrayAt[float32](vec, rowIndex)
		if safeRefSlice {
			row[i] = arr
		} else {
			row[i] = append([]float32(nil), arr...)
		}
	case types.T_array_float64:
		arr := vector.GetArrayAt[float64](vec, rowIndex)
		if safeRefSlice {
			row[i] = arr
		} else {
			row[i] = append([]float64(nil), arr...)
		}
	case types.T_date:
		row[i] = vector.GetFixedAtNoTypeCheck[types.Date](vec, rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtNoTypeCheck[types.Datetime](vec, rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtNoTypeCheck[types.Time](vec, rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_year:
		row[i] = vector.GetFixedAtNoTypeCheck[types.MoYear](vec, rowIndex)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAtNoTypeCheck[types.Uuid](vec, rowIndex).String()
	case types.T_Rowid:
		row[i] = vector.GetFixedAtNoTypeCheck[types.Rowid](vec, rowIndex)
	case types.T_Blockid:
		row[i] = vector.GetFixedAtNoTypeCheck[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAtNoTypeCheck[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = vector.GetFixedAtNoTypeCheck[types.Enum](vec, rowIndex)
	default:
		ses.Error(ctx,
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

func extractRowFromEveryVector2(ctx context.Context, ses FeSession, dataSet *batch.Batch, j int, row []any, safeRefSlice bool, colSlices *ColumnSlices) error {
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

		err := extractRowFromVector2(ctx, ses, vec, i, row, rowIndex, safeRefSlice, colSlices)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

func extractRowFromVector2(ctx context.Context, ses FeSession, vec *vector.Vector, i int, row []any, rowIndex int, safeRefSlice bool, colSlices *ColumnSlices) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	sliceIdx := colSlices.GetSliceIdx(uint64(i))
	if vec.IsConst() {
		rowIndex = 0
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(commonutil.CloneBytesIf(vec.GetBytesAt2(colSlices.arrVarlena[sliceIdx], rowIndex), !safeRefSlice))
	case types.T_bool:
		row[i] = colSlices.arrBool[sliceIdx][rowIndex]
	case types.T_bit:
		row[i] = colSlices.arrUint64[sliceIdx][rowIndex]
	case types.T_int8:
		row[i] = colSlices.arrInt8[sliceIdx][rowIndex]
	case types.T_uint8:
		row[i] = colSlices.arrUint8[sliceIdx][rowIndex]
	case types.T_int16:
		row[i] = colSlices.arrInt16[sliceIdx][rowIndex]
	case types.T_year:
		row[i] = types.MoYear(colSlices.arrInt16[sliceIdx][rowIndex])
	case types.T_uint16:
		row[i] = colSlices.arrUint16[sliceIdx][rowIndex]
	case types.T_int32:
		row[i] = colSlices.arrInt32[sliceIdx][rowIndex]
	case types.T_uint32:
		row[i] = colSlices.arrUint32[sliceIdx][rowIndex]
	case types.T_int64:
		row[i] = colSlices.arrInt64[sliceIdx][rowIndex]
	case types.T_uint64:
		row[i] = colSlices.arrUint64[sliceIdx][rowIndex]
	case types.T_float32:
		row[i] = colSlices.arrFloat32[sliceIdx][rowIndex]
	case types.T_float64:
		row[i] = colSlices.arrFloat64[sliceIdx][rowIndex]
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		row[i] = commonutil.CloneBytesIf(vec.GetBytesAt2(colSlices.arrVarlena[sliceIdx], rowIndex), !safeRefSlice)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		arr := vector.GetArrayAt2[float32](vec, colSlices.arrVarlena[sliceIdx], rowIndex)
		if safeRefSlice {
			row[i] = arr
		} else {
			row[i] = append([]float32(nil), arr...)
		}
	case types.T_array_float64:
		arr := vector.GetArrayAt2[float64](vec, colSlices.arrVarlena[sliceIdx], rowIndex)
		if safeRefSlice {
			row[i] = arr
		} else {
			row[i] = append([]float64(nil), arr...)
		}
	case types.T_date:
		row[i] = colSlices.arrDate[sliceIdx][rowIndex]
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = colSlices.arrDatetime[sliceIdx][rowIndex].String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = colSlices.arrTime[sliceIdx][rowIndex].String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = colSlices.arrTimestamp[sliceIdx][rowIndex].String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = colSlices.arrDecimal64[sliceIdx][rowIndex].Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = colSlices.arrDecimal128[sliceIdx][rowIndex].Format(scale)
	case types.T_uuid:
		row[i] = colSlices.arrUuid[sliceIdx][rowIndex].String()
	case types.T_Rowid:
		row[i] = colSlices.arrRowid[sliceIdx][rowIndex]
	case types.T_Blockid:
		row[i] = colSlices.arrBlockid[sliceIdx][rowIndex]
	case types.T_TS:
		row[i] = colSlices.arrTS[sliceIdx][rowIndex]
	case types.T_enum:
		row[i] = colSlices.arrEnum[sliceIdx][rowIndex]
	default:
		ses.Error(ctx,
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector2 : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

type ColumnSlices struct {
	ctx             context.Context
	dataSet         *batch.Batch
	colIdx2SliceIdx []int
	arrVarlena      [][]types.Varlena
	arrBool         [][]bool
	arrInt8         [][]int8
	arrInt16        [][]int16
	arrInt32        [][]int32
	arrInt64        [][]int64
	arrUint8        [][]uint8
	arrUint16       [][]uint16
	arrUint32       [][]uint32
	arrUint64       [][]uint64
	arrFloat32      [][]float32
	arrFloat64      [][]float64
	arrDate         [][]types.Date
	arrDatetime     [][]types.Datetime
	arrTime         [][]types.Time
	arrTimestamp    [][]types.Timestamp
	arrDecimal64    [][]types.Decimal64
	arrDecimal128   [][]types.Decimal128
	arrUuid         [][]types.Uuid
	arrRowid        [][]types.Rowid
	arrBlockid      [][]types.Blockid
	arrTS           [][]types.TS
	arrEnum         [][]types.Enum
	safeRefSlice    bool
}

func (slices *ColumnSlices) Close() {
	if slices == nil {
		return
	}
	slices.ctx = nil
	slices.dataSet = nil
	slices.colIdx2SliceIdx = nil
	slices.arrVarlena = nil
	slices.arrBool = nil
	slices.arrInt8 = nil
	slices.arrInt16 = nil
	slices.arrInt32 = nil
	slices.arrInt64 = nil
	slices.arrUint8 = nil
	slices.arrUint16 = nil
	slices.arrUint32 = nil
	slices.arrUint64 = nil
	slices.arrFloat32 = nil
	slices.arrFloat64 = nil
	slices.arrDate = nil
	slices.arrDatetime = nil
	slices.arrTime = nil
	slices.arrTimestamp = nil
	slices.arrDecimal64 = nil
	slices.arrDecimal128 = nil
	slices.arrUuid = nil
	slices.arrRowid = nil
	slices.arrBlockid = nil
	slices.arrTS = nil
	slices.arrEnum = nil
	slices.safeRefSlice = false
}

func (slices *ColumnSlices) GetType(colIdx uint64) *types.Type {
	return slices.dataSet.Vecs[colIdx].GetType()
}

func (slices *ColumnSlices) GetVector(colIdx uint64) *vector.Vector {
	return slices.dataSet.Vecs[colIdx]
}

func (slices *ColumnSlices) ColumnCount() int {
	return len(slices.dataSet.Vecs)
}

func (slices *ColumnSlices) GetSliceIdx(colIdx uint64) int {
	return slices.colIdx2SliceIdx[colIdx]
}

var IsNull = func(slices *ColumnSlices, rowIdx int, colIdx int) bool {
	return slices.IsNull(rowIdx, colIdx)
}

func (slices *ColumnSlices) IsNull(rowIdx int, colIdx int) bool {
	vec := slices.dataSet.Vecs[colIdx]
	return vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIdx))
}

func (slices *ColumnSlices) IsConst(colIdx uint64) bool {
	return slices.dataSet.Vecs[colIdx].IsConst()
}

var GetBool = func(slices *ColumnSlices, rowIdx uint64, colIdx uint64) (bool, error) {
	return slices.GetBool(rowIdx, colIdx)
}

func (slices *ColumnSlices) GetBool(rowIdx uint64, colIdx uint64) (bool, error) {
	if slices.IsConst(colIdx) {
		rowIdx = 0
	}
	sliceIdx := slices.GetSliceIdx(colIdx)
	typ := slices.GetType(colIdx)
	switch typ.Oid {
	case types.T_bool:
		return slices.arrBool[sliceIdx][rowIdx], nil
	default:
		return false, moerr.NewInternalError(slices.ctx, "invalid bool slice")
	}
}

var GetUint64 = func(slices *ColumnSlices, r uint64, i uint64) (uint64, error) {
	return slices.GetUint64(r, i)
}

func (slices *ColumnSlices) GetUint64(r uint64, i uint64) (uint64, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	typ := slices.GetType(i)
	switch typ.Oid {
	case types.T_int8:
		return uint64(slices.arrInt8[sliceIdx][r]), nil
	case types.T_uint8:
		return uint64(slices.arrUint8[sliceIdx][r]), nil
	case types.T_int16, types.T_year:
		return uint64(slices.arrInt16[sliceIdx][r]), nil
	case types.T_uint16:
		return uint64(slices.arrUint16[sliceIdx][r]), nil
	case types.T_int32:
		return uint64(slices.arrInt32[sliceIdx][r]), nil
	case types.T_uint32:
		return uint64(slices.arrUint32[sliceIdx][r]), nil
	case types.T_int64:
		return uint64(slices.arrInt64[sliceIdx][r]), nil
	case types.T_uint64, types.T_bit:
		return slices.arrUint64[sliceIdx][r], nil
	default:
		return 0, moerr.NewInternalError(slices.ctx, "invalid uint64 slice")
	}
}

var GetInt64 = func(slices *ColumnSlices, r uint64, i uint64) (int64, error) {
	return slices.GetInt64(r, i)
}

func (slices *ColumnSlices) GetInt64(r uint64, i uint64) (int64, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	typ := slices.GetType(i)
	switch typ.Oid {
	case types.T_int8:
		return int64(slices.arrInt8[sliceIdx][r]), nil
	case types.T_uint8:
		return int64(slices.arrUint8[sliceIdx][r]), nil
	case types.T_int16, types.T_year:
		return int64(slices.arrInt16[sliceIdx][r]), nil
	case types.T_uint16:
		return int64(slices.arrUint16[sliceIdx][r]), nil
	case types.T_int32:
		return int64(slices.arrInt32[sliceIdx][r]), nil
	case types.T_uint32:
		return int64(slices.arrUint32[sliceIdx][r]), nil
	case types.T_int64:
		return slices.arrInt64[sliceIdx][r], nil
	case types.T_uint64, types.T_bit:
		return int64(slices.arrUint64[sliceIdx][r]), nil
	default:
		return 0, moerr.NewInternalError(slices.ctx, "invalid int64 slice")
	}
}

var GetDecimal = func(slices *ColumnSlices, r uint64, i uint64) (string, error) {
	return slices.GetDecimal(r, i)
}

func (slices *ColumnSlices) GetDecimal(r uint64, i uint64) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	vec := slices.GetVector(i)
	switch vec.GetType().Oid {
	case types.T_decimal64:
		scale := vec.GetType().Scale
		return slices.arrDecimal64[sliceIdx][r].Format(scale), nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		return slices.arrDecimal128[sliceIdx][r].Format(scale), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid decimal slice")
	}
}

var GetUUID = func(slices *ColumnSlices, r uint64, i uint64) (string, error) {
	return slices.GetUUID(r, i)
}

func (slices *ColumnSlices) GetUUID(r uint64, i uint64) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	typ := slices.GetType(i)
	switch typ.Oid {
	case types.T_uuid:
		return slices.arrUuid[sliceIdx][r].String(), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid uuid slice")
	}
}

var GetFloat32 = func(slices *ColumnSlices, r uint64, i uint64) (float32, error) {
	return slices.GetFloat32(r, i)
}

func (slices *ColumnSlices) GetFloat32(r uint64, i uint64) (float32, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	typ := slices.GetType(i)
	switch typ.Oid {
	case types.T_float32:
		return slices.arrFloat32[sliceIdx][r], nil
	case types.T_float64:
		return float32(slices.arrFloat64[sliceIdx][r]), nil
	default:
		return 0, moerr.NewInternalError(slices.ctx, "invalid float32 slice")
	}
}

var GetFloat64 = func(slices *ColumnSlices, r uint64, i uint64) (float64, error) {
	return slices.GetFloat64(r, i)
}

func (slices *ColumnSlices) GetFloat64(r uint64, i uint64) (float64, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	typ := slices.GetType(i)
	switch typ.Oid {
	case types.T_float32:
		return float64(slices.arrFloat32[sliceIdx][r]), nil
	case types.T_float64:
		return slices.arrFloat64[sliceIdx][r], nil
	default:
		return 0, moerr.NewInternalError(slices.ctx, "invalid float64 slice")
	}
}

var GetStringBased = func(slices *ColumnSlices, r uint64, i uint64) (string, error) {
	return slices.GetStringBased(r, i)
}

func (slices *ColumnSlices) GetStringBased(r uint64, i uint64) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	vec := slices.GetVector(i)
	switch vec.GetType().Oid { //get col
	case types.T_json:
		return types.DecodeJson(commonutil.CloneBytesIf(vec.GetBytesAt2(slices.arrVarlena[sliceIdx], int(r)), !slices.safeRefSlice)).String(), nil
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		return types.ArrayToString[float32](vector.GetArrayAt2[float32](vec, slices.arrVarlena[sliceIdx], int(r))), nil
	case types.T_array_float64:
		return types.ArrayToString[float64](vector.GetArrayAt2[float64](vec, slices.arrVarlena[sliceIdx], int(r))), nil
	case types.T_Rowid:
		return slices.arrRowid[sliceIdx][r].String(), nil
	case types.T_Blockid:
		return slices.arrBlockid[sliceIdx][r].String(), nil
	case types.T_TS:
		return slices.arrTS[sliceIdx][r].ToString(), nil
	case types.T_enum:
		return strconv.FormatUint(uint64(slices.arrEnum[sliceIdx][r]), 10), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid string based slice")
	}
}

var GetBytesBased = func(slices *ColumnSlices, r uint64, i uint64) ([]byte, error) {
	return slices.GetBytesBased(r, i)
}

func (slices *ColumnSlices) GetBytesBased(r uint64, i uint64) ([]byte, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	vec := slices.dataSet.Vecs[i]
	switch vec.GetType().Oid { //get col
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return commonutil.CloneBytesIf(vec.GetBytesAt2(slices.arrVarlena[sliceIdx], int(r)), !slices.safeRefSlice), nil
	default:
		return nil, moerr.NewInternalError(slices.ctx, "invalid bytes based slice")
	}
}

var GetDate = func(slices *ColumnSlices, r uint64, i uint64) (types.Date, error) {
	return slices.GetDate(r, i)
}

func (slices *ColumnSlices) GetDate(r uint64, i uint64) (types.Date, error) {
	if slices.IsConst(i) {
		r = 0
	}
	typ := slices.GetType(i)

	// Note: mysql_protocol.go handles the case where MySQL column type is MYSQL_TYPE_DATE
	// but actual vector type is T_datetime (e.g., TIMESTAMPADD with DATE input + time unit).
	// In that case, it calls GetDatetime instead of GetDate.
	// So GetDate only needs to handle T_date and T_datetime vector types.
	sliceIdx := slices.GetSliceIdx(i)
	switch typ.Oid {
	case types.T_date:
		return slices.arrDate[sliceIdx][r], nil
	case types.T_datetime:
		// Handle DATETIME type when MySQL column type is MYSQL_TYPE_DATE
		// This can happen when TIMESTAMPADD returns DATETIME but caller expects DATE
		dt := slices.arrDatetime[sliceIdx][r]
		return dt.ToDate(), nil
	default:
		var d types.Date
		return d, moerr.NewInternalError(slices.ctx, "invalid date slice")
	}
}

var GetDatetime = func(slices *ColumnSlices, r uint64, i uint64) (string, error) {
	return slices.GetDatetime(r, i)
}

func (slices *ColumnSlices) GetDatetime(r uint64, i uint64) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	vec := slices.dataSet.Vecs[i]
	actualType := vec.GetType().Oid

	// Note: mysql_protocol.go handles the case where MySQL column type is MYSQL_TYPE_DATE
	// but actual vector type is T_datetime. It calls GetDatetime directly.
	// So GetDatetime only needs to handle T_datetime vector type.
	sliceIdx := slices.GetSliceIdx(i)
	switch actualType {
	case types.T_datetime:
		scale := vec.GetType().Scale
		dt := slices.arrDatetime[sliceIdx][r]
		// If fractional seconds are 0, format without fractional part (MySQL behavior)
		if scale > 0 && dt.MicroSec() == 0 {
			return dt.String2(0), nil
		}
		return dt.String2(scale), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid datetime slice")
	}
}

var GetTime = func(slices *ColumnSlices, r uint64, i uint64) (string, error) {
	return slices.GetTime(r, i)
}

func (slices *ColumnSlices) GetTime(r uint64, i uint64) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	vec := slices.dataSet.Vecs[i]
	switch vec.GetType().Oid {
	case types.T_time:
		scale := vec.GetType().Scale
		return slices.arrTime[sliceIdx][r].String2(scale), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid time slice")
	}
}

var GetTimestamp = func(slices *ColumnSlices, r uint64, i uint64, timeZone *time.Location) (string, error) {
	return slices.GetTimestamp(r, i, timeZone)
}

func (slices *ColumnSlices) GetTimestamp(r uint64, i uint64, timeZone *time.Location) (string, error) {
	if slices.IsConst(i) {
		r = 0
	}
	sliceIdx := slices.GetSliceIdx(i)
	vec := slices.dataSet.Vecs[i]
	switch vec.GetType().Oid {
	case types.T_timestamp:
		scale := vec.GetType().Scale
		return slices.arrTimestamp[sliceIdx][r].String2(timeZone, scale), nil
	default:
		return "", moerr.NewInternalError(slices.ctx, "invalid timestamp slice")
	}
}

func convertBatchToSlices(ctx context.Context, ses FeSession, dataSet *batch.Batch, colSlices *ColumnSlices) error {
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

func convertVectorToSlice(ctx context.Context, ses FeSession, vec *vector.Vector, i int, colSlices *ColumnSlices) error {
	if vec.IsConstNull() {
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrVarlena)
		colSlices.arrVarlena = append(colSlices.arrVarlena, vector.ToSliceNoTypeCheck2[types.Varlena](vec))
	case types.T_bool:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrBool)
		colSlices.arrBool = append(colSlices.arrBool, vector.ToSliceNoTypeCheck2[bool](vec))
	case types.T_bit:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUint64)
		colSlices.arrUint64 = append(colSlices.arrUint64, vector.ToSliceNoTypeCheck2[uint64](vec))
	case types.T_int8:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrInt8)
		colSlices.arrInt8 = append(colSlices.arrInt8, vector.ToSliceNoTypeCheck2[int8](vec))
	case types.T_uint8:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUint8)
		colSlices.arrUint8 = append(colSlices.arrUint8, vector.ToSliceNoTypeCheck2[uint8](vec))
	case types.T_int16:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrInt16)
		colSlices.arrInt16 = append(colSlices.arrInt16, vector.ToSliceNoTypeCheck2[int16](vec))
	case types.T_year:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrInt16)
		colSlices.arrInt16 = append(colSlices.arrInt16, vector.ToSliceNoTypeCheck2[int16](vec))
	case types.T_uint16:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUint16)
		colSlices.arrUint16 = append(colSlices.arrUint16, vector.ToSliceNoTypeCheck2[uint16](vec))
	case types.T_int32:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrInt32)
		colSlices.arrInt32 = append(colSlices.arrInt32, vector.ToSliceNoTypeCheck2[int32](vec))
	case types.T_uint32:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUint32)
		colSlices.arrUint32 = append(colSlices.arrUint32, vector.ToSliceNoTypeCheck2[uint32](vec))
	case types.T_int64:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrInt64)
		colSlices.arrInt64 = append(colSlices.arrInt64, vector.ToSliceNoTypeCheck2[int64](vec))
	case types.T_uint64:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUint64)
		colSlices.arrUint64 = append(colSlices.arrUint64, vector.ToSliceNoTypeCheck2[uint64](vec))
	case types.T_float32:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrFloat32)
		colSlices.arrFloat32 = append(colSlices.arrFloat32, vector.ToSliceNoTypeCheck2[float32](vec))
	case types.T_float64:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrFloat64)
		colSlices.arrFloat64 = append(colSlices.arrFloat64, vector.ToSliceNoTypeCheck2[float64](vec))
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrVarlena)
		colSlices.arrVarlena = append(colSlices.arrVarlena, vector.ToSliceNoTypeCheck2[types.Varlena](vec))
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrVarlena)
		colSlices.arrVarlena = append(colSlices.arrVarlena, vector.ToSliceNoTypeCheck2[types.Varlena](vec))
	case types.T_array_float64:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrVarlena)
		colSlices.arrVarlena = append(colSlices.arrVarlena, vector.ToSliceNoTypeCheck2[types.Varlena](vec))
	case types.T_date:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrDate)
		colSlices.arrDate = append(colSlices.arrDate, vector.ToSliceNoTypeCheck2[types.Date](vec))
	case types.T_datetime:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrDatetime)
		colSlices.arrDatetime = append(colSlices.arrDatetime, vector.ToSliceNoTypeCheck2[types.Datetime](vec))
	case types.T_time:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrTime)
		colSlices.arrTime = append(colSlices.arrTime, vector.ToSliceNoTypeCheck2[types.Time](vec))
	case types.T_timestamp:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrTimestamp)
		colSlices.arrTimestamp = append(colSlices.arrTimestamp, vector.ToSliceNoTypeCheck2[types.Timestamp](vec))
	case types.T_decimal64:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrDecimal64)
		colSlices.arrDecimal64 = append(colSlices.arrDecimal64, vector.ToSliceNoTypeCheck2[types.Decimal64](vec))
	case types.T_decimal128:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrDecimal128)
		colSlices.arrDecimal128 = append(colSlices.arrDecimal128, vector.ToSliceNoTypeCheck2[types.Decimal128](vec))
	case types.T_uuid:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrUuid)
		colSlices.arrUuid = append(colSlices.arrUuid, vector.ToSliceNoTypeCheck2[types.Uuid](vec))
	case types.T_Rowid:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrRowid)
		colSlices.arrRowid = append(colSlices.arrRowid, vector.ToSliceNoTypeCheck2[types.Rowid](vec))
	case types.T_Blockid:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrBlockid)
		colSlices.arrBlockid = append(colSlices.arrBlockid, vector.ToSliceNoTypeCheck2[types.Blockid](vec))
	case types.T_TS:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrTS)
		colSlices.arrTS = append(colSlices.arrTS, vector.ToSliceNoTypeCheck2[types.TS](vec))
	case types.T_enum:
		colSlices.colIdx2SliceIdx[i] = len(colSlices.arrEnum)
		colSlices.arrEnum = append(colSlices.arrEnum, vector.ToSliceNoTypeCheck2[types.Enum](vec))
	default:
		ses.Error(ctx,
			"Failed to convert vector to slice, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "convertVectorToSlice : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}
