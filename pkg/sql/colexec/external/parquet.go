// Copyright 2024 Matrix Origin
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

package external

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

func scanParquetFile(ctx context.Context, param *ExternalParam, proc *process.Process, bat *batch.Batch) error {
	_, span := trace.Start(ctx, "scanParquetFile")
	defer span.End()

	if param.parqh == nil {
		var err error
		if err = param.refreshPartitionValues(); err != nil {
			return err
		}
		param.parqh, err = newParquetHandler(param)
		if err != nil {
			return err
		}
	}

	o := param.parqh.offset
	for _, g := range param.parqh.file.RowGroups() {
		n := g.NumRows()
		if o >= n {
			o -= n
			continue
		}
		param.parqh.batchCnt = min(n-o, maxParquetBatchCnt)
	}

	h := param.parqh
	if err := h.getData(bat, param, proc); err != nil {
		return err
	}
	if bat.RowCount() > 0 && (h.filepathColIndex >= 0 || len(h.partitionColIndices) > 0) {
		if err := h.fillVirtualColumns(bat, param, proc); err != nil {
			return err
		}
	}
	if h.rowCountOnly && h.file != nil && h.offset >= h.file.NumRows() {
		param.parqh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
	}
	return nil
}

var maxParquetBatchCnt int64 = 1000

func newParquetHandler(param *ExternalParam) (*ParquetHandler, error) {
	h := ParquetHandler{
		batchCnt:         maxParquetBatchCnt,
		filepathColIndex: -1,
	}
	err := h.openFile(param)
	if err != nil {
		return nil, err
	}

	err = h.prepare(param)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func (h *ParquetHandler) openFile(param *ExternalParam) error {
	var r io.ReaderAt
	switch {
	case param.Extern.ScanType == tree.INLINE:
		r = bytes.NewReader(util.UnsafeStringToBytes(param.Extern.Data))
	case param.Extern.Local:
		return moerr.NewNYI(param.Ctx, "load parquet local")
	default:
		fs, readPath, err := plan.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
		if err != nil {
			return err
		}
		r = &fsReaderAt{
			fs:       fs,
			readPath: readPath,
			ctx:      param.Ctx,
		}
	}
	var err error
	h.file, err = parquet.OpenFile(r, param.FileSize[param.Fileparam.FileIndex-1])
	return moerr.ConvertGoError(param.Ctx, err)
}

// findColumnIgnoreCase finds a column in the Parquet schema with case-insensitive matching.
func (h *ParquetHandler) findColumnIgnoreCase(ctx context.Context, name string) (*parquet.Column, error) {
	root := h.file.Root()
	nameLower := strings.ToLower(name)

	var exactMatch *parquet.Column
	var caseInsensitiveMatches []*parquet.Column
	for _, col := range root.Columns() {
		if col.Name() == name {
			exactMatch = col
			caseInsensitiveMatches = append(caseInsensitiveMatches, col)
		} else if strings.ToLower(col.Name()) == nameLower {
			caseInsensitiveMatches = append(caseInsensitiveMatches, col)
		}
	}
	if len(caseInsensitiveMatches) > 1 {
		return nil, moerr.NewInvalidInputf(ctx,
			"ambiguous column name %s: multiple columns match case-insensitively (%s and %s)",
			name, caseInsensitiveMatches[0].Name(), caseInsensitiveMatches[1].Name())
	}
	if exactMatch != nil {
		return exactMatch, nil
	}
	if len(caseInsensitiveMatches) == 1 {
		return caseInsensitiveMatches[0], nil
	}
	return nil, nil
}

func (h *ParquetHandler) prepare(param *ExternalParam) error {
	h.cols = make([]*parquet.Column, len(param.Cols))
	h.mappers = make([]*columnMapper, len(param.Cols))
	for _, attr := range param.Attrs {
		colIdx := int(attr.ColIndex)
		if colIdx < 0 || colIdx >= len(param.Cols) {
			return moerr.NewInvalidInputf(param.Ctx, "column index %d out of range %d", attr.ColIndex, len(param.Cols))
		}
		def := param.Cols[colIdx]
		if def.Hidden {
			continue
		}

		if param.isHivePartitionCol(attr.ColName) {
			h.partitionColIndices = append(h.partitionColIndices, colIdx)
			continue
		}
		if catalog.ContainExternalHidenCol(attr.ColName) {
			h.filepathColIndex = colIdx
			continue
		}

		h.hasPhysicalCol = true

		col, err := h.findColumnIgnoreCase(param.Ctx, attr.ColName)
		if err != nil {
			return err
		}
		if col == nil {
			return moerr.NewInvalidInputf(param.Ctx, "column %s not found", attr.ColName)
		}
		if !col.Leaf() {
			return moerr.NewNYI(param.Ctx, "parquet nested type")
		}

		fn := h.getMapper(col, def.Typ)
		if fn == nil {
			st := col.Type().String()
			if col.Optional() {
				st += "(optional)"
			} else {
				st += "(required)"
			}
			dt := types.T(def.Typ.Id).String()
			if def.NotNull {
				dt += " NOT NULL"
			} else {
				dt += " NULL"
			}
			return moerr.NewNYIf(param.Ctx, "load %s to %s", st, dt)
		}
		h.cols[colIdx] = col
		h.mappers[colIdx] = fn
	}

	if !h.hasPhysicalCol && (len(h.partitionColIndices) > 0 || h.filepathColIndex >= 0) {
		h.rowCountOnly = true
	}

	return nil
}

func (*ParquetHandler) getMapper(sc *parquet.Column, dt plan.Type) *columnMapper {
	st := sc.Type()
	if st.PhysicalType() == nil {
		return nil
	}
	if sc.Optional() && dt.NotNullable {
		return nil
	}

	mp := &columnMapper{
		srcNull:            sc.Optional(),
		dstNull:            !dt.NotNullable,
		maxDefinitionLevel: byte(sc.MaxDefinitionLevel()),
	}
	switch types.T(dt.Id) {
	case types.T_bool:
		if st.Kind() != parquet.Boolean {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			if page.Dictionary() != nil {
				return moerr.NewNYIf(proc.Ctx, "indexed %s page", st)
			}
			p := make([]parquet.Value, page.NumValues())
			n, err := page.Values().ReadValues(p)
			if err != nil && !errors.Is(err, io.EOF) {
				return moerr.ConvertGoError(proc.Ctx, err)
			}
			if n != int(page.NumValues()) {
				return moerr.NewInternalError(proc.Ctx, "short read bool")
			}
			for _, v := range p {
				if v.IsNull() {
					err = vector.AppendFixed(vec, false, true, proc.Mp())
				} else {
					err = vector.AppendFixed(vec, v.Boolean(), false, proc.Mp())
				}
				if err != nil {
					return err
				}
			}
			return nil
		}
	case types.T_uint8:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint8(0), func(v parquet.Value) (uint8, error) {
				if st.Kind() == parquet.Int32 {
					return uint8(v.Int32()), nil
				}
				val, err := parquetValueToUint64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val > 255 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows TINYINT UNSIGNED", val)
				}
				return uint8(val), nil
			})
		}
	case types.T_int8:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int8(0), func(v parquet.Value) (int8, error) {
				if st.Kind() == parquet.Int32 {
					return int8(v.Int32()), nil
				}
				val, err := parquetValueToInt64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val < -128 || val > 127 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows TINYINT", val)
				}
				return int8(val), nil
			})
		}
	case types.T_int16:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int16(0), func(v parquet.Value) (int16, error) {
				if st.Kind() == parquet.Int32 {
					return int16(v.Int32()), nil
				}
				val, err := parquetValueToInt64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val < -32768 || val > 32767 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows SMALLINT", val)
				}
				return int16(val), nil
			})
		}
	case types.T_uint16:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint16(0), func(v parquet.Value) (uint16, error) {
				if st.Kind() == parquet.Int32 {
					return uint16(v.Int32()), nil
				}
				val, err := parquetValueToUint64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val > 65535 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows SMALLINT UNSIGNED", val)
				}
				return uint16(val), nil
			})
		}
	case types.T_int32:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int32(0), func(v parquet.Value) (int32, error) {
				if st.Kind() == parquet.Int32 {
					return v.Int32(), nil
				}
				val, err := parquetValueToInt64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val < -2147483648 || val > 2147483647 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows INT", val)
				}
				return int32(val), nil
			})
		}
	case types.T_int64:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int64(0), func(v parquet.Value) (int64, error) {
				return parquetValueToInt64(proc.Ctx, st, v)
			})
		}
	case types.T_uint32:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint32(0), func(v parquet.Value) (uint32, error) {
				if st.Kind() == parquet.Int32 {
					return v.Uint32(), nil
				}
				val, err := parquetValueToUint64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val > 4294967295 {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows INT UNSIGNED", val)
				}
				return uint32(val), nil
			})
		}
	case types.T_uint64:
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint64(0), func(v parquet.Value) (uint64, error) {
				return parquetValueToUint64(proc.Ctx, st, v)
			})
		}
	case types.T_float32:
		if !isParquetFloatConvertibleSource(st) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, float32(0), func(v parquet.Value) (float32, error) {
				val, err := parquetValueToFloat64(proc.Ctx, st, v)
				return float32(val), err
			})
		}
	case types.T_float64:
		if !isParquetFloatConvertibleSource(st) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, float64(0), func(v parquet.Value) (float64, error) {
				return parquetValueToFloat64(proc.Ctx, st, v)
			})
		}
	case types.T_date:
		lt := st.LogicalType()
		if lt == nil {
			break
		}
		dateT := lt.Date
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
		if dateT == nil {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			if dict := page.Dictionary(); dict != nil {
				dictData := dict.Page().Data()
				bs, _ := dictData.Data()
				dictDates := types.DecodeSlice[int32](bs)
				indices := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictDates), indices, func(idx int32) types.Date {
					return types.DaysFromUnixEpochToDate(dictDates[int(idx)])
				})
			}
			bs, _ := data.Data()
			ls := types.DecodeSlice[int32](bs)
			return copyPageToVecMap(mp, page, proc, vec, ls, func(t int32) types.Date {
				return types.DaysFromUnixEpochToDate(t)
			})
		}
	case types.T_timestamp:
		lt := st.LogicalType()
		if lt == nil {
			break
		}
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
		tsT := lt.Timestamp
		if tsT == nil || !tsT.IsAdjustedToUTC {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			switch {
			case tsT.Unit.Nanos != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Timestamp, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.UnixNanoToTimestamp(v)
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
					return types.UnixNanoToTimestamp(v)
				})
			case tsT.Unit.Micros != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Timestamp, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.UnixMicroToTimestamp(v)
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
					return types.UnixMicroToTimestamp(v)
				})
			case tsT.Unit.Millis != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Timestamp, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.UnixMicroToTimestamp(v * 1000)
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
					return types.UnixMicroToTimestamp(v * 1000)
				})
			default:
				return moerr.NewInternalError(proc.Ctx, "unknown unit")
			}
		}
	case types.T_datetime:
		lt := st.LogicalType()
		if lt == nil {
			break
		}
		dtT := lt.Timestamp
		if dtT == nil {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			switch {
			case dtT.Unit.Nanos != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Datetime, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.Datetime(types.UnixNanoToTimestamp(v))
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Datetime {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Datetime {
					return types.Datetime(types.UnixNanoToTimestamp(v))
				})
			case dtT.Unit.Micros != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Datetime, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.Datetime(types.UnixMicroToTimestamp(v))
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Datetime {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Datetime {
					return types.Datetime(types.UnixMicroToTimestamp(v))
				})
			case dtT.Unit.Millis != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Datetime, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.Datetime(types.UnixMicroToTimestamp(v * 1000))
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Datetime {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Datetime {
					return types.Datetime(types.UnixMicroToTimestamp(v * 1000))
				})
			default:
				return moerr.NewInternalError(proc.Ctx, "unknown unit")
			}
		}
	case types.T_time:
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time
		lt := st.LogicalType()
		if lt == nil {
			break
		}
		timeT := lt.Time
		if timeT == nil {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			switch {
			case timeT.Unit.Nanos != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int64()
					converted := make([]types.Time, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.Time(v / 1000)
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Time {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Time {
					return types.Time(v / 1000)
				})
			case timeT.Unit.Micros != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					bs, _ := dictData.Data()
					dictTimes := types.DecodeSlice[types.Time](bs)
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(dictTimes), indexes, func(idx int32) types.Time {
						return dictTimes[int(idx)]
					})
				}
				bs, _ := data.Data()
				return copyPageToVec(mp, page, proc, vec, types.DecodeSlice[types.Time](bs))
			case timeT.Unit.Millis != nil:
				if dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int32()
					converted := make([]types.Time, len(dictValues))
					for i, v := range dictValues {
						converted[i] = types.Time(v) * 1000
					}
					indexes := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Time {
						return converted[int(idx)]
					})
				}
				return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) types.Time {
					return types.Time(v) * 1000
				})
			default:
				return moerr.NewInternalError(proc.Ctx, "unknown unit")
			}
		}
	case types.T_decimal64:
		srcScale := parquetDecimalScale(st)
		dstScale := dt.Scale
		dstWidth := dt.Width
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(st.LogicalType()) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Decimal64, error) {
						return parseStringToDecimal64(util.UnsafeBytesToString(data), dstWidth, dstScale)
					},
					types.Decimal64(0),
				)
			}
		} else {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				kind := st.Kind()
				data := page.Data()
				if dict := page.Dictionary(); dict != nil {
					dictValues, err := decodeDecimal64Values(proc.Ctx, kind, dict.Page().Data())
					if err != nil {
						return err
					}
					if err := rescaleDecimal64Values(proc.Ctx, dictValues, srcScale, dstScale, dstWidth); err != nil {
						return err
					}
					indices := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) types.Decimal64 {
						return dictValues[int(idx)]
					})
				}
				values, err := decodeDecimal64Values(proc.Ctx, kind, data)
				if err != nil {
					return err
				}
				if err := rescaleDecimal64Values(proc.Ctx, values, srcScale, dstScale, dstWidth); err != nil {
					return err
				}
				return copyPageToVec(mp, page, proc, vec, values)
			}
		}
	case types.T_decimal128:
		srcScale := parquetDecimalScale(st)
		dstScale := dt.Scale
		dstWidth := dt.Width
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(st.LogicalType()) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Decimal128, error) {
						return parseStringToDecimal128(util.UnsafeBytesToString(data), dstWidth, dstScale)
					},
					types.Decimal128{},
				)
			}
		} else {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				kind := st.Kind()
				data := page.Data()
				if dict := page.Dictionary(); dict != nil {
					dictValues, err := decodeDecimal128Values(proc.Ctx, kind, dict.Page().Data())
					if err != nil {
						return err
					}
					if err := rescaleDecimal128Values(proc.Ctx, dictValues, srcScale, dstScale, dstWidth); err != nil {
						return err
					}
					indices := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) types.Decimal128 {
						return dictValues[int(idx)]
					})
				}
				values, err := decodeDecimal128Values(proc.Ctx, kind, data)
				if err != nil {
					return err
				}
				if err := rescaleDecimal128Values(proc.Ctx, values, srcScale, dstScale, dstWidth); err != nil {
					return err
				}
				return copyPageToVec(mp, page, proc, vec, values)
			}
		}
	case types.T_decimal256:
		srcScale := parquetDecimalScale(st)
		dstScale := dt.Scale
		dstWidth := dt.Width
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(st.LogicalType()) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Decimal256, error) {
						return parseStringToDecimal256(util.UnsafeBytesToString(data), dstWidth, dstScale)
					},
					types.Decimal256{},
				)
			}
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			kind := st.Kind()
			data := page.Data()
			if dict := page.Dictionary(); dict != nil {
				dictValues, err := decodeDecimal256Values(proc.Ctx, kind, dict.Page().Data())
				if err != nil {
					return err
				}
				if err := rescaleDecimal256Values(proc.Ctx, dictValues, srcScale, dstScale, dstWidth); err != nil {
					return err
				}
				indices := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) types.Decimal256 {
					return dictValues[int(idx)]
				})
			}
			values, err := decodeDecimal256Values(proc.Ctx, kind, data)
			if err != nil {
				return err
			}
			if err := rescaleDecimal256Values(proc.Ctx, values, srcScale, dstScale, dstWidth); err != nil {
				return err
			}
			return copyPageToVec(mp, page, proc, vec, values)
		}
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		if isParquetValueStringConvertibleSource(st) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processParquetValuesToBytes(proc.Ctx, mp, page, proc, vec, func(v parquet.Value) ([]byte, error) {
					val, err := parquetValueToString(proc.Ctx, st, v)
					if err != nil {
						return nil, err
					}
					return []byte(val), nil
				})
			}
			break
		}
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			err := vec.PreExtend(int(page.NumRows()), proc.Mp())
			if err != nil {
				return err
			}

			var loader strLoader
			var indices []int32
			var cache []*types.Varlena
			dict := page.Dictionary()
			if dict == nil {
				loader.init(page.Data())
			} else {
				loader.init(dict.Page().Data())
				data := page.Data()
				indices = data.Int32()
				cache = make([]*types.Varlena, int(dict.Len()))
			}
			for i := 0; i < int(page.NumRows()); i++ {
				isNull, err := mp.pageIsNull(proc.Ctx, page, i)
				if err != nil {
					return err
				}
				if isNull {
					err := vector.AppendBytes(vec, nil, true, proc.Mp())
					if err != nil {
						return err
					}
					continue
				}

				if dict == nil {
					data := loader.loadNext()
					err := vector.AppendBytes(vec, data, false, proc.Mp())
					if err != nil {
						return err
					}
					continue
				}

				idx := indices[loader.next]
				loader.next++
				if cache[idx] != nil {
					err := vector.AppendFixed(vec, *cache[idx], false, proc.Mp())
					if err != nil {
						return err
					}
					continue
				}

				data := loader.loadAt(idx)
				err = vector.AppendBytes(vec, data, false, proc.Mp())
				if err == nil {
					va := vector.GetFixedAtNoTypeCheck[types.Varlena](vec, vec.Length()-1)
					cache[idx] = &va
				} else {
					return err
				}
			}
			return nil
		}
	case types.T_json:
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToJson(proc.Ctx, mp, page, proc, vec)
		}
	}
	if mp.mapper != nil {
		return mp
	}
	return nil
}

type nullCheckInfo struct {
	noNulls            bool
	levels             []byte
	maxDefinitionLevel byte
	actualNonNulls     int64
}

func (nc nullCheckInfo) isNull(index int) bool {
	if nc.noNulls {
		return false
	}
	return nc.levels[index] != nc.maxDefinitionLevel
}

func prepareNullCheck(ctx context.Context, mp *columnMapper, page parquet.Page) (nullCheckInfo, error) {
	numRows := int(page.NumRows())
	if !mp.srcNull || page.NumNulls() == 0 {
		return nullCheckInfo{
			noNulls:        true,
			actualNonNulls: int64(numRows),
		}, nil
	}
	if !mp.dstNull {
		return nullCheckInfo{}, moerr.NewConstraintViolationf(ctx, "cannot load NULL value into NOT NULL column")
	}

	levels := page.DefinitionLevels()
	if len(levels) != numRows {
		return nullCheckInfo{}, moerr.NewInvalidInputf(ctx,
			"malformed page: definition levels length %d != numRows %d", len(levels), numRows)
	}

	var actualNonNulls int64
	for _, level := range levels {
		if level == mp.maxDefinitionLevel {
			actualNonNulls++
		}
	}
	expectedNonNulls := int64(numRows) - page.NumNulls()
	if actualNonNulls != expectedNonNulls {
		return nullCheckInfo{}, moerr.NewInvalidInputf(ctx,
			"malformed page: NumNulls() indicates %d non-nulls, but definition levels show %d",
			expectedNonNulls, actualNonNulls)
	}

	return nullCheckInfo{
		levels:             levels,
		maxDefinitionLevel: mp.maxDefinitionLevel,
		actualNonNulls:     actualNonNulls,
	}, nil
}

func validateStringDataCount(ctx context.Context, loader *strLoader, expectedNonNulls int64) error {
	var actualCount int64
	if loader.size != 0 {
		if loader.size <= 0 {
			return moerr.NewInvalidInputf(ctx, "malformed page: invalid fixed length %d", loader.size)
		}
		if len(loader.buf)%loader.size != 0 {
			return moerr.NewInvalidInputf(ctx,
				"malformed page: buffer length %d is not divisible by element size %d",
				len(loader.buf), loader.size)
		}
		actualCount = int64(len(loader.buf) / loader.size)
	} else if len(loader.offsets) > 0 {
		actualCount = int64(len(loader.offsets) - 1)
	}

	if actualCount != expectedNonNulls {
		return moerr.NewInvalidInputf(ctx,
			"malformed page: expected %d non-null values, but data contains %d",
			expectedNonNulls, actualCount)
	}
	return nil
}

func validateDictionaryIndicesCount(ctx context.Context, indices []int32, expectedNonNulls int64) error {
	if int64(len(indices)) != expectedNonNulls {
		return moerr.NewInvalidInputf(ctx,
			"malformed page: expected %d dictionary indices, but got %d",
			expectedNonNulls, len(indices))
	}
	return nil
}

func wrapParseError(ctx context.Context, row int, parseErr error) error {
	if parseErr == nil {
		return nil
	}
	var moErr *moerr.Error
	if errors.As(parseErr, &moErr) {
		return parseErr
	}
	return moerr.NewInternalErrorf(ctx, "row %d: %v", row, parseErr)
}

func processStringToFixed[T any](
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	parseFunc func(data []byte) (T, error),
	zeroVal T,
) error {
	numRows := int(page.NumRows())
	if numRows == 0 {
		return nil
	}

	nc, err := prepareNullCheck(ctx, mp, page)
	if err != nil {
		return err
	}

	var loader strLoader
	var indices []int32
	dict := page.Dictionary()
	if dict == nil {
		loader.init(page.Data())
		if err := validateStringDataCount(ctx, &loader, nc.actualNonNulls); err != nil {
			return err
		}
	} else {
		loader.init(dict.Page().Data())
		data := page.Data()
		indices = data.Int32()
		if err := validateDictionaryIndicesCount(ctx, indices, nc.actualNonNulls); err != nil {
			return err
		}
		if err := ensureDictionaryIndexes(ctx, int(dict.Len()), indices); err != nil {
			return err
		}
	}

	length := vec.Length()
	if err := vec.PreExtend(numRows+length, proc.Mp()); err != nil {
		return err
	}
	vec.SetLength(numRows + length)
	ret := vector.MustFixedColWithTypeCheck[T](vec)

	for i := 0; i < numRows; i++ {
		if nc.isNull(i) {
			nulls.Add(vec.GetNulls(), uint64(i+length))
			ret[i+length] = zeroVal
			continue
		}

		var data []byte
		if dict == nil {
			data = loader.loadNext()
		} else {
			idx := indices[loader.next]
			loader.next++
			data = loader.loadAt(idx)
		}

		val, parseErr := parseFunc(bytes.TrimSpace(data))
		if parseErr != nil {
			return wrapParseError(ctx, i, parseErr)
		}
		ret[i+length] = val
	}
	return nil
}

func processStringToJson(
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
) error {
	numRows := int(page.NumRows())
	if numRows == 0 {
		return nil
	}

	nc, err := prepareNullCheck(ctx, mp, page)
	if err != nil {
		return err
	}

	var loader strLoader
	var indices []int32
	dict := page.Dictionary()
	if dict == nil {
		loader.init(page.Data())
		if err := validateStringDataCount(ctx, &loader, nc.actualNonNulls); err != nil {
			return err
		}
	} else {
		loader.init(dict.Page().Data())
		data := page.Data()
		indices = data.Int32()
		if err := validateDictionaryIndicesCount(ctx, indices, nc.actualNonNulls); err != nil {
			return err
		}
		if err := ensureDictionaryIndexes(ctx, int(dict.Len()), indices); err != nil {
			return err
		}
	}

	if err := vec.PreExtend(vec.Length()+numRows, proc.Mp()); err != nil {
		return err
	}
	for i := 0; i < numRows; i++ {
		if nc.isNull(i) {
			if err := vector.AppendBytes(vec, nil, true, proc.Mp()); err != nil {
				return err
			}
			continue
		}

		var data []byte
		if dict == nil {
			data = loader.loadNext()
		} else {
			idx := indices[loader.next]
			loader.next++
			data = loader.loadAt(idx)
		}

		val, parseErr := types.ParseSliceToByteJson(bytes.TrimSpace(data))
		if parseErr != nil {
			return wrapParseError(ctx, i, parseErr)
		}
		if err := vector.AppendByteJson(vec, val, false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func readParquetPageValues(ctx context.Context, page parquet.Page) ([]parquet.Value, error) {
	n := int(page.NumRows())
	values := make([]parquet.Value, n)
	read, err := page.Values().ReadValues(values)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, moerr.ConvertGoError(ctx, err)
	}
	if read != n {
		return nil, moerr.NewInternalErrorf(ctx, "short read parquet values: got %d, expected %d", read, n)
	}
	return values, nil
}

func processParquetValuesToFixed[T any](
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	zeroVal T,
	convert func(parquet.Value) (T, error),
) error {
	values, err := readParquetPageValues(ctx, page)
	if err != nil {
		return err
	}
	if err := vec.PreExtend(vec.Length()+len(values), proc.Mp()); err != nil {
		return err
	}
	for i, v := range values {
		if v.IsNull() {
			if !mp.dstNull {
				return moerr.NewConstraintViolationf(ctx, "cannot load NULL value into NOT NULL column")
			}
			if err := vector.AppendFixed(vec, zeroVal, true, proc.Mp()); err != nil {
				return err
			}
			continue
		}
		val, err := convert(v)
		if err != nil {
			return wrapParseError(ctx, i, err)
		}
		if err := vector.AppendFixed(vec, val, false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func processParquetValuesToBytes(
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	convert func(parquet.Value) ([]byte, error),
) error {
	values, err := readParquetPageValues(ctx, page)
	if err != nil {
		return err
	}
	if err := vec.PreExtend(vec.Length()+len(values), proc.Mp()); err != nil {
		return err
	}
	for i, v := range values {
		if v.IsNull() {
			if !mp.dstNull {
				return moerr.NewConstraintViolationf(ctx, "cannot load NULL value into NOT NULL column")
			}
			if err := vector.AppendBytes(vec, nil, true, proc.Mp()); err != nil {
				return err
			}
			continue
		}
		val, err := convert(v)
		if err != nil {
			return wrapParseError(ctx, i, err)
		}
		if err := vector.AppendBytes(vec, val, false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func isParquetIntegerSource(st parquet.Type, includeBoolean bool) bool {
	if st.PhysicalType() == nil || isDecimalLogicalType(st.LogicalType()) {
		return false
	}
	lt := st.LogicalType()
	if lt != nil && (lt.Date != nil || lt.Time != nil || lt.Timestamp != nil) {
		return false
	}
	switch st.Kind() {
	case parquet.Boolean:
		return includeBoolean
	case parquet.Int32, parquet.Int64:
		return true
	default:
		return false
	}
}

func isParquetFloatConvertibleSource(st parquet.Type) bool {
	switch st.Kind() {
	case parquet.Int32, parquet.Int64:
		return isParquetIntegerSource(st, false) || isDecimalLogicalType(st.LogicalType())
	case parquet.Float, parquet.Double:
		return true
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return isDecimalLogicalType(st.LogicalType())
	default:
		return false
	}
}

func isParquetValueStringConvertibleSource(st parquet.Type) bool {
	if isDecimalLogicalType(st.LogicalType()) {
		return true
	}
	lt := st.LogicalType()
	if lt != nil && (lt.Date != nil || lt.Time != nil || lt.Timestamp != nil) {
		return true
	}
	switch st.Kind() {
	case parquet.Boolean, parquet.Int32, parquet.Int64, parquet.Float, parquet.Double:
		return true
	default:
		return false
	}
}

func parquetValueToInt64(ctx context.Context, st parquet.Type, v parquet.Value) (int64, error) {
	switch st.Kind() {
	case parquet.Boolean:
		if v.Boolean() {
			return 1, nil
		}
		return 0, nil
	case parquet.Int32:
		if lt := st.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			return int64(v.Uint32()), nil
		}
		return int64(v.Int32()), nil
	case parquet.Int64:
		if lt := st.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			val := v.Uint64()
			if val > uint64(1<<63-1) {
				return 0, moerr.NewInvalidInputf(ctx, "parquet value %d overflows BIGINT", val)
			}
			return int64(val), nil
		}
		return v.Int64(), nil
	default:
		return 0, moerr.NewInvalidInputf(ctx, "cannot convert parquet %s to integer", st.Kind())
	}
}

func parquetValueToUint64(ctx context.Context, st parquet.Type, v parquet.Value) (uint64, error) {
	switch st.Kind() {
	case parquet.Boolean:
		if v.Boolean() {
			return 1, nil
		}
		return 0, nil
	case parquet.Int32:
		if lt := st.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			return uint64(v.Uint32()), nil
		}
		val := v.Int32()
		if val < 0 {
			return 0, moerr.NewInvalidInputf(ctx, "negative parquet value %d overflows unsigned integer", val)
		}
		return uint64(val), nil
	case parquet.Int64:
		if lt := st.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			return v.Uint64(), nil
		}
		val := v.Int64()
		if val < 0 {
			return 0, moerr.NewInvalidInputf(ctx, "negative parquet value %d overflows unsigned integer", val)
		}
		return uint64(val), nil
	default:
		return 0, moerr.NewInvalidInputf(ctx, "cannot convert parquet %s to unsigned integer", st.Kind())
	}
}

func parquetValueToFloat64(ctx context.Context, st parquet.Type, v parquet.Value) (float64, error) {
	if isDecimalLogicalType(st.LogicalType()) {
		dec, err := parquetValueToDecimal256(ctx, st, v)
		if err != nil {
			return 0, err
		}
		return types.Decimal256ToFloat64(dec, parquetDecimalScale(st)), nil
	}
	switch st.Kind() {
	case parquet.Int32, parquet.Int64:
		val, err := parquetValueToInt64(ctx, st, v)
		if err != nil {
			return 0, err
		}
		return float64(val), nil
	case parquet.Float:
		return float64(v.Float()), nil
	case parquet.Double:
		return v.Double(), nil
	default:
		return 0, moerr.NewInvalidInputf(ctx, "cannot convert parquet %s to floating point", st.Kind())
	}
}

func parquetValueToString(ctx context.Context, st parquet.Type, v parquet.Value) (string, error) {
	lt := st.LogicalType()
	if isDecimalLogicalType(lt) {
		dec, err := parquetValueToDecimal256(ctx, st, v)
		if err != nil {
			return "", err
		}
		return dec.Format(parquetDecimalScale(st)), nil
	}
	if lt != nil {
		switch {
		case lt.Date != nil:
			return types.DaysFromUnixEpochToDate(v.Int32()).String(), nil
		case lt.Time != nil:
			return parquetTimeValueToString(v, lt), nil
		case lt.Timestamp != nil:
			return parquetTimestampValueToString(v, lt), nil
		}
	}
	switch st.Kind() {
	case parquet.Boolean:
		return strconv.FormatBool(v.Boolean()), nil
	case parquet.Int32, parquet.Int64:
		if lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			val, err := parquetValueToUint64(ctx, st, v)
			if err != nil {
				return "", err
			}
			return strconv.FormatUint(val, 10), nil
		}
		val, err := parquetValueToInt64(ctx, st, v)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(val, 10), nil
	case parquet.Float:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32), nil
	case parquet.Double:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64), nil
	default:
		return "", moerr.NewInvalidInputf(ctx, "cannot convert parquet %s to string", st.Kind())
	}
}

func parquetTimeValueToString(v parquet.Value, lt *format.LogicalType) string {
	switch {
	case lt.Time.Unit.Nanos != nil:
		return types.Time(v.Int64() / 1000).String()
	case lt.Time.Unit.Micros != nil:
		return types.Time(v.Int64()).String()
	default:
		return (types.Time(v.Int32()) * 1000).String()
	}
}

func parquetTimestampValueToString(v parquet.Value, lt *format.LogicalType) string {
	var micros int64
	switch {
	case lt.Timestamp.Unit.Nanos != nil:
		micros = v.Int64() / 1000
	case lt.Timestamp.Unit.Micros != nil:
		micros = v.Int64()
	default:
		micros = v.Int64() * 1000
	}
	if lt.Timestamp.IsAdjustedToUTC {
		return types.UnixMicroToTimestamp(micros).String()
	}
	return types.Datetime(types.UnixMicroToTimestamp(micros)).String()
}

func parquetValueToDecimal256(ctx context.Context, st parquet.Type, v parquet.Value) (types.Decimal256, error) {
	switch st.Kind() {
	case parquet.Int32:
		return decimal256FromInt64(int64(v.Int32())), nil
	case parquet.Int64:
		return decimal256FromInt64(v.Int64()), nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return decimalBytesToDecimal256(ctx, v.ByteArray())
	default:
		return types.Decimal256{}, moerr.NewInvalidInputf(ctx, "unsupported parquet physical type %s for decimal", st.Kind())
	}
}

type strLoader struct {
	buf     []byte
	offsets []uint32
	size    int
	next    int
}

func (ld *strLoader) init(data encoding.Values) {
	switch data.Kind() {
	case encoding.ByteArray:
		ld.buf, ld.offsets = data.ByteArray()
	case encoding.FixedLenByteArray:
		ld.buf, ld.size = data.FixedLenByteArray()
	default:
		panic("not supported kind " + data.Kind().String())
	}
}

func (ld *strLoader) loadNext() []byte {
	if ld.size != 0 {
		start := int(ld.next) * ld.size
		end := start + ld.size
		ld.next++
		return ld.buf[start:end]
	}

	start := ld.offsets[ld.next]
	end := ld.offsets[ld.next+1]
	ld.next++
	return ld.buf[start:end]
}

func (ld *strLoader) loadAt(i int32) []byte {
	if ld.size != 0 {
		start := int(i) * ld.size
		end := start + ld.size
		return ld.buf[start:end]
	}

	start := ld.offsets[i]
	end := ld.offsets[i+1]
	return ld.buf[start:end]
}

func copyPageToVec[T any](mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector, data []T) error {
	return copyPageToVecMap(mp, page, proc, vec, data, func(v T) T { return v })
}

func copyPageToVecMap[T, U any](mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector, data []T, itee func(t T) U) error {
	noNulls := !mp.srcNull || !mp.dstNull || page.NumNulls() == 0
	n := int(page.NumRows())

	length := vec.Length()
	err := vec.PreExtend(n+length, proc.Mp())
	if err != nil {
		return err
	}
	vec.SetLength(n + length)
	ret := vector.MustFixedColWithTypeCheck[U](vec)
	levels := page.DefinitionLevels()
	j := 0
	for i := 0; i < n; i++ {
		if !noNulls && levels[i] != mp.maxDefinitionLevel {
			nulls.Add(vec.GetNulls(), uint64(i+length))
		} else {
			ret[i+length] = itee(data[j])
			j++
		}
	}
	return nil
}

func ensureDictionaryIndexes(ctx context.Context, dictLen int, indexes []int32) error {
	for _, idx := range indexes {
		if idx < 0 || int(idx) >= dictLen {
			return moerr.NewInvalidInputf(ctx, "parquet dictionary index %d out of range %d", idx, dictLen)
		}
	}
	return nil
}

func copyDictPageToVec[T any](
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	dictLen int,
	indexes []int32,
	convert func(idx int32) T,
) error {
	if err := ensureDictionaryIndexes(proc.Ctx, dictLen, indexes); err != nil {
		return err
	}
	return copyPageToVecMap(mp, page, proc, vec, indexes, convert)
}

func parquetDecimalScale(st parquet.Type) int32 {
	if lt := st.LogicalType(); lt != nil && lt.Decimal != nil {
		return lt.Decimal.Scale
	}
	return 0
}

func rescaleDecimal64Values(ctx context.Context, values []types.Decimal64, srcScale, dstScale, dstWidth int32) error {
	scaleDiff := dstScale - srcScale
	for i, v := range values {
		scaled, err := v.Scale(scaleDiff)
		if err != nil {
			return err
		}
		if err := checkDecimalPrecision(ctx, decimal64ToBigInt(scaled), dstWidth, "decimal64"); err != nil {
			return err
		}
		values[i] = scaled
	}
	return nil
}

func rescaleDecimal128Values(ctx context.Context, values []types.Decimal128, srcScale, dstScale, dstWidth int32) error {
	scaleDiff := dstScale - srcScale
	for i, v := range values {
		scaled, err := v.Scale(scaleDiff)
		if err != nil {
			return err
		}
		if err := checkDecimalPrecision(ctx, decimal128ToBigInt(scaled), dstWidth, "decimal128"); err != nil {
			return err
		}
		values[i] = scaled
	}
	return nil
}

func rescaleDecimal256Values(ctx context.Context, values []types.Decimal256, srcScale, dstScale, dstWidth int32) error {
	scaleDiff := dstScale - srcScale
	for i, v := range values {
		scaled, err := v.Scale(scaleDiff)
		if err != nil {
			return err
		}
		if err := checkDecimalPrecision(ctx, decimal256ToBigInt(scaled), dstWidth, "decimal256"); err != nil {
			return err
		}
		values[i] = scaled
	}
	return nil
}

func checkDecimalPrecision(ctx context.Context, value *big.Int, width int32, typ string) error {
	if width <= 0 {
		return nil
	}
	digits := int32(len(new(big.Int).Abs(value).String()))
	if digits > width {
		return moerr.NewInvalidInputf(ctx, "parquet %s value exceeds destination precision %d", typ, width)
	}
	return nil
}

func decimal64ToBigInt(v types.Decimal64) *big.Int {
	return big.NewInt(int64(v))
}

func decimal128ToBigInt(v types.Decimal128) *big.Int {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], v.B64_127)
	binary.BigEndian.PutUint64(buf[8:16], v.B0_63)
	return decimalBytesToBigInt(buf[:])
}

func decimal256ToBigInt(v types.Decimal256) *big.Int {
	var buf [32]byte
	binary.BigEndian.PutUint64(buf[0:8], v.B192_255)
	binary.BigEndian.PutUint64(buf[8:16], v.B128_191)
	binary.BigEndian.PutUint64(buf[16:24], v.B64_127)
	binary.BigEndian.PutUint64(buf[24:32], v.B0_63)
	return decimalBytesToBigInt(buf[:])
}

var (
	maxInt64Big  = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 63), big.NewInt(1))
	minInt64Big  = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	maxInt128Big = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	minInt128Big = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	maxInt256Big = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1))
	minInt256Big = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 255))
)

func decodeDecimal64Values(ctx context.Context, kind parquet.Kind, data encoding.Values) ([]types.Decimal64, error) {
	switch kind {
	case parquet.Int32:
		src := data.Int32()
		dst := make([]types.Decimal64, len(src))
		for i, v := range src {
			dst[i] = types.Decimal64(int64(v))
		}
		return dst, nil
	case parquet.Int64:
		src := data.Int64()
		dst := make([]types.Decimal64, len(src))
		for i, v := range src {
			dst[i] = types.Decimal64(v)
		}
		return dst, nil
	case parquet.ByteArray:
		buf, offsets := data.ByteArray()
		if len(offsets) == 0 {
			return nil, nil
		}
		dst := make([]types.Decimal64, len(offsets)-1)
		for i := 0; i < len(dst); i++ {
			dec, err := decimalBytesToDecimal64(ctx, buf[offsets[i]:offsets[i+1]])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	case parquet.FixedLenByteArray:
		buf, size := data.FixedLenByteArray()
		if size <= 0 {
			return nil, moerr.NewInvalidInputf(ctx, "invalid fixed length %d for decimal64", size)
		}
		if len(buf)%size != 0 {
			return nil, moerr.NewInvalidInputf(ctx, "malformed fixed-len decimal64 data")
		}
		count := len(buf) / size
		dst := make([]types.Decimal64, count)
		for i := 0; i < count; i++ {
			dec, err := decimalBytesToDecimal64(ctx, buf[i*size:(i+1)*size])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	default:
		return nil, moerr.NewInvalidInputf(ctx, "unsupported parquet physical type %s for decimal64", kind)
	}
}

func decodeDecimal128Values(ctx context.Context, kind parquet.Kind, data encoding.Values) ([]types.Decimal128, error) {
	switch kind {
	case parquet.Int32:
		src := data.Int32()
		dst := make([]types.Decimal128, len(src))
		for i, v := range src {
			dst[i] = decimal128FromInt64(int64(v))
		}
		return dst, nil
	case parquet.Int64:
		src := data.Int64()
		dst := make([]types.Decimal128, len(src))
		for i, v := range src {
			dst[i] = decimal128FromInt64(v)
		}
		return dst, nil
	case parquet.ByteArray:
		buf, offsets := data.ByteArray()
		if len(offsets) == 0 {
			return nil, nil
		}
		dst := make([]types.Decimal128, len(offsets)-1)
		for i := 0; i < len(dst); i++ {
			dec, err := decimalBytesToDecimal128(ctx, buf[offsets[i]:offsets[i+1]])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	case parquet.FixedLenByteArray:
		buf, size := data.FixedLenByteArray()
		if size <= 0 {
			return nil, moerr.NewInvalidInputf(ctx, "invalid fixed length %d for decimal128", size)
		}
		if len(buf)%size != 0 {
			return nil, moerr.NewInvalidInputf(ctx, "malformed fixed-len decimal128 data")
		}
		count := len(buf) / size
		dst := make([]types.Decimal128, count)
		for i := 0; i < count; i++ {
			dec, err := decimalBytesToDecimal128(ctx, buf[i*size:(i+1)*size])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	default:
		return nil, moerr.NewInvalidInputf(ctx, "unsupported parquet physical type %s for decimal128", kind)
	}
}

func decodeDecimal256Values(ctx context.Context, kind parquet.Kind, data encoding.Values) ([]types.Decimal256, error) {
	switch kind {
	case parquet.Int32:
		src := data.Int32()
		dst := make([]types.Decimal256, len(src))
		for i, v := range src {
			dst[i] = decimal256FromInt64(int64(v))
		}
		return dst, nil
	case parquet.Int64:
		src := data.Int64()
		dst := make([]types.Decimal256, len(src))
		for i, v := range src {
			dst[i] = decimal256FromInt64(v)
		}
		return dst, nil
	case parquet.ByteArray:
		buf, offsets := data.ByteArray()
		if len(offsets) == 0 {
			return nil, nil
		}
		dst := make([]types.Decimal256, len(offsets)-1)
		for i := 0; i < len(dst); i++ {
			dec, err := decimalBytesToDecimal256(ctx, buf[offsets[i]:offsets[i+1]])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	case parquet.FixedLenByteArray:
		buf, size := data.FixedLenByteArray()
		if size <= 0 {
			return nil, moerr.NewInvalidInputf(ctx, "invalid fixed length %d for decimal256", size)
		}
		if len(buf)%size != 0 {
			return nil, moerr.NewInvalidInputf(ctx, "malformed fixed-len decimal256 data")
		}
		count := len(buf) / size
		dst := make([]types.Decimal256, count)
		for i := 0; i < count; i++ {
			dec, err := decimalBytesToDecimal256(ctx, buf[i*size:(i+1)*size])
			if err != nil {
				return nil, err
			}
			dst[i] = dec
		}
		return dst, nil
	default:
		return nil, moerr.NewInvalidInputf(ctx, "unsupported parquet physical type %s for decimal256", kind)
	}
}

func isDecimalLogicalType(lt *format.LogicalType) bool {
	return lt != nil && lt.Decimal != nil
}

func decimalBytesToDecimal64(ctx context.Context, b []byte) (types.Decimal64, error) {
	if len(b) == 0 {
		return 0, nil
	}
	bi := decimalBytesToBigInt(b)
	if bi.Cmp(minInt64Big) < 0 || bi.Cmp(maxInt64Big) > 0 {
		return 0, moerr.NewInvalidInputf(ctx, "decimal64 overflow for value %x", b)
	}
	return types.Decimal64(bi.Int64()), nil
}

func decimalBytesToDecimal128(ctx context.Context, b []byte) (types.Decimal128, error) {
	if len(b) == 0 {
		return types.Decimal128{}, nil
	}
	bi := decimalBytesToBigInt(b)
	if bi.Cmp(minInt128Big) < 0 || bi.Cmp(maxInt128Big) > 0 {
		return types.Decimal128{}, moerr.NewInvalidInputf(ctx, "decimal128 overflow for value %x", b)
	}
	buf, err := bigIntToTwosComplementBytes(ctx, bi, 16)
	if err != nil {
		return types.Decimal128{}, err
	}
	return types.Decimal128{
		B0_63:   binary.BigEndian.Uint64(buf[8:]),
		B64_127: binary.BigEndian.Uint64(buf[:8]),
	}, nil
}

func decimalBytesToDecimal256(ctx context.Context, b []byte) (types.Decimal256, error) {
	if len(b) == 0 {
		return types.Decimal256{}, nil
	}
	bi := decimalBytesToBigInt(b)
	if bi.Cmp(minInt256Big) < 0 || bi.Cmp(maxInt256Big) > 0 {
		return types.Decimal256{}, moerr.NewInvalidInputf(ctx, "decimal256 overflow for value %x", b)
	}
	buf, err := bigIntToTwosComplementBytes(ctx, bi, 32)
	if err != nil {
		return types.Decimal256{}, err
	}
	return types.Decimal256{
		B0_63:    binary.BigEndian.Uint64(buf[24:]),
		B64_127:  binary.BigEndian.Uint64(buf[16:24]),
		B128_191: binary.BigEndian.Uint64(buf[8:16]),
		B192_255: binary.BigEndian.Uint64(buf[:8]),
	}, nil
}

func decimal128FromInt64(v int64) types.Decimal128 {
	var hi uint64
	if v < 0 {
		hi = ^uint64(0)
	}
	return types.Decimal128{
		B0_63:   uint64(v),
		B64_127: hi,
	}
}

func decimal256FromInt64(v int64) types.Decimal256 {
	var hi uint64
	if v < 0 {
		hi = ^uint64(0)
	}
	return types.Decimal256{
		B0_63:    uint64(v),
		B64_127:  hi,
		B128_191: hi,
		B192_255: hi,
	}
}

func decimalBytesToBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return big.NewInt(0)
	}
	bi := new(big.Int).SetBytes(b)
	if b[0]&0x80 != 0 {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8)
		bi.Sub(bi, mod)
	}
	return bi
}

func bigIntToTwosComplementBytes(ctx context.Context, bi *big.Int, size int) ([]byte, error) {
	buf := make([]byte, size)
	if bi.Sign() >= 0 {
		tmp := bi.Bytes()
		if len(tmp) > size {
			return nil, moerr.NewInvalidInputf(ctx, "value does not fit in %d bytes", size)
		}
		copy(buf[size-len(tmp):], tmp)
		return buf, nil
	}

	mod := new(big.Int).Lsh(big.NewInt(1), uint(size*8))
	tmp := new(big.Int).Add(bi, mod)
	if tmp.Sign() <= 0 {
		return nil, moerr.NewInvalidInputf(ctx, "negative value out of range for %d bytes", size)
	}
	tbytes := tmp.Bytes()
	if len(tbytes) > size {
		return nil, moerr.NewInvalidInputf(ctx, "value does not fit in %d bytes", size)
	}
	copy(buf[size-len(tbytes):], tbytes)
	return buf, nil
}

func parseStringToDecimal64(s string, precision, scale int32) (types.Decimal64, error) {
	if s == "" {
		return 0, moerr.NewInvalidInputNoCtx("empty string cannot be converted to DECIMAL")
	}
	return types.ParseDecimal64(normalizeDecimalString(s), precision, scale)
}

func parseStringToDecimal128(s string, precision, scale int32) (types.Decimal128, error) {
	if s == "" {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtx("empty string cannot be converted to DECIMAL")
	}
	return types.ParseDecimal128(normalizeDecimalString(s), precision, scale)
}

func parseStringToDecimal256(s string, precision, scale int32) (types.Decimal256, error) {
	if s == "" {
		return types.Decimal256{}, moerr.NewInvalidInputNoCtx("empty string cannot be converted to DECIMAL")
	}
	return types.ParseDecimal256(normalizeDecimalString(s), precision, scale)
}

func normalizeDecimalString(s string) string {
	s = strings.ReplaceAll(s, "E", "e")
	if len(s) > 0 && s[0] == '+' {
		s = s[1:]
	}
	return s
}

func (h *ParquetHandler) getData(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	if h.rowCountOnly {
		return h.getDataRowCountOnly(bat)
	}

	length := 0
	finish := false
	for colIdx, col := range h.cols {
		if col == nil {
			continue
		}
		if param.Cols[colIdx].Hidden {
			continue
		}

		vec := bat.Vecs[colIdx]
		pages := col.Pages()
		n := h.batchCnt
		o := h.offset
	L:
		for n > 0 {
			page, err := pages.ReadPage()
			switch {
			case errors.Is(err, io.EOF):
				finish = true
				break L
			case err != nil:
				return moerr.ConvertGoError(param.Ctx, err)
			}

			nr := page.NumRows()
			if nr < o {
				o -= nr
				continue
			}

			if len(page.RepetitionLevels()) != 0 {
				return moerr.NewNYI(param.Ctx, "page has repetition")
			}

			if o > 0 || o+n < nr {
				page = page.Slice(o, min(n+o, nr))
			}
			o = 0
			n -= page.NumRows()

			err = h.mappers[colIdx].mapping(page, proc, vec)
			if err != nil {
				return err
			}
		}
		err := pages.Close()
		if err != nil {
			return moerr.ConvertGoError(param.Ctx, err)
		}
		length = vec.Length()
	}

	bat.SetRowCount(length)

	if finish {
		param.parqh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
	} else {
		h.offset += int64(length)
	}

	return nil
}

func (h *ParquetHandler) getDataRowCountOnly(bat *batch.Batch) error {
	batchLimit := int(h.batchCnt)
	rowCount := 0

	if h.rowCountRemaining > 0 {
		rowCount = min(h.rowCountRemaining, batchLimit)
		h.rowCountRemaining -= rowCount
	} else {
		rgs := h.file.RowGroups()
		if h.currentRowGroup >= len(rgs) {
			bat.SetRowCount(0)
			return nil
		}
		total := int(rgs[h.currentRowGroup].NumRows())
		h.currentRowGroup++
		rowCount = min(total, batchLimit)
		h.rowCountRemaining = total - rowCount
	}

	h.offset += int64(rowCount)
	bat.SetRowCount(rowCount)
	return nil
}

func (mp *columnMapper) pageIsNull(ctx context.Context, page parquet.Page, index int) (bool, error) {
	if !mp.srcNull || !mp.dstNull {
		return false, nil
	}
	if page.NumNulls() == 0 {
		return false, nil
	}
	levels := page.DefinitionLevels()
	if len(levels) != int(page.NumRows()) {
		return false, moerr.NewInvalidInput(ctx, "malformed page")
	}
	return levels[index] != mp.maxDefinitionLevel, nil
}

func (mp *columnMapper) mapping(page parquet.Page, proc *process.Process, vec *vector.Vector) error {
	return mp.mapper(mp, page, proc, vec)
}

type fsReaderAt struct {
	fs       fileservice.ETLFileService
	readPath string
	ctx      context.Context
}

func (r *fsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	vec := fileservice.IOVector{
		FilePath: r.readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset: off,
				Size:   int64(len(p)),
				Data:   p,
			},
		},
	}

	err = r.fs.Read(r.ctx, &vec)
	if err != nil {
		return 0, err
	}
	return int(vec.Entries[0].Size), nil
}
