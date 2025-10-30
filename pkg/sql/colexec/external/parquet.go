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
)

func scanParquetFile(ctx context.Context, param *ExternalParam, proc *process.Process, bat *batch.Batch) error {
	_, span := trace.Start(ctx, "scanParquetFile")
	defer span.End()

	if param.parqh == nil {
		var err error
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

	return param.parqh.getData(bat, param, proc)
}

var maxParquetBatchCnt int64 = 1000

func newParquetHandler(param *ExternalParam) (*ParquetHandler, error) {
	h := ParquetHandler{
		batchCnt: maxParquetBatchCnt,
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

func (h *ParquetHandler) prepare(param *ExternalParam) error {
	h.cols = make([]*parquet.Column, len(param.Attrs))
	h.mappers = make([]*columnMapper, len(param.Attrs))
	for _, attr := range param.Attrs {
		def := param.Cols[attr.ColIndex]
		if def.Hidden {
			continue
		}

		col := h.file.Root().Column(attr.ColName)
		if col == nil {
			return moerr.NewInvalidInputf(param.Ctx, "column %s not found", attr.ColName)
		}

		var fn *columnMapper
		if !col.Leaf() {
			return moerr.NewNYI(param.Ctx, "parquet nested type")
		} else {
			fn = h.getMapper(col, def.Typ)
		}
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
		h.cols[attr.ColIndex] = col
		h.mappers[attr.ColIndex] = fn
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
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			dict := page.Dictionary()
			data := page.Data()
			if dict == nil {
				return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) uint8 {
					return uint8(v)
				})
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) uint8 {
				return uint8(dictValues[int(idx)])
			})
		}
	case types.T_int8:
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			dict := page.Dictionary()
			data := page.Data()
			if dict == nil {
				return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) int8 {
					return int8(v)
				})
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int8 {
				return int8(dictValues[int(idx)])
			})
		}
	case types.T_uint16:
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) uint16 {
					return uint16(v)
				})
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) uint16 {
				return uint16(dictValues[int(idx)])
			})
		}
	case types.T_int16:
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) int16 {
					return int16(v)
				})
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int16 {
				return int16(dictValues[int(idx)])
			})
		}
	case types.T_int32:
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Int32())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int32 {
				return dictValues[int(idx)]
			})
		}
	case types.T_int64:
		if st.Kind() != parquet.Int64 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Int64())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Int64()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int64 {
				return dictValues[int(idx)]
			})
		}
	case types.T_uint32:
		if st.Kind() != parquet.Int32 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Uint32())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Uint32()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) uint32 {
				return dictValues[int(idx)]
			})
		}
	case types.T_uint64:
		if st.Kind() != parquet.Int64 {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Uint64())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Uint64()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) uint64 {
				return dictValues[int(idx)]
			})
		}
	case types.T_float32:
		if st.Kind() != parquet.Float {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Float())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Float()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) float32 {
				return dictValues[int(idx)]
			})
		}
	case types.T_float64:
		if st.Kind() != parquet.Double {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				return copyPageToVec(mp, page, proc, vec, data.Double())
			}

			dictData := dict.Page().Data()
			dictValues := dictData.Double()
			indices := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) float64 {
				return dictValues[int(idx)]
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
			dict := page.Dictionary()
			if dict == nil {
				bs, _ := data.Data()
				ls := types.DecodeSlice[int32](bs)
				return copyPageToVecMap(mp, page, proc, vec, ls, func(t int32) types.Date {
					return types.DaysFromUnixEpochToDate(t)
				})
			}

			dictData := dict.Page().Data()
			bs, _ := dictData.Data()
			dictDates := types.DecodeSlice[int32](bs)
			indexes := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictDates), indexes, func(idx int32) types.Date {
				return types.DaysFromUnixEpochToDate(dictDates[int(idx)])
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
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
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
	case types.T_decimal64:
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			kind := st.Kind()
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				values, err := decodeDecimal64Values(proc.Ctx, kind, data)
				if err != nil {
					return err
				}
				return copyPageToVec(mp, page, proc, vec, values)
			}

			dictValues, err := decodeDecimal64Values(proc.Ctx, kind, dict.Page().Data())
			if err != nil {
				return err
			}
			indexes := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indexes, func(idx int32) types.Decimal64 {
				return dictValues[int(idx)]
			})
		}
	case types.T_decimal128:
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			kind := st.Kind()
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				values, err := decodeDecimal128Values(proc.Ctx, kind, data)
				if err != nil {
					return err
				}
				return copyPageToVec(mp, page, proc, vec, values)
			}

			dictValues, err := decodeDecimal128Values(proc.Ctx, kind, dict.Page().Data())
			if err != nil {
				return err
			}
			indexes := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indexes, func(idx int32) types.Decimal128 {
				return dictValues[int(idx)]
			})
		}
	case types.T_decimal256:
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			kind := st.Kind()
			data := page.Data()
			dict := page.Dictionary()
			if dict == nil {
				values, err := decodeDecimal256Values(proc.Ctx, kind, data)
				if err != nil {
					return err
				}
				return copyPageToVec(mp, page, proc, vec, values)
			}

			dictValues, err := decodeDecimal256Values(proc.Ctx, kind, dict.Page().Data())
			if err != nil {
				return err
			}
			indexes := data.Int32()
			return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indexes, func(idx int32) types.Decimal256 {
				return dictValues[int(idx)]
			})
		}
	}
	if mp.mapper != nil {
		return mp
	}
	return nil
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

func copyDictPageToVec[T any](mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector, dictLen int, indexes []int32, convert func(idx int32) T) error {
	if err := ensureDictionaryIndexes(proc.Ctx, dictLen, indexes); err != nil {
		return err
	}
	return copyPageToVecMap(mp, page, proc, vec, indexes, convert)
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

func (h *ParquetHandler) getData(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	length := 0
	finish := false
	for colIdx, col := range h.cols {
		if param.Cols[colIdx].Hidden {
			continue
		}

		mapper := h.mappers[colIdx]
		if mapper == nil {
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
