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
	"time"

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
		param.parqh, err = newParquetHandler(param)
		if err != nil {
			return err
		}
	}

	// With cached page iterators, we read up to maxParquetBatchCnt rows per call.
	// The iterator automatically tracks position, so no need to calculate offset-based batchCnt.
	param.parqh.batchCnt = maxParquetBatchCnt

	return param.parqh.getData(bat, param, proc)
}

var maxParquetBatchCnt int64 = 100000

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

		// For S3 sources, pre-read the entire file into memory to avoid
		// many small random HTTP requests which cause severe performance issues.
		// Parquet reading involves many small random reads (metadata, page headers, etc.)
		// which are fast on local disk but extremely slow over S3 due to HTTP RTT.
		if param.Extern.ScanType == tree.S3 {
			fileSize := param.FileSize[param.Fileparam.FileIndex-1]
			data := make([]byte, fileSize)
			vec := fileservice.IOVector{
				FilePath: readPath,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   fileSize,
						Data:   data,
					},
				},
			}
			if err := fs.Read(param.Ctx, &vec); err != nil {
				return err
			}
			r = bytes.NewReader(data)
		} else {
			r = &fsReaderAt{
				fs:       fs,
				readPath: readPath,
				ctx:      param.Ctx,
			}
		}
	}
	var err error
	h.file, err = parquet.OpenFile(r, param.FileSize[param.Fileparam.FileIndex-1])
	return moerr.ConvertGoError(param.Ctx, err)
}

// findColumnIgnoreCase finds a column in the Parquet schema with case-insensitive matching.
// It first tries exact match for performance, then falls back to case-insensitive match.
// Returns error if multiple columns match case-insensitively (ambiguous), even if one is an exact match.
func (h *ParquetHandler) findColumnIgnoreCase(ctx context.Context, name string) (*parquet.Column, error) {
	root := h.file.Root()
	nameLower := strings.ToLower(name)

	// Single pass: find all columns that match case-insensitively
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

	// Check for ambiguity: multiple columns match case-insensitively
	if len(caseInsensitiveMatches) > 1 {
		return nil, moerr.NewInvalidInputf(ctx,
			"ambiguous column name %s: multiple columns match case-insensitively (%s and %s)",
			name, caseInsensitiveMatches[0].Name(), caseInsensitiveMatches[1].Name())
	}

	// Return exact match if found, otherwise the single case-insensitive match
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
	h.pages = make([]parquet.Pages, len(param.Cols))
	h.currentPage = make([]parquet.Page, len(param.Cols))
	h.pageOffset = make([]int64, len(param.Cols))
	for _, attr := range param.Attrs {
		def := param.Cols[attr.ColIndex]
		if def.Hidden {
			continue
		}

		// Use case-insensitive column lookup (fix for issue #15621)
		col, err := h.findColumnIgnoreCase(param.Ctx, attr.ColName)
		if err != nil {
			return err
		}
		if col == nil {
			return moerr.NewInvalidInputf(param.Ctx, "column %s not found", attr.ColName)
		}

		var fn *columnMapper
		if !col.Leaf() {
			// nested type: List/Struct/Map
			targetType := types.T(def.Typ.Id)
			if !isNestedTargetTypeSupported(targetType) {
				return moerr.NewInvalidInputf(param.Ctx,
					"parquet nested column %s must map to JSON or TEXT type, got %s",
					attr.ColName, targetType.String())
			}
			h.hasNestedCols = true
			fn = h.getNestedMapper(col, def.Typ)
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
		h.pages[attr.ColIndex] = col.Pages()
	}

	// init row reader if has nested columns
	if h.hasNestedCols {
		h.rowReader = parquet.NewReader(h.file)
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
		if st.Kind() == parquet.Int32 {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to UINT8 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (uint8, error) {
						intVal, err := strconv.ParseUint(util.UnsafeBytesToString(data), 10, 8)
						if err != nil {
							return 0, err
						}
						return uint8(intVal), nil
					},
					uint8(0),
				)
			}
		}
	case types.T_int8:
		if st.Kind() == parquet.Int32 {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to INT8 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (int8, error) {
						intVal, err := strconv.ParseInt(util.UnsafeBytesToString(data), 10, 8)
						if err != nil {
							return 0, err
						}
						return int8(intVal), nil
					},
					int8(0),
				)
			}
		}
	case types.T_uint16:
		if st.Kind() == parquet.Int32 {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to UINT16 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (uint16, error) {
						intVal, err := strconv.ParseUint(util.UnsafeBytesToString(data), 10, 16)
						if err != nil {
							return 0, err
						}
						return uint16(intVal), nil
					},
					uint16(0),
				)
			}
		}
	case types.T_int16:
		if st.Kind() == parquet.Int32 {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to INT16 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (int16, error) {
						intVal, err := strconv.ParseInt(util.UnsafeBytesToString(data), 10, 16)
						if err != nil {
							return 0, err
						}
						return int16(intVal), nil
					},
					int16(0),
				)
			}
		}
	case types.T_int32:
		if st.Kind() == parquet.Int32 {
			// Direct mapping: INT32 → INT32
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to INT32 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (int32, error) {
						intVal, err := strconv.ParseInt(util.UnsafeBytesToString(data), 10, 32)
						if err != nil {
							return 0, err
						}
						return int32(intVal), nil
					},
					int32(0),
				)
			}
		}
	case types.T_int64:
		if st.Kind() == parquet.Int64 {
			// Direct mapping: INT64 → INT64
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
		} else if st.Kind() == parquet.Int32 {
			// Widening conversion: INT32 → INT64 (always safe)
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				dict := page.Dictionary()
				if dict == nil {
					return copyPageToVecMap(mp, page, proc, vec, data.Int32(), func(v int32) int64 {
						return int64(v)
					})
				}

				dictData := dict.Page().Data()
				dictValues := dictData.Int32()
				indices := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int64 {
					return int64(dictValues[int(idx)])
				})
			}
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to INT64 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (int64, error) {
						intVal, err := strconv.ParseInt(util.UnsafeBytesToString(data), 10, 64)
						if err != nil {
							return 0, err
						}
						return intVal, nil
					},
					int64(0),
				)
			}
		}
	case types.T_uint32:
		if st.Kind() == parquet.Int32 {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to UINT32 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (uint32, error) {
						intVal, err := strconv.ParseUint(util.UnsafeBytesToString(data), 10, 32)
						if err != nil {
							return 0, err
						}
						return uint32(intVal), nil
					},
					uint32(0),
				)
			}
		}
	case types.T_uint64:
		if st.Kind() == parquet.Int64 {
			// Direct mapping: INT64 → UINT64
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
		} else if st.Kind() == parquet.Int32 {
			// Widening conversion: INT32 → UINT64 (always safe for unsigned target)
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				dict := page.Dictionary()
				if dict == nil {
					return copyPageToVecMap(mp, page, proc, vec, data.Uint32(), func(v uint32) uint64 {
						return uint64(v)
					})
				}

				dictData := dict.Page().Data()
				dictValues := dictData.Uint32()
				indices := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) uint64 {
					return uint64(dictValues[int(idx)])
				})
			}
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to UINT64 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (uint64, error) {
						intVal, err := strconv.ParseUint(util.UnsafeBytesToString(data), 10, 64)
						if err != nil {
							return 0, err
						}
						return intVal, nil
					},
					uint64(0),
				)
			}
		}
	case types.T_float32:
		if st.Kind() == parquet.Float {
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
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to FLOAT32 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (float32, error) {
						floatVal, err := strconv.ParseFloat(util.UnsafeBytesToString(data), 32)
						if err != nil {
							return 0, err
						}
						return float32(floatVal), nil
					},
					float32(0),
				)
			}
		}
	case types.T_float64:
		if st.Kind() == parquet.Double {
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
		} else if st.Kind() == parquet.Float {
			// Widening conversion: FLOAT32 → FLOAT64 (always safe)
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				dict := page.Dictionary()
				if dict == nil {
					return copyPageToVecMap(mp, page, proc, vec, data.Float(), func(v float32) float64 {
						return float64(v)
					})
				}

				dictData := dict.Page().Data()
				dictValues := dictData.Float()
				indices := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) float64 {
					return float64(dictValues[int(idx)])
				})
			}
		} else if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to FLOAT64 conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (float64, error) {
						floatVal, err := strconv.ParseFloat(util.UnsafeBytesToString(data), 64)
						if err != nil {
							return 0, err
						}
						return floatVal, nil
					},
					float64(0),
				)
			}
		}
	case types.T_date:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to DATE conversion
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Date, error) {
						dateVal, err := types.ParseDateCast(util.UnsafeBytesToString(data))
						if err != nil {
							return 0, err
						}
						return dateVal, nil
					},
					types.Date(0),
				)
			}
		} else {
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
		}
	case types.T_timestamp:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to TIMESTAMP conversion
			scale := dt.Scale
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				// Get timezone from process (must be done at runtime)
				loc := proc.Base.SessionInfo.TimeZone
				if loc == nil {
					loc = time.UTC
				}

				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Timestamp, error) {
						timestampVal, err := types.ParseTimestamp(loc, util.UnsafeBytesToString(data), scale)
						if err != nil {
							return 0, err
						}
						return timestampVal, nil
					},
					types.Timestamp(0),
				)
			}
		} else {
			lt := st.LogicalType()
			if lt == nil {
				break
			}
			// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
			tsT := lt.Timestamp
			if tsT == nil {
				break
			}
			// isAdjustedToUTC: true=UTC (use directly), false=local time (subtract session timezone offset for correct MO display)
			isAdjustedToUTC := tsT.IsAdjustedToUTC
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				dict := page.Dictionary()

				// Get timezone offset for non-UTC timestamps
				var tzOffsetMicros int64 = 0
				if !isAdjustedToUTC {
					loc := proc.Base.SessionInfo.TimeZone
					if loc == nil {
						loc = time.Local
					}
					// Get the timezone offset in seconds, then convert to microseconds
					// We use current time to get the offset, which handles DST correctly for current data
					_, offset := time.Now().In(loc).Zone()
					tzOffsetMicros = int64(offset) * 1000000 // seconds to microseconds
				}

				switch {
				case tsT.Unit.Nanos != nil:
					tzOffsetNanos := tzOffsetMicros * 1000 // convert to nanoseconds
					if dict != nil {
						dictData := dict.Page().Data()
						dictValues := dictData.Int64()
						converted := make([]types.Timestamp, len(dictValues))
						for i, v := range dictValues {
							converted[i] = types.UnixNanoToTimestamp(v - tzOffsetNanos)
						}
						indexes := data.Int32()
						return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
							return converted[int(idx)]
						})
					}
					return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
						return types.UnixNanoToTimestamp(v - tzOffsetNanos)
					})
				case tsT.Unit.Micros != nil:
					if dict != nil {
						dictData := dict.Page().Data()
						dictValues := dictData.Int64()
						converted := make([]types.Timestamp, len(dictValues))
						for i, v := range dictValues {
							converted[i] = types.UnixMicroToTimestamp(v - tzOffsetMicros)
						}
						indexes := data.Int32()
						return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
							return converted[int(idx)]
						})
					}
					return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
						return types.UnixMicroToTimestamp(v - tzOffsetMicros)
					})
				case tsT.Unit.Millis != nil:
					tzOffsetMillis := tzOffsetMicros / 1000 // convert to milliseconds
					if dict != nil {
						dictData := dict.Page().Data()
						dictValues := dictData.Int64()
						converted := make([]types.Timestamp, len(dictValues))
						for i, v := range dictValues {
							converted[i] = types.UnixMicroToTimestamp((v - tzOffsetMillis) * 1000)
						}
						indexes := data.Int32()
						return copyDictPageToVec(mp, page, proc, vec, len(converted), indexes, func(idx int32) types.Timestamp {
							return converted[int(idx)]
						})
					}
					return copyPageToVecMap(mp, page, proc, vec, data.Int64(), func(v int64) types.Timestamp {
						return types.UnixMicroToTimestamp((v - tzOffsetMillis) * 1000)
					})
				default:
					return moerr.NewInternalError(proc.Ctx, "unknown unit")
				}
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to TIME conversion
			scale := dt.Scale
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Time, error) {
						timeVal, err := types.ParseTime(util.UnsafeBytesToString(data), scale)
						if err != nil {
							return 0, err
						}
						return timeVal, nil
					},
					types.Time(0),
				)
			}
		} else {
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
		}
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			numRows := int(page.NumRows())
			if numRows == 0 {
				return nil
			}

			// Prepare null check info once
			nc, err := prepareNullCheck(proc.Ctx, mp, page)
			if err != nil {
				return err
			}

			err = vec.PreExtend(numRows, proc.Mp())
			if err != nil {
				return err
			}

			var loader strLoader
			var indices []int32
			var cache []*types.Varlena
			dict := page.Dictionary()
			if dict == nil {
				loader.init(page.Data())
				// Validate string data count for non-dictionary mode
				if err := validateStringDataCount(proc.Ctx, &loader, nc.actualNonNulls); err != nil {
					return err
				}
			} else {
				loader.init(dict.Page().Data())
				data := page.Data()
				indices = data.Int32()
				dictLen := int(dict.Len())
				cache = make([]*types.Varlena, dictLen)

				// Validate dictionary indices count matches expected non-null rows
				if err := validateDictionaryIndicesCount(proc.Ctx, indices, nc.actualNonNulls); err != nil {
					return err
				}

				// Validate dictionary indices are in range
				if err := ensureDictionaryIndexes(proc.Ctx, dictLen, indices); err != nil {
					return err
				}
			}
			for i := 0; i < numRows; i++ {
				if nc.isNull(i) {
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
				// idx is already validated by ensureDictionaryIndexes above
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
		lt := st.LogicalType()
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(lt) {
			// STRING to DECIMAL64
			precision := int32(dt.Width)
			scale := int32(dt.Scale)
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Decimal64, error) {
						decVal, err := parseStringToDecimal64(util.UnsafeBytesToString(data), precision, scale)
						if err != nil {
							return 0, err
						}
						return decVal, nil
					},
					types.Decimal64(0),
				)
			}
		} else {
			// Binary DECIMAL
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
		}
	case types.T_decimal128:
		lt := st.LogicalType()
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(lt) {
			// STRING to DECIMAL128 conversion (no DECIMAL LogicalType means it's stored as string)
			precision := int32(dt.Width)
			scale := int32(dt.Scale)
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Decimal128, error) {
						decVal, err := parseStringToDecimal128(util.UnsafeBytesToString(data), precision, scale)
						if err != nil {
							return types.Decimal128{}, err
						}
						return decVal, nil
					},
					types.Decimal128{},
				)
			}
		} else {
			// Binary DECIMAL (INT32/INT64/FixedLenByteArray/ByteArray with DECIMAL LogicalType)
			// PyArrow stores DECIMAL as FixedLenByteArray with DECIMAL LogicalType (big-endian two's complement)
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

// nullCheckInfo holds pre-processed information for null checking.
// This avoids calling page.DefinitionLevels() repeatedly in tight loops.
type nullCheckInfo struct {
	noNulls            bool   // fast path: no null check needed
	levels             []byte // DefinitionLevels array
	maxDefinitionLevel byte   // definition level for non-null values
	actualNonNulls     int64  // actual non-null count from traversing levels
}

// isNull checks if the value at the given index is null.
// This method is inline-friendly for use in tight loops.
func (nc *nullCheckInfo) isNull(index int) bool {
	if nc.noNulls {
		return false
	}
	return nc.levels[index] != nc.maxDefinitionLevel
}

// prepareNullCheck prepares null check information outside of the loop.
// It traverses DefinitionLevels to compute actual non-null count, not trusting NumNulls().
func prepareNullCheck(ctx context.Context, mp *columnMapper, page parquet.Page) (nullCheckInfo, error) {
	numRows := int(page.NumRows())

	// Fast path: source or destination doesn't allow null
	if !mp.srcNull || !mp.dstNull {
		return nullCheckInfo{
			noNulls:        true,
			actualNonNulls: int64(numRows),
		}, nil
	}

	// Fast path: page has no null values
	if page.NumNulls() == 0 {
		return nullCheckInfo{
			noNulls:        true,
			actualNonNulls: int64(numRows),
		}, nil
	}

	levels := page.DefinitionLevels()

	// Length validation
	if len(levels) != numRows {
		return nullCheckInfo{}, moerr.NewInvalidInputf(ctx,
			"malformed page: definition levels length %d != numRows %d",
			len(levels), numRows)
	}

	// Traverse levels to compute actual non-null count, not trusting NumNulls()
	var actualNonNulls int64
	for _, level := range levels {
		if level == mp.maxDefinitionLevel {
			actualNonNulls++
		}
	}

	// Consistency check with NumNulls() to detect corrupted pages
	expectedNonNulls := int64(numRows) - page.NumNulls()
	if actualNonNulls != expectedNonNulls {
		return nullCheckInfo{}, moerr.NewInvalidInputf(ctx,
			"malformed page: NumNulls() indicates %d non-nulls, but definition levels show %d",
			expectedNonNulls, actualNonNulls)
	}

	return nullCheckInfo{
		noNulls:            false,
		levels:             levels,
		maxDefinitionLevel: mp.maxDefinitionLevel,
		actualNonNulls:     actualNonNulls,
	}, nil
}

// validateStringDataCount validates that string data count matches expected non-null rows.
func validateStringDataCount(ctx context.Context, loader *strLoader, expectedNonNulls int64) error {
	var actualCount int64

	if loader.size != 0 {
		// FixedLenByteArray
		if loader.size <= 0 {
			return moerr.NewInvalidInputf(ctx, "malformed page: invalid fixed length %d", loader.size)
		}
		// Check len(buf) % size == 0
		if len(loader.buf)%loader.size != 0 {
			return moerr.NewInvalidInputf(ctx,
				"malformed page: buffer length %d is not divisible by element size %d",
				len(loader.buf), loader.size)
		}
		actualCount = int64(len(loader.buf) / loader.size)
	} else {
		// ByteArray
		if len(loader.offsets) == 0 {
			actualCount = 0
		} else {
			actualCount = int64(len(loader.offsets) - 1)
		}
	}

	if actualCount != expectedNonNulls {
		return moerr.NewInvalidInputf(ctx,
			"malformed page: expected %d non-null values, but data contains %d",
			expectedNonNulls, actualCount)
	}

	return nil
}

// validateDictionaryIndicesCount validates that dictionary indices count matches expected non-null rows.
func validateDictionaryIndicesCount(ctx context.Context, indices []int32, expectedNonNulls int64) error {
	if int64(len(indices)) != expectedNonNulls {
		return moerr.NewInvalidInputf(ctx,
			"malformed page: expected %d dictionary indices, but got %d",
			expectedNonNulls, len(indices))
	}
	return nil
}

// wrapParseError wraps parse errors with row context.
// If the error is already a *moerr.Error, it returns it directly to preserve the original error code.
func wrapParseError(ctx context.Context, row int, parseErr error) error {
	if parseErr == nil {
		return nil
	}

	// If already a moerr, return directly to preserve original error code
	var moErr *moerr.Error
	if errors.As(parseErr, &moErr) {
		return parseErr
	}

	// Only wrap plain errors with row context
	return moerr.NewInternalErrorf(ctx, "row %d: %v", row, parseErr)
}

// processStringToFixed is a generic function for STRING → fixed-length type conversion.
// Execution order: validate → extend length → process data
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

	// ========== Phase 1: Validate (don't modify vector state) ==========

	// 1.1 Prepare null check info (traverse DefinitionLevels to compute actual non-null count)
	nc, err := prepareNullCheck(ctx, mp, page)
	if err != nil {
		return err
	}

	// 1.2 Initialize strLoader and dictionary indices
	var loader strLoader
	var indices []int32
	dict := page.Dictionary()

	if dict == nil {
		loader.init(page.Data())
		// 1.3 Validate plain page data count
		if err := validateStringDataCount(ctx, &loader, nc.actualNonNulls); err != nil {
			return err
		}
	} else {
		loader.init(dict.Page().Data())
		data := page.Data()
		indices = data.Int32()
		// 1.4 Validate dictionary indices count
		if err := validateDictionaryIndicesCount(ctx, indices, nc.actualNonNulls); err != nil {
			return err
		}
		// 1.5 Validate dictionary index bounds
		if err := ensureDictionaryIndexes(ctx, int(dict.Len()), indices); err != nil {
			return err
		}
	}

	// ========== Phase 2: Extend vector (only after validation passes) ==========

	length := vec.Length()
	if err := vec.PreExtend(numRows+length, proc.Mp()); err != nil {
		return err
	}
	vec.SetLength(numRows + length)
	ret := vector.MustFixedColWithTypeCheck[T](vec)

	// ========== Phase 3: Process data ==========

	for i := 0; i < numRows; i++ {
		if nc.isNull(i) {
			nulls.Add(vec.GetNulls(), uint64(i+length))
			ret[i+length] = zeroVal
			continue
		}

		// Read data
		var data []byte
		if dict == nil {
			data = loader.loadNext()
		} else {
			idx := indices[loader.next]
			loader.next++
			data = loader.loadAt(idx)
		}

		// Use bytes.TrimSpace to avoid string allocation
		data = bytes.TrimSpace(data)

		// Parse and write
		val, parseErr := parseFunc(data)
		if parseErr != nil {
			return wrapParseError(ctx, i, parseErr)
		}
		ret[i+length] = val
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

func (h *ParquetHandler) getData(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	if h.hasNestedCols {
		return h.getDataByRow(bat, param, proc)
	}
	return h.getDataByPage(bat, param, proc)
}

func (h *ParquetHandler) getDataByPage(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	length := 0
	finish := false
	for _, attr := range param.Attrs {
		colIdx := attr.ColIndex
		if param.Cols[colIdx].Hidden {
			continue
		}

		mapper := h.mappers[colIdx]
		if mapper == nil {
			continue
		}

		vec := bat.Vecs[colIdx]

		pages := h.pages[colIdx]
		n := h.batchCnt
		pageOff := h.pageOffset[colIdx]
	L:
		for n > 0 {
			// Use cached page if available, otherwise read next page
			page := h.currentPage[colIdx]
			if page == nil {
				var err error
				page, err = pages.ReadPage()
				switch {
				case errors.Is(err, io.EOF):
					finish = true
					break L
				case err != nil:
					return moerr.ConvertGoError(param.Ctx, err)
				}
				h.currentPage[colIdx] = page
				pageOff = 0
			}

			nr := page.NumRows()
			if nr <= pageOff {
				// Current page exhausted, clear cache and read next
				h.currentPage[colIdx] = nil
				h.pageOffset[colIdx] = 0
				pageOff = 0
				continue
			}

			if len(page.RepetitionLevels()) != 0 {
				return moerr.NewNYI(param.Ctx, "page has repetition")
			}

			// Calculate how many rows to read from this page
			remaining := nr - pageOff
			toRead := min(n, remaining)

			slicedPage := page.Slice(pageOff, pageOff+toRead)
			pageOff += toRead
			n -= toRead

			// Update page offset
			h.pageOffset[colIdx] = pageOff

			// If we've consumed the entire page, clear the cache
			if pageOff >= nr {
				h.currentPage[colIdx] = nil
				h.pageOffset[colIdx] = 0
			}

			err := h.mappers[colIdx].mapping(slicedPage, proc, vec)
			if err != nil {
				return err
			}
		}
		length = vec.Length()
	}

	bat.SetRowCount(length)

	h.offset += int64(length)
	if h.file != nil && h.offset >= h.file.NumRows() {
		finish = true
	}

	if finish {
		// Close all page iterators when file processing is complete
		for _, pages := range h.pages {
			if pages != nil {
				pages.Close()
			}
		}
		param.parqh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
		}
	}

	return nil
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

// parseStringToDecimal64 converts a string to DECIMAL64 with given precision and scale.
// It supports:
// - Normal decimal numbers: "123.45"
// - Scientific notation: "1.23e10", "1E5", "1.23E10" (both lowercase e and uppercase E)
// - Negative numbers: "-123.45"
// - Positive sign: "+123.45"
// - Leading zeros: "0123.45"
// The string should already be trimmed of whitespace.
func parseStringToDecimal64(s string, precision, scale int32) (types.Decimal64, error) {
	if s == "" {
		return 0, moerr.NewInvalidInputNoCtx("empty string cannot be converted to DECIMAL")
	}

	// Normalize the string to be more user-friendly:
	// 1. Convert uppercase E to lowercase e (for scientific notation)
	// 2. Remove leading positive sign (ParseDecimal64 doesn't support it)
	s = normalizeDecimalString(s)

	// Use existing ParseDecimal64 which handles:
	// - Scientific notation (lowercase e only)
	// - Normal decimals
	// - Precision and scale validation
	result, err := types.ParseDecimal64(s, precision, scale)
	if err != nil {
		return 0, err
	}

	return result, nil
}

// normalizeDecimalString normalizes a decimal string to match ParseDecimal expectations.
// This allows us to support more user-friendly formats (like uppercase E and positive sign)
// while using the existing ParseDecimal functions.
func normalizeDecimalString(s string) string {
	// Convert uppercase E to lowercase e (for scientific notation like "1E5" → "1e5")
	// This makes us compatible with PostgreSQL, MySQL, SQL Server, Oracle
	s = strings.ReplaceAll(s, "E", "e")

	// Remove leading positive sign ("+123.45" → "123.45")
	// ParseDecimal doesn't support it, but all major databases do
	if len(s) > 0 && s[0] == '+' {
		s = s[1:]
	}

	return s
}

// parseStringToDecimal128 converts a string to DECIMAL128 with given precision and scale.
// It supports the same formats as parseStringToDecimal64.
func parseStringToDecimal128(s string, precision, scale int32) (types.Decimal128, error) {
	if s == "" {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtx("empty string cannot be converted to DECIMAL")
	}

	// Normalize the string (same as DECIMAL64)
	s = normalizeDecimalString(s)

	// Use existing ParseDecimal128
	result, err := types.ParseDecimal128(s, precision, scale)
	if err != nil {
		return types.Decimal128{}, err
	}

	return result, nil
}
