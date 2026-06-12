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
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

var maxParquetBatchCnt int64 = 100000

const maxParquetS3PrefetchSize int64 = 128 * 1024 * 1024

func newParquetHandler(param *ExternalParam) (*ParquetHandler, error) {
	h := ParquetHandler{
		batchCnt:         maxParquetBatchCnt,
		filepathColIndex: -1, // sentinel: not projected
	}
	err := h.openFile(param, hasPhysicalParquetAttrs(param))
	if err != nil {
		return nil, err
	}
	if err := h.initRowGroupSelection(param); err != nil {
		return nil, err
	}

	// Empty file handling (0 rows): only check column count, skip column name and type checks.
	if h.file.NumRows() == 0 {
		if param.Extern.ExternType == int32(plan.ExternType_LOAD) &&
			param.ColumnListLen > int32(len(param.Attrs)) {
			return nil, moerr.NewNYI(param.Ctx, "parquet load with @variables in column list")
		}
		// Skip column count check in Hive mode: partition-only projections have
		// 0 expected physical columns while the empty file still has schema columns.
		if !param.Extern.HivePartitioning {
			parquetColCnt := len(h.file.Root().Columns())
			tableColCnt := getParquetExpectedColCnt(param)
			if parquetColCnt != tableColCnt {
				return nil, moerr.NewInvalidInputf(param.Ctx,
					"column count mismatch: parquet file has %d columns, but table has %d columns",
					parquetColCnt, tableColCnt)
			}
		}
		// Caller treats (nil, nil) as "empty file, advance to next".
		return nil, nil
	}
	if h.rowGroupRows == 0 {
		return nil, nil
	}

	err = h.prepare(param)
	if err != nil {
		return nil, err
	}

	return &h, nil
}

func (h *ParquetHandler) initRowGroupSelection(param *ExternalParam) error {
	all := h.file.RowGroups()
	if len(param.ParquetRowGroupShards) == 0 {
		h.rowGroups = all
	} else {
		currentFileIndex := int32(0)
		if param.Fileparam != nil && param.Fileparam.FileIndex > 0 {
			currentFileIndex = int32(param.Fileparam.FileIndex - 1)
		}
		for _, shard := range param.ParquetRowGroupShards {
			if shard.FileIndex != currentFileIndex {
				continue
			}
			start := int(shard.RowGroupStart)
			end := int(shard.RowGroupEnd)
			if start < 0 || end <= start || end > len(all) {
				return moerr.NewInvalidInputf(param.Ctx,
					"invalid parquet row group shard [%d,%d) for file index %d with %d row groups",
					start, end, currentFileIndex, len(all))
			}
			h.rowGroups = append(h.rowGroups, all[start:end]...)
		}
	}
	if len(h.rowGroups) == 0 {
		h.rowGroup = parquet.MultiRowGroup()
		h.rowGroupRows = 0
		return nil
	}
	h.rowGroup = parquet.MultiRowGroup(h.rowGroups...)
	h.rowGroupRows = h.rowGroup.NumRows()
	return nil
}

func hasPhysicalParquetAttrs(param *ExternalParam) bool {
	for _, attr := range param.Attrs {
		colIdx := int(attr.ColIndex)
		if colIdx < 0 || colIdx >= len(param.Cols) {
			return true
		}
		if param.Cols[colIdx].Hidden {
			continue
		}
		if param.isHivePartitionCol(attr.ColName) {
			continue
		}
		if catalog.ContainExternalHidenCol(attr.ColName) {
			continue
		}
		return true
	}
	return false
}

func (h *ParquetHandler) openFile(param *ExternalParam, prefetchS3 bool) error {
	var r io.ReaderAt
	var fileSize int64
	switch {
	case param.Extern.ScanType == tree.INLINE:
		data := util.UnsafeStringToBytes(param.Extern.Data)
		r = bytes.NewReader(data)
		fileSize = int64(len(data))
		param.addParquetProfile(process.ParquetProfileStats{BytesRead: fileSize})
	case param.Extern.Local:
		return moerr.NewNYI(param.Ctx, "load parquet local")
	default:
		fs, readPath, err := plan2.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
		if err != nil {
			return err
		}
		// Validate FileIndex to avoid out-of-bounds access
		if param.Fileparam.FileIndex <= 0 || param.Fileparam.FileIndex > len(param.FileSize) {
			return moerr.NewInternalErrorf(param.Ctx, "invalid FileIndex %d for FileSize length %d",
				param.Fileparam.FileIndex, len(param.FileSize))
		}
		fileSize = param.FileSize[param.Fileparam.FileIndex-1]

		if shouldPrefetchS3Parquet(param.Extern.ScanType, prefetchS3, fileSize, len(param.ParquetRowGroupShards) > 0) {
			data := make([]byte, int(fileSize))
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
			param.addParquetProfile(process.ParquetProfileStats{
				BytesRead:     fileSize,
				PrefetchBytes: fileSize,
			})
			r = bytes.NewReader(data)
		} else {
			r = &fsReaderAt{
				fs:       fs,
				readPath: readPath,
				ctx:      param.Ctx,
				param:    param,
			}
		}
	}
	var err error
	h.file, err = parquet.OpenFile(r, fileSize)
	return moerr.ConvertGoError(param.Ctx, err)
}

func shouldPrefetchS3Parquet(scanType int, prefetchS3 bool, fileSize int64, hasRowGroupShards bool) bool {
	return scanType == tree.S3 &&
		prefetchS3 &&
		!hasRowGroupShards &&
		fileSize >= 0 &&
		fileSize <= maxParquetS3PrefetchSize
}

type parquetColumnLookup struct {
	exact  map[string]*parquet.Column
	folded map[string][]*parquet.Column
}

func newParquetColumnLookup(root *parquet.Column) parquetColumnLookup {
	lookup := parquetColumnLookup{
		exact:  make(map[string]*parquet.Column),
		folded: make(map[string][]*parquet.Column),
	}
	for _, col := range root.Columns() {
		lookup.exact[col.Name()] = col
		nameLower := strings.ToLower(col.Name())
		lookup.folded[nameLower] = append(lookup.folded[nameLower], col)
	}
	return lookup
}

// find finds a column in the Parquet schema with case-insensitive matching.
// It returns an ambiguity error if multiple columns match case-insensitively,
// even when one of them is an exact match.
func (lookup parquetColumnLookup) find(ctx context.Context, name string) (*parquet.Column, error) {
	caseInsensitiveMatches := lookup.folded[strings.ToLower(name)]
	// Check for ambiguity: multiple columns match case-insensitively
	if len(caseInsensitiveMatches) > 1 {
		return nil, moerr.NewInvalidInputf(ctx,
			"ambiguous column name %s: multiple columns match case-insensitively (%s and %s)",
			name, caseInsensitiveMatches[0].Name(), caseInsensitiveMatches[1].Name())
	}

	// Return exact match if found, otherwise the single case-insensitive match
	if exactMatch := lookup.exact[name]; exactMatch != nil {
		return exactMatch, nil
	}
	if len(caseInsensitiveMatches) == 1 {
		return caseInsensitiveMatches[0], nil
	}

	return nil, nil
}

// findColumnIgnoreCase is kept for direct unit tests; prepare() builds the
// lookup once and uses it for all target columns.
func (h *ParquetHandler) findColumnIgnoreCase(ctx context.Context, name string) (*parquet.Column, error) {
	return newParquetColumnLookup(h.file.Root()).find(ctx, name)
}

func (h *ParquetHandler) prepare(param *ExternalParam) error {
	if h.rowGroup == nil && h.file != nil {
		if len(h.rowGroups) == 0 {
			h.rowGroups = h.file.RowGroups()
		}
		if len(h.rowGroups) > 0 {
			h.rowGroup = parquet.MultiRowGroup(h.rowGroups...)
			h.rowGroupRows = h.rowGroup.NumRows()
		}
	}

	h.cols = make([]*parquet.Column, len(param.Cols))
	h.mappers = make([]*columnMapper, len(param.Cols))
	h.pages = make([]parquet.Pages, len(param.Cols))
	h.currentPage = make([]parquet.Page, len(param.Cols))
	h.pageOffset = make([]int64, len(param.Cols))
	columnLookup := newParquetColumnLookup(h.file.Root())
	var rowGroupChunks []parquet.ColumnChunk
	if h.rowGroup != nil {
		rowGroupChunks = h.rowGroup.ColumnChunks()
	}
	for _, attr := range param.Attrs {
		colIdx := int(attr.ColIndex)
		if colIdx < 0 || colIdx >= len(param.Cols) {
			return moerr.NewInvalidInputf(param.Ctx, "invalid column index %d for column %s", attr.ColIndex, attr.ColName)
		}
		def := param.Cols[colIdx]
		if def.Hidden {
			continue
		}

		// Skip virtual columns: they are not in Parquet schema.
		if param.isHivePartitionCol(attr.ColName) {
			h.partitionColIndices = append(h.partitionColIndices, colIdx)
			continue
		}
		if catalog.ContainExternalHidenCol(attr.ColName) {
			h.filepathColIndex = colIdx
			continue
		}

		h.hasPhysicalCol = true

		// Use case-insensitive column lookup (fix for issue #15621)
		col, err := columnLookup.find(param.Ctx, attr.ColName)
		if err != nil {
			return err
		}
		if col == nil {
			return moerr.NewInvalidInputf(param.Ctx, "column %s not found", attr.ColName)
		}
		physicalCol := col
		var fn *columnMapper
		if !col.Leaf() {
			targetType := types.T(def.Typ.Id)
			switch targetType {
			case types.T_array_float32, types.T_array_float64:
				physicalCol, fn = h.getNestedListMapper(col, def.Typ)
			default:
				if !isNestedTargetTypeSupported(targetType) {
					return moerr.NewInvalidInputf(param.Ctx,
						"parquet nested column %s must map to JSON or TEXT type, got %s",
						attr.ColName, targetType.String())
				}
				h.hasNestedCols = true
				fn = h.getNestedMapper(col, def.Typ)
			}
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
		h.cols[colIdx] = physicalCol
		h.mappers[colIdx] = fn
		if physicalCol.Leaf() {
			leafIdx := int(physicalCol.Index())
			if leafIdx < 0 || leafIdx >= len(rowGroupChunks) {
				return moerr.NewInvalidInputf(param.Ctx,
					"invalid parquet leaf column index %d for column %s", leafIdx, attr.ColName)
			}
			h.pages[colIdx] = rowGroupChunks[leafIdx].Pages()
		}
	}

	if !h.hasPhysicalCol {
		h.rowCountOnly = true
	}

	// init row reader if has nested columns
	if h.hasNestedCols {
		h.rowReader = h.rowGroup.Rows()
	}

	return nil
}

func (*ParquetHandler) getNestedListMapper(sc *parquet.Column, dt plan.Type) (*parquet.Column, *columnMapper) {
	leaf, ok := parquetListElementLeaf(sc)
	if !ok {
		return nil, nil
	}
	width := int(dt.Width)
	if width <= 0 {
		width = types.MaxArrayDimension
	}
	elemCanBeNull := leaf.Optional()
	if elemCanBeNull && width == types.MaxArrayDimension {
		// MO array vectors cannot represent NULL elements inside variable-length array rows yet.
		return nil, nil
	}
	if sc.Optional() && dt.NotNullable {
		return nil, nil
	}
	maxDefinitionLevel := byte(leaf.MaxDefinitionLevel())
	if maxDefinitionLevel == 0 {
		return nil, nil
	}
	listEmptyLevel := maxDefinitionLevel - 1
	if elemCanBeNull {
		if maxDefinitionLevel < 2 {
			return nil, nil
		}
		listEmptyLevel = maxDefinitionLevel - 2
	}

	mp := &columnMapper{
		srcNull:            true,
		dstNull:            !dt.NotNullable,
		maxDefinitionLevel: maxDefinitionLevel,
		allowRepetition:    true,
		listCanBeNull:      sc.Optional(),
		listElemCanBeNull:  elemCanBeNull,
		// maxDL means "element present"; empty-list rows use the level before element presence.
		listEmptyLevel: listEmptyLevel,
	}
	if mp.listElemCanBeNull {
		mp.listElemNullLevel = maxDefinitionLevel - 1
	}
	if mp.listCanBeNull {
		if mp.listEmptyLevel == 0 {
			return nil, nil
		}
		mp.listNullLevel = mp.listEmptyLevel - 1
	}

	switch types.T(dt.Id) {
	case types.T_array_float32:
		if leaf.Type().Kind() != parquet.Float {
			return nil, nil
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetListToArray(proc.Ctx, mp, page, proc, vec, width, func(v parquet.Value) (float32, error) {
				return v.Float(), nil
			})
		}
	case types.T_array_float64:
		if leaf.Type().Kind() != parquet.Double {
			return nil, nil
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetListToArray(proc.Ctx, mp, page, proc, vec, width, func(v parquet.Value) (float64, error) {
				return v.Double(), nil
			})
		}
	default:
		return nil, nil
	}
	return leaf, mp
}

func parquetListElementLeaf(sc *parquet.Column) (*parquet.Column, bool) {
	if sc == nil || sc.Leaf() {
		return nil, false
	}
	st := sc.Type()
	if st == nil {
		return nil, false
	}
	lt := st.LogicalType()
	if lt == nil || lt.List == nil {
		return nil, false
	}

	listCol := sc.Column("list")
	if listCol == nil || !listCol.Repeated() {
		return nil, false
	}
	elem := listCol.Column("element")
	if elem == nil {
		elem = listCol.Column("item")
	}
	if elem == nil && len(listCol.Columns()) == 1 {
		elem = listCol.Columns()[0]
	}
	if elem == nil || !elem.Leaf() {
		return nil, false
	}
	return elem, true
}

func (*ParquetHandler) getMapper(sc *parquet.Column, dt plan.Type) *columnMapper {
	st := sc.Type()
	if st.PhysicalType() == nil {
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

			// Fail early: if page has NULLs and destination doesn't allow them
			if mp.srcNull && page.NumNulls() > 0 && !mp.dstNull {
				return moerr.NewConstraintViolationf(proc.Ctx,
					"cannot load NULL value into NOT NULL column")
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint8(0), func(v parquet.Value) (uint8, error) {
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int8(0), func(v parquet.Value) (int8, error) {
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
	case types.T_uint16:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint16(0), func(v parquet.Value) (uint16, error) {
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
	case types.T_int16:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int16(0), func(v parquet.Value) (int16, error) {
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
	case types.T_int32:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		if st.Kind() == parquet.Int32 && isPlainOrSignedIntegerLogical(st) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				if dict := page.Dictionary(); dict != nil {
					dictData := dict.Page().Data()
					dictValues := dictData.Int32()
					indices := data.Int32()
					return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) int32 {
						return dictValues[int(idx)]
					})
				}
				return copyPageToVec(mp, page, proc, vec, data.Int32())
			}
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int32(0), func(v parquet.Value) (int32, error) {
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, int64(0), func(v parquet.Value) (int64, error) {
				return parquetValueToInt64(proc.Ctx, st, v)
			})
		}
	case types.T_uint32:
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint32(0), func(v parquet.Value) (uint32, error) {
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
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
			break
		}
		if !isParquetIntegerSource(st, true) {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint64(0), func(v parquet.Value) (uint64, error) {
				return parquetValueToUint64(proc.Ctx, st, v)
			})
		}
	case types.T_bit:
		if !isParquetIntegerSource(st, true) {
			break
		}
		bitWidth := parquetBitWidth(dt)
		maxValue := maxParquetBitValue(bitWidth)
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, uint64(0), func(v parquet.Value) (uint64, error) {
				val, err := parquetValueToUint64(proc.Ctx, st, v)
				if err != nil {
					return 0, err
				}
				if val > maxValue {
					return 0, moerr.NewInvalidInputf(proc.Ctx, "parquet value %d overflows BIT(%d)", val, bitWidth)
				}
				return val, nil
			})
		}
	case types.T_float32:
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(st.LogicalType()) {
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
			break
		}
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
		if (st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray) && !isDecimalLogicalType(st.LogicalType()) {
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
			break
		}
		if !isParquetFloatConvertibleSource(st) {
			break
		}
		if st.LogicalType() == nil {
			switch st.Kind() {
			case parquet.Double:
				mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
					data := page.Data()
					if dict := page.Dictionary(); dict != nil {
						dictData := dict.Page().Data()
						dictValues := dictData.Double()
						indices := data.Int32()
						return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) float64 {
							return dictValues[int(idx)]
						})
					}
					return copyPageToVec(mp, page, proc, vec, data.Double())
				}
			case parquet.Float:
				mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
					data := page.Data()
					if dict := page.Dictionary(); dict != nil {
						dictData := dict.Page().Data()
						dictValues := dictData.Float()
						indices := data.Int32()
						return copyDictPageToVec(mp, page, proc, vec, len(dictValues), indices, func(idx int32) float64 {
							return float64(dictValues[int(idx)])
						})
					}
					return copyPageToVecMap(mp, page, proc, vec, data.Float(), func(v float32) float64 {
						return float64(v)
					})
				}
			}
			if mp.mapper != nil {
				break
			}
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, float64(0), func(v parquet.Value) (float64, error) {
				return parquetValueToFloat64(proc.Ctx, st, v)
			})
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
		if st.Kind() == parquet.ByteArray || st.Kind() == parquet.FixedLenByteArray {
			// Support STRING to DATETIME conversion
			scale := dt.Scale
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processStringToFixed(proc.Ctx, mp, page, proc, vec,
					func(data []byte) (types.Datetime, error) {
						datetimeVal, err := types.ParseDatetime(util.UnsafeBytesToString(data), scale)
						if err != nil {
							return 0, err
						}
						return datetimeVal, nil
					},
					types.Datetime(0),
				)
			}
			break
		}
		lt := st.LogicalType()
		if lt == nil {
			break
		}
		if lt.Date != nil {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				data := page.Data()
				dict := page.Dictionary()
				if dict == nil {
					bs, _ := data.Data()
					ls := types.DecodeSlice[int32](bs)
					return copyPageToVecMap(mp, page, proc, vec, ls, func(t int32) types.Datetime {
						return types.DaysFromUnixEpochToDate(t).ToDatetime()
					})
				}

				dictData := dict.Page().Data()
				bs, _ := dictData.Data()
				dictDates := types.DecodeSlice[int32](bs)
				indexes := data.Int32()
				return copyDictPageToVec(mp, page, proc, vec, len(dictDates), indexes, func(idx int32) types.Datetime {
					return types.DaysFromUnixEpochToDate(dictDates[int(idx)]).ToDatetime()
				})
			}
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
		} else if isParquetIntegerSource(st, false) {
			mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
				return processParquetValuesToFixed(proc.Ctx, mp, page, proc, vec, types.Time(0), func(v parquet.Value) (types.Time, error) {
					val, err := parquetValueToInt64(proc.Ctx, st, v)
					if err != nil {
						return 0, err
					}
					return types.Time(val), nil
				})
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
	case types.T_json:
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToJson(proc.Ctx, mp, page, proc, vec)
		}
	case types.T_uuid:
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToFixed(proc.Ctx, mp, page, proc, vec,
				func(data []byte) (types.Uuid, error) {
					return types.ParseUuid(util.UnsafeBytesToString(data))
				},
				types.Uuid{},
			)
		}
	case types.T_enum:
		if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
			break
		}
		enumValues := dt.Enumvalues
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToFixed(proc.Ctx, mp, page, proc, vec,
				func(data []byte) (types.Enum, error) {
					return types.ParseEnum(enumValues, util.UnsafeBytesToString(data))
				},
				types.Enum(0),
			)
		}
	case types.T_array_float32:
		if !isPlainStringLikeType(st) {
			break
		}
		width := int(dt.Width)
		if width <= 0 {
			width = types.MaxArrayDimension
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToArray[float32](proc.Ctx, mp, page, proc, vec, width)
		}
	case types.T_array_float64:
		if !isPlainStringLikeType(st) {
			break
		}
		width := int(dt.Width)
		if width <= 0 {
			width = types.MaxArrayDimension
		}
		mp.mapper = func(mp *columnMapper, page parquet.Page, proc *process.Process, vec *vector.Vector) error {
			return processStringToArray[float64](proc.Ctx, mp, page, proc, vec, width)
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

	// Fast path: source doesn't allow null
	if !mp.srcNull {
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

	// Page has NULLs - check if destination allows them
	if !mp.dstNull {
		return nullCheckInfo{}, moerr.NewConstraintViolationf(ctx,
			"cannot load NULL value into NOT NULL column")
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

func processStringToArray[T types.RealNumbers](
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	width int,
) error {
	numRows := int(page.NumRows())
	if numRows == 0 {
		return nil
	}
	if width <= 0 {
		return moerr.NewInvalidInputf(ctx, "invalid vector dimension %d", width)
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
			if err := vector.AppendArray[T](vec, nil, true, proc.Mp()); err != nil {
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

		val, parseErr := parseStringArrayValue[T](data)
		if parseErr != nil {
			return wrapParseError(ctx, i, parseErr)
		}
		if width != types.MaxArrayDimension && len(val) != width {
			return moerr.NewArrayDefMismatchNoCtx(width, len(val))
		}
		if err := vector.AppendArray[T](vec, val, false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func parseStringArrayValue[T types.RealNumbers](data []byte) ([]T, error) {
	text := strings.TrimSpace(util.UnsafeBytesToString(data))
	if isEmptyArrayText(text) {
		return []T{}, nil
	}
	return types.StringToArray[T](text)
}

func isEmptyArrayText(text string) bool {
	if len(text) < 2 || text[0] != '[' || text[len(text)-1] != ']' {
		return false
	}
	return strings.TrimSpace(text[1:len(text)-1]) == ""
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

func readParquetPageAllValues(ctx context.Context, page parquet.Page) ([]parquet.Value, error) {
	n := int(page.NumValues())
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

func processParquetListToArray[T types.RealNumbers](
	ctx context.Context,
	mp *columnMapper,
	page parquet.Page,
	proc *process.Process,
	vec *vector.Vector,
	width int,
	convert func(parquet.Value) (T, error),
) error {
	values, err := readParquetPageAllValues(ctx, page)
	if err != nil {
		return err
	}
	numRows := int(page.NumRows())
	if numRows == 0 {
		return nil
	}
	if len(values) == 0 {
		return moerr.NewInvalidInputf(ctx, "malformed parquet list page: %d rows but no values", numRows)
	}
	if values[0].RepetitionLevel() != 0 {
		return moerr.NewInvalidInputf(ctx, "malformed parquet list page: first repetition level is %d", values[0].RepetitionLevel())
	}
	if err := vec.PreExtend(vec.Length()+numRows, proc.Mp()); err != nil {
		return err
	}

	capHint := width
	if capHint == types.MaxArrayDimension {
		capHint = 0
	}
	row := make([]T, 0, capHint)
	rowNull := false
	rowEmpty := false
	rowCount := 0
	flushRow := func() error {
		if rowNull {
			rowCount++
			return vector.AppendArray[T](vec, nil, true, proc.Mp())
		}
		if width != types.MaxArrayDimension && len(row) != width {
			return moerr.NewArrayDefMismatchNoCtx(width, len(row))
		}
		rowCount++
		return vector.AppendArray[T](vec, row, false, proc.Mp())
	}

	for i, v := range values {
		if i > 0 && v.RepetitionLevel() == 0 {
			if err := flushRow(); err != nil {
				return err
			}
			row = row[:0]
			rowNull = false
			rowEmpty = false
		}

		definitionLevel := byte(v.DefinitionLevel())
		if mp.listCanBeNull && definitionLevel == mp.listNullLevel {
			if len(row) != 0 || rowEmpty || v.RepetitionLevel() != 0 {
				return moerr.NewInvalidInput(ctx, "malformed parquet list page: NULL row has repeated values")
			}
			if !mp.dstNull {
				return moerr.NewConstraintViolationf(ctx, "cannot load NULL value into NOT NULL column")
			}
			rowNull = true
			continue
		}
		if mp.listElemCanBeNull && definitionLevel == mp.listElemNullLevel {
			return moerr.NewInvalidInput(ctx, "parquet list NULL elements are not supported for vector columns")
		}
		if definitionLevel == mp.listEmptyLevel {
			if len(row) != 0 || rowNull || v.RepetitionLevel() != 0 {
				return moerr.NewInvalidInput(ctx, "malformed parquet list page: empty row has repeated values")
			}
			rowEmpty = true
			continue
		}
		if definitionLevel != mp.maxDefinitionLevel {
			return moerr.NewInvalidInputf(ctx,
				"parquet list value cannot map to vector: definition level %d, expected %d",
				definitionLevel, mp.maxDefinitionLevel)
		}
		if rowNull || rowEmpty {
			return moerr.NewInvalidInput(ctx, "malformed parquet list page: NULL row has repeated values")
		}

		val, err := convert(v)
		if err != nil {
			return wrapParseError(ctx, rowCount, err)
		}
		row = append(row, val)
	}
	if err := flushRow(); err != nil {
		return err
	}
	if rowCount != numRows {
		return moerr.NewInvalidInputf(ctx, "malformed parquet list page: mapped %d rows, expected %d", rowCount, numRows)
	}
	return nil
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

func parquetBitWidth(dt plan.Type) int {
	width := int(dt.Width)
	if width <= 0 || width > types.MaxBitLen {
		return types.MaxBitLen
	}
	return width
}

func maxParquetBitValue(width int) uint64 {
	if width >= types.MaxBitLen {
		return math.MaxUint64
	}
	return (uint64(1) << width) - 1
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

func isPlainOrSignedIntegerLogical(st parquet.Type) bool {
	lt := st.LogicalType()
	return lt == nil || (lt.Integer != nil && lt.Integer.IsSigned)
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
			if val > uint64(math.MaxInt64) {
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
		if lt := st.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			val, err := parquetValueToUint64(ctx, st, v)
			if err != nil {
				return 0, err
			}
			return float64(val), nil
		}
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
	n := int(page.NumRows())

	// Only skip NULL check if source doesn't allow null OR page has no nulls
	noNulls := !mp.srcNull || page.NumNulls() == 0

	// Fail early: if page has NULLs and destination doesn't allow them
	if !noNulls && !mp.dstNull {
		return moerr.NewConstraintViolationf(proc.Ctx,
			"cannot load NULL value into NOT NULL column")
	}

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

func isPlainStringLikeType(st parquet.Type) bool {
	if st.Kind() != parquet.ByteArray && st.Kind() != parquet.FixedLenByteArray {
		return false
	}
	lt := st.LogicalType()
	return lt == nil || lt.UTF8 != nil || lt.Json != nil
}

func parquetDecimalScale(st parquet.Type) int32 {
	if lt := st.LogicalType(); lt != nil && lt.Decimal != nil {
		return lt.Decimal.Scale
	}
	return 0
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
	if h.rowCountOnly {
		return h.getDataRowCountOnly(bat)
	}
	if h.hasNestedCols {
		return h.getDataByRow(bat, param, proc)
	}
	return h.getDataByPage(bat, param, proc)
}

func (h *ParquetHandler) isFinished() bool {
	return h == nil || h.offset >= h.rowGroupRows
}

func (h *ParquetHandler) closePages(ctx context.Context) error {
	var firstErr error
	for i, pages := range h.pages {
		if pages != nil {
			if err := pages.Close(); err != nil && firstErr == nil {
				firstErr = moerr.ConvertGoError(ctx, err)
			}
		}
		h.pages[i] = nil
		h.currentPage[i] = nil
		h.pageOffset[i] = 0
	}
	return firstErr
}

func (h *ParquetHandler) closePagesOnError(ctx context.Context, err error) error {
	if closeErr := h.closePages(ctx); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

func (h *ParquetHandler) getDataRowCountOnly(bat *batch.Batch) error {
	batchLimit := int(h.batchCnt)
	rowCount := 0

	if h.rowCountRemaining > 0 {
		rowCount = min(h.rowCountRemaining, batchLimit)
		h.rowCountRemaining -= rowCount
	} else {
		if h.currentRowGroup >= len(h.rowGroups) {
			bat.SetRowCount(0)
			return nil
		}
		total := int(h.rowGroups[h.currentRowGroup].NumRows())
		h.currentRowGroup++
		rowCount = min(total, batchLimit)
		h.rowCountRemaining = total - rowCount
	}

	h.offset += int64(rowCount)
	bat.SetRowCount(rowCount)
	return nil
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
				readStart := time.Now()
				page, err = pages.ReadPage()
				param.addParquetProfile(process.ParquetProfileStats{
					ReadPageTime: time.Since(readStart).Nanoseconds(),
				})
				switch {
				case errors.Is(err, io.EOF):
					finish = true
					break L
				case err != nil:
					return h.closePagesOnError(param.Ctx, moerr.ConvertGoError(param.Ctx, err))
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

			if len(page.RepetitionLevels()) != 0 && !h.mappers[colIdx].allowRepetition {
				err := moerr.NewNYI(param.Ctx, "page has repetition")
				return h.closePagesOnError(param.Ctx, err)
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

			mapStart := time.Now()
			err := h.mappers[colIdx].mapping(slicedPage, proc, vec)
			param.addParquetProfile(process.ParquetProfileStats{
				MapTime: time.Since(mapStart).Nanoseconds(),
			})
			if err != nil {
				return h.closePagesOnError(param.Ctx, err)
			}
		}
		length = vec.Length()
	}

	bat.SetRowCount(length)

	h.offset += int64(length)
	if h.isFinished() {
		finish = true
	}

	if finish {
		if err := h.closePages(param.Ctx); err != nil {
			return err
		}
		// File completion (FileFin/End) is now handled by Call's finishCurrentFile
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
	param    *ExternalParam
}

func (r *fsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	vec := fileservice.IOVector{
		FilePath: r.readPath,
		Policy:   fileservice.SkipFullFilePreloads,
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
	n = int(vec.Entries[0].Size)
	if n > 0 {
		r.param.addParquetProfile(process.ParquetProfileStats{BytesRead: int64(n)})
	}
	return n, nil
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

// getParquetExpectedColCnt calculates the expected column count for parquet loading.
// It excludes external hidden columns like __mo_filepath.
func getParquetExpectedColCnt(param *ExternalParam) int {
	cnt := 0
	for _, attr := range param.Attrs {
		if catalog.ContainExternalHidenCol(attr.ColName) {
			continue
		}
		if param.isHivePartitionCol(attr.ColName) {
			continue
		}
		cnt++
	}
	return cnt
}
