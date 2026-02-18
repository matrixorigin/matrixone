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
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

// --- ColumnMetaFetcher adapter ---

// parquetColumnMetaFetcher implements objectio.ColumnMetaFetcher.
// It bridges Parquet RowGroup statistics to MO's ZoneMap evaluation.
type parquetColumnMetaFetcher struct {
	metas map[uint16]objectio.ColumnMeta
}

func (f *parquetColumnMetaFetcher) MustGetColumn(seqnum uint16) objectio.ColumnMeta {
	if meta, ok := f.metas[seqnum]; ok {
		return meta
	}
	return objectio.BuildColumnMeta()
}

// --- Parquet statistics → ZoneMap conversion ---

// buildColumnMetaFromParquet builds a ColumnMeta with ZoneMap and NullCnt
// from a Parquet ColumnChunk's Bounds() statistics.
// Returns nil if statistics are unavailable or the type is unsupported.
func buildColumnMetaFromParquet(chunk parquet.ColumnChunk, moType types.T) objectio.ColumnMeta {
	cc, ok := chunk.(*parquet.FileColumnChunk)
	if !ok {
		return nil
	}

	minVal, maxVal, hasBounds := cc.Bounds()
	if !hasBounds {
		return nil
	}

	zm := parquetValuesToZoneMap(minVal, maxVal, moType)
	if zm == nil {
		return nil
	}

	meta := objectio.BuildColumnMeta()
	meta.SetZoneMap(zm)
	meta.SetNullCnt(uint32(cc.NullCount()))
	return meta
}

// parquetValuesToZoneMap converts Parquet min/max Values to a MO ZoneMap.
// Returns nil for unsupported types (caller should skip filtering).
func parquetValuesToZoneMap(minVal, maxVal parquet.Value, moType types.T) index.ZM {
	if minVal.IsNull() || maxVal.IsNull() {
		return nil
	}

	zm := index.NewZM(moType, 0)

	var minBytes, maxBytes []byte
	switch moType {
	case types.T_bool:
		minB := minVal.Boolean()
		maxB := maxVal.Boolean()
		minBytes = types.EncodeBool(&minB)
		maxBytes = types.EncodeBool(&maxB)

	case types.T_int8:
		v1 := int8(minVal.Int32())
		v2 := int8(maxVal.Int32())
		minBytes = types.EncodeInt8(&v1)
		maxBytes = types.EncodeInt8(&v2)

	case types.T_int16:
		v1 := int16(minVal.Int32())
		v2 := int16(maxVal.Int32())
		minBytes = types.EncodeInt16(&v1)
		maxBytes = types.EncodeInt16(&v2)

	case types.T_int32:
		v1 := minVal.Int32()
		v2 := maxVal.Int32()
		minBytes = types.EncodeInt32(&v1)
		maxBytes = types.EncodeInt32(&v2)

	case types.T_int64:
		v1 := minVal.Int64()
		v2 := maxVal.Int64()
		minBytes = types.EncodeInt64(&v1)
		maxBytes = types.EncodeInt64(&v2)

	case types.T_uint8:
		v1 := uint8(minVal.Int32())
		v2 := uint8(maxVal.Int32())
		minBytes = types.EncodeUint8(&v1)
		maxBytes = types.EncodeUint8(&v2)

	case types.T_uint16:
		v1 := uint16(minVal.Int32())
		v2 := uint16(maxVal.Int32())
		minBytes = types.EncodeUint16(&v1)
		maxBytes = types.EncodeUint16(&v2)

	case types.T_uint32:
		v1 := uint32(minVal.Int32())
		v2 := uint32(maxVal.Int32())
		minBytes = types.EncodeUint32(&v1)
		maxBytes = types.EncodeUint32(&v2)

	case types.T_uint64:
		v1 := uint64(minVal.Int64())
		v2 := uint64(maxVal.Int64())
		minBytes = types.EncodeUint64(&v1)
		maxBytes = types.EncodeUint64(&v2)

	case types.T_float32:
		v1 := minVal.Float()
		v2 := maxVal.Float()
		minBytes = types.EncodeFloat32(&v1)
		maxBytes = types.EncodeFloat32(&v2)

	case types.T_float64:
		v1 := minVal.Double()
		v2 := maxVal.Double()
		minBytes = types.EncodeFloat64(&v1)
		maxBytes = types.EncodeFloat64(&v2)

	case types.T_date:
		// Parquet DATE is int32 days since epoch
		v1 := types.DaysFromUnixEpochToDate(minVal.Int32())
		v2 := types.DaysFromUnixEpochToDate(maxVal.Int32())
		minBytes = types.EncodeDate(&v1)
		maxBytes = types.EncodeDate(&v2)

	case types.T_timestamp, types.T_datetime:
		// Parquet TIMESTAMP is int64 (micros/millis/nanos)
		// ZoneMap comparison is ordinal, so raw int64 works
		v1 := minVal.Int64()
		v2 := maxVal.Int64()
		minBytes = types.EncodeInt64(&v1)
		maxBytes = types.EncodeInt64(&v2)

	case types.T_varchar, types.T_char, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob:
		minBytes = minVal.ByteArray()
		maxBytes = maxVal.ByteArray()

	default:
		return nil
	}

	index.UpdateZM(zm, minBytes)
	index.UpdateZM(zm, maxBytes)
	return zm
}

// --- ParquetHandler RowGroup filter methods ---

// initRowGroupFilter initializes RowGroup filtering state on the handler.
func (h *ParquetHandler) initRowGroupFilter(param *ExternalParam) {
	h.filterExpr = param.Filter.FilterExpr
	if h.filterExpr == nil || len(param.Filter.columnMap) == 0 {
		return
	}

	// Build mapping: plan col position -> parquet column index
	h.filterColMap = make(map[int]int)
	for planColPos := range param.Filter.columnMap {
		if planColPos >= len(param.Attrs) {
			continue
		}
		colName := param.Attrs[planColPos].ColName
		for parquetIdx, col := range h.cols {
			if col != nil && col.Name() == colName {
				h.filterColMap[planColPos] = parquetIdx
				break
			}
		}
	}

	h.canFilter = len(h.filterColMap) > 0
}

// canSkipRowGroup returns true if the given RowGroup can be skipped
// based on its column statistics and the filter expression.
func (h *ParquetHandler) canSkipRowGroup(
	rg parquet.RowGroup,
	param *ExternalParam,
	proc *process.Process,
) bool {
	if !h.canFilter || h.filterExpr == nil {
		return false
	}

	chunks := rg.ColumnChunks()

	fetcher := &parquetColumnMetaFetcher{
		metas: make(map[uint16]objectio.ColumnMeta, len(h.filterColMap)),
	}
	for planColPos, parquetColIdx := range h.filterColMap {
		if parquetColIdx >= len(chunks) {
			return false
		}
		moType := types.T(param.Cols[planColPos].Typ.Id)
		meta := buildColumnMetaFromParquet(chunks[parquetColIdx], moType)
		if meta == nil {
			return false
		}
		fetcher.metas[uint16(planColPos)] = meta
	}

	zms := make([]objectio.ZoneMap, param.Filter.AuxIdCnt)
	vecs := make([]*vector.Vector, param.Filter.AuxIdCnt)

	notReportErrCtx := errutil.ContextWithNoReport(param.Ctx, true)
	needRead := colexec.EvaluateFilterByZoneMap(
		notReportErrCtx, proc, h.filterExpr, fetcher, h.filterColMap, zms, vecs,
	)

	return !needRead
}

// --- RowGroup-aware data reading ---

// getDataByRowGroup reads data with RowGroup-level filtering.
// It skips RowGroups whose statistics don't match the filter expression,
// then reads pages from matching RowGroups.
func (h *ParquetHandler) getDataByRowGroup(
	bat *batch.Batch, param *ExternalParam, proc *process.Process,
) error {
	length := 0

	for h.curRGIdx < len(h.rowGroups) {
		rg := h.rowGroups[h.curRGIdx]

		// RowGroup filtering: skip if statistics prove no match
		if h.canSkipRowGroup(rg, param, proc) {
			h.offset += rg.NumRows()
			h.curRGIdx++
			continue
		}

		// Open page iterators for this RowGroup if not already open
		if !h.rgPagesOpen(param) {
			chunks := rg.ColumnChunks()
			for _, attr := range param.Attrs {
				colIdx := attr.ColIndex
				if param.Cols[colIdx].Hidden || h.mappers[colIdx] == nil {
					continue
				}
				h.pages[colIdx] = chunks[colIdx].Pages()
			}
		}

		// Read pages from current RowGroup
		rgDone, err := h.readPagesFromCurrentRG(bat, param, proc, &length)
		if err != nil {
			return err
		}

		if rgDone {
			h.closeRGPages()
			h.offset += rg.NumRows()
			h.curRGIdx++
		}

		if int64(length) >= h.batchCnt {
			break
		}
	}

	bat.SetRowCount(length)

	// Signal file completion via offset
	if h.curRGIdx >= len(h.rowGroups) {
		h.offset = h.file.NumRows()
		h.closeRGPages()
	}

	return nil
}

// readPagesFromCurrentRG reads up to batchCnt rows from the current RowGroup.
// Returns true if the RowGroup is fully consumed.
func (h *ParquetHandler) readPagesFromCurrentRG(
	bat *batch.Batch, param *ExternalParam, proc *process.Process, length *int,
) (rgDone bool, err error) {
	rgDone = false
	for _, attr := range param.Attrs {
		colIdx := attr.ColIndex
		if param.Cols[colIdx].Hidden || h.mappers[colIdx] == nil {
			continue
		}

		vec := bat.Vecs[colIdx]
		pages := h.pages[colIdx]
		n := h.batchCnt - int64(*length)
		pageOff := h.pageOffset[colIdx]

		for n > 0 {
			page := h.currentPage[colIdx]
			if page == nil {
				page, err = pages.ReadPage()
				if errors.Is(err, io.EOF) {
					rgDone = true
					break
				}
				if err != nil {
					return false, moerr.ConvertGoError(param.Ctx, err)
				}
				h.currentPage[colIdx] = page
				pageOff = 0
			}

			nr := page.NumRows()
			if nr <= pageOff {
				h.currentPage[colIdx] = nil
				h.pageOffset[colIdx] = 0
				pageOff = 0
				continue
			}

			if len(page.RepetitionLevels()) != 0 {
				return false, moerr.NewNYI(param.Ctx, "page has repetition")
			}

			toRead := min(n, nr-pageOff)
			slicedPage := page.Slice(pageOff, pageOff+toRead)
			pageOff += toRead
			n -= toRead

			h.pageOffset[colIdx] = pageOff
			if pageOff >= nr {
				h.currentPage[colIdx] = nil
				h.pageOffset[colIdx] = 0
			}

			if err = h.mappers[colIdx].mapping(slicedPage, proc, vec); err != nil {
				return false, err
			}
		}
		*length = vec.Length()
	}
	return rgDone, nil
}

// rgPagesOpen returns true if page iterators are currently open for a RowGroup.
func (h *ParquetHandler) rgPagesOpen(param *ExternalParam) bool {
	for _, attr := range param.Attrs {
		colIdx := attr.ColIndex
		if param.Cols[colIdx].Hidden || h.mappers[colIdx] == nil {
			continue
		}
		return h.pages[colIdx] != nil
	}
	return false
}

// closeRGPages closes all page iterators and resets page state.
func (h *ParquetHandler) closeRGPages() {
	for i, pages := range h.pages {
		if pages != nil {
			pages.Close()
			h.pages[i] = nil
		}
		h.currentPage[i] = nil
		h.pageOffset[i] = 0
	}
}
