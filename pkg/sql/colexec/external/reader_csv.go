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
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CsvReader handles CSV and JSONLINE formats.
type CsvReader struct {
	param      *ExternalParam
	plh        *ParseLineHandler
	reader     io.ReadCloser
	prevStr    string
	ignoreLine int
	ignoreTag  int
	// pending aliases the parser's current record. Never call Read while it is
	// populated: Read reuses both LastRow and the record's backing bytes.
	pending         []csvparser.Field
	pendingRawBytes uint64
	rowArea         []int
}

// NewCsvReader creates a CsvReader.
// JSONLINE invalid config returns explicit error to match existing semantics.
func NewCsvReader(param *ExternalParam, proc *process.Process) (*CsvReader, error) {
	if param.Extern.Format == tree.JSONLINE {
		if param.Extern.JsonData != tree.OBJECT && param.Extern.JsonData != tree.ARRAY {
			return nil, moerr.NewNotSupportedf(proc.Ctx,
				"the jsonline format '%s' is not supported now", param.Extern.JsonData)
		}
	}
	r := &CsvReader{
		ignoreTag:  int(param.Extern.Tail.IgnoredLines),
		ignoreLine: int(param.Extern.Tail.IgnoredLines),
	}
	return r, nil
}

func (r *CsvReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	r.param = param
	r.ignoreLine = r.ignoreTag
	r.pending = nil
	r.pendingRawBytes = 0
	r.prevStr = ""

	// Open file and get io.ReadCloser (preserves offset read + decompression)
	baseReader, err := readFile(param, proc)
	if err != nil || baseReader == nil {
		return baseReader == nil && err == nil, err
	}
	r.reader, err = crt.GetUnCompressReader(proc, param.Extern.CompressType, param.Fileparam.Filepath, baseReader)
	if err != nil {
		baseReader.Close()
		return false, err
	}
	// GetUnCompressReader returns (nil, nil) on EOF (e.g., empty compressed file)
	// Treat as empty file to avoid NPE in ReadBatch
	if r.reader == nil {
		baseReader.Close()
		return true, nil
	}

	csvParser, err := newCSVParserFromReader(param.Extern, r.reader)
	if err != nil {
		r.reader.Close()
		r.reader = nil
		return false, err
	}
	// Preserve enough extra fields for strict LOAD's existing N/N+1 check;
	// non-strict LOAD already ignores fields beyond every projected column.
	retainedFields := int(param.ColumnListLen) + 2
	for _, attr := range param.Attrs {
		retainedFields = max(retainedFields, int(attr.ColFieldIndex)+1)
	}
	csvParser.RetainFields(retainedFields)
	if proc.Base.ImportMemory != nil {
		csvParser.LimitRecordBytes(csvRecordBytes(&param.ExParamConst, param.Extern))
		memory := proc.Base.ImportMemory
		if memory.GrowRecord != nil {
			csvParser.SetRecordBudget(memory.RecordBytes, func(need int64) (int64, error) {
				return memory.GrowRecord(proc.Ctx, need)
			})
		}
	}
	r.plh = &ParseLineHandler{csvReader: csvParser}
	return false, nil
}

func (r *CsvReader) ReadBatch(
	ctx context.Context, buf *batch.Batch,
	proc *process.Process, analyzer process.Analyzer,
) (fileFinished bool, err error) {
	_, span := trace.Start(ctx, "CsvReader.ReadBatch")
	defer span.End()

	return r.makeBatchRows(proc, buf)
}

func (r *CsvReader) Close() error {
	r.prevStr = ""
	r.pending = nil
	r.pendingRawBytes = 0
	r.rowArea = nil
	r.plh = nil
	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		return err
	}
	return nil
}

// makeBatchRows reads rows from CSV/JSONLINE into the batch.
// Migrated from external.go makeBatchRows, with state from CsvReader.
func (r *CsvReader) makeBatchRows(proc *process.Process, bat *batch.Batch) (fileFinished bool, err error) {
	param := r.param
	ctx := proc.Ctx
	csvReader := r.plh.csvReader
	var curBatchSize uint64
	var finish bool
	var row []csvparser.Field
	var unexpectEOF bool
	unit := param.maxBatchSize
	if unit == 0 || unit > 64<<20 {
		unit = 64 << 20
	}
	if limit := proc.GetLim().BatchSize; limit > 0 && uint64(limit) < unit {
		unit = uint64(limit)
	}
	// Admission may lower the query's unit independently of the RPC batch
	// limit. Raw records and typed capacities must use that same admitted unit.
	if memory := proc.Base.ImportMemory; memory != nil && memory.UnitBytes > 0 {
		unit = min(unit, uint64(memory.UnitBytes))
	}
	// A previous singleton is permitted to exceed the normal batching target.
	// Do not make its oversized capacity the baseline for all following rows.
	if bat.RowCount() == 0 && uint64(bat.Allocated()) > unit {
		bat.FreeColumns(proc.Mp())
	}
	// Backing capacity is unchanged between growths. Derive this frontier
	// afresh for every fill so Reset/reuse cannot retain stale capacity.
	allocated, dataRows := 0, 0
	refreshCapacity := func() {
		allocated, dataRows = bat.Allocated(), OneBatchMaxRow
		for _, vec := range bat.Vecs {
			width := vec.GetType().TypeSize()
			if width <= 0 {
				dataRows = 0
				break
			}
			dataRows = min(dataRows, cap(vec.GetData())/width)
		}
	}
	refreshCapacity()

	for i := 0; i < OneBatchMaxRow; i++ {
		select {
		case <-ctx.Done():
			finish = true
		default:
		}
		pending := r.pending != nil
		if !finish && pending {
			row = r.pending
			r.pending = nil
		} else if !finish {
			row, err = csvReader.Read(csvReader.LastRow)
			csvReader.LastRow = row
			if err != nil {
				if err == io.EOF {
					finish = true
				} else {
					return false, err
				}
			}
		}

		if finish {
			r.pending = nil
			r.pendingRawBytes = 0
			closeErr := r.reader.Close()
			if closeErr != nil {
				logutil.Errorf("close file failed. err:%v", closeErr)
				return false, closeErr
			}
			r.reader = nil
			r.plh = nil
			fileFinished = true
			break
		}

		if r.ignoreLine > 0 {
			if !param.Extern.Parallel || param.FileOffset[0] == 0 {
				r.ignoreLine--
				i--
				continue
			}
		}

		var rawBytes uint64
		if pending {
			rawBytes = r.pendingRawBytes
			r.pendingRawBytes = 0
		} else {
			for j := range row {
				rawBytes += uint64(len(row[j].Val))
			}
		}

		rowIdx := i
		if !pending && param.Extern.Format == tree.JSONLINE {
			row, err = r.transJson2Lines(proc.Ctx, row[0].Val, param.Attrs, param.Cols, param.Extern.JsonData)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					logutil.Infof("unexpected EOF, wait for next batch")
					unexpectEOF = true
					continue
				}
				return false, err
			}
		}

		// Test both raw input and the complete new typed capacities before the
		// append. A resize's old allocation remains live while N is allocated.
		// A single row may exceed the target; largest-entry-limit remains the
		// existing support boundary, rather than introducing a new row limit.
		var areaGrowth bool
		r.rowArea, areaGrowth = csvRowAreas(r.rowArea, row, param, bat)
		mayGrow := bat.Vecs[0].Length() >= dataRows || areaGrowth
		peak := int64(allocated)
		if mayGrow {
			for j, vec := range bat.Vecs {
				peak += vec.AppendAllocation(1, r.rowArea[j])
			}
		}
		if bat.Vecs[0].Length() > 0 && (curBatchSize+rawBytes > unit || uint64(peak) > unit) {
			r.pending = row
			r.pendingRawBytes = rawBytes
			break
		}

		if err = getOneRowData(proc, bat, row, rowIdx, param); err != nil {
			r.pending = nil
			r.pendingRawBytes = 0
			return false, err
		}
		curBatchSize += rawBytes
		if mayGrow {
			refreshCapacity()
		}

		if curBatchSize >= unit || uint64(allocated) >= unit {
			break
		}
	}

	n := bat.Vecs[0].Length()
	if unexpectEOF && n > 0 {
		n--
		for i := 0; i < bat.VectorCount(); i++ {
			vec := bat.GetVector(int32(i))
			vec.SetLength(n)
		}
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return fileFinished, nil
}

// csvRowAreas bounds the out-of-line typed payload of one row without parsing
// it a second time. Fixed-width storage and inline varlena bytes are charged by
// AppendAllocation. Array literals can expand from one digit to eight bytes;
// JSON's type tags, offsets and number representation are conservatively
// bounded here rather than treating its raw text size as its binary size.
func csvRowAreas(dst []int, row []csvparser.Field, param *ExternalParam, bat *batch.Batch) ([]int, bool) {
	if cap(dst) < len(bat.Vecs) {
		dst = make([]int, len(bat.Vecs))
	} else {
		dst = dst[:len(bat.Vecs)]
		clear(dst)
	}
	var mayGrow bool
	for _, attr := range param.Attrs {
		if attr.ColIndex < 0 || int(attr.ColIndex) >= len(bat.Vecs) {
			continue
		}
		field := getFieldFromLine(row, attr.ColName, param, attr.ColFieldIndex)
		if field.IsNull || getNullFlag(param.Extern.NullMap, attr.ColName, field.Val) {
			continue
		}
		vec := bat.Vecs[attr.ColIndex]
		if !vec.GetType().IsVarlen() {
			continue
		}
		n := len(field.Val)
		if !param.ParallelLoad {
			switch vec.GetType().Oid {
			case types.T_array_float32, types.T_array_float64:
				text := strings.TrimSpace(field.Val)
				if text == "[]" {
					n = 0
				} else {
					n = (strings.Count(text, ",") + 1) * 4
					if vec.GetType().Oid == types.T_array_float64 {
						n *= 2
					}
				}
			case types.T_json:
				if param.Extern.Format == tree.CSV {
					n = 16*n + 64
				}
			}
		}
		if n > types.VarlenaInlineSize {
			dst[attr.ColIndex] = n
			backing := vec.GetArea()
			mayGrow = mayGrow || n > cap(backing)-len(backing)
		}
	}
	return dst, mayGrow
}

// transJson2Lines converts JSON string to CSV fields.
// Uses r.prevStr instead of param.prevStr.
func (r *CsvReader) transJson2Lines(ctx context.Context, str string, attrs []plan.ExternAttr, cols []*plan.ColDef, jsonData string) ([]csvparser.Field, error) {
	switch jsonData {
	case tree.OBJECT:
		return r.transJsonObject2Lines(ctx, str, attrs, cols)
	case tree.ARRAY:
		return r.transJsonArray2Lines(ctx, str, attrs, cols)
	default:
		return nil, moerr.NewNotSupportedf(ctx, "the jsonline format '%s' is not support now", jsonData)
	}
}

func (r *CsvReader) transJsonObject2Lines(ctx context.Context, str string, attrs []plan.ExternAttr, cols []*plan.ColDef) ([]csvparser.Field, error) {
	var (
		err error
		res = make([]csvparser.Field, 0, len(attrs))
	)
	if config.LargestEntryLimit > 0 && len(str) > config.LargestEntryLimit-len(r.prevStr) {
		r.prevStr = ""
		return nil, moerr.NewInternalError(ctx, "size of row cannot exceed the max value of txn-entry-size-limit")
	}
	if r.prevStr != "" {
		str = r.prevStr + str
		r.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		r.prevStr = str
		return nil, err
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || !g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a object")
	}
	if len(g.Keys) < getRealAttrCnt(attrs) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, attr := range attrs {
		if cols[idx].Hidden {
			continue
		}
		ki := slices.Index(g.Keys, attr.ColName)
		if ki < 0 {
			return nil, moerr.NewInvalidInputf(ctx, "the attr %s is not in json", attr.ColName)
		}

		valN := g.Values[ki]
		if valN.V == nil {
			res = append(res, csvparser.Field{IsNull: true})
			continue
		}

		tp := cols[idx].Typ.Id
		if tp == int32(types.T_json) {
			data, err := valN.ByteJsonRaw()
			if err != nil {
				return nil, err
			}
			res = append(res, csvparser.Field{Val: string(data)})
			continue
		}

		val := fmt.Sprint(valN)
		res = append(res, csvparser.Field{Val: val, IsNull: val == JsonNull})
	}
	return res, nil
}

func (r *CsvReader) transJsonArray2Lines(ctx context.Context, str string, attrs []plan.ExternAttr, cols []*plan.ColDef) ([]csvparser.Field, error) {
	var (
		err error
		res = make([]csvparser.Field, 0, len(attrs))
	)
	if config.LargestEntryLimit > 0 && len(str) > config.LargestEntryLimit-len(r.prevStr) {
		r.prevStr = ""
		return nil, moerr.NewInternalError(ctx, "size of row cannot exceed the max value of txn-entry-size-limit")
	}
	if r.prevStr != "" {
		str = r.prevStr + str
		r.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		r.prevStr = str
		return nil, err
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a json array")
	}
	if len(g.Values) < getRealAttrCnt(attrs) {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, valN := range g.Values {
		if idx >= len(cols) {
			return nil, moerr.NewInvalidInput(ctx, str+" , wrong number of colunms")
		}

		if valN.V == nil {
			res = append(res, csvparser.Field{IsNull: true})
			continue
		}

		tp := cols[idx].Typ.Id
		if tp == int32(types.T_json) {
			data, err := valN.ByteJsonRaw()
			if err != nil {
				return nil, err
			}
			res = append(res, csvparser.Field{Val: string(data)})
			continue
		}

		val := fmt.Sprint(valN)
		res = append(res, csvparser.Field{Val: val, IsNull: val == JsonNull})
	}
	return res, nil
}
