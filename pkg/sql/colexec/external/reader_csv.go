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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	if bat == nil || bat.VectorCount() == 0 {
		return false, moerr.NewInternalError(proc.Ctx, "external CSV reader requires at least one materialized column")
	}
	param := r.param
	ctx := proc.Ctx
	csvReader := r.plh.csvReader
	var curBatchSize uint64
	var finish bool
	var row []csvparser.Field

	for i := 0; i < OneBatchMaxRow; i++ {
		select {
		case <-ctx.Done():
			finish = true
		default:
		}
		if !finish {
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

		for j := 0; j < len(row); j++ {
			curBatchSize += uint64(len(row[j].Val))
		}

		// The physical line this record starts on, for __mo_file_line. Recorded
		// per record so a multi-line quoted field reports its FIRST line.
		if param.ErrorMode.WantLine {
			param.ErrorMode.RecordLine = csvReader.RecordLine()
		}

		rowIdx := i
		if param.Extern.Format == tree.JSONLINE {
			raw := row[0].Val
			if param.ErrorMode.Tolerate {
				// The text this record is parsed from, so a failure reports
				// the source line rather than the decoded fields.
				param.ErrorMode.RawText = r.prevStr + raw
			}
			row, err = r.transJson2Lines(proc.Ctx, raw, param.Attrs, param.Cols, param.Extern.JsonData)
			if err != nil {
				// transJson2Lines holds the text over in prevStr only when the
				// record is genuinely truncated; that -- not the EOF error
				// alone -- is what makes this a continuation.
				truncated := r.prevStr != ""
				if truncated && !param.ErrorMode.Tolerate {
					// Held over for the next line. Nothing is appended for
					// this record, so there is no partial row to trim once
					// the batch ends. (Rows used to be written into
					// pre-allocated slots, which is why the batch tail was
					// trimmed here; they are appended now.)
					logutil.Infof("unexpected EOF, wait for next batch")
					continue
				}
				if !param.ErrorMode.Tolerate {
					return false, err
				}
				if truncated {
					// Under tolerance a truncated object is THIS line's
					// failure, not a claim on the lines after it. Holding it
					// over would append every following line to it and report
					// one failure at EOF, losing every good record in between
					// -- and it would buy nothing: a continuation line that
					// starts with a quote makes the CSV splitter fail the
					// whole scan, so a multi-line object is not reliably
					// readable in the first place.
					r.prevStr = ""
					err = moerr.NewInvalidInput(proc.Ctx, "incomplete json record")
				}
				// A line that is not valid JSON is reported as a failed record
				// rather than failing the query.
				if err = appendErrorRow(proc, bat, nil, i, param, err); err != nil {
					return false, err
				}
				continue
			}
		}

		if err = getOneRowData(proc, bat, row, rowIdx, param); err != nil {
			return false, err
		}

		if curBatchSize >= param.maxBatchSize {
			break
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return fileFinished, nil
}

// jsonLineTruncated reports whether a failed parse means "truncated" rather
// than "malformed": the parser ran off the end of the text, and the text opens
// the expected value, so appending the next line can still complete it. Only
// such text is held over in prevStr. The prefix test matters because the
// parser reports a bare word ("this is not json") as an unexpected EOF too,
// and holding that over would glue it onto the following line.
func jsonLineTruncated(err error, str string, open byte) bool {
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	trimmed := strings.TrimSpace(str)
	return len(trimmed) > 0 && trimmed[0] == open
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
	resultSize := 0
	realCnt := 0
	for idx, attr := range attrs {
		if catalog.ContainExternalHidenCol(attr.ColName) || isKafkaSyntheticAttr(r.param, attr.ColName) ||
			isExternalErrorAttr(attr, cols) {
			continue
		}
		if idx >= len(cols) || cols[idx] == nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"missing external column definition for attribute %s", attr.ColName)
		}
		if cols[idx].Hidden {
			continue
		}
		if attr.ColFieldIndex < 0 || attr.ColFieldIndex >= int32(plan.TableColumnCountLimit) {
			return nil, moerr.NewInternalErrorf(ctx,
				"invalid external field index %d for column %s", attr.ColFieldIndex, attr.ColName)
		}
		resultSize = max(resultSize, int(attr.ColFieldIndex)+1)
		realCnt++
	}
	res := make([]csvparser.Field, resultSize)
	if r.prevStr != "" {
		str = r.prevStr + str
		r.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		logutil.Errorf("json unmarshal err:%v", err)
		if jsonLineTruncated(err, str, '{') {
			r.prevStr = str
			return nil, err
		}
		return nil, moerr.NewInvalidInput(ctx, "the line is not a well-formed json object")
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || !g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a object")
	}
	if len(g.Keys) < realCnt {
		return nil, moerr.NewInternalError(ctx, ColumnCntLargerErrorInfo)
	}
	for idx, attr := range attrs {
		if catalog.ContainExternalHidenCol(attr.ColName) || cols[idx].Hidden ||
			isKafkaSyntheticAttr(r.param, attr.ColName) || isExternalErrorAttr(attr, cols) {
			continue
		}
		ki := slices.Index(g.Keys, attr.ColName)
		if ki < 0 {
			return nil, moerr.NewInvalidInputf(ctx, "the attr %s is not in json", attr.ColName)
		}

		valN := g.Values[ki]
		if valN.V == nil {
			res[attr.ColFieldIndex] = csvparser.Field{IsNull: true}
			continue
		}

		tp := cols[idx].Typ.Id
		if tp == int32(types.T_json) {
			data, err := valN.ByteJsonRaw()
			if err != nil {
				return nil, err
			}
			res[attr.ColFieldIndex] = csvparser.Field{Val: string(data)}
			continue
		}

		val := fmt.Sprint(valN)
		res[attr.ColFieldIndex] = csvparser.Field{Val: val, IsNull: val == JsonNull}
	}
	return res, nil
}

func (r *CsvReader) transJsonArray2Lines(ctx context.Context, str string, attrs []plan.ExternAttr, cols []*plan.ColDef) ([]csvparser.Field, error) {
	var (
		err error
		res = make([]csvparser.Field, 0, len(attrs))
	)
	if r.prevStr != "" {
		str = r.prevStr + str
		r.prevStr = ""
	}
	jsonNode, err := bytejson.ParseNodeString(str)
	if err != nil {
		if jsonLineTruncated(err, str, '[') {
			r.prevStr = str
			return nil, err
		}
		return nil, moerr.NewInvalidInput(ctx, "the line is not a well-formed json array")
	}
	defer jsonNode.Free()
	g, ok := jsonNode.V.(*bytejson.Group)
	if !ok || g.Obj {
		return nil, moerr.NewInvalidInput(ctx, "not a json array")
	}
	if len(g.Values) < getRealAttrCnt(attrs, cols) {
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

// isExternalErrorAttr reports whether an attribute is one of the synthesized
// error-mode columns. They are produced by the scan, never matched against a
// JSON key, so they must not be counted as record fields either.
func isExternalErrorAttr(attr plan.ExternAttr, cols []*plan.ColDef) bool {
	var colId uint64
	if idx := int(attr.ColIndex); idx < len(cols) && cols[idx] != nil {
		colId = cols[idx].ColId
	}
	return catalog.IsExternalErrorCol(attr.ColName, colId)
}
