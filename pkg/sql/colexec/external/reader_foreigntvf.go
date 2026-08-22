// Copyright 2026 Matrix Origin
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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ForeignTVFSource selects the CSV dialect of a foreign-TVF result stream.
type ForeignTVFSource int

const (
	// ForeignTVFSourceSQL is the MySQL-compatible dialect produced by the
	// sql_tvf row encoder: backslash-escaped, `\N` for NULL, no header line.
	ForeignTVFSourceSQL ForeignTVFSource = iota
	// ForeignTVFSourceESQL is the RFC 4180 dialect Elasticsearch ES|QL emits
	// for `format=csv`: double-quote-doubled (no backslash escaping), a single
	// header line, and an empty field for NULL. An empty numeric field is
	// materialized as NULL by getColData's trim path; an empty string column
	// stays an empty string (CSV cannot distinguish NULL from "" for text).
	ForeignTVFSourceESQL
)

// BuildForeignTVFExternParam builds the ExternalParam a ForeignTVFReader scans
// with. outCols are the operator's OUTPUT columns (possibly pruned/reordered by
// the optimizer); fullSchemaNames is the ORIGINAL declared column order, which
// fixes the CSV field positions. Each output column is mapped back to its
// original field index by name, so projection/pruning never misaligns columns.
// It reuses the ordinary external CSV path, so type coercion and NULL handling
// are identical to a CSV external table.
func BuildForeignTVFExternParam(proc *process.Process, outCols []*plan.ColDef, fullSchemaNames []string, src ForeignTVFSource) *ExternalParam {
	nameToField := make(map[string]int, len(fullSchemaNames))
	for i, nm := range fullSchemaNames {
		nameToField[nm] = i
	}
	attrs := make([]plan.ExternAttr, len(outCols))
	for j, col := range outCols {
		fieldIdx, ok := nameToField[col.Name]
		if !ok {
			// No declared schema (single JSON column) or an unmatched name:
			// fall back to positional mapping.
			fieldIdx = j
		}
		attrs[j] = plan.ExternAttr{
			ColName:       col.Name,
			ColIndex:      int32(j),
			ColFieldIndex: int32(fieldIdx),
		}
	}
	columnListLen := len(fullSchemaNames)
	if columnListLen == 0 {
		columnListLen = len(outCols)
	}

	tail := new(tree.TailParameter)
	if src == ForeignTVFSourceESQL {
		// RFC 4180: disable backslash escaping (EscapedBy value 0 -> "") so
		// embedded quotes are read as doubled quotes, and skip the header row.
		tail.Fields = &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0}}
		tail.IgnoredLines = 1
	}

	extern := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   tree.CSV,
			Tail:     tail,
		},
	}

	param := &ExternalParam{}
	param.Cols = outCols
	param.Attrs = attrs
	param.ColumnListLen = int32(columnListLen)
	param.Extern = extern
	// Non-strict, non-parallel single virtual "file": takes the simple row
	// path in getOneRowData and skips the header on every batch.
	param.StrictSqlMode = false

	if proc.GetLim().MaxMsgSize == 0 {
		param.maxBatchSize = uint64(morpc.GetMessageSize())
	} else {
		param.maxBatchSize = proc.GetLim().MaxMsgSize
	}
	param.maxBatchSize = uint64(float64(param.maxBatchSize) * 0.6)
	return param
}

// ForeignTVFReader materializes batches from a foreign-TVF CSV byte stream
// (an ES|QL response body or an encoded sql.Rows stream) using the shared
// external CSV machinery. It owns the stream and closes it.
type ForeignTVFReader struct {
	csv CsvReader
}

// NewForeignTVFReader wraps an already-open CSV byte stream. param must come
// from BuildForeignTVFExternParam. Ownership of stream transfers to the reader.
func NewForeignTVFReader(param *ExternalParam, stream io.ReadCloser) (*ForeignTVFReader, error) {
	parser, err := newCSVParserFromReader(param.Extern, stream)
	if err != nil {
		return nil, err
	}
	r := &ForeignTVFReader{}
	r.csv.param = param
	r.csv.reader = stream
	r.csv.plh = &ParseLineHandler{csvReader: parser}
	r.csv.ignoreTag = int(param.Extern.Tail.IgnoredLines)
	r.csv.ignoreLine = int(param.Extern.Tail.IgnoredLines)
	return r, nil
}

// ReadBatch fills buf with the next rows, returning finished=true after the
// stream is exhausted (at which point the stream has been closed).
func (r *ForeignTVFReader) ReadBatch(
	ctx context.Context, buf *batch.Batch, proc *process.Process, analyzer process.Analyzer,
) (finished bool, err error) {
	return r.csv.makeBatchRows(proc, buf)
}

// Close releases the underlying stream if makeBatchRows has not already done so
// (it closes the reader itself on EOF). Safe to call more than once.
func (r *ForeignTVFReader) Close() error {
	if r.csv.reader != nil {
		err := r.csv.reader.Close()
		r.csv.reader = nil
		r.csv.plh = nil
		return err
	}
	return nil
}

// ForeignTVFRawReader reads a foreign-TVF CSV stream row by row as raw string
// fields, for the schema-less esql_tvf / sql_tvf path that returns one JSON
// array column per row. It uses the same CSV dialect as ForeignTVFReader.
type ForeignTVFRawReader struct {
	parser *csvparser.CSVParser
	reader io.ReadCloser
	ignore int
}

// NewForeignTVFRawReader wraps an open CSV byte stream. param must come from
// BuildForeignTVFExternParam (its Extern selects the dialect and header skip).
func NewForeignTVFRawReader(param *ExternalParam, stream io.ReadCloser) (*ForeignTVFRawReader, error) {
	parser, err := newCSVParserFromReader(param.Extern, stream)
	if err != nil {
		return nil, err
	}
	return &ForeignTVFRawReader{
		parser: parser,
		reader: stream,
		ignore: int(param.Extern.Tail.IgnoredLines),
	}, nil
}

// ReadRow returns the next row's field values. ok is false at end of stream.
func (r *ForeignTVFRawReader) ReadRow() (fields []string, ok bool, err error) {
	for {
		row, rerr := r.parser.Read(r.parser.LastRow)
		r.parser.LastRow = row
		if rerr != nil {
			if rerr == io.EOF {
				return nil, false, nil
			}
			return nil, false, rerr
		}
		if r.ignore > 0 {
			r.ignore--
			continue
		}
		fields = make([]string, len(row))
		for i := range row {
			fields[i] = row[i].Val
		}
		return fields, true, nil
	}
}

// Close releases the underlying stream. Safe to call more than once.
func (r *ForeignTVFRawReader) Close() error {
	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		return err
	}
	return nil
}
