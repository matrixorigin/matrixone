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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// foreignScanKindESQL mirrors foreignext.KindESQL for the getColData fast path
// (a local const avoids re-evaluating a cross-package selector per field).
const foreignScanKindESQL = foreignext.KindESQL

// trimISO8601Zulu strips the trailing 'Z' from an ISO 8601 UTC datetime
// ("2026-01-15T10:20:30.123Z" -> "2026-01-15T10:20:30.123"), which MO's
// temporal parsers accept. Values without the T...Z shape pass through.
func trimISO8601Zulu(v string) string {
	if strings.HasSuffix(v, "Z") && strings.Contains(v, "T") {
		return v[:len(v)-1]
	}
	return v
}

// ForeignExternParam builds the synthetic tree.ExternParam an ESQL/SQL foreign
// external table scans with. Unlike the datastream INLINE param, the scan type
// is INFILE so the operator's file loop iterates FileList (the derived query
// texts) and publishes each into Fileparam.Filepath — which both routes the
// text to the reader and fills the hidden __mo_query column per row.
// The CSV dialect matches the foreigntvf sources: MySQL-style for SQL, RFC
// 4180 with one header line for ES|QL.
func ForeignExternParam(kind string) *tree.ExternParam {
	tail := new(tree.TailParameter)
	if kind == foreignext.KindESQL {
		tail.Fields = &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0}}
		tail.IgnoredLines = 1
	}
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Format:   tree.CSV,
			Tail:     tail,
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_FOREIGN_TB),
		},
	}
}

// ForeignScanReader is the ExternalFileReader of an ESQL/SQL foreign external
// table. Each "file" is one query text: Open resolves the session-cached
// foreign connection, sends the current Fileparam.Filepath as the query, and
// parses the returned CSV stream with the shared CSV machinery.
type ForeignScanReader struct {
	csv CsvReader
}

func NewForeignScanReader(param *ExternalParam) *ForeignScanReader {
	return &ForeignScanReader{}
}

func (r *ForeignScanReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	fs := param.ForeignScan
	if fs == nil {
		return false, moerr.NewInternalError(proc.Ctx, "foreign scan reader without scan metadata")
	}
	kind := foreigntvf.Kind(fs.Kind)

	cache, ok := proc.GetSession().(process.ForeignConnCache)
	if !ok {
		return false, moerr.NewInvalidInput(proc.Ctx, "an ESQL/SQL external table can only be read in an interactive session")
	}

	// Config resolution order: table option (inline JSON or env:NAME resolved
	// on the CN right now, so secrets never sat in the catalog) -> session
	// variable @esql_tvf_config / @sql_tvf_config -> error.
	configJSON, err := foreignext.ResolveConfig(proc.Ctx, fs.Config)
	if err != nil {
		return false, err
	}
	if configJSON == "" {
		if configJSON, err = foreigntvf.ConfigFromSessionVar(proc.Ctx, proc, kind); err != nil {
			return false, err
		}
	}
	conn, _, err := foreigntvf.ResolveOrConnect(proc.Ctx, cache, kind, configJSON)
	if err != nil {
		return false, err
	}

	queryText := param.Fileparam.Filepath
	stream, err := conn.Query(proc.Ctx, queryText)
	if err != nil {
		return false, err
	}
	parser, err := newCSVParserFromReader(param.Extern, stream)
	if err != nil {
		stream.Close()
		return false, err
	}
	r.csv.param = param
	r.csv.reader = stream
	r.csv.plh = &ParseLineHandler{csvReader: parser}
	// re-arm the header skip for every query (ES|QL sends one header line per
	// response)
	r.csv.ignoreTag = int(param.Extern.Tail.IgnoredLines)
	r.csv.ignoreLine = int(param.Extern.Tail.IgnoredLines)
	return false, nil
}

func (r *ForeignScanReader) ReadBatch(
	ctx context.Context, buf *batch.Batch,
	proc *process.Process, analyzer process.Analyzer,
) (fileFinished bool, err error) {
	return r.csv.makeBatchRows(proc, buf)
}

// Close releases the current stream (makeBatchRows already closed it if the
// stream reached EOF). The cached connection stays open: the session owns it.
func (r *ForeignScanReader) Close() error {
	if r.csv.reader != nil {
		err := r.csv.reader.Close()
		r.csv.reader = nil
		r.csv.plh = nil
		return err
	}
	return nil
}
