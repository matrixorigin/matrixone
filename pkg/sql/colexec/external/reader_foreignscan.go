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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqldatastream "github.com/matrixorigin/matrixone/pkg/sql/datastream"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// foreignScanKindESQL mirrors foreignext.KindESQL for the getColData fast path
// (a local const avoids re-evaluating a cross-package selector per field).
const foreignScanKindESQL = foreignext.KindESQL

// normalizeISO8601Zulu converts an ISO 8601 UTC datetime ("...T...Z", the
// format ES|QL CSV emits) into the equivalent wall-clock text in loc,
// PRESERVING THE INSTANT: MO's temporal parsers interpret zone-less text in
// the session time zone, so merely stripping the 'Z' would shift the stored
// instant by the session offset. Values without the T...Z shape (or that fail
// RFC 3339 parsing) pass through unchanged and take the ordinary parse path.
func normalizeISO8601Zulu(v string, loc *time.Location) string {
	if !strings.HasSuffix(v, "Z") || !strings.Contains(v, "T") {
		return v
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return v
	}
	if loc == nil {
		loc = time.UTC
	}
	return t.In(loc).Format("2006-01-02 15:04:05.999999999")
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

	// Config resolution order: the table option (inline JSON) -> session
	// variable @esql_tvf_config / @sql_tvf_config -> error. User input or
	// session only; query processing never reads the CN process environment.
	configJSON := fs.Config
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
	sentText, pushed := pushdownQueryText(proc, conn, fs, param, queryText)
	stream, err := conn.Query(proc.Ctx, sentText)
	if err != nil {
		if pushed {
			// The probe already ran this text's shape as a derived table, so a
			// failure here is the source's own, not the wrapper's -- but say
			// which text failed, or the user reads an error about a query they
			// did not write.
			return false, moerr.NewInvalidInputf(proc.Ctx,
				"sql external table query failed with predicate pushdown on: %v (query sent: %s)", err, sentText)
		}
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

// pushdownQueryText decides what MO actually sends to a SQL source for one
// query text: either the user's text verbatim, or that text wrapped as a
// derived table filtered by the conjuncts compile offered.  The bool reports
// which.
//
// The hard part is names.  A foreign scan maps the source's result onto the
// declared columns BY POSITION, so `select a_id, a_name from src` legitimately
// feeds columns declared (id, name) and the source may have no column called
// `id` at all.  A WHERE clause, however, must name what it filters.  So MO
// asks the source: it probes the wrapped-but-unfiltered query for its column
// names, matches them to the declared columns positionally -- the very
// correspondence the scan already relies on -- and renders the predicate in
// the source's vocabulary.
//
// Everything here is best-effort.  The conjuncts stay in the scan's own filter
// list, so if the connection cannot be probed, the query cannot be a derived
// table, the arity disagrees, or nothing renders, MO sends the verbatim text
// and filters the rows itself: slower, never wrong.
func pushdownQueryText(
	proc *process.Process, conn foreigntvf.Conn,
	fs *plan.ForeignScan, param *ExternalParam, queryText string,
) (string, bool) {
	if fs == nil || !fs.Pushdown || len(fs.PushdownFilters) == 0 {
		return queryText, false
	}
	prober, ok := conn.(foreigntvf.PushdownProber)
	if !ok {
		return queryText, false
	}
	declared := foreignDeclaredCols(param)
	if len(declared) == 0 {
		return queryText, false
	}

	// Render once against MO's own names first. It costs no I/O and answers
	// "is anything pushable at all?", so a scan whose predicates the deparser
	// cannot express never pays for a round trip.
	if text, _ := sqldatastream.DeparseFilters(
		fs.PushdownFilters, declared, proc.GetSessionInfo().TimeZone); text == "" {
		return queryText, false
	}

	names, err := prober.ProbeColumns(proc.Ctx, foreignext.WrapPushdownProbe(queryText))
	if err != nil || len(names) != len(declared) {
		// An arity mismatch is not this function's error to raise: the scan
		// itself reports it, against the verbatim query, in the terms the user
		// already knows ("the data of row 1 contained is not equal to input
		// columns").
		return queryText, false
	}
	remote := make([]*plan.ColDef, len(declared))
	for i, name := range names {
		if name == "" {
			return queryText, false
		}
		// Only the name is substituted: the type stays the declared one, which
		// is what the deparser consults to render literals.
		col := *declared[i]
		col.Name = name
		col.OriginName = name
		remote[i] = &col
	}

	filter, _ := sqldatastream.DeparseFilters(fs.PushdownFilters, remote, proc.GetSessionInfo().TimeZone)
	if filter == "" {
		return queryText, false
	}
	return foreignext.WrapPushdownQuery(queryText, filter), true
}

// foreignDeclaredCols returns the scan's declared columns in DDL order -- the
// columns the source produces, positionally. The synthetic columns
// (__mo_query, the error-mode columns) are appended after them and are not
// part of the source's result.
func foreignDeclaredCols(param *ExternalParam) []*plan.ColDef {
	out := make([]*plan.ColDef, 0, len(param.Cols))
	for _, col := range param.Cols {
		if col == nil || catalog.IsReservedExternalColName(col.Name) {
			break
		}
		out = append(out, col)
	}
	return out
}
