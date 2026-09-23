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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	// A table that opted into predicate pushdown has a name contract with its
	// source (see checkPushedColumns), so its result columns are checked. That
	// needs a connection that can report them; one that cannot is a source MO
	// must not have narrowed in the first place.
	var stream io.ReadCloser
	if fs.Pushdown && kind == foreigntvf.KindSQL {
		named, ok := conn.(foreigntvf.NamedQuerier)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx,
				"this connection cannot report the columns a query returns, which predicate pushdown ('pushdown' = 'true') requires")
		}
		var names []string
		if stream, names, err = named.QueryNamed(proc.Ctx, queryText); err != nil {
			return false, err
		}
		if err = checkPushedColumns(proc.Ctx, names, param); err != nil {
			stream.Close()
			return false, err
		}
	} else if stream, err = conn.Query(proc.Ctx, queryText); err != nil {
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

// checkPushedColumns enforces the claim a table makes when it opts into
// predicate pushdown: that the source's result columns ARE the declared ones.
//
// A foreign scan normally maps the result by POSITION and never reads the
// source's column names, so `select a_id, a_name from src` may legitimately
// feed columns declared (id, name).  Pushdown cannot live with that freedom:
// a WHERE clause has to name what it filters, and the only names MO has are
// its own.  So MO writes the declared names into the SQL it sends, and checks
// here that the source answered with those same columns.  Comparison is
// case-insensitive: dialects differ on identifier case, and the CSV that
// follows is positional anyway.
//
// A mismatch has to be an error rather than a silent fallback: the source has
// already run a query MO narrowed, and MO cannot tell whether the rows it is
// about to read were filtered on the columns it meant.
//
// Both inputs survive column pruning, which is why they are used instead of
// param.Cols: ColumnListLen is the count of DECLARED columns (the map it comes
// from is built before the synthetic ones are appended, and before any
// pruning), and each attr carries its own declared position in ColFieldIndex.
// A pruned scan therefore checks the columns it actually reads, each at its
// true position in the source's result.
func checkPushedColumns(ctx context.Context, got []string, param *ExternalParam) error {
	if len(got) != int(param.ColumnListLen) {
		return moerr.NewInvalidInputf(ctx,
			"predicate pushdown ('pushdown' = 'true') requires the query to return the table's declared columns, but it returned %d column(s) for %d declared",
			len(got), param.ColumnListLen)
	}
	for _, attr := range param.Attrs {
		// the synthetic columns are MO's own; the source knows nothing of them
		if catalog.IsReservedExternalColName(attr.ColName) {
			continue
		}
		idx := int(attr.ColFieldIndex)
		if idx < 0 || idx >= len(got) {
			return moerr.NewInvalidInputf(ctx,
				"predicate pushdown ('pushdown' = 'true') cannot place declared column %q at result position %d of %d",
				attr.ColName, idx+1, len(got))
		}
		if !strings.EqualFold(got[idx], attr.ColName) {
			return moerr.NewInvalidInputf(ctx,
				"predicate pushdown ('pushdown' = 'true') requires the query to return the table's declared columns, but column %d is named %q at the source and %q in the table; alias it, or drop the option to keep the positional mapping",
				idx+1, got[idx], attr.ColName)
		}
	}
	return nil
}
