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

package iscp

import (
	"context"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"go.uber.org/zap"
)

const (
	ignoredSqlBatch   = 1024
	ignoredLogSamples = 5
)

// ignoredRows tracks, during one iteration, the source rows an index cannot store. The
// index's ignored table holds exactly the source rows that exist and are not in the index:
// a row is added when its value cannot be stored, and removed when it is deleted or
// replaced by a value the index stores. It never reads the table's rows: its memory is
// bounded by the iteration's own changes, never by the size of the table, and an iteration
// that rebuilds the index keeps the rows it ignores in a local file. More than limit rows in
// the table stop the job with a permanent error.
type ignoredRows struct {
	db     string
	tbl    string
	table  string
	pkType *types.Type
	limit  int64
	// count is the number of changes that record a row, and kept the number of rows a rebuild
	// stores.
	count, kept int64
	// firstPk and firstErr are the key and the error of the first row a rebuild cannot store.
	firstPk  any
	firstErr error
	// reset replaces the whole table, for an iteration that rebuilds the index; spill holds the
	// rows it ignores. Such an iteration reads the table as of one time, so each key once.
	reset bool
	spill *vectorindex.IgnoredSpill
	ctx   context.Context
	// stored reports whether the table had a row when the iteration started; while it had
	// none, a row the index stores needs no removal and is not recorded.
	stored  bool
	changes map[any]ignoredChange
	ts      types.TS
}

type ignoredChange struct {
	pk      any
	code    uint16
	ts      types.TS
	ignored bool
}

// ignoredKey returns a map key equal for equal primary keys.
func ignoredKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

// newIgnoredRows returns the tracker of the index whose hidden tables are indexdefs, or nil
// when the index has no ignored table.
func newIgnoredRows(dbName string, tableDef *plan.TableDef, indexdefs []*plan.IndexDef) (*ignoredRows, error) {
	for _, idx := range indexdefs {
		if idx.IndexAlgoTableType != catalog.IndexIgnored_TblType {
			continue
		}
		limit, err := catalog.IndexParamMaxBadRecords(idx.IndexAlgoParams)
		if err != nil {
			return nil, err
		}
		pk := tableDef.Cols[tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]].Typ
		return &ignoredRows{
			db:      dbName,
			tbl:     idx.IndexTableName,
			table:   sqlquote.QualifiedIdent(dbName, idx.IndexTableName),
			pkType:  &types.Type{Oid: types.T(pk.Id), Width: pk.Width, Scale: pk.Scale},
			limit:   limit,
			changes: make(map[any]ignoredChange),
		}, nil
	}
	return nil, nil
}

// setTs sets the commit time recorded for the rows ignored next.
func (g *ignoredRows) setTs(ts types.TS) {
	g.ts = ts
}

// ignore records that the row with key pk is not in the index because of err, a reason with an
// unstorable code. When the iteration rebuilds the index, one row more than the limit stops it.
func (g *ignoredRows) ignore(pk any, err error) error {
	code, _ := vectorindex.UnstorableCode(err)
	if !g.reset {
		key := ignoredKey(pk)
		if !g.changes[key].ignored {
			g.count++
		}
		g.changes[key] = ignoredChange{pk: pk, code: code, ts: g.ts, ignored: true}
		return nil
	}
	g.count++
	if g.firstErr == nil {
		g.firstPk, g.firstErr = pk, err
	}
	if g.count > g.limit {
		return NewPermanentError(moerr.NewIndexTooManyBadRecordsNoCtx(g.limit, fmt.Sprint(pk), err.Error()))
	}
	lit, cerr := convertColIntoSql(g.ctx, pk, g.pkType, nil)
	if cerr != nil {
		return cerr
	}
	return g.spill.Add(string(lit), code)
}

// indexed records that the row with key pk is in the index or no longer exists.
func (g *ignoredRows) indexed(pk any) {
	if g.reset {
		g.kept++
		return
	}
	key := ignoredKey(pk)
	c, ok := g.changes[key]
	if c.ignored {
		g.count--
	}
	if ok || g.stored {
		g.changes[key] = ignoredChange{pk: pk}
	}
}

// load starts an iteration: it reads whether the table has a row, or starts from an empty
// table, spilling to spillDir ("" is $TMPDIR), when the iteration rebuilds the index.
func (g *ignoredRows) load(ctx context.Context, service string, txn client.TxnOperator, rebuild bool, spillDir string) error {
	g.close()
	clear(g.changes)
	g.reset, g.stored, g.count, g.kept = rebuild, false, 0, 0
	g.firstPk, g.firstErr = nil, nil
	g.ctx = ctx
	if rebuild {
		g.spill = vectorindex.NewIgnoredSpill(spillDir)
		return nil
	}
	res, err := ExecWithResult(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", g.table), service, txn)
	if err != nil {
		return err
	}
	defer res.Close()
	for _, b := range res.Batches {
		if b != nil && b.RowCount() > 0 {
			g.stored = true
		}
	}
	return nil
}

// close removes the spill of the iteration.
func (g *ignoredRows) close() {
	if g.spill != nil {
		g.spill.Close()
		g.spill = nil
	}
}

// flush writes the changes of the iteration in the transaction that commits it. A table left
// with more rows than the limit, or a rebuild that stores none of the rows, stops the job.
func (g *ignoredRows) flush(ctx context.Context, service string, txn client.TxnOperator, indexName string) error {
	exec := func(sql string) error {
		res, err := ExecWithResult(ctx, sql, service, txn)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}
	if g.reset {
		defer g.close()
		if g.count > 0 && g.kept == 0 {
			return NewPermanentError(moerr.NewIndexAllRowsBadNoCtx(g.count, fmt.Sprint(g.firstPk), g.firstErr.Error()))
		}
		if err := exec(fmt.Sprintf("DELETE FROM %s WHERE TRUE", g.table)); err != nil {
			return err
		}
		cols := vectorindex.IgnoredColumns{Pk: catalog.IndexIgnored_TblCol_Pk, Code: catalog.IndexIgnored_TblCol_Code, Ts: catalog.IndexIgnored_TblCol_Ts}
		if err := g.spill.Sqls(g.db, g.tbl, cols, g.ts.Physical(), exec); err != nil {
			return err
		}
		if g.count > 0 {
			logutil.Warn("ISCP-Task index ignored rows",
				zap.String("index", indexName),
				zap.Int64("ignored", g.count),
				zap.String("first", fmt.Sprintf("%v: %v", g.firstPk, g.firstErr)))
		}
		return nil
	}
	if len(g.changes) == 0 {
		return nil
	}
	keys := make([]any, 0, len(g.changes))
	for k := range g.changes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })

	var removed, ignored []any
	for _, k := range keys {
		if g.changes[k].ignored {
			ignored = append(ignored, k)
		} else {
			removed = append(removed, k)
		}
	}

	for start := 0; start < len(removed); start += ignoredSqlBatch {
		end := min(start+ignoredSqlBatch, len(removed))
		var buf []byte
		buf = appendString(buf, fmt.Sprintf("DELETE FROM %s WHERE `%s` IN (", g.table, catalog.IndexIgnored_TblCol_Pk))
		for i, k := range removed[start:end] {
			if i > 0 {
				buf = appendString(buf, ",")
			}
			var err error
			if buf, err = convertColIntoSql(ctx, g.changes[k].pk, g.pkType, buf); err != nil {
				return err
			}
		}
		buf = appendString(buf, ")")
		if err := exec(string(buf)); err != nil {
			return err
		}
	}
	rows := make([]vectorindex.IgnoredRow, 0, len(ignored))
	for _, k := range ignored {
		lit, err := convertColIntoSql(ctx, g.changes[k].pk, g.pkType, nil)
		if err != nil {
			return err
		}
		c := g.changes[k]
		rows = append(rows, vectorindex.IgnoredRow{Pk: string(lit), Code: c.code, Ts: c.ts.Physical()})
	}
	for _, sql := range vectorindex.IgnoredRowsSql(g.db, g.tbl, vectorindex.IgnoredColumns{Pk: catalog.IndexIgnored_TblCol_Pk, Code: catalog.IndexIgnored_TblCol_Code, Ts: catalog.IndexIgnored_TblCol_Ts}, rows) {
		if err := exec(sql); err != nil {
			return err
		}
	}
	if len(ignored) > 0 {
		n, err := g.rowCount(ctx, service, txn)
		if err != nil {
			return err
		}
		if n > g.limit {
			c := g.changes[ignored[0]]
			return NewPermanentError(moerr.NewIndexTooManyBadRecordsNoCtx(g.limit, fmt.Sprint(c.pk), moerr.MessageOf(c.code)))
		}
	}

	if len(ignored) > 0 {
		samples := make([]string, 0, ignoredLogSamples)
		for _, k := range ignored[:min(len(ignored), ignoredLogSamples)] {
			samples = append(samples, fmt.Sprintf("%v: %d", k, g.changes[k].code))
		}
		logutil.Warn("ISCP-Task index ignored rows",
			zap.String("index", indexName),
			zap.Int("ignored", len(ignored)),
			zap.Int("cleared", len(removed)),
			zap.Strings("samples", samples))
	}
	return nil
}

// rowCount returns the number of rows in the table.
func (g *ignoredRows) rowCount(ctx context.Context, service string, txn client.TxnOperator) (int64, error) {
	res, err := ExecWithResult(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", g.table), service, txn)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, b := range res.Batches {
		if b != nil && b.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0), nil
		}
	}
	return 0, nil
}
