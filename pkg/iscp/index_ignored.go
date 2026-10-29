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
// replaced by a value the index stores.
type ignoredRows struct {
	db     string
	tbl    string
	table  string
	pkType *types.Type
	// reset replaces the whole table, for an iteration that rebuilds the index.
	reset bool
	// repr is how the paired writer receives primary keys.
	repr ValueRepr
	// present holds the keys in the table when the iteration started.
	present map[any]struct{}
	changes map[any]ignoredChange
	ts      types.TS
}

type ignoredChange struct {
	pk      any
	reason  string
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
func newIgnoredRows(dbName string, tableDef *plan.TableDef, indexdefs []*plan.IndexDef) *ignoredRows {
	for _, idx := range indexdefs {
		if idx.IndexAlgoTableType != catalog.IndexIgnored_TblType {
			continue
		}
		pk := tableDef.Cols[tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]].Typ
		return &ignoredRows{
			db:      dbName,
			tbl:     idx.IndexTableName,
			table:   sqlquote.QualifiedIdent(dbName, idx.IndexTableName),
			pkType:  &types.Type{Oid: types.T(pk.Id), Width: pk.Width, Scale: pk.Scale},
			changes: make(map[any]ignoredChange),
		}
	}
	return nil
}

// setTs sets the commit time recorded for the rows ignored next.
func (g *ignoredRows) setTs(ts types.TS) {
	g.ts = ts
}

// ignore records that the row with key pk is not in the index, and why.
func (g *ignoredRows) ignore(pk any, reason string) {
	g.changes[ignoredKey(pk)] = ignoredChange{pk: pk, reason: reason, ts: g.ts, ignored: true}
}

// indexed records that the row with key pk is in the index or no longer exists.
func (g *ignoredRows) indexed(pk any) {
	key := ignoredKey(pk)
	if _, ok := g.changes[key]; ok {
		g.changes[key] = ignoredChange{pk: pk}
		return
	}
	if _, ok := g.present[key]; ok {
		g.changes[key] = ignoredChange{pk: pk}
	}
}

// load reads the keys of the table, or starts from an empty table when the iteration
// rebuilds the index.
func (g *ignoredRows) load(ctx context.Context, service string, txn client.TxnOperator, rebuild bool) error {
	g.present = make(map[any]struct{})
	clear(g.changes)
	g.reset = rebuild
	if rebuild {
		return nil
	}
	sql := fmt.Sprintf("SELECT `%s` FROM %s", catalog.IndexIgnored_TblCol_Pk, g.table)
	res, err := ExecWithResult(ctx, sql, service, txn)
	if err != nil {
		return err
	}
	defer res.Close()
	row := make([]any, 1)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if err = extractRowFromVector(ctx, cols[0], 0, row, i, g.repr); err != nil {
				return false
			}
			g.present[ignoredKey(row[0])] = struct{}{}
		}
		return true
	})
	return err
}

// flush writes the changes of the iteration in the transaction that commits it.
func (g *ignoredRows) flush(ctx context.Context, service string, txn client.TxnOperator, indexName string) error {
	if !g.reset && len(g.changes) == 0 {
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
		} else if !g.reset {
			removed = append(removed, k)
		}
	}

	exec := func(sql string) error {
		res, err := ExecWithResult(ctx, sql, service, txn)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}

	if g.reset {
		if err := exec(fmt.Sprintf("DELETE FROM %s WHERE TRUE", g.table)); err != nil {
			return err
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
		rows = append(rows, vectorindex.IgnoredRow{Pk: string(lit), Reason: c.reason, Ts: c.ts.Physical()})
	}
	for _, sql := range vectorindex.IgnoredRowsSql(g.db, g.tbl, vectorindex.IgnoredColumns{Pk: catalog.IndexIgnored_TblCol_Pk, Reason: catalog.IndexIgnored_TblCol_Reason, Ts: catalog.IndexIgnored_TblCol_Ts}, rows) {
		if err := exec(sql); err != nil {
			return err
		}
	}

	if len(ignored) > 0 {
		samples := make([]string, 0, ignoredLogSamples)
		for _, k := range ignored[:min(len(ignored), ignoredLogSamples)] {
			samples = append(samples, fmt.Sprintf("%v: %s", k, g.changes[k].reason))
		}
		logutil.Warn("ISCP-Task index ignored rows",
			zap.String("index", indexName),
			zap.Int("ignored", len(ignored)),
			zap.Int("cleared", len(removed)),
			zap.Strings("samples", samples))
	}
	return nil
}
