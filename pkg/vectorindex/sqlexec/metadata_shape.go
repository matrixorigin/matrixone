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

package sqlexec

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// provenanceShape memoizes, per index metadata table, whether it carries the appended provenance
// columns. Only a POSITIVE answer is cached: a table gains those columns when its tenant's
// v4_0_7 migration runs, so "not yet" is a transient answer, while "has them" is permanent --
// nothing drops them again.
var provenanceShape sync.Map // "db.table" -> struct{}

// HasProvenanceColumns reports whether db.table already carries col, the appended provenance
// column, and is how a writer stays correct during a rolling upgrade.
//
// The metadata tables are created per index at CREATE INDEX, so an index created before this
// feature keeps the four-column shape until its tenant's v4_0_7 migration widens it. That
// migration is asynchronous and per tenant, while a CN that has already been upgraded serves
// that tenant's CREATE/REINDEX/CDC immediately -- so between the two there is a window in which
// a new writer meets an old table. Writing the wide shape there fails the whole index build.
//
// Callers use the answer to decide whether to NAME the provenance columns in their INSERT.
// Getting it wrong in the conservative direction costs only provenance: the columns default to
// 0, which is already the documented "unknown" sentinel.
//
// A read failure answers false for the same reason -- degrade to the shape that works on both.
func HasProvenanceColumns(sqlproc *SqlProcess, db, table, col string) (ok bool) {
	if db == "" || table == "" || col == "" {
		return false
	}
	key := db + "." + table
	if _, ok := provenanceShape.Load(key); ok {
		return true
	}
	if sqlproc == nil {
		return false
	}

	// A shape probe must never be the thing that breaks a write. RunSql reaches the executor,
	// which panics rather than erroring when a process has no lock service (internal callers,
	// unit contexts), and the honest answer in that case is the same as for a failed read:
	// write the shape that works on both.
	defer func() {
		if r := recover(); r != nil {
			logutil.Warnf("index metadata shape: probing %s panicked, writing the legacy shape: %v", key, r)
			ok = false
		}
	}()

	sql := fmt.Sprintf(
		"select count(*) from %s.%s where att_database = %s and att_relname = %s and attname = %s",
		catalog.MO_CATALOG, catalog.MO_COLUMNS,
		sqlquote.String(db), sqlquote.String(table), sqlquote.String(col))

	res, err := RunSql(sqlproc, sql)
	if err != nil {
		logutil.Warnf("index metadata shape: reading %s columns failed, writing the legacy shape: %v", key, err)
		return false
	}
	defer res.Close()

	var count int64
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) == 0 {
			continue
		}
		counts := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
		for _, n := range counts {
			count += n
		}
	}
	if count == 0 {
		return false
	}
	provenanceShape.Store(key, struct{}{})
	return true
}

// MarkProvenanceColumns records that db.table is known to carry the provenance columns, so the
// next writer skips the catalog probe. A caller that just created the table knows this without
// asking -- CREATE INDEX builds the current shape.
func MarkProvenanceColumns(db, table string) {
	if db == "" || table == "" {
		return
	}
	provenanceShape.Store(db+"."+table, struct{}{})
}

// ForgetProvenanceShape drops the memo for one table. For tests, and for a DROP that could see
// the same qualified name created again with a different shape.
func ForgetProvenanceShape(db, table string) {
	provenanceShape.Delete(db + "." + table)
}
