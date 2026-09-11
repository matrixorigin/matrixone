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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// provenanceShape memoizes, per index metadata table, whether it carries the appended provenance
// columns. BOTH answers are cached, and both EXPIRE.
//
// Neither answer is permanent, which is what a cache-forever got wrong in both directions:
//
//   - "has them" is not permanent. The memo is keyed by qualified name, and a RESTORE / PITR /
//     CLONE can put a generation from before the v4_0_7 widening back under that same name. A
//     memo that never expires keeps answering true, every writer keeps naming build_ts, and
//     every CDC flush and rebuild for that index fails with "unknown column" until someone
//     restarts the CN.
//   - "not yet" was not cached at all, so an index whose tenant has not migrated paid a
//     mo_catalog.mo_columns round-trip on EVERY flush, forever -- a catalog query in the CDC
//     write path, which is the one place it should not be.
//
// A TTL fixes both without a distributed invalidation: writers re-probe once per interval per
// table instead of once per write, and any shape change -- a migration widening the table, or a
// restore narrowing it -- is picked up within one interval.
var provenanceShape sync.Map // "db.table" -> provenanceShapeEntry

// provenanceShapeTTL bounds how long a writer may act on a remembered shape. Short enough that a
// restored table heals on its own well inside an operator's attention span, long enough that the
// catalog probe is amortized across the flushes of a busy index rather than paid by each one.
const provenanceShapeTTL = time.Minute

type provenanceShapeEntry struct {
	has     bool
	fetched time.Time
}

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
// A read failure answers false for the same reason -- degrade to the shape that works on both --
// and is NOT memoized, so the next writer retries rather than inheriting a guess.
func HasProvenanceColumns(sqlproc *SqlProcess, db, table, col string) (ok bool) {
	if db == "" || table == "" || col == "" {
		return false
	}
	key := db + "." + table
	if v, loaded := provenanceShape.Load(key); loaded {
		if e, cast := v.(provenanceShapeEntry); cast && time.Since(e.fetched) < provenanceShapeTTL {
			return e.has
		}
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
	// A definite answer either way is memoized; a failed or panicking probe is NOT, so the next
	// writer retries rather than inheriting a guess.
	if count == 0 {
		provenanceShape.Store(key, provenanceShapeEntry{has: false, fetched: time.Now()})
		return false
	}
	provenanceShape.Store(key, provenanceShapeEntry{has: true, fetched: time.Now()})
	return true
}

// MarkProvenanceColumns records that db.table is known to carry the provenance columns, so
// writers skip the catalog probe until the memo expires. A caller that just created the table
// knows this without asking -- CREATE INDEX builds the current shape.
func MarkProvenanceColumns(db, table string) {
	if db == "" || table == "" {
		return
	}
	provenanceShape.Store(db+"."+table, provenanceShapeEntry{has: true, fetched: time.Now()})
}

// ForgetProvenanceShape drops the memo for one table. For tests, and for a DROP that could see
// the same qualified name created again with a different shape.
func ForgetProvenanceShape(db, table string) {
	provenanceShape.Delete(db + "." + table)
}
