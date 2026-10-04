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

package hnsw

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// indirected for unit tests.
var runSqlAutoCommit = sqlexec.RunSqlAutoCommit

// hnswGenerationSql returns the freshness-generation query for the VectorIndexCache cross-CN
// IsStale check: every model's file checksum (an MD5 of the model file). The generation is the
// MULTISET of those checksums — a pure CONTENT fingerprint, independent of any clock:
//
//   - any content change (a CDC append/delete rewrites a model file, or a REBUILD/MERGE) yields a
//     new checksum → the fingerprint changes;
//   - emptying a model deletes its checksum row → the fingerprint changes;
//   - a rebuild that reproduces byte-identical content yields the same checksums → the same
//     generation, which is correct (serving identical bytes is not stale).
//
// It deliberately does NOT use metadata.timestamp: a timestamp cannot survive an intermediate empty
// state (a zero-row rebuild deletes every row), so an (X,N)→(0,0)→(X,N) rebuild sequence under a
// reused/backward wall-clock could re-mint a generation a warm remote cache already holds. A content
// checksum makes different content a different generation regardless of the clock — closing that ABA.
func hnswGenerationSql(tblcfg vectorindex.IndexTableConfig) string {
	return fmt.Sprintf("SELECT %s FROM %s",
		catalog.Hnsw_TblCol_Metadata_Checksum,
		sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.MetadataTable))
}

// genChecksums folds a checksum result set into an order-independent 64-bit fingerprint plus the
// model-row count (a cheap second dimension that guards a fingerprint hash collision). The checksums
// are sorted first so the fingerprint is a pure multiset digest, insensitive to row order; each is
// NUL-delimited so ["a","bc"] and ["ab","c"] cannot collide.
func genChecksums(res executor.Result) (fp uint64, count int64) {
	var sums []string
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		v := bat.Vecs[0]
		n := bat.RowCount()
		for i := 0; i < n; i++ {
			sums = append(sums, v.GetStringAt(i))
		}
	}
	sort.Strings(sums)
	h := fnv.New64a()
	for _, s := range sums {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64(), int64(len(sums))
}

// ClearIndexSqls returns the statements that empty the hnsw hidden tables (metadata + storage).
// The full-build TVF issues these before rebuilding, so it owns clear+rebuild.
//
// WHERE TRUE is deliberate: a bare "DELETE FROM t" is rewritten to a TRUNCATE, which swaps the
// table's physical id (a new index table under the same name). This in-place rebuild keeps the
// table stable and relies on the checksum generation for freshness, so we force a plain row delete.
func ClearIndexSqls(tblcfg vectorindex.IndexTableConfig) []string {
	return []string{
		"DELETE FROM " + sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.MetadataTable) + " WHERE TRUE",
		"DELETE FROM " + sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable) + " WHERE TRUE",
	}
}

// loadHnswGeneration reads the current (checksumFingerprint, modelCount) generation using the
// caller's LIVE sqlproc/txn — captured at load so it reflects exactly the snapshot the cached index
// was built from. Panics from the internal SQL executor (e.g. no executor in a unit-test proc) are
// recovered into an error; the caller degrades to genValid=false rather than failing the load.
func loadHnswGeneration(sqlproc *sqlexec.SqlProcess, tblcfg vectorindex.IndexTableConfig) (fp uint64, count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			fp, count, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("loadHnswGeneration recovered: %v", r))
		}
	}()
	res, err := runSql(sqlproc, hnswGenerationSql(tblcfg))
	if err != nil {
		return 0, 0, err
	}
	fp, count = genChecksums(res)
	res.Close()
	return fp, count, nil
}

// queryHnswGeneration reads the (checksumFingerprint, modelCount) generation in the BACKGROUND
// (housekeeping goroutine, no live sqlproc) via an executor-managed auto-commit txn, keyed by the
// CN UUID + tenant captured at load. Used by HnswSearch.IsStale.
func queryHnswGeneration(ctx context.Context, cnUUID string, accountID uint32, tblcfg vectorindex.IndexTableConfig) (fp uint64, count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			fp, count, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("queryHnswGeneration recovered: %v", r))
		}
	}()
	res, err := runSqlAutoCommit(ctx, cnUUID, accountID, tblcfg.DbName, hnswGenerationSql(tblcfg))
	if err != nil {
		return 0, 0, err
	}
	fp, count = genChecksums(res)
	res.Close()
	return fp, count, nil
}
