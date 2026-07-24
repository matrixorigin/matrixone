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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// indirected for unit tests.
var runSqlAutoCommit = sqlexec.RunSqlAutoCommit

// hnswGenerationSqls returns the two freshness-generation queries for the hnsw metadata table,
// used by the VectorIndexCache cross-CN freshness check (IsStale):
//
//   - MAX(timestamp): a per-model row carries the save timestamp; a CDC append, in-place model
//     rewrite, REBUILD or MERGE deletes the old row and inserts one with a strictly-greater
//     timestamp (see HnswSync.nextTimestamp), so MAX advances on any content change that keeps
//     the row alive.
//   - COUNT(*): the number of live model rows. When CDC deletes every vector from a model, its
//     SaveToFile destroys the index file so ToSql emits no insert — only the delete lands, so the
//     model's metadata row disappears WITHOUT bumping MAX(timestamp) (the surviving models keep
//     their older timestamps). COUNT(*) drops in that case, catching the change that MAX(timestamp)
//     alone misses. Together the pair is complete: any insert advances MAX(timestamp) (monotonic),
//     and any pure deletion (the only way no insert happens) drops COUNT(*).
//
// hnsw does NOT use the shared cachegen (CdcTailId, tag=1) chunk-id read: it has no CDC event tail
// (it rewrites model files in place), so that read is a constant -1 for hnsw and cannot observe an
// emptied model. COUNT(*) replaces it as the second, deletion-sensitive half of the generation.
func hnswGenerationSqls(tblcfg vectorindex.IndexTableConfig) (tsSQL, countSQL string) {
	meta := sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.MetadataTable)
	tsSQL = fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", catalog.Hnsw_TblCol_Metadata_Timestamp, meta)
	countSQL = fmt.Sprintf("SELECT COUNT(*) FROM %s", meta)
	return
}

func genScalarInt64(res executor.Result) int64 {
	for _, bat := range res.Batches {
		if bat != nil && bat.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
		}
	}
	return 0
}

// loadHnswGeneration reads the current (timestamp, modelCount) generation using the caller's LIVE
// sqlproc/txn — called at load time so the captured generation reflects exactly the txn snapshot
// the cached index was built from. Panics from the internal SQL executor (e.g. no executor in a
// unit-test proc) are converted to an error, since generation capture is best-effort — the caller
// degrades to genValid=false rather than failing the load.
func loadHnswGeneration(sqlproc *sqlexec.SqlProcess, tblcfg vectorindex.IndexTableConfig) (ts int64, count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, count, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("loadHnswGeneration recovered: %v", r))
		}
	}()
	tsSQL, countSQL := hnswGenerationSqls(tblcfg)
	res, err := runSql(sqlproc, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = genScalarInt64(res)
	res.Close()
	res, err = runSql(sqlproc, countSQL)
	if err != nil {
		return 0, 0, err
	}
	count = genScalarInt64(res)
	res.Close()
	return ts, count, nil
}

// queryHnswGeneration reads the current (timestamp, modelCount) generation in the BACKGROUND
// (housekeeping goroutine, no live sqlproc) via an executor-managed auto-commit txn, keyed by the
// CN UUID + tenant captured at load. Used by HnswSearch.IsStale.
func queryHnswGeneration(ctx context.Context, cnUUID string, accountID uint32, tblcfg vectorindex.IndexTableConfig) (ts int64, count int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, count, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("queryHnswGeneration recovered: %v", r))
		}
	}()
	tsSQL, countSQL := hnswGenerationSqls(tblcfg)
	res, err := runSqlAutoCommit(ctx, cnUUID, accountID, tblcfg.DbName, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = genScalarInt64(res)
	res.Close()
	res, err = runSqlAutoCommit(ctx, cnUUID, accountID, tblcfg.DbName, countSQL)
	if err != nil {
		return 0, 0, err
	}
	count = genScalarInt64(res)
	res.Close()
	return ts, count, nil
}
