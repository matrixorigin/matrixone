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

// Package cachegen provides the VectorIndexCache freshness-check (IsStale) generation queries
// for CDC-maintained cuvs/hnsw indexes. It lives in its own package — not pkg/vectorindex — so
// that importing pkg/vectorindex/sqlexec (which pulls in vm/engine → objectio → catalog) does
// not create an import cycle back through pkg/vectorindex under the GPU build.
package cachegen

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// indirected for unit tests.
var (
	runSql           = sqlexec.RunSql
	runSqlAutoCommit = sqlexec.RunSqlAutoCommit
)

// CdcGenerationSqls returns the (timestamp, tail-chunk) generation queries shared by every
// CDC-maintained cuvs/hnsw index, for the VectorIndexCache cross-CN freshness check (IsStale):
//   - MAX(metadata.timestamp): a REBUILD/MERGE writes a new model row with a fresh timestamp.
//   - MAX(storage.chunk_id) of the (vectorindex.CdcTailId, tag=1) event tail: a CDC append bumps it.
//
// A change in EITHER means the loaded index is stale. Two reads because timestamp and tag live
// in different tables (metadata has no tag; storage has no timestamp). The tail read is scoped
// to (vectorindex.CdcTailId, tag=1) — the exact CDC delta, which every cuvs index writes under activeIndexId
// == vectorindex.CdcTailId — so an unrelated base sub-index's higher chunk_id can't mask a fresh append. An
// index with no event tail (hnsw rebuilds the model instead) simply reads a constant -1 for the
// tail half; MAX(timestamp) then catches all of its changes. The hidden-table column names
// ("timestamp","chunk_id","index_id","tag") are uniform across these index types.
func CdcGenerationSqls(tblcfg vectorindex.IndexTableConfig) (tsSQL, tailSQL string) {
	tsSQL = fmt.Sprintf("SELECT COALESCE(MAX(timestamp), 0) FROM %s",
		sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.MetadataTable))
	tailSQL = fmt.Sprintf("SELECT COALESCE(MAX(chunk_id), -1) FROM %s WHERE index_id = %s AND tag = %d",
		sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(vectorindex.CdcTailId), int(vectorindex.Tag_CdcEvents))
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

// LoadCdcGeneration reads the current (timestamp, tailChunk) generation using the caller's LIVE
// sqlproc/txn — called at load time so the captured generation reflects exactly the txn snapshot
// the cached index was built from. Panics from the internal SQL executor (e.g. no executor in a
// unit-test proc) are converted to an error, since generation capture is best-effort — the
// caller degrades to genValid=false (IsStale becomes a no-op) rather than failing the load.
func LoadCdcGeneration(sqlproc *sqlexec.SqlProcess, tblcfg vectorindex.IndexTableConfig) (ts int64, tail int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, tail, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("LoadCdcGeneration recovered: %v", r))
		}
	}()
	tsSQL, tailSQL := CdcGenerationSqls(tblcfg)
	res, err := runSql(sqlproc, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = genScalarInt64(res)
	res.Close()
	res, err = runSql(sqlproc, tailSQL)
	if err != nil {
		return 0, 0, err
	}
	tail = genScalarInt64(res)
	res.Close()
	return ts, tail, nil
}

// QueryCdcGeneration reads the current (timestamp, tailChunk) generation in the BACKGROUND
// (housekeeping goroutine, no live sqlproc) via an executor-managed auto-commit txn, keyed by
// the CN UUID + tenant captured at load. Used by an index's IsStale.
func QueryCdcGeneration(ctx context.Context, cnUUID string, accountID uint32, tblcfg vectorindex.IndexTableConfig) (ts int64, tail int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, tail, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("QueryCdcGeneration recovered: %v", r))
		}
	}()
	tsSQL, tailSQL := CdcGenerationSqls(tblcfg)
	res, err := runSqlAutoCommit(ctx, cnUUID, accountID, tblcfg.DbName, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = genScalarInt64(res)
	res.Close()
	res, err = runSqlAutoCommit(ctx, cnUUID, accountID, tblcfg.DbName, tailSQL)
	if err != nil {
		return 0, 0, err
	}
	tail = genScalarInt64(res)
	res.Close()
	return ts, tail, nil
}
