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

// Package idxcron carries the shared cuvs idxcron Updatable body.
//
// Lives one directory away from pkg/vectorindex/idxcron (the cron
// consumer/executor) to avoid a test-only import cycle on the GPU
// tag: pkg/vectorindex/idxcron tests pull in pkg/sql/plan via
// testengine, pkg/sql/plan pulls in pkg/indexplugin/all, and on the
// GPU tag that pulls in pkg/vectorindex/cagra/plugin/idxcron which
// needs the shared helper. Putting the helper in a separate package
// breaks the cycle.
package idxcron

import (
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CuvsUpdatableSpec carries the per-algorithm configuration the shared
// CuvsUpdatable body needs. Struct (not positional args) so future
// knobs can be added without churning every call site — e.g. an
// absolute MinRecordCount floor, a ThresholdMultiplier, alternate
// CountStrategy (records vs chunks vs bytes), etc.
type CuvsUpdatableSpec struct {
	// StorageTableType is the IndexAlgoTableType holding the tag=1
	// CDC event log (catalog.Cagra_TblType_Storage /
	// catalog.Ivfpq_TblType_Storage).
	StorageTableType string

	// ThresholdParam is the indexAlgoParams key whose int64 value
	// sets the minimum delta-record count needed before the
	// cron-triggered rebuild fires. For CAGRA:
	// catalog.IntermediateGraphDegree; for IVF-PQ:
	// catalog.IndexAlgoParamLists.
	ThresholdParam string
}

// runSelectChunkSql runs the SELECT used to fetch tag=1 chunk data.
// Stubbed as a var so tests can replace it.
var runSelectChunkSql = sqlexec.RunSql

// CuvsUpdatable counts CDC delta records in the index's tag=1 chunks
// and compares against the threshold (read fresh from
// indexAlgoParams[spec.ThresholdParam] every tick). The shared body
// that CAGRA's and IVF-PQ's Updatable hooks delegate to.
//
// Returns:
//   - (true,  "",     nil) when delta record count >= threshold
//   - (false, reason, nil) when delta < threshold (rebuild deferred)
//   - (_, _, err)          on SQL / decode failures
//
// When tag=1 is empty (no CDC events since last rebuild) the count is
// zero and the gate skips. Likewise when the threshold is missing or
// non-positive in indexAlgoParams — the rebuild is deferred until the
// user supplies a sensible threshold.
func CuvsUpdatable(
	in idxcronplugin.UpdatableInput,
	spec CuvsUpdatableSpec,
) (ok bool, reason string, err error) {
	if spec.StorageTableType == "" || spec.ThresholdParam == "" {
		return false, "", moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: spec must set StorageTableType and ThresholdParam")
	}

	// Cadence: skip when the last rebuild is still within the
	// configured interval. The executor enforces createdAt+interval
	// universally; this is the per-run cadence that used to live in
	// the executor's listsAware=false branch.
	if in.LastUpdateAt != nil {
		last := time.Unix(in.LastUpdateAt.Unix(), 0)
		now := time.Now()
		if last.Add(in.Interval).After(now) {
			return false, fmt.Sprintf(
				"current time < interval after lastUpdateAt (%v + %v > %v)",
				last.Format("2006-01-02 15:04:05"), in.Interval, now.Format("2006-01-02 15:04:05")), nil
		}
	}

	// Locate the storage IndexDef + read threshold from its
	// indexAlgoParams.
	var storageTbl, algoParams string
	for _, idx := range in.TableDef.Indexes {
		if idx.IndexName == in.IndexName && idx.IndexAlgoTableType == spec.StorageTableType {
			storageTbl = idx.IndexTableName
			algoParams = idx.IndexAlgoParams
			break
		}
	}
	if storageTbl == "" {
		return false, "", moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: no IndexDef found for index %q with table-type %q",
			in.IndexName, spec.StorageTableType)
	}

	threshold, err := readInt64Param(algoParams, spec.ThresholdParam)
	if err != nil {
		return false, "", err
	}
	if threshold <= 0 {
		return false, fmt.Sprintf("threshold param %q missing or non-positive in indexAlgoParams",
			spec.ThresholdParam), nil
	}

	// Derive dim + includeBytesPerRow for DecodeEventRecord. The
	// values must match the writer side so records frame correctly.
	dim, ibpr, err := deriveCuvsRecordShape(in.TableDef, in.IndexName, spec.StorageTableType, algoParams)
	if err != nil {
		return false, "", err
	}

	count, err := countTag1Records(in.Sqlproc, in.TableDef.DbName, storageTbl, dim, ibpr)
	if err != nil {
		return false, "", err
	}

	if count < threshold {
		return false, fmt.Sprintf(
			"CDC delta records %d < threshold %d (param %s)",
			count, threshold, spec.ThresholdParam), nil
	}
	return true, "", nil
}

// readInt64Param extracts a int64 value at key from a JSON
// indexAlgoParams blob, returning 0 if the key is absent. A parse
// error on a present key surfaces; a missing key is benign (0).
func readInt64Param(algoParams, key string) (int64, error) {
	if algoParams == "" {
		return 0, nil
	}
	ast, err := sonic.Get([]byte(algoParams), key)
	if err != nil {
		// Key absent — not an error, just no threshold configured.
		return 0, nil
	}
	v, err := ast.Int64()
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: indexAlgoParams[%q] is not int64: %v", key, err)
	}
	return v, nil
}

// deriveCuvsRecordShape returns (dim, includeBytesPerRow) for the
// (table, index) pair so DecodeEventRecord can walk tag=1 chunk
// bytes. dim is the vector column's Width (the index's first part).
// includeBytesPerRow is computed from indexAlgoParams' INCLUDE
// columns via ResolveIncludeColumns — same path the writer used to
// encode the chunks, so widths agree by construction.
func deriveCuvsRecordShape(
	tableDef *plan.TableDef,
	indexName string,
	storageTblType string,
	algoParams string,
) (dim, includeBytesPerRow int, err error) {
	// Find any IndexDef row with our index name (metadata or storage
	// — both share parts/algoParams). Prefer the storage row since
	// it carries the algoParams we already parsed.
	var partsCol string
	for _, idx := range tableDef.Indexes {
		if idx.IndexName == indexName && idx.IndexAlgoTableType == storageTblType {
			if len(idx.Parts) == 0 {
				return 0, 0, moerr.NewInternalErrorNoCtxf(
					"CuvsUpdatable: index %q storage def has no Parts", indexName)
			}
			partsCol = idx.Parts[0]
			break
		}
	}
	if partsCol == "" {
		return 0, 0, moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: storage IndexDef not found for index %q", indexName)
	}

	pos, ok := tableDef.Name2ColIndex[partsCol]
	if !ok {
		return 0, 0, moerr.NewInternalErrorNoCtxf(
			"CuvsUpdatable: vector column %q not in tableDef", partsCol)
	}
	col := tableDef.Cols[pos]
	dim = int(col.Typ.Width)

	// Resolve INCLUDE columns from algoParams (may be empty → ibpr=0).
	includedColumns := includedColumnsFromAlgoParams(algoParams)
	_, _, includeBytesPerRow, err = cuvscdc.ResolveIncludeColumns(
		includedColumns,
		tableDef.Name2ColIndex,
		func(p int32) int32 { return tableDef.Cols[p].Typ.Id },
	)
	if err != nil {
		return 0, 0, err
	}
	return dim, includeBytesPerRow, nil
}

// includedColumnsFromAlgoParams extracts the comma-separated INCLUDE
// column names from indexAlgoParams. Returns "" if absent.
func includedColumnsFromAlgoParams(algoParams string) string {
	if algoParams == "" {
		return ""
	}
	const key = "included_columns"
	ast, err := sonic.Get([]byte(algoParams), key)
	if err != nil {
		return ""
	}
	s, err := ast.StrictString()
	if err != nil {
		return ""
	}
	return s
}

// countTag1Records runs the chunk-fetch SQL, unframes each row's
// chunk data, and walks records with DecodeEventRecord, summing the
// total record count across all chunks.
func countTag1Records(
	sqlproc *sqlexec.SqlProcess,
	dbName, storageTbl string,
	dim, includeBytesPerRow int,
) (int64, error) {
	sql := fmt.Sprintf(
		"SELECT data FROM `%s`.`%s` WHERE index_id = '%s' AND tag = %d",
		dbName, storageTbl, vectorindex.CdcTailId, vectorindex.Tag_CdcEvents)

	res, err := runSelectChunkSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	var total int64
	for _, bat := range res.Batches {
		if bat.RowCount() == 0 {
			continue
		}
		dataVec := bat.Vecs[0]
		for i := 0; i < bat.RowCount(); i++ {
			framed := dataVec.GetBytesAt(i)
			if len(framed) == 0 {
				continue
			}
			records, err := cuvscdc.UnframeCdcChunk(framed)
			if err != nil {
				return 0, moerr.NewInternalErrorNoCtxf(
					"countTag1Records: unframe chunk: %v", err)
			}
			pos := 0
			for pos < len(records) {
				_, n, ok := cuvscdc.DecodeEventRecord(records[pos:], dim, includeBytesPerRow)
				if !ok {
					return 0, moerr.NewInternalErrorNoCtxf(
						"countTag1Records: malformed record at offset %d", pos)
				}
				total++
				pos += n
			}
		}
	}
	return total, nil
}
