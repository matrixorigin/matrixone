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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// CdcTailRowsUpperBound estimates how many rows a CDC tail will contribute to the overflow index,
// BEFORE the tail is read -- which is what lets admission reserve for it instead of discovering
// the bytes after they are already allocated on the GPU.
//
// Two sources, in order:
//
//   - the per-frame metadata rows this branch writes, whose nrow is the record count of the flush
//     that wrote them. Summed, that is the tail's record count exactly. It counts deletes too,
//     which the overflow index does not hold, so the figure is an upper bound -- the safe
//     direction for a budget.
//   - a tail written before those rows existed has none to sum, and reading the tail itself to
//     count would double the cost of every cache miss (Load reads it in full moments later). Its
//     chunk count bounds it instead: a chunk holds at most MaxChunkSize bytes and a record is at
//     least its vector, so chunks*MaxChunkSize/vectorBytes is a ceiling on the rows.
//
// vectorBytes is the storage width of one vector (dimensions x element size). Zero or negative
// disables the fallback, since the division is what makes it a row count.
func CdcTailRowsUpperBound(sqlproc *SqlProcess, db, metaTable, storageTable string, vectorBytes int64) (rows int64, err error) {
	if sqlproc == nil || db == "" || metaTable == "" {
		return 0, nil
	}
	// COVERAGE, not "is the sum positive". The frame rows describe the flushes that wrote
	// them; a tail can be described in PART, and then their sum is the size of the described
	// part rather than of the tail:
	//
	//   - a pre-upgrade tail already holding rows gains the columns when its tenant migrates,
	//     but no rows for the flushes already on disk. One later 1-row flush adds one row, and
	//     summing it reports 1 while loadCdcTail replays every one of the originals.
	//   - a narrow table has no nrow column at all, so the SUM errors.
	//
	// Both end at the same place: count what the rows account for, and bound the rest by the
	// chunks actually stored.
	covered, coveredChunks, cerr := tailFrameCoverage(sqlproc, db, metaTable)
	if cerr != nil {
		// Narrow table (no nrow column) or an unreadable metadata table. This is an ordinary
		// rollout state, not an outage, so it must not silently reserve zero.
		return chunkBound(sqlproc, db, storageTable, vectorBytes), nil
	}
	totalChunks := tailChunkCount(sqlproc, db, storageTable)
	uncovered := totalChunks - coveredChunks
	if uncovered <= 0 || vectorBytes <= 0 {
		return covered, nil
	}
	// A chunk holds at most MaxChunkSize bytes and a record is at least its vector, so this
	// ceilings the rows those chunks can carry.
	perChunk := int64(vectorindex.MaxChunkSize) / vectorBytes
	if perChunk < 1 {
		perChunk = 1
	}
	if uncovered > (math.MaxInt64-covered)/perChunk {
		return math.MaxInt64, nil
	}
	return covered + uncovered*perChunk, nil
}

// tailFrameCoverage returns the rows the frame rows account for and the chunks those flushes
// occupy. filesize is the flush's byte length, written with the chunks it describes, so
// ceil(filesize / MaxChunkSize) is the span each row covers.
func tailFrameCoverage(sqlproc *SqlProcess, db, metaTable string) (rows, chunks int64, err error) {
	sql := fmt.Sprintf(
		"SELECT CAST(COALESCE(SUM(%s), 0) AS SIGNED), CAST(COALESCE(SUM((%s + %d) DIV %d), 0) AS SIGNED) "+
			"FROM %s WHERE %s LIKE %s",
		catalog.IndexMetadata_TblCol_Nrow,
		catalog.IndexMetadata_TblCol_Filesize,
		vectorindex.MaxChunkSize-1, vectorindex.MaxChunkSize,
		sqlquote.QualifiedIdent(db, metaTable),
		catalog.IndexMetadata_TblCol_Index_Id, sqlquote.String(vectorindex.TailFrameMetaPrefix+"%"))
	res, err := coverageRead(sqlproc, sql)
	if err != nil {
		return 0, 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) < 2 {
			continue
		}
		return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0),
			vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0), nil
	}
	return 0, 0, nil
}

// coverageRead is scalarInt64's guard for the two-column coverage query.
func coverageRead(sqlproc *SqlProcess, sql string) (res executor.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Warnf("cdc tail sizing: coverage read panicked, treating as unreadable: %v", r)
			err = moerr.NewInternalErrorNoCtxf("cdc tail sizing: %v", r)
		}
	}()
	return runSqlForTest(sqlproc, sql)
}

// tailChunkCount counts the tag=1 chunks actually stored. Unreadable answers 0, which leaves the
// caller with whatever the frame rows covered.
func tailChunkCount(sqlproc *SqlProcess, db, storageTable string) int64 {
	if storageTable == "" {
		return 0
	}
	sql := fmt.Sprintf("SELECT CAST(COUNT(*) AS SIGNED) FROM %s WHERE %s = %s AND %s = %d",
		sqlquote.QualifiedIdent(db, storageTable),
		catalog.IndexStorage_TblCol_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.IndexStorage_TblCol_Tag, int(vectorindex.Tag_CdcEvents))
	n, _ := scalarInt64(sqlproc, sql)
	return n
}

// chunkBound is the fallback when the frame rows cannot be read at all: every stored chunk,
// ceilinged by how many records one can hold.
func chunkBound(sqlproc *SqlProcess, db, storageTable string, vectorBytes int64) int64 {
	if vectorBytes <= 0 {
		return 0
	}
	chunks := tailChunkCount(sqlproc, db, storageTable)
	if chunks <= 0 {
		return 0
	}
	perChunk := int64(vectorindex.MaxChunkSize) / vectorBytes
	if perChunk < 1 {
		perChunk = 1
	}
	if chunks > math.MaxInt64/perChunk {
		return math.MaxInt64
	}
	return chunks * perChunk
}

// runSqlForTest indirects the read so the sizing rules are testable without a cluster.
var runSqlForTest = RunSql

// scalarInt64 reads a one-column, one-row result.
//
// A SIZING read must never be the thing that breaks a load, and RunSql reaches the executor,
// which PANICS rather than erroring when a process has no lock service (internal callers, unit
// contexts). Recovering HERE -- at the single point that touches RunSql -- rather than around the
// caller matters: a recover wrapped around the caller would have to re-run a query to produce its
// fallback, and a panic raised inside a deferred function propagates rather than being caught
// again. Each read degrades independently, and the caller's own fallback chain does the rest.
func scalarInt64(sqlproc *SqlProcess, sql string) (n int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Warnf("cdc tail sizing: read panicked, treating as unreadable: %v", r)
			n, err = 0, moerr.NewInternalErrorNoCtxf("cdc tail sizing: %v", r)
		}
	}()
	res, err := runSqlForTest(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) == 0 {
			continue
		}
		return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
	}
	return 0, nil
}
