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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	// A SIZING read must never be the thing that breaks a load. RunSql reaches the executor,
	// which panics rather than erroring when a process has no lock service (internal callers,
	// unit contexts) -- and the honest answer there is the same as for a failed read: no
	// estimate, which is exactly the behaviour that existed before this estimate did.
	defer func() {
		if r := recover(); r != nil {
			logutil.Warnf("cdc tail sizing: probing %s.%s panicked, admitting without an estimate: %v",
				db, metaTable, r)
			rows, err = 0, nil
		}
	}()
	sql := fmt.Sprintf("SELECT CAST(COALESCE(SUM(%s), 0) AS SIGNED) FROM %s WHERE %s LIKE %s",
		catalog.IndexMetadata_TblCol_Nrow,
		sqlquote.QualifiedIdent(db, metaTable),
		catalog.IndexMetadata_TblCol_Index_Id, sqlquote.String(vectorindex.TailFrameMetaPrefix+"%"))
	rows, err = scalarInt64(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	if rows > 0 {
		return rows, nil
	}
	if storageTable == "" || vectorBytes <= 0 {
		return 0, nil
	}
	sql = fmt.Sprintf("SELECT CAST(COUNT(*) AS SIGNED) FROM %s WHERE %s = %s AND %s = %d",
		sqlquote.QualifiedIdent(db, storageTable),
		catalog.IndexStorage_TblCol_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.IndexStorage_TblCol_Tag, int(vectorindex.Tag_CdcEvents))
	chunks, cerr := scalarInt64(sqlproc, sql)
	if cerr != nil || chunks <= 0 {
		return 0, cerr
	}
	if chunks > (1<<62)/int64(vectorindex.MaxChunkSize) {
		return 0, nil // absurd count; no useful bound
	}
	return chunks * int64(vectorindex.MaxChunkSize) / vectorBytes, nil
}

// runSqlForTest indirects the read so the sizing rules are testable without a cluster.
var runSqlForTest = RunSql

func scalarInt64(sqlproc *SqlProcess, sql string) (int64, error) {
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
