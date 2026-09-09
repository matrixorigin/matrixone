//go:build gpu

// Copyright 2022 Matrix Origin
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

package cagra

import (
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// recordingTxn captures all SQL statements that pass through CagraSync.Save
// without actually executing them — lets us assert what the sync would have
// written to the storage table.
type recordingTxn struct {
	statements []string
}

func (r *recordingTxn) install(t *testing.T) func() {
	t.Helper()
	origRun := runTxn
	runTxn = func(_ *sqlexec.SqlProcess, fn func(executor.TxnExecutor) error) error {
		tx := &recordingTxnExec{rec: r}
		return fn(tx)
	}
	return func() { runTxn = origRun }
}

type recordingTxnExec struct {
	rec *recordingTxn
}

func (e *recordingTxnExec) Exec(sql string, _ executor.StatementOption) (executor.Result, error) {
	e.rec.statements = append(e.rec.statements, sql)
	return executor.Result{}, nil
}

func (e *recordingTxnExec) LockTable(_ string) error { return nil }
func (e *recordingTxnExec) Use(_ string)             {}
func (e *recordingTxnExec) Txn() client.TxnOperator  { return nil }

// idxdefs returns the minimal IndexDef slice CagraSync needs.
func idxdefs(metaTbl, storageTbl string) []*plan.IndexDef {
	return []*plan.IndexDef{
		{IndexTableName: metaTbl, IndexAlgoTableType: catalog.Cagra_TblType_Metadata},
		{IndexTableName: storageTbl, IndexAlgoTableType: catalog.Cagra_TblType_Storage},
	}
}

// installNextChunkIdMock wires runSql to return a fixed chunk_id from the
// `SELECT COALESCE(MAX(chunk_id) + 1, 0)` query Save runs before appending.
func installNextChunkIdMock(t *testing.T, proc *process.Process, nextId int64) func() {
	t.Helper()
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if !strings.Contains(sql, "COALESCE(MAX(chunk_id)") {
			t.Fatalf("unexpected runSql call from sync: %s", sql)
		}
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed[int64](b.Vecs[0], nextId, false, proc.Mp())
		b.SetRowCount(1)
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{b}}, nil
	}
	return func() { runSql = origRun }
}

var unhexLitRe2 = regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)

// extractRecordsFromSql concatenates every unhex blob found in the given
// SQL statements (in document order) — the bytes the storage layer would
// see for tag=1 chunks.
func extractRecordsFromSql(t *testing.T, sqls []string) []byte {
	t.Helper()
	var out []byte
	for _, s := range sqls {
		for _, m := range unhexLitRe2.FindAllStringSubmatch(s, -1) {
			b, err := hex.DecodeString(m[1])
			require.NoError(t, err)
			out = append(out, b...)
		}
	}
	return out
}

// chunksFromSql converts the captured sync output into one EventChunk per
// unhex literal. ChunkIds increment from startId so the test can drive them
// through ReplayEventLog.
func chunksFromSql(t *testing.T, sqls []string, startId int64) []cuvscdc.EventChunk {
	t.Helper()
	var chunks []cuvscdc.EventChunk
	id := startId
	for _, s := range sqls {
		for _, m := range unhexLitRe2.FindAllStringSubmatch(s, -1) {
			b, err := hex.DecodeString(m[1])
			require.NoError(t, err)
			chunks = append(chunks, cuvscdc.EventChunk{ChunkId: id, Data: b})
			id++
		}
	}
	return chunks
}

// TestCagraSync_Update_AllInsert: pure-INSERT batch encodes 3 INSERT records;
// Save emits one INSERT INTO ... tag=1 chunk that round-trips through replay
// into the same overflow set.
func TestCagraSync_Update_AllInsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)
	require.Equal(t, vectorindex.CdcTailId, s.activeIndexId)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_INSERT, PKey: 101, Vec: []float32{5, 6, 7, 8}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 2,
		"expected 2 INSERT records buffered")

	require.NoError(t, s.Save(sqlproc))
	// The chunk statement only: the frame's metadata row is written just once the table has the
	// provenance columns, because a row an un-upgraded CN would read as a sub-index must not
	// exist before every CN can exclude it. Here the probe answers narrow.
	require.Len(t, rec.statements, 1)
	require.Contains(t, rec.statements[0], "INSERT INTO `db`.`__storage` VALUES")
	require.Contains(t, rec.statements[0], "INSERT INTO `db`.`__storage` VALUES")
	require.Contains(t, rec.statements[0], "'cdc_tail', 0,")

	// Round-trip: replay the persisted chunks and expect 2 overflow rows, no
	// deletes.
	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 16, 0)
	require.NoError(t, err)
	require.Empty(t, state.Deleted)
	require.Len(t, state.Overflow, 2)
	require.Equal(t, int64(100), state.Overflow[0].Pkid)
	require.Equal(t, int64(101), state.Overflow[1].Pkid)
}

// TestCagraSync_Update_DeleteAndInsert: a DELETE plus an INSERT in one flush
// produces 1 DELETE + 1 INSERT record packed into the same chunk; replay
// produces deleted={42} overflow={100}.
func TestCagraSync_Update_DeleteAndInsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 7)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 42},
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 2)

	require.NoError(t, s.Save(sqlproc))
	// The chunk statement only: the frame's metadata row is written just once the table has the
	// provenance columns, because a row an un-upgraded CN would read as a sub-index must not
	// exist before every CN can exclude it. Here the probe answers narrow.
	require.Len(t, rec.statements, 1)
	require.Contains(t, rec.statements[0], "INSERT INTO `db`.`__storage` VALUES")
	// chunk_id == 7 (nextChunkId mock).
	require.Contains(t, rec.statements[0], "'cdc_tail', 7,")

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 7), 16, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{42}, state.Deleted)
	require.Len(t, state.Overflow, 1)
	require.Equal(t, int64(100), state.Overflow[0].Pkid)
}

// TestCagraSync_Update_DeleteInsertDelete is the user's collapse case — the
// flaw the unified-log design exists to fix. The writer emits 3 records
// verbatim; replay (run at search-side load time) collapses them so the
// final state has pkid=1 deleted and overflow empty.
func TestCagraSync_Update_DeleteInsertDelete(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 1},
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_DELETE, PKey: 1},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 3,
		"writer must persist all 3 records verbatim — replay collapses them, not the writer")

	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 16, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, state.Deleted,
		"final state must have pkid=1 deleted (last event was DELETE)")
	require.Empty(t, state.Overflow,
		"overflow must be empty — DELETE-after-INSERT-after-DELETE collapses cleanly")
}

// TestCagraSync_Update_DeleteIdempotent: 3 DELETE records survive in the log
// (writer is intentionally non-deduplicating); replay collapses to a single
// pkid in Deleted (delete_id is idempotent on the cuvs side too).
func TestCagraSync_Update_DeleteIdempotent(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 5},
			{Type: vectorindex.CDC_DELETE, PKey: 5},
			{Type: vectorindex.CDC_DELETE, PKey: 7},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 3)

	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 16, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{5, 7}, state.Deleted)
}

// TestCagraSync_Update_Upsert: UPSERT decomposes into DELETE+INSERT records;
// replay collapses to a single overflow entry with the latest vec.
func TestCagraSync_Update_Upsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_UPSERT, PKey: 100, Vec: []float32{9, 9, 9, 9}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 2,
		"INSERT + UPSERT → 2 records (UPSERT is a single op, not DELETE+INSERT)")

	require.NoError(t, s.Save(sqlproc))
	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 16, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{100}, state.Deleted,
		"UPSERT marks pkid in deleted (filters any pre-rebuild main-index entry)")
	require.Len(t, state.Overflow, 1)
	require.Equal(t, int64(100), state.Overflow[0].Pkid)
	require.Equal(t, []float32{9, 9, 9, 9}, util.UnsafeSliceCast[float32](state.Overflow[0].Vec),
		"UPSERT wrote the latest vec; replay surfaces it")
}

// TestCagraSync_Update_DimMismatch: a vector with the wrong length surfaces
// as an error at Update time (writer-side validation).
func TestCagraSync_Update_DimMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 2, 3}}, // dim=3, want 4
		},
	}
	err = s.Update(sqlproc, cdc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vec length")
}

// TestCagraSync_Update_WithIncludeBytes: an index with INCLUDE columns
// requires every INSERT/UPSERT to carry includeBytes of the right length.
// Round-trip the bytes through Update + Save + replay.
func TestCagraSync_Update_WithIncludeBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	colMetaJSON := `[{"name":"tier","type":1}]`
	expectedIBPR, err := cuvscdc.CdcIncludeBytesPerRow(colMetaJSON)
	require.NoError(t, err)
	require.Equal(t, 9, expectedIBPR)

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, colMetaJSON)
	require.NoError(t, err)
	require.Equal(t, 9, s.includeBytesPerRow)

	include := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x00}
	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 100,
				Vec: []float32{1, 2, 3, 4}, IncludeBytes: include},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 16, 9)
	require.NoError(t, err)
	require.Len(t, state.Overflow, 1)
	require.Equal(t, include, state.Overflow[0].Include)

	// Subsequent Update with a wrong include length must error.
	require.Error(t, s.Update(sqlproc, &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 101, Vec: []float32{1, 2, 3, 4}, IncludeBytes: []byte{0x00}},
		},
	}))
}

// TestCagraSync_Update_NoOpSaveSkipsSql: a flush of pure no-op events writes
// zero SQL — the pending buffer was empty so Save short-circuits before even
// asking for a chunk_id.
func TestCagraSync_Update_NoOpSaveSkipsSql(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	rec := &recordingTxn{}
	defer rec.install(t)()
	// runSql shouldn't be called at all; install a panicking stub.
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		t.Fatalf("unexpected runSql call: %s", sql)
		return executor.Result{}, nil
	}
	defer func() { runSql = origRun }()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))
	require.Empty(t, rec.statements)
}

// TestCagraSync_NewSync_Stateless: NewCagraSync issues no SQL — it doesn't
// load metadata or prior tag=1 contents. The writer is stateless across
// flushes by construction.
func TestCagraSync_NewSync_Stateless(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	called := 0
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		called++
		t.Fatalf("NewCagraSync should not call runSql, got: %s", sql)
		return executor.Result{}, nil
	}
	defer func() { runSql = origRun }()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)
	require.Equal(t, vectorindex.CdcTailId, s.activeIndexId)
	require.Equal(t, 0, called)
}

// TestCagraSync_RunOnce: smoke test the full Update + Save + Destroy chain.
func TestCagraSync_RunOnce(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 1},
			{Type: vectorindex.CDC_INSERT, PKey: 2, Vec: []float32{4, 4, 4, 4}},
		},
	}
	require.NoError(t, s.RunOnce(sqlproc, cdc))
	// Destroy was called via defer — pending buffer is cleared.
	require.Nil(t, s.pendingRecords)
	require.Nil(t, s.pendingSizes)
	require.NotEmpty(t, rec.statements,
		"RunOnce should have produced at least one SQL statement")
}

// TestCagraSync_MultiFlush: after Save, the pending buffer resets and the
// next Update + Save cycle appends fresh records at the next chunk_id.
func TestCagraSync_MultiFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// Two consecutive saves: both ask for nextChunkId; serve 0 then 1.
	calls := 0
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		require.Contains(t, sql, "COALESCE(MAX(chunk_id)")
		nextId := int64(calls)
		calls++
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed[int64](b.Vecs[0], nextId, false, proc.Mp())
		b.SetRowCount(1)
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{b}}, nil
	}
	defer func() { runSql = origRun }()

	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	flush1 := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 0, 0, 0}},
		},
	}
	require.NoError(t, s.Update(sqlproc, flush1))
	require.NoError(t, s.Save(sqlproc))
	require.Empty(t, s.pendingSizes,
		"Save must reset the pending buffer")

	flush2 := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 1},
		},
	}
	require.NoError(t, s.Update(sqlproc, flush2))
	require.NoError(t, s.Save(sqlproc))

	// First flush at chunk_id=0, second at chunk_id=1.
	var chunkStmts, metaStmts []string
	for _, st := range rec.statements {
		if strings.Contains(st, "__meta") {
			metaStmts = append(metaStmts, st)
		} else {
			chunkStmts = append(chunkStmts, st)
		}
	}
	require.Len(t, chunkStmts, 2)
	require.Contains(t, chunkStmts[0], "'cdc_tail', 0,")
	require.Contains(t, chunkStmts[1], "'cdc_tail', 1,")
	require.Empty(t, metaStmts, "narrow table: no frame rows yet")
}

// On a WIDENED metadata table the flush also writes the frame's row: its bytes, its record
// count, and the version ISCP applied. The row is gated on the table shape because an
// un-upgraded CN reads this table with SELECT * and would treat 'cdc_tail:N' as a sub-index.
func TestCagraSyncWritesTheFrameRowOnceTheTableHasIt(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	// Its own metadata table name: provenanceShape is process-wide, so sharing "__meta" with
	// the other sync tests would let them switch this one's shape out from under it.
	const meta = "__meta_frame_row"
	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs(meta, "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	sqlexec.MarkProvenanceColumns("db", meta)
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape("db", meta) })
	s.SetBuildTS(4242)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 2, 3, 4}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))

	var metaSql string
	for _, st := range rec.statements {
		if strings.Contains(st, meta) {
			metaSql += st
		}
	}
	require.Contains(t, metaSql, "'cdc_tail:0'", "keyed by the chunk id the frame starts at")
	require.Contains(t, metaSql, "4242", "and carrying the version this flush applied")
}

// A flush that spans SEVERAL chunks writes one metadata row per chunk, each carrying that
// chunk's own stored length -- not one row carrying the flush's record total.
//
// The reader derives the chunks a row covers as ceil(filesize / MaxChunkSize) and compares the
// sum against the chunks actually stored. Record bytes always divide into fewer chunks than were
// written (records are packed under MaxChunkSize minus frame overhead and header, and never
// split), so one row per flush reported a fully described tail as partly undescribed, and
// CdcTailRowsUpperBound added a chunk's worth of phantom rows for every chunk it could not see.
func TestCagraSyncWritesOneFrameRowPerChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	const meta = "__meta_frame_rows_per_chunk"
	s, err := NewCagraSync(sqlproc, "db", "src", "idxname",
		idxdefs(meta, "__storage"), 4, types.T_array_float32, "")
	require.NoError(t, err)

	sqlexec.MarkProvenanceColumns("db", meta)
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape("db", meta) })

	// Records enough to fill several chunks: 9 + 4*4 bytes each.
	const nrec = 4 * vectorindex.MaxChunkSize / 25
	entries := make([]vectorindex.VectorIndexCdcEntry[float32], 0, nrec)
	for i := 0; i < nrec; i++ {
		entries = append(entries, vectorindex.VectorIndexCdcEntry[float32]{
			Type: vectorindex.CDC_INSERT, PKey: int64(i + 1), Vec: []float32{1, 2, 3, 4},
		})
	}
	require.NoError(t, s.Update(sqlproc, &vectorindex.VectorIndexCdc[float32]{Data: entries}))
	require.NoError(t, s.Save(sqlproc))

	var metaSql, chunkSql string
	for _, st := range rec.statements {
		if strings.Contains(st, meta) {
			metaSql += st
		} else if strings.Contains(st, "__storage") {
			chunkSql += st
		}
	}
	storedChunks := strings.Count(chunkSql, "'cdc_tail', ")
	require.Greater(t, storedChunks, 1, "the fixture must span several chunks")

	ids := regexp.MustCompile(`'cdc_tail:(\d+)'`).FindAllStringSubmatch(metaSql, -1)
	require.Len(t, ids, storedChunks, "one metadata row per stored chunk, not one per flush")
	for i, m := range ids {
		require.Equal(t, strconv.Itoa(i), m[1], "keyed by its own chunk id, contiguous")
	}

	// Every row's filesize is one chunk's framed length, so the reader's ceil resolves to
	// exactly one chunk per row and the coverage sum equals the chunks stored.
	sizes := regexp.MustCompile(`'cdc_tail:\d+', '[^']*', \d+, (\d+),`).FindAllStringSubmatch(metaSql, -1)
	require.Len(t, sizes, storedChunks)
	covered := 0
	for _, m := range sizes {
		n, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		require.Positive(t, n)
		require.LessOrEqual(t, n, vectorindex.MaxChunkSize)
		covered += (n + vectorindex.MaxChunkSize - 1) / vectorindex.MaxChunkSize
	}
	require.Equal(t, storedChunks, covered,
		"the tail is fully described: no uncovered chunk, so no phantom rows in the estimate")
}
