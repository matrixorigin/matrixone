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

package ivfpq

import (
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

type recordingTxn struct {
	statements []string
}

func (r *recordingTxn) install(t *testing.T) func() {
	t.Helper()
	origRun := runTxn
	runTxn = func(_ *sqlexec.SqlProcess, fn func(executor.TxnExecutor) error) error {
		return fn(&recordingTxnExec{rec: r})
	}
	return func() { runTxn = origRun }
}

type recordingTxnExec struct{ rec *recordingTxn }

func (e *recordingTxnExec) Exec(sql string, _ executor.StatementOption) (executor.Result, error) {
	e.rec.statements = append(e.rec.statements, sql)
	return executor.Result{}, nil
}

func (e *recordingTxnExec) LockTable(_ string) error { return nil }
func (e *recordingTxnExec) Use(_ string)             {}
func (e *recordingTxnExec) Txn() client.TxnOperator  { return nil }

func idxdefs(metaTbl, storageTbl string) []*plan.IndexDef {
	return []*plan.IndexDef{
		{IndexTableName: metaTbl, IndexAlgoTableType: catalog.Ivfpq_TblType_Metadata},
		{IndexTableName: storageTbl, IndexAlgoTableType: catalog.Ivfpq_TblType_Storage},
	}
}

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

func TestIvfpqSync_Update_AllInsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)
	require.Equal(t, vectorindex.CdcTailId, s.activeIndexId)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_INSERT, PKey: 101, Vec: []float32{5, 6, 7, 8}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 2)

	require.NoError(t, s.Save(sqlproc))
	require.Len(t, rec.statements, 1)
	require.Contains(t, rec.statements[0], "'cdc_tail', 0,")

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 4, 0)
	require.NoError(t, err)
	require.Empty(t, state.Deleted)
	require.Len(t, state.Overflow, 2)
}

func TestIvfpqSync_Update_DeleteAndInsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 7)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 42},
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))
	require.Len(t, rec.statements, 1)
	require.Contains(t, rec.statements[0], "'cdc_tail', 7,")

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 7), 4, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{42}, state.Deleted)
	require.Len(t, state.Overflow, 1)
}

// TestIvfpqSync_Update_DeleteInsertDelete: the user's collapse case.
func TestIvfpqSync_Update_DeleteInsertDelete(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 1},
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_DELETE, PKey: 1},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 3)

	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 4, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, state.Deleted)
	require.Empty(t, state.Overflow)
}

func TestIvfpqSync_Update_DeleteIdempotent(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 5},
			{Type: vectorindex.CDC_DELETE, PKey: 5},
			{Type: vectorindex.CDC_DELETE, PKey: 7},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 4, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{5, 7}, state.Deleted)
}

func TestIvfpqSync_Update_Upsert(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 100, Vec: []float32{1, 2, 3, 4}},
			{Type: vectorindex.CDC_UPSERT, PKey: 100, Vec: []float32{9, 9, 9, 9}},
		},
	}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.Len(t, s.pendingSizes, 3)
	require.NoError(t, s.Save(sqlproc))

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 4, 0)
	require.NoError(t, err)
	require.Empty(t, state.Deleted)
	require.Len(t, state.Overflow, 1)
	require.Equal(t, []float32{9, 9, 9, 9}, state.Overflow[0].Vec)
}

func TestIvfpqSync_Update_DimMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 1, Vec: []float32{1, 2, 3}},
		},
	}
	err = s.Update(sqlproc, cdc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vec length")
}

func TestIvfpqSync_Update_WithIncludeBytes(t *testing.T) {
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

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, colMetaJSON)
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

	state, err := cuvscdc.ReplayEventLog(chunksFromSql(t, rec.statements, 0), 4, 9)
	require.NoError(t, err)
	require.Len(t, state.Overflow, 1)
	require.Equal(t, include, state.Overflow[0].Include)

	require.Error(t, s.Update(sqlproc, &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_INSERT, PKey: 101, Vec: []float32{1, 2, 3, 4}, IncludeBytes: []byte{0x00}},
		},
	}))
}

func TestIvfpqSync_Update_NoOpSaveSkipsSql(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	rec := &recordingTxn{}
	defer rec.install(t)()
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		t.Fatalf("unexpected runSql call: %s", sql)
		return executor.Result{}, nil
	}
	defer func() { runSql = origRun }()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{}
	require.NoError(t, s.Update(sqlproc, cdc))
	require.NoError(t, s.Save(sqlproc))
	require.Empty(t, rec.statements)
}

func TestIvfpqSync_NewSync_Stateless(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	called := 0
	origRun := runSql
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		called++
		t.Fatalf("NewIvfpqSync should not call runSql: %s", sql)
		return executor.Result{}, nil
	}
	defer func() { runSql = origRun }()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)
	require.Equal(t, vectorindex.CdcTailId, s.activeIndexId)
	require.Equal(t, 0, called)
}

func TestIvfpqSync_RunOnce(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)

	defer installNextChunkIdMock(t, proc, 0)()
	rec := &recordingTxn{}
	defer rec.install(t)()

	s, err := NewIvfpqSync(sqlproc, "db", "src", "idxname",
		idxdefs("__meta", "__storage"), 4, "")
	require.NoError(t, err)

	cdc := &vectorindex.VectorIndexCdc[float32]{
		Data: []vectorindex.VectorIndexCdcEntry[float32]{
			{Type: vectorindex.CDC_DELETE, PKey: 1},
			{Type: vectorindex.CDC_INSERT, PKey: 2, Vec: []float32{4, 4, 4, 4}},
		},
	}
	require.NoError(t, s.RunOnce(sqlproc, cdc))
	require.Nil(t, s.pendingRecords)
	require.Nil(t, s.pendingSizes)
	require.NotEmpty(t, rec.statements)
}
