// Copyright 2024 Matrix Origin
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

package casgc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// makeSqlEnv returns a minimal *sqlEnv for testing (fields are nil — safe
// because runTxnWithSqlContext and runSql are both stubbed before use).
func makeSqlEnv() *sqlEnv {
	return &sqlEnv{uuid: "test-uuid"}
}

// stubTxnPassthrough stubs runTxnWithSqlContext to call f directly without
// touching the real engine or txn client, then returns the stub so the caller
// can defer stub.Reset().
func stubTxnPassthrough() *gostub.Stubs {
	return gostub.Stub(&runTxnWithSqlContext,
		func(
			_ context.Context,
			_ engine.Engine,
			_ client.TxnClient,
			_ string,
			_ uint32,
			_ time.Duration,
			_ func(string, bool, bool) (interface{}, error),
			cbdata any,
			f func(*sqlexec.SqlProcess, any) error,
		) error {
			return f(&sqlexec.SqlProcess{}, cbdata)
		},
	)
}

// makeInt32Batch creates an executor.Result with a single T_int32 column
// populated with vals, using nullMask to mark null rows.
func makeInt32Batch(mp *mpool.MPool, vals []int32, nullMask []bool) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	for i, v := range vals {
		isNull := len(nullMask) > i && nullMask[i]
		_ = vector.AppendFixed[int32](bat.Vecs[0], v, isNull, mp)
	}
	bat.SetRowCount(len(vals))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// makeVarcharBatch creates an executor.Result with a single T_varchar column.
func makeVarcharBatch(mp *mpool.MPool, vals []string, nullMask []bool) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	for i, v := range vals {
		isNull := len(nullMask) > i && nullMask[i]
		_ = vector.AppendBytes(bat.Vecs[0], []byte(v), isNull, mp)
	}
	bat.SetRowCount(len(vals))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// makeColumnsBatch creates a 4-column batch matching the mo_columns query:
// att_database (varchar), att_relname (varchar), attname (varchar), atttyp (varbinary).
func makeColumnsBatch(
	mp *mpool.MPool,
	dbs, tables, cols []string,
	atttypes [][]byte,
	typNullMask []bool,
) executor.Result {
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	bat.Vecs[3] = vector.NewVec(types.New(types.T_varbinary, types.MaxVarcharLen, 0))

	n := len(dbs)
	for i := 0; i < n; i++ {
		_ = vector.AppendBytes(bat.Vecs[0], []byte(dbs[i]), false, mp)
		_ = vector.AppendBytes(bat.Vecs[1], []byte(tables[i]), false, mp)
		_ = vector.AppendBytes(bat.Vecs[2], []byte(cols[i]), false, mp)
		isNull := len(typNullMask) > i && typNullMask[i]
		_ = vector.AppendBytes(bat.Vecs[3], atttypes[i], isNull, mp)
	}
	bat.SetRowCount(n)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// makeSnapshotsBatch creates a 2-column batch: sname (varchar), ts (T_int64).
func makeSnapshotsBatch(mp *mpool.MPool, names []string, ts []int64, nameNullMask []bool) executor.Result {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))

	n := len(names)
	for i := 0; i < n; i++ {
		isNull := len(nameNullMask) > i && nameNullMask[i]
		_ = vector.AppendBytes(bat.Vecs[0], []byte(names[i]), isNull, mp)
		_ = vector.AppendFixed[int64](bat.Vecs[1], ts[i], false, mp)
	}
	bat.SetRowCount(n)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// ---------------------------------------------------------------------------
// TestListAccountIDs
// ---------------------------------------------------------------------------

func TestListAccountIDs(t *testing.T) {
	mp := mpool.MustNewZero()

	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	// rows: account_id values [1, 0 (sys), 5, null]
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return makeInt32Batch(mp,
				[]int32{1, 0, 5, 0},
				[]bool{false, false, false, true}, // last row is null
			), nil
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	ids, err := env.listAccountIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 0, 5}, ids)
}

func TestListAccountIDs_Error(t *testing.T) {
	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	wantErr := errors.New("sql error")
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{}, wantErr
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	_, err := env.listAccountIDs(context.Background())
	require.ErrorIs(t, err, wantErr)
}

// ---------------------------------------------------------------------------
// TestDatalinkColumns
// ---------------------------------------------------------------------------

func TestDatalinkColumns(t *testing.T) {
	mp := mpool.MustNewZero()

	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	// Build atttyp bytes: one datalink, one varchar, one will be null.
	dlType := types.New(types.T_datalink, 0, 0)
	dlBytes := types.EncodeType(&dlType)
	vcType := types.New(types.T_varchar, 255, 0)
	vcBytes := types.EncodeType(&vcType)

	var capturedSQL string
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			capturedSQL = sql
			return makeColumnsBatch(
				mp,
				[]string{"mydb", "otherdb", "anydb"},
				[]string{"t1", "t2", "t3"},
				[]string{"dl_col", "vc_col", "null_col"},
				[][]byte{dlBytes, vcBytes, {}},
				// third row has null atttyp
				[]bool{false, false, true},
			), nil
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 7}
	cols, err := refs.datalinkColumns(context.Background())
	require.NoError(t, err)

	// Only the datalink-typed row should survive.
	require.Len(t, cols, 1)
	require.Equal(t, "mydb", cols[0].DBName)
	require.Equal(t, "t1", cols[0].TableName)
	require.Equal(t, "dl_col", cols[0].ColName)

	// SQL must contain the mo_columns query with system-db exclusion and hidden filter.
	require.Contains(t, capturedSQL, "mo_catalog.mo_columns")
	require.Contains(t, capturedSQL, "mo_catalog")
	require.Contains(t, capturedSQL, "att_is_hidden = 0")
}

func TestDatalinkColumns_Error(t *testing.T) {
	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	wantErr := errors.New("columns error")
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{}, wantErr
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 7}
	_, err := refs.datalinkColumns(context.Background())
	require.ErrorIs(t, err, wantErr)
}

// ---------------------------------------------------------------------------
// TestScanColumn
// ---------------------------------------------------------------------------

func TestScanColumn_Live(t *testing.T) {
	mp := mpool.MustNewZero()

	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	var capturedSQL string
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			capturedSQL = sql
			return makeVarcharBatch(
				mp,
				[]string{"mo://a?x=1", "mo://b?x=2", ""},
				[]bool{false, false, true}, // last row is null
			), nil
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 3}
	ref := columnRef{DBName: "mydb", TableName: "mytable", ColName: "dl"}
	vals, err := refs.scanColumn(context.Background(), ref, "")
	require.NoError(t, err)
	require.Equal(t, []string{"mo://a?x=1", "mo://b?x=2"}, vals)

	// SQL should be a backtick-quoted SELECT without snapshot hint.
	require.True(t, strings.HasPrefix(capturedSQL, "SELECT `dl` FROM `mydb`.`mytable`"),
		"unexpected SQL: %q", capturedSQL)
	require.False(t, strings.Contains(capturedSQL, "{snapshot"), "live query must not have snapshot hint")
}

func TestScanColumn_WithSnapshot(t *testing.T) {
	mp := mpool.MustNewZero()

	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	var capturedSQL string
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			capturedSQL = sql
			return makeVarcharBatch(mp, []string{"mo://x"}, nil), nil
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 3}
	ref := columnRef{DBName: "db2", TableName: "tbl2", ColName: "c"}
	vals, err := refs.scanColumn(context.Background(), ref, "{snapshot = 'snap1'}")
	require.NoError(t, err)
	require.Equal(t, []string{"mo://x"}, vals)

	require.Contains(t, capturedSQL, "{snapshot = 'snap1'}")
}

func TestScanColumn_Error(t *testing.T) {
	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	wantErr := errors.New("scan error")
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{}, wantErr
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 3}
	ref := columnRef{DBName: "db", TableName: "tbl", ColName: "c"}
	_, err := refs.scanColumn(context.Background(), ref, "")
	require.ErrorIs(t, err, wantErr)
}

// ---------------------------------------------------------------------------
// TestLiveSnapshots
// ---------------------------------------------------------------------------

func TestLiveSnapshots(t *testing.T) {
	mp := mpool.MustNewZero()

	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	// Two rows; second has null name (should be skipped).
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return makeSnapshotsBatch(
				mp,
				[]string{"snap_a", "snap_b"},
				[]int64{1000, 2000},
				[]bool{false, true}, // snap_b name is null → skipped
			), nil
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 5}
	snaps, err := refs.liveSnapshots(context.Background())
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	require.Equal(t, snapshotRef{Name: "snap_a", TS: 1000}, snaps[0])
}

func TestLiveSnapshots_Error(t *testing.T) {
	stubTxn := stubTxnPassthrough()
	defer stubTxn.Reset()

	wantErr := errors.New("snapshots error")
	stubSQL := gostub.Stub(&runSql,
		func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{}, wantErr
		},
	)
	defer stubSQL.Reset()

	env := makeSqlEnv()
	refs := &acctRefs{env: env, accountID: 5}
	_, err := refs.liveSnapshots(context.Background())
	require.ErrorIs(t, err, wantErr)
}

// ---------------------------------------------------------------------------
// TestRefsForAccount
// ---------------------------------------------------------------------------

func TestRefsForAccount(t *testing.T) {
	env := makeSqlEnv()
	refs, err := env.refsForAccount(context.Background(), 42)
	require.NoError(t, err)
	ar, ok := refs.(*acctRefs)
	require.True(t, ok)
	require.Equal(t, uint32(42), ar.accountID)
	require.Equal(t, env, ar.env)
}

// ---------------------------------------------------------------------------
// TestExecutorMetadata — covers executor.go cheaply
// ---------------------------------------------------------------------------

func TestDatalinkCASGCTaskMetadata(t *testing.T) {
	md := DatalinkCASGCTaskMetadata(task.TaskCode_DatalinkCASGCExecutor)
	require.Equal(t, "DatalinkCASGCTask", md.ID)
	require.Equal(t, task.TaskCode_DatalinkCASGCExecutor, md.Executor)
	require.Equal(t, uint32(1), md.Options.Concurrency)
}
