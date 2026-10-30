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

package iscp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func newTestHnswIgnoredTableDef(quantization string, maxBadRecords int) *plan.TableDef {
	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	params := fmt.Sprintf(`{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops","quantization":"%s","max_bad_records":"%d"}`, quantization, maxBadRecords)
	for _, idx := range tabledef.Indexes {
		idx.IndexAlgoParams = params
	}
	tabledef.Indexes = append(tabledef.Indexes, &plan.IndexDef{
		IndexName:          "hnsw_idx",
		TableExist:         true,
		IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
		IndexAlgoTableType: catalog.IndexIgnored_TblType,
		IndexTableName:     "ignored_tbl",
		Parts:              []string{"vec"},
		IndexAlgoParams:    params,
	})
	return tabledef
}

// captureIgnoredSql records the statements run, and answers a COUNT(*) with *count rows.
func captureIgnoredSql(t *testing.T) (*[]string, *int64) {
	var sqls []string
	var count int64
	mp := mpool.MustNewZero()
	old := ExecWithResult
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator, _ ...executor.StatementOption) (executor.Result, error) {
		sqls = append(sqls, sql)
		if !strings.HasPrefix(sql, "SELECT COUNT(*)") {
			return executor.Result{}, nil
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], count, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	t.Cleanup(func() { ExecWithResult = old })
	return &sqls, &count
}

func outOfRange(detail string) error { return moerr.NewIndexVectorOutOfRangeNoCtx(detail) }

func TestHnswWriterIgnoresUnstorableVectors(t *testing.T) {
	ctx := context.Background()
	tabledef := newTestHnswIgnoredTableDef("float16", 10)
	writer, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	ignored, err := newIgnoredRows("test_db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.NotNil(t, ignored)
	// the table holds rows 4 and 5
	ignored.stored = true
	writer.(ignoredRowWriter).setIgnored(ignored)
	ignored.setTs(types.BuildTS(7, 0))

	require.NoError(t, writer.Insert(ctx, []any{int64(1), []float32{1, 1}, nil}))
	// float16 cannot hold 100000
	require.NoError(t, writer.Insert(ctx, []any{int64(2), []float32{100000, 1}, nil}))
	// an update to a value that cannot be stored removes the indexed version
	require.NoError(t, writer.Upsert(ctx, []any{int64(3), []float32{1, 70000}, nil}))
	// an ignored row that is updated to a storable value or deleted leaves the table
	require.NoError(t, writer.Upsert(ctx, []any{int64(4), []float32{2, 2}, nil}))
	require.NoError(t, writer.Delete(ctx, []any{int64(5)}))

	js, err := writer.ToSql()
	require.NoError(t, err)
	require.JSONEq(t, `{"cdc":[{"t":"I","pk":1,"v":[1,1]},{"t":"D","pk":3},{"t":"U","pk":4,"v":[2,2]},{"t":"D","pk":5}]}`, string(js))

	sqls, _ := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "hnsw_idx"))
	require.Equal(t, []string{
		"DELETE FROM `test_db`.`ignored_tbl` WHERE `__mo_index_pk` IN (1,4,5)",
		fmt.Sprintf("REPLACE INTO `test_db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_ts`) VALUES (2, %d, 7), (3, %d, 7)",
			moerr.ErrIndexVectorOutOfRange, moerr.ErrIndexVectorOutOfRange),
		"SELECT COUNT(*) FROM `test_db`.`ignored_tbl`",
	}, *sqls)
}

func TestIgnoredRowsTransitions(t *testing.T) {
	ctx := context.Background()
	tabledef := newTestHnswIgnoredTableDef("float16", 10)
	ignored, err := newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)

	// while the table is empty, a row that is indexed needs no change
	ignored.indexed(int64(9))
	// ignored, then stored: the last change wins
	require.NoError(t, ignored.ignore(int64(2), outOfRange("bad")))
	ignored.indexed(int64(2))
	// stored, then ignored
	ignored.indexed(int64(1))
	require.NoError(t, ignored.ignore(int64(1), moerr.NewIndexVectorNonFiniteNoCtx()))

	sqls, _ := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Equal(t, []string{
		"DELETE FROM `db`.`ignored_tbl` WHERE `__mo_index_pk` IN (2)",
		fmt.Sprintf("REPLACE INTO `db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_ts`) VALUES (1, %d, 0)", moerr.ErrIndexVectorNonFinite),
		"SELECT COUNT(*) FROM `db`.`ignored_tbl`",
	}, *sqls)

	// nothing changed: no statement
	*sqls = nil
	require.NoError(t, ignored.load(ctx, "", nil, true, t.TempDir()))
	ignored.reset = false
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Empty(t, *sqls)

	// a rebuild replaces the table
	require.NoError(t, ignored.load(ctx, "", nil, true, t.TempDir()))
	require.NoError(t, ignored.ignore(int64(3), outOfRange("bad")))
	ignored.indexed(int64(4))
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Equal(t, []string{
		"DELETE FROM `db`.`ignored_tbl` WHERE TRUE",
		fmt.Sprintf("REPLACE INTO `db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_ts`) VALUES (3, %d, 0)", moerr.ErrIndexVectorOutOfRange),
	}, *sqls)
}

func TestNewIgnoredRowsWithoutTable(t *testing.T) {
	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	ignored, err := newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.Nil(t, ignored)

	// without an ignored table the writer leaves every vector to the index
	writer, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.NoError(t, writer.Insert(context.Background(), []any{int64(1), []float32{1, 1}, nil}))
	require.False(t, writer.Empty())
}

// An iteration never reads the ignored table's rows: it only asks whether there is one, and
// records the keys it touches only then, so its memory follows its own changes. Removals are
// written in bounded statements.
func TestIgnoredRowsLoadIsBounded(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	tabledef := newTestTableDef("pk", types.T_varbinary, "vec", types.T_array_float32, 2)
	tabledef.Indexes = newTestHnswIgnoredTableDef("float16", 10).Indexes
	ignored, err := newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)

	var sqls []string
	rows := 0
	old := ExecWithResult
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator, _ ...executor.StatementOption) (executor.Result, error) {
		sqls = append(sqls, sql)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for i := 0; i < rows; i++ {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		}
		bat.SetRowCount(rows)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	t.Cleanup(func() { ExecWithResult = old })

	require.NoError(t, ignored.load(ctx, "", nil, false, ""))
	require.Equal(t, []string{"SELECT 1 FROM `db`.`ignored_tbl` LIMIT 1"}, sqls)
	require.False(t, ignored.stored)
	for i := 0; i < 10000; i++ {
		ignored.indexed([]byte(fmt.Sprint(i)))
	}
	require.Empty(t, ignored.changes, "an empty table records nothing for indexed rows")

	rows = 1
	require.NoError(t, ignored.load(ctx, "", nil, false, ""))
	require.True(t, ignored.stored)
	n := 2*ignoredSqlBatch + 1
	for i := 0; i < n; i++ {
		ignored.indexed([]byte(fmt.Sprint(i)))
	}
	require.Len(t, ignored.changes, n, "only the keys the iteration touched")
	// a writer delivers a key as a new byte slice; it still finds the recorded change
	require.NoError(t, ignored.ignore([]byte("c"), outOfRange("bad")))
	ignored.indexed([]byte("c"))

	sqls = nil
	rows = 0
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Len(t, sqls, 3)
	total := 0
	for _, sql := range sqls {
		require.Contains(t, sql, "DELETE FROM `db`.`ignored_tbl` WHERE `__mo_index_pk` IN (")
		keys := strings.Count(sql, ",") + 1
		require.LessOrEqual(t, keys, ignoredSqlBatch)
		total += keys
	}
	require.Equal(t, n+1, total)
}

// More ignored rows than max_bad_records stop the job: a rebuild counts the rows as it records
// them and holds at most the limit; an incremental sync counts the table after its changes.
func TestIgnoredRowsLimit(t *testing.T) {
	ctx := context.Background()
	tooMany := func(err error) {
		require.True(t, isPermanentError(err), "%v", err)
		var me *moerr.Error
		require.True(t, errors.As(err, &me), "%v", err)
		require.Equal(t, moerr.ErrIndexTooManyBadRecords, me.ErrorCode())
	}

	tabledef := newTestHnswIgnoredTableDef("float16", 1)
	ignored, err := newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	// a rebuild reads each row once, and spills the rows it ignores
	spillDir := t.TempDir()
	require.NoError(t, ignored.load(ctx, "", nil, true, spillDir))
	ignored.indexed(int64(1))
	require.NoError(t, ignored.ignore(int64(2), outOfRange("c")))
	require.Empty(t, ignored.changes, "a rebuild holds no row in memory")
	err = ignored.ignore(int64(3), outOfRange("d"))
	tooMany(err)
	require.ErrorContains(t, err, "max_bad_records = 1 rows; row 3: ")
	ignored.close()
	files, err := os.ReadDir(spillDir)
	require.NoError(t, err)
	require.Empty(t, files, "the spill is removed")

	// an incremental sync: the table after the changes decides
	sqls, count := captureIgnoredSql(t)
	require.NoError(t, ignored.load(ctx, "", nil, false, ""))
	require.NoError(t, ignored.ignore(int64(5), outOfRange("e")))
	require.NoError(t, ignored.ignore(int64(6), outOfRange("f")))
	*count = 1
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Equal(t, "SELECT COUNT(*) FROM `db`.`ignored_tbl`", (*sqls)[len(*sqls)-1])
	*count = 2
	tooMany(ignored.flush(ctx, "", nil, "idx"))

	// only removals cannot add rows: no count
	require.NoError(t, ignored.load(ctx, "", nil, false, ""))
	ignored.stored = true
	ignored.indexed(int64(5))
	*sqls = nil
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Len(t, *sqls, 1)

	// max_bad_records = 0: the first unstorable row of a rebuild stops the writer
	tabledef = newTestHnswIgnoredTableDef("float16", 0)
	writer, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	ignored, err = newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	writer.(ignoredRowWriter).setIgnored(ignored)
	require.NoError(t, ignored.load(ctx, "", nil, true, t.TempDir()))
	require.NoError(t, writer.Upsert(ctx, []any{int64(1), []float32{1, 1}, nil}))
	tooMany(writer.Upsert(ctx, []any{int64(2), []float32{100000, 1}, nil}))

	// a rebuild that stores none of its rows stops the job, within the limit
	tabledef = newTestHnswIgnoredTableDef("float16", 10)
	ignored, err = newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.NoError(t, ignored.load(ctx, "", nil, true, t.TempDir()))
	require.NoError(t, ignored.ignore(int64(7), outOfRange("g")))
	require.NoError(t, ignored.ignore(int64(8), outOfRange("h")))
	err = ignored.flush(ctx, "", nil, "idx")
	require.True(t, isPermanentError(err), "%v", err)
	var me *moerr.Error
	require.True(t, errors.As(err, &me))
	require.Equal(t, moerr.ErrIndexAllRowsBad, me.ErrorCode())
	require.ErrorContains(t, err, "any of the 2 rows; row 7: ")
	// one stored row is enough
	require.NoError(t, ignored.load(ctx, "", nil, true, t.TempDir()))
	require.NoError(t, ignored.ignore(int64(7), outOfRange("g")))
	ignored.indexed(int64(9))
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))

	// an absent max_bad_records is 0; an invalid one is an error
	tabledef = newTestHnswIgnoredTableDef("float16", 0)
	for _, idx := range tabledef.Indexes {
		idx.IndexAlgoParams = `{"op_type":"vector_l2_ops"}`
	}
	ignored, err = newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.Zero(t, ignored.limit)
	for _, idx := range tabledef.Indexes {
		idx.IndexAlgoParams = `{"max_bad_records":"-1"}`
	}
	_, err = newIgnoredRows("db", tabledef, tabledef.Indexes)
	require.Error(t, err)
}

// A permanent error stops the job with the message of its cause.
func TestPermanentErrorCause(t *testing.T) {
	cause := moerr.NewIndexTooManyBadRecordsNoCtx(0, "1", "x")
	err := NewPermanentError(cause)
	require.True(t, isPermanentError(err))
	require.True(t, isPermanentError(fmt.Errorf("wrapped: %w", err)))
	require.ErrorIs(t, err, cause)
	var status JobStatus
	status.SetError(err)
	require.True(t, status.PermanentlyFailed())
	require.Equal(t, cause.Error(), status.ErrorMsg)
	require.Equal(t, "permanent error", errPermanent.Error())
	// a build past the limit, as an InitSQL REINDEX returns it, stops the job too
	require.True(t, isPermanentError(cause))
	require.True(t, isPermanentError(errors.Join(moerr.NewIndexAllRowsBadNoCtx(2, "1", "x"), errors.New("rollback"))))
	require.False(t, isPermanentError(moerr.NewIndexVectorOutOfRangeNoCtx("x")))
	require.False(t, isPermanentError(errors.New("lost connection")))
}

// SHOW INDEX STATUS shows a job stopped by a permanent error by its state in mo_iscp_log.
func TestStoppedJobState(t *testing.T) {
	require.Equal(t, ISCPJobState_Error, catalog.MoISCPJobStateError)
}
