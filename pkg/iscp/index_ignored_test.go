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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func newTestHnswIgnoredTableDef(quantization string) *plan.TableDef {
	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	params := `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops","quantization":"` + quantization + `"}`
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

func captureIgnoredSql(t *testing.T) *[]string {
	var sqls []string
	old := ExecWithResult
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator, _ ...executor.StatementOption) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	}
	t.Cleanup(func() { ExecWithResult = old })
	return &sqls
}

func TestHnswWriterIgnoresUnstorableVectors(t *testing.T) {
	ctx := context.Background()
	tabledef := newTestHnswIgnoredTableDef("float16")
	writer, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	ignored := newIgnoredRows("test_db", tabledef, tabledef.Indexes)
	require.NotNil(t, ignored)
	ignored.present = map[any]struct{}{int64(4): {}, int64(5): {}}
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

	sqls := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "hnsw_idx"))
	require.Equal(t, []string{
		"DELETE FROM `test_db`.`ignored_tbl` WHERE `__mo_index_pk` IN (4,5)",
		"REPLACE INTO `test_db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_reason`, `__mo_index_ts`) VALUES " +
			"(2, 'data out of range: data type hnsw quantization, vector element 100000 cannot be stored as F16', 7), " +
			"(3, 'data out of range: data type hnsw quantization, vector element 70000 cannot be stored as F16', 7)",
	}, *sqls)
}

func TestIgnoredRowsTransitions(t *testing.T) {
	ctx := context.Background()
	tabledef := newTestHnswIgnoredTableDef("float16")
	ignored := newIgnoredRows("db", tabledef, tabledef.Indexes)
	ignored.present = map[any]struct{}{int64(1): {}}

	// a row not in the table that is indexed needs no change
	ignored.indexed(int64(9))
	// ignored, then stored: the last change wins
	ignored.ignore(int64(2), "bad")
	ignored.indexed(int64(2))
	// stored, then ignored
	ignored.indexed(int64(1))
	ignored.ignore(int64(1), "worse")

	sqls := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Equal(t, []string{
		"DELETE FROM `db`.`ignored_tbl` WHERE `__mo_index_pk` IN (2)",
		"REPLACE INTO `db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_reason`, `__mo_index_ts`) VALUES (1, 'worse', 0)",
	}, *sqls)

	// nothing changed: no statement
	*sqls = nil
	require.NoError(t, ignored.load(ctx, "", nil, true))
	ignored.reset = false
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Empty(t, *sqls)

	// a rebuild replaces the table
	require.NoError(t, ignored.load(ctx, "", nil, true))
	ignored.ignore(int64(3), "it's bad")
	ignored.indexed(int64(4))
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Equal(t, []string{
		"DELETE FROM `db`.`ignored_tbl` WHERE TRUE",
		"REPLACE INTO `db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_reason`, `__mo_index_ts`) VALUES (3, 'it''s bad', 0)",
	}, *sqls)
}

func TestNewIgnoredRowsWithoutTable(t *testing.T) {
	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	require.Nil(t, newIgnoredRows("db", tabledef, tabledef.Indexes))

	// without an ignored table the writer leaves every vector to the index
	writer, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.NoError(t, writer.Insert(context.Background(), []any{int64(1), []float32{1, 1}, nil}))
	require.False(t, writer.Empty())
}

func TestIgnoredRowsLoadsKeysAsTheWriterSeesThem(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	tabledef := newTestTableDef("pk", types.T_varbinary, "vec", types.T_array_float32, 2)
	tabledef.Indexes = newTestHnswIgnoredTableDef("float16").Indexes
	ignored := newIgnoredRows("db", tabledef, tabledef.Indexes)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
	for _, k := range []string{"a\x00", "b\x01"} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(k), false, mp))
	}
	bat.SetRowCount(2)
	old := ExecWithResult
	ExecWithResult = func(context.Context, string, string, client.TxnOperator, ...executor.StatementOption) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	t.Cleanup(func() { ExecWithResult = old })
	require.NoError(t, ignored.load(ctx, "", nil, false))
	require.Len(t, ignored.present, 2)

	// a writer delivers a key as a new byte slice; it still finds the recorded row
	ignored.indexed([]byte("a\x00"))
	ignored.ignore([]byte("c"), "bad")
	sqls := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "idx"))
	require.Len(t, *sqls, 2)
	require.Contains(t, (*sqls)[0], "DELETE FROM `db`.`ignored_tbl` WHERE `__mo_index_pk` IN (")
	require.Contains(t, (*sqls)[1], "'bad'")
}
