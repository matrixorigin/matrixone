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
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func newTestIvfflatIgnoredTableDef(maxBadRecords string) *plan.TableDef {
	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	tabledef.Indexes = append(tabledef.Indexes, &plan.IndexDef{
		IndexName:          "ivfidx",
		TableExist:         true,
		IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexAlgoTableType: catalog.IndexIgnored_TblType,
		IndexTableName:     "ignored_tbl",
		Parts:              []string{"vec"},
		IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops","max_bad_records":"` + maxBadRecords + `"}`,
	})
	return tabledef
}

// stubIgnoredCount answers the count of the ignored table with n rows, a sample key and code.
func stubIgnoredCount(t *testing.T, n int64) *[]string {
	return stubIgnoredCountPk(t, n, []byte("7"))
}

// stubIgnoredCountPk is stubIgnoredCount with the sample key pk.
func stubIgnoredCountPk(t *testing.T, n int64, pk []byte) *[]string {
	var sqls []string
	mp := mpool.MustNewZero()
	old := ExecWithResult
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator, _ ...executor.StatementOption) (executor.Result, error) {
		sqls = append(sqls, sql)
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
		if n > 0 {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], n, false, mp))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], pk, false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(moerr.ErrIndexVectorOutOfRange), false, mp))
			bat.SetRowCount(1)
		}
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	t.Cleanup(func() { ExecWithResult = old })
	return &sqls
}

func TestIvfflatWriterIgnoredTable(t *testing.T) {
	ctx := context.Background()
	wr, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), newTestIvfflatIgnoredTableDef("2"), newTestIvfflatIgnoredTableDef("2").Indexes)
	require.NoError(t, err)
	w := wr.(*IvfflatSqlWriter)
	require.True(t, w.hasIgnored())
	require.Equal(t, int64(2), w.maxBadRecords)

	// the upsert passes the index to product_l2 and names the key column for it
	require.NoError(t, w.Insert(ctx, []any{int64(1), []float32{1, 2}, nil}))
	require.NoError(t, w.Insert(ctx, []any{int64(2), nil, nil}))
	require.Equal(t, int64(1), w.rows, "a NULL vector is not a row the index stores")
	sql, err := w.ToSql()
	require.NoError(t, err)
	require.Contains(t, string(sql), "as `__mo_index_pri_col`")
	require.Contains(t, string(sql), `"ignored":"ignored_tbl"`)
	require.Contains(t, string(sql), `"max_bad_records":2`)
	require.Nil(t, w.followingSql())

	// a delete removes the same keys from the entries and from the ignored rows
	require.NoError(t, w.Delete(ctx, []any{int64(3)}))
	sql, err = w.ToSql()
	require.NoError(t, err)
	require.Equal(t, "DELETE FROM `test_db`.`entries_tbl` WHERE `__mo_index_pri_col` IN (3)", string(sql))
	require.Equal(t, "DELETE FROM `test_db`.`ignored_tbl` WHERE `__mo_index_pk` IN (3)", string(w.followingSql()))
	require.Nil(t, w.followingSql(), "the following statement is handed out once")

	sqls := stubIgnoredCount(t, 2)
	require.NoError(t, w.checkIgnored(ctx, "", nil, false), "2 rows within the limit")
	require.Contains(t, (*sqls)[0], "FROM `test_db`.`ignored_tbl`")
	require.Equal(t, int64(0), w.rows)
	sqls = stubIgnoredCount(t, 3)
	require.NoError(t, w.checkIgnored(ctx, "", nil, false), "an iteration that stores no row")
	require.Empty(t, *sqls)

	w.rows = 1
	err = w.checkIgnored(ctx, "", nil, false)
	require.True(t, isPermanentError(err), "%v", err)
	require.ErrorContains(t, err, "max_bad_records = 2 rows; row 7")

	// a rebuild that stores none of the rows it upserted stops
	stubIgnoredCount(t, 1)
	require.NoError(t, w.Upsert(ctx, []any{int64(4), []float32{1e30, 1}, nil}))
	err = w.checkIgnored(ctx, "", nil, true)
	require.True(t, isPermanentError(err), "%v", err)
	require.ErrorContains(t, err, "cannot store any of the 1 rows")
	stubIgnoredCount(t, 0)
	require.NoError(t, w.checkIgnored(ctx, "", nil, true))

	// the serial of a composite key reads as its hex literal
	p := types.NewPacker()
	defer p.Close()
	p.EncodeInt64(1)
	p.EncodeStringType([]byte("x"))
	w.tabledef.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"pk", "vec"}}
	stubIgnoredCountPk(t, 3, p.GetBuf())
	w.rows = 1
	require.ErrorContains(t, w.checkIgnored(ctx, "", nil, false), "max_bad_records = 2 rows; row "+vectorindex.SerialPkString(p.GetBuf()))
}

func TestIvfflatWriterWithoutIgnoredTable(t *testing.T) {
	ctx := context.Background()
	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	wr, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	w := wr.(*IvfflatSqlWriter)
	require.False(t, w.hasIgnored())
	require.NoError(t, w.Insert(ctx, []any{int64(1), []float32{1, 2}, nil}))
	sql, err := w.ToSql()
	require.NoError(t, err)
	require.Contains(t, string(sql), "CENTROIDX('vector_l2_ops') JOIN")
	require.NoError(t, w.Delete(ctx, []any{int64(3)}))
	_, err = w.ToSql()
	require.NoError(t, err)
	require.Nil(t, w.followingSql())
	require.NoError(t, w.checkIgnored(ctx, "", nil, true))

	c := &IndexConsumer{sqlWriter: w}
	require.False(t, c.recordsIgnored())
	c.sqlWriter, _ = NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), newTestIvfflatIgnoredTableDef("0"), newTestIvfflatIgnoredTableDef("0").Indexes)
	require.True(t, c.recordsIgnored())
}

// sendSql sends the statement that follows a writer's statement right after it.
func TestSendSqlFollowing(t *testing.T) {
	ctx := context.Background()
	wr, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), newTestIvfflatIgnoredTableDef("2"), newTestIvfflatIgnoredTableDef("2").Indexes)
	require.NoError(t, err)
	require.NoError(t, wr.Delete(ctx, []any{int64(3)}))
	c := &IndexConsumer{sqlWriter: wr, sqlBufSendCh: make(chan []byte, 2), info: newTestConsumerInfo()}
	require.NoError(t, c.sendSql(ctx, make(chan error, 1), wr))
	require.Len(t, c.sqlBufSendCh, 2)
	require.True(t, strings.HasPrefix(string(<-c.sqlBufSendCh), "DELETE FROM `test_db`.`entries_tbl`"))
	require.True(t, strings.HasPrefix(string(<-c.sqlBufSendCh), "DELETE FROM `test_db`.`ignored_tbl`"))
}

func newTestFulltext2IgnoredWriter(t *testing.T, pkType types.T, parser string) (*Fulltext2SqlWriter, *ignoredRows) {
	tabledef := &plan.TableDef{
		Name2ColIndex: map[string]int32{"id": 0, "body": 1},
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(pkType), Scale: 2}},
			{Name: "body", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	params := `{"parser":"` + parser + `","max_bad_records":"5"}`
	indexdef := []*plan.IndexDef{
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store", Parts: []string{"body"}, IndexAlgoParams: params},
		{IndexName: "ft2", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta", Parts: []string{"body"}, IndexAlgoParams: params},
		{IndexName: "ft2", IndexAlgoTableType: catalog.IndexIgnored_TblType, IndexTableName: "__ignored", Parts: []string{"body"}, IndexAlgoParams: params},
	}
	wr, err := NewFulltext2SqlWriter("fulltext2", JobID{}, &ConsumerInfo{DBName: "db"}, tabledef, indexdef)
	require.NoError(t, err)
	g, err := newIgnoredRows("db", tabledef, indexdef)
	require.NoError(t, err)
	w := wr.(*Fulltext2SqlWriter)
	w.setIgnored(g)
	g.stored = true
	return w, g
}

func TestFulltext2WriterIgnoresInvalidJSON(t *testing.T) {
	ctx := context.Background()
	w, g := newTestFulltext2IgnoredWriter(t, types.T_int64, "json")

	require.NoError(t, w.Insert(ctx, []any{int64(1), []byte(`{"a":"x"}`)}))
	require.NoError(t, w.Insert(ctx, []any{int64(2), []byte(`{bad`)}))
	require.Equal(t, 1, w.cdc.Len(), "the bad row is left out")
	require.NoError(t, w.Upsert(ctx, []any{int64(1), []byte(`[bad`)}))
	require.Equal(t, 2, w.cdc.Len(), "an upsert to a bad value deletes the version the index holds")
	require.NoError(t, w.Delete(ctx, []any{int64(2)}))
	require.Equal(t, int64(1), g.count)

	sqls, _ := captureIgnoredSql(t)
	require.NoError(t, g.flush(ctx, "", nil, "ft2"))
	joined := strings.Join(*sqls, ";")
	require.Contains(t, joined, "DELETE FROM `db`.`__ignored` WHERE `__mo_index_pk` IN (2)")
	require.Contains(t, joined, "(1, 20351, 'the text the index parses as JSON is not valid JSON: ")

	// an error that is not about the row's text is returned
	w.ignored = nil
	err := w.Insert(ctx, []any{int64(3), []byte(`{bad`)})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexInvalidJSON), "%v", err)
}

// A native primary key is recorded as the SQL string the tracker writes.
func TestFulltext2WriterIgnoredNativeKey(t *testing.T) {
	ctx := context.Background()
	w, g := newTestFulltext2IgnoredWriter(t, types.T_decimal64, "json")
	d, err := types.Decimal64FromFloat64(12.5, 10, 2)
	require.NoError(t, err)
	require.NoError(t, w.Insert(ctx, []any{d, []byte(`{bad`)}))
	sqls, _ := captureIgnoredSql(t)
	require.NoError(t, g.flush(ctx, "", nil, "ft2"))
	require.Contains(t, strings.Join(*sqls, ";"), "('12.50', 20351, ")
}

func TestSqlStringRepr(t *testing.T) {
	typ := &types.Type{Scale: 2}
	dt, err := types.ParseDatetime("2026-11-03 10:00:00.25", 2)
	require.NoError(t, err)
	require.Equal(t, "2026-11-03 10:00:00.25", sqlStringRepr(dt, typ))
	tm, err := types.ParseTime("10:00:00.25", 2)
	require.NoError(t, err)
	require.Equal(t, "10:00:00.25", sqlStringRepr(tm, typ))
	ts, err := types.ParseTimestamp(nil, "2026-11-03 10:00:00.25", 2)
	require.NoError(t, err)
	require.Equal(t, "2026-11-03 10:00:00.25", sqlStringRepr(ts, typ))
	d128, err := types.Decimal128FromFloat64(3.5, 20, 2)
	require.NoError(t, err)
	require.Equal(t, "3.50", sqlStringRepr(d128, typ))
	u, err := types.ParseUuid("6d1b1fdb-5dc2-11ed-bbe0-a4bb6d0c0b97")
	require.NoError(t, err)
	require.Equal(t, "6d1b1fdb-5dc2-11ed-bbe0-a4bb6d0c0b97", sqlStringRepr(u, typ))
	require.Equal(t, int64(4), sqlStringRepr(int64(4), typ))
}

func TestSqlLiteral(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("it's"), false, mp))
	lit, err := SqlLiteral(context.Background(), vec, 0)
	require.NoError(t, err)
	require.Equal(t, `'it\'s'`, lit)
	c, err := vector.NewConstFixed(types.T_int32.ToType(), int32(9), 3, mp)
	require.NoError(t, err)
	lit, err = SqlLiteral(context.Background(), c, 2)
	require.NoError(t, err)
	require.Equal(t, "9", lit)
}
