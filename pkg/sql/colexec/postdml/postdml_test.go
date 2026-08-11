// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package postdml

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestFullText(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator

	arg := PostDml{
		PostDmlCtx: &PostDmlCtx{
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			PrimaryKeyIdx:  1,
			PrimaryKeyName: "pk",
			IsDelete:       true,
			IsInsert:       true,
			FullText: &PostDmlFullTextCtx{
				SourceTableName: "src",
				IndexTableName:  "index_tblan",
				Parts:           []string{"body", "title"},
				AlgoParams:      "",
			},
		},
		ctr: container{},
	}

	arg.GetOperatorBase()

	tn := arg.TypeName()
	require.Equal(t, tn, "postdml")

	rows := arg.AffectedRows()
	require.Equal(t, rows, uint64(0))

	rows = *arg.GetAffectedRows()
	require.Equal(t, rows, uint64(0))

	resetChildren(&arg, proc.Mp())
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)

	err = arg.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&arg, proc)
	require.NoError(t, err)

	sqls := []string{"DELETE FROM `testDb`.`index_tblan` WHERE doc_id IN (1,1000)",
		"INSERT INTO `testDb`.`index_tblan` SELECT f.* FROM `testDb`.`src` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body, src.title) as f WHERE src.pk IN (1,1000)"}

	for i, s := range sqls {
		rs, ok := proc.Base.PostDmlSqlList.Get(i)
		require.True(t, ok)
		require.Equal(t, s, rs)
	}
	arg.Free(proc, false, nil)

	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestReplaceCycleCheckUsesEvaluatedCompositePrimaryKey(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = context.Background()
	arg := PostDml{PostDmlCtx: &PostDmlCtx{
		PrimaryKeyIdx: 0,
		ReplaceCycleCheck: `{"child_schema":"d","child_table":"child","primary_key":[` +
			`{"name":"id","pos":0},{"name":"sub","pos":1}],"foreign_keys":[` +
			`{"parent_schema":"d","parent_table":"parent","child_cols":["pid"],"parent_cols":["id"]}]}`,
	}}
	require.NoError(t, arg.Prepare(proc))

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3}, []uint64{2}, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b", "ignored"}, nil, proc.Mp())
	bat.SetRowCount(3)
	require.NoError(t, arg.appendReplaceCycleChecks(proc, bat))

	got, ok := proc.Base.PostDmlSqlList.Get(0)
	require.True(t, ok)
	require.Equal(t, "REPLACE_CYCLE_CHECK:select count(*) = 0 from ("+
		"select distinct `child`.`pid` from `d`.`child` where "+
		"((`child`.`id` = 1 and `child`.`sub` = 'a') or (`child`.`id` = 2 and `child`.`sub` = 'b')) "+
		"and `child`.`pid` is not null except select distinct `parent`.`id` from `d`.`parent`) "+
		"as __mo_fk_check_source", got)

	bat.Clean(proc.Mp())
	arg.Reset(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.GetMPool().CurrNB())
}

// TestAuditFullTextStringKeyPreservesQuotedLiteral guards issue #26280: a
// character primary key must be emitted as exactly one well-formed SQL string
// literal, so the fulltext maintenance statement denotes the same key as the
// source row. Plain "'" + value + "'" wrapping breaks on three input classes:
// a bare quote yields invalid SQL (the DML aborts), a quote-comma sequence
// yields VALID SQL that silently addresses different doc_ids, and a backslash
// must be encoded differently depending on whether NO_BACKSLASH_ESCAPES is
// active.
func TestAuditFullTextStringKeyPreservesQuotedLiteral(t *testing.T) {
	testcases := []struct {
		name               string
		pk                 string
		sqlMode            string
		useCapturedSQLMode bool
		wantDelete         string
		wantInsert         string
		wantInsertLiterals []string
	}{{
		name:               "single quote stays one literal",
		pk:                 `a'b`,
		wantDelete:         "DELETE FROM `d`.`idx` WHERE doc_id IN ('a''b')",
		wantInsert:         "INSERT INTO `d`.`idx` SELECT f.* FROM `d`.`t` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('a''b')",
		wantInsertLiterals: []string{"", `a'b`},
	}, {
		name:               "quote comma must not split into two literals",
		pk:                 `x','y`,
		wantDelete:         "DELETE FROM `d`.`idx` WHERE doc_id IN ('x'',''y')",
		wantInsert:         "INSERT INTO `d`.`idx` SELECT f.* FROM `d`.`t` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('x'',''y')",
		wantInsertLiterals: []string{"", `x','y`},
	}, {
		name:               "backslash survives scanner escape decoding",
		pk:                 `C:\p`,
		wantDelete:         "DELETE FROM `d`.`idx` WHERE doc_id IN ('C:\\\\p')",
		wantInsert:         "INSERT INTO `d`.`idx` SELECT f.* FROM `d`.`t` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('C:\\\\p')",
		wantInsertLiterals: []string{"", `C:\p`},
	}, {
		name:               "no backslash escapes preserves quote and backslash",
		pk:                 `a'b\c`,
		sqlMode:            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
		wantDelete:         `DELETE FROM ` + "`d`.`idx`" + ` WHERE doc_id IN ('a''b\c')`,
		wantInsert:         `INSERT INTO ` + "`d`.`idx`" + ` SELECT f.* FROM ` + "`d`.`t`" + ` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('a''b\c')`,
		wantInsertLiterals: []string{"", `a'b\c`},
	}, {
		name:               "remote process uses captured no backslash mode",
		pk:                 `C:\p`,
		sqlMode:            "NO_BACKSLASH_ESCAPES",
		useCapturedSQLMode: true,
		wantDelete:         `DELETE FROM ` + "`d`.`idx`" + ` WHERE doc_id IN ('C:\p')`,
		wantInsert:         `INSERT INTO ` + "`d`.`idx`" + ` SELECT f.* FROM ` + "`d`.`t`" + ` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('C:\p')`,
		wantInsertLiterals: []string{"", `C:\p`},
	}, {
		name:               "remote process uses captured default mode",
		pk:                 `C:\p`,
		useCapturedSQLMode: true,
		wantDelete:         "DELETE FROM `d`.`idx` WHERE doc_id IN ('C:\\\\p')",
		wantInsert:         "INSERT INTO `d`.`idx` SELECT f.* FROM `d`.`t` as src CROSS APPLY fulltext_index_tokenize('', src.pk, src.body) as f WHERE src.pk IN ('C:\\\\p')",
		wantInsertLiterals: []string{"", `C:\p`},
	}}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.Ctx = context.TODO()
			if tc.useCapturedSQLMode {
				proc.GetSessionInfo().SqlMode = tc.sqlMode
			} else {
				proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
					require.Equal(t, "sql_mode", name)
					require.True(t, system)
					require.False(t, global)
					return tc.sqlMode, nil
				})
			}

			arg := PostDml{
				PostDmlCtx: &PostDmlCtx{
					Ref: &plan.ObjectRef{
						SchemaName: "d",
						ObjName:    "t",
					},
					PrimaryKeyIdx:  0,
					PrimaryKeyName: "pk",
					IsDelete:       true,
					IsInsert:       true,
					FullText: &PostDmlFullTextCtx{
						SourceTableName: "t",
						IndexTableName:  "idx",
						Parts:           []string{"body"},
					},
				},
				ctr: container{},
			}

			bat := batch.New([]string{"pk"})
			bat.Vecs[0] = testutil.MakeVarcharVector([]string{tc.pk}, nil, proc.Mp())
			bat.SetRowCount(1)

			op := colexec.NewMockOperator()
			op.WithBatchs([]*batch.Batch{bat})
			arg.Children = nil
			arg.AppendChild(op)

			err := arg.Prepare(proc)
			require.NoError(t, err)
			_, err = vm.Exec(&arg, proc)
			require.NoError(t, err)

			deleteSQL, ok := proc.Base.PostDmlSqlList.Get(0)
			require.True(t, ok)
			require.Equal(t, tc.wantDelete, deleteSQL)
			require.Equal(t, []string{tc.pk}, scanPostDmlStringLiterals(t, deleteSQL, tc.sqlMode))

			insertSQL, ok := proc.Base.PostDmlSqlList.Get(1)
			require.True(t, ok)
			require.Equal(t, tc.wantInsert, insertSQL)
			require.Equal(t, tc.wantInsertLiterals, scanPostDmlStringLiterals(t, insertSQL, tc.sqlMode))

			arg.Free(proc, false, nil)
			arg.Release()
			proc.Free()
			require.Equal(t, int64(0), proc.GetMPool().CurrNB())
		})
	}
}

func scanPostDmlStringLiterals(t *testing.T, sql, sqlMode string) []string {
	t.Helper()
	scanner := mysql.NewScannerWithSQLMode(dialect.MYSQL, sql, mysql.ParseSQLModeFlags(sqlMode))
	defer mysql.PutScanner(scanner)

	var literals []string
	for {
		token, value := scanner.Scan()
		if token == 0 {
			return literals
		}
		require.NotEqual(t, mysql.LEX_ERROR, token)
		if token == mysql.STRING {
			literals = append(literals, value)
		}
	}
}

func resetChildren(arg *PostDml, m *mpool.MPool) {
	op := colexec.NewMockOperator()
	bat := colexec.MakeMockBatchsWithRowID(m)
	op.WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestGetAny(t *testing.T) {
	{ // test const vector
		mp := mpool.MustNewZero()
		v := vector.NewVec(types.T_int8.ToType())
		err := vector.AppendFixed(v, int8(0), false, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		v.Free(mp)
		require.Equal(t, "0", s)
	}
	{ // test const vector
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_varchar.ToType())
		err := vector.AppendBytes(w, []byte("x"), false, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		w.Free(mp)
		require.Equal(t, "x", s)
	}
	{ // bool
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_bool.ToType())
		err := vector.AppendFixedList(w, []bool{true, false, true, false}, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "true", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int8
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_int8.ToType())
		err := vector.AppendFixedList(w, []int8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_int16.ToType())
		err := vector.AppendFixedList(w, []int16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_int32.ToType())
		err := vector.AppendFixedList(w, []int32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_int64.ToType())
		err := vector.AppendFixedList(w, []int64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uint8
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_uint8.ToType())
		err := vector.AppendFixedList(w, []uint8{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int16
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_uint16.ToType())
		err := vector.AppendFixedList(w, []uint16{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int32
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_uint32.ToType())
		err := vector.AppendFixedList(w, []uint32{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // int64
		mp := mpool.MustNewZero()
		w := vector.NewVec(types.T_uint64.ToType())
		err := vector.AppendFixedList(w, []uint64{1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(w, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		w.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // text
		mp := mpool.MustNewZero()
		v := vector.NewVec(types.T_text.ToType())
		err := vector.AppendBytesList(v, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}, nil, mp)
		require.NoError(t, err)

		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "1", s)

		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // time
		mp := mpool.MustNewZero()
		v := vector.NewVec(types.T_time.ToType())
		err := vector.AppendFixedList(v, []types.Time{12 * 3600 * 1000 * 1000, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "12:00:00", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // timestamp
		mp := mpool.MustNewZero()
		v := vector.NewVec(types.T_timestamp.ToType())
		err := vector.AppendFixedList(v, []types.Timestamp{10000000, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "0001-01-01 00:00:10.000000 UTC", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal64
		mp := mpool.MustNewZero()
		typ := types.T_decimal64.ToType()
		typ.Scale = 2
		v := vector.NewVec(typ)
		err := vector.AppendFixedList(v, []types.Decimal64{1234, 2000}, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "12.34", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // decimal128
		mp := mpool.MustNewZero()
		typ := types.T_decimal128.ToType()
		typ.Scale = 2
		v := vector.NewVec(typ)
		err := vector.AppendFixedList(v, []types.Decimal128{{B0_63: 1234, B64_127: 0}, {B0_63: 2345, B64_127: 0}}, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "12.34", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // uuid
		mp := mpool.MustNewZero()
		vs := make([]types.Uuid, 4)
		v := vector.NewVec(types.T_uuid.ToType())
		err := vector.AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // ts
		mp := mpool.MustNewZero()
		vs := make([]types.TS, 4)
		v := vector.NewVec(types.T_TS.ToType())
		err := vector.AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "0-0", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
	{ // rowid
		mp := mpool.MustNewZero()
		vs := make([]types.Rowid, 4)
		v := vector.NewVec(types.T_Rowid.ToType())
		err := vector.AppendFixedList(v, vs, nil, mp)
		require.NoError(t, err)
		s, err := GetAnyAsString(v, 0)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000-0-0-0", s)
		v.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	}
}
