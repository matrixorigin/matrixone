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

	proc := testutil.NewProc()
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

	resetChildren(&arg)
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

func resetChildren(arg *PostDml) {
	op := colexec.NewMockOperator()
	bat := colexec.MakeMockBatchsWithRowID()
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
