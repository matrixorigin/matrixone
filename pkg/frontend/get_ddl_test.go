// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Test_handleGetDdl(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetDdl succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock database
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		// Mock relation
		mockRel := mock_frontend.NewMockRelation(ctrl)
		mockRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan2.TableDef{
			Name:     "test_table",
			DbName:   "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:     []*plan2.TableDefType{},
		}).AnyTimes()
		mockRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(123)).AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), "test_table", nil).Return(mockRel, nil).AnyTimes()

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOperator).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec for permission check
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for account name query
		erAccount := mock_frontend.NewMockExecResult(ctrl)
		erAccount.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erAccount.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("sys", nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erAccount}).AnyTimes()

		// Mock exec result for publication query
		erPub := mock_frontend.NewMockExecResult(ctrl)
		erPub.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erPub.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("test_db", nil).AnyTimes()
		erPub.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("*", nil).AnyTimes()
		erPub.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("*", nil).AnyTimes()

		// Setup system variables
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: catalog.System_Account,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("test_db")

		// Mock TxnHandler
		txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
		ses.txnHandler = txnHandler

		// Mock GetMemPool
		mp := mpool.MustNewZero()
		ses.SetMemPool(mp)

		proto.SetSession(ses)

		// Test with database and table
		dbName := tree.Identifier("test_db")
		tableName := tree.Identifier("test_table")
		stmt := &tree.GetDdl{
			Database: &dbName,
			Table:    &tableName,
		}

		convey.So(handleGetDdl(ctx, ses, stmt), convey.ShouldBeNil)
	})
}

func Test_handleGetDdl_NoTxn(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetDdl no txn error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		ses.mrs = &MysqlResultSet{}

		// TxnHandler without txn
		txnHandler := InitTxnHandler("", eng, ctx, nil)
		ses.txnHandler = txnHandler

		mp := mpool.MustNewZero()
		ses.SetMemPool(mp)

		dbName := tree.Identifier("test_db")
		stmt := &tree.GetDdl{
			Database: &dbName,
		}

		err = handleGetDdl(ctx, ses, stmt)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

func Test_visitTableDdl(t *testing.T) {
	ctx := context.Background()
	convey.Convey("visitTableDdl succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()

		// Create batch
		bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
		defer bat.Clean(mp)

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil)
		mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name:      "test_table",
			DbName:    "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
		})
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(123))

		err := visitTableDdl(ctx, "test_db", "test_table", bat, txnOperator, eng, mp)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat.RowCount(), convey.ShouldEqual, 1)
	})
}

func Test_visitTableDdl_InvalidInput(t *testing.T) {
	ctx := context.Background()
	convey.Convey("visitTableDdl invalid input", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)

		// Test nil batch
		err := visitTableDdl(ctx, "test_db", "test_table", nil, txnOperator, eng, mp)
		convey.So(err, convey.ShouldNotBeNil)

		// Test batch with insufficient columns
		bat := batch.New([]string{"col1"})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		err = visitTableDdl(ctx, "test_db", "test_table", bat, txnOperator, eng, mp)
		convey.So(err, convey.ShouldNotBeNil)
		bat.Clean(mp)

		// Test nil mpool
		bat2 := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
		err = visitTableDdl(ctx, "test_db", "test_table", bat2, txnOperator, eng, nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_getddlbatch(t *testing.T) {
	ctx := context.Background()
	convey.Convey("getddlbatch succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).AnyTimes()
		mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil).AnyTimes()
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name:      "test_table",
			DbName:    "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
		}).AnyTimes()
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(123)).AnyTimes()

		bat, err := getddlbatch(ctx, "test_db", "test_table", eng, mp, txnOperator)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat, convey.ShouldNotBeNil)
		bat.Clean(mp)
	})
}

func Test_getddlbatch_InvalidInput(t *testing.T) {
	ctx := context.Background()
	convey.Convey("getddlbatch invalid input", t, func() {
		mp := mpool.MustNewZero()

		// Test nil engine
		_, err := getddlbatch(ctx, "test_db", "test_table", nil, mp, nil)
		convey.So(err, convey.ShouldNotBeNil)

		// Test nil mpool
		eng := mock_frontend.NewMockEngine(nil)
		_, err = getddlbatch(ctx, "test_db", "test_table", eng, nil, nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_GetDdlBatchWithoutSession(t *testing.T) {
	ctx := context.Background()
	convey.Convey("GetDdlBatchWithoutSession succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).AnyTimes()
		mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil).AnyTimes()
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name:      "test_table",
			DbName:    "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
		}).AnyTimes()
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(123)).AnyTimes()
		txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOperator).AnyTimes()

		// Test without snapshot
		bat, err := GetDdlBatchWithoutSession(ctx, "test_db", "test_table", eng, txnOperator, mp, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat, convey.ShouldNotBeNil)
		bat.Clean(mp)

		// Test with snapshot
		ts := types.BuildTS(1000, 0)
		snapshotTS := ts.ToTimestamp()
		snapshot := &plan2.Snapshot{
			TS: &snapshotTS,
		}
		bat2, err := GetDdlBatchWithoutSession(ctx, "test_db", "test_table", eng, txnOperator, mp, snapshot)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat2, convey.ShouldNotBeNil)
		bat2.Clean(mp)
	})
}
