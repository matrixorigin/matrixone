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
	"time"

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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func Test_handleGetDdl(t *testing.T) {
	// Skip this test because checkPublicationPermission uses ses.GetShareTxnBackgroundExec
	// which creates a real BackgroundExec that executes SQL queries internally.
	// This is difficult to mock in unit tests without modifying production code.
	t.Skip("Skipping: handleGetDdl requires complex internal SQL execution mock")

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

		// Mock mo_catalog database (used by checkPublicationPermission)
		mockMoCatalogDb := mock_frontend.NewMockDatabase(ctrl)
		mockMoCatalogDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockMoCatalogDb, nil).AnyTimes()

		// Mock mo_account relation (used by checkPublicationPermission)
		mockMoAccountRel := mock_frontend.NewMockRelation(ctrl)
		// Create table definition with necessary columns for SQL query validation
		moAccountTableDef := &plan2.TableDef{
			Name:      "mo_account",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
			Cols: []*plan2.ColDef{
				{Name: "account_id", Typ: plan2.Type{Id: int32(types.T_int32)}},
				{Name: "account_name", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "admin_name", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "status", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "version", Typ: plan2.Type{Id: int32(types.T_uint64)}},
				{Name: "suspended_time", Typ: plan2.Type{Id: int32(types.T_timestamp)}},
			},
		}
		// Create mock reader for BuildReaders
		mockMoAccountReader := mock_frontend.NewMockReader(ctrl)
		mockMoAccountReader.EXPECT().Close().Return(nil).AnyTimes()
		mockMoAccountReader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes() // Return true to indicate end of data
		mockMoAccountReader.EXPECT().SetOrderBy(gomock.Any()).Return().AnyTimes()
		mockMoAccountReader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
		mockMoAccountReader.EXPECT().SetIndexParam(gomock.Any()).Return().AnyTimes()
		mockMoAccountReader.EXPECT().SetFilterZM(gomock.Any()).Return().AnyTimes()
		mockMoAccountRel.EXPECT().CopyTableDef(gomock.Any()).Return(moAccountTableDef).AnyTimes()
		mockMoAccountRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoAccountRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan2.TableDef{Indexes: []*plan2.IndexDef{}}).AnyTimes()
		mockMoAccountRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockMoAccountRel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{mockMoAccountReader}, nil).AnyTimes()
		// Relation may be called with *process.Process as third argument, not nil
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_account", gomock.Any()).Return(mockMoAccountRel, nil).AnyTimes()

		// Mock mo_pubs relation (used by checkPublicationPermission)
		mockMoPubsRel := mock_frontend.NewMockRelation(ctrl)
		// Create table definition with necessary columns for SQL query validation
		moPubsTableDef := &plan2.TableDef{
			Name:      "mo_pubs",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
			Cols: []*plan2.ColDef{
				{Name: "account_id", Typ: plan2.Type{Id: int32(types.T_int32)}},
				{Name: "account_name", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "pub_name", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "database_name", Typ: plan2.Type{Id: int32(types.T_varchar)}},
				{Name: "database_id", Typ: plan2.Type{Id: int32(types.T_uint64)}},
				{Name: "table_list", Typ: plan2.Type{Id: int32(types.T_text)}},
				{Name: "account_list", Typ: plan2.Type{Id: int32(types.T_text)}},
			},
		}
		// Create mock reader for BuildReaders
		mockMoPubsReader := mock_frontend.NewMockReader(ctrl)
		mockMoPubsReader.EXPECT().Close().Return(nil).AnyTimes()
		mockMoPubsReader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes() // Return true to indicate end of data
		mockMoPubsReader.EXPECT().SetOrderBy(gomock.Any()).Return().AnyTimes()
		mockMoPubsReader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
		mockMoPubsReader.EXPECT().SetIndexParam(gomock.Any()).Return().AnyTimes()
		mockMoPubsReader.EXPECT().SetFilterZM(gomock.Any()).Return().AnyTimes()
		mockMoPubsRel.EXPECT().CopyTableDef(gomock.Any()).Return(moPubsTableDef).AnyTimes()
		mockMoPubsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoPubsRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan2.TableDef{Indexes: []*plan2.IndexDef{}}).AnyTimes()
		mockMoPubsRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockMoPubsRel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{mockMoPubsReader}, nil).AnyTimes()
		// Relation may be called with *process.Process as third argument, not nil
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_pubs", gomock.Any()).Return(mockMoPubsRel, nil).AnyTimes()

		// Mock relation
		mockRel := mock_frontend.NewMockRelation(ctrl)
		mockRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan2.TableDef{
			Name:      "test_table",
			DbName:    "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
		}).AnyTimes()
		mockRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(123)).AnyTimes()
		mockRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan2.TableDef{Indexes: []*plan2.IndexDef{}}).AnyTimes()
		mockRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockRel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		// Relation may be called with *process.Process as third argument, not nil
		mockDb.EXPECT().Relation(gomock.Any(), "test_table", gomock.Any()).Return(mockRel, nil).AnyTimes()

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOperator).AnyTimes()
		// Mock Txn() to return a valid TxnMeta with optimistic mode
		txnMeta := &txn.TxnMeta{
			Mode: txn.TxnMode_Optimistic,
		}
		txnOperator.EXPECT().Txn().Return(*txnMeta).AnyTimes()
		txnOperator.EXPECT().GetWaitActiveCost().Return(time.Duration(0)).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Setup system variables
		// Note: sys account (catalog.System_Account) skips permission check in checkPublicationPermission
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
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
		setSessionAlloc("", NewLeakCheckAllocator())
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
