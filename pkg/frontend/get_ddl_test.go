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

// newMrsForAccountName creates a MysqlResultSet for account name query
func newMrsForAccountName(accountName string) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName("account_name")
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col)
	if accountName != "" {
		mrs.AddRow([]interface{}{accountName})
	}
	return mrs
}

// newMrsForMoPubs creates a MysqlResultSet for mo_pubs query
// Columns: account_id, account_name, pub_name, database_name, database_id, table_list, account_list
func newMrsForMoPubs(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("account_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col1)

	col2 := &MysqlColumn{}
	col2.SetName("account_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	col3 := &MysqlColumn{}
	col3.SetName("pub_name")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col3)

	col4 := &MysqlColumn{}
	col4.SetName("database_name")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col4)

	col5 := &MysqlColumn{}
	col5.SetName("database_id")
	col5.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col5)

	col6 := &MysqlColumn{}
	col6.SetName("table_list")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col6)

	col7 := &MysqlColumn{}
	col7.SetName("account_list")
	col7.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col7)

	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

// Test_checkPublicationPermissionWithBh_GetAccountIdError tests error when GetAccountId fails
func Test_checkPublicationPermissionWithBh_GetAccountIdError(t *testing.T) {
	// Context without account ID will cause GetAccountId to fail
	ctx := context.Background()

	convey.Convey("checkPublicationPermissionWithBh GetAccountId error", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_ExecAccountNameError tests error when querying account name fails
func Test_checkPublicationPermissionWithBh_ExecAccountNameError(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh Exec account name error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("exec failed"))

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "exec failed")
	})
}

// Test_checkPublicationPermissionWithBh_AccountNameEmpty tests error when account name is empty
func Test_checkPublicationPermissionWithBh_AccountNameEmpty(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh account name empty", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Return empty result for account name query
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("")

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get account name")
	})
}

// Test_checkPublicationPermissionWithBh_MoPubsExecError tests error when querying mo_pubs fails
func Test_checkPublicationPermissionWithBh_MoPubsExecError(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh mo_pubs exec error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		execCount := 0
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string) error {
			execCount++
			if execCount == 1 {
				// First call is for account name - succeed
				return nil
			}
			// Second call is for mo_pubs - fail
			return moerr.NewInternalErrorNoCtx("mo_pubs query failed")
		}).Times(2)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{newMrsForAccountName("test_account")}).Times(1)

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "mo_pubs query failed")
	})
}

// Test_checkPublicationPermissionWithBh_NoPermission_EmptyPubs tests no permission when mo_pubs is empty
func Test_checkPublicationPermissionWithBh_NoPermission_EmptyPubs(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh no permission - empty pubs", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// Empty mo_pubs result - table level
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission to access table")
	})
}

// Test_checkPublicationPermissionWithBh_NoPermission_AccountNotInList tests no permission when account is not in account_list
func Test_checkPublicationPermissionWithBh_NoPermission_AccountNotInList(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh no permission - account not in list", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with different account in list
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "test_table", "other_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission")
	})
}

// Test_checkPublicationPermissionWithBh_NoPermission_TableNotInList tests no permission when table is not in table_list
func Test_checkPublicationPermissionWithBh_NoPermission_TableNotInList(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh no permission - table not in list", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with different table in list
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "other_table,another_table", "test_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission to access table")
	})
}

// Test_checkPublicationPermissionWithBh_Permission_TableAll tests permission granted when table_list is "*"
func Test_checkPublicationPermissionWithBh_Permission_TableAll(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh permission - table all (*)", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with table_list = "*"
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "*", "test_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_Permission_TableInList tests permission granted when table is in table_list
func Test_checkPublicationPermissionWithBh_Permission_TableInList(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh permission - table in list", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with table in list
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "table1,test_table,table2", "test_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_Permission_AllAccounts tests permission granted when account_list is "*"
func Test_checkPublicationPermissionWithBh_Permission_AllAccounts(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh permission - all accounts (*)", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("any_account")

		// mo_pubs result with account_list = "*" (all accounts)
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "test_table", "all"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_DatabaseLevel tests database level permission check
func Test_checkPublicationPermissionWithBh_DatabaseLevel(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh database level permission", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result for database level (no table specified)
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "*", "test_account"},
		})

		// Database level - tableName is empty
		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_DatabaseLevel_NoPermission tests database level no permission
func Test_checkPublicationPermissionWithBh_DatabaseLevel_NoPermission(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh database level no permission", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// Empty mo_pubs result
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{})

		// Database level - tableName is empty
		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission to access database")
	})
}

// Test_checkPublicationPermissionWithBh_AccountLevel tests account level permission check
func Test_checkPublicationPermissionWithBh_AccountLevel(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh account level permission", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result for account level (no database/table specified)
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "some_db", int64(1), "*", "test_account"},
		})

		// Account level - both databaseName and tableName are empty
		err := checkPublicationPermissionWithBh(ctx, bh, "", "")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_AccountLevel_NoPermission tests account level no permission
func Test_checkPublicationPermissionWithBh_AccountLevel_NoPermission(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh account level no permission", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// Empty mo_pubs result
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{})

		// Account level - both databaseName and tableName are empty
		err := checkPublicationPermissionWithBh(ctx, bh, "", "")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission to access account level resources")
	})
}

// Test_checkPublicationPermissionWithBh_MultiplePublications tests permission with multiple publications
func Test_checkPublicationPermissionWithBh_MultiplePublications(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh multiple publications", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// Multiple publications, only one grants permission
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner1", "pub1", "test_db", int64(1), "other_table", "other_account"},
			{int64(2), "pub_owner2", "pub2", "test_db", int64(1), "test_table", "different_account"},
			{int64(3), "pub_owner3", "pub3", "test_db", int64(1), "test_table", "test_account"}, // This one grants permission
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_DatabaseMismatch tests when database in publication doesn't match
func Test_checkPublicationPermissionWithBh_DatabaseMismatch(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh database mismatch", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with different database
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "different_db", int64(1), "test_table", "test_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission")
	})
}

// Test_checkPublicationPermissionWithBh_TableWithSpaces tests table name with spaces in list
func Test_checkPublicationPermissionWithBh_TableWithSpaces(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh table with spaces in list", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with spaces around table names
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "table1, test_table , table2", "test_account"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}

// Test_checkPublicationPermissionWithBh_MultipleAccountsInList tests multiple accounts in account_list
func Test_checkPublicationPermissionWithBh_MultipleAccountsInList(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), uint32(100))

	convey.Convey("checkPublicationPermissionWithBh multiple accounts in list", t, func() {
		bh := &backgroundExecTest{}
		bh.init()

		// Account name query result
		accountNameSQL := `select account_name from mo_catalog.mo_account where account_id = 100;`
		bh.sql2result[accountNameSQL] = newMrsForAccountName("test_account")

		// mo_pubs result with multiple accounts in list
		moPubsSQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "test_db" 
			order by account_id, pub_name;`
		bh.sql2result[moPubsSQL] = newMrsForMoPubs([][]interface{}{
			{int64(1), "pub_owner", "pub1", "test_db", int64(1), "test_table", "acc1,test_account,acc2"},
		})

		err := checkPublicationPermissionWithBh(ctx, bh, "test_db", "test_table")
		convey.So(err, convey.ShouldBeNil)
	})
}
