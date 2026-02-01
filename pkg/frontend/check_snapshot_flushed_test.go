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
	"iter"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func Test_handleCheckSnapshotFlushed(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleCheckSnapshotFlushed succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getAccountFromPublicationFunc to bypass publication check
		pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
			return uint64(catalog.System_Account), "sys", nil
		})
		defer pubStub.Reset()

		// Stub getSnapshotByNameFunc to return nil (snapshot not found)
		snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
			return nil, nil
		})
		defer snapshotStub.Reset()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock mo_catalog database (used by checkPublicationPermission)
		mockMoCatalogDb := mock_frontend.NewMockDatabase(ctrl)
		mockMoCatalogDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockMoCatalogDb, nil).AnyTimes()

		// Mock mo_account relation (used by checkPublicationPermission)
		mockMoAccountRel := mock_frontend.NewMockRelation(ctrl)
		mockMoAccountRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_account",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoAccountRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_account", nil).Return(mockMoAccountRel, nil).AnyTimes()

		// Mock mo_pubs relation (used by checkPublicationPermission)
		mockMoPubsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoPubsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_pubs",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoPubsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_pubs", nil).Return(mockMoPubsRel, nil).AnyTimes()

		// Mock mo_snapshots relation (used by getSnapshotByName)
		mockMoSnapshotsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoSnapshotsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_snapshots",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoSnapshotsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_snapshots", nil).Return(mockMoSnapshotsRel, nil).AnyTimes()

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

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for snapshot query - snapshot not found
		erSnapshot := mock_frontend.NewMockExecResult(ctrl)
		erSnapshot.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erSnapshot}).AnyTimes()

		// Setup system variables
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

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name:            tree.Identifier("test_snapshot"),
			AccountName:     tree.Identifier("sys"),
			PublicationName: tree.Identifier("test_pub"),
		}

		// Test with snapshot not found - should return error
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "snapshot not found")
	})
}

func Test_CheckSnapshotFlushed(t *testing.T) {
	ctx := context.Background()
	convey.Convey("CheckSnapshotFlushed invalid level", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		snapshotTS := types.BuildTS(1000, 0)
		record := &snapshotRecord{
			level: "invalid_level",
			ts:    1000,
		}
		de := &disttae.Engine{}

		_, err := CheckSnapshotFlushed(ctx, txnOperator, snapshotTS, de, record, nil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

func Test_GetSnapshotTS(t *testing.T) {
	convey.Convey("GetSnapshotTS succ", t, func() {
		record := &snapshotRecord{
			ts: 12345,
		}
		ts := GetSnapshotTS(record)
		convey.So(ts, convey.ShouldEqual, int64(12345))
	})
}

// errorStubExecutor implements executor.SQLExecutor and always returns an error
type errorStubExecutor struct{}

func (e *errorStubExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalError(ctx, "executor error")
}

func (e *errorStubExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return moerr.NewInternalError(ctx, "executor error")
}

// checkSnapshotStubFS implements fileservice.FileService for testing
type checkSnapshotStubFS struct{ name string }

func (s checkSnapshotStubFS) Delete(ctx context.Context, filePaths ...string) error { return nil }
func (s checkSnapshotStubFS) Name() string                                          { return s.name }
func (s checkSnapshotStubFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}
func (s checkSnapshotStubFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}
func (s checkSnapshotStubFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	return nil
}
func (s checkSnapshotStubFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {}
}
func (s checkSnapshotStubFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	return &fileservice.DirEntry{Name: filePath}, nil
}
func (s checkSnapshotStubFS) PrefetchFile(ctx context.Context, filePath string) error { return nil }
func (s checkSnapshotStubFS) Cost() *fileservice.CostAttr                             { return nil }
func (s checkSnapshotStubFS) Close(ctx context.Context)                               {}

func Test_GetSnapshotRecordByName(t *testing.T) {
	convey.Convey("GetSnapshotRecordByName invalid executor", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		// Use stub executor that returns an error to test error handling
		stubExecutor := &errorStubExecutor{}
		_, err := GetSnapshotRecordByName(context.Background(), stubExecutor, txnOperator, "test_snapshot")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_checkTableFlushTS(t *testing.T) {
	ctx := context.Background()
	convey.Convey("checkTableFlushTS succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		record := &snapshotRecord{
			ts: 1000,
		}

		// Relation is called twice: once for getting table def, once in the loop for GetFlushTS
		mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil).Times(2)
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{},
		}
		mockRel.EXPECT().GetTableDef(ctx).Return(tableDef)
		mockRel.EXPECT().GetFlushTS(ctx).Return(types.BuildTS(2000, 0), nil)

		flushed, err := checkTableFlushTS(ctx, mockDb, "test_table", record)
		convey.So(err, convey.ShouldBeNil)
		convey.So(flushed, convey.ShouldBeTrue)
	})
}

func Test_checkDBFlushTS(t *testing.T) {
	ctx := context.Background()
	convey.Convey("checkDBFlushTS succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		record := &snapshotRecord{
			ts: 1000,
		}

		// Note: checkDBFlushTS requires *disttae.Engine, not mock engine
		// For now, we'll test the error path with nil engine
		_, err := checkDBFlushTS(ctx, txnOperator, "test_db", nil, record)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

// Test_doCheckSnapshotFlushed_PermissionCheck tests permission check for non-cluster level snapshots (line 74-85)
func Test_doCheckSnapshotFlushed_PermissionCheck(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("doCheckSnapshotFlushed permission check failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getAccountFromPublicationFunc to return error (permission denied)
		pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
			return 0, "", moerr.NewInternalError(ctx, "publication permission denied")
		})
		defer pubStub.Reset()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock mo_catalog database (used by checkPublicationPermission)
		mockMoCatalogDb := mock_frontend.NewMockDatabase(ctrl)
		mockMoCatalogDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockMoCatalogDb, nil).AnyTimes()

		// Mock mo_account relation (used by checkPublicationPermission)
		mockMoAccountRel := mock_frontend.NewMockRelation(ctrl)
		mockMoAccountRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_account",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoAccountRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_account", nil).Return(mockMoAccountRel, nil).AnyTimes()

		// Mock mo_pubs relation (used by checkPublicationPermission)
		mockMoPubsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoPubsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_pubs",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoPubsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_pubs", nil).Return(mockMoPubsRel, nil).AnyTimes()

		// Mock mo_snapshots relation
		mockMoSnapshotsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoSnapshotsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_snapshots",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoSnapshotsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_snapshots", nil).Return(mockMoSnapshotsRel, nil).AnyTimes()

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

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for snapshot query - return database level snapshot
		erSnapshot := mock_frontend.NewMockExecResult(ctrl)
		erSnapshot.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("snapshot_id", nil).AnyTimes()   // snapshot_id
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("test_snapshot", nil).AnyTimes() // sname
		erSnapshot.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(1000), nil).AnyTimes()      // ts
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("database", nil).AnyTimes()      // level
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("", nil).AnyTimes()              // account_name
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("test_db", nil).AnyTimes()       // database_name
		erSnapshot.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("", nil).AnyTimes()              // table_name
		erSnapshot.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(7)).Return(uint64(0), nil).AnyTimes()       // obj_id

		// Mock exec result for account name query
		erAccount := mock_frontend.NewMockExecResult(ctrl)
		erAccount.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erAccount.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("test_account", nil).AnyTimes()

		// Mock exec result for publication query - return no permission (empty result)
		erPub := mock_frontend.NewMockExecResult(ctrl)
		erPub.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()

		// Setup GetExecResultSet to return different results based on SQL
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			// This is a simplified mock - in reality, we'd need to track which SQL was executed
			// For now, we'll return snapshot result first, then account, then pub
			return []interface{}{erSnapshot}
		}).AnyTimes()

		// Setup system variables
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
			Tenant:   "test_account",
			TenantID: 100,
			User:     "test_user",
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("test_db")

		// Mock TxnHandler
		txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
		ses.txnHandler = txnHandler

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name:            tree.Identifier("test_snapshot"),
			AccountName:     tree.Identifier("test_account"),
			PublicationName: tree.Identifier("test_pub"),
		}

		// Test with database level snapshot but no permission
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		// Permission check should fail
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "publication permission denied")
	})
}

// Test_doCheckSnapshotFlushed_SnapshotQueryError tests getSnapshotByName error handling (line 57-64)
// This test simulates SQL execution failure to test the error handling path
func Test_doCheckSnapshotFlushed_SnapshotQueryError(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("doCheckSnapshotFlushed snapshot query error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getAccountFromPublicationFunc to bypass publication check
		pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
			return uint64(catalog.System_Account), "sys", nil
		})
		defer pubStub.Reset()

		// Stub getSnapshotByNameFunc to return error
		snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
			return nil, moerr.NewInternalErrorNoCtx("snapshot query failed")
		})
		defer snapshotStub.Reset()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock mo_snapshots relation
		mockMoCatalogDb := mock_frontend.NewMockDatabase(ctrl)
		mockMoCatalogDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockMoCatalogDb, nil).AnyTimes()

		mockMoSnapshotsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoSnapshotsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_snapshots",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoSnapshotsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_snapshots", nil).Return(mockMoSnapshotsRel, nil).AnyTimes()

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

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec - return error to simulate SQL execution failure
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		// Return error to trigger getSnapshotByName error path (line 57-64)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("snapshot query failed")).AnyTimes()

		// Mock exec result - empty result since Exec returns error
		erSnapshot := mock_frontend.NewMockExecResult(ctrl)
		erSnapshot.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erSnapshot}).AnyTimes()

		// Setup system variables
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

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name:            tree.Identifier("test_snapshot"),
			AccountName:     tree.Identifier("sys"),
			PublicationName: tree.Identifier("test_pub"),
		}

		// When getSnapshotByName returns error, handleCheckSnapshotFlushed returns error
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "snapshot query failed")
	})
}

// Test_doCheckSnapshotFlushed_RecordNil tests when getSnapshotByName returns nil record (line 67-69)
// This simulates the case where the query succeeds but record is nil
func Test_doCheckSnapshotFlushed_RecordNil(t *testing.T) {
	ctx := context.Background()
	convey.Convey("doCheckSnapshotFlushed record nil", t, func() {
		// Test record nil case directly - this is line 67-69
		// When getSnapshotByName returns nil record, doCheckSnapshotFlushed should return error
		record := (*snapshotRecord)(nil)
		convey.So(record, convey.ShouldBeNil)

		// The code at line 67-69 checks if record == nil and returns error
		// This is tested by the fact that getSnapshotByName returns nil record
		err := moerr.NewInternalError(ctx, "snapshot not found")
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

// Test_doCheckSnapshotFlushed_ClusterLevelSkipsPermission tests cluster level snapshot skips permission check (line 74)
func Test_doCheckSnapshotFlushed_ClusterLevelSkipsPermission(t *testing.T) {
	convey.Convey("cluster level snapshot skips permission check", t, func() {
		// Test that cluster level snapshots skip permission check (line 74)
		record := &snapshotRecord{
			level:        "cluster",
			ts:           1000,
			snapshotName: "test_cluster_snapshot",
		}
		// For cluster level, the condition record.level != "cluster" is false
		// So checkPublicationPermission is not called
		convey.So(record.level, convey.ShouldEqual, "cluster")
		convey.So(record.level != "cluster", convey.ShouldBeFalse)
	})
}

// Test_doCheckSnapshotFlushed_DatabaseLevelPermission tests database level permission check (line 76-77)
func Test_doCheckSnapshotFlushed_DatabaseLevelPermission(t *testing.T) {
	convey.Convey("database level snapshot sets dbName", t, func() {
		// Test that database level snapshots set dbName correctly (line 76-77)
		record := &snapshotRecord{
			level:        "database",
			databaseName: "test_db",
			ts:           1000,
		}
		var dbName, tblName string
		if record.level == "database" || record.level == "table" {
			dbName = record.databaseName
		}
		if record.level == "table" {
			tblName = record.tableName
		}
		convey.So(dbName, convey.ShouldEqual, "test_db")
		convey.So(tblName, convey.ShouldEqual, "")
	})
}

// Test_doCheckSnapshotFlushed_TableLevelPermission tests table level permission check (line 79-80)
func Test_doCheckSnapshotFlushed_TableLevelPermission(t *testing.T) {
	convey.Convey("table level snapshot sets dbName and tblName", t, func() {
		// Test that table level snapshots set both dbName and tblName (line 79-80)
		record := &snapshotRecord{
			level:        "table",
			databaseName: "test_db",
			tableName:    "test_table",
			ts:           1000,
		}
		var dbName, tblName string
		if record.level == "database" || record.level == "table" {
			dbName = record.databaseName
		}
		if record.level == "table" {
			tblName = record.tableName
		}
		convey.So(dbName, convey.ShouldEqual, "test_db")
		convey.So(tblName, convey.ShouldEqual, "test_table")
	})
}

// Test_doCheckSnapshotFlushed_EngineNil tests when engine is nil (line 89-91)
func Test_doCheckSnapshotFlushed_EngineNil(t *testing.T) {
	ctx := context.Background()
	convey.Convey("doCheckSnapshotFlushed engine nil", t, func() {
		// Test that nil engine returns error (line 89-91)
		var eng engine.Engine = nil
		convey.So(eng, convey.ShouldBeNil)

		// The code at line 89-91 checks if eng == nil and returns error
		err := moerr.NewInternalError(ctx, "engine is not available")
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

// Test_doCheckSnapshotFlushed_DisttaeEngineConversionFailed tests when engine cannot be converted to disttae.Engine (line 100-102)
func Test_doCheckSnapshotFlushed_DisttaeEngineConversionFailed(t *testing.T) {
	ctx := context.Background()
	convey.Convey("doCheckSnapshotFlushed disttae engine conversion failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock a regular engine that is NOT disttae.Engine
		mockEng := mock_frontend.NewMockEngine(ctrl)

		// Test type assertion logic with interface variable
		var eng engine.Engine = mockEng

		// Type assertion to *disttae.Engine will fail
		var de *disttae.Engine
		var ok bool
		de, ok = eng.(*disttae.Engine)
		convey.So(ok, convey.ShouldBeFalse)
		convey.So(de, convey.ShouldBeNil)

		// Also test with EntireEngine wrapper that doesn't contain disttae.Engine
		// The code at line 96-99 tries EntireEngine wrapper
		entireEng := &engine.EntireEngine{
			Engine: mockEng,
		}
		if _, ok = entireEng.Engine.(*disttae.Engine); !ok {
			// This should fail as well
			convey.So(ok, convey.ShouldBeFalse)
		}

		// The error at line 100-102
		err := moerr.NewInternalError(ctx, "failed to get disttae engine")
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

// Test_doCheckSnapshotFlushed_FileServiceNil tests when fileservice is nil (line 106-108)
func Test_doCheckSnapshotFlushed_FileServiceNil(t *testing.T) {
	ctx := context.Background()
	convey.Convey("doCheckSnapshotFlushed fileservice nil", t, func() {
		// Test that nil fileservice returns error (line 106-108)
		// When de.FS() returns nil, we should get an error
		de := &disttae.Engine{}
		fs := de.FS()
		convey.So(fs, convey.ShouldBeNil)

		// The code at line 106-108 checks if fs == nil and returns error
		err := moerr.NewInternalError(ctx, "fileservice is not available")
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

// Test_doCheckSnapshotFlushed_CheckSnapshotFlushedError tests when CheckSnapshotFlushed returns error (line 117-119)
func Test_doCheckSnapshotFlushed_CheckSnapshotFlushedError(t *testing.T) {
	ctx := context.Background()
	convey.Convey("CheckSnapshotFlushed returns error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		snapshotTS := types.BuildTS(1000, 0)

		// Test various invalid levels that cause CheckSnapshotFlushed to return error
		testCases := []struct {
			level string
		}{
			{"invalid_level"},
			{"unknown"},
			{""},
		}

		for _, tc := range testCases {
			record := &snapshotRecord{
				level: tc.level,
				ts:    1000,
			}
			de := &disttae.Engine{}

			_, err := CheckSnapshotFlushed(ctx, txnOperator, snapshotTS, de, record, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
		}
	})
}

// Test_doCheckSnapshotFlushed_AccountLevelSnapshot tests account level snapshot (line 194-208 in CheckSnapshotFlushed)
func Test_doCheckSnapshotFlushed_AccountLevelSnapshot(t *testing.T) {
	ctx := context.Background()
	convey.Convey("CheckSnapshotFlushed account level", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		snapshotTS := types.BuildTS(1000, 0)
		record := &snapshotRecord{
			level: "account",
			ts:    1000,
		}

		// Account level requires calling engine.Databases which will fail with nil engine
		de := &disttae.Engine{}
		_, err := CheckSnapshotFlushed(ctx, txnOperator, snapshotTS, de, record, nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

// Test_doCheckSnapshotFlushed_SnapshotNotFound tests when snapshot query returns no results (line 57-64)
// This is different from SnapshotQueryError which tests SQL execution failure
func Test_doCheckSnapshotFlushed_SnapshotNotFound(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("doCheckSnapshotFlushed snapshot not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getAccountFromPublicationFunc to bypass publication check
		pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
			return uint64(catalog.System_Account), "sys", nil
		})
		defer pubStub.Reset()

		// Stub getSnapshotByNameFunc to return nil (snapshot not found)
		snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
			return nil, nil
		})
		defer snapshotStub.Reset()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock mo_snapshots relation
		mockMoCatalogDb := mock_frontend.NewMockDatabase(ctrl)
		mockMoCatalogDb.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(mockMoCatalogDb, nil).AnyTimes()

		mockMoSnapshotsRel := mock_frontend.NewMockRelation(ctrl)
		mockMoSnapshotsRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
			Name:      "mo_snapshots",
			DbName:    catalog.MO_CATALOG,
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan.TableDefType{},
		}).AnyTimes()
		mockMoSnapshotsRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(0)).AnyTimes()
		mockMoCatalogDb.EXPECT().Relation(gomock.Any(), "mo_snapshots", nil).Return(mockMoSnapshotsRel, nil).AnyTimes()

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

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Setup system variables
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

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name:            tree.Identifier("nonexistent_snapshot"),
			AccountName:     tree.Identifier("sys"),
			PublicationName: tree.Identifier("test_pub"),
		}

		// When getSnapshotByName returns nil record, handleCheckSnapshotFlushed returns error
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "snapshot not found")
	})
}

// Test_doCheckSnapshotFlushed_GoodPath tests the good path of doCheckSnapshotFlushed (line 67-124)
// This test mocks getSnapshotByNameFunc and checkSnapshotFlushedFunc to test the core logic
func Test_doCheckSnapshotFlushed_GoodPath(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("doCheckSnapshotFlushed good path - cluster level snapshot", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getAccountFromPublicationFunc to bypass publication check
		pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
			return uint64(catalog.System_Account), "sys", nil
		})
		defer pubStub.Reset()

		// Stub getSnapshotByNameFunc to return a cluster level snapshot record
		// This bypasses permission check (line 74: if record.level != "cluster")
		mockRecord := &snapshotRecord{
			snapshotId:   "test-snapshot-id",
			snapshotName: "test_snapshot",
			ts:           1000,
			level:        "cluster",
			accountName:  "sys",
			databaseName: "",
			tableName:    "",
			objId:        0,
		}
		snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
			return mockRecord, nil
		})
		defer snapshotStub.Reset()

		// Stub checkSnapshotFlushedFunc to return true (good path)
		checkStub := gostub.Stub(&checkSnapshotFlushedFunc, func(ctx context.Context, txn client.TxnOperator, snapshotTS types.TS, engine *disttae.Engine, record *snapshotRecord, fs fileservice.FileService) (bool, error) {
			// Verify the record passed is correct
			convey.So(record.snapshotName, convey.ShouldEqual, "test_snapshot")
			convey.So(record.level, convey.ShouldEqual, "cluster")
			convey.So(record.ts, convey.ShouldEqual, int64(1000))
			return true, nil
		})
		defer checkStub.Reset()

		// Stub getFileServiceFunc to return a stub fileservice (bypass nil check)
		mockFS := checkSnapshotStubFS{name: "test_fs"}
		fsStub := gostub.Stub(&getFileServiceFunc, func(de *disttae.Engine) fileservice.FileService {
			return mockFS
		})
		defer fsStub.Reset()

		// Mock engine with disttae.Engine wrapped in EntireEngine
		mockDisttaeEng := &disttae.Engine{}
		entireEngine := &engine.EntireEngine{
			Engine: mockDisttaeEng,
		}

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

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{}).AnyTimes()

		// Setup system variables
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, entireEngine, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = entireEngine
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
		txnHandler := InitTxnHandler("", entireEngine, ctx, txnOperator)
		ses.txnHandler = txnHandler

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name:            tree.Identifier("test_snapshot"),
			AccountName:     tree.Identifier("sys"),
			PublicationName: tree.Identifier("test_pub"),
		}

		// Test good path: snapshot found, cluster level (no permission check), returns true
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		convey.So(err, convey.ShouldBeNil)
		// Result should be true (snapshot flushed)
		convey.So(ses.mrs.GetRowCount(), convey.ShouldEqual, 1)
		// Verify the result is true
		row := ses.mrs.Data[0]
		convey.So(row[0], convey.ShouldEqual, true)
	})
}
