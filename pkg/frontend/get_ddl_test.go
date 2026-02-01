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
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
)

// newMrsForGetAccountFromPublication creates a MysqlResultSet for getAccountFromPublication query
func newMrsForGetAccountFromPublication(accountID uint64, accountName, pubName, dbName string, dbID uint64, tableList, accountList string) *MysqlResultSet {
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

	mrs.AddRow([]interface{}{accountID, accountName, pubName, dbName, dbID, tableList, accountList})
	return mrs
}

// Test_visitTableDdl_GoodPath tests all good paths in visitTableDdl function
func Test_visitTableDdl_GoodPath(t *testing.T) {
	ctx := context.Background()

	convey.Convey("visitTableDdl good paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		// Test 1: Normal table - success case
		convey.Convey("normal table DDL generation", func() {
			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
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

		// Test 2: View table - should be skipped (return nil without adding row)
		convey.Convey("view table is skipped", func() {
			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "view_table", nil).Return(mockRel, nil)
			// Return view table type ("V")
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: "V"})

			err := visitTableDdl(ctx, "test_db", "view_table", bat, txnOperator, eng, mp)
			convey.So(err, convey.ShouldBeNil)
			// View should be skipped, no row added
			convey.So(bat.RowCount(), convey.ShouldEqual, 0)
		})

		// Test 3: Table with existing PropFromPublication property
		convey.Convey("table with existing PropFromPublication property", func() {
			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "pub_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "pub_table",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs: []*plan2.TableDefType{
					{
						Def: &plan2.TableDef_DefType_Properties{
							Properties: &plan2.PropertiesDef{
								Properties: []*plan2.Property{
									{Key: catalog.PropFromPublication, Value: "true"},
								},
							},
						},
					},
				},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(456))

			err := visitTableDdl(ctx, "test_db", "pub_table", bat, txnOperator, eng, mp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 1)
		})
	})
}

// Test_visitDatabaseDdl_GoodPath tests good paths in visitDatabaseDdl function
func Test_visitDatabaseDdl_GoodPath(t *testing.T) {
	ctx := context.Background()

	convey.Convey("visitDatabaseDdl good paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		// Test 1: Single table (tableName provided)
		convey.Convey("single table specified", func() {
			mockRel := mock_frontend.NewMockRelation(ctrl)

			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			// Database is called twice: once in visitDatabaseDdl and once in visitTableDdl
			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).Times(2)
			mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "test_table",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(123))

			err := visitDatabaseDdl(ctx, "test_db", "test_table", bat, txnOperator, eng, mp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 1)
		})

		// Test 2: All tables in database (tableName empty)
		convey.Convey("all tables in database", func() {
			mockRel1 := mock_frontend.NewMockRelation(ctrl)
			mockRel2 := mock_frontend.NewMockRelation(ctrl)

			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			// Once for visitDatabaseDdl, twice for each table in visitTableDdl
			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).Times(3)
			mockDb.EXPECT().Relations(ctx).Return([]string{"table1", "table2"}, nil)

			mockDb.EXPECT().Relation(ctx, "table1", nil).Return(mockRel1, nil)
			mockRel1.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel1.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "table1",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel1.EXPECT().GetTableID(ctx).Return(uint64(101))

			mockDb.EXPECT().Relation(ctx, "table2", nil).Return(mockRel2, nil)
			mockRel2.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel2.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "table2",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel2.EXPECT().GetTableID(ctx).Return(uint64(102))

			err := visitDatabaseDdl(ctx, "test_db", "", bat, txnOperator, eng, mp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 2)
		})
	})
}

// Test_getddlbatch_GoodPath tests good paths in getddlbatch function
func Test_getddlbatch_GoodPath(t *testing.T) {
	ctx := context.Background()

	convey.Convey("getddlbatch good paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		// Test 1: Single database with single table
		convey.Convey("single database and table", func() {
			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockRel := mock_frontend.NewMockRelation(ctrl)

			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).Times(2)
			mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "test_table",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(123))

			bat, err := getddlbatch(ctx, "test_db", "test_table", eng, mp, txnOperator)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat, convey.ShouldNotBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 1)
			bat.Clean(mp)
		})

		// Test 2: All databases (databaseName empty)
		convey.Convey("all databases", func() {
			mockDb1 := mock_frontend.NewMockDatabase(ctrl)
			mockDb2 := mock_frontend.NewMockDatabase(ctrl)
			mockRel1 := mock_frontend.NewMockRelation(ctrl)
			mockRel2 := mock_frontend.NewMockRelation(ctrl)

			eng.EXPECT().Databases(ctx, txnOperator).Return([]string{"db1", "db2"}, nil)

			// First database
			eng.EXPECT().Database(ctx, "db1", txnOperator).Return(mockDb1, nil).Times(2)
			mockDb1.EXPECT().Relations(ctx).Return([]string{"table1"}, nil)
			mockDb1.EXPECT().Relation(ctx, "table1", nil).Return(mockRel1, nil)
			mockRel1.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel1.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "table1",
				DbName:    "db1",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel1.EXPECT().GetTableID(ctx).Return(uint64(101))

			// Second database
			eng.EXPECT().Database(ctx, "db2", txnOperator).Return(mockDb2, nil).Times(2)
			mockDb2.EXPECT().Relations(ctx).Return([]string{"table2"}, nil)
			mockDb2.EXPECT().Relation(ctx, "table2", nil).Return(mockRel2, nil)
			mockRel2.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel2.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "table2",
				DbName:    "db2",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel2.EXPECT().GetTableID(ctx).Return(uint64(102))

			bat, err := getddlbatch(ctx, "", "", eng, mp, txnOperator)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat, convey.ShouldNotBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 2)
			bat.Clean(mp)
		})
	})
}

// Test_GetDdlBatchWithoutSession_GoodPath tests good paths in GetDdlBatchWithoutSession function
func Test_GetDdlBatchWithoutSession_GoodPath(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetDdlBatchWithoutSession good paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		// Test 1: Without snapshot
		convey.Convey("without snapshot", func() {
			eng.EXPECT().Database(ctx, "test_db", txnOperator).Return(mockDb, nil).Times(2)
			mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "test_table",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(123))

			bat, err := GetDdlBatchWithoutSession(ctx, "test_db", "test_table", eng, txnOperator, mp, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat, convey.ShouldNotBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 1)
			bat.Clean(mp)
		})

		// Test 2: With snapshot
		convey.Convey("with snapshot", func() {
			clonedTxn := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(clonedTxn)

			eng.EXPECT().Database(ctx, "test_db", clonedTxn).Return(mockDb, nil).Times(2)
			mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "test_table",
				DbName:    "test_db",
				TableType: catalog.SystemOrdinaryRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(123))

			ts := types.BuildTS(1000, 0)
			snapshotTS := ts.ToTimestamp()
			snapshot := &plan2.Snapshot{
				TS: &snapshotTS,
			}

			bat, err := GetDdlBatchWithoutSession(ctx, "test_db", "test_table", eng, txnOperator, mp, snapshot)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat, convey.ShouldNotBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 1)
			bat.Clean(mp)
		})
	})
}

// Test_handleGetDdlWithChecker_GoodPath tests the good path of handleGetDdlWithChecker
// by mocking getAccountFromPublication and getSnapshotByNameFunc
func Test_handleGetDdlWithChecker_GoodPath(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleGetDdlWithChecker good path", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock database
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		eng.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDb, nil).AnyTimes()

		// Mock relation
		mockRel := mock_frontend.NewMockRelation(ctrl)
		mockDb.EXPECT().Relation(gomock.Any(), "test_table", nil).Return(mockRel, nil).AnyTimes()
		mockRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel}).AnyTimes()
		mockRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan2.TableDef{
			Name:      "test_table",
			DbName:    "test_db",
			TableType: catalog.SystemOrdinaryRel,
			Defs:      []*plan2.TableDefType{},
		}).AnyTimes()
		mockRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(123)).AnyTimes()

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
		txnMeta := &txn.TxnMeta{Mode: txn.TxnMode_Optimistic}
		txnOperator.EXPECT().Txn().Return(*txnMeta).AnyTimes()
		txnOperator.EXPECT().GetWaitActiveCost().Return(time.Duration(0)).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec with proper result set for getAccountFromPublication
		bh := &backgroundExecTest{}
		bh.init()

		// Setup mock result for getAccountFromPublication query
		// The function queries: SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list FROM mo_catalog.mo_pubs WHERE account_name = 'pub_account' AND pub_name = 'test_pub'
		pubQuerySQL := `SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = 'pub_account' AND pub_name = 'test_pub'`
		bh.sql2result[pubQuerySQL] = newMrsForGetAccountFromPublication(uint64(1), "pub_account", "test_pub", "test_db", uint64(1), "*", "all")

		// Stub NewShareTxnBackgroundExec
		bhStub := gostub.StubFunc(&NewShareTxnBackgroundExec, bh)
		defer bhStub.Reset()

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
			Tenant:   "test_tenant",
			TenantID: uint32(100),
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

		// Test 1: Without snapshot
		convey.Convey("without snapshot", func() {
			dbName := tree.Identifier("test_db")
			tableName := tree.Identifier("test_table")
			pubName := tree.Identifier("test_pub")
			stmt := &tree.GetDdl{
				Database:                &dbName,
				Table:                   &tableName,
				PubName:                 &pubName,
				SubscriptionAccountName: "pub_account",
			}

			err := handleGetDdlWithChecker(ctx, ses, stmt, defaultSnapshotResolver)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ses.GetMysqlResultSet().GetRowCount(), convey.ShouldBeGreaterThan, uint64(0))
		})

		// Test 2: With snapshot
		convey.Convey("with snapshot", func() {
			// Stub getSnapshotByNameFunc to return a valid snapshot record
			snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
				return &snapshotRecord{
					snapshotId:   "test-snapshot-id",
					snapshotName: "test_snapshot",
					ts:           1000,
					level:        "cluster",
					accountName:  "sys",
				}, nil
			})
			defer snapshotStub.Reset()

			dbName := tree.Identifier("test_db")
			tableName := tree.Identifier("test_table")
			pubName := tree.Identifier("test_pub")
			snapshotName := tree.Identifier("test_snapshot")
			stmt := &tree.GetDdl{
				Database:                &dbName,
				Table:                   &tableName,
				PubName:                 &pubName,
				SubscriptionAccountName: "pub_account",
				Snapshot:                &snapshotName,
			}

			ses.ClearAllMysqlResultSet()
			err := handleGetDdlWithChecker(ctx, ses, stmt, defaultSnapshotResolver)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ses.GetMysqlResultSet().GetRowCount(), convey.ShouldBeGreaterThan, uint64(0))
		})
	})
}
