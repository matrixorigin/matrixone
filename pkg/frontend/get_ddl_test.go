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
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

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

// Test_GetSnapshotCoveredScope_GoodPath tests the good path of GetSnapshotCoveredScope
func Test_GetSnapshotCoveredScope_GoodPath(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetSnapshotCoveredScope good path - table level snapshot", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Stub getSnapshotByNameFunc to return a table level snapshot record
		mockRecord := &snapshotRecord{
			snapshotId:   "test-snapshot-id",
			snapshotName: "test_snapshot",
			ts:           1000,
			level:        "table",
			accountName:  "sys",
			databaseName: "test_db",
			tableName:    "test_table",
			objId:        0,
		}
		snapshotStub := gostub.Stub(&getSnapshotByNameFunc, func(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
			convey.So(snapshotName, convey.ShouldEqual, "test_snapshot")
			return mockRecord, nil
		})
		defer snapshotStub.Reset()

		// Mock background exec (not actually used since we stubbed getSnapshotByNameFunc)
		bh := mock_frontend.NewMockBackgroundExec(ctrl)

		// Test good path
		scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "test_snapshot")
		convey.So(err, convey.ShouldBeNil)
		convey.So(scope, convey.ShouldNotBeNil)
		convey.So(scope.Level, convey.ShouldEqual, "table")
		convey.So(scope.DatabaseName, convey.ShouldEqual, "test_db")
		convey.So(scope.TableName, convey.ShouldEqual, "test_table")
		convey.So(ts, convey.ShouldEqual, int64(1000))
	})
}

// Test_BuildDdlMysqlResultSet_GoodPath tests the good path of BuildDdlMysqlResultSet
func Test_BuildDdlMysqlResultSet_GoodPath(t *testing.T) {
	convey.Convey("BuildDdlMysqlResultSet good path", t, func() {
		mrs := &MysqlResultSet{}

		BuildDdlMysqlResultSet(mrs)

		// Verify 4 columns are added
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))

		// Verify column names
		col0, _ := mrs.GetColumn(context.Background(), 0)
		col1, _ := mrs.GetColumn(context.Background(), 1)
		col2, _ := mrs.GetColumn(context.Background(), 2)
		col3, _ := mrs.GetColumn(context.Background(), 3)

		convey.So(col0.Name(), convey.ShouldEqual, "dbname")
		convey.So(col1.Name(), convey.ShouldEqual, "tablename")
		convey.So(col2.Name(), convey.ShouldEqual, "tableid")
		convey.So(col3.Name(), convey.ShouldEqual, "tablesql")
	})
}

// Test_FillDdlMysqlResultSet_GoodPath tests the good path of FillDdlMysqlResultSet
func Test_FillDdlMysqlResultSet_GoodPath(t *testing.T) {
	convey.Convey("FillDdlMysqlResultSet good path", t, func() {
		mp := mpool.MustNewZero()

		// Create a batch with test data
		bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
		defer bat.Clean(mp)

		// Add test data to batch
		_ = vector.AppendBytes(bat.Vecs[0], []byte("test_db"), false, mp)
		_ = vector.AppendBytes(bat.Vecs[1], []byte("test_table"), false, mp)
		_ = vector.AppendFixed[int64](bat.Vecs[2], int64(123), false, mp)
		_ = vector.AppendBytes(bat.Vecs[3], []byte("CREATE TABLE test_table (id INT)"), false, mp)
		bat.SetRowCount(1)

		// Create MysqlResultSet and fill it
		mrs := &MysqlResultSet{}
		BuildDdlMysqlResultSet(mrs)
		FillDdlMysqlResultSet(bat, mrs)

		// Verify row count
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))

		// Verify row data
		row := mrs.Data[0]
		convey.So(string(row[0].([]byte)), convey.ShouldEqual, "test_db")
		convey.So(string(row[1].([]byte)), convey.ShouldEqual, "test_table")
		convey.So(row[2], convey.ShouldEqual, int64(123))
		convey.So(string(row[3].([]byte)), convey.ShouldEqual, "CREATE TABLE test_table (id INT)")
	})
}
