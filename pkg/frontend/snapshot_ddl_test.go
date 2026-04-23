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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// ===========================================================================
// get_ddl.go error paths
// ===========================================================================

func TestSnapshotDdlCoverage_GetSnapshotTsByName(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetSnapshotTsByName error paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// GetSnapshotTsByName calls getSnapshotByName (direct function, not variable).
		// getSnapshotByName calls getSnapshotRecords which uses bh.ClearExecResultSet/Exec/GetExecResultSet.
		// We need to properly mock the BackgroundExec.

		convey.Convey("bh.Exec returns error => 'failed to query snapshot'", func() {
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().ClearExecResultSet().Return()
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(fmt.Errorf("db connection lost"))

			ts, err := GetSnapshotTsByName(ctx, bh, "snap1")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to query snapshot")
			convey.So(ts, convey.ShouldEqual, 0)
		})

		convey.Convey("no records found => wrapped error", func() {
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().ClearExecResultSet().Return()
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)

			// Return empty result set (0 rows)
			erResult := mock_frontend.NewMockExecResult(ctrl)
			erResult.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
			bh.EXPECT().GetExecResultSet().Return([]interface{}{erResult})

			ts, err := GetSnapshotTsByName(ctx, bh, "snap_missing")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to query snapshot")
			convey.So(ts, convey.ShouldEqual, 0)
		})

		convey.Convey("valid record => success", func() {
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().ClearExecResultSet().Return()
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)

			// Return one row
			erResult := mock_frontend.NewMockExecResult(ctrl)
			erResult.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("snap-id", nil)
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("snap_ok", nil)
			erResult.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(12345), nil)
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("account", nil)
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("sys", nil)
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("", nil)
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("", nil)
			erResult.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(7)).Return(uint64(0), nil)
			bh.EXPECT().GetExecResultSet().Return([]interface{}{erResult})

			ts, err := GetSnapshotTsByName(ctx, bh, "snap_ok")
			convey.So(err, convey.ShouldBeNil)
			convey.So(ts, convey.ShouldEqual, 12345)
		})
	})
}

func TestSnapshotDdlCoverage_GetSnapshotCoveredScope(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetSnapshotCoveredScope error/edge paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)

		convey.Convey("query error", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return nil, fmt.Errorf("conn error")
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s1")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to query snapshot")
			convey.So(scope, convey.ShouldBeNil)
			convey.So(ts, convey.ShouldEqual, 0)
		})

		convey.Convey("nil record", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return nil, nil
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s_nil")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "does not exist")
			convey.So(scope, convey.ShouldBeNil)
			convey.So(ts, convey.ShouldEqual, 0)
		})

		convey.Convey("unsupported level", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return &snapshotRecord{level: "unknown_level", ts: 100}, nil
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s_bad_level")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "unsupported snapshot level")
			convey.So(scope, convey.ShouldBeNil)
			convey.So(ts, convey.ShouldEqual, 0)
		})

		convey.Convey("database level", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return &snapshotRecord{level: "database", databaseName: "mydb", ts: 200}, nil
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s_db")
			convey.So(err, convey.ShouldBeNil)
			convey.So(scope.Level, convey.ShouldEqual, "database")
			convey.So(scope.DatabaseName, convey.ShouldEqual, "mydb")
			convey.So(scope.TableName, convey.ShouldEqual, "")
			convey.So(ts, convey.ShouldEqual, 200)
		})

		convey.Convey("account level", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return &snapshotRecord{level: "account", ts: 300}, nil
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s_acct")
			convey.So(err, convey.ShouldBeNil)
			convey.So(scope.Level, convey.ShouldEqual, "account")
			convey.So(scope.DatabaseName, convey.ShouldEqual, "")
			convey.So(scope.TableName, convey.ShouldEqual, "")
			convey.So(ts, convey.ShouldEqual, 300)
		})

		convey.Convey("cluster level", func() {
			stub := gostub.Stub(&getSnapshotByNameFunc, func(_ context.Context, _ BackgroundExec, _ string) (*snapshotRecord, error) {
				return &snapshotRecord{level: "cluster", ts: 400}, nil
			})
			defer stub.Reset()

			scope, ts, err := GetSnapshotCoveredScope(ctx, bh, "s_cluster")
			convey.So(err, convey.ShouldBeNil)
			convey.So(scope.Level, convey.ShouldEqual, "cluster")
			convey.So(ts, convey.ShouldEqual, 400)
		})
	})
}

func TestSnapshotDdlCoverage_GetDdlBatchWithoutSession_NilChecks(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetDdlBatchWithoutSession nil parameter checks", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		convey.Convey("nil engine", func() {
			bat, err := GetDdlBatchWithoutSession(ctx, "db", "tbl", nil, txnOp, mp, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "engine is nil")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("nil txn", func() {
			bat, err := GetDdlBatchWithoutSession(ctx, "db", "tbl", eng, nil, mp, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "txn is nil")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("nil mpool", func() {
			bat, err := GetDdlBatchWithoutSession(ctx, "db", "tbl", eng, txnOp, nil, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mpool is nil")
			convey.So(bat, convey.ShouldBeNil)
		})
	})
}

func TestSnapshotDdlCoverage_getddlbatch_NilChecks(t *testing.T) {
	ctx := context.Background()

	convey.Convey("getddlbatch nil parameter checks", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		convey.Convey("nil engine", func() {
			bat, err := getddlbatch(ctx, "db", "tbl", nil, mp, txnOp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "engine is nil")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("nil mpool", func() {
			bat, err := getddlbatch(ctx, "db", "tbl", eng, nil, txnOp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mpool is nil")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("nil txn", func() {
			bat, err := getddlbatch(ctx, "db", "tbl", eng, mp, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "txn is nil")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("Databases() error when databaseName is empty", func() {
			eng.EXPECT().Databases(ctx, txnOp).Return(nil, fmt.Errorf("databases error"))

			bat, err := getddlbatch(ctx, "", "", eng, mp, txnOp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get database names")
			convey.So(bat, convey.ShouldBeNil)
		})

		convey.Convey("system databases are skipped", func() {
			eng.EXPECT().Databases(ctx, txnOp).Return(catalog.SystemDatabases, nil)

			bat, err := getddlbatch(ctx, "", "", eng, mp, txnOp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat, convey.ShouldNotBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 0)
			bat.Clean(mp)
		})
	})
}

func TestSnapshotDdlCoverage_visitTableDdl_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	convey.Convey("visitTableDdl error paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		convey.Convey("nil batch", func() {
			err := visitTableDdl(ctx, "db", "tbl", nil, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "batch is nil")
		})

		convey.Convey("batch with fewer than 4 vecs", func() {
			bat := batch.New([]string{"a", "b"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "at least 4 columns")
		})

		makeBatch := func() *batch.Batch {
			bat := batch.New([]string{"a", "b", "c", "d"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			return bat
		}

		convey.Convey("nil mpool", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mpool is nil")
		})

		convey.Convey("nil engine", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, nil, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "engine is nil")
		})

		convey.Convey("nil txn", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			err := visitTableDdl(ctx, "db", "tbl", bat, nil, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "txn is nil")
		})

		convey.Convey("Database() returns error", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			eng.EXPECT().Database(ctx, "db", txnOp).Return(nil, fmt.Errorf("no such db"))
			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get database")
		})

		convey.Convey("Relation() returns error", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "tbl", nil).Return(nil, fmt.Errorf("no such table"))

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get table")
		})

		convey.Convey("index table prefix is skipped", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockRel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, catalog.IndexTableNamePrefix+"idx1", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})

			err := visitTableDdl(ctx, "db", catalog.IndexTableNamePrefix+"idx1", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(bat.RowCount(), convey.ShouldEqual, 0)
		})

		convey.Convey("nil CopyTableDef", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockRel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "tbl", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(nil)

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get table definition")
		})

		convey.Convey("cluster table type returns error", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockRel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "tbl", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "tbl",
				DbName:    "db",
				TableType: catalog.SystemClusterRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(1))

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "cluster table is not supported")
		})

		convey.Convey("external table type returns error", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockRel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relation(ctx, "tbl", nil).Return(mockRel, nil)
			mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
			mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
				Name:      "tbl",
				DbName:    "db",
				TableType: catalog.SystemExternalRel,
				Defs:      []*plan2.TableDefType{},
			})
			mockRel.EXPECT().GetTableID(ctx).Return(uint64(2))

			err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "external table is not supported")
		})
	})
}

func TestSnapshotDdlCoverage_visitDatabaseDdl_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	convey.Convey("visitDatabaseDdl error paths", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		convey.Convey("nil batch", func() {
			err := visitDatabaseDdl(ctx, "db", "", nil, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "batch is nil")
		})

		convey.Convey("too few vecs", func() {
			bat := batch.New([]string{"a"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(mp)
			err := visitDatabaseDdl(ctx, "db", "", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "at least 4 columns")
		})

		makeBatch := func() *batch.Batch {
			bat := batch.New([]string{"a", "b", "c", "d"})
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			return bat
		}

		convey.Convey("nil mpool", func() {
			bat := makeBatch()
			defer bat.Clean(mp)
			err := visitDatabaseDdl(ctx, "db", "", bat, txnOp, eng, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mpool is nil")
		})

		convey.Convey("nil engine", func() {
			bat := makeBatch()
			defer bat.Clean(mp)
			err := visitDatabaseDdl(ctx, "db", "", bat, txnOp, nil, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "engine is nil")
		})

		convey.Convey("nil txn", func() {
			bat := makeBatch()
			defer bat.Clean(mp)
			err := visitDatabaseDdl(ctx, "db", "", bat, nil, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "txn is nil")
		})

		convey.Convey("Database() error", func() {
			bat := makeBatch()
			defer bat.Clean(mp)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(nil, fmt.Errorf("db error"))

			err := visitDatabaseDdl(ctx, "db", "", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get database")
		})

		convey.Convey("Relations() error when tableName empty", func() {
			bat := makeBatch()
			defer bat.Clean(mp)

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
			mockDb.EXPECT().Relations(ctx).Return(nil, fmt.Errorf("relations error"))

			err := visitDatabaseDdl(ctx, "db", "", bat, txnOp, eng, mp)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "failed to get table names")
		})
	})
}

func TestSnapshotDdlCoverage_FillDdlMysqlResultSet_NilBatch(t *testing.T) {
	convey.Convey("FillDdlMysqlResultSet with nil batch", t, func() {
		mrs := &MysqlResultSet{}
		BuildDdlMysqlResultSet(mrs)
		FillDdlMysqlResultSet(nil, mrs)
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(0))
	})

	convey.Convey("FillDdlMysqlResultSet with empty batch (0 rows)", t, func() {
		mp := mpool.MustNewZero()
		bat := batch.New([]string{"a", "b", "c", "d"})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
		bat.SetRowCount(0)
		defer bat.Clean(mp)

		mrs := &MysqlResultSet{}
		BuildDdlMysqlResultSet(mrs)
		FillDdlMysqlResultSet(bat, mrs)
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(0))
	})
}

func TestSnapshotDdlCoverage_ComputeDdlBatch(t *testing.T) {
	ctx := context.Background()

	convey.Convey("ComputeDdlBatch delegates to getddlbatch", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		eng.EXPECT().Database(ctx, "db1", txnOp).Return(mockDb, nil).Times(2)
		mockDb.EXPECT().Relation(ctx, "tbl1", nil).Return(mockRel, nil)
		mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name: "tbl1", DbName: "db1", TableType: catalog.SystemOrdinaryRel, Defs: []*plan2.TableDefType{},
		})
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(1))

		bat, err := ComputeDdlBatch(ctx, "db1", "tbl1", eng, mp, txnOp)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(bat.RowCount(), convey.ShouldEqual, 1)
		bat.Clean(mp)
	})
}

func TestSnapshotDdlCoverage_ComputeDdlBatchWithSnapshot(t *testing.T) {
	ctx := context.Background()

	convey.Convey("ComputeDdlBatchWithSnapshot with snapshotTs=0 (no clone)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		eng.EXPECT().Database(ctx, "db1", txnOp).Return(mockDb, nil).Times(2)
		mockDb.EXPECT().Relation(ctx, "tbl1", nil).Return(mockRel, nil)
		mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name: "tbl1", DbName: "db1", TableType: catalog.SystemOrdinaryRel, Defs: []*plan2.TableDefType{},
		})
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(1))

		// snapshotTs=0 means no clone
		bat, err := ComputeDdlBatchWithSnapshot(ctx, "db1", "tbl1", eng, mp, txnOp, 0)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat, convey.ShouldNotBeNil)
		bat.Clean(mp)
	})

	convey.Convey("ComputeDdlBatchWithSnapshot with snapshotTs!=0 (clone)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		clonedTxn := mock_frontend.NewMockTxnOperator(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(clonedTxn)
		eng.EXPECT().Database(ctx, "db1", clonedTxn).Return(mockDb, nil).Times(2)
		mockDb.EXPECT().Relation(ctx, "tbl1", nil).Return(mockRel, nil)
		mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name: "tbl1", DbName: "db1", TableType: catalog.SystemOrdinaryRel, Defs: []*plan2.TableDefType{},
		})
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(1))

		bat, err := ComputeDdlBatchWithSnapshot(ctx, "db1", "tbl1", eng, mp, txnOp, 12345)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat, convey.ShouldNotBeNil)
		bat.Clean(mp)
	})
}

// ===========================================================================
// util.go parseCmd functions
// ===========================================================================

func TestSnapshotDdlCoverage_ParseCmdGetSnapshotTs(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdGetSnapshotTs", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdGetSnapshotTs(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "GET_SNAPSHOT_TS")
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_get_snapshot_ts snap1 acct1"
			_, err := parseCmdGetSnapshotTs(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "invalid")
		})
		convey.Convey("success", func() {
			sql := "__++__internal_get_snapshot_ts snap1 acct1 pub1"
			cmd, err := parseCmdGetSnapshotTs(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
			convey.So(cmd.accountName, convey.ShouldEqual, "acct1")
			convey.So(cmd.publicationName, convey.ShouldEqual, "pub1")
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdGetDatabases(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdGetDatabases", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdGetDatabases(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_get_databases snap1 acct1"
			_, err := parseCmdGetDatabases(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success", func() {
			sql := "__++__internal_get_databases snap1 acct1 pub1 account db1 tbl1"
			cmd, err := parseCmdGetDatabases(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
			convey.So(cmd.level, convey.ShouldEqual, "account")
			convey.So(cmd.dbName, convey.ShouldEqual, "db1")
			convey.So(cmd.tableName, convey.ShouldEqual, "tbl1")
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdGetMoIndexes(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdGetMoIndexes", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdGetMoIndexes(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_get_mo_indexes 123 acct1"
			_, err := parseCmdGetMoIndexes(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("invalid tableId", func() {
			sql := "__++__internal_get_mo_indexes notanumber acct1 pub1 snap1"
			_, err := parseCmdGetMoIndexes(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "invalid tableId")
		})
		convey.Convey("success", func() {
			sql := "__++__internal_get_mo_indexes 42 acct1 pub1 snap1"
			cmd, err := parseCmdGetMoIndexes(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.tableId, convey.ShouldEqual, 42)
			convey.So(cmd.subscriptionAccountName, convey.ShouldEqual, "acct1")
			convey.So(cmd.publicationName, convey.ShouldEqual, "pub1")
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdGetDdl(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdGetDdl", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdGetDdl(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_get_ddl snap1 acct1"
			_, err := parseCmdGetDdl(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success", func() {
			sql := "__++__internal_get_ddl snap1 acct1 pub1 account db1 tbl1"
			cmd, err := parseCmdGetDdl(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
			convey.So(cmd.subscriptionAccountName, convey.ShouldEqual, "acct1")
			convey.So(cmd.level, convey.ShouldEqual, "account")
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdGetObject(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdGetObject", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdGetObject(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_get_object acct1 pub1"
			_, err := parseCmdGetObject(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("invalid chunkIndex", func() {
			sql := "__++__internal_get_object acct1 pub1 obj1 notanumber"
			_, err := parseCmdGetObject(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "invalid chunkIndex")
		})
		convey.Convey("success", func() {
			sql := "__++__internal_get_object acct1 pub1 obj1 5"
			cmd, err := parseCmdGetObject(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.subscriptionAccountName, convey.ShouldEqual, "acct1")
			convey.So(cmd.objectName, convey.ShouldEqual, "obj1")
			convey.So(cmd.chunkIndex, convey.ShouldEqual, 5)
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdObjectList(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdObjectList", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdObjectList(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_object_list snap1"
			_, err := parseCmdObjectList(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success with dash against snapshot", func() {
			sql := "__++__internal_object_list snap1 - acct1 pub1"
			cmd, err := parseCmdObjectList(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
			convey.So(cmd.againstSnapshotName, convey.ShouldEqual, "")
			convey.So(cmd.subscriptionAccountName, convey.ShouldEqual, "acct1")
		})
		convey.Convey("success with real against snapshot", func() {
			sql := "__++__internal_object_list snap1 snap2 acct1 pub1"
			cmd, err := parseCmdObjectList(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.againstSnapshotName, convey.ShouldEqual, "snap2")
		})
	})
}

func TestSnapshotDdlCoverage_ParseCmdCheckSnapshotFlushed(t *testing.T) {
	ctx := context.Background()
	convey.Convey("parseCmdCheckSnapshotFlushed", t, func() {
		convey.Convey("not the right command", func() {
			_, err := parseCmdCheckSnapshotFlushed(ctx, "SELECT 1")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("wrong number of params", func() {
			sql := "__++__internal_check_snapshot_flushed snap1"
			_, err := parseCmdCheckSnapshotFlushed(ctx, sql)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success", func() {
			sql := "__++__internal_check_snapshot_flushed snap1 acct1 pub1"
			cmd, err := parseCmdCheckSnapshotFlushed(ctx, sql)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cmd.snapshotName, convey.ShouldEqual, "snap1")
			convey.So(cmd.subscriptionAccountName, convey.ShouldEqual, "acct1")
			convey.So(cmd.publicationName, convey.ShouldEqual, "pub1")
		})
	})
}

// ===========================================================================
// snapshot.go handler functions (handleGetSnapshotTs, handleGetDatabases,
// handleGetMoIndexes, handleInternalGetDdl) — these are also the functions
// dispatched in self_handle.go / execInFrontend
// ===========================================================================

// setupTestSession creates a session with the minimum scaffolding needed
// for handler tests. It returns the session with TxnHandler, mrs, and tenant set up.
func setupHandlerTestSession(
	t *testing.T,
	ctx context.Context,
	ctrl *gomock.Controller,
	eng engine.Engine,
	txnOperator *mock_frontend.MockTxnOperator,
) *Session {
	t.Helper()

	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOperator).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Fatal(err)
	}
	pu := config.NewParameterUnit(sv, eng, txnClient, nil)
	pu.SV.SkipCheckUser = true
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())
	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		t.Fatal(err)
	}
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

	txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
	ses.txnHandler = txnHandler

	proto.SetSession(ses)
	return ses
}

func TestSnapshotDdlCoverage_handleGetSnapshotTs(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleGetSnapshotTs", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			mock_frontend.NewMockDatabase(ctrl), nil,
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		ses := setupHandlerTestSession(t, ctx, ctrl, eng, txnOperator)

		// Stub NewBackgroundExec to return a mock BackgroundExec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{}).AnyTimes()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		convey.Convey("permission error", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return 0, "", moerr.NewInternalErrorNoCtx("permission denied")
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetSnapshotTs{
				snapshotName: "snap1", accountName: "sys", publicationName: "pub1",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetSnapshotTs(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "permission denied")
		})

		convey.Convey("snapshot query error (getSnapshotByName returns error)", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetSnapshotTs{
				snapshotName: "snap_missing", accountName: "sys", publicationName: "pub1",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetSnapshotTs(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("success with properly mocked bh returning snapshot record", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			// Override bh to return proper result for getSnapshotByName
			bhSuccess := mock_frontend.NewMockBackgroundExec(ctrl)
			bhSuccess.EXPECT().Close().Return().AnyTimes()
			bhSuccess.EXPECT().ClearExecResultSet().Return().AnyTimes()
			bhSuccess.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			erResult := mock_frontend.NewMockExecResult(ctrl)
			erResult.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("snap-id", nil).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("snap_ok", nil).AnyTimes()
			erResult.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(99999), nil).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("account", nil).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("sys", nil).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("", nil).AnyTimes()
			erResult.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("", nil).AnyTimes()
			erResult.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(7)).Return(uint64(0), nil).AnyTimes()
			bhSuccess.EXPECT().GetExecResultSet().Return([]interface{}{erResult}).AnyTimes()

			bhStub2 := gostub.StubFunc(&NewBackgroundExec, bhSuccess)
			defer bhStub2.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetSnapshotTs{
				snapshotName: "snap_ok", accountName: "sys", publicationName: "pub1",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetSnapshotTs(ses, execCtx, cmd)
			convey.So(err, convey.ShouldBeNil)

			mrs := ses.GetMysqlResultSet()
			convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))
		})
	})
}

func TestSnapshotDdlCoverage_handleGetDatabases(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleGetDatabases", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			mock_frontend.NewMockDatabase(ctrl), nil,
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		ses := setupHandlerTestSession(t, ctx, ctrl, eng, txnOperator)

		// Stub NewBackgroundExec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{}).AnyTimes()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		convey.Convey("permission error", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return 0, "", moerr.NewInternalErrorNoCtx("permission denied")
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDatabases{
				snapshotName: "snap1", accountName: "sys", publicationName: "pub1",
				level: "database", dbName: "db1", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetDatabases(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("database level with dash snapshot (no snapshot lookup)", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDatabases{
				snapshotName: "-", accountName: "sys", publicationName: "pub1",
				level: "database", dbName: "mydb", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetDatabases(ses, execCtx, cmd)
			convey.So(err, convey.ShouldBeNil)
			mrs := ses.GetMysqlResultSet()
			convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))
		})

		convey.Convey("table level with dash dbName placeholder", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDatabases{
				snapshotName: "-", accountName: "sys", publicationName: "pub1",
				level: "table", dbName: "-", tableName: "tbl1",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetDatabases(ses, execCtx, cmd)
			convey.So(err, convey.ShouldBeNil)
			mrs := ses.GetMysqlResultSet()
			convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))
			// The dbName should be empty string (dash replaced)
			convey.So(mrs.Data[0][0], convey.ShouldEqual, "")
		})
	})
}

func TestSnapshotDdlCoverage_handleGetMoIndexes(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleGetMoIndexes", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			mock_frontend.NewMockDatabase(ctrl), nil,
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		ses := setupHandlerTestSession(t, ctx, ctrl, eng, txnOperator)

		// Stub NewBackgroundExec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		erResult := mock_frontend.NewMockExecResult(ctrl)
		erResult.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erResult}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		convey.Convey("permission error", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return 0, "", moerr.NewInternalErrorNoCtx("no perm")
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetMoIndexes{
				tableId: 42, subscriptionAccountName: "sys", publicationName: "pub1", snapshotName: "snap1",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetMoIndexes(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("snapshot query error (bh returns no records)", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetMoIndexes{
				tableId: 42, subscriptionAccountName: "sys", publicationName: "pub1", snapshotName: "snap_missing",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetMoIndexes(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("dash snapshot name (use current ts, no index results)", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetMoIndexes{
				tableId: 42, subscriptionAccountName: "sys", publicationName: "pub1", snapshotName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleGetMoIndexes(ses, execCtx, cmd)
			convey.So(err, convey.ShouldBeNil)

			mrs := ses.GetMysqlResultSet()
			// 4 columns should be set up
			convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))
		})
	})
}

func TestSnapshotDdlCoverage_handleInternalGetDdl(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)

	convey.Convey("handleInternalGetDdl", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			mock_frontend.NewMockDatabase(ctrl), nil,
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		ses := setupHandlerTestSession(t, ctx, ctrl, eng, txnOperator)

		// Stub NewBackgroundExec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{}).AnyTimes()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		convey.Convey("permission error", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return 0, "", moerr.NewInternalErrorNoCtx("no access")
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDdl{
				snapshotName: "snap1", subscriptionAccountName: "sys", publicationName: "pub1",
				level: "database", dbName: "db1", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleInternalGetDdl(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("snapshot query error (bh returns no records)", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDdl{
				snapshotName: "snap_missing", subscriptionAccountName: "sys", publicationName: "pub1",
				level: "database", dbName: "db1", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleInternalGetDdl(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("success with dash snapshot and dash placeholders", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			// Stub ComputeDdlBatchWithSnapshotFunc to return an empty batch
			mp := mpool.MustNewZero()
			emptyBatch := batch.New([]string{"a", "b", "c", "d"})
			emptyBatch.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			emptyBatch.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			emptyBatch.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			emptyBatch.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			emptyBatch.SetRowCount(0)

			ddlStub := gostub.Stub(&ComputeDdlBatchWithSnapshotFunc, func(
				_ context.Context, _ string, _ string, _ engine.Engine, _ *mpool.MPool, _ TxnOperator, _ int64,
			) (*batch.Batch, error) {
				return emptyBatch, nil
			})
			defer ddlStub.Reset()
			defer emptyBatch.Clean(mp)

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDdl{
				snapshotName: "-", subscriptionAccountName: "sys", publicationName: "pub1",
				level: "account", dbName: "-", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleInternalGetDdl(ses, execCtx, cmd)
			convey.So(err, convey.ShouldBeNil)

			mrs := ses.GetMysqlResultSet()
			convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))
		})

		convey.Convey("ComputeDdlBatchWithSnapshot returns error", func() {
			pubStub := gostub.Stub(&getAccountFromPublicationFunc, func(_ context.Context, _ BackgroundExec, _ string, _ string, _ string) (uint64, string, error) {
				return uint64(catalog.System_Account), "sys", nil
			})
			defer pubStub.Reset()

			ddlStub := gostub.Stub(&ComputeDdlBatchWithSnapshotFunc, func(
				_ context.Context, _ string, _ string, _ engine.Engine, _ *mpool.MPool, _ TxnOperator, _ int64,
			) (*batch.Batch, error) {
				return nil, moerr.NewInternalErrorNoCtx("ddl computation failed")
			})
			defer ddlStub.Reset()

			ses.mrs = &MysqlResultSet{}
			cmd := &InternalCmdGetDdl{
				snapshotName: "-", subscriptionAccountName: "sys", publicationName: "pub1",
				level: "database", dbName: "db1", tableName: "-",
			}
			execCtx := &ExecCtx{reqCtx: ctx}
			err := handleInternalGetDdl(ses, execCtx, cmd)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "ddl computation failed")
		})
	})
}

// ===========================================================================
// self_handle.go / execInFrontend dispatching to InternalCmd types
// ===========================================================================

func TestSnapshotDdlCoverage_visitTableDdl_PropertiesNil(t *testing.T) {
	ctx := context.Background()

	convey.Convey("visitTableDdl with no existing PropertiesDef creates one", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		bat := batch.New([]string{"a", "b", "c", "d"})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
		defer bat.Clean(mp)

		eng.EXPECT().Database(ctx, "db", txnOp).Return(mockDb, nil)
		mockDb.EXPECT().Relation(ctx, "tbl", nil).Return(mockRel, nil)
		mockRel.EXPECT().GetTableDef(ctx).Return(&plan2.TableDef{TableType: catalog.SystemOrdinaryRel})
		// Return a table def with a PropertiesDef that does NOT have PropFromPublication
		mockRel.EXPECT().CopyTableDef(ctx).Return(&plan2.TableDef{
			Name:      "tbl",
			DbName:    "db",
			TableType: catalog.SystemOrdinaryRel,
			Defs: []*plan2.TableDefType{
				{
					Def: &plan2.TableDef_DefType_Properties{
						Properties: &plan2.PropertiesDef{
							Properties: []*plan2.Property{
								{Key: "other_key", Value: "val"},
							},
						},
					},
				},
			},
		})
		mockRel.EXPECT().GetTableID(ctx).Return(uint64(10))

		err := visitTableDdl(ctx, "db", "tbl", bat, txnOp, eng, mp)
		convey.So(err, convey.ShouldBeNil)
		convey.So(bat.RowCount(), convey.ShouldEqual, 1)
	})
}

func TestSnapshotDdlCoverage_getddlbatch_VisitDatabaseError(t *testing.T) {
	ctx := context.Background()

	convey.Convey("getddlbatch with visitDatabaseDdl error for specific db", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		// Database() returns error
		eng.EXPECT().Database(ctx, "bad_db", txnOp).Return(nil, fmt.Errorf("db not found"))

		bat, err := getddlbatch(ctx, "bad_db", "tbl", eng, mp, txnOp)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(bat, convey.ShouldBeNil)
	})

	convey.Convey("getddlbatch with error iterating all databases", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZero()
		eng := mock_frontend.NewMockEngine(ctrl)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		eng.EXPECT().Databases(ctx, txnOp).Return([]string{"good_db"}, nil)
		eng.EXPECT().Database(ctx, "good_db", txnOp).Return(nil, fmt.Errorf("db error"))

		bat, err := getddlbatch(ctx, "", "", eng, mp, txnOp)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(bat, convey.ShouldBeNil)
	})
}
