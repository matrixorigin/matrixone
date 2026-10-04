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

package frontend

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestAcquireReleaseBuffer(t *testing.T) {
	t.Run("nil pool allocates fresh buffer", func(t *testing.T) {
		buf := acquireBuffer(nil)
		require.NotNil(t, buf)
		buf.WriteString("x")
		releaseBuffer(nil, buf)
		require.Zero(t, buf.Len())
	})

	t.Run("pool buffer is reset and reused", func(t *testing.T) {
		pool := &sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		}
		buf := acquireBuffer(pool)
		buf.WriteString("payload")
		releaseBuffer(pool, buf)

		reused := acquireBuffer(pool)
		require.Zero(t, reused.Len())
		releaseBuffer(pool, reused)
	})
}

func TestDataBranchSQLKeyEqual(t *testing.T) {
	require.Equal(t, "left_key = right_key",
		dataBranchSQLKeyEqual("left_key", "right_key", types.T_int64.ToType()))

	for _, typ := range []types.Type{types.T_float32.ToType(), types.T_float64.ToType()} {
		require.Equal(t, "serial(left_key) = serial(right_key)",
			dataBranchSQLKeyEqual("left_key", "right_key", typ),
		)
	}
}

func TestValidateDataBranchNamedSnapshotScope(t *testing.T) {
	ctx := context.Background()
	snapshot := &plan2.Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
		Name:  "snapshot",
		Level: tree.SNAPSHOTLEVELTABLE.String(),
		ObjId: 7,
	}}
	namedSnapshot := &tree.AtTimeStamp{Type: tree.ATTIMESTAMPSNAPSHOT}

	t.Run("unnamed source does not require a relation", func(t *testing.T) {
		require.NoError(t, validateDataBranchNamedSnapshotScope(
			ctx, nil, snapshot, "db", "table", nil,
		))
	})

	tests := []struct {
		name     string
		tableDef *planpb.TableDef
		err      string
	}{
		{
			name:     "matches logical table identity",
			tableDef: &planpb.TableDef{DbId: 1, TblId: 8, LogicalId: 7},
		},
		{
			name:     "rejects another table",
			tableDef: &planpb.TableDef{DbId: 1, TblId: 8, LogicalId: 9},
			err:      "internal error: table-level snapshot(snapshot) does not belong to the table(db-table)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			relation := mock_frontend.NewMockRelation(ctrl)
			relation.EXPECT().GetTableDef(ctx).Return(test.tableDef)

			err := validateDataBranchNamedSnapshotScope(
				ctx, namedSnapshot, snapshot, "db", "table", relation,
			)
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}

func TestNewEmitter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stop, err := newEmitter(ctx, make(chan struct{}), make(chan batchWithKind, 1))(batchWithKind{})
	require.False(t, stop)
	require.ErrorIs(t, err, context.Canceled)

	stopCh := make(chan struct{})
	close(stopCh)
	stop, err = newEmitter(context.Background(), stopCh, make(chan batchWithKind, 1))(batchWithKind{})
	require.True(t, stop)
	require.NoError(t, err)

	retCh := make(chan batchWithKind, 1)
	wrapped := batchWithKind{kind: diffInsert, side: diffSideTarget}
	stop, err = newEmitter(context.Background(), make(chan struct{}), retCh)(wrapped)
	require.False(t, stop)
	require.NoError(t, err)
	require.Equal(t, wrapped, <-retCh)
}

func TestRunSQL_BackgroundExecPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("converts mysql result set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), "drop database test_db").Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{buildRunSQLResultSet()}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		ret, err := runSql(context.Background(), ses, bh, "drop database test_db", nil, nil)
		require.NoError(t, err)
		require.Len(t, ret.Batches, 1)
		require.Equal(t, 1, ret.Batches[0].RowCount())
		require.Equal(t, int64(7), vectorValueAsInt64(ret.Batches[0], 0, 0))
		ret.Close()
	})

	t.Run("rejects unexpected result set type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), "drop database bad_db").Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{"bad-result"}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		_, err := runSql(context.Background(), ses, bh, "drop database bad_db", nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected result set type")
	})
}

func TestExecRetryableSQLStatementsUsesBackgroundExec(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "delete temp table target",
			sql:  "delete from test.__mo_diff_ins_merge_1",
		},
		{
			name: "delete main table using diff delete table",
			sql:  "delete from `db1`.`base` where `id` in (select `branch_apply_key_0` from `db1`.`__mo_diff_del_x`)",
		},
		{
			name: "insert main table using diff insert table",
			sql:  "insert into `db1`.`base` (`id`, `name`) select `id`, `name` from `db1`.`__mo_diff_ins_x`",
		},
		{
			name: "insert update staging row",
			sql:  "insert into `db1`.`__mo_diff_upd_x` values (1, 'new', 1)",
		},
		{
			name: "update main table using diff update table",
			sql:  "update `db1`.`base` as branch_apply_base join `db1`.`__mo_diff_upd_x` as branch_apply_stage on branch_apply_base.`id` = branch_apply_stage.`branch_apply_key_0` set branch_apply_base.`name` = branch_apply_stage.`name`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ses := newValidateSession(t)
			spyExec := &pickStreamingExecutor{err: moerr.NewTxnNeedRetryWithDefChangedNoCtx()}
			_ = newPickStreamingBackExecForTest(t, ses, spyExec)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().Exec(gomock.Any(), tt.sql).Return(nil).Times(1)
			bh.EXPECT().GetExecResultSet().Return(nil).Times(1)
			bh.EXPECT().ClearExecResultSet().Times(1)

			err := execRetryableSQLStatements(context.Background(), ses, bh, nil, []string{tt.sql})
			require.NoError(t, err)
			require.Empty(t, spyExec.sql)
		})
	}
}

func TestRunSQL_DataBranchUserIdentifiersUseInternalExec(t *testing.T) {
	statements := []string{
		"delete from test.orders where id = 1",
		"select * from test.__mo_diff_orders",
		"select * from `__mo_diff_orders`.`orders`",
		"select __mo_diff_flag from test.orders",
		"select * from test.__mo_diff_upd_orders",
	}

	for _, stmt := range statements {
		t.Run(stmt, func(t *testing.T) {
			ses := newValidateSession(t)
			spyExec := &pickStreamingExecutor{}
			bh := newPickStreamingBackExecForTest(t, ses, spyExec)

			ret, err := runSql(context.Background(), ses, bh, stmt, nil, nil)
			require.NoError(t, err)
			ret.Close()
			require.Equal(t, stmt, spyExec.sql)
		})
	}
}

func TestScanSnapshotRelationByID_EarlyAndErrorPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("empty attrs returns nil", func(t *testing.T) {
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			nil,
			nil,
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.NoError(t, err)
	})

	t.Run("attrs and col types mismatch", func(t *testing.T) {
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
	})

	t.Run("propagates get relation error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

		eng := mock_frontend.NewMockEngine(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("get relation failed")
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", nil, wantErr).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("uses historical fallback when current relation is gone", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

		currentLookupErr := moerr.NewInternalErrorNoCtx("can not find table by id 7")
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", nil, currentLookupErr).
			Times(1)

		historicalRel := mock_frontend.NewMockRelation(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("historical ranges reached")
		historicalRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
			Return(nil, wantErr).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByIDWithFallback(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			historicalRel,
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("returns error when range relation is missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", nil, nil).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot resolve range relation")
	})

	t.Run("propagates ranges error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

		rangeRel := mock_frontend.NewMockRelation(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("ranges failed")
		rangeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
			Return(nil, wantErr).
			Times(1)

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(7)).
			Return("", "", rangeRel, nil).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			7,
			types.BuildTS(20, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.ErrorIs(t, err, wantErr)
	})
}

func buildRunSQLResultSet() *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName("id")
	col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col)
	mrs.AddRow([]interface{}{int64(7)})
	return mrs
}

func vectorValueAsInt64(bat *batch.Batch, colIdx int, rowIdx int) int64 {
	return vector.MustFixedColWithTypeCheck[int64](bat.Vecs[colIdx])[rowIdx]
}
