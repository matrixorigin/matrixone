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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
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

func TestContainsDataBranchTempTableName(t *testing.T) {
	require.True(t, containsDataBranchTempTableName("delete from test.__mo_diff_del_merge_1"))
	require.True(t, containsDataBranchTempTableName("insert into __mo_diff_ins_merge_1 values (1)"))
	require.False(t, containsDataBranchTempTableName("select '__mo_diff_del_merge_1'"))
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

func TestRunSQL_DataBranchTempTablesUseBackgroundExec(t *testing.T) {
	ses := newValidateSession(t)
	spyExec := &pickStreamingExecutor{err: moerr.NewTxnNeedRetryWithDefChangedNoCtx()}
	_ = newPickStreamingBackExecForTest(t, ses, spyExec)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	stmt := "delete from test.__mo_diff_ins_merge_1"
	bh.EXPECT().Exec(gomock.Any(), stmt).Return(nil).Times(1)
	bh.EXPECT().GetExecResultSet().Return(nil).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	_, err := runSql(context.Background(), ses, bh, stmt, nil, nil)
	require.NoError(t, err)
	require.Empty(t, spyExec.sql)
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

	t.Run("returns error when range relation is missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

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
