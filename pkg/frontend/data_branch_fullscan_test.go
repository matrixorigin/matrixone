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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestDiffFullScanHashmaps_Basic(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tarHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{
			{int64(1), "tar-only", "h1"},
			{int64(2), "same", "target-hidden"},
			{int64(3), "new", "h3"},
		},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{
			{int64(2), "same", "base-hidden"},
			{int64(3), "old", "h4"},
			{int64(4), "base-only", "h5"},
		},
	)
	defer func() {
		require.NoError(t, baseHashmap.Close())
	}()

	got := make(map[string]capturedBatch)
	var mu sync.Mutex
	emit := func(w batchWithKind) (bool, error) {
		mu.Lock()
		got[fmt.Sprintf("%s-%d", w.kind, w.side)] = capturedBatch{
			kind: w.kind,
			side: w.side,
			rows: decodeCapturedRows(t, w.batch, tblStuff.def.colTypes),
		}
		mu.Unlock()
		tblStuff.retPool.releaseRetBatch(w.batch, false)
		return false, nil
	}

	err := diffFullScanHashmaps(context.Background(), ses, tblStuff, compositeOption{}, emit, tarHashmap, baseHashmap)
	require.NoError(t, err)

	require.Len(t, got, 3)
	require.Equal(t, [][]any{{int64(1), "tar-only", "h1"}}, got["INSERT-1"].rows)
	require.Equal(t, [][]any{{int64(3), "new", "h3"}}, got["UPDATE-1"].rows)
	require.Equal(t, [][]any{{int64(4), "base-only", "h5"}}, got["INSERT-2"].rows)
}

func TestDiffFullScanHashmaps_ExpandUpdate(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tarHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{{int64(7), "after", "target"}},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{{int64(7), "before", "base"}},
	)
	defer func() {
		require.NoError(t, baseHashmap.Close())
	}()

	var got []capturedBatch
	var mu sync.Mutex
	emit := func(w batchWithKind) (bool, error) {
		mu.Lock()
		got = append(got, capturedBatch{
			kind: w.kind,
			side: w.side,
			rows: decodeCapturedRows(t, w.batch, tblStuff.def.colTypes),
		})
		mu.Unlock()
		tblStuff.retPool.releaseRetBatch(w.batch, false)
		return false, nil
	}

	err := diffFullScanHashmaps(
		context.Background(), ses, tblStuff, compositeOption{expandUpdate: true}, emit, tarHashmap, baseHashmap,
	)
	require.NoError(t, err)

	require.Len(t, got, 2)
	require.Equal(t, diffDelete, got[0].kind)
	require.Equal(t, diffSideTarget, got[0].side)
	require.Equal(t, [][]any{{int64(7), "before", "base"}}, got[0].rows)
	require.Equal(t, diffInsert, got[1].kind)
	require.Equal(t, diffSideTarget, got[1].side)
	require.Equal(t, [][]any{{int64(7), "after", "target"}}, got[1].rows)
}

func TestShouldFallbackToFullScan(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "no commit ts", err: engine.ErrNoCommitTSColumn, want: true},
		{name: "file not found", err: moerr.NewFileNotFoundNoCtx("obj"), want: true},
		{name: "other", err: moerr.NewInternalErrorNoCtx("other"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldFallbackToFullScan(tc.err))
		})
	}
}

func TestScanSnapshotRelationByID_EarlyValidation(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("empty attrs", func(t *testing.T) {
		called := false
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			1,
			types.BuildTS(10, 0),
			nil,
			nil,
			nil,
			0,
			func(*batch.Batch) error {
				called = true
				return nil
			},
		)
		require.NoError(t, err)
		require.False(t, called)
	})

	t.Run("attrs type mismatch", func(t *testing.T) {
		err := scanSnapshotRelationByID(
			context.Background(),
			"unit-test",
			ses,
			1,
			types.BuildTS(10, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
	})

	t.Run("range relation must be disttae relation", func(t *testing.T) {
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		rangeRel := mock_frontend.NewMockRelation(ctrl)
		rangeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
			Return(readutil.NewBlockListRelationData(0), nil).
			Times(1)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(99)).
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
			99,
			types.BuildTS(10, 0),
			[]string{"id"},
			[]types.Type{types.T_int64.ToType()},
			nil,
			0,
			func(*batch.Batch) error { return nil },
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
	})
}

func TestScanTableIntoHashmap_EarlyValidation(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("empty attrs", func(t *testing.T) {
		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.def.colNames = nil
		tblStuff.def.colTypes = nil

		hm, err := scanTableIntoHashmap(
			context.Background(),
			ses,
			tblStuff,
			1,
			types.BuildTS(10, 0),
			"target",
		)
		require.NoError(t, err)
		require.NotNil(t, hm)
		require.Equal(t, int64(0), hm.ItemCount())
		require.NoError(t, hm.Close())
	})

	t.Run("attrs type mismatch", func(t *testing.T) {
		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.def.colNames = []string{"id"}
		tblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
		}

		hm, err := scanTableIntoHashmap(
			context.Background(),
			ses,
			tblStuff,
			1,
			types.BuildTS(10, 0),
			"target",
		)
		require.Error(t, err)
		require.Nil(t, hm)
		require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
	})
}

func TestFullTableScanDiff_PropagatesTargetScanError(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
	ses.txnHandler = &TxnHandler{txnOp: txnOp}

	tblStuff := newTestFullScanTableStuff(ctrl)
	tblStuff.tarRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()
	tblStuff.def.colNames = []string{"id"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}

	err := fullTableScanDiff(
		context.Background(),
		ses,
		tblStuff,
		compositeOption{},
		func(batchWithKind) (bool, error) {
			t.Fatal("scan error should stop before emit")
			return false, nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "attrs/colTypes length mismatch")
}

func TestFullTableScanDiff_PropagatesSnapshotReaderError(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	rangeRel := mock_frontend.NewMockRelation(ctrl)
	rangeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
		Return(readutil.NewBlockListRelationData(0), nil).
		Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(10)).
		Return("", "", rangeRel, nil).
		Times(1)

	ses.txnHandler = &TxnHandler{
		storage: eng,
		txnOp:   txnOp,
	}

	tblStuff := newTestFullScanTableStuff(ctrl)
	tblStuff.tarRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(11)).AnyTimes()

	err := fullTableScanDiff(
		context.Background(),
		ses,
		tblStuff,
		compositeOption{},
		func(batchWithKind) (bool, error) {
			t.Fatal("scan error should stop before emit")
			return false, nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
}

func TestAppendTupleToBat_TrimsTrailingCommitTS(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	defer tblStuff.retPool.releaseRetBatch(bat, false)

	err := appendTupleToBat(
		ses,
		bat,
		types.Tuple{int64(9), []byte("alice"), []byte("hidden"), types.BuildTS(10, 0)},
		tblStuff,
	)
	require.NoError(t, err)
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, int64(9), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])[0])
	require.Equal(t, "alice", string(bat.Vecs[1].GetBytesAt(0)))
	require.Equal(t, "hidden", string(bat.Vecs[2].GetBytesAt(0)))
}

func TestAppendTupleToBat_RejectsWidthMismatch(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	defer tblStuff.retPool.releaseRetBatch(bat, false)

	err := appendTupleToBat(ses, bat, types.Tuple{int64(9), []byte("short")}, tblStuff)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected tuple width")
}

func TestEmitUpdate_StopAfterDelete(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)

	var got []capturedBatch
	err := emitUpdate(
		ses,
		compositeOption{expandUpdate: true},
		tblStuff,
		func(w batchWithKind) (bool, error) {
			got = append(got, capturedBatch{
				kind: w.kind,
				side: w.side,
				rows: decodeCapturedRows(t, w.batch, tblStuff.def.colTypes),
			})
			return true, nil
		},
		types.Tuple{int64(7), []byte("after"), []byte("target")},
		types.Tuple{int64(7), []byte("before"), []byte("base")},
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, diffDelete, got[0].kind)
	require.Equal(t, [][]any{{int64(7), "before", "base"}}, got[0].rows)
}

func TestDiffFullScanHashmaps_ContextCanceled(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tarHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{{int64(1), "tar-only", "h1"}},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(t, mp, tblStuff.def.colTypes, nil)
	defer func() {
		require.NoError(t, baseHashmap.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := diffFullScanHashmaps(ctx, ses, tblStuff, compositeOption{}, func(batchWithKind) (bool, error) {
		t.Fatal("emit should not be called after cancellation")
		return false, nil
	}, tarHashmap, baseHashmap)
	require.ErrorIs(t, err, context.Canceled)
}

type capturedBatch struct {
	kind string
	side int
	rows [][]any
}

func newTestFullScanTableStuff(ctrl *gomock.Controller) tableStuff {
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel.EXPECT().GetTableName().Return("target").AnyTimes()
	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()

	var tblStuff tableStuff
	tblStuff.tarRel = tarRel
	tblStuff.baseRel = baseRel
	tblStuff.def.colNames = []string{"id", "name", "hidden"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.pkKind = normalKind
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0}
	tblStuff.retPool = &retBatchList{}
	return tblStuff
}

func buildTestBranchHashmap(
	t *testing.T,
	mp *mpool.MPool,
	colTypes []types.Type,
	rows [][]any,
) databranchutils.BranchHashmap {
	t.Helper()

	vecs := make([]*vector.Vector, len(colTypes))
	for i, typ := range colTypes {
		vecs[i] = vector.NewVec(typ)
		defer vecs[i].Free(mp)
	}
	for _, row := range rows {
		require.Len(t, row, len(colTypes))
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(vecs[i], val, mp))
		}
	}

	hm, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	require.NoError(t, hm.PutByVectors(vecs, []int{0}))
	return hm
}

func appendTestVectorValue(vec *vector.Vector, val any, mp *mpool.MPool) error {
	if val == nil {
		return vector.AppendAny(vec, nil, true, mp)
	}
	switch x := val.(type) {
	case int64:
		return vector.AppendFixed(vec, x, false, mp)
	case string:
		return vector.AppendBytes(vec, []byte(x), false, mp)
	case []byte:
		return vector.AppendBytes(vec, x, false, mp)
	default:
		return vector.AppendAny(vec, x, false, mp)
	}
}

func decodeCapturedRows(t *testing.T, bat *batch.Batch, colTypes []types.Type) [][]any {
	t.Helper()

	rows := make([][]any, 0, bat.RowCount())
	for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
		row := make([]any, len(colTypes))
		for colIdx, typ := range colTypes {
			vec := bat.Vecs[colIdx]
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colIdx] = nil
				continue
			}
			switch typ.Oid {
			case types.T_int64:
				row[colIdx] = vector.MustFixedColWithTypeCheck[int64](vec)[rowIdx]
			case types.T_varchar:
				row[colIdx] = string(vec.GetBytesAt(rowIdx))
			default:
				row[colIdx] = types.DecodeValue(vec.GetRawBytesAt(rowIdx), typ.Oid)
			}
		}
		rows = append(rows, row)
	}
	return rows
}
