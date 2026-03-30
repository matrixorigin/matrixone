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
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestCompareRowInWrappedBatches(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	insertBatch := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{
		{int64(1), "same"},
		{int64(2), "tar"},
	})
	defer insertBatch.Clean(ses.proc.Mp())

	otherBatch := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{
		{int64(1), "same"},
		{int64(2), "base"},
	})
	defer otherBatch.Clean(ses.proc.Mp())

	cmp, err := compareRowInWrappedBatches(
		context.Background(),
		ses,
		tblStuff,
		0,
		0,
		true,
		batchWithKind{kind: diffDelete, batch: insertBatch},
		batchWithKind{kind: diffDelete, batch: otherBatch},
	)
	require.NoError(t, err)
	require.Zero(t, cmp)

	cmp, err = compareRowInWrappedBatches(
		context.Background(),
		ses,
		tblStuff,
		1,
		1,
		false,
		batchWithKind{kind: diffInsert, batch: insertBatch},
		batchWithKind{kind: diffInsert, batch: otherBatch},
	)
	require.NoError(t, err)
	require.NotZero(t, cmp)
}

func TestCheckConflictAndAppendToBat(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tarTuple := types.Tuple{int64(7), []byte("after"), []byte("target")}
	baseTuple := types.Tuple{int64(7), []byte("before"), []byte("base")}

	t.Run("default appends both sides", func(t *testing.T) {
		tarBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		defer tblStuff.retPool.releaseRetBatch(tarBat, false)
		defer tblStuff.retPool.releaseRetBatch(baseBat, false)

		err := checkConflictAndAppendToBat(ses, compositeOption{}, tblStuff, tarBat, baseBat, tarTuple, baseTuple)
		require.NoError(t, err)
		require.Equal(t, 1, tarBat.RowCount())
		require.Equal(t, 1, baseBat.RowCount())
	})

	t.Run("skip keeps both batches empty", func(t *testing.T) {
		tarBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		defer tblStuff.retPool.releaseRetBatch(tarBat, false)
		defer tblStuff.retPool.releaseRetBatch(baseBat, false)

		err := checkConflictAndAppendToBat(ses, compositeOption{
			conflictOpt: &tree.ConflictOpt{Opt: tree.CONFLICT_SKIP},
		}, tblStuff, tarBat, baseBat, tarTuple, baseTuple)
		require.NoError(t, err)
		require.Zero(t, tarBat.RowCount())
		require.Zero(t, baseBat.RowCount())
	})

	t.Run("accept appends target only", func(t *testing.T) {
		tarBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		defer tblStuff.retPool.releaseRetBatch(tarBat, false)
		defer tblStuff.retPool.releaseRetBatch(baseBat, false)

		err := checkConflictAndAppendToBat(ses, compositeOption{
			conflictOpt: &tree.ConflictOpt{Opt: tree.CONFLICT_ACCEPT},
		}, tblStuff, tarBat, baseBat, tarTuple, baseTuple)
		require.NoError(t, err)
		require.Equal(t, 1, tarBat.RowCount())
		require.Zero(t, baseBat.RowCount())
	})

	t.Run("fail returns conflict error", func(t *testing.T) {
		tarBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		defer tblStuff.retPool.releaseRetBatch(tarBat, false)
		defer tblStuff.retPool.releaseRetBatch(baseBat, false)

		err := checkConflictAndAppendToBat(ses, compositeOption{
			conflictOpt: &tree.ConflictOpt{Opt: tree.CONFLICT_FAIL},
		}, tblStuff, tarBat, baseBat, tarTuple, baseTuple)
		require.Error(t, err)
		require.Contains(t, err.Error(), "conflict:")
	})
}

func TestDiffDataHelper_Basic(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tarHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{
			{int64(1), "tar-only", "h1"},
			{int64(2), "same", "shared"},
			{int64(3), "new", "h3"},
		},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{
			{int64(2), "same", "shared"},
			{int64(3), "old", "h4"},
			{int64(4), "base-only", "h5"},
		},
	)
	defer func() {
		require.NoError(t, baseHashmap.Close())
	}()

	got := make(map[string][][]any)
	var mu sync.Mutex
	err := diffDataHelper(
		context.Background(),
		ses,
		0,
		compositeOption{},
		tblStuff,
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			tblStuff.retPool.releaseRetBatch(w.batch, false)
			if len(rows) == 0 {
				return false, nil
			}

			mu.Lock()
			key := fmt.Sprintf("%s-%d", w.kind, w.side)
			got[key] = append(got[key], rows...)
			mu.Unlock()
			return false, nil
		},
		tarHashmap,
		baseHashmap,
	)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.ElementsMatch(t, [][]any{
		{int64(1), "tar-only", "h1"},
		{int64(3), "new", "h3"},
	}, got["INSERT-1"])
	require.ElementsMatch(t, [][]any{
		{int64(3), "old", "h4"},
		{int64(4), "base-only", "h5"},
	}, got["INSERT-2"])
}

func TestDiffDataHelper_ConflictAcceptExpandUpdate(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
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

	got := make(map[string][][]any)
	var mu sync.Mutex
	err := diffDataHelper(
		context.Background(),
		ses,
		0,
		compositeOption{
			conflictOpt:  &tree.ConflictOpt{Opt: tree.CONFLICT_ACCEPT},
			expandUpdate: true,
		},
		tblStuff,
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			tblStuff.retPool.releaseRetBatch(w.batch, false)
			if len(rows) == 0 {
				return false, nil
			}

			mu.Lock()
			key := fmt.Sprintf("%s-%d", w.kind, w.side)
			got[key] = append(got[key], rows...)
			mu.Unlock()
			return false, nil
		},
		tarHashmap,
		baseHashmap,
	)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(7), "after", "target"}}, got["INSERT-1"])
	require.Equal(t, [][]any{{int64(7), "before", "base"}}, got["DELETE-2"])
}

type capturedBatch struct {
	kind string
	side int
	rows [][]any
}

func TestRunLCAProbeWithReaderFallback_EarlyReturns(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)

	t.Run("nil batch", func(t *testing.T) {
		ret, err := runLCAProbeWithReaderFallback(
			context.Background(),
			ses,
			nil,
			tblStuff,
			types.BuildTS(10, 0),
		)
		require.NoError(t, err)
		require.Nil(t, ret.Batches)
	})

	t.Run("empty batch", func(t *testing.T) {
		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		defer tBat.Clean(ses.proc.Mp())

		ret, err := runLCAProbeWithReaderFallback(
			context.Background(),
			ses,
			tBat,
			tblStuff,
			types.BuildTS(10, 0),
		)
		require.NoError(t, err)
		require.Len(t, ret.Batches, 1)
		require.Equal(t, 0, ret.Batches[0].RowCount())
		ret.Close()
	})
}

func TestRunLCAProbeWithReaderFallback_PrepareAndPropagateReaderError(t *testing.T) {
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
	eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(77)).
		Return("db1", "lca_tbl", rangeRel, nil).
		Times(1)

	ses.txnHandler = &TxnHandler{
		storage: eng,
		txnOp:   txnOp,
	}

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
	lcaDef := &plan.TableDef{
		DbName: "db1",
		Name:   "lca_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()

	tBat := batch.NewWithSize(1)
	tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(1), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(2), false, ses.proc.Mp()))
	tBat.SetRowCount(2)
	defer tBat.Clean(ses.proc.Mp())

	ret, err := runLCAProbeWithReaderFallback(
		context.Background(),
		ses,
		tBat,
		tblStuff,
		types.BuildTS(10, 0),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
	ret.Close()
}

func TestHandleDelsOnLCA_EarlyPaths(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("invalid snapshot", func(t *testing.T) {
		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)

		_, err := handleDelsOnLCA(
			context.Background(),
			ses,
			nil,
			nil,
			tblStuff,
			timestamp.Timestamp{},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid branch ts")
	})

	t.Run("reader probe with empty tombstone batch", func(t *testing.T) {
		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		tblStuff.lcaReaderProbeMode = &atomic.Bool{}
		tblStuff.lcaReaderProbeMode.Store(true)

		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		tBat.SetRowCount(0)
		defer tBat.Clean(ses.proc.Mp())

		dBat, err := handleDelsOnLCA(
			context.Background(),
			ses,
			nil,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.NotNil(t, dBat)
		require.Equal(t, 0, dBat.RowCount())
		require.Equal(t, 0, tBat.RowCount())
		tblStuff.retPool.releaseRetBatch(dBat, false)
	})
}

func newTestBranchTableStuff(ctrl *gomock.Controller) tableStuff {
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel := mock_frontend.NewMockRelation(ctrl)

	tarTableDef := &plan.TableDef{
		DbName: "db1",
		Name:   "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	baseTableDef := &plan.TableDef{
		DbName: "db1",
		Name:   "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}

	tarRel.EXPECT().GetTableName().Return("target").AnyTimes()
	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
	tarRel.EXPECT().GetTableDef(gomock.Any()).Return(tarTableDef).AnyTimes()
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(baseTableDef).AnyTimes()

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

func TestHandleDelsOnLCA_SQLPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("sql result keeps hits and leaves misses in tombstone batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{buildLCAProbeResultSet()}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(1), false, ses.proc.Mp()))
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(2), false, ses.proc.Mp()))
		tBat.SetRowCount(2)
		defer tBat.Clean(ses.proc.Mp())

		dBat, err := handleDelsOnLCA(
			context.Background(),
			ses,
			bh,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.Equal(t, 1, dBat.RowCount())
		require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](dBat.Vecs[0])[0])
		require.Equal(t, "alice", string(dBat.Vecs[1].GetBytesAt(0)))
		require.Equal(t, "h1", string(dBat.Vecs[2].GetBytesAt(0)))
		require.Equal(t, []int64{2}, vector.MustFixedColWithTypeCheck[int64](tBat.Vecs[0]))
		tblStuff.retPool.releaseRetBatch(dBat, false)
	})

	t.Run("recoverable sql error falls back to reader mode", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		tblStuff.lcaReaderProbeMode = &atomic.Bool{}
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(78)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(moerr.NewBadDBNoCtx("snapshot_gc")).Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		tBat.SetRowCount(0)
		defer tBat.Clean(ses.proc.Mp())

		dBat, err := handleDelsOnLCA(
			context.Background(),
			ses,
			bh,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.NotNil(t, dBat)
		require.Equal(t, 0, dBat.RowCount())
		require.Equal(t, 0, tBat.RowCount())
		require.True(t, tblStuff.lcaReaderProbeMode.Load())
		tblStuff.retPool.releaseRetBatch(dBat, false)
	})

	t.Run("fake pk sql builder propagates non recoverable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"__mo_fake_pk_col"},
				PkeyColName: "__mo_fake_pk_col",
			},
		}
		baseRel := mock_frontend.NewMockRelation(ctrl)
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"__mo_fake_pk_col"},
				PkeyColName: "__mo_fake_pk_col",
			},
		}
		baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
		baseRel.EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()
		tblStuff.baseRel = baseRel
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(79)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("sql failed")
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(wantErr).Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], uint64(42), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		defer tBat.Clean(ses.proc.Mp())

		_, err := handleDelsOnLCA(
			context.Background(),
			ses,
			bh,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("composite pk sql builder propagates non recoverable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id", "name"},
				PkeyColName: "__cpkey__",
			},
		}
		baseRel := mock_frontend.NewMockRelation(ctrl)
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id", "name"},
				PkeyColName: "__cpkey__",
				CompPkeyCol: &plan.ColDef{Name: "__cpkey__"},
			},
		}
		baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
		baseRel.EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()
		tblStuff.baseRel = baseRel
		tblStuff.def.pkColIdxes = []int{0, 1}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(80)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("sql failed")
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(wantErr).Times(1)

		packer := types.NewPacker()
		packer.EncodeInt64(1)
		packer.EncodeStringType([]byte("alice"))
		defer packer.Close()

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(tBat.Vecs[0], packer.GetBuf(), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		defer tBat.Clean(ses.proc.Mp())

		_, err := handleDelsOnLCA(
			context.Background(),
			ses,
			bh,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestHashDiff_NoLCAWithStubHandles(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.maxTombstoneBatchCnt = 1
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

	tarData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{int64(1), "old", "h1", commitTSBytes(types.BuildTS(5, 0))},
		{int64(1), "new", "h2", commitTSBytes(types.BuildTS(15, 0))},
	})
	tarTombstone := buildHashDiffTombstoneBatch(t, ses.proc.Mp(), [][]any{
		{int64(1), commitTSBytes(types.BuildTS(10, 0))},
	})
	baseData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{int64(3), "base-only", "hb", commitTSBytes(types.BuildTS(12, 0))},
	})

	var got []capturedBatch
	err = hashDiff(
		context.Background(),
		ses,
		nil,
		tblStuff,
		branchMetaInfo{lcaType: lcaEmpty},
		compositeOption{},
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			got = append(got, capturedBatch{
				kind: w.kind,
				side: w.side,
				rows: rows,
			})
			tblStuff.retPool.releaseRetBatch(w.batch, false)
			if len(rows) == 0 {
				got = got[:len(got)-1]
			}
			return false, nil
		},
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{
				data:      tarData,
				tombstone: tarTombstone,
			}},
		}},
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{
				data: baseData,
			}},
		}},
	)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, diffInsert, got[0].kind)
	require.Equal(t, diffSideTarget, got[0].side)
	require.Equal(t, [][]any{{int64(1), "new", "h2"}}, got[0].rows)
	require.Equal(t, diffInsert, got[1].kind)
	require.Equal(t, diffSideBase, got[1].side)
	require.Equal(t, [][]any{{int64(3), "base-only", "hb"}}, got[1].rows)
}

func buildVisibleComparisonBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"id", "name"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

	for _, row := range rows {
		require.Len(t, row, 2)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row[0].(int64), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row[1].(string)), false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildLCAProbeResultSet() *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, col := range []struct {
		name string
		typ  defines.MysqlType
	}{
		{name: "__idx_", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "id", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "name", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "hidden", typ: defines.MYSQL_TYPE_VARCHAR},
	} {
		mysqlCol := &MysqlColumn{}
		mysqlCol.SetName(col.name)
		mysqlCol.SetColumnType(col.typ)
		mrs.AddColumn(mysqlCol)
	}

	mrs.AddRow([]interface{}{int64(0), int64(1), "alice", "h1"})
	mrs.AddRow([]interface{}{int64(1), nil, nil, nil})
	return mrs
}

type stubEngineChangesHandle struct {
	responses []stubEngineChangesHandleResponse
	idx       int
}

type stubEngineChangesHandleResponse struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
	err       error
}

func (s *stubEngineChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if s.idx >= len(s.responses) {
		return nil, nil, engine.ChangesHandle_Snapshot, nil
	}
	resp := s.responses[s.idx]
	s.idx++
	return resp.data, resp.tombstone, resp.hint, resp.err
}

func (s *stubEngineChangesHandle) Close() error {
	return nil
}

func buildHashDiffDataBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{"id", "name", "hidden", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())

	for _, row := range rows {
		require.Len(t, row, 4)
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildHashDiffTombstoneBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"id", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

	for _, row := range rows {
		require.Len(t, row, 2)
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func commitTSBytes(ts types.TS) []byte {
	buf := make([]byte, len(ts))
	copy(buf, ts[:])
	return buf
}
