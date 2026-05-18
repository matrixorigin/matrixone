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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestCompareRowInWrappedBatches(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
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

	tblStuff := newTestFullScanTableStuff(ctrl)
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

	tblStuff := newTestFullScanTableStuff(ctrl)
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

	got := make(map[string][][]any)
	var mu sync.Mutex
	err := diffDataHelper(
		context.Background(),
		ses,
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

func TestRetBatchList_TombstoneBatchesHaveRowIDAndKeyVectors(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tombBat := tblStuff.retPool.acquireRetBatch(tblStuff, true)
	require.Equal(t, 3, tombBat.VectorCount())
	require.Equal(t, types.T_int64, tombBat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_Rowid, tombBat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_varbinary, tombBat.Vecs[2].GetType().Oid)

	rowID := testHashDiffRowid(11)
	require.NoError(t, vector.AppendFixed(tombBat.Vecs[0], int64(7), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendFixed(tombBat.Vecs[1], rowID, false, ses.proc.Mp()))
	require.NoError(t, vector.AppendBytes(tombBat.Vecs[2], []byte("encoded-key"), false, ses.proc.Mp()))
	tombBat.SetRowCount(1)
	tblStuff.retPool.releaseRetBatch(tombBat, true)

	reused := tblStuff.retPool.acquireRetBatch(tblStuff, true)
	defer tblStuff.retPool.releaseRetBatch(reused, true)
	require.Equal(t, 0, reused.RowCount())
	require.Equal(t, 0, reused.Vecs[0].Length())
	require.Equal(t, 0, reused.Vecs[1].Length())
	require.Equal(t, 0, reused.Vecs[2].Length())
}

func TestValidateLeadingRowIDAndSamples(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	valid := buildHashDiffDataBatch(t, mp, [][]any{
		{testHashDiffRowid(1), int64(1), "alice", "h1", commitTSBytes(types.BuildTS(10, 0))},
	})
	defer valid.Clean(mp)
	require.NoError(t, validateLeadingRowID("target", "tbl", false, valid))
	samples := batchSampleRowsForLog(valid, 1)
	require.Len(t, samples, 1)
	require.NotEmpty(t, samples[0])

	missing := batch.NewWithSize(0)
	missing.SetRowCount(1)
	require.Error(t, validateLeadingRowID("target", "tbl", false, missing))

	wrongType := batch.NewWithSize(1)
	wrongType.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(wrongType.Vecs[0], int64(1), false, mp))
	wrongType.SetRowCount(1)
	require.Error(t, validateLeadingRowID("target", "tbl", false, wrongType))
	wrongType.Clean(mp)

	nullRowID := batch.NewWithSize(1)
	nullRowID.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendAny(nullRowID.Vecs[0], nil, true, mp))
	nullRowID.SetRowCount(1)
	require.Error(t, validateLeadingRowID("target", "tbl", false, nullRowID))
	nullRowID.Clean(mp)

	emptyRowID := batch.NewWithSize(1)
	emptyRowID.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(emptyRowID.Vecs[0], types.EmptyRowid, false, mp))
	emptyRowID.SetRowCount(1)
	require.Error(t, validateLeadingRowID("target", "tbl", false, emptyRowID))
	emptyRowID.Clean(mp)
}

func TestTupleHelpersForPrune(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tuple := types.Tuple{int64(7), []byte("alice"), []byte("h1")}
	probeBat := tblStuff.retPool.acquireRetBatch(tblStuff, true)
	defer tblStuff.retPool.releaseRetBatch(probeBat, true)

	require.NoError(t, appendPruneProbeRow(ses, tblStuff, probeBat, []byte("key-7"), tuple))
	require.Equal(t, 1, probeBat.RowCount())
	require.Equal(t, int64(7), vector.MustFixedColWithTypeCheck[int64](probeBat.Vecs[0])[0])
	require.Equal(t, []byte("key-7"), probeBat.Vecs[2].GetBytesAt(0))

	lcaBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	defer tblStuff.retPool.releaseRetBatch(lcaBat, false)
	require.NoError(t, appendTupleToBat(ses, lcaBat, tuple, tblStuff))
	equal, err := visibleTupleEqualBatchRow(tuple, lcaBat, 0, tblStuff)
	require.NoError(t, err)
	require.True(t, equal)

	equal, err = visibleTupleEqualBatchRow(types.Tuple{int64(7), []byte("bob"), []byte("h1")}, lcaBat, 0, tblStuff)
	require.NoError(t, err)
	require.False(t, equal)

	require.True(t, tupleValueEqualVector([]byte("alice"), lcaBat.Vecs[1], 0))
	require.False(t, tupleValueEqualVector([]byte("bob"), lcaBat.Vecs[1], 0))
	require.Equal(t, []int{1, 2}, visibleTupleKeyIdxes(tblStuff))

	_, err = getTupleColumnValue(tuple, tblStuff, -1)
	require.Error(t, err)
	_, err = getTupleColumnValue(types.Tuple{int64(1)}, tblStuff, 0)
	require.Error(t, err)
}

func TestHandleDelsOnLCA_EarlyPaths(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("invalid snapshot", func(t *testing.T) {
		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)

		_, _, err := handleDelsOnLCA(
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

	t.Run("empty tombstone batch", func(t *testing.T) {
		tblStuff := newTestFullScanTableStuff(ctrl)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		tBat.SetRowCount(0)
		defer tBat.Clean(ses.proc.Mp())

		dBat, _, err := handleDelsOnLCA(
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

func TestHandleDelsOnLCA_SQLPaths(t *testing.T) {
	ses := newValidateSession(t)

	t.Run("sql result keeps hits and leaves misses in tombstone batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
		tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

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

		dBat, _, err := handleDelsOnLCA(
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

	t.Run("sql error is propagated", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(78)).AnyTimes()
		tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(moerr.NewBadDBNoCtx("snapshot_gc")).Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(1), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		defer tBat.Clean(ses.proc.Mp())

		_, _, err := handleDelsOnLCA(
			context.Background(),
			ses,
			bh,
			tBat,
			tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB))
	})

	t.Run("fake pk sql builder propagates non recoverable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"__mo_fake_pk_col"},
				PkeyColName: "__mo_fake_pk_col",
			},
		}
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"__mo_fake_pk_col"},
				PkeyColName: "__mo_fake_pk_col",
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(79)).AnyTimes()
		tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		wantErr := moerr.NewInternalErrorNoCtx("sql failed")
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(wantErr).Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], uint64(42), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		defer tBat.Clean(ses.proc.Mp())

		_, _, err := handleDelsOnLCA(
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

		tblStuff := newTestFullScanTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id", "name"},
				PkeyColName: "__cpkey__",
			},
		}
		baseDef := &plan.TableDef{
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id", "name"},
				PkeyColName: "__cpkey__",
				CompPkeyCol: &plan.ColDef{Name: "__cpkey__"},
			},
		}
		tblStuff.def.pkColIdxes = []int{0, 1}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(80)).AnyTimes()
		tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

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

		_, _, err := handleDelsOnLCA(
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

	tblStuff := newTestFullScanTableStuff(ctrl)
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.maxTombstoneBatchCnt = 1
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

	oldRowID := testHashDiffRowid(1)
	newRowID := testHashDiffRowid(2)
	baseRowID := testHashDiffRowid(3)
	tarData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{oldRowID, int64(1), "old", "h1", commitTSBytes(types.BuildTS(5, 0))},
		{newRowID, int64(1), "new", "h2", commitTSBytes(types.BuildTS(15, 0))},
	})
	tarTombstone := buildHashDiffTombstoneBatch(t, ses.proc.Mp(), [][]any{
		{oldRowID, int64(1), commitTSBytes(types.BuildTS(10, 0))},
	})
	baseData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{baseRowID, int64(3), "base-only", "hb", commitTSBytes(types.BuildTS(12, 0))},
	})

	var got []capturedBatch
	var mu sync.Mutex
	err = hashDiff(
		context.Background(),
		ses,
		nil,
		tblStuff,
		branchMetaInfo{},
		compositeOption{},
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			mu.Lock()
			got = append(got, capturedBatch{
				kind: w.kind,
				side: w.side,
				rows: rows,
			})
			if len(rows) == 0 {
				got = got[:len(got)-1]
			}
			mu.Unlock()
			tblStuff.retPool.releaseRetBatch(w.batch, false)
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
		nil, // no pickKeyHashmap for DIFF test
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

func TestPruneUnchangedDataOnLCA_RemovesOnlyRowsVisibleAtBothSnapshots(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newPruneTestTableStuff(ctrl, 81)
	tblStuff.maxTombstoneBatchCnt = 1

	dataHashmap := buildTestBranchHashmap(
		t,
		ses.proc.Mp(),
		tblStuff.def.colTypes,
		[][]any{{int64(1), "alice", "h1"}},
	)
	defer func() { require.NoError(t, dataHashmap.Close()) }()
	tombstoneHashmap, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	gomock.InOrder(
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
	)

	err = pruneUnchangedDataOnLCA(
		context.Background(),
		ses,
		bh,
		tblStuff,
		"target",
		types.BuildTS(10, 0),
		types.BuildTS(5, 0),
		dataHashmap,
		tombstoneHashmap,
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), dataHashmap.ItemCount())
}

func TestPruneUnchangedDataOnLCA_DoesNotUseTombstoneCountAsContainment(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newPruneTestTableStuff(ctrl, 82)
	tblStuff.maxTombstoneBatchCnt = 1

	dataHashmap := buildTestBranchHashmap(
		t,
		ses.proc.Mp(),
		tblStuff.def.colTypes,
		[][]any{{int64(1), "alice", "h1"}},
	)
	defer func() { require.NoError(t, dataHashmap.Close()) }()
	tombstoneHashmap := buildTestBranchHashmap(
		t,
		ses.proc.Mp(),
		tblStuff.def.colTypes,
		[][]any{
			{int64(2), "deleted", "h2"},
			{int64(3), "deleted", "h3"},
		},
	)
	defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	gomock.InOrder(
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
	)

	err := pruneUnchangedDataOnLCA(
		context.Background(),
		ses,
		bh,
		tblStuff,
		"target",
		types.BuildTS(10, 0),
		types.BuildTS(5, 0),
		dataHashmap,
		tombstoneHashmap,
	)
	require.NoError(t, err)
	require.Equal(t, int64(0), dataHashmap.ItemCount())
}

func TestPruneUnchangedDataOnLCA_KeepsRowsChangedAtCommonPrefix(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newPruneTestTableStuff(ctrl, 83)
	tblStuff.maxTombstoneBatchCnt = 1
	dataHashmap := buildTestBranchHashmap(
		t,
		ses.proc.Mp(),
		tblStuff.def.colTypes,
		[][]any{{int64(1), "alice", "h1"}},
	)
	defer func() { require.NoError(t, dataHashmap.Close()) }()
	tombstoneHashmap, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	gomock.InOrder(
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildLCAProbeResultSetFromRows([]interface{}{int64(0), int64(1), "bob", "h1"}),
		}),
		bh.EXPECT().ClearExecResultSet(),
	)

	err = pruneUnchangedDataOnLCA(
		context.Background(),
		ses,
		bh,
		tblStuff,
		"target",
		types.BuildTS(10, 0),
		types.BuildTS(5, 0),
		dataHashmap,
		tombstoneHashmap,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), dataHashmap.ItemCount())
}

func newPruneTestTableStuff(ctrl *gomock.Controller, lcaTableID uint64) tableStuff {
	tblStuff := newTestFullScanTableStuff(ctrl)
	tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
	lcaDef := &plan.TableDef{
		DbName: "db1",
		Name:   "lca_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(lcaTableID).AnyTimes()
	tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()
	return tblStuff
}

func TestBuildHashmapForTable_UsesRowIDAdjustedPKIndexes(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

	pickKeyHashmap := buildTestBranchHashmap(
		t,
		ses.proc.Mp(),
		[]types.Type{types.T_int64.ToType()},
		[][]any{{int64(2)}, {int64(3)}},
	)
	defer func() { require.NoError(t, pickKeyHashmap.Close()) }()

	dataBat := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{testHashDiffRowid(1), int64(1), "filtered-data", "h1", commitTSBytes(types.BuildTS(5, 0))},
		{testHashDiffRowid(2), int64(2), "kept-data", "h2", commitTSBytes(types.BuildTS(6, 0))},
	})
	tombstoneBat := buildHashDiffTombstoneBatch(t, ses.proc.Mp(), [][]any{
		{testHashDiffRowid(3), int64(3), commitTSBytes(types.BuildTS(7, 0))},
		{testHashDiffRowid(4), int64(4), commitTSBytes(types.BuildTS(8, 0))},
	})

	dataHashmap, tombstoneHashmap, err := buildHashmapForTable(
		context.Background(),
		ses.proc.Mp(),
		&tblStuff,
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{
				data:      dataBat,
				tombstone: tombstoneBat,
			}},
		}},
		"target",
		pickKeyHashmap,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, dataHashmap.Close()) }()
	defer func() { require.NoError(t, tombstoneHashmap.Close()) }()
	require.Equal(t, int64(1), dataHashmap.ItemCount())
	require.Equal(t, int64(1), tombstoneHashmap.ItemCount())

	idVec := vector.NewVec(types.T_int64.ToType())
	defer idVec.Free(ses.proc.Mp())
	require.NoError(t, vector.AppendFixed(idVec, int64(2), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendFixed(idVec, int64(3), false, ses.proc.Mp()))
	results, err := dataHashmap.GetByVectors([]*vector.Vector{idVec})
	require.NoError(t, err)
	require.True(t, results[0].Exists)
	require.False(t, results[1].Exists)

	results, err = tombstoneHashmap.GetByVectors([]*vector.Vector{idVec})
	require.NoError(t, err)
	require.False(t, results[0].Exists)
	require.True(t, results[1].Exists)
}

func TestFindDeleteAndUpdateBat_RemovesOnlyMatchingRowID(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestFullScanTableStuff(ctrl)
	tblStuff.maxTombstoneBatchCnt = 10
	tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
	lcaDef := &plan.TableDef{
		DbName: "db1",
		Name:   "lca_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
	tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(82)).AnyTimes()
	tblStuff.baseRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

	oldRowID := testHashDiffRowid(1)
	liveRowID := testHashDiffRowid(2)
	dataBat := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{oldRowID, int64(1), "old", "h1", commitTSBytes(types.BuildTS(5, 0))},
		{liveRowID, int64(1), "live", "h2", commitTSBytes(types.BuildTS(15, 0))},
	})
	defer dataBat.Clean(ses.proc.Mp())
	dataHashmap, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, dataHashmap.Close()) }()
	require.NoError(t, dataHashmap.PutByVectors(dataBat.Vecs, []int{1}))

	tombstoneBat := buildHashDiffTombstoneBatch(t, ses.proc.Mp(), [][]any{
		{oldRowID, int64(1), commitTSBytes(types.BuildTS(10, 0))},
	})
	defer tombstoneBat.Clean(ses.proc.Mp())
	tombstoneHashmap, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, tombstoneHashmap.Close()) }()
	require.NoError(t, tombstoneHashmap.PutByVectors(tombstoneBat.Vecs, []int{1}))

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{
		buildLCAProbeResultSetFromRows([]interface{}{int64(0), nil, nil, nil}),
	}).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	tmpCh := make(chan batchWithKind, 2)
	err = findDeleteAndUpdateBat(
		context.Background(),
		ses,
		bh,
		tblStuff,
		"target",
		diffSideTarget,
		tmpCh,
		types.BuildTS(10, 0),
		false,
		dataHashmap,
		tombstoneHashmap,
	)
	require.NoError(t, err)
	require.Len(t, tmpCh, 0)

	idVec := vector.NewVec(types.T_int64.ToType())
	defer idVec.Free(ses.proc.Mp())
	require.NoError(t, vector.AppendFixed(idVec, int64(1), false, ses.proc.Mp()))
	results, err := dataHashmap.GetByVectors([]*vector.Vector{idVec})
	require.NoError(t, err)
	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)
	tuple, _, err := dataHashmap.DecodeRow(results[0].Rows[0])
	require.NoError(t, err)
	gotRowID := types.DecodeFixed[types.Rowid](tuple[0].([]byte))
	require.Equal(t, liveRowID, gotRowID)
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
	return buildLCAProbeResultSetFromRows(
		[]interface{}{int64(0), int64(1), "alice", "h1"},
		[]interface{}{int64(1), nil, nil, nil},
	)
}

func buildLCAProbeResultSetFromRows(rows ...[]interface{}) *MysqlResultSet {
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

	for _, row := range rows {
		mrs.AddRow(row)
	}
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

	bat := batch.NewWithSize(5)
	bat.SetAttributes([]string{"__rowid", "id", "name", "hidden", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType())

	for _, row := range rows {
		require.Len(t, row, 5)
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildHashDiffTombstoneBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"__rowid", "id", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

	for _, row := range rows {
		require.Len(t, row, 3)
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func testHashDiffRowid(offset uint32) types.Rowid {
	var blk types.Blockid
	copy(blk[:], []byte("hashdiff-test-block-id"))
	return types.NewRowid(&blk, offset)
}

func commitTSBytes(ts types.TS) []byte {
	buf := make([]byte, len(ts))
	copy(buf, ts[:])
	return buf
}

func TestLCAProbeJoinCastType(t *testing.T) {
	tests := []struct {
		name string
		typ  types.Type
		want string
		ok   bool
	}{
		{name: "bit", typ: types.T_bit.ToType(), want: types.T_bit.ToType().DescString(), ok: true},
		{name: "int32", typ: types.T_int32.ToType(), want: "INT", ok: true},
		{name: "int64", typ: types.T_int64.ToType(), want: "BIGINT", ok: true},
		{name: "uint32", typ: types.T_uint32.ToType(), want: "INT UNSIGNED", ok: true},
		{name: "uint64", typ: types.T_uint64.ToType(), want: "BIGINT UNSIGNED", ok: true},
		{name: "float32", typ: types.T_float32.ToType(), want: "FLOAT", ok: true},
		{name: "float64", typ: types.T_float64.ToType(), want: "DOUBLE", ok: true},
		{name: "char", typ: types.T_char.ToType(), want: "VARCHAR", ok: true},
		{name: "varchar", typ: types.T_varchar.ToType(), want: "VARCHAR", ok: true},
		{name: "text", typ: types.T_text.ToType(), want: "VARCHAR", ok: true},
		{name: "binary", typ: types.T_binary.ToType(), want: "VARBINARY", ok: true},
		{name: "varbinary", typ: types.T_varbinary.ToType(), want: "VARBINARY", ok: true},
		{name: "decimal64", typ: types.New(types.T_decimal64, 12, 2), want: types.New(types.T_decimal64, 12, 2).DescString(), ok: true},
		{name: "decimal128", typ: types.New(types.T_decimal128, 18, 4), want: types.New(types.T_decimal128, 18, 4).DescString(), ok: true},
		{name: "date", typ: types.T_date.ToType(), want: types.T_date.ToType().String(), ok: true},
		{name: "datetime", typ: types.New(types.T_datetime, 0, 6), want: types.New(types.T_datetime, 0, 6).String(), ok: true},
		{name: "time", typ: types.New(types.T_time, 0, 6), want: types.New(types.T_time, 0, 6).String(), ok: true},
		{name: "timestamp", typ: types.New(types.T_timestamp, 0, 6), want: types.New(types.T_timestamp, 0, 6).String(), ok: true},
		{name: "unsupported", typ: types.T_bool.ToType(), want: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := lcaProbeJoinCastType(tt.typ)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
