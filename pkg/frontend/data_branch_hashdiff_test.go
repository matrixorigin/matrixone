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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

func newTestBranchTableStuff(ctrl *gomock.Controller) tableStuff {
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel.EXPECT().GetTableName().Return("target").AnyTimes()
	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
	tarRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "target",
	}).AnyTimes()
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "base",
	}).AnyTimes()

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
