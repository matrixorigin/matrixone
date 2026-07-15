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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

func TestCompareTupleWithBatchRow(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	bat := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{{int64(1), "alice"}})
	defer bat.Clean(ses.proc.Mp())

	cmp, err := compareTupleWithBatchRow(
		tblStuff,
		types.Tuple{int64(2), []byte("alice"), []byte("h1")},
		bat,
		0,
		true,
	)
	require.NoError(t, err)
	require.Zero(t, cmp)

	cmp, err = compareTupleWithBatchRow(
		tblStuff,
		types.Tuple{int64(2), []byte("alice"), []byte("h1")},
		bat,
		0,
		false,
	)
	require.NoError(t, err)
	require.NotZero(t, cmp)

	cmp, err = compareTupleWithBatchRow(
		tblStuff,
		types.Tuple{int64(1), []byte("bob"), []byte("h1")},
		bat,
		0,
		true,
	)
	require.NoError(t, err)
	require.NotZero(t, cmp)

	tblStuff.def.visibleIdxes = []int{2}
	_, err = compareTupleWithBatchRow(
		tblStuff,
		types.Tuple{int64(1), []byte("alice"), []byte("h1")},
		bat,
		0,
		false,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
}

func TestCompareTupleValueWithVectorNormalizesValues(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	t.Run("varchar accepts string and bytes", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendBytes(vec, []byte("alice"), false, mp))

		cmp, err := compareTupleValueWithVector("alice", vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)

		cmp, err = compareTupleValueWithVector([]byte("alice"), vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)

		cmp, err = compareTupleValueWithVector("bob", vec, 0)
		require.NoError(t, err)
		require.NotZero(t, cmp)
	})

	t.Run("json accepts raw bytes", func(t *testing.T) {
		vec := vector.NewVec(types.T_json.ToType())
		defer vec.Free(mp)
		jsonVal, err := types.ParseStringToByteJson(`{"k":1}`)
		require.NoError(t, err)
		raw, err := jsonVal.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, raw, false, mp))

		cmp, err := compareTupleValueWithVector(raw, vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("array accepts raw bytes", func(t *testing.T) {
		vec := vector.NewVec(types.T_array_float32.ToType())
		defer vec.Free(mp)
		val := []float32{1, 2}
		require.NoError(t, vector.AppendArray(vec, val, false, mp))

		cmp, err := compareTupleValueWithVector(types.ArrayToBytes(val), vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("array float64 accepts raw bytes", func(t *testing.T) {
		vec := vector.NewVec(types.T_array_float64.ToType())
		defer vec.Free(mp)
		val := []float64{1, 2}
		require.NoError(t, vector.AppendArray(vec, val, false, mp))

		cmp, err := compareTupleValueWithVector(types.ArrayToBytes(val), vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("rowid accepts raw bytes", func(t *testing.T) {
		vec := vector.NewVec(types.T_Rowid.ToType())
		defer vec.Free(mp)
		rowID := buildHashDiffRowID(t, 7)
		require.NoError(t, vector.AppendFixed(vec, rowID, false, mp))

		cmp, err := compareTupleValueWithVector(types.EncodeFixed(rowID), vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("decimal256 accepts raw DecodeRow bytes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		decimalTyp := types.New(types.T_decimal256, 40, 4)
		amount, err := types.ParseDecimal256("12345678901234567890123456789012345.1234", decimalTyp.Width, decimalTyp.Scale)
		require.NoError(t, err)

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.def.colNames = []string{"id", "amount"}
		tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), decimalTyp}
		tblStuff.def.visibleIdxes = []int{0, 1}
		tblStuff.def.pkColIdx = 0
		tblStuff.def.pkColIdxes = []int{0}

		hm := buildTestBranchHashmap(
			t,
			mp,
			tblStuff.def.colTypes,
			[][]any{{int64(1), amount}},
		)
		defer func() {
			require.NoError(t, hm.Close())
		}()

		probe := buildFixedVector(t, mp, types.T_int64.ToType(), int64(1))
		defer probe.Free(mp)

		results, err := hm.GetByVectors([]*vector.Vector{probe})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.True(t, results[0].Exists)
		require.Len(t, results[0].Rows, 1)

		tuple, _, err := hm.DecodeRow(results[0].Rows[0])
		require.NoError(t, err)
		require.Len(t, tuple, 2)
		rawAmount, ok := tuple[1].([]byte)
		require.True(t, ok)
		require.Equal(t, types.EncodeFixed(amount), rawAmount)

		bat := batch.NewWithSize(2)
		defer bat.Clean(mp)
		bat.Vecs[0] = buildFixedVector(t, mp, types.T_int64.ToType(), int64(1))
		bat.Vecs[1] = buildFixedVector(t, mp, decimalTyp, amount)
		bat.SetRowCount(1)

		cmp, err := compareTupleWithBatchRow(tblStuff, tuple, bat, 0, false)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("enum accepts uint16", func(t *testing.T) {
		vec := vector.NewVec(types.T_enum.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendFixed(vec, types.Enum(3), false, mp))

		cmp, err := compareTupleValueWithVector(uint16(3), vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)
	})

	t.Run("null handling", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))

		cmp, err := compareTupleValueWithVector(nil, vec, 0)
		require.NoError(t, err)
		require.Zero(t, cmp)

		cmp, err = compareTupleValueWithVector(int64(1), vec, 0)
		require.NoError(t, err)
		require.NotZero(t, cmp)
	})
}

func TestDataBranchCompareValueErrors(t *testing.T) {
	cmp, err := compareSingleValueByType(types.T_int64, nil, int64(1))
	require.NoError(t, err)
	require.Negative(t, cmp)

	_, err = compareSingleValueByType(types.T_int64, int64(1), int32(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported or mismatched type")

	_, err = compareSingleValueByType(types.T_int64, struct{}{}, struct{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported or mismatched type")

	_, err = normalizeCompareValue(types.T_json.ToType(), "not-json-bytes")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected compare value type")

	_, err = normalizeCompareValue(types.T_Rowid.ToType(), []byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty raw compare value")

	_, err = normalizeCompareValue(types.T_Rowid.ToType(), "not-rowid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected compare value type")
}

func TestNormalizeCompareValueSupportedForms(t *testing.T) {
	jsonVal, err := types.ParseStringToByteJson(`{"k":1}`)
	require.NoError(t, err)
	got, err := normalizeCompareValue(types.T_json.ToType(), jsonVal)
	require.NoError(t, err)
	require.Equal(t, jsonVal, got)

	got, err = normalizeCompareValue(types.T_varchar.ToType(), []byte("alice"))
	require.NoError(t, err)
	require.Equal(t, []byte("alice"), got)

	array32 := []float32{1, 2}
	got, err = normalizeCompareValue(types.T_array_float32.ToType(), array32)
	require.NoError(t, err)
	require.Equal(t, array32, got)

	array64 := []float64{1, 2}
	got, err = normalizeCompareValue(types.T_array_float64.ToType(), array64)
	require.NoError(t, err)
	require.Equal(t, array64, got)

	rowID := buildHashDiffRowID(t, 8)
	got, err = normalizeCompareValue(types.T_Rowid.ToType(), rowID)
	require.NoError(t, err)
	require.Equal(t, rowID, got)

	var blockID types.Blockid
	got, err = normalizeCompareValue(types.T_Blockid.ToType(), types.EncodeFixed(blockID))
	require.NoError(t, err)
	require.Equal(t, blockID, got)

	ts := types.BuildTS(1, 2)
	got, err = normalizeCompareValue(types.T_TS.ToType(), types.EncodeFixed(ts))
	require.NoError(t, err)
	require.Equal(t, ts, got)

	year := types.MoYear(2026)
	got, err = normalizeCompareValue(types.T_year.ToType(), types.EncodeFixed(year))
	require.NoError(t, err)
	require.Equal(t, year, got)

	enum := types.Enum(5)
	got, err = normalizeCompareValue(types.T_enum.ToType(), enum)
	require.NoError(t, err)
	require.Equal(t, enum, got)

	got, err = normalizeCompareValue(types.T_int64.ToType(), int64(9))
	require.NoError(t, err)
	require.Equal(t, int64(9), got)
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

func TestDiffDataHelperClosesMigratedHashmaps(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.pkKind = fakeKind

	baseMigratedHashmap := &closeTrackingBranchHashmap{}
	tarMigratedHashmap := &closeTrackingBranchHashmap{}
	baseHashmap := &closeTrackingBranchHashmap{migrated: baseMigratedHashmap}
	tarHashmap := &closeTrackingBranchHashmap{migrated: tarMigratedHashmap}

	err := diffDataHelper(
		context.Background(),
		nil,
		compositeOption{},
		tblStuff,
		func(batchWithKind) (bool, error) {
			return false, nil
		},
		tarHashmap,
		baseHashmap,
	)
	require.NoError(t, err)
	require.Equal(t, 1, baseHashmap.migrateCalls)
	require.Equal(t, 1, tarHashmap.migrateCalls)
	require.Equal(t, []int{1, 2}, baseHashmap.migrateKeyCols)
	require.Equal(t, []int{1, 2}, tarHashmap.migrateKeyCols)
	require.Equal(t, 1, baseHashmap.closeCalls)
	require.Equal(t, 1, tarHashmap.closeCalls)
	require.Equal(t, 1, baseMigratedHashmap.closeCalls)
	require.Equal(t, 1, tarMigratedHashmap.closeCalls)
}

func TestDiffDataHelperFakePKUsesCommonVisibleColumnsAsKey(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.pkKind = fakeKind
	tblStuff.def.colNames = []string{"a", "c", "b", catalog.FakePrimaryKeyColName}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_uint64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.commonIdxes = []int{0, 2}
	tblStuff.def.commonVisibleIdxes = []int{0, 2}
	tblStuff.def.tarOnlyIdxes = []int{1}
	tblStuff.def.pkColIdxes = []int{0, 2}

	// Rows differ only in target-only column c. Fake-PK matching must use the
	// common visible columns [a,b], otherwise schema evolution turns a no-op
	// target-only update into one target INSERT plus one base INSERT.
	rowColTypes := append([]types.Type{types.T_Rowid.ToType()}, tblStuff.def.colTypes...)
	tarHashmap := buildTestBranchHashmap(
		t, mp, rowColTypes,
		[][]any{{buildHashDiffRowID(t, 1), int64(1), int64(99), int64(1), uint64(11)}},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(
		t, mp, rowColTypes,
		[][]any{{buildHashDiffRowID(t, 2), int64(1), nil, int64(1), uint64(22)}},
	)
	defer func() {
		require.NoError(t, baseHashmap.Close())
	}()

	var got []capturedBatch
	var mu sync.Mutex
	err := diffDataHelper(
		context.Background(),
		ses,
		compositeOption{},
		tblStuff,
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			mu.Lock()
			if len(rows) != 0 {
				got = append(got, capturedBatch{
					kind: w.kind,
					side: w.side,
					rows: rows,
				})
			}
			mu.Unlock()
			tblStuff.retPool.releaseRetBatch(w.batch, false)
			return false, nil
		},
		tarHashmap,
		baseHashmap,
	)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBuildHashmapForTableProjectsBaseRowsBeforeKeying(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"a", "b", "c"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.commonIdxes = []int{0, 1}
	tblStuff.def.commonVisibleIdxes = []int{0, 1}
	tblStuff.def.tarOnlyIdxes = []int{2}
	tblStuff.def.baseColToTarIdx = []int{0, 1}
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0}
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker

	baseRowID := buildHashDiffRowID(t, 1)
	baseBat := buildIntDataBatch(
		t, ses.proc.Mp(), []string{catalog.Row_ID, "a", "b"},
		[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
		[][]any{{baseRowID, int64(1), int64(1)}},
	)
	baseDataHashmap, baseTombstoneHashmap, err := buildHashmapForTable(
		context.Background(),
		ses.proc.Mp(),
		&tblStuff,
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{data: baseBat}},
		}},
		"base",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, baseDataHashmap.Close())
		require.NoError(t, baseTombstoneHashmap.Close())
	}()

	targetRowID := buildHashDiffRowID(t, 2)
	targetBat := buildIntDataBatch(
		t, ses.proc.Mp(), []string{catalog.Row_ID, "a", "b", "c"},
		[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
		[][]any{{targetRowID, int64(1), int64(99), int64(0)}},
	)
	targetDataHashmap, targetTombstoneHashmap, err := buildHashmapForTable(
		context.Background(),
		ses.proc.Mp(),
		&tblStuff,
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{data: targetBat}},
		}},
		"target",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, targetDataHashmap.Close())
		require.NoError(t, targetTombstoneHashmap.Close())
	}()

	require.NoError(t, targetDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			ret, err := baseDataHashmap.PopByEncodedKey(key, false)
			require.NoError(t, err)
			require.True(t, ret.Exists)
			require.Len(t, ret.Rows, 1)
			baseTuple, _, err := baseDataHashmap.DecodeRow(ret.Rows[0])
			require.NoError(t, err)
			require.Len(t, baseTuple, 4)
			return nil
		})
	}, -1))
}

func TestDataBranchLineageTableIDReportsLegacyLogicalFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dag := databranchutils.NewDAG([]databranchutils.DataBranchMetadata{
		{TableID: 10, PTableID: 1, CloneTS: 100},
	})
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		TblId:     20,
		LogicalId: 10,
	}).AnyTimes()

	tableID, legacyFallback := dataBranchLineageTableID(context.Background(), dag, rel)
	require.Equal(t, uint64(10), tableID)
	require.True(t, legacyFallback)
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

func TestDiffDataHelper_ConflictAcceptExpandUpdateWithSparseCommonIdxes(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"id", "target_only", "name"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.commonIdxes = []int{0, 2}
	tblStuff.def.commonVisibleIdxes = []int{0, 2}
	tblStuff.def.tarOnlyIdxes = []int{1}

	// Both sides changed the same PK after branching. The target-only value is
	// identical, while the trailing common column differs; conflict detection
	// must therefore compare target physical index 2, not dense ordinal 1.
	tarHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{{int64(7), "same-extra", "target-update"}},
	)
	defer func() {
		require.NoError(t, tarHashmap.Close())
	}()
	baseHashmap := buildTestBranchHashmap(
		t, mp, tblStuff.def.colTypes,
		[][]any{{int64(7), "same-extra", "base-update"}},
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
	require.Equal(t, [][]any{{int64(7), "same-extra", "target-update"}}, got["INSERT-1"])
	require.Equal(t, [][]any{{int64(7), "same-extra", "base-update"}}, got["DELETE-2"])
}

type closeTrackingBranchHashmap struct {
	migrated           databranchutils.BranchHashmap
	migrateCalls       int
	migrateKeyCols     []int
	migrateParallelism int
	closeCalls         int
}

func (h *closeTrackingBranchHashmap) PutByVectors(_ []*vector.Vector, _ []int) error {
	return nil
}

func (h *closeTrackingBranchHashmap) GetByVectors(_ []*vector.Vector) ([]databranchutils.GetResult, error) {
	return nil, nil
}

func (h *closeTrackingBranchHashmap) GetByEncodedKey(_ []byte) (databranchutils.GetResult, error) {
	return databranchutils.GetResult{}, nil
}

func (h *closeTrackingBranchHashmap) PopByVectors(_ []*vector.Vector, _ bool) ([]databranchutils.GetResult, error) {
	return nil, nil
}

func (h *closeTrackingBranchHashmap) PopByVectorsStream(_ []*vector.Vector, _ bool, _ func(idx int, key []byte, row []byte) error) (int, error) {
	return 0, nil
}

func (h *closeTrackingBranchHashmap) PopByEncodedKey(_ []byte, _ bool) (databranchutils.GetResult, error) {
	return databranchutils.GetResult{}, nil
}

func (h *closeTrackingBranchHashmap) PopByEncodedKeyValue(_ []byte, _ []byte, _ bool) (int, error) {
	return 0, nil
}

func (h *closeTrackingBranchHashmap) PopByEncodedFullValue(_ []byte, _ bool) (databranchutils.GetResult, error) {
	return databranchutils.GetResult{}, nil
}

func (h *closeTrackingBranchHashmap) PopByEncodedFullValueExact(_ []byte, _ bool) (int, error) {
	return 0, nil
}

func (h *closeTrackingBranchHashmap) ForEachShardParallel(_ func(databranchutils.ShardCursor) error, _ int) error {
	return nil
}

func (h *closeTrackingBranchHashmap) Project(_ []int, _ int) (databranchutils.BranchHashmap, error) {
	return h.migrated, nil
}

func (h *closeTrackingBranchHashmap) Migrate(keyCols []int, parallelism int) (databranchutils.BranchHashmap, error) {
	h.migrateCalls++
	h.migrateKeyCols = append([]int(nil), keyCols...)
	h.migrateParallelism = parallelism
	return h.migrated, nil
}

func (h *closeTrackingBranchHashmap) ItemCount() int64 {
	return 0
}

func (h *closeTrackingBranchHashmap) ShardCount() int {
	return 0
}

func (h *closeTrackingBranchHashmap) DecodeRow(_ []byte) (types.Tuple, []types.Type, error) {
	return nil, nil, nil
}

func (h *closeTrackingBranchHashmap) Close() error {
	h.closeCalls++
	return nil
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
	txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

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
	lcaDef := newTestBranchTableDef("lca_tbl", "name")
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

		lcaDef := newTestBranchTableDef("lca_tbl", "name")
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

	tarTableDef := newTestBranchTableDef("target", "name")
	baseTableDef := newTestBranchTableDef("base", "name")

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

func newTestBranchTableDef(tableName, dataColumnName string) *plan.TableDef {
	return &plan.TableDef{
		DbName: "db1",
		Name:   tableName,
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: dataColumnName, ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "hidden", ColId: 3, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
}

func TestLCAProbeColumnLayoutExcludesTargetOnlyColumns(t *testing.T) {
	lcaDef := &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}},
	}}

	layout := lcaProbeColumnLayout(
		lcaDef,
		[]string{"id", "name", "added"},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()},
	)

	require.Equal(t, []string{"id", "name"}, layout.attrs)
	require.Equal(t, []int{0, 1}, layout.targetIdxes)
	require.Equal(t, []types.T{types.T_int64, types.T_varchar}, []types.T{layout.types[0].Oid, layout.types[1].Oid})
}

func TestLCAProbeResultTargetIndexes(t *testing.T) {
	layout := lcaProbeLayout{targetIdxes: []int{1, 2}}

	got, err := lcaProbeResultTargetIndexes(layout, 3, 2, false)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, got, "SQL probe returns only LCA/common columns")

	got, err = lcaProbeResultTargetIndexes(layout, 3, 3, true)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2}, got, "reader fallback returns the full target layout")

	_, err = lcaProbeResultTargetIndexes(layout, 3, 1, false)
	require.ErrorContains(t, err, "unexpected LCA probe result width")
}

func makeTestBranchTableStuffFakePK(tblStuff *tableStuff) {
	tblStuff.def.pkKind = fakeKind
	tblStuff.def.pkColIdxes = []int{0, 1}
	tblStuff.def.pkColIdx = 0
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
	case uint64:
		return vector.AppendFixed(vec, x, false, mp)
	case types.Rowid:
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

	t.Run("target-only first column preserves LCA delete", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		tblStuff.def.colNames = []string{"added", "id", "name"}
		tblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(),
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
		}
		tblStuff.def.visibleIdxes = []int{0, 1, 2}
		tblStuff.def.pkColIdx = 1
		tblStuff.def.pkColIdxes = []int{1}
		tblStuff.retPool = &retBatchList{}
		lcaDef := &plan.TableDef{
			DbName: "db1",
			Name:   "lca_tbl",
			Pkey: &plan.PrimaryKeyDef{
				Names:       []string{"id"},
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
		}
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(76)).AnyTimes()

		mrs := &MysqlResultSet{}
		for _, col := range []struct {
			name string
			typ  defines.MysqlType
		}{
			{name: "__idx_", typ: defines.MYSQL_TYPE_LONGLONG},
			{name: "id", typ: defines.MYSQL_TYPE_LONGLONG},
			{name: "name", typ: defines.MYSQL_TYPE_VARCHAR},
		} {
			mysqlCol := &MysqlColumn{}
			mysqlCol.SetName(col.name)
			mysqlCol.SetColumnType(col.typ)
			mrs.AddColumn(mysqlCol)
		}
		mrs.AddRow([]interface{}{int64(0), int64(1), "alice"})

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{mrs}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(1), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		defer tBat.Clean(ses.proc.Mp())

		dBat, err := handleDelsOnLCA(
			context.Background(), ses, bh, tBat, tblStuff,
			types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.Equal(t, 1, dBat.RowCount())
		require.True(t, dBat.Vecs[0].IsNull(0))
		require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](dBat.Vecs[1]))
		require.Equal(t, "alice", string(dBat.Vecs[2].GetBytesAt(0)))
		require.Zero(t, tBat.RowCount())
		tblStuff.retPool.releaseRetBatch(dBat, false)
	})

	t.Run("sql result keeps hits and leaves misses in tombstone batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		targetTblDef := tblStuff.tarRel.GetTableDef(context.Background())
		targetTblDef.Cols[1].Name = "bb"
		tblStuff.def.colNames[1] = "bb"
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := newTestBranchTableDef("lca_tbl", "b")
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			require.Contains(t, sql, "lca.`b`")
			require.NotContains(t, sql, "lca.`bb`")
			return nil
		}).Times(1)
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
		lcaDef := newTestBranchTableDef("lca_tbl", "name")
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

	t.Run("recoverable sql error returns fallback error when reader probe fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
		txnOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOp).Times(1)

		eng := mock_frontend.NewMockEngine(ctrl)
		rangeRel := mock_frontend.NewMockRelation(ctrl)
		rangeRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).
			Return(readutil.NewBlockListRelationData(0), nil).
			Times(1)
		eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(781)).
			Return("db1", "lca_tbl", rangeRel, nil).
			Times(1)

		ses.txnHandler = &TxnHandler{
			storage: eng,
			txnOp:   txnOp,
		}

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		tblStuff.lcaReaderProbeMode = &atomic.Bool{}
		lcaDef := newTestBranchTableDef("lca_tbl", "name")
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
		tblStuff.lcaRel.(*mock_frontend.MockRelation).EXPECT().GetTableID(gomock.Any()).Return(uint64(781)).AnyTimes()

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(moerr.NewBadDBNoCtx("snapshot_gc")).Times(1)

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(tBat.Vecs[0], int64(1), false, ses.proc.Mp()))
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "snapshot scan requires disttae relation")
		require.True(t, tblStuff.lcaReaderProbeMode.Load())
	})

	t.Run("fake pk sql builder propagates non recoverable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.lcaRel = mock_frontend.NewMockRelation(ctrl)
		lcaDef := newTestBranchTableDef("lca_tbl", "name")
		lcaDef.Pkey = &plan.PrimaryKeyDef{
			Names:       []string{"__mo_fake_pk_col"},
			PkeyColName: "__mo_fake_pk_col",
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
		lcaDef := newTestBranchTableDef("lca_tbl", "name")
		lcaDef.Pkey = &plan.PrimaryKeyDef{
			Names:       []string{"id", "name"},
			PkeyColName: "__cpkey__",
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

func TestHashDiff_HasLCANoopUpdateFiltering(t *testing.T) {
	for _, tt := range []struct {
		name      string
		rowName   string
		hidden    string
		wantRows  [][]any
		wantEmits int
	}{
		{
			name:      "update reverted to lca value is suppressed",
			rowName:   "alice",
			hidden:    "h1",
			wantEmits: 0,
		},
		{
			name:      "real update is still emitted",
			rowName:   "bob",
			hidden:    "h2",
			wantRows:  [][]any{{int64(1), "bob", "h2"}},
			wantEmits: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ses := newValidateSession(t)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()
			ses.txnHandler = &TxnHandler{
				txnOp: txnOp,
			}

			tblStuff := newTestBranchTableStuff(ctrl)
			worker, err := ants.NewPool(1)
			require.NoError(t, err)
			defer worker.Release()
			tblStuff.worker = worker
			tblStuff.maxTombstoneBatchCnt = 1
			tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

			lcaRel := mock_frontend.NewMockRelation(ctrl)
			lcaDef := newTestBranchTableDef("lca_tbl", "name")
			lcaRel.EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
			lcaRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
			tblStuff.lcaRel = lcaRel

			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, sql string) error {
					// The raw changes contain two same-PK tombstones, but
					// buildHashmapForTable compacts them before LCA probing.
					require.Equal(t, 1, strings.Count(sql, "row("))
					return nil
				},
			).Times(1)
			bh.EXPECT().GetExecResultSet().Return([]interface{}{
				buildLCAProbeResultSetWithRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
			}).Times(1)
			bh.EXPECT().ClearExecResultSet().Times(1)

			oldRowID := buildHashDiffRowID(t, 0)
			midRowID := buildHashDiffRowID(t, 1)
			newRowID := buildHashDiffRowID(t, 2)
			tarData := buildHashDiffDataBatchWithRowIDs(
				t, ses.proc.Mp(), []types.Rowid{midRowID, newRowID},
				[][]any{
					{int64(1), "intermediate", "h0", commitTSBytes(types.BuildTS(12, 0))},
					{int64(1), tt.rowName, tt.hidden, commitTSBytes(types.BuildTS(15, 0))},
				},
			)
			tarTombstone := buildHashDiffTombstoneBatchWithRowIDs(
				t, ses.proc.Mp(), []types.Rowid{oldRowID, midRowID},
				[][]any{
					{int64(1), commitTSBytes(types.BuildTS(12, 0))},
					{int64(1), commitTSBytes(types.BuildTS(15, 0))},
				},
			)

			dagInfo := branchMetaInfo{
				lcaTableId:          77,
				pathFromLCAToTar:    []uint64{77, 88},
				pathFromLCAToTarTS:  []types.TS{{}, types.BuildTS(10, 0)},
				pathFromLCAToBase:   []uint64{77},
				pathFromLCAToBaseTS: []types.TS{{}},
			}

			var got []capturedBatch
			var mu sync.Mutex
			err = hashDiff(
				context.Background(),
				ses,
				bh,
				tblStuff,
				dagInfo,
				compositeOption{},
				func(w batchWithKind) (bool, error) {
					rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
					mu.Lock()
					if len(rows) != 0 {
						got = append(got, capturedBatch{
							kind: w.kind,
							side: w.side,
							rows: rows,
						})
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
				nil,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, got, tt.wantEmits)
			if tt.wantEmits == 0 {
				return
			}
			require.Equal(t, diffUpdate, got[0].kind)
			require.Equal(t, diffSideTarget, got[0].side)
			require.Equal(t, tt.wantRows, got[0].rows)
		})
	}
}

func TestHashDiff_HasLCAUpdateIgnoresTargetOnlyColumns(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()
	ses.txnHandler = &TxnHandler{
		txnOp: txnOp,
	}

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.commonIdxes = []int{0, 1}
	tblStuff.def.commonVisibleIdxes = []int{0, 1}
	tblStuff.def.tarOnlyIdxes = []int{2}
	tblStuff.def.baseColToTarIdx = []int{0, 1}
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.maxTombstoneBatchCnt = 1
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

	lcaRel := mock_frontend.NewMockRelation(ctrl)
	lcaDef := &plan.TableDef{
		DbName: "db1",
		Name:   "lca_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}
	lcaRel.EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
	lcaRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
	tblStuff.lcaRel = lcaRel

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{
		buildLCAProbeResultSetWithRows([]interface{}{int64(0), int64(1), "before", nil}),
	}).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	oldRowID := buildHashDiffRowID(t, 0)
	newRowID := buildHashDiffRowID(t, 1)
	tarData := buildHashDiffDataBatchWithRowIDs(
		t, ses.proc.Mp(), []types.Rowid{newRowID},
		[][]any{{int64(1), "after", "target-only", commitTSBytes(types.BuildTS(15, 0))}},
	)
	tarTombstone := buildHashDiffTombstoneBatchWithRowIDs(
		t, ses.proc.Mp(), []types.Rowid{oldRowID},
		[][]any{{int64(1), commitTSBytes(types.BuildTS(12, 0))}},
	)

	dagInfo := branchMetaInfo{
		lcaTableId:          77,
		pathFromLCAToTar:    []uint64{77, 88},
		pathFromLCAToTarTS:  []types.TS{{}, types.BuildTS(10, 0)},
		pathFromLCAToBase:   []uint64{77},
		pathFromLCAToBaseTS: []types.TS{{}},
	}

	var got []capturedBatch
	var mu sync.Mutex
	err = hashDiff(
		context.Background(),
		ses,
		bh,
		tblStuff,
		dagInfo,
		compositeOption{},
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			mu.Lock()
			if len(rows) != 0 {
				got = append(got, capturedBatch{
					kind: w.kind,
					side: w.side,
					rows: rows,
				})
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
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, diffUpdate, got[0].kind)
	require.Equal(t, diffSideTarget, got[0].side)
	require.Equal(t, [][]any{{int64(1), "after", "target-only"}}, got[0].rows)
}

func TestHashDiff_HasLCAFakePKUpdateIsNotNoop(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()
	ses.txnHandler = &TxnHandler{
		txnOp: txnOp,
	}

	tblStuff := newTestBranchTableStuff(ctrl)
	makeTestBranchTableStuffFakePK(&tblStuff)
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.maxTombstoneBatchCnt = 1
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)

	lcaRel := mock_frontend.NewMockRelation(ctrl)
	lcaDef := newTestBranchTableDef("lca_tbl", "name")
	lcaRel.EXPECT().GetTableDef(gomock.Any()).Return(lcaDef).AnyTimes()
	lcaRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(77)).AnyTimes()
	tblStuff.lcaRel = lcaRel

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{
		buildLCAProbeResultSetWithRows([]interface{}{int64(0), int64(1), "alice", "h1"}),
	}).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	oldRowID := buildHashDiffRowID(t, 0)
	newRowID := buildHashDiffRowID(t, 1)
	tarData := buildHashDiffDataBatchWithRowIDs(
		t, ses.proc.Mp(), []types.Rowid{newRowID},
		[][]any{{int64(1), "bob", "h1", commitTSBytes(types.BuildTS(15, 0))}},
	)
	tarTombstone := buildHashDiffTombstoneBatchWithRowIDs(
		t, ses.proc.Mp(), []types.Rowid{oldRowID},
		[][]any{{int64(1), commitTSBytes(types.BuildTS(12, 0))}},
	)

	dagInfo := branchMetaInfo{
		lcaTableId:          77,
		pathFromLCAToTar:    []uint64{77, 88},
		pathFromLCAToTarTS:  []types.TS{{}, types.BuildTS(10, 0)},
		pathFromLCAToBase:   []uint64{77},
		pathFromLCAToBaseTS: []types.TS{{}},
	}

	var got []capturedBatch
	var mu sync.Mutex
	err = hashDiff(
		context.Background(),
		ses,
		bh,
		tblStuff,
		dagInfo,
		compositeOption{},
		func(w batchWithKind) (bool, error) {
			rows := decodeCapturedRows(t, w.batch, tblStuff.def.colTypes)
			mu.Lock()
			if len(rows) != 0 {
				got = append(got, capturedBatch{
					kind: w.kind,
					side: w.side,
					rows: rows,
				})
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
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, diffUpdate, got[0].kind)
	require.Equal(t, diffSideTarget, got[0].side)
	require.Equal(t, [][]any{{int64(1), "bob", "h1"}}, got[0].rows)
}

func TestHashDiff_FakePKClosesMigratedHashmaps(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.pkKind = fakeKind
	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	defer worker.Release()
	tblStuff.worker = worker
	tblStuff.maxTombstoneBatchCnt = 1
	alloc := &trackingHashmapAllocator{}
	tblStuff.hashmapAllocator = &branchHashmapAllocator{upstream: alloc}

	tarData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{int64(1), "target", "h1", commitTSBytes(types.BuildTS(15, 0))},
	})
	baseData := buildHashDiffDataBatch(t, ses.proc.Mp(), [][]any{
		{int64(2), "base", "h2", commitTSBytes(types.BuildTS(12, 0))},
	})

	err = hashDiff(
		context.Background(),
		ses,
		nil,
		tblStuff,
		branchMetaInfo{},
		compositeOption{},
		func(w batchWithKind) (bool, error) {
			tblStuff.retPool.releaseRetBatch(w.batch, false)
			return false, nil
		},
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{data: tarData}},
		}},
		[]engine.ChangesHandle{&stubEngineChangesHandle{
			responses: []stubEngineChangesHandleResponse{{data: baseData}},
		}},
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, alloc.outstanding.Load())
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

func buildIntDataBatch(t *testing.T, mp *mpool.MPool, attrs []string, colTypes []types.Type, rows [][]any) *batch.Batch {
	t.Helper()

	require.Len(t, attrs, len(colTypes))
	bat := batch.NewWithSize(len(colTypes))
	bat.SetAttributes(attrs)
	for i, typ := range colTypes {
		bat.Vecs[i] = vector.NewVec(typ)
	}
	for _, row := range rows {
		require.Len(t, row, len(colTypes))
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildLCAProbeResultSet() *MysqlResultSet {
	return buildLCAProbeResultSetWithRows(
		[]interface{}{int64(0), int64(1), "alice", "h1"},
		[]interface{}{int64(1), nil, nil, nil},
	)
}

func buildLCAProbeResultSetWithRows(rows ...[]interface{}) *MysqlResultSet {
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

type trackingHashmapAllocator struct {
	outstanding atomic.Int64
}

func (a *trackingHashmapAllocator) Allocate(size uint64, _ malloc.Hints) ([]byte, malloc.Deallocator, error) {
	a.outstanding.Add(int64(size))
	return make([]byte, int(size)), &trackingHashmapDeallocator{
		allocator: a,
		size:      size,
	}, nil
}

type trackingHashmapDeallocator struct {
	allocator *trackingHashmapAllocator
	size      uint64
}

func (d *trackingHashmapDeallocator) Deallocate() {
	d.allocator.outstanding.Add(-int64(d.size))
}

func (d *trackingHashmapDeallocator) As(malloc.Trait) bool {
	return false
}

func buildHashDiffDataBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	rowIDs := make([]types.Rowid, len(rows))
	for i := range rows {
		rowIDs[i] = buildHashDiffRowID(t, i)
	}
	return buildHashDiffDataBatchWithRowIDs(t, mp, rowIDs, rows)
}

func buildHashDiffDataBatchWithRowIDs(t *testing.T, mp *mpool.MPool, rowIDs []types.Rowid, rows [][]any) *batch.Batch {
	t.Helper()
	require.Len(t, rowIDs, len(rows))

	bat := batch.NewWithSize(5)
	bat.SetAttributes([]string{catalog.Row_ID, "id", "name", "hidden", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType())

	for rowIdx, row := range rows {
		require.Len(t, row, 4)
		require.NoError(t, appendTestVectorValue(bat.Vecs[0], rowIDs[rowIdx], mp))
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i+1], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildHashDiffTombstoneBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	rowIDs := make([]types.Rowid, len(rows))
	for i := range rows {
		rowIDs[i] = buildHashDiffRowID(t, i)
	}
	return buildHashDiffTombstoneBatchWithRowIDs(t, mp, rowIDs, rows)
}

func buildHashDiffTombstoneBatchWithRowIDs(t *testing.T, mp *mpool.MPool, rowIDs []types.Rowid, rows [][]any) *batch.Batch {
	t.Helper()
	require.Len(t, rowIDs, len(rows))

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{catalog.Row_ID, "id", "__commit_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

	for rowIdx, row := range rows {
		require.Len(t, row, 2)
		require.NoError(t, appendTestVectorValue(bat.Vecs[0], rowIDs[rowIdx], mp))
		for i, val := range row {
			require.NoError(t, appendTestVectorValue(bat.Vecs[i+1], val, mp))
		}
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildHashDiffRowID(t *testing.T, rowIdx int) types.Rowid {
	t.Helper()
	var uid types.Uuid
	uid[0] = byte(rowIdx + 1)
	uid[15] = byte(rowIdx + 1)
	blkID := objectio.NewBlockid(&uid, 0, 1)
	return types.NewRowid(blkID, uint32(rowIdx))
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
		{name: "varchar", typ: types.T_varchar.ToType(), want: "VARCHAR", ok: true},
		{name: "varbinary", typ: types.T_varbinary.ToType(), want: "VARBINARY", ok: true},
		{name: "decimal64", typ: types.New(types.T_decimal64, 12, 2), want: types.New(types.T_decimal64, 12, 2).DescString(), ok: true},
		{name: "decimal256", typ: types.New(types.T_decimal256, 39, 4), want: types.New(types.T_decimal256, 39, 4).DescString(), ok: true},
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

func TestAppendLCAProbeValue(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("restores enum code from right join label", func(t *testing.T) {
		src := vector.NewVec(types.T_varchar.ToType())
		dst := vector.NewVec(types.T_enum.ToType())
		defer src.Free(mp)
		defer dst.Free(mp)

		require.NoError(t, vector.AppendBytes(src, []byte("blue"), false, mp))
		require.NoError(t, appendLCAProbeValue(dst, src, 0, "red,blue,green", mp))
		require.Equal(t, []types.Enum{2}, vector.MustFixedColWithTypeCheck[types.Enum](dst))
	})

	t.Run("rejects unsupported storage conversion", func(t *testing.T) {
		src := vector.NewVec(types.T_varchar.ToType())
		dst := vector.NewVec(types.T_int64.ToType())
		defer src.Free(mp)
		defer dst.Free(mp)

		require.NoError(t, vector.AppendBytes(src, []byte("2"), false, mp))
		err := appendLCAProbeValue(dst, src, 0, "", mp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected LCA probe type conversion")
	})
}

func TestValidateLeadingRowID(t *testing.T) {
	mp := mpool.MustNewZero()

	makeBatchWithLeadingType := func(t *testing.T, oid types.T) *batch.Batch {
		t.Helper()
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.Row_ID, "id"})
		bat.Vecs[0] = vector.NewVec(oid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
		bat.SetRowCount(1)
		return bat
	}

	t.Run("nil and empty pass", func(t *testing.T) {
		require.NoError(t, validateLeadingRowID("base", "t", false, nil))
		bat := batch.NewWithSize(0)
		bat.SetRowCount(0)
		require.NoError(t, validateLeadingRowID("base", "t", false, bat))
	})

	t.Run("missing rowid vector", func(t *testing.T) {
		bat := batch.NewWithSize(0)
		bat.SetRowCount(1)
		err := validateLeadingRowID("base", "t", false, bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataBranch-CollectChanges-MissingRowID")
	})

	t.Run("wrong leading type", func(t *testing.T) {
		bat := makeBatchWithLeadingType(t, types.T_int64)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		err := validateLeadingRowID("base", "t", false, bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataBranch-CollectChanges-InvalidRowIDVector")
	})

	t.Run("length mismatch", func(t *testing.T) {
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.Row_ID, "id"})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(2), false, mp))
		uid, err := types.BuildUuid()
		require.NoError(t, err)
		blkID := objectio.NewBlockid(&uid, 0, 1)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(blkID, 0), false, mp))
		bat.SetRowCount(2)
		err = validateLeadingRowID("base", "t", false, bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataBranch-CollectChanges-RowIDLenMismatch")
	})

	t.Run("null rowid", func(t *testing.T) {
		bat := makeBatchWithLeadingType(t, types.T_Rowid)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.Rowid{}, true, mp))
		err := validateLeadingRowID("base", "t", false, bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataBranch-CollectChanges-NullRowID")
	})

	t.Run("empty rowid", func(t *testing.T) {
		bat := makeBatchWithLeadingType(t, types.T_Rowid)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.EmptyRowid, false, mp))
		err := validateLeadingRowID("base", "t", false, bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataBranch-CollectChanges-InvalidRowID")
	})

	t.Run("valid rowid passes", func(t *testing.T) {
		bat := makeBatchWithLeadingType(t, types.T_Rowid)
		uid, err := types.BuildUuid()
		require.NoError(t, err)
		blkID := objectio.NewBlockid(&uid, 0, 1)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(blkID, 0), false, mp))
		require.NoError(t, validateLeadingRowID("base", "t", false, bat))
	})
}

func TestProjectBaseBatchToTarget(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)

	// Simulate target having [a, c, b], base has [a, b].
	tblStuff.def.colNames = []string{"a", "c", "b"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	// base has [a, b] only. baseColToTarIdx says:
	// base col 0 (a) -> target idx 0
	// base col 1 (b) -> target idx 2
	// c is target-only and sits between the common columns
	tblStuff.def.baseColToTarIdx = []int{0, 2}
	tblStuff.def.tarOnlyIdxes = []int{1}

	mp := ses.proc.Mp()

	// Build a base-side data batch with 1 RowID + 2 columns [a, b]
	baseBat := batch.NewWithSize(3)
	baseBat.SetAttributes([]string{catalog.Row_ID, "a", "b"})
	baseBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	baseBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	baseBat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

	uid, err := types.BuildUuid()
	require.NoError(t, err)
	blkID := objectio.NewBlockid(&uid, 0, 1)
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[0], types.NewRowid(blkID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[1], int64(42), false, mp))
	require.NoError(t, vector.AppendBytes(baseBat.Vecs[2], []byte("hello"), false, mp))
	baseBat.SetRowCount(1)

	rowIDVec := baseBat.Vecs[0]
	aVec := baseBat.Vecs[1]
	bVec := baseBat.Vecs[2]
	projected := projectBaseBatchToTarget(baseBat, &tblStuff, mp)

	// Should have 4 vectors: RowID + [a, c, b]
	require.Equal(t, 4, projected.VectorCount())
	require.Equal(t, 1, projected.RowCount())

	// Moved vectors transfer ownership to projected.
	require.Same(t, rowIDVec, projected.Vecs[0])
	require.Same(t, aVec, projected.Vecs[1])
	require.Same(t, bVec, projected.Vecs[3])

	// RowID should be preserved
	require.Equal(t, types.T_Rowid, projected.Vecs[0].GetType().Oid)

	// Col a (index 1): should be int64(42)
	require.Equal(t, types.T_int64, projected.Vecs[1].GetType().Oid)
	require.Equal(t, int64(42), vector.MustFixedColWithTypeCheck[int64](projected.Vecs[1])[0])

	// Col c (index 2): should be constant NULL (target-only)
	require.Equal(t, types.T_int64, projected.Vecs[2].GetType().Oid)
	require.True(t, projected.Vecs[2].IsConst())
	require.True(t, projected.Vecs[2].IsConstNull(), "target-only column should be const NULL")

	// Col b (index 3): should be "hello"
	require.Equal(t, types.T_varchar, projected.Vecs[3].GetType().Oid)
	require.Equal(t, "hello", string(projected.Vecs[3].GetBytesAt(0)))

	projected.Clean(mp)
}

func TestProjectBaseBatchToTargetPreservesTrailingCommitTS(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"a", "c", "b"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.baseColToTarIdx = []int{0, 2}
	tblStuff.def.tarOnlyIdxes = []int{1}

	mp := ses.proc.Mp()
	baseline := mp.CurrNB()
	baseBat := batch.NewWithSize(4)
	baseBat.SetAttributes([]string{catalog.Row_ID, "a", "b", objectio.DefaultCommitTS_Attr})
	baseBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	baseBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	baseBat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	baseBat.Vecs[3] = vector.NewVec(types.T_TS.ToType())

	uid, err := types.BuildUuid()
	require.NoError(t, err)
	blkID := objectio.NewBlockid(&uid, 0, 1)
	commitTS := types.BuildTS(42, 7)
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[0], types.NewRowid(blkID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[1], int64(11), false, mp))
	require.NoError(t, vector.AppendBytes(baseBat.Vecs[2], []byte("base"), false, mp))
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[3], commitTS, false, mp))
	baseBat.SetRowCount(1)

	rowIDVec := baseBat.Vecs[0]
	aVec := baseBat.Vecs[1]
	bVec := baseBat.Vecs[2]
	commitTSVec := baseBat.Vecs[3]
	projected := projectBaseBatchToTarget(baseBat, &tblStuff, mp)

	// CollectChanges data batches are [RowID, data..., commit_ts]. Projection
	// changes only the data-column layout and retains both boundary vectors.
	require.Equal(t, 5, projected.VectorCount())
	require.True(t, dataBranchBatchHasTargetLayout(projected, &tblStuff))
	require.Same(t, rowIDVec, projected.Vecs[0])
	require.Same(t, aVec, projected.Vecs[1])
	require.True(t, projected.Vecs[2].IsConstNull())
	require.Same(t, bVec, projected.Vecs[3])
	require.Same(t, commitTSVec, projected.Vecs[4])
	require.Equal(t, commitTS, vector.GetFixedAtNoTypeCheck[types.TS](projected.Vecs[4], 0))

	projected.Clean(mp)
	require.Equal(t, baseline, mp.CurrNB(), "projection must transfer or clean every input vector")
}

func TestDataBranchSourceColToTargetIdxRejectsHistoricalTypeDrift(t *testing.T) {
	sourceDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
	}
	targetDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
	}

	_, err := dataBranchSourceColToTargetIdx(sourceDef, targetDef, []string{"a"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "historical data branch column a has a different type")
}

func TestDataBranchSourceColToTargetIdxRejectsHistoricalPrimaryKeyDrift(t *testing.T) {
	cols := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	sourceDef := &plan.TableDef{
		Cols: cols,
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "b", Names: []string{"b"}},
	}
	targetDef := &plan.TableDef{
		Cols: cols,
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
	}

	_, err := dataBranchSourceColToTargetIdx(sourceDef, targetDef, []string{"a", "b"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "historical data branch primary key is incompatible")
}

func TestDataBranchSourceColToTargetIdxRejectsHistoricalFakePrimaryKeyDrift(t *testing.T) {
	fakePK := &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}
	sourceDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.FakePrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: fakePK,
	}
	targetDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.FakePrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: fakePK,
	}

	_, err := dataBranchSourceColToTargetIdx(
		sourceDef, targetDef, []string{"a", "b", catalog.FakePrimaryKeyColName},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "historical data branch fake primary key schema differs")
}

func TestDataBranchNeedsHistoricalProjection(t *testing.T) {
	require.False(t, dataBranchNeedsHistoricalProjection([]int{0, 1}, 2),
		"a lineage ancestor with the endpoint layout must keep its raw change rows")
	require.True(t, dataBranchNeedsHistoricalProjection([]int{0, 2}, 3),
		"a target-only column requires projection and endpoint hydration")
	require.True(t, dataBranchNeedsHistoricalProjection([]int{1, 0}, 2),
		"a reordered layout requires projection")
}

func TestOverlayDataBranchProbeResultHydratesTargetDefaults(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	projected := batch.NewWithSize(5)
	projected.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	projected.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	projected.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	projected.Vecs[3] = vector.NewConstNull(types.T_int64.ToType(), 1, mp)
	projected.Vecs[4] = vector.NewVec(types.T_TS.ToType())
	rowID := buildHashDiffRowID(t, 1)
	require.NoError(t, vector.AppendFixed(projected.Vecs[0], rowID, false, mp))
	require.NoError(t, vector.AppendFixed(projected.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(projected.Vecs[2], int64(11), false, mp))
	commitTS := types.BuildTS(20, 1)
	require.NoError(t, vector.AppendFixed(projected.Vecs[4], commitTS, false, mp))
	projected.SetRowCount(1)
	defer projected.Clean(mp)

	probeBatch := batch.NewWithSize(4)
	probeBatch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	probeBatch.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	probeBatch.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	probeBatch.Vecs[3] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[0], int64(0), false, mp))
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[2], int64(11), false, mp))
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[3], int64(0), false, mp))
	probeBatch.SetRowCount(1)
	probe := executor.Result{Mp: mp, Batches: []*batch.Batch{probeBatch}}
	defer probe.Close()

	require.NoError(t, overlayDataBranchProbeResult(projected, probe, 0, mp))
	require.Equal(t, []int64{0}, vector.MustFixedColWithTypeCheck[int64](projected.Vecs[3]))
	require.Equal(t, []types.TS{commitTS}, vector.MustFixedColWithTypeCheck[types.TS](projected.Vecs[4]))
}

func TestProjectBaseBatchToTargetMovesHiddenKeyColumns(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"a", "b", "d", catalog.CPrimaryKeyColName}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.baseColToTarIdx = []int{0, 1, 3}
	tblStuff.def.tarOnlyIdxes = []int{2}
	tblStuff.def.pkColIdx = 3
	tblStuff.def.pkColIdxes = []int{0, 1}

	mp := ses.proc.Mp()
	baseBat := batch.NewWithSize(4)
	baseBat.SetAttributes([]string{catalog.Row_ID, "a", "b", catalog.CPrimaryKeyColName})
	baseBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	baseBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	baseBat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	baseBat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())

	uid, err := types.BuildUuid()
	require.NoError(t, err)
	blkID := objectio.NewBlockid(&uid, 0, 1)
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[0], types.NewRowid(blkID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(baseBat.Vecs[2], int64(2), false, mp))
	require.NoError(t, vector.AppendBytes(baseBat.Vecs[3], []byte("cpk-1-2"), false, mp))
	baseBat.SetRowCount(1)

	cpkVec := baseBat.Vecs[3]
	projected := projectBaseBatchToTarget(baseBat, &tblStuff, mp)
	defer projected.Clean(mp)

	require.Equal(t, 5, projected.VectorCount())
	require.Same(t, cpkVec, projected.Vecs[4])
	require.False(t, projected.Vecs[4].IsConstNull())
	require.Equal(t, "cpk-1-2", string(projected.Vecs[4].GetBytesAt(0)))
	require.True(t, projected.Vecs[3].IsConstNull())
}

func TestCompareRowInWrappedBatches_WithCommonIdxes(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)

	// Set up: target has [id(0), extra(1), name(2)]
	// commonIdxes = [0, 2] (only id and name are common, extra is target-only)
	tblStuff.def.colNames = []string{"id", "extra", "name"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.pkKind = normalKind
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.commonIdxes = []int{0, 2} // only compare id and name
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0}

	mp := ses.proc.Mp()

	// Batch1: [id=1, extra=999, name="same"]
	bat1 := batch.NewWithSize(3)
	bat1.SetAttributes([]string{"id", "extra", "name"})
	bat1.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat1.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat1.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat1.Vecs[1], int64(999), false, mp))
	require.NoError(t, vector.AppendBytes(bat1.Vecs[2], []byte("same"), false, mp))
	bat1.SetRowCount(1)

	// Batch2: [id=1, extra=0, name="same"] (extra differs but is target-only)
	bat2 := batch.NewWithSize(3)
	bat2.SetAttributes([]string{"id", "extra", "name"})
	bat2.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat2.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat2.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat2.Vecs[1], int64(0), false, mp))
	require.NoError(t, vector.AppendBytes(bat2.Vecs[2], []byte("same"), false, mp))
	bat2.SetRowCount(1)

	wrapped1 := batchWithKind{kind: diffInsert, batch: bat1}
	wrapped2 := batchWithKind{kind: diffInsert, batch: bat2}

	// Rows should be considered equal because extra (index 1) is NOT in commonIdxes
	cmp, err := compareRowInWrappedBatches(
		context.Background(),
		ses,
		tblStuff,
		0, 0,
		true, // skipPKCols
		wrapped1,
		wrapped2,
	)
	require.NoError(t, err)
	require.Zero(t, cmp, "rows should be considered equal when only non-common columns differ")

	// Now test with a difference in a common column (name)
	bat3 := batch.NewWithSize(3)
	bat3.SetAttributes([]string{"id", "extra", "name"})
	bat3.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat3.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat3.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat3.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat3.Vecs[1], int64(999), false, mp))
	require.NoError(t, vector.AppendBytes(bat3.Vecs[2], []byte("different"), false, mp))
	bat3.SetRowCount(1)

	wrapped3 := batchWithKind{kind: diffInsert, batch: bat3}

	// Rows should differ because name differs and name IS in commonIdxes
	cmp, err = compareRowInWrappedBatches(
		context.Background(),
		ses,
		tblStuff,
		0, 0,
		false, // do NOT skip PK
		wrapped1,
		wrapped3,
	)
	require.NoError(t, err)
	require.NotZero(t, cmp, "rows should differ when a common column differs")

	bat1.Clean(mp)
	bat2.Clean(mp)
	bat3.Clean(mp)
}
