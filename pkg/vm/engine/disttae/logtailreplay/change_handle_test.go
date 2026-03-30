// Copyright 2024 Matrix Origin
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

package logtailreplay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

func TestBatchHandleNext_ReturnsEOBOnSchemaMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(42), false, mp))
	src.SetAttributes([]string{"value"})
	src.SetRowCount(1)

	dst := batch.NewWithSize(1)
	dst.Vecs[0] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(dst.Vecs[0], types.BuildTS(1, 0), false, mp))
	dst.SetAttributes([]string{"value"})
	dst.SetRowCount(1)
	defer dst.Clean(mp)

	handle := NewRowHandle(src, mp, &baseHandle{changesHandle: &ChangeHandler{}}, context.Background(), false)
	defer handle.Close()

	err := handle.Next(&dst, mp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	require.Equal(t, 0, handle.rowOffsetCursor)
}

func TestAObjectHandleNext_ReturnsEOBOnSchemaMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	current := batch.NewWithSize(1)
	current.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(current.Vecs[0], int64(7), false, mp))
	current.SetAttributes([]string{"value"})
	current.SetRowCount(1)

	dst := batch.NewWithSize(1)
	dst.Vecs[0] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(dst.Vecs[0], types.BuildTS(2, 0), false, mp))
	dst.SetAttributes([]string{"value"})
	dst.SetRowCount(1)
	defer dst.Clean(mp)

	handle := &AObjectHandle{
		currentBatch: current,
		batchLength:  1,
		mp:           mp,
		p:            &baseHandle{changesHandle: &ChangeHandler{}},
	}
	defer current.Clean(mp)

	err := handle.Next(context.Background(), &dst, mp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	require.Equal(t, 0, handle.rowOffsetCursor)
	require.NotNil(t, handle.currentBatch)
}

func TestUpdateCNTombstoneBatch_IsIdempotent(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	rowid := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowid, types.Rowid{}, false, mp))
	bat.Vecs[0] = rowid

	pk := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp))
	bat.Vecs[1] = pk

	commitTS := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(commitTS, types.BuildTS(10, 0), false, mp))
	bat.Vecs[2] = commitTS
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	updateCNTombstoneBatch(bat, types.BuildTS(20, 0), mp)
	updateCNTombstoneBatch(bat, types.BuildTS(30, 0), mp)

	require.Equal(t, 2, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
}

func TestUpdateCNDataBatch_RemoveTSVector(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch with multiple vectors including T_TS type
	bat := batch.NewWithSize(3)

	// Create an int64 vector (non-TS)
	vec1 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec1, int64(1), false, mp)
	vector.AppendFixed(vec1, int64(2), false, mp)
	bat.Vecs[0] = vec1

	// Create a T_TS vector (should be removed)
	tsVec1 := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec1, types.BuildTS(1, 0), false, mp)
	vector.AppendFixed(tsVec1, types.BuildTS(2, 0), false, mp)
	bat.Vecs[1] = tsVec1

	// Create another int64 vector (non-TS)
	vec2 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec2, int64(3), false, mp)
	vector.AppendFixed(vec2, int64(4), false, mp)
	bat.Vecs[2] = vec2

	bat.SetRowCount(2)

	// Verify initial state: should have 3 vectors
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[2].GetType().Oid)

	// Call updateCNDataBatch
	newCommitTS := types.BuildTS(100, 0)
	updateCNDataBatch(bat, newCommitTS, mp)

	// Verify T_TS vector is removed and new commitTS vector is added at the end
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[2].GetType().Oid)

	// Verify the new commitTS vector has the correct value
	require.True(t, bat.Vecs[2].IsConst())
	tsVal := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[2])[0]
	require.Equal(t, newCommitTS, tsVal)

	bat.Clean(mp)
}

func TestUpdateCNDataBatch_NoTSVector(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch without T_TS vectors
	bat := batch.NewWithSize(2)

	vec1 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec1, int64(1), false, mp)
	vector.AppendFixed(vec1, int64(2), false, mp)
	bat.Vecs[0] = vec1

	vec2 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec2, int64(3), false, mp)
	vector.AppendFixed(vec2, int64(4), false, mp)
	bat.Vecs[1] = vec2

	bat.SetRowCount(2)

	// Verify initial state: should have 2 vectors
	require.Equal(t, 2, len(bat.Vecs))

	// Call updateCNDataBatch
	newCommitTS := types.BuildTS(100, 0)
	updateCNDataBatch(bat, newCommitTS, mp)

	// Verify commitTS vector is added at the end
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[2].GetType().Oid)

	bat.Clean(mp)
}

func TestUpdateDataBatch_PreservesTrailingColumnsWithoutRowid(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{"id", "created_at", "updated_at", objectio.DefaultCommitTS_Attr})

	idVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(idVec, []byte("row-1"), false, mp))
	bat.Vecs[0] = idVec

	createdAt := vector.NewVec(types.New(types.T_datetime, 0, 6))
	createdAtVal, err := types.ParseDatetime("2026-03-12 19:18:00.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(createdAt, createdAtVal, false, mp))
	bat.Vecs[1] = createdAt

	updatedAt := vector.NewVec(types.New(types.T_datetime, 0, 6))
	updatedAtVal, err := types.ParseDatetime("2026-03-12 19:19:00.654321", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(updatedAt, updatedAtVal, false, mp))
	bat.Vecs[2] = updatedAt

	commitTS := vector.NewVec(types.T_TS.ToType())
	tsVal := types.BuildTS(100, 0)
	require.NoError(t, vector.AppendFixed(commitTS, tsVal, false, mp))
	bat.Vecs[3] = commitTS
	bat.SetRowCount(1)

	updateDataBatch(bat, types.BuildTS(50, 0), types.BuildTS(150, 0), mp)

	require.Equal(t, 4, len(bat.Vecs))
	require.Equal(t, []string{"id", "created_at", "updated_at", objectio.DefaultCommitTS_Attr}, bat.Attrs)
	require.Equal(t, types.T_varchar, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[2].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[3].GetType().Oid)
	require.Equal(t, updatedAtVal, vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[2])[0])

	bat.Clean(mp)
}

func TestAObjectHandleShouldReadBlock_UsesCachedPlan(t *testing.T) {
	obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(10, 0))
	handle := &AObjectHandle{
		start: types.BuildTS(5, 0),
		end:   types.BuildTS(15, 0),
		p: &baseHandle{changesHandle: &ChangeHandler{
			enableCommitTSBlockPrune: true,
		}},
		blockPlans: map[string]*aobjBlockPlan{
			obj.ObjectShortName().ShortString(): {
				initialized:      true,
				evaluable:        true,
				shouldReadByBlks: []bool{false, true},
			},
		},
	}

	ok, err := handle.shouldReadBlock(context.Background(), obj, 0)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = handle.shouldReadBlock(context.Background(), obj, 1)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestAObjectHandleShouldReadBlock_NonEvaluablePlanFallback(t *testing.T) {
	obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(10, 0))
	key := obj.ObjectShortName().ShortString()

	t.Run("non strict keeps block readable", func(t *testing.T) {
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{
				enableCommitTSBlockPrune: true,
			}},
			blockPlans: map[string]*aobjBlockPlan{
				key: {
					initialized:      true,
					evaluable:        false,
					shouldReadByBlks: []bool{true},
				},
			},
		}

		ok, err := handle.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("strict non evaluable returns file not found", func(t *testing.T) {
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{
				enableCommitTSBlockPrune: true,
				strictCommitTSBlockPrune: true,
			}},
			blockPlans: map[string]*aobjBlockPlan{
				key: {
					initialized:      true,
					evaluable:        false,
					shouldReadByBlks: []bool{true},
				},
			},
		}

		ok, err := handle.shouldReadBlock(context.Background(), obj, 0)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	})
}

func TestAObjectHandleShouldReadBlock_ShortCircuits(t *testing.T) {
	t.Run("nil object", func(t *testing.T) {
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{enableCommitTSBlockPrune: true}},
		}
		ok, err := handle.shouldReadBlock(context.Background(), nil, 0)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("prune disabled", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(10, 0))
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{enableCommitTSBlockPrune: false}},
		}
		ok, err := handle.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("appendable and cn-created bypass block plan", func(t *testing.T) {
		appendable := makeTestObjectEntry(t, 1, true, false, types.BuildTS(10, 0))
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{enableCommitTSBlockPrune: true}},
		}
		ok, err := handle.shouldReadBlock(context.Background(), appendable, 0)
		require.NoError(t, err)
		require.True(t, ok)

		cnCreated := makeTestObjectEntry(t, 1, false, true, types.BuildTS(10, 0))
		ok, err = handle.shouldReadBlock(context.Background(), cnCreated, 0)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("block index out of range", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(10, 0))
		handle := &AObjectHandle{
			p: &baseHandle{changesHandle: &ChangeHandler{enableCommitTSBlockPrune: true}},
			blockPlans: map[string]*aobjBlockPlan{
				obj.ObjectShortName().ShortString(): {
					initialized:      true,
					evaluable:        true,
					shouldReadByBlks: []bool{true},
				},
			},
		}
		ok, err := handle.shouldReadBlock(context.Background(), obj, 1)
		require.NoError(t, err)
		require.False(t, ok)
	})
}

func TestAObjectHandleNextPrefetchTarget_SkipsPrunedBlocks(t *testing.T) {
	obj := makeTestObjectEntry(t, 3, false, false, types.BuildTS(10, 0))
	handle := &AObjectHandle{
		objects: []*objectio.ObjectEntry{obj},
		p: &baseHandle{changesHandle: &ChangeHandler{
			enableCommitTSBlockPrune: true,
		}},
		blockPlans: map[string]*aobjBlockPlan{
			obj.ObjectShortName().ShortString(): {
				initialized:      true,
				evaluable:        true,
				shouldReadByBlks: []bool{false, false, true},
			},
		},
	}

	gotObj, blk, ok, err := handle.nextPrefetchTarget(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, obj, gotObj)
	require.Equal(t, uint16(2), blk)
	require.Equal(t, 1, handle.objectOffsetCursor)
	require.Equal(t, 0, handle.blkOffsetCursor)
}

func TestAObjectHandleGetNextAObject_UsesCachedBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	makeDataBatch := func(value int64, ts types.TS) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Attrs = []string{"value", objectio.DefaultCommitTS_Attr}
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], ts, false, mp))
		bat.SetRowCount(1)
		return bat
	}

	outOfRangeBat := makeDataBatch(1, types.BuildTS(2, 0))
	defer outOfRangeBat.Clean(mp)
	inRangeBat := makeDataBatch(2, types.BuildTS(10, 0))
	defer inRangeBat.Clean(mp)

	handle := &AObjectHandle{
		start: types.BuildTS(5, 0),
		end:   types.BuildTS(15, 0),
		mp:    mp,
		cache: []*batch.Batch{outOfRangeBat, inRangeBat},
		p:     &baseHandle{changesHandle: &ChangeHandler{}},
	}

	err := handle.getNextAObject(context.Background())
	require.NoError(t, err)
	require.Same(t, inRangeBat, handle.currentBatch)
	require.Equal(t, 1, handle.batchLength)
	require.Len(t, handle.cache, 0)
}

func TestBaseHandleFillInSkipTSFromObjects(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)
	inRange := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
	inRange.DeleteTime = types.BuildTS(12, 0)
	outOfRange := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
	outOfRange.DeleteTime = types.BuildTS(30, 0)
	missingDeleteTS := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))

	handle := &baseHandle{skipTS: make(map[types.TS]struct{})}
	handle.fillInSkipTSFromObjects(start, end, []*objectio.ObjectEntry{inRange, outOfRange, missingDeleteTS})

	require.Contains(t, handle.skipTS, inRange.DeleteTime)
	require.NotContains(t, handle.skipTS, outOfRange.DeleteTime)
	require.Len(t, handle.skipTS, 1)
}

func TestBaseHandleGetObjectEntries_ClassifiesAndSorts(t *testing.T) {
	tree := btree.NewBTreeGOptions(objectio.ObjectEntry.ObjectNameIndexLess, btree.Options{Degree: 8})

	appendable := makeTestObjectEntry(t, 1, true, false, types.BuildTS(6, 0))
	appendableDeletedBeforeStart := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
	appendableDeletedBeforeStart.DeleteTime = types.BuildTS(4, 0)
	cnCreated := makeTestObjectEntry(t, 1, false, true, types.BuildTS(8, 0))
	cnCreatedBeforeStart := makeTestObjectEntry(t, 1, false, true, types.BuildTS(2, 0))
	tnLate := makeTestObjectEntry(t, 1, false, false, types.BuildTS(30, 0))
	tnCreate4 := makeTestObjectEntry(t, 1, false, false, types.BuildTS(4, 0))
	tnCreate9 := makeTestObjectEntry(t, 1, false, false, types.BuildTS(9, 0))

	for _, obj := range []objectio.ObjectEntry{
		*tnLate,
		*appendableDeletedBeforeStart,
		*cnCreatedBeforeStart,
		*tnCreate9,
		*appendable,
		*tnCreate4,
		*cnCreated,
	} {
		tree.Set(obj)
	}

	handle := &baseHandle{}
	aobj, cnObj, tnByCreateTS, tnKeys := handle.getObjectEntries(
		tree.Iter(),
		types.BuildTS(5, 0),
		types.BuildTS(20, 0),
	)

	require.Len(t, aobj, 1)
	require.Equal(t, appendable.ObjectShortName().ShortString(), aobj[0].ObjectShortName().ShortString())
	require.Len(t, cnObj, 1)
	require.Equal(t, cnCreated.ObjectShortName().ShortString(), cnObj[0].ObjectShortName().ShortString())
	require.Equal(t, []types.TS{types.BuildTS(4, 0), types.BuildTS(9, 0)}, tnKeys)
	require.Len(t, tnByCreateTS[types.BuildTS(4, 0)], 1)
	require.Len(t, tnByCreateTS[types.BuildTS(9, 0)], 1)
}

func TestBaseHandleNextTS_SelectsEarliestHandle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	makeBatchHandle := func(ts types.TS) *BatchHandle {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], ts, false, mp))
		bat.SetRowCount(1)
		return &BatchHandle{batches: bat, batchLength: 1}
	}

	makeAObjectHandle := func(ts types.TS) *AObjectHandle {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], ts, false, mp))
		bat.SetRowCount(1)
		return &AObjectHandle{currentBatch: bat, batchLength: 1}
	}

	p := &baseHandle{
		inMemoryHandle: makeBatchHandle(types.BuildTS(8, 0)),
		aobjHandle:     makeAObjectHandle(types.BuildTS(10, 0)),
		cnObjectHandle: &CNObjectHandle{objects: []*objectio.ObjectEntry{{CreateTime: types.BuildTS(12, 0)}}},
	}

	ts, kind := p.nextTS()
	require.Equal(t, types.BuildTS(8, 0), ts)
	require.Equal(t, NextChangeHandle_InMemory, kind)

	p.inMemoryHandle.rowOffsetCursor = 1
	ts, kind = p.nextTS()
	require.Equal(t, types.BuildTS(10, 0), ts)
	require.Equal(t, NextChangeHandle_AObj, kind)

	p.aobjHandle.rowOffsetCursor = 1
	p.aobjHandle.batchLength = 1
	ts, kind = p.nextTS()
	require.Equal(t, types.BuildTS(12, 0), ts)
	require.Equal(t, NextChangeHandle_CNObj, kind)

	p.aobjHandle.currentBatch.Clean(mp)
	p.inMemoryHandle.batches.Clean(mp)
}

func makeTestObjectEntry(
	t *testing.T,
	blkCnt int,
	appendable bool,
	cnCreated bool,
	createTS types.TS,
) *objectio.ObjectEntry {
	t.Helper()

	oid := types.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&oid, appendable, false, cnCreated)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, uint32(blkCnt)))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, uint32(blkCnt)))

	return &objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  createTS,
	}
}
