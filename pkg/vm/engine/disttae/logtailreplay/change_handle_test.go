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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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

func TestBatchesShareAppendSchema(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	t.Run("both nil", func(t *testing.T) {
		require.True(t, batchesShareAppendSchema(nil, nil))
	})
	t.Run("one nil", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.True(t, batchesShareAppendSchema(bat, nil))
		require.True(t, batchesShareAppendSchema(nil, bat))
	})
	t.Run("different vec count", func(t *testing.T) {
		a := batch.NewWithSize(1)
		a.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		b := batch.NewWithSize(2)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.False(t, batchesShareAppendSchema(a, b))
	})
	t.Run("same schema", func(t *testing.T) {
		a := batch.NewWithSize(2)
		a.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		a.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		b := batch.NewWithSize(2)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.True(t, batchesShareAppendSchema(a, b))
	})
	t.Run("different types", func(t *testing.T) {
		a := batch.NewWithSize(1)
		a.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.False(t, batchesShareAppendSchema(a, b))
	})
	t.Run("nil vec match", func(t *testing.T) {
		a := batch.NewWithSize(2)
		a.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		a.Vecs[1] = nil
		b := batch.NewWithSize(2)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		b.Vecs[1] = nil
		require.True(t, batchesShareAppendSchema(a, b))
	})
	t.Run("nil vs non-nil vec", func(t *testing.T) {
		a := batch.NewWithSize(1)
		a.Vecs[0] = nil
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.False(t, batchesShareAppendSchema(a, b))
	})
}

func TestBlockCommitTSOverlapsRange(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	t.Run("no meta columns", func(t *testing.T) {
		blk := objectio.BuildBlockMeta(0)
		overlap, evaluable, reason, _ := blockCommitTSOverlapsRange(blk, start, end)
		require.False(t, overlap)
		require.False(t, evaluable)
		require.Equal(t, "no_meta_columns", reason)
	})

	t.Run("tail column not TS type", func(t *testing.T) {
		blk := objectio.BuildBlockMeta(1)
		hdr := objectio.BuildBlockHeader()
		hdr.SetMetaColumnCount(1)
		blk.SetBlockMetaHeader(hdr)
		overlap, evaluable, reason, _ := blockCommitTSOverlapsRange(blk, start, end)
		require.False(t, overlap)
		require.False(t, evaluable)
		require.Equal(t, "tail_column_not_ts", reason)
	})
}

func TestCalcPruneRate(t *testing.T) {
	require.Equal(t, 0.0, calcPruneRate(0, 0))
	require.Equal(t, 0.0, calcPruneRate(0, -1))
	require.Equal(t, 0.5, calcPruneRate(5, 10))
	require.Equal(t, 1.0, calcPruneRate(10, 10))
}

func TestLookupDeleteChainSuccessor(t *testing.T) {
	ts5 := types.BuildTS(5, 0)
	ts10 := types.BuildTS(10, 0)
	ts15 := types.BuildTS(15, 0)
	ts20 := types.BuildTS(20, 0)

	obj10 := makeTestObjectEntry(t, 1, false, false, ts10)
	obj15 := makeTestObjectEntry(t, 1, false, false, ts15)

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		ts10: {obj10},
		ts15: {obj15},
	}
	tnKeys := []types.TS{ts10, ts15}

	t.Run("exact match", func(t *testing.T) {
		next, ts, exact := lookupDeleteChainSuccessor(ts10, tnByCreateTS, tnKeys)
		require.Len(t, next, 1)
		require.Equal(t, ts10, ts)
		require.True(t, exact)
	})
	t.Run("fuzzy match", func(t *testing.T) {
		next, ts, exact := lookupDeleteChainSuccessor(types.BuildTS(12, 0), tnByCreateTS, tnKeys)
		require.Len(t, next, 1)
		require.Equal(t, ts15, ts)
		require.False(t, exact)
	})
	t.Run("no match past last key", func(t *testing.T) {
		next, _, exact := lookupDeleteChainSuccessor(ts20, tnByCreateTS, tnKeys)
		require.Nil(t, next)
		require.False(t, exact)
	})
	t.Run("empty keys", func(t *testing.T) {
		next, _, exact := lookupDeleteChainSuccessor(ts5, nil, nil)
		require.Nil(t, next)
		require.False(t, exact)
	})
	t.Run("match before first key", func(t *testing.T) {
		next, ts, exact := lookupDeleteChainSuccessor(ts5, tnByCreateTS, tnKeys)
		require.Len(t, next, 1)
		require.Equal(t, ts10, ts)
		require.False(t, exact)
	})
}

func TestClassifyResolvedObjects(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		aobjs, cnObjs := classifyResolvedObjects()
		require.Empty(t, aobjs)
		require.Empty(t, cnObjs)
	})

	t.Run("classifies and deduplicates", func(t *testing.T) {
		cn1 := makeTestObjectEntry(t, 1, false, true, types.BuildTS(10, 0))
		cn2 := makeTestObjectEntry(t, 1, false, true, types.BuildTS(5, 0))
		tn1 := makeTestObjectEntry(t, 1, false, false, types.BuildTS(8, 0))
		tn2 := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))

		aobjs, cnObjs := classifyResolvedObjects(
			[]*objectio.ObjectEntry{cn1, tn1},
			[]*objectio.ObjectEntry{cn2, tn2},
		)
		require.Len(t, cnObjs, 2)
		require.Len(t, aobjs, 2)
		// sorted by create time
		require.True(t, aobjs[0].CreateTime.LT(&aobjs[1].CreateTime))
		require.True(t, cnObjs[0].CreateTime.LT(&cnObjs[1].CreateTime))
	})

	t.Run("skips nil entries", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(1, 0))
		aobjs, cnObjs := classifyResolvedObjects([]*objectio.ObjectEntry{nil, obj, nil})
		require.Len(t, aobjs, 1)
		require.Empty(t, cnObjs)
	})

	t.Run("deduplicates same object", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, true, types.BuildTS(5, 0))
		aobjs, cnObjs := classifyResolvedObjects(
			[]*objectio.ObjectEntry{obj},
			[]*objectio.ObjectEntry{obj},
		)
		require.Len(t, cnObjs, 1)
		require.Empty(t, aobjs)
	})
}

func TestLogRangeReplaySelection_DoesNotPanic(t *testing.T) {
	// logRangeReplaySelection is a logging-only function.
	// This test ensures it does not panic on various inputs including nils.
	start := types.BuildTS(1, 0)
	end := types.BuildTS(10, 0)
	obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	logRangeReplaySelection(start, end, nil, nil, nil, nil)
	logRangeReplaySelection(start, end, []*objectio.ObjectEntry{obj}, nil, nil, nil)
	logRangeReplaySelection(start, end, nil, nil, []*objectio.ObjectEntry{nil, obj}, nil)
}

func TestResolveVisibleObjectsByDeleteChain_EmptyInputs(t *testing.T) {
	base := &baseHandle{
		changesHandle: &ChangeHandler{enableDeleteChainResolve: true},
	}
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil,
		nil,
		nil,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Empty(t, resolved)
}

func TestResolveVisibleObjectsByDeleteChain_VisibleObjectExists(t *testing.T) {
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		obj.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{obj},
		nil,
		nil,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestResolveVisibleObjectsByDeleteChain_MissingNoDeleteTime(t *testing.T) {
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{obj},
		nil,
		nil,
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestResolveVisibleObjectsByDeleteChain_FollowsChain(t *testing.T) {
	missing := makeTestObjectEntry(t, 1, true, false, types.BuildTS(3, 0))
	missing.DeleteTime = types.BuildTS(5, 0)
	successor := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		successor.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		successor.CreateTime: {successor},
	}
	tnKeys := []types.TS{successor.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{missing},
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, successor.ObjectShortName().ShortString(), resolved[0].ObjectShortName().ShortString())
}

func TestResolveVisibleObjectsByDeleteChain_NonAppendableDeletedBeforeEnd(t *testing.T) {
	// A non-appendable object that is deleted before end should follow chain
	deleted := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	deleted.DeleteTime = types.BuildTS(7, 0)
	replacement := makeTestObjectEntry(t, 1, false, false, types.BuildTS(7, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		replacement.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		replacement.CreateTime: {replacement},
	}
	tnKeys := []types.TS{replacement.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestResolveVisibleObjectsByDeleteChain_AppendableDeletedBeforeEnd(t *testing.T) {
	// An appendable object that is deleted before end should also follow chain
	// so replay keeps only the terminal successor visible at range end.
	deleted := makeTestObjectEntry(t, 1, true, false, types.BuildTS(3, 0))
	deleted.DeleteTime = types.BuildTS(7, 0)
	successor := makeTestObjectEntry(t, 1, false, false, types.BuildTS(7, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		deleted.ObjectName().String():   {},
		successor.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		successor.CreateTime: {successor},
	}
	tnKeys := []types.TS{successor.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, successor.ObjectShortName().ShortString(), resolved[0].ObjectShortName().ShortString())
}

func TestResolveVisibleObjectsByDeleteChain_OrphanSweep(t *testing.T) {
	// Orphan TN object whose appendable predecessor was GC'd
	orphan := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		orphan.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		orphan.CreateTime: {orphan},
	}
	tnKeys := []types.TS{orphan.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil, // no visible objects -> orphan sweep should find the TN object
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, orphan.ObjectShortName().ShortString(), resolved[0].ObjectShortName().ShortString())
}

func TestResolveVisibleObjectsByDeleteChain_OrphanDeletedBeforeEnd(t *testing.T) {
	// Orphan TN object that is already deleted before end should be skipped
	orphan := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
	orphan.DeleteTime = types.BuildTS(8, 0)

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		orphan.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		orphan.CreateTime: {orphan},
	}
	tnKeys := []types.TS{orphan.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil,
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Empty(t, resolved)
}

func TestResolveVisibleObjectsByDeleteChain_NonAppendableNoSuccessor(t *testing.T) {
	// Non-appendable object deleted before end, but no successor in TN map -> error
	deleted := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	deleted.DeleteTime = types.BuildTS(7, 0)

	stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		nil, nil, // empty TN map → no successor
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestResolveVisibleObjectsByDeleteChain_FuzzyHop(t *testing.T) {
	// Non-appendable deleted before end with fuzzy (non-exact) successor match
	deleted := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	deleted.DeleteTime = types.BuildTS(7, 0)
	// Successor created at ts=9, but delete-time is 7 -> fuzzy match
	successor := makeTestObjectEntry(t, 1, false, false, types.BuildTS(9, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		successor.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		successor.CreateTime: {successor},
	}
	tnKeys := []types.TS{successor.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestResolveVisibleObjectsByDeleteChain_MissingWithDeleteTimeNoReplacement(t *testing.T) {
	// Missing file with delete-time but no replacement in TN map
	missing := makeTestObjectEntry(t, 1, true, false, types.BuildTS(3, 0))
	missing.DeleteTime = types.BuildTS(7, 0)

	stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{missing},
		nil, nil, // no TN entries
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestResolveVisibleObjectsByDeleteChain_MissingWithDeleteTimeFuzzyReplacement(t *testing.T) {
	// Missing file with delete-time, replaced by fuzzy-matched TN object
	missing := makeTestObjectEntry(t, 1, true, false, types.BuildTS(2, 0))
	missing.DeleteTime = types.BuildTS(5, 0)
	// Successor at ts=8, but delete-time is 5 -> fuzzy
	replacement := makeTestObjectEntry(t, 1, false, false, types.BuildTS(8, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		replacement.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	tnByCreateTS := map[types.TS][]*objectio.ObjectEntry{
		replacement.CreateTime: {replacement},
	}
	tnKeys := []types.TS{replacement.CreateTime}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{missing},
		tnByCreateTS,
		tnKeys,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestResolveVisibleObjectsByDeleteChain_ObjectFileExistsError(t *testing.T) {
	// objectFileExists returns a non-FileNotFound error
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{otherErr: moerr.NewInternalErrorNoCtx("disk error")}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{obj},
		nil, nil,
		false,
		"data",
	)
	require.Error(t, err)
}

func TestResolveVisibleObjectsByDeleteChain_SkipVisitedObjects(t *testing.T) {
	// Same object appears twice in visible list - should be deduped
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))

	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		obj.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{obj, obj},
		nil, nil,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestResolveVisibleObjectsByDeleteChain_NilInQueue(t *testing.T) {
	// nil entries in visible list should be skipped
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		obj.ObjectName().String(): {},
	}}
	base := &baseHandle{
		changesHandle: &ChangeHandler{
			enableDeleteChainResolve: true,
			fs:                       stubFS,
		},
	}

	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{nil, obj, nil},
		nil, nil,
		false,
		"data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
}

func TestObjectFileExists(t *testing.T) {
	t.Run("nil object", func(t *testing.T) {
		base := &baseHandle{changesHandle: &ChangeHandler{}}
		exists, err := base.objectFileExists(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("file exists", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		stubFS := &stubStatFileFS{existing: map[string]struct{}{
			obj.ObjectName().String(): {},
		}}
		base := &baseHandle{changesHandle: &ChangeHandler{fs: stubFS}}
		exists, err := base.objectFileExists(context.Background(), obj)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("file not found", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
		base := &baseHandle{changesHandle: &ChangeHandler{fs: stubFS}}
		exists, err := base.objectFileExists(context.Background(), obj)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("other error", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		stubFS := &stubStatFileFS{otherErr: moerr.NewInternalErrorNoCtx("disk error")}
		base := &baseHandle{changesHandle: &ChangeHandler{fs: stubFS}}
		exists, err := base.objectFileExists(context.Background(), obj)
		require.Error(t, err)
		require.False(t, exists)
	})
}

func TestClassifyCheckpointObject(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	t.Run("range_appendable_in_range", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindRowCommitTS, kind)
	})

	t.Run("range_appendable_after_end", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(25, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})

	t.Run("range_appendable_deleted_before_start", func(t *testing.T) {
		entry := makeTestObjectEntry(t, 1, true, false, types.BuildTS(1, 0))
		entry.DeleteTime = types.BuildTS(5, 0)
		kind := classifyCheckpointObject(*entry, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})

	t.Run("range_cn_in_range", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, true, types.BuildTS(15, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindConstantCommitTS, kind)
	})

	t.Run("range_cn_before_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, true, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})

	t.Run("range_tn_in_range", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(15, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindRowCommitTS, kind)
	})

	t.Run("range_tn_before_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})

	t.Run("recovery_appendable_ge_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(15, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRecovery)
		require.Equal(t, checkpointObjectKindRowCommitTS, kind)
	})

	t.Run("recovery_appendable_before_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRecovery)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})

	t.Run("recovery_cn_ge_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, true, types.BuildTS(10, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRecovery)
		require.Equal(t, checkpointObjectKindConstantCommitTS, kind)
	})

	t.Run("recovery_non_appendable_before_start", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRecovery)
		require.Equal(t, checkpointObjectKindIgnore, kind)
	})
}

func TestCheckpointObjectMapToSlice(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result := checkpointObjectMapToSlice(map[string]*objectio.ObjectEntry{}, true)
		require.Empty(t, result)
	})

	t.Run("sorted", func(t *testing.T) {
		a := makeTestObjectEntry(t, 1, false, false, types.BuildTS(10, 0))
		b := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		entries := map[string]*objectio.ObjectEntry{
			"a": a,
			"b": b,
		}
		result := checkpointObjectMapToSlice(entries, true)
		require.Len(t, result, 2)
		require.True(t, result[0].CreateTime.LT(&result[1].CreateTime))
	})

	t.Run("unsorted", func(t *testing.T) {
		a := makeTestObjectEntry(t, 1, false, false, types.BuildTS(10, 0))
		entries := map[string]*objectio.ObjectEntry{"a": a}
		result := checkpointObjectMapToSlice(entries, false)
		require.Len(t, result, 1)
	})
}

func TestBaseHandleClose(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	t.Run("nil handle", func(t *testing.T) {
		var p *baseHandle
		p.Close() // should not panic
	})

	t.Run("non nil with inMemoryHandle", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
		p := &baseHandle{
			inMemoryHandle: &BatchHandle{batches: bat},
		}
		p.Close() // should not panic
	})
}

func TestBaseHandleLess(t *testing.T) {
	p := &baseHandle{}

	t.Run("a empty", func(t *testing.T) {
		require.False(t, p.less(types.TS{}, types.BuildTS(5, 0)))
	})
	t.Run("b empty", func(t *testing.T) {
		require.True(t, p.less(types.BuildTS(5, 0), types.TS{}))
	})
	t.Run("a < b", func(t *testing.T) {
		require.True(t, p.less(types.BuildTS(3, 0), types.BuildTS(5, 0)))
	})
	t.Run("a > b", func(t *testing.T) {
		require.False(t, p.less(types.BuildTS(8, 0), types.BuildTS(5, 0)))
	})
	t.Run("a == b", func(t *testing.T) {
		require.True(t, p.less(types.BuildTS(5, 0), types.BuildTS(5, 0)))
	})
}

func TestCheckTS(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)
	require.True(t, checkTS(start, end, types.BuildTS(10, 0)))
	require.True(t, checkTS(start, end, types.BuildTS(15, 0)))
	require.True(t, checkTS(start, end, types.BuildTS(20, 0)))
	require.False(t, checkTS(start, end, types.BuildTS(9, 0)))
	require.False(t, checkTS(start, end, types.BuildTS(21, 0)))
}

func TestShouldReadBlock_BuildPlanError(t *testing.T) {
	// When buildBlockPlan fails (e.g. file not found), shouldReadBlock should
	// propagate the error. This covers the plan creation and error paths.
	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
	}
	p := &baseHandle{changesHandle: ch}
	stubFS := &stubErrorFS{}
	h := &AObjectHandle{
		p:          p,
		objects:    nil,
		blockPlans: make(map[string]*aobjBlockPlan),
		start:      types.BuildTS(10, 0),
		end:        types.BuildTS(20, 0),
		fs:         stubFS,
	}

	obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(5, 0))
	ok, err := h.shouldReadBlock(context.Background(), obj, 0)
	require.Error(t, err)
	require.False(t, ok)
	// The plan should have been created in the map
	_, exists := h.blockPlans[obj.ObjectShortName().ShortString()]
	require.True(t, exists)
}

// stubErrorFS returns errors for all read operations.
type stubErrorFS struct {
	fileservice.FileService
}

func (s *stubErrorFS) Name() string { return "stub" }
func (s *stubErrorFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	return moerr.NewFileNotFoundNoCtx(vector.FilePath)
}
func (s *stubErrorFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}
func (s *stubErrorFS) StatFile(ctx context.Context, name string) (*fileservice.DirEntry, error) {
	return nil, moerr.NewFileNotFoundNoCtx(name)
}
func (s *stubErrorFS) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}

// stubStatFileFS is a minimal fileservice stub for objectFileExists tests.
type stubStatFileFS struct {
	existing map[string]struct{}
	otherErr error
	fileservice.FileService
}

func (s *stubStatFileFS) StatFile(ctx context.Context, name string) (*fileservice.DirEntry, error) {
	if s.otherErr != nil {
		return nil, s.otherErr
	}
	if _, ok := s.existing[name]; ok {
		return &fileservice.DirEntry{}, nil
	}
	return nil, moerr.NewFileNotFoundNoCtx(name)
}

func TestFillInSkipTSFromObjects(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	t.Run("empty groups", func(t *testing.T) {
		p := &baseHandle{skipTS: make(map[types.TS]struct{})}
		p.fillInSkipTSFromObjects(start, end)
		require.Empty(t, p.skipTS)
	})

	t.Run("collects delete times in range", func(t *testing.T) {
		p := &baseHandle{skipTS: make(map[types.TS]struct{})}
		inRange := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		inRange.DeleteTime = types.BuildTS(15, 0)
		outOfRange := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
		outOfRange.DeleteTime = types.BuildTS(25, 0)
		noDelete := makeTestObjectEntry(t, 1, false, false, types.BuildTS(8, 0))

		p.fillInSkipTSFromObjects(start, end, []*objectio.ObjectEntry{inRange, outOfRange, noDelete, nil})
		require.Len(t, p.skipTS, 1)
		_, ok := p.skipTS[types.BuildTS(15, 0)]
		require.True(t, ok)
	})
}

func TestFillInSkipTS(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	tree := btree.NewBTreeGOptions[objectio.ObjectEntry](func(a, b objectio.ObjectEntry) bool {
		return a.ObjectShortName().ShortString() < b.ObjectShortName().ShortString()
	}, btree.Options{NoLocks: true})

	e1 := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
	e1.DeleteTime = types.BuildTS(15, 0)
	tree.Set(e1)

	e2 := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	e2.DeleteTime = types.BuildTS(25, 0)
	tree.Set(e2)

	e3 := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(8, 0))
	tree.Set(e3)

	p := &baseHandle{skipTS: make(map[types.TS]struct{})}
	iter := tree.Iter()
	p.fillInSkipTS(iter, start, end)
	iter.Release()

	require.Len(t, p.skipTS, 1)
	_, ok := p.skipTS[types.BuildTS(15, 0)]
	require.True(t, ok)
}

func TestNewBaseHandlerWithObjEntries(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
	ch := &ChangeHandler{
		scheduler: tasks.NewParallelJobScheduler(1),
		fs:        stubFS,
		mp:        mp,
		start:     types.BuildTS(1, 0),
		end:       types.BuildTS(10, 0),
	}
	defer ch.scheduler.Stop()

	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
	cnObj := makeTestObjectEntry(t, 1, false, true, types.BuildTS(7, 0))

	p, err := NewBaseHandlerWithObjEntries(
		context.Background(),
		ch,
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{obj},
		[]*objectio.ObjectEntry{cnObj},
		false,
		mp,
		stubFS,
	)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.False(t, p.aobjHandle.IsEmpty())
	require.False(t, p.cnObjectHandle.IsEmpty())
}

func TestBaseHandleIsEmptyAndIsSmall(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	stubFS := &stubStatFileFS{existing: map[string]struct{}{}}
	ch := &ChangeHandler{
		scheduler: tasks.NewParallelJobScheduler(1),
		fs:        stubFS,
		mp:        mp,
	}
	defer ch.scheduler.Stop()

	p, err := NewBaseHandlerWithObjEntries(
		context.Background(),
		ch,
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil, nil,
		false,
		mp,
		stubFS,
	)
	require.NoError(t, err)
	require.True(t, p.IsEmpty())
	require.True(t, p.IsSmall())
}

func TestGetObjectEntries(t *testing.T) {
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	tree := btree.NewBTreeGOptions[objectio.ObjectEntry](func(a, b objectio.ObjectEntry) bool {
		return a.ObjectShortName().ShortString() < b.ObjectShortName().ShortString()
	}, btree.Options{NoLocks: true})

	// appendable in range
	a1 := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
	tree.Set(a1)

	// appendable after end (excluded)
	a2 := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(25, 0))
	tree.Set(a2)

	// appendable deleted before start (excluded)
	a3 := *makeTestObjectEntry(t, 1, true, false, types.BuildTS(3, 0))
	a3.DeleteTime = types.BuildTS(8, 0)
	tree.Set(a3)

	// CN object in range
	cn1 := *makeTestObjectEntry(t, 1, false, true, types.BuildTS(15, 0))
	tree.Set(cn1)

	// CN object before start (excluded)
	cn2 := *makeTestObjectEntry(t, 1, false, true, types.BuildTS(5, 0))
	tree.Set(cn2)

	// TN non-appendable in range
	tn1 := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(15, 0))
	tree.Set(tn1)

	// TN non-appendable after end (excluded)
	tn2 := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(25, 0))
	tree.Set(tn2)

	p := &baseHandle{changesHandle: &ChangeHandler{}}
	iter := tree.Iter()
	aobj, cnObj, tnByCreateTS, tnKeys := p.getObjectEntries(iter, start, end)
	iter.Release()

	require.Len(t, aobj, 1)
	require.Len(t, cnObj, 1)
	require.Len(t, tnByCreateTS, 1)
	require.Len(t, tnKeys, 1)
}

func TestShouldReadBlock(t *testing.T) {
	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: false,
	}
	p := &baseHandle{changesHandle: ch}
	h := &AObjectHandle{
		p:          p,
		blockPlans: make(map[string]*aobjBlockPlan),
		start:      types.BuildTS(10, 0),
		end:        types.BuildTS(20, 0),
	}

	t.Run("nil object", func(t *testing.T) {
		ok, err := h.shouldReadBlock(context.Background(), nil, 0)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("prune disabled", func(t *testing.T) {
		ch.enableCommitTSBlockPrune = false
		obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(5, 0))
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
		ch.enableCommitTSBlockPrune = true
	})

	t.Run("appendable object always read", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(5, 0))
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("cn object always read", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, true, types.BuildTS(5, 0))
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("pre-populated plan evaluable block overlap", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(5, 0))
		key := obj.ObjectShortName().ShortString()
		h.blockPlans[key] = &aobjBlockPlan{
			initialized:      true,
			evaluable:        true,
			shouldReadByBlks: []bool{true, false},
			totalBlocks:      2,
			evaluableBlocks:  2,
		}
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = h.shouldReadBlock(context.Background(), obj, 1)
		require.NoError(t, err)
		require.False(t, ok)
		delete(h.blockPlans, key)
	})

	t.Run("pre-populated plan non-evaluable non-strict returns true", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		key := obj.ObjectShortName().ShortString()
		h.blockPlans[key] = &aobjBlockPlan{
			initialized: true,
			evaluable:   false,
		}
		ch.strictCommitTSBlockPrune = false
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.NoError(t, err)
		require.True(t, ok)
		delete(h.blockPlans, key)
	})

	t.Run("pre-populated plan non-evaluable strict returns error", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		key := obj.ObjectShortName().ShortString()
		h.blockPlans[key] = &aobjBlockPlan{
			initialized:         true,
			evaluable:           false,
			nonEvaluableReasons: map[string]int{"test": 1},
		}
		ch.strictCommitTSBlockPrune = true
		ok, err := h.shouldReadBlock(context.Background(), obj, 0)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
		require.False(t, ok)
		delete(h.blockPlans, key)
		ch.strictCommitTSBlockPrune = false
	})

	t.Run("block index out of range returns false", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		key := obj.ObjectShortName().ShortString()
		h.blockPlans[key] = &aobjBlockPlan{
			initialized:      true,
			evaluable:        true,
			shouldReadByBlks: []bool{true},
		}
		ok, err := h.shouldReadBlock(context.Background(), obj, 5) // out of range
		require.NoError(t, err)
		require.False(t, ok)
		delete(h.blockPlans, key)
	})
}

func TestNextPrefetchTarget(t *testing.T) {
	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: false,
	}
	p := &baseHandle{changesHandle: ch}
	obj := makeTestObjectEntry(t, 2, false, false, types.BuildTS(5, 0))
	key := obj.ObjectShortName().ShortString()
	h := &AObjectHandle{
		p:       p,
		objects: []*objectio.ObjectEntry{obj},
		blockPlans: map[string]*aobjBlockPlan{
			key: {
				initialized:      true,
				evaluable:        true,
				shouldReadByBlks: []bool{false, true}, // skip block 0, read block 1
				totalBlocks:      2,
				evaluableBlocks:  2,
			},
		},
		start: types.BuildTS(10, 0),
		end:   types.BuildTS(20, 0),
	}

	// First call should skip block 0, return block 1
	retObj, blk, ok, err := h.nextPrefetchTarget(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint16(1), blk)
	require.Equal(t, obj, retObj)

	// Second call should return no more targets
	_, _, ok, err = h.nextPrefetchTarget(context.Background())
	require.NoError(t, err)
	require.False(t, ok)
}

// writeTestObjectWithCommitTS creates an in-memory object file with a commit-TS
// column and returns its ObjectEntry plus the FileService that holds it.
func writeTestObjectWithCommitTS(
	t *testing.T,
	mp *mpool.MPool,
	tsValues []types.TS,
) (*objectio.ObjectEntry, fileservice.FileService) {
	t.Helper()
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)

	writer := ioutil.ConstructWriter(
		0,
		[]uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS},
		-1, false, false, fs,
	)

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	var blk types.Blockid
	for i, ts := range tsValues {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1],
			types.NewRowid(&blk, uint32(i+1)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], ts, false, mp))
	}
	bat.SetRowCount(len(tsValues))

	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(context.Background())
	require.NoError(t, err)
	require.True(t, len(blocks) > 0)

	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, uint32(len(blocks))))

	entry := &objectio.ObjectEntry{
		ObjectStats: stats,
		CreateTime:  types.BuildTS(50, 0),
	}
	return entry, fs
}

func TestBlockCommitTSOverlapsRange_WithRealZonemap(t *testing.T) {
	mp := mpool.MustNewZero()
	// Commit-TS range: [100, 200]
	obj, fs := writeTestObjectWithCommitTS(t, mp, []types.TS{
		types.BuildTS(100, 0),
		types.BuildTS(150, 0),
		types.BuildTS(200, 0),
	})

	ctx := context.Background()
	metaLoc := obj.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &metaLoc, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blk := dataMeta.GetBlockMeta(0)

	// Range that overlaps [100, 200]
	overlap, evaluable, _, _ := blockCommitTSOverlapsRange(
		blk, types.BuildTS(50, 0), types.BuildTS(120, 0),
	)
	require.True(t, evaluable, "block with TS zonemap should be evaluable")
	require.True(t, overlap, "range [50,120] overlaps commit-TS [100,200]")

	// Range completely after → no overlap
	overlap, evaluable, _, _ = blockCommitTSOverlapsRange(
		blk, types.BuildTS(300, 0), types.BuildTS(400, 0),
	)
	require.True(t, evaluable)
	require.False(t, overlap, "range [300,400] does not overlap commit-TS [100,200]")

	// Range completely before → no overlap
	overlap, evaluable, _, _ = blockCommitTSOverlapsRange(
		blk, types.BuildTS(1, 0), types.BuildTS(50, 0),
	)
	require.True(t, evaluable)
	require.False(t, overlap, "range [1,50] does not overlap commit-TS [100,200]")
}

func TestBuildBlockPlan_SuccessPath(t *testing.T) {
	mp := mpool.MustNewZero()
	// Object with commit-TS in [100, 200]
	obj, fs := writeTestObjectWithCommitTS(t, mp, []types.TS{
		types.BuildTS(100, 0),
		types.BuildTS(150, 0),
		types.BuildTS(200, 0),
	})

	sched := tasks.NewParallelJobScheduler(2)
	defer sched.Stop()

	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: false,
		scheduler:                sched,
	}
	h := &AObjectHandle{
		p:          &baseHandle{changesHandle: ch},
		fs:         fs,
		start:      types.BuildTS(50, 0),
		end:        types.BuildTS(120, 0),
		blockPlans: make(map[string]*aobjBlockPlan),
	}

	ctx := context.Background()
	plan := &aobjBlockPlan{}
	err := h.buildBlockPlan(ctx, obj, plan)
	require.NoError(t, err)
	require.True(t, plan.initialized)
	require.True(t, plan.evaluable, "should be evaluable with valid TS zonemap")
	require.Equal(t, 1, plan.totalBlocks)
	require.Equal(t, 1, plan.evaluableBlocks)
	require.Equal(t, 1, plan.overlapBlocks)
	require.Equal(t, 0, plan.prunedBlocks)
}

func TestBuildBlockPlan_PrunesBlock(t *testing.T) {
	mp := mpool.MustNewZero()
	// Object with commit-TS in [100, 200]
	obj, fs := writeTestObjectWithCommitTS(t, mp, []types.TS{
		types.BuildTS(100, 0),
		types.BuildTS(150, 0),
		types.BuildTS(200, 0),
	})

	sched := tasks.NewParallelJobScheduler(2)
	defer sched.Stop()

	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: false,
		scheduler:                sched,
	}
	// Range [300,400] does NOT overlap [100,200] → block should be pruned
	h := &AObjectHandle{
		p:          &baseHandle{changesHandle: ch},
		fs:         fs,
		start:      types.BuildTS(300, 0),
		end:        types.BuildTS(400, 0),
		blockPlans: make(map[string]*aobjBlockPlan),
	}

	ctx := context.Background()
	plan := &aobjBlockPlan{}
	err := h.buildBlockPlan(ctx, obj, plan)
	require.NoError(t, err)
	require.True(t, plan.evaluable)
	require.Equal(t, 1, plan.prunedBlocks, "block should be pruned — range doesn't overlap")
	require.Equal(t, 0, plan.overlapBlocks)
}

func TestShouldReadBlock_WithBuildBlockPlan(t *testing.T) {
	mp := mpool.MustNewZero()
	// Object with commit-TS in [100, 200]
	obj, fs := writeTestObjectWithCommitTS(t, mp, []types.TS{
		types.BuildTS(100, 0),
		types.BuildTS(150, 0),
		types.BuildTS(200, 0),
	})

	sched := tasks.NewParallelJobScheduler(2)
	defer sched.Stop()

	ch := &ChangeHandler{
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: false,
		scheduler:                sched,
	}

	// Overlapping range: shouldReadBlock → true
	h := &AObjectHandle{
		p:          &baseHandle{changesHandle: ch},
		fs:         fs,
		start:      types.BuildTS(50, 0),
		end:        types.BuildTS(120, 0),
		blockPlans: make(map[string]*aobjBlockPlan),
	}
	read, err := h.shouldReadBlock(context.Background(), obj, 0)
	require.NoError(t, err)
	require.True(t, read, "block overlaps range → should read")

	// Non-overlapping range: shouldReadBlock → false
	h2 := &AObjectHandle{
		p:          &baseHandle{changesHandle: ch},
		fs:         fs,
		start:      types.BuildTS(300, 0),
		end:        types.BuildTS(400, 0),
		blockPlans: make(map[string]*aobjBlockPlan),
	}
	read, err = h2.shouldReadBlock(context.Background(), obj, 0)
	require.NoError(t, err)
	require.False(t, read, "block doesn't overlap range → should NOT read")
}
