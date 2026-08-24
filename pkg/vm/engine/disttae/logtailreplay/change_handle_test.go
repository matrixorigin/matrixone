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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/tidwall/btree"
)

func noSpecialColumnLayout() objectio.SpecialColumnLayout {
	return objectio.SpecialColumnLayout{
		PhysicalAddr: objectio.InvalidSpecialColumnPosition,
		CommitTS:     objectio.InvalidSpecialColumnPosition,
		Abort:        objectio.InvalidSpecialColumnPosition,
	}
}

func TestValidateChangeObjectBlockCount(t *testing.T) {
	require.Error(t, func() error {
		_, err := validateChangeObjectBlockCount(nil)
		return err
	}())

	stats := objectio.NewObjectStats()
	for _, test := range []struct {
		count   uint32
		wantErr bool
	}{
		{count: 0, wantErr: true},
		{count: 1},
		{count: maxChangeObjectBlockCount},
		{count: maxChangeObjectBlockCount + 1, wantErr: true},
	} {
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, test.count))
		count, err := validateChangeObjectBlockCount(stats)
		if test.wantErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, int(test.count), count)
	}
}

func TestAObjectNextPrefetchTargetSupportsMaximumBlockID(t *testing.T) {
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, maxChangeObjectBlockCount))
	entry := &objectio.ObjectEntry{ObjectStats: *stats}
	handle := &AObjectHandle{
		objects:         []*objectio.ObjectEntry{entry},
		blkOffsetCursor: int(maxChangeObjectBlockCount) - 1,
		p:               &baseHandle{changesHandle: &ChangeHandler{}},
		blockPlans:      make(map[string]*aobjBlockPlan),
	}

	got, blockID, ok, err := handle.nextPrefetchTarget(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, entry, got)
	require.Equal(t, ^uint16(0), blockID)
	require.Equal(t, 1, handle.objectOffsetCursor)
	require.Zero(t, handle.blkOffsetCursor)
}

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

func TestAObjectHandleConversionFailureIsStickyAndReleasesCache(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	baseline := mp.CurrNB()

	malformed := batch.NewWithSize(2)
	malformed.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
	malformed.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	malformed.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(malformed.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(malformed.Vecs[1], int64(2), false, mp))
	malformed.SetRowCount(1)

	later := batch.NewWithSize(2)
	later.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
	later.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	later.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(later.Vecs[0], int64(3), false, mp))
	require.NoError(t, vector.AppendFixed(later.Vecs[1], types.BuildTS(10, 0), false, mp))
	later.SetRowCount(1)

	changes := &ChangeHandler{}
	base := &baseHandle{
		changesHandle: changes,
		skipTS:        make(map[types.TS]struct{}),
	}
	handle := &AObjectHandle{
		start: types.BuildTS(1, 0),
		end:   types.BuildTS(20, 0),
		mp:    mp,
		cache: []*batch.Batch{malformed, later},
		p:     base,
	}
	base.aobjHandle = handle
	changes.dataHandle = base

	err := handle.getNextAObject(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit column")
	require.Equal(t, baseline, mp.CurrNB(), "the failed block and later cache must be released")
	require.Nil(t, handle.currentBatch)
	require.Nil(t, handle.cache)

	// Every layer must preserve the first failure. Otherwise a caller retry can
	// select a later source and silently omit the malformed block.
	retryErr := handle.getNextAObject(context.Background())
	require.EqualError(t, retryErr, err.Error())
	var output *batch.Batch
	retryErr = base.Next(context.Background(), &output, mp)
	require.EqualError(t, retryErr, err.Error())
	data, tombstone, _, retryErr := changes.Next(context.Background(), mp)
	require.EqualError(t, retryErr, err.Error())
	require.Nil(t, data)
	require.Nil(t, tombstone)
}

func TestCNObjectHandleStructuralFailureIsStickyAndReleasesCache(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	baseline := mp.CurrNB()

	cached := batch.NewOffHeapWithSize(1)
	cached.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(cached.Vecs[0], int64(1), false, mp))
	cached.SetRowCount(1)
	require.Greater(t, mp.CurrNB(), baseline)

	changes := &ChangeHandler{}
	base := &baseHandle{changesHandle: changes}
	handle := &CNObjectHandle{
		mp:    mp,
		base:  base,
		cache: []*batch.Batch{cached},
		// The missing prepared/TS/layout entries model a corrupted cache.
	}
	base.cnObjectHandle = handle
	changes.dataHandle = base

	var output *batch.Batch
	err := handle.Next(context.Background(), &output, mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cache metadata")
	require.Nil(t, output)
	require.Equal(t, baseline, mp.CurrNB())
	require.Nil(t, handle.cache)

	retryErr := handle.Next(context.Background(), &output, mp)
	require.EqualError(t, retryErr, err.Error())
	require.EqualError(t, base.terminalError(), err.Error())
}

func TestAObjectHandleNextTSRejectsMalformedCurrentBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	malformed := batch.NewWithSize(1)
	malformed.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(malformed.Vecs[0], int64(1), false, mp))
	malformed.SetRowCount(1)
	handle := &AObjectHandle{
		currentBatch: malformed,
		batchLength:  1,
		mp:           mp,
	}

	nextTS := handle.NextTS()
	require.True(t, nextTS.IsEmpty())
	require.Error(t, handle.terminalError())
	require.Nil(t, handle.currentBatch)
}

func TestCNObjectHandleEOBDoesNotReprepareOrLeakSource(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, mp))
	source.SetRowCount(1)
	handle := &CNObjectHandle{
		cache:    []*batch.Batch{source},
		TSs:      []types.TS{types.BuildTS(10, 0)},
		layouts:  []objectio.SpecialColumnLayout{noSpecialColumnLayout()},
		prepared: []bool{false},
		mp:       mp,
		base:     &baseHandle{changesHandle: &ChangeHandler{}},
	}
	defer handle.Close()

	incompatible := batch.NewWithSize(1)
	incompatible.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	incompatible.SetRowCount(0)
	defer incompatible.Clean(mp)
	err := handle.Next(context.Background(), &incompatible, mp)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	require.Len(t, handle.cache, 1)
	require.True(t, handle.prepared[0])
	require.Len(t, source.Vecs, 2)

	var output *batch.Batch
	require.NoError(t, handle.Next(context.Background(), &output, mp))
	defer output.Clean(mp)
	require.Empty(t, handle.cache)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0))
	commitTSs, err := ioutil.ValidateTombstoneCommitTSColumn(1, output.Vecs[1])
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(10, 0), commitTSs.At(0))
}

func TestCNObjectHandlePreparationFailureDoesNotSkipSource(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := batch.NewWithSize(1)
	source.SetRowCount(1)
	handle := &CNObjectHandle{
		cache:    []*batch.Batch{source},
		TSs:      []types.TS{types.BuildTS(10, 0)},
		layouts:  []objectio.SpecialColumnLayout{noSpecialColumnLayout()},
		prepared: []bool{false},
		mp:       mp,
		base:     &baseHandle{changesHandle: &ChangeHandler{}},
	}
	defer handle.Close()

	var output *batch.Batch
	require.Error(t, handle.Next(context.Background(), &output, mp))
	require.Nil(t, output)
	require.Len(t, handle.cache, 1)
	require.False(t, handle.prepared[0])

	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, mp))
	require.NoError(t, handle.Next(context.Background(), &output, mp))
	defer output.Clean(mp)
	require.Empty(t, handle.cache)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0))
}

func TestCNObjectHandleNextTSHandlesMalformedState(t *testing.T) {
	var nilHandle *CNObjectHandle
	ts := nilHandle.NextTS()
	require.True(t, ts.IsEmpty())

	handle := &CNObjectHandle{objects: []*objectio.ObjectEntry{nil}}
	ts = handle.NextTS()
	require.True(t, ts.IsEmpty())

	handle.cache = []*batch.Batch{batch.NewWithSize(0)}
	ts = handle.NextTS()
	require.True(t, ts.IsEmpty())
}

func TestFillInInsertBatchProjectsSparsePhysicalSeqnumsToLogicalSchema(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := batch.NewWithSize(8)
	source.SetAttributes([]string{
		catalog.Row_ID,
		objectio.DefaultCommitTS_Attr,
		"value",
		"",
		"",
		"",
		"",
		"pk",
	})
	source.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	source.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	source.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	for position := 3; position < 7; position++ {
		source.Vecs[position] = vector.NewVec(types.T_int8.ToType())
	}
	source.Vecs[7] = vector.NewVec(types.T_varchar.ToType())
	defer source.Clean(mp)
	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(blockID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(source.Vecs[1], types.BuildTS(10, 0), false, mp))
	require.NoError(t, vector.AppendFixed(source.Vecs[2], int64(42), false, mp))
	require.NoError(t, vector.AppendBytes(source.Vecs[7], []byte("key"), false, mp))
	source.SetRowCount(1)

	schema := &engine.CollectChangesSchema{
		Attrs:   []string{"value", "pk", "added"},
		Types:   []types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int32.ToType()},
		Seqnums: []uint16{0, 5, 6},
	}
	var output *batch.Batch
	err := fillInInsertBatchWithSchema(
		&output,
		&RowEntry{Batch: source, Offset: 0},
		schema,
		false,
		mp,
	)
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Clean(mp)
	require.Equal(t, []string{"value", "pk", "added", objectio.DefaultCommitTS_Attr}, output.Attrs)
	require.Equal(t, 1, output.RowCount())
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0))
	require.Equal(t, "key", output.Vecs[1].GetStringAt(0))
	require.Equal(t, types.T_int32, output.Vecs[2].GetType().Oid)
	require.True(t, output.Vecs[2].IsNull(0))
	require.Equal(t, types.BuildTS(10, 0), vector.GetFixedAtNoTypeCheck[types.TS](output.Vecs[3], 0))

	handler := &ChangeHandler{primarySeqnum: 5}
	ctx := engine.WithCollectChangesSchema(context.Background(), schema)
	require.NoError(t, handler.applyRequestSchema(ctx))
	require.Equal(t, 1, handler.primaryPosition)
}

func TestFillInInsertBatchRejectsPopulatedSparsePlaceholder(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	source := batch.NewWithSize(3)
	source.SetAttributes([]string{
		catalog.Row_ID,
		objectio.DefaultCommitTS_Attr,
		"",
	})
	source.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	source.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	source.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	defer source.Clean(mp)
	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	require.NoError(t, vector.AppendFixed(
		source.Vecs[0], types.NewRowid(blockID, 0), false, mp,
	))
	require.NoError(t, vector.AppendFixed(
		source.Vecs[1], types.BuildTS(10, 0), false, mp,
	))
	require.NoError(t, vector.AppendFixed(source.Vecs[2], int64(42), false, mp))
	source.SetRowCount(1)

	schema := &engine.CollectChangesSchema{
		Attrs:   []string{"value"},
		Types:   []types.Type{types.T_int64.ToType()},
		Seqnums: []uint16{0},
	}
	var output *batch.Batch
	err := fillInInsertBatchWithSchema(
		&output,
		&RowEntry{Batch: source, Offset: 0},
		schema,
		false,
		mp,
	)
	require.ErrorContains(t, err, "placeholder column 2 contains 1 rows")
	require.Nil(t, output, "invalid sparse input must not publish a partial batch")
}

func TestPersistedDataProjectionDoesNotConfuseNewSeqnumWithOldSpecialColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_bool.ToType())
	blockID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(7), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("pk"), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.NewRowid(blockID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.BuildTS(10, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[4], false, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	schema := &engine.CollectChangesSchema{
		Attrs:   []string{"value", "pk", "added"},
		Types:   []types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int32.ToType()},
		Seqnums: []uint16{0, 5, 6},
	}
	err := updatePersistedDataBatchWithSchema(
		bat,
		types.BuildTS(1, 0),
		types.BuildTS(20, 0),
		blockID,
		objectio.SpecialColumnLayout{PhysicalAddr: 2, CommitTS: 3, Abort: 4},
		[]uint16{0, 5, 6, 7, 8},
		schema,
		false,
		mp,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"value", "pk", "added", objectio.DefaultCommitTS_Attr}, bat.Attrs)
	require.Len(t, bat.Vecs, 4)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
	require.Equal(t, "pk", bat.Vecs[1].GetStringAt(0))
	require.Equal(t, types.T_int32, bat.Vecs[2].GetType().Oid)
	require.True(t, bat.Vecs[2].IsNull(0), "old physical rowid at sequence 6 must not become the new user column")
}

func TestObjectPrefetchReturnsSchedulerFailureWithoutWaiting(t *testing.T) {
	scheduler := tasks.NewParallelJobScheduler(1)
	scheduler.Stop()
	changes := &ChangeHandler{scheduler: scheduler}
	base := &baseHandle{changesHandle: changes}
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(10, 0))

	t.Run("CN object", func(t *testing.T) {
		handle := NewCNObjectHandle(false, []*objectio.ObjectEntry{obj}, nil, base, nil)
		err := handle.prefetch(context.Background())
		require.Error(t, err)
		require.Empty(t, handle.cache)
		require.Zero(t, handle.objectOffsetCursor)
		require.Zero(t, handle.blkOffsetCursor)
	})

	t.Run("appendable object", func(t *testing.T) {
		handle := NewAObjectHandle(
			context.Background(), base, false, types.TS{}, types.MaxTs(),
			[]*objectio.ObjectEntry{obj}, nil, nil,
		)
		err := handle.prefetch(context.Background())
		require.Error(t, err)
		require.Empty(t, handle.cache)
		require.Zero(t, handle.objectOffsetCursor)
		require.Zero(t, handle.blkOffsetCursor)
	})
}

func TestAppendChangeBatchWindowMaterializesConstantDestination(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	firstTS := types.BuildTS(10, 0)
	secondTS := types.BuildTS(20, 0)
	dst := batch.NewWithSize(2)
	dst.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, value := range []int64{1, 2} {
		require.NoError(t, vector.AppendFixed(dst.Vecs[0], value, false, mp))
	}
	var err error
	dst.Vecs[1], err = vector.NewConstFixed(types.T_TS.ToType(), firstTS, 2, mp)
	require.NoError(t, err)
	dst.SetRowCount(2)
	defer dst.Clean(mp)

	src := batch.NewWithSize(2)
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, value := range []int64{3, 4} {
		require.NoError(t, vector.AppendFixed(src.Vecs[0], value, false, mp))
	}
	src.Vecs[1], err = vector.NewConstFixed(types.T_TS.ToType(), secondTS, 2, mp)
	require.NoError(t, err)
	src.SetRowCount(2)
	defer src.Clean(mp)

	require.NoError(t, appendChangeBatchWindow(dst, src, 0, 2, mp))
	require.Equal(t, 4, dst.RowCount())
	require.False(t, dst.Vecs[1].IsConst())
	require.Equal(t, []int64{1, 2, 3, 4}, vector.MustFixedColNoTypeCheck[int64](dst.Vecs[0]))
	require.Equal(t, []types.TS{firstTS, firstTS, secondTS, secondTS},
		vector.MustFixedColNoTypeCheck[types.TS](dst.Vecs[1]))

	malformed := batch.NewWithSize(2)
	malformed.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	malformed.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(malformed.Vecs[0], int64(5), false, mp))
	require.NoError(t, vector.AppendFixed(malformed.Vecs[1], secondTS, false, mp))
	malformed.SetRowCount(1)
	defer malformed.Clean(mp)

	require.Error(t, appendChangeBatchWindow(dst, malformed, 0, 2, mp))
	require.Equal(t, 4, dst.RowCount())
	require.Equal(t, []int64{1, 2, 3, 4}, vector.MustFixedColNoTypeCheck[int64](dst.Vecs[0]))
}

func TestAppendChangeBatchWindowRollsBackAllocationFailure(t *testing.T) {
	dstMP, err := mpool.NewMPool("change-window-rollback", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	srcMP := mpool.MustNewZero()

	dst := batch.NewWithSize(2)
	dst.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	dst.Vecs[0].SetOffHeap(true)
	dst.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	dst.Vecs[1].SetOffHeap(true)
	require.NoError(t, vector.AppendFixed(dst.Vecs[0], int64(1), false, dstMP))
	require.NoError(t, vector.AppendBytes(dst.Vecs[1], []byte("kept"), false, dstMP))
	dst.SetRowCount(1)

	src := batch.NewWithSize(2)
	src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	src.Vecs[0].SetOffHeap(true)
	src.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	src.Vecs[1].SetOffHeap(true)
	require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(2), false, srcMP))
	require.NoError(t, vector.AppendBytes(src.Vecs[1], make([]byte, 2<<20), false, srcMP))
	src.SetRowCount(1)

	require.Error(t, appendChangeBatchWindow(dst, src, 0, 1, dstMP))
	require.Equal(t, 1, dst.RowCount())
	require.Equal(t, []int64{1}, vector.MustFixedColNoTypeCheck[int64](dst.Vecs[0]))
	require.Equal(t, "kept", dst.Vecs[1].GetStringAt(0))
	require.Equal(t, 1, dst.Vecs[1].Length())

	dst.Clean(dstMP)
	src.Clean(srcMP)
	require.Zero(t, dstMP.CurrNB())
	require.Zero(t, srcMP.CurrNB())
	mpool.DeleteMPool(dstMP)
	mpool.DeleteMPool(srcMP)
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

	require.NoError(t, updateCNTombstoneBatch(
		bat, types.BuildTS(20, 0), nil, noSpecialColumnLayout(), false, mp,
	))
	require.NoError(t, updateCNTombstoneBatch(
		bat, types.BuildTS(30, 0), nil, noSpecialColumnLayout(), false, mp,
	))

	require.Equal(t, 2, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
}

func TestUpdateCNTombstoneBatchPreservesTimestampPrimaryKey(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	pkValues := []types.TS{types.BuildTS(1, 0), types.BuildTS(2, 0)}
	for row, pk := range pkValues {
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[0], types.NewRowid(new(types.Blockid), uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], pk, false, mp))
	}
	bat.SetRowCount(len(pkValues))
	defer bat.Clean(mp)

	commitTS := types.BuildTS(10, 0)
	require.NoError(t, updateCNTombstoneBatch(
		bat, commitTS, nil, noSpecialColumnLayout(), false, mp,
	))
	require.Len(t, bat.Vecs, 2)
	require.Equal(t, pkValues, vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[0]))
	validated, err := ioutil.ValidateTombstoneCommitTSColumn(len(pkValues), bat.Vecs[1])
	require.NoError(t, err)
	for row := range pkValues {
		require.Equal(t, commitTS, validated.At(row))
	}
}

func TestUpdateCNTombstoneBatchUsesLayoutToDropPhysicalRowID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	deletedBlock := objectio.NewBlockid(objectio.NewSegmentid(), 1, 0)
	storageBlock := objectio.NewBlockid(objectio.NewSegmentid(), 2, 0)
	semanticRowID := types.NewRowid(deletedBlock, 11)
	physicalRowID := types.NewRowid(storageBlock, 0)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], semanticRowID, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(7), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], physicalRowID, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	layout := noSpecialColumnLayout()
	layout.PhysicalAddr = 2
	require.NoError(t, updateCNTombstoneBatch(
		bat, types.BuildTS(10, 0), nil, layout, true, mp,
	))
	require.Len(t, bat.Vecs, 3)
	require.Equal(t, semanticRowID,
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], 0))
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[1], 0))
	require.Equal(t, types.T_TS, bat.Vecs[2].GetType().Oid)
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
	layout := noSpecialColumnLayout()
	layout.CommitTS = 1
	require.NoError(t, updateCNDataBatch(bat, newCommitTS, nil, layout, false, mp))

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
	require.NoError(t, updateCNDataBatch(
		bat, newCommitTS, nil, noSpecialColumnLayout(), false, mp,
	))

	// Verify commitTS vector is added at the end
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[2].GetType().Oid)

	bat.Clean(mp)
}

func TestUpdateCNDataBatchPreservesUserTimestampColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	userTS := []types.TS{types.BuildTS(1, 0), types.BuildTS(2, 0)}
	for row, ts := range userTS {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], ts, false, mp))
	}
	bat.SetRowCount(len(userTS))
	defer bat.Clean(mp)

	commitTS := types.BuildTS(100, 0)
	require.NoError(t, updateCNDataBatch(
		bat, commitTS, nil, noSpecialColumnLayout(), false, mp,
	))
	require.Len(t, bat.Vecs, 3)
	require.Equal(t, userTS, vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1]))
	validated, err := ioutil.ValidateTombstoneCommitTSColumn(len(userTS), bat.Vecs[2])
	require.NoError(t, err)
	for row := range userTS {
		require.Equal(t, commitTS, validated.At(row))
	}
}

func TestAppendFromEntryDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.New(types.T_decimal256, 39, 4)
	src := vector.NewVec(typ)
	dst := vector.NewVec(typ)
	defer src.Free(mp)
	defer dst.Free(mp)

	val, err := types.ParseDecimal256("12345678901234567890123456789012345.6789", typ.Width, typ.Scale)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(src, val, false, mp))
	require.NoError(t, vector.AppendFixed(src, types.Decimal256{}, true, mp))

	require.NoError(t, appendFromEntry(src, dst, 0, mp))
	require.NoError(t, appendFromEntry(src, dst, 1, mp))

	require.Equal(t, 2, dst.Length())
	require.Equal(t, val, vector.GetFixedAtNoTypeCheck[types.Decimal256](dst, 0))
	require.False(t, dst.GetNulls().Contains(0))
	require.True(t, dst.GetNulls().Contains(1))
}

func TestAppendFromEntryYear(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.T_year.ToType()
	src := vector.NewVec(typ)
	dst := vector.NewVec(typ)
	defer src.Free(mp)
	defer dst.Free(mp)

	val := types.MoYear(2024)
	require.NoError(t, vector.AppendFixed(src, val, false, mp))
	require.NoError(t, vector.AppendFixed(src, types.MoYear(0), true, mp))

	require.NoError(t, appendFromEntry(src, dst, 0, mp))
	require.NoError(t, appendFromEntry(src, dst, 1, mp))

	require.Equal(t, 2, dst.Length())
	require.Equal(t, val, vector.GetFixedAtNoTypeCheck[types.MoYear](dst, 0))
	require.False(t, dst.GetNulls().Contains(0))
	require.True(t, dst.GetNulls().Contains(1))
}

func TestAppendFromEntryGeometry(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	for _, typ := range []types.Type{types.T_geometry.ToType(), types.T_geometry32.ToType()} {
		src := vector.NewVec(typ)
		dst := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(src, []byte{1, 2, 3}, false, mp))
		require.NoError(t, vector.AppendBytes(src, nil, true, mp))

		require.NoError(t, appendFromEntry(src, dst, 0, mp))
		require.NoError(t, appendFromEntry(src, dst, 1, mp))

		require.Equal(t, []byte{1, 2, 3}, dst.GetBytesAt(0))
		require.True(t, dst.GetNulls().Contains(1))
		src.Free(mp)
		dst.Free(mp)
	}
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

	require.NoError(t, updateDataBatch(bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, nil, false, mp))

	require.Equal(t, 4, len(bat.Vecs))
	require.Equal(t, []string{"id", "created_at", "updated_at", objectio.DefaultCommitTS_Attr}, bat.Attrs)
	require.Equal(t, types.T_varchar, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[2].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[3].GetType().Oid)
	require.Equal(t, updatedAtVal, vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[2])[0])

	bat.Clean(mp)
}

func TestUpdateDataBatchPreservesUserTSColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"id", "user_ts", objectio.DefaultCommitTS_Attr})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	userTS := []types.TS{types.BuildTS(1, 0), types.BuildTS(2, 0)}
	commitTS := []types.TS{types.BuildTS(100, 0), types.BuildTS(200, 0)}
	for row := range userTS {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], userTS[row], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], commitTS[row], false, mp))
	}
	bat.SetRowCount(len(userTS))
	defer bat.Clean(mp)

	require.NoError(t, updateDataBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, nil, false, mp,
	))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, []string{"id", "user_ts", objectio.DefaultCommitTS_Attr}, bat.Attrs)
	require.Equal(t, userTS[0], vector.GetFixedAtNoTypeCheck[types.TS](bat.Vecs[1], 0))
	require.Equal(t, commitTS[0], vector.GetFixedAtNoTypeCheck[types.TS](bat.Vecs[2], 0))
}

func TestUpdateDataBatchPreservesAttributedUserRowIDColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"id", "user_rowid", objectio.DefaultCommitTS_Attr})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	userBlock := objectio.NewBlockid(objectio.NewSegmentid(), 3, 0)
	userRowID := types.NewRowid(userBlock, 9)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], userRowID, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(100, 0), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	physicalBlock := objectio.NewBlockid(objectio.NewSegmentid(), 7, 0)
	require.NoError(t, updateDataBatch(
		bat,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
		physicalBlock,
		nil,
		true,
		mp,
	))
	require.Equal(t, []string{
		catalog.Row_ID, "id", "user_rowid", objectio.DefaultCommitTS_Attr,
	}, bat.Attrs)
	require.Len(t, bat.Vecs, 4)
	require.Equal(t, types.NewRowid(physicalBlock, 0),
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], 0))
	require.Equal(t, userRowID,
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[2], 0))
}

func TestUpdateDataBatchPreservesLegacyUserRowIDColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	// Attribute-less replay batches use only a *leading* ROWID as the hidden
	// physical address. The second vector here is user data and must survive.
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var userBlock types.Blockid
	userRowID := types.NewRowid(&userBlock, 7)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], userRowID, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(100, 0), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	physicalBlock := objectio.NewBlockid(objectio.NewSegmentid(), 2, 0)
	require.NoError(t, updateDataBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), physicalBlock, nil, true, mp,
	))
	require.Len(t, bat.Vecs, 4)
	require.Empty(t, bat.Attrs)
	require.Equal(t, types.NewRowid(physicalBlock, 0),
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], 0))
	require.Equal(t, userRowID,
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[2], 0))
}

func TestUpdateTombstoneBatchPreservesLegacyRowIDPrimaryKey(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var physicalBlock, userPKBlock types.Blockid
	physicalRowID := types.NewRowid(&physicalBlock, 1)
	userPK := types.NewRowid(&userPKBlock, 9)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], physicalRowID, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], userPK, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(100, 0), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.NoError(t, updateTombstoneBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, false,
		&physicalBlock, nil, false, mp,
	))
	require.Equal(t, []string{
		objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr,
	}, bat.Attrs)
	require.Equal(t, userPK,
		vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], 0))
}

func TestChangeCommitTSPositionRejectsCompleteAttrsWithoutCommitMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{"id", "user_ts"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	defer bat.Clean(mp)

	_, err := changeCommitTSPosition(bat)
	require.Error(t, err)
}

func TestFilterBatchPreservesSemanticUserRowIDPosition(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	var blockID types.Blockid
	pk := types.NewRowid(&blockID, 7)
	data := batch.NewWithSize(2)
	data.SetAttributes([]string{"user_rowid", objectio.DefaultCommitTS_Attr})
	data.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	data.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], pk, false, mp))
	require.NoError(t, vector.AppendFixed(data.Vecs[1], types.BuildTS(100, 0), false, mp))
	data.SetRowCount(1)
	defer data.Clean(mp)

	tombstone := batch.NewWithSize(2)
	tombstone.SetAttributes([]string{
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr,
	})
	tombstone.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstone.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], pk, false, mp))
	require.NoError(t, vector.AppendFixed(tombstone.Vecs[1], types.BuildTS(200, 0), false, mp))
	tombstone.SetRowCount(1)
	defer tombstone.Clean(mp)

	require.NoError(t, filterBatch(data, tombstone, 0, false, false))
	require.Zero(t, data.RowCount())
	require.Zero(t, tombstone.RowCount())
}

func TestFilterBatchRejectsNullPrimaryKeyWithoutMutation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newBatch := func(pkNull bool) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), pkNull, mp))
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[1], types.BuildTS(10, 0), false, mp,
		))
		bat.SetRowCount(1)
		return bat
	}
	data := newBatch(true)
	tombstone := newBatch(false)
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	err := filterBatch(data, tombstone, 0, false, false)
	require.ErrorContains(t, err, "null primary key")
	require.Equal(t, 1, data.RowCount())
	require.Equal(t, 1, tombstone.RowCount())
}

func TestFilterBatchRejectsMismatchedPrimaryKeyTypesWithoutMutation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(2)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	data.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(
		data.Vecs[1], types.BuildTS(10, 0), false, mp,
	))
	data.SetRowCount(1)
	defer data.Clean(mp)

	tombstone := batch.NewWithSize(2)
	tombstone.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	tombstone.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendBytes(tombstone.Vecs[0], []byte("1"), false, mp))
	require.NoError(t, vector.AppendFixed(
		tombstone.Vecs[1], types.BuildTS(20, 0), false, mp,
	))
	tombstone.SetRowCount(1)
	defer tombstone.Clean(mp)

	err := filterBatch(data, tombstone, 0, false, false)
	require.ErrorContains(t, err, "primary-key types differ")
	require.Equal(t, 1, data.RowCount())
	require.Equal(t, 1, tombstone.RowCount())
}

func TestUpdatePersistedDataBatchDropsPartialAttributes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(4)
	bat.Attrs = []string{"user", "stale-physical-name"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	var blockID types.Blockid
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.NewRowid(&blockID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(100, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], false, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	layout := objectio.SpecialColumnLayout{PhysicalAddr: 1, CommitTS: 2, Abort: 3}
	require.NoError(t, updatePersistedDataBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), &blockID, layout, false, mp,
	))
	require.Len(t, bat.Vecs, 2)
	require.Empty(t, bat.Attrs)
}

func TestUpdateTombstoneBatchPreservesTimestampPrimaryKey(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var blk types.Blockid
	pkValues := []types.TS{types.BuildTS(1, 0), types.BuildTS(2, 0)}
	commitTS := []types.TS{types.BuildTS(100, 0), types.BuildTS(200, 0)}
	for row := range pkValues {
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[0], types.NewRowid(&blk, uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], pkValues[row], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], commitTS[row], false, mp))
	}
	bat.SetRowCount(len(pkValues))
	defer bat.Clean(mp)

	require.NoError(t, updateTombstoneBatch(
		bat,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
		nil,
		false,
		nil,
		nil,
		false,
		mp,
	))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, pkValues[0], vector.GetFixedAtNoTypeCheck[types.TS](bat.Vecs[0], 0))
	require.Equal(t, commitTS[0], vector.GetFixedAtNoTypeCheck[types.TS](bat.Vecs[1], 0))
}

func TestFillInInsertBatchPreservesUserTSColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := batch.NewWithSize(3)
	src.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "user_ts"})
	src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	src.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	src.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var blk types.Blockid
	require.NoError(t, vector.AppendFixed(src.Vecs[0], types.NewRowid(&blk, 0), false, mp))
	require.NoError(t, vector.AppendFixed(src.Vecs[1], types.BuildTS(100, 0), false, mp))
	userTS := types.BuildTS(7, 0)
	require.NoError(t, vector.AppendFixed(src.Vecs[2], userTS, false, mp))
	src.SetRowCount(1)
	defer src.Clean(mp)

	var output *batch.Batch
	require.NoError(t, fillInInsertBatch(
		&output,
		&RowEntry{Batch: src, Offset: 0, Time: types.BuildTS(100, 0)},
		false,
		mp,
	))
	require.NotNil(t, output)
	defer output.Clean(mp)
	require.Equal(t, 1, output.RowCount())
	require.Equal(t, []string{"user_ts", objectio.DefaultCommitTS_Attr}, output.Attrs)
	require.Equal(t, userTS, vector.GetFixedAtNoTypeCheck[types.TS](output.Vecs[0], 0))
	require.Equal(t, types.BuildTS(100, 0), vector.GetFixedAtNoTypeCheck[types.TS](output.Vecs[1], 0))
}

func TestFillInInsertBatchRejectsCommitTimestampDrift(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := batch.NewWithSize(3)
	src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	src.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	src.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	var blockID types.Blockid
	rowID := types.NewRowid(&blockID, 0)
	require.NoError(t, vector.AppendFixed(src.Vecs[0], rowID, false, mp))
	require.NoError(t, vector.AppendFixed(src.Vecs[1], types.BuildTS(10, 0), false, mp))
	require.NoError(t, vector.AppendFixed(src.Vecs[2], int64(1), false, mp))
	src.SetRowCount(1)
	defer src.Clean(mp)

	var output *batch.Batch
	err := fillInInsertBatch(
		&output,
		&RowEntry{
			RowID:  rowID,
			Time:   types.BuildTS(11, 0),
			Batch:  src,
			Offset: 0,
		},
		false,
		mp,
	)
	require.ErrorContains(t, err, "does not match row entry")
	require.Nil(t, output)
}

func TestFillInInsertBatchRollsBackAllocationFailure(t *testing.T) {
	dstMP, err := mpool.NewMPool("in-memory-change-row-rollback", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	srcMP := mpool.MustNewZero()

	src := batch.NewWithSize(3)
	src.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "payload"})
	src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	src.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	src.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	var blockID types.Blockid
	require.NoError(t, vector.AppendFixed(src.Vecs[0], types.NewRowid(&blockID, 0), false, srcMP))
	require.NoError(t, vector.AppendFixed(src.Vecs[1], types.BuildTS(1, 0), false, srcMP))
	require.NoError(t, vector.AppendBytes(src.Vecs[2], make([]byte, 2<<20), false, srcMP))
	src.SetRowCount(1)

	output := newDataBatchWithBatch(src, true)
	for _, vec := range output.Vecs {
		vec.SetOffHeap(true)
	}
	err = fillInInsertBatch(
		&output,
		&RowEntry{Batch: src, Offset: 0, Time: types.BuildTS(1, 0)},
		true,
		dstMP,
	)
	require.Error(t, err)
	require.NotNil(t, output)
	require.Zero(t, output.RowCount())
	for _, vec := range output.Vecs {
		require.Zero(t, vec.Length())
	}

	output.Clean(dstMP)
	src.Clean(srcMP)
	require.Zero(t, dstMP.CurrNB())
	require.Zero(t, srcMP.CurrNB())
	mpool.DeleteMPool(dstMP)
	mpool.DeleteMPool(srcMP)
}

func TestFillInInsertBatchRejectsSchemaDriftWithoutPublishingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newSource := func(attr string, value int32) *batch.Batch {
		src := batch.NewWithSize(3)
		src.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, attr})
		src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		src.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		src.Vecs[2] = vector.NewVec(types.T_int32.ToType())
		var blockID types.Blockid
		require.NoError(t, vector.AppendFixed(src.Vecs[0], types.NewRowid(&blockID, 0), false, mp))
		require.NoError(t, vector.AppendFixed(src.Vecs[1], types.BuildTS(1, 0), false, mp))
		require.NoError(t, vector.AppendFixed(src.Vecs[2], value, false, mp))
		src.SetRowCount(1)
		return src
	}
	first := newSource("old_value", 1)
	second := newSource("new_value", 2)
	defer first.Clean(mp)
	defer second.Clean(mp)

	var output *batch.Batch
	require.NoError(t, fillInInsertBatch(
		&output, &RowEntry{Batch: first, Offset: 0}, false, mp,
	))
	require.Error(t, fillInInsertBatch(
		&output, &RowEntry{Batch: second, Offset: 0}, false, mp,
	))
	defer output.Clean(mp)
	require.Equal(t, 1, output.RowCount())
	require.Equal(t, []string{"old_value", objectio.DefaultCommitTS_Attr}, output.Attrs)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](output.Vecs[0]))
}

func TestUpdateDataBatch_RetainsSynthesizedRowID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"a", "b", objectio.DefaultCommitTS_Attr})

	aVec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(aVec, int32(4), false, mp))
	require.NoError(t, vector.AppendFixed(aVec, int32(5), false, mp))
	bat.Vecs[0] = aVec

	bVec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bVec, int32(40), false, mp))
	require.NoError(t, vector.AppendFixed(bVec, int32(50), false, mp))
	bat.Vecs[1] = bVec

	tsVec := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp))
	require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(200, 0), false, mp))
	bat.Vecs[2] = tsVec
	bat.SetRowCount(2)

	blk := types.Blockid{}
	require.NoError(t, updateDataBatch(bat, types.BuildTS(50, 0), types.BuildTS(150, 0), &blk, nil, true, mp))

	require.Equal(t, 4, len(bat.Vecs))
	require.Equal(t, catalog.Row_ID, bat.Attrs[0])
	require.Equal(t, types.T_Rowid, bat.Vecs[0].GetType().Oid)
	require.Equal(t, 1, bat.Vecs[0].Length())

	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
	require.Equal(t, types.NewRowid(&blk, 0), rowIDs[0])

	bat.Clean(mp)
}

func TestUpdatePersistedDataBatch_RetainsLeadingRowID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(5)
	bat.SetAttributes([]string{"a", "b", catalog.Row_ID, objectio.DefaultCommitTS_Attr, objectio.DefaultAbort_Attr})
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_bool.ToType())
	blk := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	for i, aborted := range []bool{false, true, false} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32((i+1)*10), false, mp))
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[2], types.NewRowid(blk, uint32(i)), false, mp))
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[3], types.BuildTS(int64(100+i), 0), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], aborted, false, mp))
	}
	bat.SetRowCount(3)

	layout := objectio.SpecialColumnLayout{
		PhysicalAddr: 2,
		CommitTS:     3,
		Abort:        4,
	}
	require.NoError(t, updatePersistedDataBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), blk, layout, true, mp))

	require.Equal(t, []string{
		catalog.Row_ID, "a", "b", objectio.DefaultCommitTS_Attr,
	}, bat.Attrs)
	require.Equal(t, []types.T{
		types.T_Rowid, types.T_int32, types.T_int32, types.T_TS,
	}, []types.T{
		bat.Vecs[0].GetType().Oid,
		bat.Vecs[1].GetType().Oid,
		bat.Vecs[2].GetType().Oid,
		bat.Vecs[3].GetType().Oid,
	})
	require.Equal(t, 2, bat.RowCount())
	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
	require.Equal(t, uint32(0), rowIDs[0].GetRowOffset())
	require.Equal(t, uint32(2), rowIDs[1].GetRowOffset())
	require.Equal(t, []int32{1, 3}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1]))

	bat.Clean(mp)
}

func TestUpdatePersistedTombstoneBatch_ConstMVCCColumns(t *testing.T) {
	for _, test := range []struct {
		name         string
		commitTS     types.TS
		withAbort    bool
		expectedRows int
	}{
		{name: "constant false abort keeps every row", commitTS: types.BuildTS(100, 0), withAbort: true, expectedRows: 2},
		{name: "missing abort still filters every row", commitTS: types.BuildTS(200, 0), expectedRows: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)

			vectorCount := 3
			if test.withAbort {
				vectorCount++
			}
			bat := batch.NewWithSize(vectorCount)
			defer bat.Clean(mp)
			bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			blk := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
			for row := range 2 {
				require.NoError(t, vector.AppendFixed(
					bat.Vecs[0], types.NewRowid(blk, uint32(row)), false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(row+1), false, mp))
			}
			var err error
			bat.Vecs[2], err = vector.NewConstFixed(types.T_TS.ToType(), test.commitTS, 2, mp)
			require.NoError(t, err)

			layout := objectio.SpecialColumnLayout{
				PhysicalAddr: objectio.InvalidSpecialColumnPosition,
				CommitTS:     2,
				Abort:        objectio.InvalidSpecialColumnPosition,
			}
			if test.withAbort {
				bat.Vecs[3], err = vector.NewConstFixed(types.T_bool.ToType(), false, 2, mp)
				require.NoError(t, err)
				layout.Abort = 3
			}
			bat.SetRowCount(2)

			require.NoError(t, updatePersistedTombstoneBatch(
				bat,
				types.BuildTS(50, 0),
				types.BuildTS(150, 0),
				nil,
				false,
				layout,
				true,
				mp,
			))
			require.Equal(t, test.expectedRows, bat.RowCount())
			require.Equal(t, test.expectedRows, bat.Vecs[0].Length())
			require.Equal(t, test.expectedRows, bat.Vecs[1].Length())
			require.Equal(t, test.expectedRows, bat.Vecs[2].Length())
		})
	}
}

func TestUpdatePersistedDataBatch_ConstMVCCColumns(t *testing.T) {
	for _, test := range []struct {
		name         string
		commitTS     types.TS
		withAbort    bool
		expectedRows int
	}{
		{name: "constant false abort keeps every row", commitTS: types.BuildTS(100, 0), withAbort: true, expectedRows: 2},
		{name: "missing abort still filters every row", commitTS: types.BuildTS(200, 0), expectedRows: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)

			vectorCount := 2
			if test.withAbort {
				vectorCount++
			}
			bat := batch.NewWithSize(vectorCount)
			defer bat.Clean(mp)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			for row := range 2 {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row+1), false, mp))
			}
			var err error
			bat.Vecs[1], err = vector.NewConstFixed(types.T_TS.ToType(), test.commitTS, 2, mp)
			require.NoError(t, err)

			layout := objectio.SpecialColumnLayout{
				PhysicalAddr: objectio.InvalidSpecialColumnPosition,
				CommitTS:     1,
				Abort:        objectio.InvalidSpecialColumnPosition,
			}
			if test.withAbort {
				bat.Vecs[2], err = vector.NewConstFixed(types.T_bool.ToType(), false, 2, mp)
				require.NoError(t, err)
				layout.Abort = 2
			}
			bat.SetAttributes(make([]string, vectorCount))
			bat.SetRowCount(2)

			require.NoError(t, updatePersistedDataBatch(
				bat,
				types.BuildTS(50, 0),
				types.BuildTS(150, 0),
				nil,
				layout,
				false,
				mp,
			))
			require.Equal(t, test.expectedRows, bat.RowCount())
			require.Len(t, bat.Vecs, 2)
			require.Equal(t, test.expectedRows, bat.Vecs[0].Length())
			require.Equal(t, test.expectedRows, bat.Vecs[1].Length())
		})
	}
}

func TestUpdatePersistedDataBatchBroadcastsConstantCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const rowCount = 3
	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{"value", catalog.Row_ID, objectio.DefaultCommitTS_Attr, objectio.DefaultAbort_Attr})
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	var err error
	bat.Vecs[2], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(100, 0), rowCount, mp,
	)
	require.NoError(t, err)
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	blk := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	for row, aborted := range []bool{false, true, false} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[1], types.NewRowid(blk, uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], aborted, false, mp))
	}
	bat.SetRowCount(rowCount)
	defer bat.Clean(mp)

	layout := objectio.SpecialColumnLayout{
		PhysicalAddr: 1,
		CommitTS:     2,
		Abort:        3,
	}
	require.NoError(t, updatePersistedDataBatch(
		bat,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
		blk,
		layout,
		true,
		mp,
	))
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int32{1, 3}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1]))
}

func TestUpdatePersistedTombstoneBatchBroadcastsConstantCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const rowCount = 3
	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr,
		objectio.DefaultAbort_Attr,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	var err error
	bat.Vecs[2], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(100, 0), rowCount, mp,
	)
	require.NoError(t, err)
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	blk := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	for row, aborted := range []bool{false, true, false} {
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[0], types.NewRowid(blk, uint32(row)), false, mp,
		))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], aborted, false, mp))
	}
	bat.SetRowCount(rowCount)
	defer bat.Clean(mp)

	layout := objectio.SpecialColumnLayout{CommitTS: 2, Abort: 3}
	require.NoError(t, updatePersistedTombstoneBatch(
		bat,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
		nil,
		false,
		layout,
		true,
		mp,
	))
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, []int32{1, 3}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1]))
}

func TestUpdatePersistedTombstoneBatchRejectsUndeclaredColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_int32.ToType())
	var blockID types.Blockid
	require.NoError(t, vector.AppendFixed(
		bat.Vecs[0], types.NewRowid(&blockID, 0), false, mp,
	))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(100, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], false, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[4], int32(9), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	layout := objectio.SpecialColumnLayout{
		PhysicalAddr: objectio.InvalidSpecialColumnPosition,
		CommitTS:     2,
		Abort:        3,
	}
	err := updatePersistedTombstoneBatch(
		bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, false,
		layout, true, mp,
	)
	require.ErrorContains(t, err, "undeclared column 4")
	require.Len(t, bat.Vecs, 5, "validation failure must leave ownership with the caller")
}

func TestApplyTSFilterForBatchBroadcastsConstantCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const rowCount = 3
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for row := range rowCount {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(row), false, mp))
	}
	var err error
	bat.Vecs[1], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(200, 0), rowCount, mp,
	)
	require.NoError(t, err)
	bat.SetRowCount(rowCount)
	defer bat.Clean(mp)

	require.NoError(t, applyTSFilterForBatch(
		bat,
		1,
		nil,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
	))
	require.Zero(t, bat.Vecs[0].Length())
	require.Zero(t, bat.Vecs[1].Length())
}

func TestApplyTSFilterForBatchRejectsNilLeadingColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(2)
	var err error
	bat.Vecs[1], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(100, 0), 1, mp,
	)
	require.NoError(t, err)
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.ErrorContains(t, applyTSFilterForBatch(
		bat, 1, nil, types.BuildTS(50, 0), types.BuildTS(150, 0),
	), "nil leading column")
}

func TestValidateSyntheticRowIDCountRejectsOffsetOverflow(t *testing.T) {
	require.Error(t, validateSyntheticRowIDCount(-1))
	require.NoError(t, validateSyntheticRowIDCount(int(objectio.BlockMaxRows)))
	if ^uint(0) > uint(^uint32(0)) {
		tooManyRows := int(uint64(^uint32(0)) + 1)
		require.ErrorContains(t, validateSyntheticRowIDCount(tooManyRows), "invalid block row count")
	}
}

func TestFilterBatchBroadcastsConstantCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newBatch := func(keys []int32, commitTS types.TS) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		for _, key := range keys {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, mp))
		}
		var err error
		bat.Vecs[1], err = vector.NewConstFixed(
			types.T_TS.ToType(), commitTS, len(keys), mp,
		)
		require.NoError(t, err)
		bat.SetRowCount(len(keys))
		return bat
	}

	data := newBatch([]int32{1, 2}, types.BuildTS(10, 0))
	tombstone := newBatch([]int32{3, 4}, types.BuildTS(20, 0))
	defer data.Clean(mp)
	defer tombstone.Clean(mp)

	require.NoError(t, filterBatch(data, tombstone, 0, false, false))
	require.Equal(t, 2, data.RowCount())
	require.Equal(t, 2, tombstone.RowCount())
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
			changeObjectIdentity(obj): {
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
	key := changeObjectIdentity(obj)

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
				changeObjectIdentity(obj): {
					initialized:      true,
					evaluable:        true,
					shouldReadByBlks: []bool{true},
				},
			},
		}
		ok, err := handle.shouldReadBlock(context.Background(), obj, 1)
		require.ErrorContains(t, err, "outside planned block range")
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
			changeObjectIdentity(obj): {
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
	aobj, cnObj, tnByCreateTS := handle.getObjectEntries(
		tree.Iter(),
		types.BuildTS(5, 0),
		types.BuildTS(20, 0),
	)

	require.Len(t, aobj, 1)
	require.Equal(t, appendable.ObjectShortName().ShortString(), aobj[0].ObjectShortName().ShortString())
	require.Len(t, cnObj, 1)
	require.Equal(t, cnCreated.ObjectShortName().ShortString(), cnObj[0].ObjectShortName().ShortString())
	require.Len(t, tnByCreateTS[types.BuildTS(4, 0)], 1)
	require.Len(t, tnByCreateTS[types.BuildTS(9, 0)], 1)
}

func TestTombstoneObjectPruningDoesNotComparePhysicalAndUserRowIDs(t *testing.T) {
	start, end := types.BuildTS(5, 0), types.BuildTS(20, 0)
	obj := makeTestObjectEntry(t, 1, true, false, types.BuildTS(10, 0))

	physicalBlock := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	physicalRowID := types.NewRowid(physicalBlock, 1)
	physicalZM := index.NewZM(types.T_Rowid, 0)
	index.UpdateZM(physicalZM, physicalRowID[:])
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&obj.ObjectStats, physicalZM))

	userPKBlock := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	userPK := types.NewRowid(userPKBlock, 7)
	userPKSegment := index.NewZM(types.T_Rowid, 0)
	index.UpdateZM(userPKSegment, userPK[:])
	filter := &engine.PKFilter{
		Segments:     [][]byte{append([]byte(nil), userPKSegment...)},
		ObjectZMIsPK: true,
	}

	newTree := func() *btree.BTreeG[objectio.ObjectEntry] {
		tree := btree.NewBTreeGOptions(
			objectio.ObjectEntry.ObjectNameIndexLess, btree.Options{Degree: 8},
		)
		tree.Set(*obj)
		return tree
	}

	dataHandle := &baseHandle{changesHandle: &ChangeHandler{pkFilter: filter}}
	dataIter := newTree().Iter()
	dataObjects, _, _ := dataHandle.getObjectEntries(dataIter, start, end)
	dataIter.Release()
	require.Empty(t, dataObjects, "data sort-key pruning should still reject a disjoint ROWID PK")

	clusterFilter := &engine.PKFilter{
		Segments: [][]byte{append([]byte(nil), userPKSegment...)},
		// ObjectZMIsPK intentionally remains false: the ROWID zonemap can be a
		// cluster-by key with the same physical type as the user primary key.
	}
	clusterHandle := &baseHandle{changesHandle: &ChangeHandler{pkFilter: clusterFilter}}
	clusterIter := newTree().Iter()
	clusterObjects, _, _ := clusterHandle.getObjectEntries(clusterIter, start, end)
	clusterIter.Release()
	require.Len(t, clusterObjects, 1,
		"a semantically different sort-key zonemap must not prune user-PK changes")

	tombstoneHandle := &baseHandle{
		changesHandle: &ChangeHandler{pkFilter: filter},
		isTombstone:   true,
	}
	tombstoneIter := newTree().Iter()
	tombstoneObjects, _, _ := tombstoneHandle.getObjectEntries(tombstoneIter, start, end)
	tombstoneIter.Release()
	require.Len(t, tombstoneObjects, 1,
		"tombstone sort key is a physical rowid, not the user ROWID primary key")
}

func TestGetObjectEntriesKeepsPKDisjointTNSuccessorForDeleteChain(t *testing.T) {
	start, end := types.BuildTS(5, 0), types.BuildTS(20, 0)
	deleteTS := types.BuildTS(10, 0)

	predecessor := makeTestObjectEntry(t, 1, true, false, types.BuildTS(3, 0))
	predecessor.DeleteTime = deleteTS
	successor := makeTestObjectEntry(t, 1, false, false, deleteTS)

	predecessorZM := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(predecessorZM, int64(100))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&predecessor.ObjectStats, predecessorZM))
	successorZM := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(successorZM, int64(200))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&successor.ObjectStats, successorZM))

	segment := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(segment, int64(100))
	filter := &engine.PKFilter{
		Segments:     [][]byte{append([]byte(nil), segment...)},
		ObjectZMIsPK: true,
	}
	stubFS := &stubStatFileFS{existing: map[string]struct{}{
		successor.ObjectName().String(): {},
	}}
	base := &baseHandle{changesHandle: &ChangeHandler{
		pkFilter:                 filter,
		enableDeleteChainResolve: true,
		fs:                       stubFS,
	}}

	tree := btree.NewBTreeGOptions(
		objectio.ObjectEntry.ObjectNameIndexLess,
		btree.Options{Degree: 8},
	)
	tree.Set(*predecessor)
	tree.Set(*successor)
	iter := tree.Iter()
	aobjs, _, tnByCreateTS := base.getObjectEntries(iter, start, end)
	iter.Release()

	require.Len(t, aobjs, 1)
	require.Len(t, tnByCreateTS[deleteTS], 1,
		"a PK-disjoint TN successor is structural delete-chain metadata")
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(), start, end, aobjs, tnByCreateTS, false, "data",
	)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, successor.ObjectShortName().ShortString(), resolved[0].ObjectShortName().ShortString())
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

func TestPKFilterSegmentValidationAndFailOpen(t *testing.T) {
	first := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(first, int64(1))
	second := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(second, int64(2))
	require.True(t, changePKFilterSegmentsValid([][]byte{first, second}))
	require.False(t, changePKFilterSegmentsValid([][]byte{{1}}))
	require.False(t, changePKFilterSegmentsValid([][]byte{second, first}),
		"out-of-order segments violate the binary-search contract")
	require.False(t, changePKFilterSegmentsValid([][]byte{first, first}),
		"overlapping segments violate the binary-search contract")

	malformedObjectZM := append(objectio.ZoneMap(nil), first...)
	malformedObjectZM[30] = 0 // corrupt the encoded minimum bound length
	require.False(t, changeZoneMapDisjoint(malformedObjectZM, [][]byte{second}),
		"malformed pruning metadata must fail open")

	handle := &ChangeHandler{pkFilter: &engine.PKFilter{
		Segments:     [][]byte{{1}},
		ObjectZMIsPK: true,
	}}
	require.NoError(t, handle.applyRequestSchema(context.Background()))
	require.Nil(t, handle.pkFilter,
		"a malformed optional filter with no valid replay hint should be disabled")
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
	t.Run("exact match", func(t *testing.T) {
		next := lookupDeleteChainSuccessor(ts10, tnByCreateTS)
		require.Len(t, next, 1)
	})
	t.Run("does not attach a later unrelated compaction", func(t *testing.T) {
		next := lookupDeleteChainSuccessor(types.BuildTS(12, 0), tnByCreateTS)
		require.Nil(t, next)
	})
	t.Run("no match past last key", func(t *testing.T) {
		next := lookupDeleteChainSuccessor(ts20, tnByCreateTS)
		require.Nil(t, next)
	})
	t.Run("empty map", func(t *testing.T) {
		next := lookupDeleteChainSuccessor(ts5, nil)
		require.Nil(t, next)
	})
	t.Run("does not match before first key", func(t *testing.T) {
		next := lookupDeleteChainSuccessor(ts5, tnByCreateTS)
		require.Nil(t, next)
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

func TestChangeObjectIdentityDoesNotUseDisplayShortString(t *testing.T) {
	var firstID, secondID objectio.ObjectId
	firstID[0] = 1
	secondID[0] = 2
	for i := 10; i < len(firstID); i++ {
		firstID[i] = byte(i)
		secondID[i] = byte(i)
	}
	first := &objectio.ObjectEntry{
		ObjectStats: *objectio.NewObjectStatsWithObjectID(&firstID, false, false, false),
		CreateTime:  types.BuildTS(1, 0),
	}
	second := &objectio.ObjectEntry{
		ObjectStats: *objectio.NewObjectStatsWithObjectID(&secondID, false, false, false),
		CreateTime:  types.BuildTS(2, 0),
	}

	require.Equal(t, first.ObjectShortName().ShortString(), second.ObjectShortName().ShortString())
	require.NotEqual(t, changeObjectIdentity(first), changeObjectIdentity(second))
	aobjs, cnObjs := classifyResolvedObjects([]*objectio.ObjectEntry{first, second})
	require.Len(t, aobjs, 2)
	require.Empty(t, cnObjs)
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
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{missing},
		tnByCreateTS,
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
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
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
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
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
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil, // no visible objects -> orphan sweep should find the TN object
		tnByCreateTS,
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
	resolved, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		nil,
		tnByCreateTS,
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
		nil, // empty TN map → no successor
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestResolveVisibleObjectsByDeleteChain_RejectsLaterUnrelatedSuccessor(t *testing.T) {
	// A later create timestamp does not prove that this object replaced deleted.
	deleted := makeTestObjectEntry(t, 1, false, false, types.BuildTS(3, 0))
	deleted.DeleteTime = types.BuildTS(7, 0)
	// This may belong to a completely unrelated compaction.
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
	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{deleted},
		tnByCreateTS,
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
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
		nil, // no TN entries
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestResolveVisibleObjectsByDeleteChain_MissingRejectsLaterUnrelatedReplacement(t *testing.T) {
	// A missing file must not be replaced without an exact transaction match.
	missing := makeTestObjectEntry(t, 1, true, false, types.BuildTS(2, 0))
	missing.DeleteTime = types.BuildTS(5, 0)
	// This later object has no lineage relationship to missing.
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
	_, err := base.resolveVisibleObjectsByDeleteChain(
		context.Background(),
		types.BuildTS(1, 0),
		types.BuildTS(10, 0),
		[]*objectio.ObjectEntry{missing},
		tnByCreateTS,
		false,
		"data",
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
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
		nil,
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
		nil,
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
		nil,
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

	t.Run("range_tn_created_before_start_can_contain_rows_in_range", func(t *testing.T) {
		obj := *makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		kind := classifyCheckpointObject(obj, false, start, end, checkpointObjectSelectionRange)
		require.Equal(t, checkpointObjectKindRowCommitTS, kind)
	})

	t.Run("range_tn_deleted_before_start", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(1, 0))
		obj.DeleteTime = types.BuildTS(5, 0)
		kind := classifyCheckpointObject(*obj, true, start, end, checkpointObjectSelectionRange)
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
	require.False(t, checkTS(start, types.MaxTs(), types.MaxTs()))
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
	plan, exists := h.blockPlans[changeObjectIdentity(obj)]
	require.True(t, exists)
	require.False(t, plan.initialized, "failed metadata load must remain retryable")
	ok, err = h.shouldReadBlock(context.Background(), obj, 0)
	require.Error(t, err)
	require.False(t, ok)
	require.False(t, plan.initialized)
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
	aobj, cnObj, tnByCreateTS := p.getObjectEntries(iter, start, end)
	iter.Release()

	require.Len(t, aobj, 1)
	require.Len(t, cnObj, 1)
	require.Len(t, tnByCreateTS, 1)
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
		key := changeObjectIdentity(obj)
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
		key := changeObjectIdentity(obj)
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
		key := changeObjectIdentity(obj)
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

	t.Run("block index out of range returns error", func(t *testing.T) {
		obj := makeTestObjectEntry(t, 1, false, false, types.BuildTS(5, 0))
		key := changeObjectIdentity(obj)
		h.blockPlans[key] = &aobjBlockPlan{
			initialized:      true,
			evaluable:        true,
			shouldReadByBlks: []bool{true},
		}
		ok, err := h.shouldReadBlock(context.Background(), obj, 5) // out of range
		require.ErrorContains(t, err, "outside planned block range")
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
	key := changeObjectIdentity(obj)
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

func writeTestObjectForPKBlockPruning(
	t *testing.T,
	mp *mpool.MPool,
	seqnums []uint16,
	vecs []*vector.Vector,
	isTombstone bool,
) (*objectio.ObjectEntry, fileservice.FileService) {
	t.Helper()
	require.NotEmpty(t, vecs)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)

	writer := ioutil.ConstructWriter(0, seqnums, -1, false, isTombstone, fs)
	bat := batch.NewWithSize(len(vecs))
	copy(bat.Vecs, vecs)
	bat.SetRowCount(vecs[0].Length())
	defer bat.Clean(mp)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, blocks)

	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, uint32(len(blocks))))
	return &objectio.ObjectEntry{
		ObjectStats: stats,
		CreateTime:  types.BuildTS(50, 0),
	}, fs
}

func TestBuildBlockPlanUsesPKFilterPhysicalSeqnum(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	first := vector.NewVec(types.T_int64.ToType())
	pk := vector.NewVec(types.T_int64.ToType())
	rowID := vector.NewVec(types.T_Rowid.ToType())
	commitTS := vector.NewVec(types.T_TS.ToType())
	var blk types.Blockid
	require.NoError(t, vector.AppendFixed(first, int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(pk, int64(100), false, mp))
	require.NoError(t, vector.AppendFixed(rowID, types.NewRowid(&blk, 0), false, mp))
	require.NoError(t, vector.AppendFixed(commitTS, types.BuildTS(100, 0), false, mp))
	obj, fs := writeTestObjectForPKBlockPruning(
		t,
		mp,
		[]uint16{0, 5, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS},
		[]*vector.Vector{first, pk, rowID, commitTS},
		false,
	)

	segment := index.NewZM(types.T_int64, 0)
	index.UpdateZMAny(segment, int64(100))
	filter := &engine.PKFilter{
		Segments:      [][]byte{append([]byte(nil), segment...)},
		PrimarySeqnum: 5,
	}
	h := &AObjectHandle{
		p: &baseHandle{changesHandle: &ChangeHandler{
			// Deliberately different: this field also serves logical batch
			// reconciliation and must not select the metadata column.
			primarySeqnum: 0,
			pkFilter:      filter,
		}},
		fs:         fs,
		start:      types.BuildTS(50, 0),
		end:        types.BuildTS(150, 0),
		blockPlans: make(map[string]*aobjBlockPlan),
	}
	plan := &aobjBlockPlan{}
	require.NoError(t, h.buildBlockPlan(context.Background(), obj, plan))
	require.True(t, plan.shouldReadByBlks[0],
		"the PK at physical sequence 5 overlaps even though sequence 0 does not")
	require.Equal(t, 1, plan.overlapBlocks)
}

func TestBuildBlockPlanTombstoneUsesUserPKInsteadOfTargetRowID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	physicalBlock := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	physicalRowID := types.NewRowid(physicalBlock, 1)
	userPKBlock := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	userPK := types.NewRowid(userPKBlock, 7)
	physical := vector.NewVec(types.T_Rowid.ToType())
	pk := vector.NewVec(types.T_Rowid.ToType())
	commitTS := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(physical, physicalRowID, false, mp))
	require.NoError(t, vector.AppendFixed(pk, userPK, false, mp))
	require.NoError(t, vector.AppendFixed(commitTS, types.BuildTS(100, 0), false, mp))
	obj, fs := writeTestObjectForPKBlockPruning(
		t,
		mp,
		[]uint16{
			objectio.TombstoneAttr_Rowid_SeqNum,
			objectio.TombstoneAttr_PK_SeqNum,
			objectio.SEQNUM_COMMITTS,
		},
		[]*vector.Vector{physical, pk, commitTS},
		true,
	)

	segment := index.NewZM(types.T_Rowid, 0)
	index.UpdateZM(segment, userPK[:])
	h := &AObjectHandle{
		p: &baseHandle{
			changesHandle: &ChangeHandler{pkFilter: &engine.PKFilter{
				Segments:      [][]byte{append([]byte(nil), segment...)},
				PrimarySeqnum: 0,
			}},
			isTombstone: true,
		},
		isTombstone: true,
		fs:          fs,
		start:       types.BuildTS(50, 0),
		end:         types.BuildTS(150, 0),
		blockPlans:  make(map[string]*aobjBlockPlan),
	}
	plan := &aobjBlockPlan{}
	require.NoError(t, h.buildBlockPlan(context.Background(), obj, plan))
	require.True(t, plan.shouldReadByBlks[0],
		"the user ROWID PK overlaps even though the target physical ROWID does not")
	require.Equal(t, 1, plan.overlapBlocks)
}

func writeTestObjectWithAbort(
	t *testing.T,
	mp *mpool.MPool,
	tsValues []types.TS,
	abortValues []bool,
) (*objectio.ObjectEntry, fileservice.FileService) {
	t.Helper()
	require.Len(t, abortValues, len(tsValues))
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)

	writer := ioutil.ConstructWriter(
		0,
		[]uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
		-1, false, false, fs,
	)
	writer.SetAppendable()
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	defer bat.Clean(mp)

	var blk types.Blockid
	for i, ts := range tsValues {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[1], types.NewRowid(&blk, uint32(i+1)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], ts, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], abortValues[i], false, mp))
	}
	bat.SetRowCount(len(tsValues))
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, blocks)

	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, uint32(len(blocks))))
	return &objectio.ObjectEntry{
		ObjectStats: stats,
		CreateTime:  types.BuildTS(50, 0),
	}, fs
}

func TestAObjectHandlePersistedAbortColumnIsHidden(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	obj, fs := writeTestObjectWithAbort(
		t,
		mp,
		[]types.TS{types.BuildTS(100, 0), types.BuildTS(110, 0)},
		[]bool{false, true},
	)
	scheduler := tasks.NewParallelJobScheduler(1)
	defer scheduler.Stop()
	handle := NewAObjectHandle(
		context.Background(),
		&baseHandle{changesHandle: &ChangeHandler{scheduler: scheduler}},
		false,
		types.BuildTS(50, 0),
		types.BuildTS(150, 0),
		[]*objectio.ObjectEntry{obj},
		fs,
		mp,
	)
	require.NoError(t, handle.init(context.Background(), false))
	require.NotNil(t, handle.currentBatch)
	defer handle.currentBatch.Clean(mp)

	require.Len(t, handle.currentBatch.Vecs, 2)
	require.Equal(t, types.T_int32, handle.currentBatch.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, handle.currentBatch.Vecs[1].GetType().Oid)
	require.Equal(t, 1, handle.currentBatch.RowCount())
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](handle.currentBatch.Vecs[0], 0))
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

	commitPos, ok := objectio.ResolveSpecialColumnLayout(blk).Resolve(objectio.SEQNUM_COMMITTS)
	require.True(t, ok)
	commitZM := blk.ColumnMeta(commitPos).ZoneMap()
	commitZM[30] = 1 // TS bounds must contain the complete 12-byte value.
	overlap, evaluable, reason, _ := blockCommitTSOverlapsRange(
		blk, types.BuildTS(1, 0), types.BuildTS(400, 0),
	)
	require.False(t, overlap)
	require.False(t, evaluable)
	require.Equal(t, "zonemap_invalid_bounds", reason)
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

func TestUpdateCNTombstoneBatch_NilGuard(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("nil batch", func(t *testing.T) {
		err := updateCNTombstoneBatch(
			nil, types.BuildTS(1, 0), nil, noSpecialColumnLayout(), false, mp,
		)
		require.Error(t, err)
	})

	t.Run("missing pk vector", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
		defer bat.Clean(mp)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTS(1, 0), false, mp))
		err := updateCNTombstoneBatch(
			bat, types.BuildTS(2, 0), nil, noSpecialColumnLayout(), false, mp,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing pk vector")
	})
}

func TestUpdateCNDataBatch_NilGuard(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("nil batch", func(t *testing.T) {
		err := updateCNDataBatch(
			nil, types.BuildTS(1, 0), nil, noSpecialColumnLayout(), false, mp,
		)
		require.Error(t, err)
	})

	t.Run("only ts column - no vectors after strip", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
		defer bat.Clean(mp)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTS(1, 0), false, mp))
		layout := noSpecialColumnLayout()
		layout.CommitTS = 0
		err := updateCNDataBatch(bat, types.BuildTS(2, 0), nil, layout, false, mp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no user vectors")
	})
}

// TestCDCSchema_NoRowIDWhenRetainRowIDFalse locks the CDC-facing batch shapes:
// the disttae.NewChangesHandler path leaves retainRowID=false, and every batch
// mutator must produce output that contains no T_Rowid column. Regressions here
// would silently break CDC sinks that index columns by position.
func TestCDCSchema_NoRowIDWhenRetainRowIDFalse(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	assertNoRowID := func(t *testing.T, bat *batch.Batch) {
		t.Helper()
		for i, vec := range bat.Vecs {
			require.NotEqual(t, types.T_Rowid, vec.GetType().Oid,
				"CDC batch must not contain T_Rowid vec at idx=%d attrs=%v", i, bat.Attrs)
		}
		for _, attr := range bat.Attrs {
			require.NotEqual(t, catalog.Row_ID, attr,
				"CDC batch must not contain Row_ID attr; attrs=%v", bat.Attrs)
		}
	}

	t.Run("updateTombstoneBatch drops input rowid vec", func(t *testing.T) {
		bat := batch.NewWithSize(3)
		// Simulate a tombstone batch that happens to carry a rowid column.
		ridVec := vector.NewVec(types.T_Rowid.ToType())
		blk := types.Blockid{}
		require.NoError(t, vector.AppendFixed(ridVec, types.NewRowid(&blk, 0), false, mp))
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pkVec, int64(7), false, mp))
		tsVec := vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp))
		bat.Vecs[0] = ridVec
		bat.Vecs[1] = pkVec
		bat.Vecs[2] = tsVec
		bat.SetRowCount(1)

		require.NoError(t, updateTombstoneBatch(
			bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, false, nil, nil, false, mp))

		require.Equal(t, []string{
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		}, bat.Attrs)
		require.Equal(t, 2, len(bat.Vecs))
		assertNoRowID(t, bat)
		bat.Clean(mp)
	})

	t.Run("persisted tombstone drops and filters abort vec", func(t *testing.T) {
		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
		var blk types.Blockid
		for i, aborted := range []bool{false, true} {
			require.NoError(t, vector.AppendFixed(
				bat.Vecs[0], types.NewRowid(&blk, uint32(i)), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(i+1), false, mp))
			require.NoError(t, vector.AppendFixed(
				bat.Vecs[2], types.BuildTS(int64(100+i), 0), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[3], aborted, false, mp))
		}
		bat.SetRowCount(2)
		layout := objectio.SpecialColumnLayout{
			PhysicalAddr: objectio.InvalidSpecialColumnPosition,
			CommitTS:     2,
			Abort:        3,
		}
		require.NoError(t, updateTombstoneBatch(
			bat,
			types.BuildTS(50, 0),
			types.BuildTS(150, 0),
			nil,
			false,
			nil,
			&layout,
			false,
			mp,
		))
		require.Len(t, bat.Vecs, 2)
		require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
		require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
		require.Equal(t, 1, bat.RowCount())
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
		bat.Clean(mp)
	})

	t.Run("updateDataBatch drops input rowid vec", func(t *testing.T) {
		bat := batch.NewWithSize(4)
		bat.SetAttributes([]string{catalog.Row_ID, "a", "b", objectio.DefaultCommitTS_Attr})
		ridVec := vector.NewVec(types.T_Rowid.ToType())
		blk := types.Blockid{}
		require.NoError(t, vector.AppendFixed(ridVec, types.NewRowid(&blk, 0), false, mp))
		aVec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(aVec, int32(1), false, mp))
		bVec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(bVec, int32(10), false, mp))
		tsVec := vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp))
		bat.Vecs[0] = ridVec
		bat.Vecs[1] = aVec
		bat.Vecs[2] = bVec
		bat.Vecs[3] = tsVec
		bat.SetRowCount(1)

		require.NoError(t, updateDataBatch(
			bat, types.BuildTS(50, 0), types.BuildTS(150, 0), nil, nil, false, mp))

		assertNoRowID(t, bat)
		// Trailing column must remain commit_ts.
		require.Equal(t, types.T_TS, bat.Vecs[len(bat.Vecs)-1].GetType().Oid)
		require.Equal(t, objectio.DefaultCommitTS_Attr, bat.Attrs[len(bat.Attrs)-1])
		bat.Clean(mp)
	})

	t.Run("fillInDeleteBatch produces 2-col layout", func(t *testing.T) {
		// RowEntry.Batch layout expected by fillInDeleteBatch: [Rowid, TS, pk]
		src := batch.NewWithSize(3)
		ridVec := vector.NewVec(types.T_Rowid.ToType())
		blk := types.Blockid{}
		require.NoError(t, vector.AppendFixed(ridVec, types.NewRowid(&blk, 0), false, mp))
		tsSrc := vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(tsSrc, types.BuildTS(100, 0), false, mp))
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pkVec, int64(42), false, mp))
		src.Vecs[0] = ridVec
		src.Vecs[1] = tsSrc
		src.Vecs[2] = pkVec
		src.SetRowCount(1)

		// A recovered/synthetic RowEntry may not carry its redundant Time key. The
		// validated source commit-ts must still be preserved in the output.
		entry := &RowEntry{Batch: src, Offset: 0}
		var out *batch.Batch
		require.NoError(t, fillInDeleteBatch(&out, entry, false, mp))
		require.NotNil(t, out)
		require.Equal(t, []string{
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		}, out.Attrs)
		require.Equal(t, 2, len(out.Vecs))
		assertNoRowID(t, out)
		require.Equal(t, types.BuildTS(100, 0),
			vector.GetFixedAtNoTypeCheck[types.TS](out.Vecs[1], 0))
		out.Clean(mp)
		src.Clean(mp)
	})

	t.Run("fillInInsertBatch omits rowid column", func(t *testing.T) {
		// RowEntry.Batch layout: [Rowid, TS, pk, val]; src.Attrs[2:] = [pk, val]
		src := batch.NewWithSize(4)
		src.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "pk", "val"})
		ridVec := vector.NewVec(types.T_Rowid.ToType())
		blk := types.Blockid{}
		require.NoError(t, vector.AppendFixed(ridVec, types.NewRowid(&blk, 0), false, mp))
		tsSrc := vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(tsSrc, types.BuildTS(100, 0), false, mp))
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pkVec, int64(7), false, mp))
		valVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(valVec, int64(70), false, mp))
		src.Vecs[0] = ridVec
		src.Vecs[1] = tsSrc
		src.Vecs[2] = pkVec
		src.Vecs[3] = valVec
		src.SetRowCount(1)

		entry := &RowEntry{Batch: src, Offset: 0, Time: types.BuildTS(100, 0)}
		var out *batch.Batch
		require.NoError(t, fillInInsertBatch(&out, entry, false, mp))
		require.NotNil(t, out)
		require.Equal(t, []string{"pk", "val", objectio.DefaultCommitTS_Attr}, out.Attrs)
		require.Equal(t, 3, len(out.Vecs))
		assertNoRowID(t, out)
		// Trailing column must be commit_ts.
		require.Equal(t, types.T_TS, out.Vecs[len(out.Vecs)-1].GetType().Oid)
		out.Clean(mp)
		src.Clean(mp)
	})
}
