// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi_update

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMetadataPreservesDeleteBeforeInsert(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	w := &s3WriterDelegate{updateCtxs: []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}}}}
	w.memController.throttler = &admissionThrottler{capacity: 1 << 30}
	defer w.free(proc)
	src := batch.NewWithSize(1)
	defer src.Clean(proc.Mp())
	src.Vecs[0] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
	src.SetRowCount(1)
	for i, action := range []actionType{actionInsert, actionDelete, actionInsert, actionDelete} {
		require.NoError(t, w.appendMetadata(proc, action, 0, uint64(i+1), "", src))
	}
	var actions []uint8
	var rows []uint64
	for {
		require.NoError(t, w.readMetadataFrame(proc))
		if w.outputBat == nil {
			break
		}
		actions = append(actions, vector.MustFixedColNoTypeCheck[uint8](w.outputBat.Vecs[0])...)
		rows = append(rows, vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2])...)
	}
	require.Equal(t, []uint8{uint8(actionDelete), uint8(actionDelete), uint8(actionInsert), uint8(actionInsert)}, actions)
	require.Equal(t, []uint64{2, 4, 1, 3}, rows)
}

func TestS3WriterZeroDataCreditsSpillsUsingWorkspace(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer proc.Free()
	defer proc.GetFileService().Close(context.Background())
	ctx, cancel := context.WithTimeout(proc.Ctx, 10*time.Second)
	defer cancel()
	proc.Ctx = ctx
	owner := rscthrottler.NewMemThrottler("writer-working-set", 1, rscthrottler.WithConstLimit(8<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1 << 20, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 8 << 20}, {Name: "output", Count: 1, Bytes: 8 << 20}}, IOBytes: 128 << 20}})
	require.NoError(t, err)
	defer ws.Close()
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: 1 << 20, SourceBytes: 8 << 20, OutputBytes: 8 << 20, IOBytes: 128 << 20}
	tc := buildTestCase(prepareTestInsertMultiUpdateCtx(false, false), prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
	defer tc.op.Free(proc, false, nil)
	require.NoError(t, tc.op.Prepare(proc))
	w := tc.op.ctr.s3Writer
	deny := &admissionThrottler{}
	deny.beforeWait = func(*admissionThrottler) {
		t.Fatal("a borrowed input must spill without waiting for ordinary data credits")
	}
	w.memController.throttler = deny
	bat := batch.NewWithSize(4)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{2, 1}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"b", "a"}, nil, proc.Mp())
	bat.Vecs[2] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	bat.Vecs[3] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	bat.SetRowCount(2)
	require.NoError(t, w.append(proc, tc.op.OpAnalyzer, bat))
	require.Zero(t, deny.granted)
	require.Zero(t, w.retainedBytes(), "D never owns the destructible W clone or encoded metadata")
	var rows uint64
	for {
		require.NoError(t, w.flushTailAndWriteToOutput(proc, tc.op.OpAnalyzer))
		if w.outputBat == nil {
			break
		}
		for _, n := range vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]) {
			rows += n
		}
	}
	require.Equal(t, uint64(2), rows)
	require.NoError(t, w.free(proc))
	// Every runner returned W; a second independent operation can use it.
	lease, err := ws.AcquireIO(ctx, 128<<20)
	require.NoError(t, err)
	lease.Release()
}

type blockedWriterFS struct {
	fileservice.FileService
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *blockedWriterFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.once.Do(func() { close(fs.entered) })
	select {
	case <-fs.release:
		return fs.FileService.Write(ctx, v)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func TestS3WriterDefaultImportPipelinesAllProjections(t *testing.T) {
	for _, capacity := range []int64{0, 1 << 20} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) { checkS3WriterPipeline(t, capacity, false, false) })
	}
	t.Run("borrowed-reset", func(t *testing.T) { checkS3WriterPipeline(t, 0, true, false) })
	t.Run("borrowed-to-retained", func(t *testing.T) { checkS3WriterPipeline(t, 0, false, true) })
}

func checkS3WriterPipeline(t *testing.T, capacity int64, reset, resume bool) {
	_, ctrl, proc := prepareTestCtx(t, true)
	fs := proc.GetFileService()
	t.Cleanup(func() { fs.Close(context.Background()) })
	t.Cleanup(proc.Free)
	ctx, cancel := context.WithTimeout(proc.Ctx, 10*time.Second)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	require.Nil(t, ctx.Value(ioutil.PipelineFlushKey))
	blocked := &blockedWriterFS{FileService: fs, entered: make(chan struct{}), release: make(chan struct{})}
	proc.Base.FileService = blocked
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	owner := rscthrottler.NewMemThrottler("projection-upload", 1, rscthrottler.WithConstLimit(8<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{
		UnitBytes: 1 << 20, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 8 << 20}, {Name: "output", Count: 1, Bytes: 8 << 20}}, IOBytes: 128 << 20,
	}})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: 1 << 20, OutputBytes: 8 << 20, IOBytes: 128 << 20}
	tc := buildTestCase(prepareTestInsertMultiUpdateCtx(true, true), prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
	t.Cleanup(func() { tc.op.Free(proc, false, nil) })
	t.Cleanup(unblock)
	t.Cleanup(cancel)
	require.NoError(t, tc.op.Prepare(proc))
	w := tc.op.ctr.s3Writer
	credits := &admissionThrottler{capacity: capacity}
	w.memController.throttler = credits
	b := batch.NewWithSize(6)
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{2, 1}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeVarcharVector([]string{"b", "a"}, nil, proc.Mp())
	b.Vecs[2] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	b.Vecs[3] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	b.Vecs[4] = testutil.MakeVarcharVector([]string{"u2", "u1"}, nil, proc.Mp())
	b.Vecs[5] = testutil.MakeVarcharVector([]string{"s2", "s1"}, nil, proc.Mp())
	b.SetRowCount(2)
	// Reserve one parent plus two waves of independent projection uploads.
	var parent, uploads int64
	for _, update := range w.updateCtxs {
		projection := b.SelectColumns(update.InsertCols, nil)
		peak, upload, err := ioutil.BatchIOEnvelope(projection, w.writerUnit(proc))
		require.NoError(t, err)
		parent = max(parent, peak)
		uploads += upload
	}
	require.NoError(t, ws.TryGrow(ctx, rscthrottler.WorkingSetSpec{
		UnitBytes: ws.UnitBytes(), Inputs: []rscthrottler.InputSlotSpec{
			{Name: "csv", Count: 1, Bytes: ws.InputCapacity(0)},
			{Name: "output", Count: 1, Bytes: ws.InputCapacity(1)},
		}, IOBytes: max(ws.IOBytes(), parent+2*uploads),
	}))
	require.NoError(t, w.append(proc, tc.op.OpAnalyzer, b))
	ids := vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])
	ids[0], ids[1] = 4, 3
	require.NoError(t, vector.SetBytesAt(b.Vecs[4], 0, []byte("u4"), proc.Mp()))
	require.NoError(t, vector.SetBytesAt(b.Vecs[4], 1, []byte("u3"), proc.Mp()))
	if resume {
		credits.capacity = 1 << 20
	}
	require.NoError(t, w.append(proc, tc.op.OpAnalyzer, b), "a cold D retry must not join ready uploads")
	if resume {
		require.Positive(t, w.ownedMP.AllocatedBytes(), "resumed D buffers must belong to the persistent operator pool")
	}
	require.NoError(t, w.submitInsertSinkers(proc, tc.op.OpAnalyzer))
	if capacity == 0 && !resume {
		require.Zero(t, w.retainedBytes(), "ready borrowed jobs own private W pools")
	}
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	require.Equal(t, [2]*batch.Batch{}, w.metadata)
	inputRows := uint64(12)
	if reset {
		require.ErrorIs(t, w.reset(proc), context.Canceled)
		require.Zero(t, w.retainedBytes())
		available, err := ws.AcquireIO(ctx, ws.IOBytes())
		require.NoError(t, err)
		available.Release()
		inputRows = 6
	}
	unblock()
	if reset {
		require.NoError(t, w.append(proc, tc.op.OpAnalyzer, b))
	}
	var rows uint64
	var objects int
	for {
		require.NoError(t, w.flushTailAndWriteToOutput(proc, tc.op.OpAnalyzer))
		if w.outputBat == nil {
			break
		}
		objects += w.outputBat.RowCount()
		for _, n := range vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]) {
			rows += n
		}
	}
	require.EqualValues(t, inputRows, rows, "base, unique and regular index retain exactly this generation")
	expectedObjects := 3
	if capacity == 0 && !reset {
		expectedObjects = 6
	}
	require.Equal(t, expectedObjects, objects)
	require.NoError(t, w.free(proc))
	require.Zero(t, credits.reserved)
	require.False(t, credits.invalidRelease)
}

func TestUncommittedDeleteDedupPreservesRowsAcrossWindows(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	w := &s3WriterDelegate{deleteBlockMap: make([]map[types.Blockid]*deleteBlockData, 1), updateCtxs: []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}}}}
	w.memController.throttler = &admissionThrottler{capacity: 1 << 30}
	defer w.free(proc)
	for _, offset := range []uint32{7, 7, 8} {
		var blk types.Blockid
		copy(blk[:], colexec.TxnWorkspaceSegment[:])
		rowID := types.NewRowid(&blk, offset)
		b := batch.NewWithSize(2)
		b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
		b.Vecs[1] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(b.Vecs[0], rowID, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(offset), false, proc.Mp()))
		b.SetRowCount(1)
		out, err := w.prepareDeleteBatches(proc, 0, []*batch.Batch{b}, true)
		require.NoError(t, err)
		require.Empty(t, out)
		require.Empty(t, w.deleteBlockMap[0], "uncommitted block bitmaps cannot grow with imported bytes")
		require.Zero(t, w.retainedBytes())
	}
	var rows uint64
	for {
		require.NoError(t, w.readMetadataFrame(proc))
		if w.outputBat == nil {
			break
		}
		for _, n := range vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]) {
			rows += n
		}
	}
	require.Equal(t, uint64(2), rows, "duplicate row ID across pressure flushes must be published once")
	require.NoError(t, w.reset(proc))
	require.Nil(t, w.deleteKeys[0])
}

func TestDeleteKeySetKeepsStateOnDisk(t *testing.T) {
	s := &deleteKeySet{}
	defer s.close()
	for i := uint64(0); i < 4096; i++ {
		var key types.Rowid
		binary.LittleEndian.PutUint64(key[:], i)
		fresh, err := s.add(context.Background(), key)
		require.NoError(t, err)
		require.True(t, fresh)
	}
	end := s.end
	for i := uint64(0); i < 4096; i++ {
		var key types.Rowid
		binary.LittleEndian.PutUint64(key[:], i)
		fresh, err := s.add(context.Background(), key)
		require.NoError(t, err)
		require.False(t, fresh)
	}
	require.Equal(t, end, s.end)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.add(ctx, types.Rowid{})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, s.close())
	require.NoError(t, s.close())
}

func TestMetadataMemoryGrowthPreservesFramesOnCancel(t *testing.T) {
	proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}}, false)
	owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(8<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{
		UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv"}, {Name: "output", Count: 1, Bytes: 32 << 10}}, IOBytes: 1,
	}})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: 1, OutputBytes: 32 << 10}
	source := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_text.ToType()})
	t.Cleanup(func() { source.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(strings.Repeat("x", 2048)), false, proc.Mp()))
	source.SetRowCount(1)
	for i := range 8 {
		require.NoError(t, w.appendMetadata(proc, actionInsert, 0, uint64(i+1), "", source))
	}
	require.Greater(t, w.outputLease.Capacity(), int64(32<<10), "cumulative backing grows the existing reservation")
	require.LessOrEqual(t, 4*w.metadataPool().AllocatedBytes(), w.outputLease.Capacity())
	before := w.metadataPool().AllocatedBytes()
	parent := proc.Ctx
	ctx, cancel := context.WithCancel(parent)
	cancel()
	proc.Ctx = ctx
	require.ErrorIs(t, w.appendMetadata(proc, actionInsert, 0, 9, "", source), context.Canceled)
	require.Equal(t, before, w.metadataPool().AllocatedBytes())
	proc.Ctx = parent
	for i := range 8 {
		require.NoError(t, w.readMetadataFrame(proc))
		require.Equal(t, []uint64{uint64(i + 1)}, vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]))
	}
	require.NoError(t, w.readMetadataFrame(proc))
	require.Nil(t, w.outputBat)
	require.Equal(t, [2]*batch.Batch{}, w.metadata)
	require.Zero(t, w.metadataPool().AllocatedBytes())
	lease, err := ws.AcquireInput(parent, 1)
	require.NoError(t, err)
	lease.Release()
}

func TestLegacyMetadataRetainsBudgetAcrossFrames(t *testing.T) {
	proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, DeleteCols: []int{0, 1}}}, false)
	credits := &admissionThrottler{capacity: 1}
	w.memController.throttler = credits
	source := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_text.ToType()})
	t.Cleanup(func() { source.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(strings.Repeat("x", 2048)), false, proc.Mp()))
	source.SetRowCount(1)
	var capacity *rscthrottler.WorkingSetCapacityError
	require.ErrorAs(t, w.appendMetadata(proc, actionDelete, 0, 1, objectio.PhysicalAddr_Attr, source), &capacity)
	require.Zero(t, w.metadataPool().AllocatedBytes(), "deny backing before allocation")
	credits.capacity = 1 << 20
	require.NoError(t, w.appendMetadata(proc, actionDelete, 0, 1, objectio.PhysicalAddr_Attr, source))
	require.Positive(t, credits.reserved)
	require.GreaterOrEqual(t, credits.reserved, 4*w.metadataPool().AllocatedBytes(), "legacy metadata retains its existing D owner")
	before := w.metadataPool().AllocatedBytes()
	credits.capacity = credits.reserved
	source.CleanOnlyData()
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte(strings.Repeat("x", 8192)), false, proc.Mp()))
	source.SetRowCount(1)
	require.ErrorAs(t, w.appendMetadata(proc, actionDelete, 0, 2, objectio.PhysicalAddr_Attr, source), &capacity)
	require.Equal(t, before, w.metadataPool().AllocatedBytes(), "denied growth preserves prior frames")
	credits.capacity = 1 << 20
	require.NoError(t, w.readMetadataFrame(proc))
	require.Equal(t, []uint64{1}, vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]))
	require.NoError(t, w.readMetadataFrame(proc))
	require.Nil(t, w.outputBat, "denied frame was never published")
	require.Zero(t, credits.reserved)
	require.False(t, credits.invalidRelease)
}
