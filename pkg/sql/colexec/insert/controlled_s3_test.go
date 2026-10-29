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

package insert

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type controlledTestThrottler struct {
	t        *testing.T
	mu       sync.Mutex
	grants   int
	acquired int64
	released int64
	balance  int64
	attempt  chan struct{}
	once     sync.Once
}

func (t *controlledTestThrottler) Acquire(n int64) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.attempt != nil {
		t.once.Do(func() { close(t.attempt) })
	}
	if t.grants == 0 {
		return 0, false
	}
	t.grants--
	t.acquired += n
	t.balance += n
	return 0, true
}
func (t *controlledTestThrottler) Release(n int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.released += n
	t.balance -= n
	return t.balance
}
func (t *controlledTestThrottler) Available() int64 { return 21 << 30 }
func (t *controlledTestThrottler) PrintUsage()      {}
func (t *controlledTestThrottler) Refresh()         { t.t.Error("controlled Insert must not force-refresh") }
func (t *controlledTestThrottler) AcquireContext(context.Context, int64) error {
	t.t.Error("borrowed input must never wait for ordinary D")
	return fmt.Errorf("unexpected ordinary memory wait")
}
func (t *controlledTestThrottler) MaxAcquire() int64 { return 64 << 20 }
func (t *controlledTestThrottler) BatchBytes() int64 { return 64 << 20 }

func controlledInsertFixture(t *testing.T, throttler *controlledTestThrottler) (*process.Process, *Insert, *rscthrottler.WorkingSet) {
	t.Helper()
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	oldRuntime := runtime.ServiceRuntime(proc.GetService())
	runtime.SetupServiceBasedRuntime(proc.GetService(), runtime.DefaultRuntime())
	t.Cleanup(func() { runtime.SetupServiceBasedRuntime(proc.GetService(), oldRuntime) })
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.CNMemoryThrottler, throttler)
	const unit = 512 << 10
	ioBytes, err := ioutil.IOEnvelope([][]types.Type{{types.T_int64.ToType(), types.T_varchar.ToType()}}, unit, unit)
	require.NoError(t, err)
	owner := rscthrottler.NewMemThrottler("insert-working-set", 1, rscthrottler.WithConstLimit(8<<30))
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{
		UnitBytes: unit, Inputs: []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 8 << 20}, {Name: "output", Count: 1, Bytes: 16 << 20}}, IOBytes: ioBytes,
	}})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: unit, SourceBytes: 8 << 20, OutputBytes: 16 << 20, IOBytes: ioBytes}
	op := &Insert{ToWriteS3: true, InsertCtx: &InsertCtx{TableDef: testInsertS3TableDef(), Attrs: []string{"a", "b"}, AddAffectedRows: true}}
	t.Cleanup(func() { op.Free(proc, false, nil) })
	require.NoError(t, op.Prepare(proc))
	return proc, op, ws
}

func controlledInput(t *testing.T, proc *process.Process, count, rows int) *colexec.MockOperator {
	t.Helper()
	batches := make([]*batch.Batch, 0, count)
	for k := range count {
		ids := make([]int64, rows)
		texts := make([]string, rows)
		for i := range ids {
			ids[i] = int64(k*rows + i + 1)
			texts[i] = "x"
		}
		b := batch.NewWithSize(3)
		b.Vecs[0] = testutil.MakeInt64Vector(ids, nil, proc.Mp())
		b.Vecs[1] = testutil.MakeVarcharVector(texts, nil, proc.Mp())
		var err error
		b.Vecs[2], err = vector.NewConstBytes(types.T_text.ToType(), []byte(strings.Repeat("z", 128)), rows, proc.Mp())
		require.NoError(t, err)
		b.SetRowCount(rows)
		batches = append(batches, b)
	}
	input := colexec.NewMockOperator().WithBatchs(batches)
	t.Cleanup(func() { input.Free(proc, false, nil) })
	return input
}

type observingInsert struct {
	*Insert
	onOutput func(*batch.Batch)
}

func (op *observingInsert) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := op.Insert.Call(proc)
	if err == nil && result.Batch != nil {
		op.onOutput(result.Batch)
	}
	return result, err
}

func TestControlledInsertZeroDataCreditsStreamsCompleteObjectFrames(t *testing.T) {
	deny := &controlledTestThrottler{t: t}
	proc, op, ws := controlledInsertFixture(t, deny)
	const rows = objectio.BlockMaxRows + 1
	op.AppendChild(controlledInput(t, proc, 3, rows))
	state := op.ctr.controlled
	var frames int
	observed := &observingInsert{Insert: op, onOutput: func(b *batch.Batch) {
		frames++
		require.Equal(t, vm.Eval, op.ctr.state, "first metadata frame cannot terminate the stream")
		require.Equal(t, 2, b.Vecs[0].Length())
		require.Equal(t, 1, b.Vecs[1].Length(), "all blocks of this object share one object-stats record")
		require.LessOrEqual(t, state.outputMP.AllocatedBytes(), proc.Base.ImportMemory.OutputBytes)
		require.Zero(t, state.dataMP.AllocatedBytes())
	}}
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(rel, nil).AnyTimes()
	var committed [][]byte
	var firstBlock []byte
	rel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, b *batch.Batch) error {
		require.Equal(t, 2, b.Vecs[0].Length())
		require.Equal(t, 1, b.Vecs[1].Length())
		if firstBlock == nil {
			firstBlock = bytes.Clone(b.Vecs[0].GetBytesAt(0))
		}
		committed = append(committed, bytes.Clone(b.Vecs[1].GetBytesAt(0)))
		return nil
	}).Times(3)
	consumer := mergeblock.NewArgument().WithEngine(eng).WithObjectRef(&plan.ObjectRef{SchemaName: "test", ObjName: "t1"}).WithAddAffectedRows(true)
	t.Cleanup(func() { consumer.Free(proc, false, nil); consumer.Release() })
	consumer.AppendChild(observed)
	require.NoError(t, consumer.Prepare(proc))
	for calls := 0; ; calls++ {
		require.Less(t, calls, 5)
		result, err := consumer.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
	}
	require.Equal(t, 3, frames)
	require.Equal(t, uint64(3*rows), consumer.GetAffectedRows())
	require.Equal(t, uint64(3*rows), op.GetAffectedRows())
	var total uint32
	for _, encoded := range committed {
		stats := objectio.ObjectStats(encoded)
		require.True(t, stats.GetCNCreated())
		require.Equal(t, uint32(2), stats.BlkCnt())
		total += stats.Rows()
	}
	require.Equal(t, uint32(3*rows), total)
	require.Zero(t, deny.acquired)
	require.Zero(t, deny.balance)
	fd := state.spool
	dataMP, outputMP := state.dataMP, state.outputMP
	op.Reset(proc, false, nil)
	require.Zero(t, dataMP.AllocatedBytes())
	require.Zero(t, outputMP.AllocatedBytes())
	_, err := fd.Stat()
	require.Error(t, err, "Reset closes the anonymous metadata FD")
	require.NotEmpty(t, firstBlock)
	firstStats := objectio.ObjectStats(committed[0])
	require.Equal(t, uint32(rows), firstStats.Rows(), "later frames and Reset must not alter committed metadata")
	lease, err := ws.AcquireInput(proc.Ctx, 1)
	require.NoError(t, err)
	lease.Release()
	ioLease, err := proc.Base.ImportMemory.AcquireIO(proc.Ctx)
	require.NoError(t, err)
	ioLease.Release()
}

func TestControlledInsertRetiresGrantedDataBeforeBorrowedSpill(t *testing.T) {
	deny := &controlledTestThrottler{t: t, grants: 1}
	proc, op, _ := controlledInsertFixture(t, deny)
	op.AppendChild(controlledInput(t, proc, 3, 1))
	state := op.ctr.controlled
	var total uint32
	for {
		result, err := op.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		for i := 0; i < result.Batch.Vecs[1].Length(); i++ {
			stats := objectio.ObjectStats(result.Batch.Vecs[1].GetBytesAt(i))
			total += stats.Rows()
		}
	}
	require.Equal(t, uint32(3), total)
	require.Positive(t, deny.acquired)
	require.Equal(t, deny.acquired, deny.released)
	require.Zero(t, deny.balance)
	require.Zero(t, state.dataMP.AllocatedBytes())
}

func TestControlledInsertCancelWaitingForWorkspaceClosesOwners(t *testing.T) {
	deny := &controlledTestThrottler{t: t, attempt: make(chan struct{})}
	proc, op, ws := controlledInsertFixture(t, deny)
	ctx, cancel := context.WithCancel(proc.Ctx)
	defer cancel()
	proc.Ctx = ctx
	op.AppendChild(controlledInput(t, proc, 1, 1))
	state := op.ctr.controlled
	dataMP, outputMP := state.dataMP, state.outputMP
	busy, err := proc.Base.ImportMemory.AcquireIO(ctx)
	require.NoError(t, err)
	defer busy.Release()
	done := make(chan error, 1)
	go func() { _, err := op.Call(proc); done <- err }()
	<-deny.attempt
	cancel()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancel did not terminate a workspace waiter")
	}
	require.Zero(t, dataMP.AllocatedBytes())
	require.Zero(t, outputMP.AllocatedBytes())
	lease, err := ws.AcquireInput(context.Background(), 1)
	require.NoError(t, err)
	lease.Release()
	require.Zero(t, deny.balance)
	op.Reset(proc, true, context.Canceled)
	op.Free(proc, true, context.Canceled)
}

func TestControlledInsertInvalidMetadataAndCloseErrorReleaseOwners(t *testing.T) {
	proc, op, ws := controlledInsertFixture(t, &controlledTestThrottler{t: t})
	s := op.ctr.controlled
	var err error
	s.spool, err = os.CreateTemp(t.TempDir(), "bad-frame-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(s.spool.Name()))
	var header [8]byte
	n := uint64(s.outputSlot.Capacity()/4 + 1)
	binary.LittleEndian.PutUint64(header[:], n)
	_, err = s.spool.Write(header[:])
	require.NoError(t, err)
	s.written = int64(n) + 8
	require.NoError(t, s.spool.Truncate(s.written))
	_, err = s.next()
	require.ErrorContains(t, err, "invalid insert metadata frame")
	require.Zero(t, s.outputMP.AllocatedBytes(), "reject the frame before decoding any capacity")
	dataMP, outputMP := s.dataMP, s.outputMP
	require.NoError(t, s.spool.Close())
	require.Error(t, s.close(), "surface an FD Close error while completing remaining cleanup")
	require.NoError(t, s.close(), "cleanup is idempotent after a Close error")
	require.Zero(t, dataMP.AllocatedBytes())
	require.Zero(t, outputMP.AllocatedBytes())
	lease, err := ws.AcquireInput(proc.Ctx, 1)
	require.NoError(t, err)
	lease.Release()
}

func TestControlledInsertReleaseCallbackPreservesPrepaidGrowth(t *testing.T) {
	credits := &controlledTestThrottler{t: t, grants: 2}
	proc, op, _ := controlledInsertFixture(t, credits)
	s := op.ctr.controlled
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(s.dataMP)
	s.begin()
	require.True(t, s.acquire(v.AppendAllocation(1, 0)))
	require.NoError(t, v.PreExtend(1, s.dataMP))
	s.end()
	old := s.dataMP.AllocatedBytes()
	s.begin()
	next := v.AppendAllocation(1000, 0)
	require.True(t, s.acquire(next))
	done := make(chan struct{})
	go func() { s.sourceReleased(); close(done) }()
	<-done
	require.Equal(t, old+next, s.granted, "a concurrent old-source free must not refund the prepaid complete new N")
	require.NoError(t, v.PreExtend(1000, s.dataMP))
	s.end()
	require.Equal(t, s.dataMP.AllocatedBytes(), s.granted)
	v.Free(s.dataMP)
	s.sourceReleased()
	require.Zero(t, credits.balance)
	require.Zero(t, s.dataMP.AllocatedBytes())
	_ = proc
}

type blockedImportFS struct {
	fileservice.FileService
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *blockedImportFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.once.Do(func() { close(fs.entered) })
	select {
	case <-fs.release:
		return fs.FileService.Write(ctx, v)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func TestControlledInsertDefaultImportPipelinesUpload(t *testing.T) {
	for _, grants := range []int{0, 1} {
		t.Run(fmt.Sprint(grants), func(t *testing.T) { checkControlledInsertPipeline(t, grants, false) })
	}
	t.Run("borrowed-cancel", func(t *testing.T) { checkControlledInsertPipeline(t, 0, true) })
}

func checkControlledInsertPipeline(t *testing.T, grants int, cancelUpload bool) {
	credits := &controlledTestThrottler{t: t, grants: grants}
	proc, op, ws := controlledInsertFixture(t, credits)
	ctx, cancel := context.WithTimeout(proc.Ctx, 10*time.Second)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	// One parent and two ready uploads must fit independently in this test.
	require.NoError(t, ws.TryGrow(ctx, rscthrottler.WorkingSetSpec{
		UnitBytes: ws.UnitBytes(), Inputs: []rscthrottler.InputSlotSpec{
			{Name: "csv", Count: 1, Bytes: ws.InputCapacity(0)},
			{Name: "output", Count: 1, Bytes: ws.InputCapacity(1)},
		}, IOBytes: 3 * ws.IOBytes(),
	}))
	require.Nil(t, ctx.Value(ioutil.PipelineFlushKey), "ordinary LOAD needs no backfill context flag")
	s := op.ctr.controlled
	fs := &blockedImportFS{FileService: s.fs, entered: make(chan struct{}), release: make(chan struct{})}
	s.fs = fs
	var once sync.Once
	unblock := func() { once.Do(func() { close(fs.release) }) }
	t.Cleanup(unblock)
	b := batch.NewWithSize(2)
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{2, 1}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeVarcharVector([]string{"b", "a"}, nil, proc.Mp())
	b.SetRowCount(2)
	require.NoError(t, s.append(proc, op.OpAnalyzer, b))
	require.NoError(t, s.flushPending(proc), "boundary flush submits instead of awaiting S3")
	select {
	case <-fs.entered:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	// Ready jobs keep their own W; the borrowed parent must be available again.
	need, _, err := ioutil.BatchIOEnvelope(b, s.bufferBytes)
	require.NoError(t, err)
	available, err := ws.AcquireIO(ctx, need)
	require.NoError(t, err)
	available.Release()
	ids := vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])
	ids[0], ids[1] = 4, 3
	require.NoError(t, s.append(proc, op.OpAnalyzer, b), "a cold D retry must not join the first upload")
	require.NoError(t, s.flushPending(proc))
	require.Zero(t, s.written, "incomplete uploads must not produce transaction metadata")
	if cancelUpload {
		cancel()
		require.ErrorIs(t, s.flush(proc, op.OpAnalyzer), context.Canceled)
		dataMP := s.dataMP
		require.NoError(t, s.close())
		require.Zero(t, dataMP.AllocatedBytes())
		require.Zero(t, credits.balance)
		checkCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		lease, err := ws.AcquireInput(checkCtx, 1)
		require.NoError(t, err)
		lease.Release()
		available, err := ws.AcquireIO(checkCtx, ws.IOBytes())
		require.NoError(t, err)
		available.Release()
		return
	}
	unblock()
	require.NoError(t, s.flush(proc, op.OpAnalyzer))
	var rows uint32
	var objects int
	for {
		frame, err := s.next()
		require.NoError(t, err)
		if frame == nil {
			break
		}
		objects += frame.Vecs[1].Length()
		for i := 0; i < frame.Vecs[1].Length(); i++ {
			stats := objectio.ObjectStats(frame.Vecs[1].GetBytesAt(i))
			rows += stats.Rows()
		}
	}
	require.EqualValues(t, 4, rows)
	require.Equal(t, 2, objects)
	require.Zero(t, credits.balance)
	lease, err := ws.AcquireIO(ctx, ws.IOBytes())
	require.NoError(t, err)
	lease.Release()
}
