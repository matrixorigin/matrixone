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
	"errors"
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
	ioBytes, err := ioutil.IOEnvelope([][]types.Type{{types.T_int64.ToType(), types.T_varchar.ToType()}}, unit, 2<<20)
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

func TestControlledInsertUsesOptionalStagingWithoutWaiting(t *testing.T) {
	for _, test := range []struct {
		name               string
		occupied, noBudget bool
	}{
		{name: "available"},
		{name: "occupied", occupied: true},
		{name: "no_ordinary_throttler", noBudget: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			deny := &controlledTestThrottler{t: t}
			proc, op, ws := controlledInsertFixture(t, deny)
			ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
			require.NoError(t, err)
			require.True(t, ok)
			if test.noBudget {
				op.ctr.controlled.throttler = nil
			}
			if test.occupied {
				lease, leaseErr := ws.TryAcquireStaging(proc.Ctx)
				require.NoError(t, leaseErr)
				require.NotNil(t, lease)
				defer lease.Release()
			}
			op.AppendChild(controlledInput(t, proc, 3, 1))
			var objects, rows uint32
			for {
				result, callErr := op.Call(proc)
				require.NoError(t, callErr)
				if result.Batch == nil {
					break
				}
				for i := 0; i < result.Batch.Vecs[1].Length(); i++ {
					objects++
					stats := objectio.ObjectStats(result.Batch.Vecs[1].GetBytesAt(i))
					rows += stats.Rows()
				}
			}
			require.Equal(t, uint32(3), rows)
			if test.occupied {
				require.Equal(t, uint32(3), objects, "an unavailable slot uses the existing direct-write path")
			} else {
				require.Equal(t, uint32(1), objects, "small input batches share one object")
				lease, leaseErr := ws.TryAcquireStaging(proc.Ctx)
				require.NoError(t, leaseErr)
				require.NotNil(t, lease, "the flushed buffer returned its shared slot")
				lease.Release()
			}
			require.Zero(t, deny.balance)
		})
	}
}

func TestControlledInsertStagingPreservesAmpleDataBudget(t *testing.T) {
	for _, test := range []struct {
		name    string
		staging bool
		grants  int
		objects uint32
	}{
		{name: "ordinary_only", grants: 10, objects: 1},
		{name: "ordinary_with_staging", staging: true, grants: 10, objects: 1},
		{name: "staging_after_denial", staging: true, grants: 1, objects: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			credits := &controlledTestThrottler{t: t, grants: test.grants}
			proc, op, ws := controlledInsertFixture(t, credits)
			if test.staging {
				ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
				require.NoError(t, err)
				require.True(t, ok)
			}
			batches := make([]*batch.Batch, 2)
			for i := range batches {
				b := batch.NewWithSize(2)
				b.Vecs[0] = testutil.MakeInt64Vector([]int64{int64(i + 1)}, nil, proc.Mp())
				b.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 200<<10)}, nil, proc.Mp())
				b.SetRowCount(1)
				batches[i] = b
			}
			input := colexec.NewMockOperator().WithBatchs(batches)
			t.Cleanup(func() { input.Free(proc, false, nil) })
			op.AppendChild(input)
			var objects, rows uint32
			for {
				result, err := op.Call(proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				for i := 0; i < result.Batch.Vecs[1].Length(); i++ {
					objects++
					stats := objectio.ObjectStats(result.Batch.Vecs[1].GetBytesAt(i))
					rows += stats.Rows()
				}
			}
			require.Equal(t, uint32(2), rows)
			require.Equal(t, test.objects, objects)
			require.Positive(t, credits.acquired)
			require.Zero(t, credits.balance)
			if test.staging {
				lease, err := ws.TryAcquireStaging(proc.Ctx)
				require.NoError(t, err)
				require.NotNil(t, lease)
				lease.Release()
			}
		})
	}
}

type failingStagingFS struct {
	fileservice.FileService
	err error
}

func (fs *failingStagingFS) Write(context.Context, fileservice.IOVector) error { return fs.err }

func TestControlledInsertStagingWriteFailureReleasesSlot(t *testing.T) {
	deny := &controlledTestThrottler{t: t}
	proc, op, ws := controlledInsertFixture(t, deny)
	ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
	require.NoError(t, err)
	require.True(t, ok)
	state := op.ctr.controlled
	dataMP := state.dataMP
	writeErr := errors.New("injected object write failure")
	state.fs = &failingStagingFS{FileService: state.fs, err: writeErr}
	op.AppendChild(controlledInput(t, proc, 3, 1))
	_, err = op.Call(proc)
	require.ErrorIs(t, err, writeErr)
	require.Zero(t, dataMP.AllocatedBytes())
	lease, err := ws.TryAcquireStaging(proc.Ctx)
	require.NoError(t, err)
	require.NotNil(t, lease, "write failure must retire the staged owner")
	lease.Release()
	require.Zero(t, deny.balance)
}

func TestControlledInsertStagingCancellationReleasesSlot(t *testing.T) {
	deny := &controlledTestThrottler{t: t}
	proc, op, ws := controlledInsertFixture(t, deny)
	ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
	require.NoError(t, err)
	require.True(t, ok)
	state := op.ctr.controlled
	dataMP := state.dataMP
	b := batch.NewWithSize(2)
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeVarcharVector([]string{"pending"}, nil, proc.Mp())
	b.SetRowCount(1)
	defer b.Clean(proc.Mp())
	require.NoError(t, state.append(proc, op.OpAnalyzer, b))
	require.Positive(t, dataMP.AllocatedBytes())
	lease, err := ws.TryAcquireStaging(proc.Ctx)
	require.NoError(t, err)
	require.Nil(t, lease, "the pending writer owns the shared slot")
	op.AppendChild(controlledInput(t, proc, 1, 1))
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	_, err = op.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, dataMP.AllocatedBytes())
	lease, err = ws.TryAcquireStaging(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lease)
	lease.Release()
	require.Zero(t, deny.balance)
}

func TestControlledInsertOversizedRowFallsBackFromStaging(t *testing.T) {
	deny := &controlledTestThrottler{t: t}
	proc, op, ws := controlledInsertFixture(t, deny)
	ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
	require.NoError(t, err)
	require.True(t, ok)
	b := batch.NewWithSize(3)
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 1<<20)}, nil, proc.Mp())
	b.Vecs[2] = testutil.MakeVarcharVector([]string{"unused"}, nil, proc.Mp())
	b.SetRowCount(1)
	input := colexec.NewMockOperator().WithBatchs([]*batch.Batch{b})
	t.Cleanup(func() { input.Free(proc, false, nil) })
	op.AppendChild(input)
	var rows uint32
	for {
		result, callErr := op.Call(proc)
		require.NoError(t, callErr)
		if result.Batch == nil {
			break
		}
		for i := 0; i < result.Batch.Vecs[1].Length(); i++ {
			stats := objectio.ObjectStats(result.Batch.Vecs[1].GetBytesAt(i))
			rows += stats.Rows()
		}
	}
	require.Equal(t, uint32(1), rows)
	lease, err := ws.TryAcquireStaging(proc.Ctx)
	require.NoError(t, err)
	require.NotNil(t, lease)
	lease.Release()
	require.Zero(t, deny.balance)
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
	for _, staging := range []bool{false, true} {
		t.Run(fmt.Sprintf("staging=%t", staging), func(t *testing.T) {
			credits := &controlledTestThrottler{t: t, grants: 2}
			proc, op, ws := controlledInsertFixture(t, credits)
			if staging {
				credits.grants = 0
				ok, err := ws.TryReserveStaging(proc.Ctx, proc.Base.ImportMemory.UnitBytes)
				require.NoError(t, err)
				require.True(t, ok)
			}
			s := op.ctr.controlled
			v := vector.NewVec(types.T_int64.ToType())
			defer v.Free(s.dataMP)
			s.begin()
			granted, err := s.acquire(proc.Ctx, v.AppendAllocation(1, 0))
			require.NoError(t, err)
			require.True(t, granted)
			require.NoError(t, v.PreExtend(1, s.dataMP))
			s.end()
			old := s.dataMP.AllocatedBytes()
			s.begin()
			next := v.AppendAllocation(1000, 0)
			granted, err = s.acquire(proc.Ctx, next)
			require.NoError(t, err)
			require.True(t, granted)
			done := make(chan struct{})
			go func() { s.sourceReleased(); close(done) }()
			<-done
			require.Equal(t, old+next, s.granted, "a callback must not refund pending growth")
			require.NoError(t, v.PreExtend(1000, s.dataMP))
			s.end()
			require.Equal(t, s.dataMP.AllocatedBytes(), s.granted)
			v.Free(s.dataMP)
			s.sourceReleased()
			if staging {
				lease, err := ws.TryAcquireStaging(proc.Ctx)
				require.NoError(t, err)
				require.NotNil(t, lease)
				lease.Release()
			}
			require.Zero(t, credits.balance)
			require.Zero(t, s.dataMP.AllocatedBytes())
		})
	}
}
