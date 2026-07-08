// Copyright 2021 Matrix Origin
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

package insert

import (
	"context"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestInsertOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Reset(gomock.Any()).Return(nil).AnyTimes()
	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	batch1 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2, 0}, []uint64{2}, proc.Mp()),
			testutil.MakeScalarInt64(3, 3, proc.Mp()),
			testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, proc.Mp()),
			testutil.MakeScalarVarchar("d", 3, proc.Mp()),
			testutil.MakeScalarNull(t, types.T_int64, 3, proc.Mp()),
		},
		Attrs: []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
	}
	batch1.SetRowCount(3)
	argument1 := Insert{
		InsertCtx: &InsertCtx{
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			Engine:          eng,
			AddAffectedRows: true,
			Attrs:           []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
		ctr: container{
			state: vm.Build,
		},
	}
	resetChildren(&argument1, batch1)
	err := argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(3), argument1.ctr.affectedRows)

	argument1.Reset(proc, false, nil)

	resetChildren(&argument1, batch1)
	err = argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(3), argument1.ctr.affectedRows)

	argument1.Reset(proc, false, nil)
	argument1.Free(proc, false, nil)
	argument1.GetChildren(0).Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *Insert, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
	arg.ctr.state = vm.Build
}

func TestInsert_initBufForS3(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "initialize buffer for S3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			insert := &Insert{
				InsertCtx: &InsertCtx{
					Engine: nil,
				},
			}
			insert.initBufForS3()
			require.NotNil(t, insert.ctr.buf)
			require.Equal(t, 2, len(insert.ctr.buf.Attrs))
			require.Equal(t, catalog.BlockMeta_BlockInfo, insert.ctr.buf.Attrs[0])
			require.Equal(t, catalog.ObjectMeta_ObjectStats, insert.ctr.buf.Attrs[1])
			require.Equal(t, types.T_text, insert.ctr.buf.Vecs[0].GetType().Oid)
			require.Equal(t, types.T_binary, insert.ctr.buf.Vecs[1].GetType().Oid)
		})

		t.Run(tt.name, func(t *testing.T) {
			insert := &Insert{
				InsertCtx: &InsertCtx{
					Engine: &memoryengine.BindedEngine{},
				},
			}
			insert.initBufForS3()
			require.NotNil(t, insert.ctr.buf)
			require.Equal(t, 3, len(insert.ctr.buf.Attrs))
			require.Equal(t, catalog.BlockMeta_TableIdx_Insert, insert.ctr.buf.Attrs[0])
			require.Equal(t, catalog.BlockMeta_BlockInfo, insert.ctr.buf.Attrs[1])
			require.Equal(t, catalog.ObjectMeta_ObjectStats, insert.ctr.buf.Attrs[2])
			require.Equal(t, types.T_int16, insert.ctr.buf.Vecs[0].GetType().Oid)
			require.Equal(t, types.T_text, insert.ctr.buf.Vecs[1].GetType().Oid)
			require.Equal(t, types.T_binary, insert.ctr.buf.Vecs[2].GetType().Oid)
		})
	}
}

type testS3MemThrottler struct {
	grants       []bool
	acquired     []int64
	released     int64
	forceRefresh int
	ops          []string
}

func (t *testS3MemThrottler) Refresh() {}

func (t *testS3MemThrottler) ForceRefresh() {
	t.forceRefresh++
	t.ops = append(t.ops, "force-refresh")
}

func (t *testS3MemThrottler) PrintUsage() {}

func (t *testS3MemThrottler) Acquire(size int64) (int64, bool) {
	t.acquired = append(t.acquired, size)
	if len(t.grants) == 0 {
		return 0, false
	}
	grant := t.grants[0]
	t.grants = t.grants[1:]
	return 0, grant
}

func (t *testS3MemThrottler) Release(size int64) int64 {
	t.released += size
	t.ops = append(t.ops, "release")
	return 0
}

func (t *testS3MemThrottler) Available() int64 {
	return 0
}

type testS3MemThrottlerWithDecision struct {
	testS3MemThrottler
	shouldRefresh bool
}

func (t *testS3MemThrottlerWithDecision) ShouldRefreshBeforeRelease() bool {
	return t.shouldRefresh
}

type testRefreshOnlyThrottler struct {
	refreshed int
}

func (t *testRefreshOnlyThrottler) Refresh() {
	t.refreshed++
}

func TestInsertAcquireS3WriteMemory(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	throttler := &testS3MemThrottler{grants: []bool{true, true}}
	insert := &Insert{
		InsertCtx: &InsertCtx{},
		ctr: container{
			s3MemThrottler: throttler,
		},
	}

	err := insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), 1024)
	require.NoError(t, err)
	require.Equal(t, []int64{1024}, throttler.acquired)
	require.Equal(t, int64(1024), insert.ctr.s3MemGranted)
	require.Equal(t, 0, throttler.forceRefresh)

	err = insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), 1024)
	require.NoError(t, err)
	require.Equal(t, []int64{1024, 1024}, throttler.acquired)
	require.Equal(t, int64(2048), insert.ctr.s3MemGranted)
	require.Equal(t, 0, throttler.forceRefresh)
}

func TestInsertAcquireS3WriteMemoryFlushesBeforeRetry(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	throttler := &testS3MemThrottler{grants: []bool{false, false, true}}
	insert := &Insert{
		InsertCtx: &InsertCtx{},
		ctr: container{
			s3MemGranted:   2048,
			s3MemThrottler: throttler,
		},
	}

	err := insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), 4096)
	require.NoError(t, err)
	require.Equal(t, []int64{4096, 4096, 4096}, throttler.acquired)
	require.Equal(t, int64(2048), throttler.released)
	require.Equal(t, int64(4096), insert.ctr.s3MemGranted)
	require.Equal(t, 1, throttler.forceRefresh)
}

func TestInsertAcquireS3WriteMemoryCapsReservationToS3Threshold(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	throttler := &testS3MemThrottler{grants: []bool{true}}
	insert := &Insert{
		InsertCtx: &InsertCtx{},
		ctr: container{
			s3MemThrottler: throttler,
		},
	}

	size := colexec.WriteS3Threshold * 4
	err := insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), size)
	require.NoError(t, err)
	require.Equal(t, []int64{int64(colexec.WriteS3Threshold)}, throttler.acquired)
	require.Equal(t, int64(colexec.WriteS3Threshold), insert.ctr.s3MemGranted)
	require.Equal(t, 0, throttler.forceRefresh)

	err = insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), size)
	require.NoError(t, err)
	require.Equal(t, []int64{int64(colexec.WriteS3Threshold)}, throttler.acquired)
}

func TestInsertAcquireS3WriteMemoryDoesNotCapPipelineFlush(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	throttler := &testS3MemThrottler{grants: []bool{true}}
	insert := &Insert{
		InsertCtx: &InsertCtx{},
		ctr: container{
			s3MemThrottler:      throttler,
			s3MemNoThresholdCap: true,
		},
	}

	size := colexec.WriteS3Threshold * 4
	err := insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), size)
	require.NoError(t, err)
	require.Equal(t, []int64{int64(size)}, throttler.acquired)
	require.Equal(t, int64(size), insert.ctr.s3MemGranted)
	require.Equal(t, 0, throttler.forceRefresh)
}

func TestInsertAcquireS3WriteMemoryReturnsExhaustedError(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	throttler := &testS3MemThrottler{grants: []bool{false, false, false, false}}
	insert := &Insert{
		InsertCtx: &InsertCtx{},
		ctr: container{
			s3MemThrottler: throttler,
		},
	}

	err := insert.acquireS3WriteMemory(proc, process.NewAnalyzer(0, false, false, "insert"), 1024)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CN S3 write memory is exhausted")
	require.Equal(t, []int64{1024, 1024, 1024, 1024}, throttler.acquired)
	require.Equal(t, 2, throttler.forceRefresh)
}

func TestForcedRefreshFallsBackToRefresh(t *testing.T) {
	throttler := &testRefreshOnlyThrottler{}
	forcedRefresh(throttler)
	require.Equal(t, 1, throttler.refreshed)
}

func TestInsertPrepareToWriteS3WithPipelineFlush(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	proc.Ctx = context.WithValue(proc.Ctx, ioutil.PipelineFlushKey, true)
	throttler := &testS3MemThrottler{}
	runtime.ServiceRuntime(proc.GetService()).SetGlobalVariables(runtime.CNMemoryThrottler, throttler)

	insert := &Insert{
		ToWriteS3: true,
		InsertCtx: &InsertCtx{
			TableDef: testInsertS3TableDef(),
		},
	}

	err := insert.Prepare(proc)
	require.NoError(t, err)
	require.True(t, insert.ctr.s3MemNoThresholdCap)
	require.Same(t, throttler, insert.ctr.s3MemThrottler)
	require.NotNil(t, insert.ctr.s3Writer)
	require.NotNil(t, insert.ctr.buf)
	require.Equal(t, colexec.WriteS3Threshold, insert.ctr.s3Writer.MemorySizeThreshold())
}

func TestInsertFlushS3WriterOnMemoryPressureAppendsBlockInfo(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	throttler := &testS3MemThrottler{}
	insert := &Insert{
		InsertCtx: &InsertCtx{
			TableDef: testInsertS3TableDef(),
		},
		ctr: container{
			s3Writer:       colexec.NewCNS3DataWriter(proc.Mp(), fs, testInsertS3TableDef(), 1, false),
			s3MemGranted:   1024,
			s3MemThrottler: throttler,
		},
	}
	insert.initBufForS3()
	defer insert.ctr.s3Writer.Close()

	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	err = insert.ctr.s3Writer.Write(proc.Ctx, bat)
	require.NoError(t, err)

	err = insert.flushS3WriterOnMemoryPressure(proc, process.NewAnalyzer(0, false, false, "insert"))
	require.NoError(t, err)
	require.Equal(t, int64(0), insert.ctr.s3MemGranted)
	require.Equal(t, int64(1024), throttler.released)
	require.Equal(t, []string{"force-refresh", "release"}, throttler.ops)
	require.NotNil(t, insert.ctr.buf)
	require.Greater(t, insert.ctr.buf.RowCount(), 0)
}

func TestInsertFlushS3WriterOnMemoryPressureRefreshesBeforeReleaseOnAppendError(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	throttler := &testS3MemThrottler{}
	insert := &Insert{
		InsertCtx: &InsertCtx{
			TableDef: testInsertS3TableDef(),
		},
		ctr: container{
			s3Writer:       colexec.NewCNS3DataWriter(proc.Mp(), fs, testInsertS3TableDef(), 1, false),
			s3MemGranted:   1024,
			s3MemThrottler: throttler,
			// Make the post-sync block-info append fail.
			buf: batch.NewWithSize(1),
		},
	}
	defer insert.ctr.s3Writer.Close()

	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	err = insert.ctr.s3Writer.Write(proc.Ctx, bat)
	require.NoError(t, err)

	err = insert.flushS3WriterOnMemoryPressure(proc, process.NewAnalyzer(0, false, false, "insert"))
	require.Error(t, err)
	require.Equal(t, int64(0), insert.ctr.s3MemGranted)
	require.Equal(t, int64(1024), throttler.released)
	require.Equal(t, []string{"force-refresh", "release"}, throttler.ops)
}

func TestInsertS3FinalFlushRefreshesBeforeReleaseOnSuccess(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	throttler := &testS3MemThrottler{}
	insert := &Insert{
		InsertCtx: &InsertCtx{
			TableDef: testInsertS3TableDef(),
		},
		ctr: container{
			state:          vm.Eval,
			s3Writer:       colexec.NewCNS3DataWriter(proc.Mp(), fs, testInsertS3TableDef(), 1, false),
			s3MemGranted:   1024,
			s3MemThrottler: throttler,
		},
	}
	insert.initBufForS3()
	defer insert.ctr.s3Writer.Close()

	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	err = insert.ctr.s3Writer.Write(proc.Ctx, bat)
	require.NoError(t, err)

	_, err = insert.insert_s3(proc, process.NewAnalyzer(0, false, false, "insert"))
	require.NoError(t, err)
	require.Equal(t, int64(0), insert.ctr.s3MemGranted)
	require.Equal(t, int64(1024), throttler.released)
	require.Equal(t, []string{"force-refresh", "release"}, throttler.ops)
}

func TestInsertS3FinalFlushRefreshesBeforeReleaseOnError(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	throttler := &testS3MemThrottler{}
	insert := &Insert{
		InsertCtx: &InsertCtx{
			TableDef: testInsertS3TableDef(),
		},
		ctr: container{
			state:          vm.Eval,
			s3Writer:       colexec.NewCNS3DataWriter(proc.Mp(), fs, testInsertS3TableDef(), 1, false),
			s3MemGranted:   1024,
			s3MemThrottler: throttler,
			buf:            batch.NewWithSize(1),
		},
	}
	defer insert.ctr.s3Writer.Close()

	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	err = insert.ctr.s3Writer.Write(proc.Ctx, bat)
	require.NoError(t, err)

	_, err = insert.insert_s3(proc, process.NewAnalyzer(0, false, false, "insert"))
	require.Error(t, err)
	require.Equal(t, int64(0), insert.ctr.s3MemGranted)
	require.Equal(t, int64(1024), throttler.released)
	require.Equal(t, []string{"force-refresh", "release"}, throttler.ops)
}

func TestRefreshAndReleaseS3MemGrantSkipsRefreshWhenCoverageIsAlreadyEnough(t *testing.T) {
	throttler := &testS3MemThrottlerWithDecision{shouldRefresh: false}
	insert := &Insert{
		ctr: container{
			s3MemGranted:   1024,
			s3MemThrottler: throttler,
		},
	}

	insert.refreshAndReleaseS3MemGrant()
	require.Equal(t, int64(0), insert.ctr.s3MemGranted)
	require.Equal(t, int64(1024), throttler.released)
	require.Equal(t, []string{"release"}, throttler.ops)
}

func TestAcquireFlushSlotWaitsForFlushSlotAfterTimeout(t *testing.T) {
	oldFlushLimiterState := flushLimiterState
	oldFlushConcurrencyForAcquire := flushConcurrencyForAcquire
	oldAcquireTimeout := flushSemaphoreAcquireTimeout
	oldRefreshInterval := flushConcurrencyRefreshInterval
	defer func() {
		flushLimiterState = oldFlushLimiterState
		flushConcurrencyForAcquire = oldFlushConcurrencyForAcquire
		flushSemaphoreAcquireTimeout = oldAcquireTimeout
		flushConcurrencyRefreshInterval = oldRefreshInterval
	}()

	flushLimiterState = newFlushLimiter()
	flushConcurrencyForAcquire = func() int { return 1 }
	flushSemaphoreAcquireTimeout = time.Millisecond
	flushConcurrencyRefreshInterval = time.Millisecond
	heldRelease, waitCh := flushLimiterState.tryAcquire()
	require.NotNil(t, heldRelease)
	require.Nil(t, waitCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	releaseCh := make(chan func(), 1)
	go func() {
		release, err := acquireFlushSlot(ctx)
		if release != nil {
			releaseCh <- release
		}
		errCh <- err
	}()

	require.Never(t, func() bool {
		return len(releaseCh) > 0
	}, 5*time.Millisecond, time.Millisecond)
	require.Equal(t, 1, flushLimiterState.inUseCount())
	heldRelease()

	release := <-releaseCh
	require.NoError(t, <-errCh)
	require.Equal(t, 1, flushLimiterState.inUseCount())
	release()
	require.Equal(t, 0, flushLimiterState.inUseCount())
}

func TestFlushConcurrencyLimitScalesWithGOMAXPROCS(t *testing.T) {
	require.Equal(t, minFlushConcurrencyLimit, flushConcurrencyForGOMAXPROCS(1))
	require.Equal(t, minFlushConcurrencyLimit, flushConcurrencyForGOMAXPROCS(8))
	require.Equal(t, 8, flushConcurrencyForGOMAXPROCS(16))
	require.Equal(t, maxFlushConcurrencyLimit, flushConcurrencyForGOMAXPROCS(64))

	oldGOMAXPROCS := goruntime.GOMAXPROCS(0)
	defer goruntime.GOMAXPROCS(oldGOMAXPROCS)
	goruntime.GOMAXPROCS(16)
	require.Equal(t, 8, flushConcurrencyForAcquire())
	goruntime.GOMAXPROCS(64)
	require.Equal(t, maxFlushConcurrencyLimit, flushConcurrencyForAcquire())
}

func TestAcquireFlushSlotWaitsWhenNormalSlotsAreFull(t *testing.T) {
	oldFlushLimiterState := flushLimiterState
	oldFlushConcurrencyForAcquire := flushConcurrencyForAcquire
	oldAcquireTimeout := flushSemaphoreAcquireTimeout
	oldRefreshInterval := flushConcurrencyRefreshInterval
	defer func() {
		flushLimiterState = oldFlushLimiterState
		flushConcurrencyForAcquire = oldFlushConcurrencyForAcquire
		flushSemaphoreAcquireTimeout = oldAcquireTimeout
		flushConcurrencyRefreshInterval = oldRefreshInterval
	}()

	flushLimiterState = newFlushLimiter()
	flushConcurrencyForAcquire = func() int { return 1 }
	flushSemaphoreAcquireTimeout = time.Millisecond
	flushConcurrencyRefreshInterval = time.Millisecond
	heldRelease, waitCh := flushLimiterState.tryAcquire()
	require.NotNil(t, heldRelease)
	require.Nil(t, waitCh)
	defer heldRelease()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		release, err := acquireFlushSlot(ctx)
		if release != nil {
			release()
		}
		errCh <- err
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	require.Equal(t, 1, flushLimiterState.inUseCount())
}

func testInsertS3TableDef() *plan.TableDef {
	tableDef := &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true, Primary: true, Default: &plan.Default{NullAbility: false}},
			{ColId: 1, Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8192}, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: "a",
			Names:       []string{"a"},
		},
		Name2ColIndex: map[string]int32{
			"a":            0,
			"b":            1,
			catalog.Row_ID: 2,
		},
	}
	return tableDef
}
