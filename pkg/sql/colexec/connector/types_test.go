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

package connector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestConnectorResetAbortsSpoolWhenTerminalSignalCannotBeDelivered(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(sp, 0)
	conn := &Connector{Reg: reg}
	conn.ctr.sp = sp
	sourceErr := moerr.NewCheckRecursiveLevel(context.Background())

	done := make(chan struct{})
	go func() {
		conn.Reset(nil, true, sourceErr)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Connector.Reset blocked on a full receiver channel")
	}
	require.Equal(t, int64(0), mp.CurrNB())
	require.Nil(t, conn.ctr.sp)
	select {
	case <-reg.Done():
	default:
		t.Fatal("Connector.Reset did not close the receiver edge Done")
	}

	staleSignal := <-reg.Ch2
	got, info := staleSignal.Action()
	require.Nil(t, got)
	require.Same(t, sourceErr, info)
}

func TestConnectorResetPreservesErrorAheadOfDeliveredTerminal(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)

	reg := process.NewPipelineEdge(2, 0)
	reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(sp, 0)
	conn := &Connector{Reg: reg}
	conn.ctr.sp = sp
	sourceErr := moerr.NewCheckRecursiveLevel(context.Background())

	conn.Reset(nil, true, sourceErr)

	receiver := process.InitPipelineSignalReceiver(context.Background(), []*process.WaitRegister{reg})
	got, info := receiver.GetNextBatch(nil)
	require.Nil(t, got)
	require.Same(t, sourceErr, info)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConnectorResetFallsBackToAbortWhenEndSignalCannotBeDelivered(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(sp, 0)
	conn := &Connector{Reg: reg}
	conn.ctr.sp = sp

	done := make(chan struct{})
	go func() {
		conn.Reset(nil, false, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Connector.Reset blocked after normal End delivery failed")
	}
	require.Nil(t, conn.ctr.sp)
	require.Nil(t, conn.cleanupSpool)
	require.Equal(t, int64(0), mp.CurrNB())
	select {
	case <-reg.Done():
	default:
		t.Fatal("fallback abort did not close Done")
	}
	require.ErrorIs(t, reg.Err(), process.ErrPipelineEndSignalDeliveryFailed)

	staleSignal := <-reg.Ch2
	got, info := staleSignal.Action()
	require.Nil(t, got)
	require.Same(t, process.ErrPipelineEndSignalDeliveryFailed, info)
}

func TestConnectorResetUndeliveredFallbackAbortWakesReceiver(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)

	conn := &Connector{Reg: reg}
	conn.Reset(nil, false, nil)

	receiverCtx, cancelReceiver := context.WithCancel(context.Background())
	defer cancelReceiver()
	receiver := process.InitPipelineSignalReceiver(receiverCtx, []*process.WaitRegister{reg})

	got, err := receiver.GetNextBatch(nil)
	require.NoError(t, err)
	require.Same(t, batch.EmptyBatch, got)

	type result struct {
		bat *batch.Batch
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		got, err := receiver.GetNextBatch(nil)
		resultCh <- result{bat: got, err: err}
	}()

	select {
	case result := <-resultCh:
		require.Nil(t, result.bat)
		require.ErrorIs(t, result.err, process.ErrPipelineEndSignalDeliveryFailed)
	case <-time.After(time.Second):
		cancelReceiver()
		<-resultCh
		t.Fatal("receiver remained blocked after the fallback Abort failed to enter the full channel")
	}
}

func TestConnectorResetPreservesRecordedTerminalError(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(sp, 0)
	originalErr := moerr.NewDuplicateEntryNoCtx("1000000", "")
	require.False(t, reg.TrySendError(originalErr))
	require.ErrorIs(t, reg.Err(), originalErr)

	conn := &Connector{Reg: reg}
	conn.ctr.sp = sp
	conn.Reset(nil, false, nil)

	staleSignal := <-reg.Ch2
	got, info := staleSignal.Action()
	require.Nil(t, got)
	require.ErrorIs(t, info, originalErr)
	require.NotErrorIs(t, info, process.ErrPipelineEndSignalDeliveryFailed)
}

func TestConnectorResetUsesSharedTerminalSendBudget(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	reg := process.NewPipelineEdge(1, 0)
	reg.Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, nil)
	conn := &Connector{Reg: reg}

	start := time.Now()
	conn.Reset(nil, false, nil)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 300*time.Millisecond)
	select {
	case <-reg.Done():
	default:
		t.Fatal("fallback abort should mark the receiver edge terminal")
	}
	require.ErrorIs(t, reg.Err(), process.ErrPipelineEndSignalDeliveryFailed)
}

func TestConnectorResetFailedNilErrorSendsTypedErrorWithCause(t *testing.T) {
	reg := process.NewPipelineEdge(1, 0)
	conn := &Connector{Reg: reg}

	conn.Reset(nil, true, nil)

	require.ErrorIs(t, reg.Err(), process.ErrPipelineTerminalWithoutCause)
	select {
	case signal := <-reg.Ch2:
		require.Equal(t, process.EventError, signal.EventType)
		require.ErrorIs(t, signal.TerminalErr(), process.ErrPipelineTerminalWithoutCause)
	default:
		t.Fatal("Connector.Reset did not send a typed failure terminal")
	}
}

func TestConnectorResetNilRegAbortsSpoolWithoutPanic(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	conn := &Connector{}
	conn.ctr.sp = sp

	require.NotPanics(t, func() {
		conn.Reset(nil, false, nil)
	})
	require.Nil(t, conn.ctr.sp)
	require.Nil(t, conn.cleanupSpool)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConnectorResetEndPreservesQueuedSpoolBatchUntilDeferredCleanup(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	reg := process.NewPipelineEdge(2, 0)
	reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(sp, 0)
	conn := &Connector{Reg: reg}
	conn.ctr.sp = sp

	conn.Reset(nil, false, nil)
	require.Nil(t, conn.ctr.sp)
	require.Same(t, sp, conn.cleanupSpool)
	select {
	case <-reg.Done():
	default:
		t.Fatal("Connector.Reset did not close Done after delivering End")
	}

	dataSignal := <-reg.Ch2
	got, info := dataSignal.Action()
	require.NoError(t, info)
	require.NotNil(t, got)
	require.Equal(t, 1024, got.RowCount())
	sp.ReleaseCurrent(0)

	terminalSignal := <-reg.Ch2
	require.Equal(t, process.EventEnd, terminalSignal.EventType)
	require.Greater(t, mp.CurrNB(), int64(0))

	conn.CleanupDeferredSpool()
	require.Nil(t, conn.cleanupSpool)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConnectorAllocationClearFinalizesAbortedSpool(t *testing.T) {
	testConnectorAllocationClearFinalizesSpool(t, true)
}

func TestConnectorAccountedDeferredCleanupReleasesReusableCache(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newConnectorSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	done, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ReleaseCurrent(0)
	require.Positive(t, mp.CurrNB())

	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	conn := &Connector{cleanupSpool: sp}
	require.NoError(t, conn.SetAllocationAccount(account))
	conn.CleanupDeferredSpool()
	require.Same(t, sp, conn.cleanupSpool)
	require.Zero(t, mp.CurrNB())
	require.NoError(t, conn.ClearAllocationAccount(account))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestConnectorAllocationAccountContract(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 1)
	require.NoError(t, err)
	first, err := registry.Open(1)
	require.NoError(t, err)
	second, err := registry.Open(1)
	require.NoError(t, err)
	conn := &Connector{}
	require.False(t, conn.ActivatesAllocationAccountLifecycle())
	require.ErrorIs(t, conn.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, conn.SetAllocationAccount(first))
	require.ErrorIs(t, conn.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, conn.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	conn.ctr.sp = &pSpool.PipelineSpool{}
	require.ErrorIs(t, conn.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	conn.ctr.sp = nil
	require.NoError(t, conn.ClearAllocationAccount(first))
	require.NoError(t, conn.ClearAllocationAccount(first))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
}

func TestConnectorAllocationClearFinalizesTerminalSpoolPending(t *testing.T) {
	testConnectorAllocationClearFinalizesSpool(t, false)
}

func testConnectorAllocationClearFinalizesSpool(t *testing.T, abort bool) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account,
		1,
		102,
		103,
		104,
		105,
	)
	require.NoError(t, err)
	src := batch.NewOffHeapWithSize(1)
	require.NoError(t, src.SetAllocationAccount(selection))
	src.SetVector(0, vector.NewOffHeapVecWithType(types.T_int64.ToType()))
	require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(1), false, mp))
	src.SetRowCount(1)

	sp := pSpool.InitMyPipelineSpool(mp, 1)
	done, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	conn := &Connector{}
	require.NoError(t, conn.SetAllocationAccount(account))
	if abort {
		got, info := sp.ReceiveBatch(0)
		require.NoError(t, info)
		require.NotNil(t, got)
		conn.ctr.sp = sp
		conn.Reg = process.NewPipelineEdge(1, 0)
		conn.Reset(nil, true, moerr.NewInternalErrorNoCtx("pipeline failed"))
		require.Same(t, sp, conn.cleanupSpool)
		sp.ReleaseCurrent(0)
	} else {
		conn.cleanupSpool = sp
		sp.ForceCleanupAfterTerminalSignal()
	}
	conn.CleanupDeferredSpool()
	require.Same(t, sp, conn.cleanupSpool)
	require.NoError(t, conn.ClearAllocationAccount(account))
	require.Nil(t, conn.cleanupSpool)
	src.Clean(mp)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func newConnectorSpoolTestBatch(t *testing.T, mp *mpool.MPool, rows int) *batch.Batch {
	t.Helper()
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.NoError(t, vector.AppendFixedList[int64](src.Vecs[0], values, nil, mp))
	src.SetRowCount(len(values))
	return src
}
