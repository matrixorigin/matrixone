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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"testing/synctest"
	"time"
)

func TestConnectorResetAbortsSpoolWhenTerminalSignalCannotBeDelivered(t *testing.T) {
	warnings := metricv2.PipelineCleanupEventCounter.WithLabelValues("connector_cleanup_send_terminal_signal")
	before := promtestutil.ToFloat64(warnings)
	t.Cleanup(func() { require.Equal(t, before, promtestutil.ToFloat64(warnings)) })
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
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		sp.Abort(nil)
		select {
		case <-reg.Ch2:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Reset did not stop during cleanup")
		}
	})
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

func TestConnectorResetRecordsEndWhenDataChannelIsFull(t *testing.T) {

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
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		sp.Abort(nil)
		select {
		case <-reg.Ch2:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Reset did not stop during cleanup")
		}
	})
	go func() {
		conn.Reset(nil, false, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Connector.Reset blocked while recording End on a full channel")
	}
	require.Nil(t, conn.ctr.sp)
	require.Same(t, sp, conn.cleanupSpool)
	require.Greater(t, mp.CurrNB(), int64(0))
	select {
	case <-reg.Done():
	default:
		t.Fatal("durable End did not close Done")
	}
	require.NoError(t, reg.Err())

	receiver := process.InitPipelineSignalReceiver(context.Background(), []*process.WaitRegister{reg})
	got, info := receiver.GetNextBatch(nil)
	require.NoError(t, info)
	require.NotNil(t, got)
	require.Equal(t, 1024, got.RowCount())
	got, info = receiver.GetNextBatch(nil)
	require.Nil(t, got)
	require.NoError(t, info)

	conn.CleanupDeferredSpool()
	require.Nil(t, conn.cleanupSpool)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestConnectorResetDurableEndWakesReceiverAfterBufferedData(t *testing.T) {

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
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		cancelReceiver()
		<-resultCh
		t.Fatal("receiver remained blocked after durable End")
	}
}

func TestConnectorResetPreservesRecordedTerminalError(t *testing.T) {
	warnings := metricv2.PipelineCleanupEventCounter.WithLabelValues("connector_cleanup_send_terminal_signal")
	before := promtestutil.ToFloat64(warnings)
	t.Cleanup(func() { require.Equal(t, before, promtestutil.ToFloat64(warnings)) })
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
	require.True(t, publishTerminalForTest(reg, process.NewErrorSignal(originalErr)))
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

func TestConnectorResetEndDoesNotWaitForChannelCapacity(t *testing.T) {

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
		t.Fatal("durable End should mark the receiver edge terminal")
	}
	require.NoError(t, reg.Err())
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
	warnings := metricv2.PipelineCleanupEventCounter.WithLabelValues("connector_cleanup_nil_reg")
	before := promtestutil.ToFloat64(warnings)
	t.Cleanup(func() { require.Equal(t, before+1, promtestutil.ToFloat64(warnings)) })
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

func TestConnectorNotificationRollback(t *testing.T) {
	for _, mode := range []string{"delivered", "successful_stop", "execution_failure"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			synctest.Test(t, func(t *testing.T) {
				proc.BuildPipelineContext(context.Background())
				defer proc.Cancel(nil)
				srcMP := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(srcMP)
				src := newConnectorSpoolTestBatch(t, srcMP, 1)
				defer src.Clean(srcMP)
				reg := process.NewPipelineEdge(1, 1)
				conn := &Connector{Reg: reg}
				conn.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{src, src}))
				require.NoError(t, conn.Prepare(proc))
				sp := conn.ctr.sp
				defer sp.Abort(nil)
				_, firstErr := conn.Call(proc)
				require.NoError(t, firstErr) // First real batch fills the notification queue.
				finished := make(chan struct{})
				var result vm.CallResult
				var err error
				go func() { result, err = conn.Call(proc); close(finished) }()
				synctest.Wait()
				require.Greater(t, proc.Mp().CurrNB(), int64(0))
				select {
				case <-finished:
					t.Fatal("Call must be blocked by the full notification queue")
				default:
				}
				var terminalErr error
				if mode == "delivered" {
					first := <-reg.Ch2
					first.Action()
					sp.ReleaseCurrent(0)
				} else if mode == "successful_stop" {
					proc.Cancel(process.ErrPipelineStopped)
				} else {
					terminalErr = moerr.NewInternalErrorNoCtx("execution failed")
					proc.Cancel(terminalErr)
				}
				<-finished
				require.NoError(t, err)
				if mode != "delivered" {
					require.Equal(t, vm.ExecStop, result.Status)
				}
				conn.Reset(proc, terminalErr != nil, terminalErr)
				conn.CleanupDeferredSpool()
				reg.DrainForCleanup()
				require.Zero(t, proc.Mp().CurrNB(), "unnotified batch must be reclaimed")
			})
		})
	}
}

func publishTerminalForTest(edge *process.PipelineEdge, signal process.PipelineSignal) bool {
	_, ok := edge.PublishTerminal(signal)
	return ok
}
