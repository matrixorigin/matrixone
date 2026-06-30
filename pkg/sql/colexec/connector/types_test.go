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

	done := make(chan struct{})
	go func() {
		conn.Reset(nil, true, moerr.NewInternalErrorNoCtx("cleanup"))
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
	require.ErrorIs(t, info, pSpool.ErrPipelineSpoolAborted)
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
	require.ErrorIs(t, info, pSpool.ErrPipelineSpoolAborted)
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
