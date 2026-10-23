// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Observe the real copy before the outer publication select. No receiver exists
// until cancellation has returned, so that data token cannot be published.
type copiedFrameContext struct {
	context.Context
	mp       *mpool.MPool
	baseline int64
	entered  chan struct{}
	once     sync.Once
}

func (c *copiedFrameContext) Done() <-chan struct{} {
	if c.mp.AllocatedBytes() > c.baseline {
		c.once.Do(func() { close(c.entered) })
	}
	return c.Context.Done()
}

func TestConnectorCancelsQueuedDataWhenReceiverNeverStarts(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	baseCtx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	proc := process.NewTopProcess(baseCtx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(proc.Free)
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(23), false, mp))
	source.SetRowCount(1)
	baseline := mp.AllocatedBytes()
	ctx := &copiedFrameContext{Context: baseCtx, mp: mp, baseline: baseline, entered: make(chan struct{})}
	proc.Ctx = ctx
	// Unbuffered and initially empty: only the real Reset can supply End.
	reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal)}
	op := &Connector{Reg: reg}
	require.NoError(t, op.Prepare(proc))
	sp := op.ctr.sp
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
	type callResult struct {
		result vm.CallResult
		err    error
	}
	results := make(chan callResult, 1)
	joined := make(chan struct{})
	var resetJoined <-chan struct{}
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("sender failed to join; cannot destroy its spool")
			return
		}
		if resetJoined != nil {
			select {
			case <-resetJoined:
			case <-time.After(process.PipelineSignalSendTimeout + time.Second):
				t.Error("Reset failed to join; cannot destroy its spool")
				return
			}
		}
		proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	})
	go func() {
		defer close(joined)
		r, err := op.Call(proc)
		results <- callResult{r, err}
	}()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("sender did not reach publication after cloning its frame")
	}
	cause := errors.New("receiver failed to start")
	cancel(cause)
	select {
	case got := <-results:
		require.ErrorIs(t, got.err, cause)
		require.Nil(t, got.result.Batch)
	case <-time.After(time.Second):
		t.Fatal("unbuffered data publication ignored cancellation")
	}
	<-joined
	require.True(t, sp.PublicationFailed())
	require.Positive(t, mp.AllocatedBytes()-baseline)

	receiver := process.InitPipelineSignalReceiver(baseCtx, []*process.WaitRegister{reg})
	resetDone := make(chan struct{})
	resetJoined = resetDone
	started := time.Now()
	go func() {
		defer close(resetDone)
		op.Reset(proc, true, cause) // The production send/close timeouts remain 30s.
	}()
	drained := receiver.WaitingEndWithTimeout(time.Second)
	if !drained {
		t.Error("real Reset did not deliver End: an orphaned frame consumed its terminal token")
		// Failure-only fixture rescue for the old Reset witness: all readers
		// have stopped, and the old Reset has queued an unnotified End. Read
		// that End to join Reset safely. It is not counted as a passing drain.
		end, err := sp.ReceiveBatch(0)
		require.Nil(t, end)
		require.ErrorIs(t, err, cause)
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("Reset entered its default 30s acknowledgement wait")
	}
	require.True(t, drained)
	require.Zero(t, receiver.State().Alive)
	require.Less(t, time.Since(started), time.Second)
	require.Same(t, sp, op.cleanupSpool)
	op.CleanupDeferredSpool()
	require.Positive(t, mp.AllocatedBytes()-baseline, "ordinary cleanup must retain the unpublished frame")
	proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes())
	proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes(), "joined destruction is idempotent")

	// Reuse the actual Connector after its failed generation. A new spool
	// must return to the ordinary ordered data/End/ack protocol.
	proc.Ctx = context.Background()
	proc.ReplaceTopCtx(proc.Ctx)
	op.Reg = &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
	op.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}), 0)
	require.NoError(t, op.Prepare(proc))
	require.NotSame(t, sp, op.ctr.sp)
	require.False(t, op.ctr.sp.PublicationFailed())
	_, err := op.Call(proc)
	require.NoError(t, err)
	nextReceiver := process.InitPipelineSignalReceiver(proc.Ctx, []*process.WaitRegister{op.Reg})
	borrowed, err := nextReceiver.GetNextBatch(nil)
	require.NoError(t, err)
	require.Equal(t, []int64{23}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
	normalDone := make(chan struct{})
	resetJoined = normalDone
	go func() {
		defer close(normalDone)
		op.Reset(proc, false, nil)
	}()
	require.True(t, nextReceiver.WaitingEndWithTimeout(time.Second))
	select {
	case <-normalDone:
	case <-time.After(time.Second):
		t.Fatal("normal reused spool did not acknowledge End")
	}
	require.Zero(t, nextReceiver.State().Alive)
	require.Nil(t, op.cleanupSpool, "successful generation must keep spool End/ack cleanup")
	require.Equal(t, baseline, mp.AllocatedBytes())
}
