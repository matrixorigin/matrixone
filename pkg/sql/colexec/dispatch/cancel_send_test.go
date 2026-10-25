// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package dispatch

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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type queuedFrameContext struct {
	context.Context
	mp       *mpool.MPool
	baseline int64
	entered  chan struct{}
	once     sync.Once
}

func (c *queuedFrameContext) Done() <-chan struct{} {
	if c.mp.AllocatedBytes() > c.baseline {
		c.once.Do(func() { close(c.entered) })
	}
	return c.Context.Done()
}

func dispatchPublicationInput(t *testing.T, mp *mpool.MPool) *batch.Batch {
	t.Helper()
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(29), false, mp))
	source.SetRowCount(1)
	return source
}

func TestLocalDispatchDataSendsObserveCancellation(t *testing.T) {
	for _, mode := range []string{"all", "any", "index", "local-match", "multi-match"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			baseCtx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(context.Canceled) })
			proc := process.NewTopProcess(baseCtx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			t.Cleanup(proc.Free)
			source := dispatchPublicationInput(t, mp)
			baseline := mp.AllocatedBytes()
			ctx := &queuedFrameContext{Context: baseCtx, mp: mp, baseline: baseline, entered: make(chan struct{})}
			proc.Ctx = ctx
			// No dummy End: Reset is the only terminal-message producer.
			reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal)}
			op := &Dispatch{FuncId: SendToAllLocalFunc, LocalRegs: []*process.WaitRegister{reg}, ShuffleRegIdxLocal: []int{0}}
			if mode == "any" {
				op.FuncId = SendToAnyLocalFunc
			} else if mode != "all" {
				op.FuncId = ShuffleToAllFunc
			}
			require.NoError(t, op.Prepare(proc))
			result := make(chan error, 1)
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
					case <-time.After(time.Second):
						t.Error("Reset failed to join; cannot destroy its spool")
						return
					}
				}
				proc.Base.ClosePipelineSpoolsAfterReadersStopped()
			})
			go func() {
				defer close(joined)
				var err error
				switch mode {
				case "all":
					_, err = sendToAllLocalFunc(source, op, proc)
				case "any":
					_, err = sendToAnyLocalFunc(source, op, proc)
				case "index":
					err = sendBatToIndex(op, proc, source, 0)
				case "local-match":
					err = sendBatToLocalMatchedReg(op, proc, source, 0)
				case "multi-match":
					err = sendBatToMultiMatchedReg(op, proc, source, 0)
				}
				result <- err
			}()
			select {
			case <-ctx.entered:
			case <-time.After(time.Second):
				t.Fatal("sender did not reach publication after cloning its frame")
			}
			cause := errors.New("receiver did not start")
			cancel(cause)
			select {
			case err := <-result:
				if mode == "all" || mode == "any" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("data publication ignored cancellation")
			}
			<-joined
			receiver := process.InitPipelineSignalReceiver(baseCtx, []*process.WaitRegister{reg})
			resetDone := make(chan struct{})
			resetJoined = resetDone
			started := time.Now()
			go func() {
				defer close(resetDone)
				op.Reset(proc, true, cause)
			}()
			drained := receiver.WaitingEndWithTimeout(time.Second)

			select {
			case <-resetDone:
			case <-time.After(time.Second):
				t.Fatal("Reset entered its default 30s acknowledgement wait")
			}
			require.True(t, drained)
			require.ErrorIs(t, reg.Err(), cause, "Reset preserves the failed-start cause durably")
			require.Zero(t, receiver.State().Alive)
			require.Less(t, time.Since(started), time.Second)
			require.Nil(t, op.cleanupSpool, "error terminal aborts pending refs")
			op.CleanupDeferredSpool()
			require.Equal(t, baseline, mp.AllocatedBytes(), "unnotified references are discarded before root release")
			proc.Base.ClosePipelineSpoolsAfterReadersStopped()
			require.Equal(t, baseline, mp.AllocatedBytes())
			proc.Base.ClosePipelineSpoolsAfterReadersStopped()
			require.Equal(t, baseline, mp.AllocatedBytes())
		})
	}
}

func TestLocalBroadcastFailedPublicationPreservesBorrowedFrameThroughReset(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	proc := process.NewTopProcess(ctx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(proc.Free)
	source := dispatchPublicationInput(t, mp)
	baseline := mp.AllocatedBytes()
	// First publication can succeed; its reader holds the real clone while
	// the second, unbuffered publication has no receiver and is canceled.
	regs := []*process.WaitRegister{
		{Ch2: make(chan process.PipelineSignal, 1)},
		{Ch2: make(chan process.PipelineSignal)},
	}
	op := &Dispatch{FuncId: SendToAllLocalFunc, LocalRegs: regs}
	require.NoError(t, op.Prepare(proc))
	receiveCtx, stopReceive := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(stopReceive)
	first := process.InitPipelineSignalReceiver(receiveCtx, regs[:1])
	second := process.InitPipelineSignalReceiver(context.Background(), regs[1:])
	result := make(chan error, 1)
	joined := make(chan struct{})
	var resetJoined <-chan struct{}
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("broadcast sender failed to join")
			return
		}
		if resetJoined != nil {
			select {
			case <-resetJoined:
			case <-time.After(time.Second):
				t.Error("broadcast Reset failed to join")
				return
			}
		}
		proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	})
	go func() {
		defer close(joined)
		_, err := sendToAllLocalFunc(source, op, proc)
		result <- err
	}()
	borrowed, err := first.GetNextBatch(nil)
	require.NoError(t, err)
	require.NotNil(t, borrowed, "first publication must reach its live receiver")
	require.Equal(t, []int64{29}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
	cause := errors.New("second broadcast publication canceled")
	cancel(cause)
	select {
	case err := <-result:
		require.NoError(t, err, "partial broadcast closure is a control stop; Reset publishes its durable error")
	case <-time.After(time.Second):
		t.Fatal("partial broadcast publication ignored cancellation")
	}
	<-joined
	resetDone := make(chan struct{})
	resetJoined = resetDone
	started := time.Now()
	go func() {
		defer close(resetDone)
		op.Reset(proc, true, cause)
	}()
	secondDrained := second.WaitingEndWithTimeout(time.Second)

	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("Reset did not return after durable terminal publication")
	}
	require.True(t, secondDrained)
	for _, reg := range regs {
		require.ErrorIs(t, reg.Err(), cause)
	}
	require.Zero(t, second.State().Alive)
	require.Less(t, time.Since(started), time.Second)
	require.Nil(t, op.cleanupSpool, "error terminal aborts pending refs")
	op.CleanupDeferredSpool()
	// First has not released its data yet. Reset/Abort must preserve its
	// borrowed vector after DiscardUnsent rolls back the missing recipient.
	require.Equal(t, []int64{29}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
	require.Positive(t, mp.AllocatedBytes()-baseline)
	require.True(t, first.WaitingEndWithTimeout(time.Second))
	require.Zero(t, first.State().Alive)
	require.Equal(t, baseline, mp.AllocatedBytes(), "the last notified borrower releases the shared data after rollback")
	proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes())
	proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes())
}
