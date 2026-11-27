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

package compile

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type dispatchCreditWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *dispatchCreditWaitContext) Done() <-chan struct{} {
	// The notify handler also observes the connection. Signal only when the
	// dispatch sender is actually waiting for the occupied credit window.
	var callers [12]uintptr
	frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".(*pipelineBatchFlow).reserveOrStop") {
			c.once.Do(func() { close(c.entered) })
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Done()
}

type dispatchCreditTestSession struct {
	morpc.ClientSession
	frames     chan morpc.Message
	beforeData func() error
}

func (*dispatchCreditTestSession) SessionCtx() context.Context { return context.Background() }

func (s *dispatchCreditTestSession) Write(_ context.Context, msg morpc.Message) error {
	frame := msg.(*pipeline.Message)
	if frame.IsBatchMessage() && s.beforeData != nil {
		if err := s.beforeData(); err != nil {
			return err
		}
	}
	data, err := frame.Marshal()
	if err != nil {
		return err
	}
	msgCopy := new(pipeline.Message)
	if err := msgCopy.Unmarshal(data); err != nil {
		return err
	}
	s.frames <- msgCopy
	return nil
}

func TestDispatchCreditStopKeepsOtherReceiverAndNotifyOutcome(t *testing.T) {
	base := context.Background()
	for _, tc := range []struct {
		name        string
		err         error
		idle        bool
		inFlight    bool
		killRequest bool
	}{
		{name: "normal receiver stop"},
		{name: "idle receiver stop survives request and connection cleanup", idle: true},
		{name: "in-flight write precedes successful end", inFlight: true},
		{name: "in-flight write failure survives stop", inFlight: true, err: moerr.NewErrFKNoReferencedRow2(base)},
		{name: "actual peer interruption", err: moerr.NewQueryInterrupted(base)},
		{name: "actual foreign-key failure", err: moerr.NewErrFKNoReferencedRow2(base)},
		{name: "request kill before receiver stop", err: moerr.NewQueryInterrupted(base), killRequest: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(base, 3*time.Second)
			defer cancel()
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.BuildPipelineContext(ctx)
			srv := colexec.NewServer(proc.GetService())
			slowID, fastID := uuid.New(), uuid.New()
			input := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
				colexec.MakeMockPartitionBatchs(10, proc.Mp()),
				colexec.MakeMockPartitionBatchs(20, proc.Mp()),
			})
			defer input.Free(proc, false, nil)
			op := dispatch.NewArgument()
			defer op.Release()
			op.FuncId = dispatch.SendToAllFunc
			op.RemoteRegs = []colexec.ReceiveInfo{{Uuid: slowID}, {Uuid: fastID}}
			op.AppendChild(input)
			require.NoError(t, op.Prepare(proc))
			defer proc.Base.ClosePipelineSpoolsAfterReadersStopped()

			slow := &dispatchCreditTestSession{frames: make(chan morpc.Message, 4)}
			fast := &dispatchCreditTestSession{frames: make(chan morpc.Message, 4)}
			slowRequest, cancelSlowRequest := context.WithCancelCause(ctx)
			defer cancelSlowRequest(nil)
			connection, disconnectSlow := context.WithCancel(ctx)
			defer disconnectSlow()
			creditWait := &dispatchCreditWaitContext{Context: connection, entered: make(chan struct{})}
			flow := newPipelineBatchFlow(1, 128)
			const slowStream, fastStream = 81, 82
			lifecycle, err := registerPipelineStreamLifecycle(slow, slowStream, flow)
			require.NoError(t, err)
			defer lifecycle.remove()
			var seq uint64
			writeEntered, releaseWrite := make(chan struct{}), make(chan struct{})
			if tc.inFlight {
				slow.beforeData = func() error {
					close(writeEntered)
					select {
					case <-releaseWrite:
						return tc.err
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			if !tc.idle && !tc.inFlight {
				seq, err = flow.reserve(ctx, ctx, 1)
				require.NoError(t, err)
			}

			var workers sync.WaitGroup
			defer func() {
				cancel()
				workers.Wait()
				srv.RemoveRelatedPipeline(slow, slowStream)
				srv.RemoveRelatedPipeline(fast, fastStream)
			}()
			run := func(fn func() error) <-chan error {
				done := make(chan error, 1)
				workers.Go(func() { done <- fn() })
				return done
			}
			startNotify := func(peer *dispatchCreditTestSession, requestCtx, connCtx context.Context, stream uint64, id uuid.UUID, life *pipelineStreamLifecycle) <-chan error {
				receiver := &messageReceiverOnServer{
					messageCtx: requestCtx, connectionCtx: connCtx, messageId: stream,
					messageTyp: pipeline.Method_PrepareDoneNotifyMessage, messageUuid: id,
					clientSession: peer, colexecServer: srv, streamLifecycle: life,
					messageAcquirer: func() morpc.Message { return new(pipeline.Message) },
				}
				return run(func() error {
					err := handlePipelineMessage(receiver)
					if err == nil && life != nil {
						err = waitUntilPipelineBatchFlowDrained(requestCtx, connCtx, life.batchFlow, nil)
					}
					if err != nil {
						return receiver.sendError(err)
					}
					return receiver.sendEndMessage()
				})
			}
			slowDone := startNotify(slow, slowRequest, creditWait, slowStream, slowID, lifecycle)
			fastDone := startNotify(fast, ctx, ctx, fastStream, fastID, nil)
			sendDone := run(func() error {
				_, err := op.Call(proc)
				return err
			})

			await := func(done <-chan error) error {
				t.Helper()
				select {
				case err := <-done:
					return err
				case <-ctx.Done():
					t.Fatal("dispatch or notify did not finish")
					return nil
				}
			}
			nextFrame := func(peer *dispatchCreditTestSession) *pipeline.Message {
				t.Helper()
				select {
				case frame := <-peer.frames:
					return frame.(*pipeline.Message)
				case <-ctx.Done():
					t.Fatal("expected a data frame")
					return nil
				}
			}

			var sendErr error
			if tc.idle {
				sendErr = await(sendDone)
				require.NoError(t, sendErr)
				first := nextFrame(slow)
				require.True(t, first.IsBatchMessage())
				seq = first.GetBatchSequence()
			} else if tc.inFlight {
				select {
				case <-writeEntered:
				case <-ctx.Done():
					t.Fatal("dispatch did not reach the reserved batch write")
				}
				require.NoError(t, handlePipelineMessage(&messageReceiverOnServer{
					messageCtx: ctx, messageTyp: pipeline.Method_StopSending, messageId: slowStream,
					clientSession: slow, colexecServer: srv,
				}))
				require.Empty(t, slow.frames, "the terminal frame must wait behind the in-flight write")
				close(releaseWrite)
				sendErr = await(sendDone)
				if tc.err == nil {
					first := nextFrame(slow)
					require.True(t, first.IsBatchMessage(), "data must arrive before the terminal frame")
					seq = first.GetBatchSequence()
				} else {
					seq = 1
				}
			} else {
				select {
				case <-creditWait.entered:
				case <-ctx.Done():
					t.Fatal("dispatch did not wait on the occupied credit window")
				}
			}
			if tc.killRequest {
				cancelSlowRequest(tc.err)
			} else if tc.err != nil && !tc.inFlight {
				flow.abort(tc.err)
			}

			ctrl := gomock.NewController(t)
			stream := mock_morpc.NewMockStream(ctrl)
			stream.EXPECT().ID().Return(uint64(slowStream))
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg morpc.Message) error {
				request := msg.(*pipeline.Message)
				return handlePipelineMessage(&messageReceiverOnServer{
					messageCtx: ctx, messageTyp: request.Cmd, messageId: request.Id,
					clientSession: slow, colexecServer: srv,
				})
			})
			stream.EXPECT().Close(true).Return(nil)
			cleanDone := run(func() error {
				result := notifyMessageResult{sender: &messageSenderOnClient{
					ctx: ctx, streamSender: stream, receiveCh: slow.frames,
				}}
				return result.clean(proc)
			})
			if !tc.idle && !tc.inFlight {
				sendErr = await(sendDone)
			}
			if tc.err == nil {
				require.NoError(t, sendErr)
				require.NoError(t, await(slowDone), "stopped notify must finish before the producer resets")
				require.NoError(t, await(cleanDone), "client cleanup must read a successful terminal frame")
				cancelSlowRequest(nil)
				disconnectSlow()
				_, err := op.Call(proc)
				require.NoError(t, err)
				for _, want := range []int32{10, 20} {
					frame := nextFrame(fast)
					require.True(t, frame.IsBatchMessage())
					bat, err := decodeBatch(proc.Mp(), frame.GetData())
					require.NoError(t, err)
					value := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])[0]
					bat.Clean(proc.Mp())
					require.Equal(t, want, value)
				}
				require.Empty(t, slow.frames)
			} else {
				require.ErrorIs(t, sendErr, tc.err)
			}
			op.Reset(proc, sendErr != nil, sendErr)
			if tc.err != nil {
				require.NoError(t, await(slowDone))
				require.True(t, moerr.IsMoErrCode(await(cleanDone), tc.err.(*moerr.Error).ErrorCode()))
			}
			require.NoError(t, await(fastDone))
			require.NoError(t, handlePipelineBatchAck(&pipeline.Message{Id: slowStream, BatchAckSequence: seq}, slow))
			if tc.killRequest {
				require.ErrorIs(t, context.Cause(proc.Ctx), tc.err)
			} else {
				require.NoError(t, proc.Ctx.Err(), "only the stopped receiver is retired")
			}
		})
	}
}
