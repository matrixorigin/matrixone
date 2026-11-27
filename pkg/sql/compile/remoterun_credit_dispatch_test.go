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
	frames chan morpc.Message
}

func (*dispatchCreditTestSession) SessionCtx() context.Context { return context.Background() }

func (s *dispatchCreditTestSession) Write(_ context.Context, msg morpc.Message) error {
	data, err := msg.(*pipeline.Message).Marshal()
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
		name string
		err  error
	}{
		{"normal receiver stop", nil},
		{"actual peer interruption", moerr.NewQueryInterrupted(base)},
		{"actual foreign-key failure", moerr.NewErrFKNoReferencedRow2(base)},
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
			creditWait := &dispatchCreditWaitContext{Context: ctx, entered: make(chan struct{})}
			flow := newPipelineBatchFlow(1, 128)
			const slowStream, fastStream = 81, 82
			lifecycle, err := registerPipelineStreamLifecycle(slow, slowStream, flow)
			require.NoError(t, err)
			defer lifecycle.remove()
			seq, err := flow.reserve(ctx, ctx, 1)
			require.NoError(t, err)

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
			startNotify := func(peer *dispatchCreditTestSession, connCtx context.Context, stream uint64, id uuid.UUID, life *pipelineStreamLifecycle) <-chan error {
				receiver := &messageReceiverOnServer{
					messageCtx: ctx, connectionCtx: connCtx, messageId: stream,
					messageTyp: pipeline.Method_PrepareDoneNotifyMessage, messageUuid: id,
					clientSession: peer, colexecServer: srv, streamLifecycle: life,
					messageAcquirer: func() morpc.Message { return new(pipeline.Message) },
				}
				return run(func() error {
					err := handlePipelineMessage(receiver)
					if err == nil && life != nil {
						err = waitUntilPipelineBatchFlowDrained(ctx, connCtx, life.batchFlow, nil)
					}
					if err != nil {
						return receiver.sendError(err)
					}
					return receiver.sendEndMessage()
				})
			}
			slowDone := startNotify(slow, creditWait, slowStream, slowID, lifecycle)
			fastDone := startNotify(fast, ctx, fastStream, fastID, nil)
			sendDone := run(func() error {
				_, err := op.Call(proc)
				return err
			})

			select {
			case <-creditWait.entered:
			case <-ctx.Done():
				t.Fatal("dispatch did not wait on the occupied credit window")
			}
			if tc.err != nil {
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

			sendErr := await(sendDone)
			if tc.err == nil {
				require.NoError(t, sendErr)
				require.NoError(t, await(slowDone), "stopped notify must finish before the producer resets")
				require.NoError(t, await(cleanDone), "client cleanup must read a successful terminal frame")
				_, err := op.Call(proc)
				require.NoError(t, err)
				for _, want := range []int32{10, 20} {
					frame := (<-fast.frames).(*pipeline.Message)
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
			require.NoError(t, proc.Ctx.Err(), "only the stopped receiver is retired")
		})
	}
}
