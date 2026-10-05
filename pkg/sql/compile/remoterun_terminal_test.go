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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRemoteNotifyReadsDispatchTerminal(t *testing.T) {
	for _, phase := range []string{"before attach", "after attach", "closed with waiter"} {
		for _, outcome := range []string{"empty success", "source failure", "stopped source failure", "query canceled", "query timeout"} {
			t.Run(phase+"/"+outcome, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				queryCtx := proc.Base.GetContextBase().BuildQueryCtx(context.Background())
				proc.BuildPipelineContext(queryCtx)
				server := colexec.NewServer(proc.GetService())
				uid := uuid.MustParse("00000000-0000-0000-0000-000000028313")
				if phase == "closed with waiter" {
					_, _, _, waiter, _ := server.AttachProcByUuidOrWait(uid)
					t.Cleanup(waiter.Close)
				}
				d := dispatch.NewArgument()
				t.Cleanup(d.Release)
				d.FuncId = dispatch.SendToAllFunc
				d.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
				registration, err := d.RegisterRemoteReceiversWithHandle(proc)
				require.NoError(t, err)
				t.Cleanup(registration.Cleanup)
				var sourceErr error
				if outcome == "source failure" || outcome == "stopped source failure" {
					sourceErr = moerr.NewInternalErrorNoCtx("build failed")
				}
				messageCtx := context.Background()
				if outcome == "query canceled" {
					ctx, cancel := context.WithCancel(messageCtx)
					cancel()
					messageCtx = ctx
				}
				if outcome == "query timeout" {
					ctx, cancel := context.WithDeadline(messageCtx, time.Time{})
					t.Cleanup(cancel)
					messageCtx = ctx
				}
				session := mock_morpc.NewMockClientSession(gomock.NewController(t))
				attached, resume := make(chan struct{}), make(chan struct{})
				var once, attachedOnce sync.Once
				unblock := func() { once.Do(func() { close(resume) }) }
				t.Cleanup(unblock)
				session.EXPECT().SessionCtx().DoAndReturn(func() context.Context {
					if phase == "after attach" {
						attachedOnce.Do(func() { close(attached) })
						<-resume
					}
					return context.Background()
				}).AnyTimes()
				receiver := &messageReceiverOnServer{messageCtx: messageCtx, connectionCtx: context.Background(), messageId: 7, messageTyp: pb.Method_PrepareDoneNotifyMessage, messageUuid: uid, clientSession: session, colexecServer: server, streamLifecycle: &pipelineStreamLifecycle{batchFlow: newPipelineBatchFlow(2, 1024)}}
				if outcome == "stopped source failure" {
					flow := newPipelineBatchFlow(2, 1024)
					flow.stop(process.ErrPipelineStopped)
					receiver.streamLifecycle = &pipelineStreamLifecycle{batchFlow: flow}
				}
				finish := func() {
					proc.Cancel(sourceErr) // Cleanup cancels even on successful termination.
					d.Reset(proc, sourceErr != nil, sourceErr)
					if phase == "closed with waiter" {
						registration.Cleanup()
					}
				}
				done := make(chan error, 1)
				if phase == "after attach" && messageCtx.Err() == nil {
					go func() { done <- handlePipelineMessage(receiver) }()
					select {
					case <-attached:
					case <-time.After(5 * time.Second):
						t.Fatal("notify did not attach")
					}
					finish()
					// An attached notify must use its captured terminal, not reused process fields.
					proc.Ctx = nil
					proc.Cancel = nil
					unblock()
				} else {
					finish()
					done <- handlePipelineMessage(receiver)
				}
				var got error
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("terminal notify did not finish")
				}
				switch outcome {
				case "empty success":
					require.NoError(t, got)
				case "source failure", "stopped source failure":
					require.ErrorIs(t, got, sourceErr)
				case "query canceled":
					require.ErrorIs(t, got, context.Canceled)
				case "query timeout":
					require.ErrorIs(t, got, context.DeadlineExceeded)
				}
				require.NoError(t, queryCtx.Err(), "local completion must not cancel the query")
				wire := new(pb.Message)
				wire.SetMoError(context.Background(), got)
				decoded, hasError := wire.TryToGetMoErr()
				require.Equal(t, outcome != "empty success", hasError)
				if sourceErr != nil && messageCtx.Err() == nil {
					require.Equal(t, sourceErr.Error(), decoded.Error())
				}
				server.RemoveRelatedPipeline(session, receiver.messageId)
			})
		}
	}
}

type observedDoneCallsContext struct {
	context.Context
	calls atomic.Int32
}

func (c *observedDoneCallsContext) Done() <-chan struct{} {
	c.calls.Add(1)
	return c.Context.Done()
}

// TestRemoteNotifyAfterDispatchAttachmentPreservesTerminalOutcome forces the
// PrepareDoneNotify stream through the real dispatch attachment channel before
// source cleanup starts. This is the lifecycle window from issue #28313: an
// empty source cancels its local pipeline during cleanup after the remote
// receiver has attached, while a real source error must survive that cleanup.
func TestRemoteNotifyAfterDispatchAttachmentPreservesTerminalOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sourceErr error
	}{
		{name: "empty source"},
		{name: "source error", sourceErr: moerr.NewDuplicateEntryNoCtx("1", "primary")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			queryCtx := proc.Base.GetContextBase().BuildQueryCtx(context.Background())
			proc.BuildPipelineContext(queryCtx)
			server := colexec.NewServer(proc.GetService())
			uid := uuid.MustParse("00000000-0000-0000-0000-000000028314")

			child := value_scan.NewArgument()
			t.Cleanup(child.Release)
			require.NoError(t, child.Prepare(proc))
			d := dispatch.NewArgument()
			t.Cleanup(d.Release)
			d.FuncId = dispatch.SendToAllFunc
			d.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid}}
			d.AppendChild(child)
			registration, err := d.RegisterRemoteReceiversWithHandle(proc)
			require.NoError(t, err)
			require.NotNil(t, registration)
			t.Cleanup(registration.Cleanup)
			require.NoError(t, d.Prepare(proc))
			// Observe the whole run, including select operand evaluation. Even
			// when Reset wins a select, a forbidden process-context dependency
			// must be detected independently of goroutine scheduling.
			pipelineCtx := &observedDoneCallsContext{Context: proc.Ctx}
			proc.Ctx = pipelineCtx

			messageCtx, cancelMessage := context.WithCancel(context.Background())
			defer cancelMessage()
			session := mock_morpc.NewMockClientSession(gomock.NewController(t))
			session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
			receiver := &messageReceiverOnServer{
				messageCtx:    messageCtx,
				connectionCtx: context.Background(),
				messageId:     9,
				messageTyp:    pb.Method_PrepareDoneNotifyMessage,
				messageUuid:   uid,
				clientSession: session,
				colexecServer: server,
			}
			done := make(chan error, 1)
			go func() { done <- handlePipelineMessage(receiver) }()
			handlerJoined := false
			type callResult struct {
				result vm.CallResult
				err    error
			}
			callDone := make(chan callResult, 1)
			callJoined := false
			defer func() {
				cancelMessage()
				if proc.Cancel != nil {
					proc.Cancel(context.Canceled)
				}
				if !callJoined {
					select {
					case <-callDone:
					case <-time.After(5 * time.Second):
						t.Errorf("dispatch Call goroutine survived test cleanup")
					}
				}
				registration.Cleanup()
				if !handlerJoined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Errorf("remote notify goroutine survived test cleanup")
					}
				}
				server.RemoveRelatedPipeline(session, receiver.messageId)
			}()

			go func() {
				result, err := d.Call(proc)
				callDone <- callResult{result: result, err: err}
			}()
			select {
			case got := <-callDone:
				callJoined = true
				require.NoError(t, got.err)
				require.Equal(t, vm.ExecStop, got.result.Status)
			case <-time.After(5 * time.Second):
				t.Fatal("dispatch did not consume the remote attachment")
			}
			// Pipeline cleanup cancels the local process even on an empty-success
			// path. Reset is the sole terminal owner and must preserve its own
			// outcome instead of exposing that local cancellation to the peer.
			cleanupErr := moerr.NewInternalErrorNoCtx("local pipeline cleanup")
			proc.Cancel(cleanupErr)
			d.Reset(proc, tc.sourceErr != nil, tc.sourceErr)
			registration.Cleanup()

			select {
			case got := <-done:
				handlerJoined = true
				// dispatch.waitRemoteRegsReady and the value-scan child's
				// vm.CancelCheck each read Done once. The terminal-backed
				// handler must never read this reusable process context.
				require.Equal(t, int32(2), pipelineCtx.calls.Load(),
					"terminal-backed notify must not depend on the process context")
				require.NotErrorIs(t, got, cleanupErr)
				if tc.sourceErr == nil {
					require.NoError(t, got)
				} else {
					require.ErrorIs(t, got, tc.sourceErr)
					require.NotErrorIs(t, got, context.Canceled)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("attached remote notify did not observe dispatch terminal")
			}
			require.NoError(t, queryCtx.Err(), "local cleanup must not cancel the query")
		})
	}
}

func TestRemoteNotifyCancellationUsesRegistrationGeneration(t *testing.T) {
	for _, connectionClosed := range []bool{false, true} {
		name := "message canceled"
		if connectionClosed {
			name = "connection closed"
		}
		t.Run(name, func(t *testing.T) {
			server := colexec.NewServer("")
			uid := uuid.MustParse("00000000-0000-0000-0000-000000028313")
			sourceCtx, sourceCancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { sourceCancel(nil) })
			source := &process.Process{Ctx: sourceCtx, Cancel: sourceCancel}
			terminal := colexec.NewRemoteReceiverTerminal(sourceCancel)
			ch := make(process.RemotePipelineInformationChannel)
			require.NoError(t, server.PutProcIntoUuidMapWithTerminal(uid, source, ch, terminal))
			t.Cleanup(func() { server.RemoveUuidsOwned([]uuid.UUID{uid}, ch) })
			externalCtx, externalCancel := context.WithCancel(context.Background())
			t.Cleanup(externalCancel)
			session := mock_morpc.NewMockClientSession(gomock.NewController(t))
			session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
			receiver := &messageReceiverOnServer{messageCtx: context.Background(), connectionCtx: context.Background(), messageId: 8, messageTyp: pb.Method_PrepareDoneNotifyMessage, messageUuid: uid, clientSession: session, colexecServer: server}
			if connectionClosed {
				receiver.connectionCtx = externalCtx
			} else {
				receiver.messageCtx = externalCtx
			}
			done := make(chan error, 1)
			go func() { done <- handlePipelineMessage(receiver) }()
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatal("notify did not reach dispatch")
			}
			// The notify owns the old cancel function, even if Process is reused
			// before the external request's cancellation arrives.
			replacementCtx, replacementCancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { replacementCancel(nil) })
			source.Ctx, source.Cancel = replacementCtx, replacementCancel
			externalCancel()
			select {
			case err := <-done:
				if connectionClosed {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.ErrorIs(t, context.Cause(sourceCtx), err)
			case <-time.After(5 * time.Second):
				t.Fatal("notify did not stop on external cancellation")
			}
			require.NoError(t, replacementCtx.Err())
			server.RemoveRelatedPipeline(session, receiver.messageId)
		})
	}
}

// StopSending must wake a notify whose producer never registered. Use the real
// handler and a durable-block barrier, rather than a timing-dependent sleep.
func TestRemoteNotifyStopsBeforeRegistration(t *testing.T) {
	for _, outcome := range []string{"stop before wait", "stop during wait", "reuse disabled/stop before wait", "reuse disabled/stop during wait", "reuse disabled/published failure", "reuse disabled/query canceled", "reuse disabled/connection closed", "nil contexts", "published failure", "source abort", "abort stop sentinel", "query canceled", "connection closed"} {
		t.Run(outcome, func(t *testing.T) {
			server := colexec.NewServer("")
			synctest.Test(t, func(t *testing.T) {
				reuseDisabled := strings.HasPrefix(outcome, "reuse disabled/")
				outcome = strings.TrimPrefix(outcome, "reuse disabled/")
				messageCtx, cancelMessage := context.WithCancelCause(context.Background())
				defer cancelMessage(context.Canceled)
				connectionCtx, cancelConnection := context.WithCancel(context.Background())
				defer cancelConnection()
				flow := newPipelineBatchFlow(2, 1024)
				uid := uuid.Must(uuid.NewV7())
				receiver := &messageReceiverOnServer{messageTyp: pb.Method_PrepareDoneNotifyMessage, messageUuid: uid, messageCtx: messageCtx, connectionCtx: connectionCtx, colexecServer: server, streamLifecycle: &pipelineStreamLifecycle{batchFlow: flow}}
				if reuseDisabled {
					receiver.streamLifecycle = nil
					defer server.RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
				}
				stop := func() {
					if receiver.streamLifecycle == nil {
						require.NoError(t, handlePipelineMessage(&messageReceiverOnServer{
							messageTyp: pb.Method_StopSending, messageId: receiver.messageId,
							clientSession: receiver.clientSession, colexecServer: server,
						}))
					} else {
						flow.stop(process.ErrPipelineStopped)
					}
				}
				beforeWait := outcome == "stop before wait"
				if outcome == "nil contexts" {
					receiver.messageCtx, receiver.connectionCtx = nil, nil
				}
				if beforeWait {
					stop()
				}
				done := make(chan error, 1)
				go func() { done <- handlePipelineMessage(receiver) }()
				synctest.Wait()
				sourceErr := moerr.NewInternalErrorNoCtx("unattached notify source abort")
				if !beforeWait {
					select {
					case <-done:
						t.Fatal("notify returned before its stop or failure")
					default:
					}
					switch outcome {
					case "published failure":
						// A credit wake is not publication or a stop. Keep the
						// waiter across it so later Finished evidence survives.
						seq, err := flow.reserve(messageCtx, connectionCtx, 10)
						require.NoError(t, err)
						flow.rollback(seq)
						synctest.Wait()
						terminal := colexec.NewRemoteReceiverTerminal(nil)
						terminal.Finish(sourceErr)
						ch := make(process.RemotePipelineInformationChannel, 1)
						require.NoError(t, server.PutProcIntoUuidMapWithTerminal(uid, &process.Process{}, ch, terminal))
						server.CloseRemoteReceivers([]uuid.UUID{uid}, ch)
						stop()
					case "source abort":
						flow.abort(sourceErr)
					case "abort stop sentinel":
						flow.abort(process.ErrPipelineStopped)
					case "query canceled":
						cancelMessage(sourceErr)
						flow.stop(process.ErrPipelineStopped)
					case "connection closed":
						cancelConnection()
						flow.stop(process.ErrPipelineStopped)
					default:
						stop()
					}
					synctest.Wait()
				}
				err := <-done
				switch outcome {
				case "source abort", "query canceled", "published failure":
					require.ErrorIs(t, err, sourceErr)
				case "abort stop sentinel":
					require.ErrorIs(t, err, process.ErrPipelineStopped)
					require.True(t, process.IsPipelineFailure(err))
				case "connection closed":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrStreamClosed))
				default:
					require.NoError(t, err)
				}
				// Retiring this attempt must not consume or tombstone a later producer.
				proc := &process.Process{}
				ch := make(process.RemotePipelineInformationChannel, 1)
				require.NoError(t, server.PutProcIntoUuidMap(uid, proc, ch))
				p, _, state, waiter, _ := server.AttachProcByUuidOrWait(uid)
				waiter.Close()
				require.Equal(t, colexec.RemoteReceiverAttachedNow, state)
				require.Same(t, proc, p)
				server.DeleteUuids([]uuid.UUID{uid})
			})
		})
	}
}

func TestRemoteNotifyDisabledReuseHandoffPreservesFailure(t *testing.T) {
	server := colexec.NewServer("")
	synctest.Test(t, func(t *testing.T) {
		sourceCtx, cancelSource := context.WithCancelCause(context.Background())
		defer cancelSource(context.Canceled)
		siblingCtx, cancelSibling := context.WithCancelCause(context.Background())
		defer cancelSibling(context.Canceled)
		server.RecordPipelineCancellation(nil, 8, cancelSibling)
		defer server.RemoveRelatedPipeline(nil, 7)
		defer server.RemoveRelatedPipeline(nil, 8)
		uid := uuid.Must(uuid.NewV7())
		terminal := colexec.NewRemoteReceiverTerminal(nil)
		ch := make(process.RemotePipelineInformationChannel, 1)
		require.NoError(t, server.PutProcIntoUuidMapWithTerminal(uid, &process.Process{Ctx: sourceCtx}, ch, terminal))
		defer server.RemoveUuidsOwned([]uuid.UUID{uid}, ch)
		receiver := &messageReceiverOnServer{messageTyp: pb.Method_PrepareDoneNotifyMessage, messageId: 7, messageUuid: uid, messageCtx: context.Background(), connectionCtx: context.Background(), colexecServer: server}
		done := make(chan error, 1)
		go func() { done <- handlePipelineMessage(receiver) }()
		synctest.Wait()
		attached := <-ch
		require.NoError(t, handlePipelineMessage(&messageReceiverOnServer{messageTyp: pb.Method_StopSending, messageId: 7, colexecServer: server}))
		require.NotNil(t, attached.ReserveBatch)
		seq, err := attached.ReserveBatch(sourceCtx, 10)
		require.Zero(t, seq)
		require.ErrorIs(t, err, process.ErrPipelineStopped)
		require.True(t, attached.ReceiverStopped())
		require.Zero(t, attached.BatchCredits)
		require.Zero(t, attached.ByteCredits)
		require.Nil(t, attached.RollbackBatch)
		require.NoError(t, sourceCtx.Err(), "the shared producer remains live")
		require.NoError(t, siblingCtx.Err(), "another stream remains live")
		// Retiring this subscription must not turn the producer's actual failure
		// into successful completion of the remote notification.
		sourceErr := moerr.NewInternalErrorNoCtx("producer failed after receiver stop")
		terminal.Finish(sourceErr)
		synctest.Wait()
		require.ErrorIs(t, <-done, sourceErr)
	})
}
