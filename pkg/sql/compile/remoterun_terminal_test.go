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
	"sync"
	"testing"
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
		for _, outcome := range []string{"empty success", "source failure", "query canceled", "query timeout"} {
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
				if outcome == "source failure" {
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
				receiver := &messageReceiverOnServer{messageCtx: messageCtx, connectionCtx: context.Background(), messageId: 7, messageTyp: pb.Method_PrepareDoneNotifyMessage, messageUuid: uid, clientSession: session, colexecServer: server}
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
				case "source failure":
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
	calls chan struct{}
}

func (c *observedDoneCallsContext) Done() <-chan struct{} {
	select {
	case c.calls <- struct{}{}:
	default:
	}
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

			messageCtx, cancelMessage := context.WithCancel(context.Background())
			defer cancelMessage()
			connectionDoneCalls := make(chan struct{}, 4)
			connectionCtx := &observedDoneCallsContext{
				Context: context.Background(),
				calls:   connectionDoneCalls,
			}
			session := mock_morpc.NewMockClientSession(gomock.NewController(t))
			session.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()
			receiver := &messageReceiverOnServer{
				messageCtx:    messageCtx,
				connectionCtx: connectionCtx,
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
			// getRemoteDispatchReceiver reads connection Done once before attach,
			// and the select that publishes the attachment reads it again. The
			// third read is made only by the post-attach terminal select.
			for call := 0; call < 3; call++ {
				select {
				case <-connectionDoneCalls:
				case <-time.After(5 * time.Second):
					t.Fatalf("remote notify did not enter post-attach terminal wait (Done call %d)", call+1)
				}
			}
			select {
			case err := <-done:
				handlerJoined = true
				t.Fatalf("remote notify returned before dispatch cleanup: %v", err)
			default:
			}

			// Pipeline cleanup cancels the local process even on an empty-success
			// path. Reset is the sole terminal owner and must preserve its own
			// outcome instead of exposing that local cancellation to the peer.
			proc.Cancel(tc.sourceErr)
			select {
			case got := <-done:
				handlerJoined = true
				t.Fatalf("local pipeline cancellation escaped before Reset published the terminal: %v", got)
			case <-time.After(20 * time.Millisecond):
			}
			d.Reset(proc, tc.sourceErr != nil, tc.sourceErr)
			registration.Cleanup()

			select {
			case got := <-done:
				handlerJoined = true
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
