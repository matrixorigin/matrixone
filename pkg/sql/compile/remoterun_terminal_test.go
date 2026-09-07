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
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
