// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type emptyNotifySession struct {
	morpc.ClientSession
	ctx      context.Context
	messages chan *pipeline.Message
}

func (s *emptyNotifySession) SessionCtx() context.Context { return s.ctx }
func (s *emptyNotifySession) Write(_ context.Context, message morpc.Message) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.messages <- message.(*pipeline.Message)
	return nil
}

type emptyNotifyChild struct {
	*value_scan.ValueScan
	err error
}

func (c *emptyNotifyChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), c.err
}

func TestRemoteDispatchRegistrationRollback(t *testing.T) {
	previous := colexec.Get()
	colexec.Set(nil)
	defer colexec.Set(previous)
	colexec.NewServer(nil)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	proc := &process.Process{Ctx: ctx, Cancel: cancel, Base: &process.BaseProcess{}}
	first, second := dispatch.NewArgument(), dispatch.NewArgument()
	defer first.Release()
	defer second.Release()
	leaf := value_scan.NewArgument()
	defer leaf.Release()
	first.AppendChild(leaf)
	first.FuncId, second.FuncId = dispatch.SendToAllFunc, dispatch.SendToAllFunc
	first.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uuid.New()}}
	second.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uuid.New()}}
	defer func() {
		first.Reset(proc, true, context.Canceled)
		second.Reset(proc, true, context.Canceled)
		for _, d := range []*dispatch.Dispatch{first, second} {
			colexec.Get().GetProcByUuid(d.RemoteRegs[0].Uuid, false)
		}
	}()
	require.NoError(t, first.RegisterRemoteReceivers(proc))
	peer := &process.WrapCs{Uid: first.RemoteRegs[0].Uuid, Err: make(chan error, 1)}
	owner, found, _, err := colexec.Get().AttachRemoteReceiver(peer.Uid, peer)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, owner)
	colexec.Get().GetProcByUuid(second.RemoteRegs[0].Uuid, true)
	err = registerRemoteDispatchReceivers(&Scope{RootOp: first, PreScopes: []*Scope{{RootOp: second}}}, proc)
	require.ErrorContains(t, err, "remote receiver already done")
	require.Len(t, peer.Err, 1, "registration failure must retire already published receivers")
	require.ErrorIs(t, <-peer.Err, err)
	_, _, found = colexec.Get().GetProcByUuid(peer.Uid, false)
	require.False(t, found)
	require.NoError(t, registerRemoteDispatchReceivers(nil, proc))
	// A retired registration must not poison the next operator generation.
	require.NoError(t, registerRemoteDispatchReceivers(&Scope{RootOp: first}, proc))
}

func Test_AttachRemoteReceiver_Timeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		previous := colexec.Get()
		colexec.Set(nil)
		defer colexec.Set(previous)
		colexec.NewServer(nil)
		ctx := context.Background()
		receiver := &messageReceiverOnServer{connectionCtx: ctx, messageCtx: ctx}
		uid := uuid.New()
		owner, err := receiver.attachRemoteReceiver(&process.WrapCs{Uid: uid})
		require.Nil(t, owner)
		require.ErrorContains(t, err, "dispatch process not registered within")
		require.Error(t, colexec.Get().PutProcIntoUuidMap(uid, nil, nil))
		_, _, found := colexec.Get().GetProcByUuid(uid, false)
		require.False(t, found)
	})
}

func TestEmptyDispatchNotifyCompletion(t *testing.T) {
	for _, mode := range []string{"late_lookup", "early_notify", "mixed", "child_error", "query_cancel", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				previous := colexec.Get()
				colexec.Set(nil)
				defer colexec.Set(previous)
				colexec.NewServer(nil)
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				proc := &process.Process{Ctx: ctx, Cancel: cancel, Base: &process.BaseProcess{}}
				proc.SetMPool(mp)
				child := &emptyNotifyChild{ValueScan: value_scan.NewArgument()}
				defer child.Release()
				require.NoError(t, child.Prepare(proc))
				defer child.Free(proc, false, nil)
				d := dispatch.NewArgument()
				defer d.Release()
				d.FuncId = dispatch.SendToAllFunc
				d.AppendChild(child)
				sessionCtx, closeSessions := context.WithCancel(context.Background())
				defer closeSessions()
				sessions := make([]*emptyNotifySession, 2)
				done := make(chan error, 2)
				reset := false
				defer func() {
					if !reset {
						d.Reset(proc, true, context.Canceled)
					}
					d.CleanupDeferredSpool()
					closeSessions()
					cancel(nil)
					synctest.Wait()
					for _, reg := range d.RemoteRegs {
						colexec.Get().GetProcByUuid(reg.Uuid, false)
					}
				}()
				for i := range sessions {
					sessions[i] = &emptyNotifySession{ctx: sessionCtx, messages: make(chan *pipeline.Message, 1)}
					d.RemoteRegs = append(d.RemoteRegs, colexec.ReceiveInfo{Uuid: uuid.New()})
				}
				require.NoError(t, d.RegisterRemoteReceivers(proc))
				require.NoError(t, d.Prepare(proc))
				startNotify := func(i int) {
					msg := &pipeline.Message{Id: uint64(i + 1), Cmd: pipeline.Method_PrepareDoneNotifyMessage, Uuid: d.RemoteRegs[i].Uuid[:]}
					go func() {
						done <- CnServerMessageHandler(sessionCtx, "", msg, sessions[i], nil, nil, nil, nil, nil, nil, nil, nil, func() morpc.Message { return &pipeline.Message{} })
					}()
				}
				early := 2
				if mode == "late_lookup" {
					early = 0
				} else if mode == "mixed" {
					early = 1
				}
				for i := 0; i < early; i++ {
					startNotify(i)
				}
				synctest.Wait()
				var terminalErr error
				if mode == "child_error" {
					child.err = moerr.NewInternalErrorNoCtx("empty child failed")
					terminalErr = child.err
				}
				if mode == "query_cancel" {
					terminalErr = moerr.NewInternalErrorNoCtx("empty query canceled")
					cancel(terminalErr)
				}
				if mode == "disconnect" {
					closeSessions()
					synctest.Wait()
					terminalErr = context.Cause(ctx)
					require.ErrorContains(t, terminalErr, "remote receiver connection closed")
				}
				result, err := d.Call(proc)
				if child.err != nil {
					require.ErrorIs(t, err, child.err)
				} else if mode == "query_cancel" || mode == "disconnect" {
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, context.Cause(ctx), terminalErr)
				} else {
					require.NoError(t, err, "healthy empty completion must not wait for a late UUID lookup")
					require.Equal(t, vm.ExecStop, result.Status)
				}
				require.Nil(t, result.Batch)
				for _, s := range sessions {
					require.Empty(t, s.messages, "Call must not publish a terminal response")
				}
				d.Reset(proc, terminalErr != nil, terminalErr)
				reset = true
				synctest.Wait()
				if early < 2 {
					time.Sleep(2 * waitRegistrationTimeout) // 虚拟时间：通知处理尚未开始，查询仍有效。
					require.NoError(t, ctx.Err())
					for i := early; i < 2; i++ {
						startNotify(i)
					}
					synctest.Wait()
				}
				for i, s := range sessions {
					if mode == "disconnect" {
						require.Empty(t, s.messages, "a closed transport cannot publish a successful End")
						continue
					}
					require.Len(t, s.messages, 1, "each notify must receive exactly one terminal response")
					message := <-s.messages
					require.Equal(t, uint64(i+1), message.GetId())
					require.Equal(t, pipeline.Status_MessageEnd, message.GetSid())
					remoteErr, hasError := message.TryToGetMoErr()
					if terminalErr == nil {
						require.False(t, hasError)
						require.NoError(t, remoteErr)
					} else {
						require.True(t, hasError)
						require.ErrorContains(t, remoteErr, terminalErr.Error())
					}
				}
				closeSessions()
				synctest.Wait()
				require.Len(t, done, 2)
				for range 2 {
					if mode == "disconnect" {
						require.ErrorIs(t, <-done, context.Canceled)
					} else {
						require.NoError(t, <-done)
					}
				}
				d.CleanupDeferredSpool()
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}
