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

package dispatch

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalDispatchChild struct {
	*value_scan.ValueScan
	err error
}

func (child *terminalDispatchChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), child.err
}

func TestDispatchEmptyInputRemoteLifecycle(t *testing.T) {
	for _, mode := range []string{"attach", "already_attached", "local_only", "cancel", "timeout", "child_error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				previous := colexec.Get()
				colexec.Set(nil)
				defer colexec.Set(previous)
				_ = colexec.NewServer(nil)
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				proc := &process.Process{Ctx: ctx, Cancel: cancel, Base: &process.BaseProcess{}}
				proc.SetMPool(mp)

				child := &terminalDispatchChild{ValueScan: value_scan.NewArgument()}
				defer child.Release()
				require.NoError(t, child.Prepare(proc))
				defer child.Free(proc, false, nil)
				if mode == "child_error" {
					child.err = moerr.NewInternalErrorNoCtx("child failed before its first batch")
				}

				d := NewArgument()
				defer d.Release()
				d.FuncId = SendToAllFunc
				d.AppendChild(child)
				peers := make([]*process.WrapCs, 0, 2)
				if mode == "local_only" {
					d.FuncId = SendToAllLocalFunc
					d.LocalRegs = []*process.WaitRegister{process.NewPipelineEdge(1, 1)}
				} else {
					for range 2 {
						uid := uuid.New()
						d.RemoteRegs = append(d.RemoteRegs, colexec.ReceiveInfo{Uuid: uid})
						peers = append(peers, &process.WrapCs{Uid: uid, Err: make(chan error, 1)})
					}
				}
				// 即使断言提前退出，也先取消并等待 Call，再回收本代资源。
				defer func() {
					cancel(nil)
					synctest.Wait()
					if d.ctr != nil {
						d.Reset(proc, true, context.Canceled)
					}
					d.CleanupDeferredSpool()
					for _, peer := range peers {
						colexec.Get().GetProcByUuid(peer.Uid, false)
					}
				}()
				require.NoError(t, d.RegisterRemoteReceivers(proc))
				require.NoError(t, d.Prepare(proc))
				var notify chan *process.WrapCs
				for _, peer := range peers {
					owner, ch, found := colexec.Get().GetProcByUuid(peer.Uid, false)
					require.True(t, found)
					require.Same(t, proc, owner)
					notify = ch
				}
				if mode == "already_attached" {
					go func() {
						for _, peer := range peers {
							notify <- peer
						}
					}()
					_, err := d.waitRemoteRegsReady(proc)
					require.NoError(t, err)
				}

				type outcome struct {
					result vm.CallResult
					err    error
				}
				done := make(chan outcome, 1)
				go func() {
					result, err := d.Call(proc)
					done <- outcome{result: result, err: err}
				}()
				synctest.Wait()
				var terminalErr error
				accepted := len(peers)
				switch mode {
				case "attach", "cancel", "timeout":
					select {
					case <-done:
						t.Fatal("empty dispatch completed before its receivers attached")
					default:
					}
					notify <- peers[0]
					synctest.Wait()
					require.Len(t, d.ctr.remoteReceivers, 1)
					if mode == "attach" {
						notify <- peers[1]
					} else {
						accepted = 1
						if mode == "cancel" {
							terminalErr = moerr.NewInternalErrorNoCtx("cancel during receiver attachment")
							cancel(terminalErr)
						}
					}
				case "child_error":
					accepted = 0
					terminalErr = child.err
				}
				got := <-done // timeout 分支由 synctest 推进已有计时器，不做真实等待。
				if mode == "timeout" {
					require.ErrorContains(t, got.err, "remote receiver not ready")
					terminalErr = got.err
				} else if mode == "child_error" {
					require.ErrorIs(t, got.err, child.err)
				} else {
					require.NoError(t, got.err)
					require.Equal(t, vm.ExecStop, got.result.Status)
					require.True(t, d.ctr.prepared)
				}
				require.Nil(t, got.result.Batch)
				require.Len(t, d.ctr.remoteReceivers, accepted)
				if mode == "cancel" {
					require.ErrorIs(t, context.Cause(proc.Ctx), terminalErr)
				}
				for _, peer := range peers {
					require.Empty(t, peer.Err, "Call must not publish terminal notifications")
				}
				d.Reset(proc, terminalErr != nil, terminalErr)
				for i, peer := range peers {
					if i < accepted {
						require.Len(t, peer.Err, 1)
						require.ErrorIs(t, <-peer.Err, terminalErr)
					} else {
						require.Empty(t, peer.Err)
					}
					owner, _, _ := colexec.Get().GetProcByUuid(peer.Uid, false)
					require.Nil(t, owner)
				}
				d.CleanupDeferredSpool()
				require.Zero(t, proc.Mp().CurrNB())
			})
		})
	}
}
