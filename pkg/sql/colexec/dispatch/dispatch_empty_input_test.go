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
	"time"

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
	for _, mode := range []string{"queued", "prepared", "local_only", "cancel", "nonempty_timeout", "child_error", "late_lookup", "mixed", "registration_rollback"} {
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
				child := &terminalDispatchChild{ValueScan: value_scan.NewArgument()}
				defer child.Release()
				require.NoError(t, child.Prepare(proc))
				defer child.Free(proc, false, nil)
				d := NewArgument()
				defer d.Release()
				d.FuncId = SendToAllFunc
				d.AppendChild(child)
				var peers []*process.WrapCs
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
				defer func() {
					cancel(nil)
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
				accepted := len(peers)
				if mode == "late_lookup" {
					accepted = 0
				} else if mode == "mixed" || mode == "cancel" || mode == "nonempty_timeout" {
					accepted = 1
				}
				for _, peer := range peers[:accepted] {
					owner, found, _, err := colexec.Get().AttachRemoteReceiver(peer.Uid, peer)
					require.NoError(t, err)
					require.True(t, found)
					require.NotNil(t, owner)
				}
				if mode == "prepared" {
					_, err := d.waitRemoteRegsReady(proc)
					require.NoError(t, err)
					require.Len(t, d.ctr.remoteReceivers, 2)
				}
				var terminalErr error
				switch mode {
				case "cancel":
					terminalErr = moerr.NewInternalErrorNoCtx("cancel after receiver publication")
					cancel(terminalErr)
				case "child_error":
					child.err = moerr.NewInternalErrorNoCtx("child failed before first batch")
					terminalErr = child.err
				case "registration_rollback":
					// 部分注册失败也须把已经发布的 receiver 交给 Reset，不能随队列丢弃。
					ids := []uuid.UUID{peers[0].Uid, peers[1].Uid}
					rollbackRemoteReceiverRegistrations(ids)
					d.drainRemoteReceivers()
					d.ctr.remoteInfo = nil
					terminalErr = moerr.NewInternalErrorNoCtx("registration failed")
				}
				if mode == "nonempty_timeout" {
					_, err := d.waitRemoteRegsReady(proc)
					require.ErrorContains(t, err, "remote receiver not ready")
					terminalErr = err
					require.Len(t, d.ctr.remoteReceivers, 1)
				} else {
					result, err := d.Call(proc)
					if child.err != nil {
						require.ErrorIs(t, err, child.err)
					} else if mode == "cancel" {
						require.ErrorIs(t, err, context.Canceled)
						require.ErrorIs(t, context.Cause(ctx), terminalErr)
					} else {
						require.NoError(t, err)
						require.Equal(t, vm.ExecStop, result.Status)
					}
					require.Nil(t, result.Batch)
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
						time.Sleep(2 * waitRemoteRegTimeout) // 虚拟时间跨过旧 EOS 超时，late lookup 仍正常完成。
						owner, found, _, err := colexec.Get().AttachRemoteReceiver(peer.Uid, peer)
						require.NoError(t, err)
						require.True(t, found)
						require.Nil(t, owner)
						require.Empty(t, peer.Err)
					}
					owner, _, found := colexec.Get().GetProcByUuid(peer.Uid, false)
					require.False(t, found)
					require.Nil(t, owner)
				}
				// 重复 Reset 不得再发送终态或移除下一代注册。
				d.Reset(proc, false, nil)
				for _, peer := range peers {
					require.Empty(t, peer.Err)
				}
				d.CleanupDeferredSpool()
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}
