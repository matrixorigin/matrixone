// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	placementTestCN1 = "cn1:6001"
	placementTestCN2 = "cn2:6001"
	placementTestCN3 = "cn3:6001"
)

func newPlacementTestScope(proc *process.Process, magic magicType, addr string, childCount int) *Scope {
	s := newScope(magic)
	s.NodeInfo = engine.Node{Addr: addr, Mcpu: 1}
	s.Proc = proc.NewNoContextChildProc(childCount)
	return s
}

func newPlacementTestDispatch(uid uuid.UUID, targetAddr string) *dispatch.Dispatch {
	dispatchOp := dispatch.NewArgument()
	dispatchOp.RemoteRegs = []colexec.ReceiveInfo{{Uuid: uid, NodeAddr: targetAddr}}
	dispatchOp.AppendChild(value_scan.NewArgument())
	return dispatchOp
}

func newPlacementTestReceiver(proc *process.Process, uid uuid.UUID, fromAddr string) *Scope {
	receiver := newPlacementTestScope(proc, Remote, placementTestCN1, 1)
	receiver.RootOp = merge.NewArgument()
	receiver.RemoteReceivRegInfos = []RemoteReceivRegInfo{{
		Idx:      0,
		Uuid:     uid,
		FromAddr: fromAddr,
	}}
	return receiver
}

func TestNormalizeRemoteDispatchReceiverAddresses(t *testing.T) {
	t.Run("retained dispatch root uses caller as owner", func(t *testing.T) {
		c := NewMockCompile(t)
		uid := uuid.Must(uuid.NewV7())

		root := newPlacementTestScope(c.proc, Merge, placementTestCN1, 0)
		t.Cleanup(root.release)
		root.RootOp = merge.NewArgument()

		localContainer := newPlacementTestScope(c.proc, Remote, placementTestCN1, 0)
		localContainer.RootOp = merge.NewArgument()
		producer := newPlacementTestScope(c.proc, Remote, placementTestCN2, 0)
		producer.RootOp = newPlacementTestDispatch(uid, placementTestCN1)
		receiver := newPlacementTestReceiver(c.proc, uid, placementTestCN2)
		unrelated := newPlacementTestReceiver(c.proc, uuid.Must(uuid.NewV7()), placementTestCN3)
		localContainer.PreScopes = []*Scope{producer, receiver, unrelated}
		root.PreScopes = []*Scope{localContainer}

		normalizeRemoteDispatchReceiverAddresses(root, placementTestCN1)
		require.Equal(t, placementTestCN1, receiver.RemoteReceivRegInfos[0].FromAddr)
		require.Equal(t, placementTestCN3, unrelated.RemoteReceivRegInfos[0].FromAddr)
	})

	t.Run("dispatch below retained connector stays on target", func(t *testing.T) {
		c := NewMockCompile(t)
		uid := uuid.Must(uuid.NewV7())

		root := newPlacementTestScope(c.proc, Merge, placementTestCN1, 0)
		t.Cleanup(root.release)
		root.RootOp = merge.NewArgument()

		producer := newPlacementTestScope(c.proc, Remote, placementTestCN2, 0)
		connectorOp := connector.NewArgument()
		connectorOp.AppendChild(newPlacementTestDispatch(uid, placementTestCN1))
		producer.RootOp = connectorOp
		receiver := newPlacementTestReceiver(c.proc, uid, placementTestCN2)
		root.PreScopes = []*Scope{producer, receiver}

		normalizeRemoteDispatchReceiverAddresses(root, placementTestCN1)
		require.Equal(t, placementTestCN2, receiver.RemoteReceivRegInfos[0].FromAddr)
	})

	t.Run("rejected remote dispatch root stays on target", func(t *testing.T) {
		c := NewMockCompile(t)
		c.proc.Base.TxnOperator = fakeTxnOperator{}
		uid := uuid.Must(uuid.NewV7())

		root := newPlacementTestScope(c.proc, Merge, placementTestCN1, 0)
		t.Cleanup(root.release)
		root.RootOp = merge.NewArgument()

		producer := newPlacementTestScope(c.proc, Remote, placementTestCN2, 0)
		producer.RootOp = newPlacementTestDispatch(uid, placementTestCN1)
		blockingPreScope := newPlacementTestScope(c.proc, Normal, placementTestCN2, 0)
		blockingConnector := connector.NewArgument()
		blockingConnector.Reg = &process.WaitRegister{
			Ch2: make(chan process.PipelineSignal, 1),
		}
		blockingPreScope.RootOp = blockingConnector
		producer.PreScopes = []*Scope{blockingPreScope}

		receiver := newPlacementTestReceiver(c.proc, uid, placementTestCN2)
		root.PreScopes = []*Scope{producer, receiver}

		require.False(t, checkPipelineStandaloneExecutableAtRemote(producer))
		normalizeRemoteDispatchReceiverAddresses(root, placementTestCN1)
		require.Equal(t, placementTestCN2, receiver.RemoteReceivRegInfos[0].FromAddr)
	})

	t.Run("rejected remote nested dispatch stays on target", func(t *testing.T) {
		c := NewMockCompile(t)
		c.proc.Base.TxnOperator = fakeTxnOperator{}
		uid := uuid.Must(uuid.NewV7())

		root := newPlacementTestScope(c.proc, Merge, placementTestCN1, 0)
		t.Cleanup(root.release)
		root.RootOp = merge.NewArgument()

		producer := newPlacementTestScope(c.proc, Remote, placementTestCN2, 0)
		producerRoot := connector.NewArgument()
		producerRoot.AppendChild(newPlacementTestDispatch(uid, placementTestCN1))
		producer.RootOp = producerRoot
		blockingPreScope := newPlacementTestScope(c.proc, Normal, placementTestCN2, 0)
		blockingConnector := connector.NewArgument()
		blockingConnector.Reg = &process.WaitRegister{
			Ch2: make(chan process.PipelineSignal, 1),
		}
		blockingPreScope.RootOp = blockingConnector
		producer.PreScopes = []*Scope{blockingPreScope}

		receiver := newPlacementTestReceiver(c.proc, uid, placementTestCN2)
		root.PreScopes = []*Scope{producer, receiver}

		require.False(t, checkPipelineStandaloneExecutableAtRemote(producer))
		normalizeRemoteDispatchReceiverAddresses(root, placementTestCN1)
		require.Equal(t, placementTestCN2, receiver.RemoteReceivRegInfos[0].FromAddr)
	})

	t.Run("rejected remote scope does not normalize descendants", func(t *testing.T) {
		c := NewMockCompile(t)
		c.proc.Base.TxnOperator = fakeTxnOperator{}
		uid := uuid.Must(uuid.NewV7())

		root := newPlacementTestScope(c.proc, Merge, placementTestCN1, 0)
		t.Cleanup(root.release)
		root.RootOp = merge.NewArgument()

		producer := newPlacementTestScope(c.proc, Remote, placementTestCN2, 0)
		producer.RootOp = merge.NewArgument()
		descendant := newPlacementTestScope(c.proc, Remote, placementTestCN3, 0)
		descendant.RootOp = newPlacementTestDispatch(uid, placementTestCN1)
		blockingPreScope := newPlacementTestScope(c.proc, Normal, placementTestCN2, 0)
		blockingConnector := connector.NewArgument()
		blockingConnector.Reg = &process.WaitRegister{
			Ch2: make(chan process.PipelineSignal, 1),
		}
		blockingPreScope.RootOp = blockingConnector
		producer.PreScopes = []*Scope{descendant, blockingPreScope}

		receiver := newPlacementTestReceiver(c.proc, uid, placementTestCN3)
		root.PreScopes = []*Scope{producer, receiver}

		require.False(t, checkPipelineStandaloneExecutableAtRemote(producer))
		normalizeRemoteDispatchReceiverAddresses(root, placementTestCN1)
		require.Equal(t, placementTestCN3, receiver.RemoteReceivRegInfos[0].FromAddr)
	})

	normalizeRemoteDispatchReceiverAddresses(nil, placementTestCN1)
}
