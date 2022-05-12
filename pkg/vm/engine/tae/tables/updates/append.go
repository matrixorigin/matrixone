// Copyright 2021 Matrix Origin
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

package updates

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type AppendNode struct {
	sync.RWMutex
	commitTs   uint64
	txn        txnif.AsyncTxn
	logIndex   *wal.Index
	maxRow     uint32
	controller *MVCCHandle
}

func MockAppendNode(ts uint64, maxRow uint32, controller *MVCCHandle) *AppendNode {
	return &AppendNode{
		commitTs:   ts,
		maxRow:     maxRow,
		controller: controller,
	}
}

func NewAppendNode(txn txnif.AsyncTxn, maxRow uint32, controller *MVCCHandle) *AppendNode {
	ts := uint64(0)
	if txn != nil {
		ts = txn.GetCommitTS()
	}
	n := &AppendNode{
		txn:        txn,
		maxRow:     maxRow,
		commitTs:   ts,
		controller: controller,
	}
	return n
}

func (n *AppendNode) GetCommitTS() uint64 { return n.commitTs }
func (n *AppendNode) GetMaxRow() uint32   { return n.maxRow }

func (n *AppendNode) PrepareCommit() error {
	return nil
}

func (n *AppendNode) ApplyCommit(index *wal.Index) error {
	n.Lock()
	defer n.Unlock()
	if n.txn == nil {
		panic("not expected")
	}
	n.txn = nil
	n.logIndex = index
	if n.controller != nil {
		logutil.Debugf("Set MaxCommitTS=%d, MaxVisibleRow=%d", n.commitTs, n.maxRow)
		n.controller.SetMaxVisible(n.commitTs)
	}
	// logutil.Infof("Apply1Index %s TS=%d", index.String(), n.commitTs)
	return nil
}

func (node *AppendNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, node.maxRow); err != nil {
		return
	}
	n = 4
	return
}

func (node *AppendNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &node.maxRow); err != nil {
		return
	}
	n = 4
	return
}

func (n *AppendNode) PrepareRollback() (err error) { return }
func (n *AppendNode) ApplyRollback() (err error)   { return }
func (n *AppendNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewAppendCmd(id, n)
	return
}
