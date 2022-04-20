package updates

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type AppendNode struct {
	sync.RWMutex
	commitTs   uint64
	txn        txnif.AsyncTxn
	maxRow     uint32
	controller *MutationController
}

func MockAppendNode(ts uint64, maxRow uint32, controller *MutationController) *AppendNode {
	return &AppendNode{
		commitTs:   ts,
		maxRow:     maxRow,
		controller: controller,
	}
}

func NewAppendNode(txn txnif.AsyncTxn, maxRow uint32, controller *MutationController) *AppendNode {
	n := &AppendNode{
		txn:        txn,
		maxRow:     maxRow,
		commitTs:   txn.GetCommitTS(),
		controller: controller,
	}
	return n
}

func (n *AppendNode) GetCommitTS() uint64 { return n.commitTs }
func (n *AppendNode) GetMaxRow() uint32   { return n.maxRow }

func (n *AppendNode) PrepareCommit() error {
	return nil
}

func (n *AppendNode) ApplyCommit() error {
	n.Lock()
	defer n.Unlock()
	if n.txn == nil {
		panic("not expected")
	}
	n.txn = nil
	if n.controller != nil {
		logutil.Debugf("Set %s MaxCommitTS=%d, MaxVisibleRow=%d", n.controller.meta.String(), n.commitTs, n.maxRow)
		n.controller.SetMaxVisible(n.commitTs)
	}
	return nil
}
