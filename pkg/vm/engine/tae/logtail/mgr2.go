package logtail

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type Manager struct {
	txnbase.NoopCommitListener
	// clock   *types.TsAlloctor
	storage *TxnTable
}

func NewManager(blockSize int, c clock.Clock) *Manager {
	mgr := &Manager{}
	mgr.storage = NewTxnTable(
		blockSize,
		types.NewTsAlloctor(c),
	)
	return mgr
}

func (mgr *Manager) OnEndPrePrepare(txn txnif.AsyncTxn) {
	mgr.storage.AddTxn(txn)
}

// func (mgr *Manager) GetReader(from, to types.TS) *TableReader {
// 	return nil
// }
