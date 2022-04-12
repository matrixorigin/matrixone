package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type mockTxn struct {
	*txnbase.TxnCtx
}

func newMockTxn() *mockTxn {
	return &mockTxn{
		TxnCtx: txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(),
			common.NextGlobalSeqNum(), nil),
	}
}

func (txn *mockTxn) GetError() error          { return nil }
func (txn *mockTxn) GetStore() txnif.TxnStore { return nil }
func (txn *mockTxn) GetTxnState(bool) int32   { return 0 }
func (txn *mockTxn) IsTerminated(bool) bool   { return false }
