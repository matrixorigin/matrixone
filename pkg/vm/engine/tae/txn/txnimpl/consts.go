package txnimpl

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"

const (
	TxnEntryCreateDatabase txnif.TxnEntryType = iota
	TxnEntryDropDatabase
	TxnEntryCretaeTable
	TxnEntryDropTable
)
