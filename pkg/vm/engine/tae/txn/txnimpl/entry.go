package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	ETInsertNode = entry.ETCustomizedStart + 1 + iota
	ETTxnRecord
)
