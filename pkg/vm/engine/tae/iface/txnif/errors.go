package txnif

import "errors"

var (
	TxnRollbacked    = errors.New("tae: rollbacked")
	TxnRWConflictErr = errors.New("tae: r-w conflict error")
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)
