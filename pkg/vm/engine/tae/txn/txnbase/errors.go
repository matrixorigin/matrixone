package txnbase

import "errors"

var (
	ErrTxnAlreadyCommitted  = errors.New("tae: txn already committed")
	ErrTxnNotCommitting     = errors.New("tae: txn not commiting")
	ErrTxnNotRollbacking    = errors.New("tae: txn not rollbacking")
	ErrTxnNotActive         = errors.New("tae: txn not active")
	ErrTxnCannotRollback    = errors.New("tae: txn cannot txn rollback")
	ErrTxnDifferentDatabase = errors.New("tae: different database used")

	ErrNotFound   = errors.New("tae: not found")
	ErrDuplicated = errors.New("tae: duplicated ")

	ErrDDLDropCreated = errors.New("tae: DDL cannot drop created in a txn")
)
