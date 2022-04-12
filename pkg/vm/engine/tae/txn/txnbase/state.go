package txnbase

import (
	"errors"
	"sync/atomic"
)

var (
	ErrTransferTransactionState = errors.New("tae: transfer transaction state eror")
)

const (
	TSUncommitted int32 = iota
	TSCommitting
	TSCommitted
	TSRollbacking
	TSRollbacked
)

type TxnState struct {
	state int32
}

func (ts *TxnState) ToCommitting() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSUncommitted, TSCommitting) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToCommitted() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSCommitting, TSCommitted) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToRollbacking() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSUncommitted, TSRollbacking) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToRollbacked() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSRollbacking, TSRollbacked) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) IsUncommitted() bool {
	return atomic.LoadInt32(&ts.state) == TSUncommitted
}

func (ts *TxnState) IsCommitted() bool {
	return atomic.LoadInt32(&ts.state) == TSCommitted
}

func (ts *TxnState) IsRollbacked() bool {
	return atomic.LoadInt32(&ts.state) == TSRollbacked
}
