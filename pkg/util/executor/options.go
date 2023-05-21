package executor

import (
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// WithTxn exec sql in a exists txn
func (opts Options) WithTxn(txnOp client.TxnOperator) Options {
	opts.txnOp = txnOp
	return opts
}

// WithDatabase exec sql in database
func (opts Options) WithDatabase(database string) Options {
	opts.database = database
	return opts
}

// WithAccountID execute sql in account
func (opts Options) WithAccountID(accoundID uint32) Options {
	opts.accoundID = accoundID
	return opts
}

// WithMinCommittedTS use minCommittedTS to exec sql. It will set txn's snapshot to
// minCommittedTS+1, so the txn can see the data which committed at minCommittedTS.
// It's not work if txn operator is set.
func (opts Options) WithMinCommittedTS(ts timestamp.Timestamp) Options {
	opts.minCommittedTS = ts
	return opts
}

// Database returns default database
func (opts Options) Database() string {
	return opts.database
}

// AccoundID returns account id
func (opts Options) AccoundID() uint32 {
	return opts.accoundID
}

// HasAccoundID returns true if account is set
func (opts Options) HasAccoundID() bool {
	return opts.accoundID > 0
}

// MinCommittedTS returns min committed ts
func (opts Options) MinCommittedTS() timestamp.Timestamp {
	return opts.minCommittedTS
}

// HasExistsTxn return true if a exists txn is set
func (opts Options) HasExistsTxn() bool {
	return opts.txnOp != nil
}

// ExistsTxn return true if the txn is a exists txn which is not create by executor
func (opts Options) ExistsTxn() bool {
	return !opts.innerTxn
}

// SetupNewTxn setup new txn
func (opts Options) SetupNewTxn(txnOp client.TxnOperator) Options {
	opts.txnOp = txnOp
	opts.innerTxn = true
	return opts
}

// Txn returns the txn operator
func (opts Options) Txn() client.TxnOperator {
	return opts.txnOp
}
