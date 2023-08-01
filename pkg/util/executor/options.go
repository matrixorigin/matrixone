// Copyright 2023 Matrix Origin
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

package executor

import (
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"time"
)

// WithDisableIncrStatement disable incr statement
func (opts Options) WithDisableIncrStatement() Options {
	opts.disableIncrStatement = true
	return opts
}

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
func (opts Options) WithAccountID(accountID uint32) Options {
	opts.accountID = accountID
	return opts
}

func (opts Options) WithTimeZone(timeZone *time.Location) Options {
	opts.timeZone = timeZone
	return opts
}

// WithMinCommittedTS use minCommittedTS to exec sql. It will set txn's snapshot to
// minCommittedTS+1, so the txn can see the data which committed at minCommittedTS.
// It's not work if txn operator is set.
func (opts Options) WithMinCommittedTS(ts timestamp.Timestamp) Options {
	opts.minCommittedTS = ts
	return opts
}

// WithWaitCommittedLogApplied if set, the executor will wait all committed log applied
// for the txn.
func (opts Options) WithWaitCommittedLogApplied() Options {
	opts.waitCommittedLogApplied = true
	return opts
}

func (opts Options) WithAutoRetry() Options {
	opts.autoRetry = true
	return opts
}

// Database returns default database
func (opts Options) Database() string {
	return opts.database
}

// AccountID returns account id
func (opts Options) AccountID() uint32 {
	return opts.accountID
}

// HasAccountID returns true if account is set
func (opts Options) HasAccountID() bool {
	return opts.accountID > 0
}

// MinCommittedTS returns min committed ts
func (opts Options) MinCommittedTS() timestamp.Timestamp {
	return opts.minCommittedTS
}

// WaitCommittedLogApplied return true means need wait committed log applied in current cn.
func (opts Options) WaitCommittedLogApplied() bool {
	return opts.waitCommittedLogApplied
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

// DisableIncrStatement returns the txn operator need incr a new input statement
func (opts Options) DisableIncrStatement() bool {
	return opts.disableIncrStatement
}

// GetTimeZone return the time zone of original session
func (opts Options) GetTimeZone() *time.Location {
	return opts.timeZone
}

// AutoRetry return true if auto retry is set
func (opts Options) AutoRetry() bool {
	return opts.autoRetry
}
