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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// SQLExecutor is used to execute internal sql. All internal requirements for writing
// data should be done using the internal sql executor, otherwise pessimistic transactions
// may not work.
type SQLExecutor interface {
	// Exec new a txn operator, used for debug.
	NewTxnOperator(ctx context.Context) client.TxnOperator
	// Exec exec a sql in a exists txn.
	Exec(ctx context.Context, sql string, opts Options) (Result, error)
	// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
	// in a transaction.
	// NOTE: Pass SQL stmts one by one to TxnExecutor.Exec(). If you pass multiple SQL stmts to
	// TxnExecutor.Exec() as `\n` seperated string, it will only execute the first SQL statement causing Bug.
	ExecTxn(ctx context.Context, execFunc func(txn TxnExecutor) error, opts Options) error
}

// TxnExecutor exec all sql in a transaction.
type TxnExecutor interface {
	Use(db string)
	LockTable(table string) error
	Exec(sql string, options StatementOption) (Result, error)
	Txn() client.TxnOperator
}

// Options execute options.
type Options struct {
	disableIncrStatement    bool
	txnOp                   client.TxnOperator
	database                string
	accountID               uint32
	minCommittedTS          timestamp.Timestamp
	innerTxn                bool
	waitCommittedLogApplied bool
	timeZone                *time.Location
	statementOptions        StatementOption
	txnOpts                 []client.TxnOption
	enableTrace             bool
}

// StatementOption statement execute option.
type StatementOption struct {
	waitPolicy lock.WaitPolicy
}

// Result exec sql result
type Result struct {
	LastInsertID uint64
	AffectedRows uint64
	Batches      []*batch.Batch
	mp           *mpool.MPool
}

// NewResult create result
func NewResult(mp *mpool.MPool) Result {
	return Result{mp: mp}
}
