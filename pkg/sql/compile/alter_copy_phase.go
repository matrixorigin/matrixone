// Copyright 2026 Matrix Origin
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

package compile

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type alterCopyCreateScope struct {
	txnID    []byte
	database string
	table    string
	active   atomic.Bool
}

func (s *alterCopyCreateScope) AllowsCopyAlterCreate(txnID []byte, database, table string) bool {
	return s.active.Load() && bytes.Equal(s.txnID, txnID) && s.database == database && s.table == table
}

var alterCopyPhaseHook atomic.Pointer[func(context.Context, string, string, string, client.TxnOperator) error]

// SetAlterCopyPhaseHookForTest observes COPY ALTER boundaries through the real
// SQL entry point. Hooks must filter by database/table and honor cancellation.
// Installers serialize their tests and restore the hook after draining callers.
func SetAlterCopyPhaseHookForTest(hook func(context.Context, string, string, string, client.TxnOperator) error) func() {
	previous := alterCopyPhaseHook.Swap(&hook)
	return func() { alterCopyPhaseHook.Store(previous) }
}

func (c *Compile) observeAlterCopyPhase(database, table, phase string) error {
	if hook := alterCopyPhaseHook.Load(); hook != nil && *hook != nil {
		return (*hook)(c.proc.Ctx, database, table, phase, c.proc.GetTxnOperator())
	}
	return nil
}

// alterCopyPublicationRetryError is private to the COPY ALTER publication
// hand-off. It lets the auto-commit executor distinguish a gate conflict from
// an ordinary SQL error without changing the public error code.
type alterCopyPublicationRetryError struct {
	err   error
	txnID []byte
}

func (e *alterCopyPublicationRetryError) Error() string { return e.err.Error() }
func (e *alterCopyPublicationRetryError) Unwrap() error { return e.err }

func markAlterCopyPublicationRetry(err error, txnID []byte) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*alterCopyPublicationRetryError); ok {
		return err
	}
	if moerr.IsMoErrCode(err, moerr.ErrLockConflict) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
		return &alterCopyPublicationRetryError{err: err, txnID: bytes.Clone(txnID)}
	}
	return err
}

func isAlterCopyPublicationRetry(err error, txnID []byte) bool {
	if err == nil {
		return false
	}
	if marked, ok := err.(*alterCopyPublicationRetryError); ok {
		return len(txnID) != 0 && bytes.Equal(marked.txnID, txnID)
	}
	// A joined cleanup/cancellation error must not be replayed merely because
	// one leaf was a gate conflict. The marker is valid only as the sole cause.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return len(joined.Unwrap()) == 1 && isAlterCopyPublicationRetry(joined.Unwrap()[0], txnID)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isAlterCopyPublicationRetry(wrapped.Unwrap(), txnID)
	}
	var retryErr *alterCopyPublicationRetryError
	return errors.As(err, &retryErr) && len(txnID) != 0 && bytes.Equal(retryErr.txnID, txnID)
}

// IsAlterCopyPublicationRetry reports the private retry marker returned by an
// automatic-commit COPY ALTER when its publication gates must be reacquired.
// Frontend owns the session transaction, so it uses this predicate after the
// first attempt has been rolled back before starting a fresh transaction.
func IsAlterCopyPublicationRetry(err error, txnID []byte) bool {
	return isAlterCopyPublicationRetry(err, txnID)
}

// UnwrapAlterCopyPublicationRetry removes the private coordination marker at
// the owning executor's terminal boundary. Frontend error encoding recognizes
// *moerr.Error directly, so an exhausted retry must expose the original code.
// Preserve aggregated cleanup/cancellation failures intact.
func UnwrapAlterCopyPublicationRetry(err error) error {
	if err == nil {
		return nil
	}
	var marked *alterCopyPublicationRetryError
	if errors.As(err, &marked) && isAlterCopyPublicationRetry(err, marked.txnID) {
		return marked.err
	}
	return err
}

func (c *Compile) markAlterCopyPublicationRetry(err error) error {
	return markAlterCopyPublicationRetry(err, c.proc.GetTxnOperator().Txn().ID)
}
