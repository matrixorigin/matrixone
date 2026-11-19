// Copyright 2021 - 2022 Matrix Origin
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

package moerr

// FindTxnUnknown preserves an unknown commit outcome even when another
// failure was wrapped or joined before it. Such an outcome must not be
// treated as proof that a business transaction rolled back.
func FindTxnUnknown(err error) *Error {
	if e, ok := err.(*Error); ok && e != nil && e.ErrorCode() == ErrTxnUnknown {
		return e
	}
	if custom, ok := err.(interface{ As(any) bool }); ok {
		var e *Error
		if custom.As(&e) && e != nil && e.ErrorCode() == ErrTxnUnknown {
			return e
		}
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range e.Unwrap() {
			if unknown := FindTxnUnknown(cause); unknown != nil {
				return unknown
			}
		}
	case interface{ Unwrap() error }:
		return FindTxnUnknown(e.Unwrap())
	}
	return nil
}

// wholeTxnRollbackCodes classifies infrastructure failures that doom the whole
// owning transaction. The lock-wait-timeout family (ErrLockWaitTimeout,
// ErrRemoteLockWaitTimeout) is deliberately NOT in this list: matching MySQL's
// default (innodb_rollback_on_timeout=OFF), an expired lock wait rolls back
// only the failing statement and leaves the transaction open, so the session
// can COMMIT work done before the contention. A deadlock still rolls back the
// whole transaction, as InnoDB does.
var wholeTxnRollbackCodes = [...]uint16{
	ErrRetryForCNRollingRestart, ErrDeadLockDetected, ErrLockTableBindChanged,
	ErrLockTableNotFound, ErrDeadlockCheckBusy, ErrLockConflict,
	ErrTxnUnknown, ErrBackendClosed, ErrNoAvailableBackend,
	ErrBackendCannotConnect, ErrTxnWriteConflict,
}

// WholeTxnRollbackCodes returns a copy for diagnostics/tests without exposing
// the shared transaction-failure policy to mutation.
func WholeTxnRollbackCodes() []uint16 {
	return append([]uint16(nil), wholeTxnRollbackCodes[:]...)
}

// MustRollbackTxn classifies infrastructure errors that doom the whole owning
// transaction, including fatal errors wrapped or joined with cleanup errors.
func MustRollbackTxn(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok && e != nil {
		for _, code := range wholeTxnRollbackCodes {
			if e.ErrorCode() == code {
				return true
			}
		}
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range e.Unwrap() {
			if MustRollbackTxn(cause) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return MustRollbackTxn(e.Unwrap())
	}
	return false
}
