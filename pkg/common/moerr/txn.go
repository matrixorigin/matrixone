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

var wholeTxnRollbackCodes = [...]uint16{
	ErrRetryForCNRollingRestart, ErrDeadLockDetected, ErrLockTableBindChanged,
	ErrLockTableNotFound, ErrDeadlockCheckBusy, ErrLockConflict,
	ErrRemoteLockWaitTimeout, ErrLockWaitTimeout, ErrTxnUnknown,
	ErrBackendClosed, ErrNoAvailableBackend, ErrBackendCannotConnect,
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
