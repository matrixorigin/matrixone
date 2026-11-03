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

import "testing"

// TestNewDataBranchConflict pins the wire contract of the DATA BRANCH
// merge/pick write-write conflict error (issue #31614): a dedicated
// non-internal moerr code that maps to MySQL ER_UNKNOWN_ERROR (1105)
// with SQLSTATE HY000, renders the two "conflict: ..." message bodies,
// and is never a member of an internal/transaction-retry/duplicate-entry
// family, so no consumer can classify it as retryable (or as 1062/1213).
func TestNewDataBranchConflict(t *testing.T) {
	// Short body: used by the hashDiff FAIL control flow.
	conflict := NewDataBranchConflictNoCtxf(
		"%s %s and %s %s on pk(%v)",
		"s_cf", "UPDATE", "s_mf", "UPDATE", 1)
	if conflict.ErrorCode() != ErrDataBranchConflict {
		t.Fatalf("ErrorCode() = %d, want ErrDataBranchConflict (%d)",
			conflict.ErrorCode(), ErrDataBranchConflict)
	}
	if conflict.ErrorCode() == ErrInternal {
		t.Fatalf("data branch conflict must not be an internal error")
	}
	if got := conflict.MySQLCode(); got != ER_UNKNOWN_ERROR {
		t.Fatalf("MySQLCode() = %d, want ER_UNKNOWN_ERROR", got)
	}
	if got := conflict.MySQLCode(); got != 1105 {
		t.Fatalf("MySQLCode() = %d, want the MySQL wire code 1105", got)
	}
	if got := conflict.SqlState(); got != MySQLDefaultSqlState {
		t.Fatalf("SqlState() = %q, want the default SQLSTATE", got)
	}
	if got := conflict.SqlState(); got != "HY000" {
		t.Fatalf("SqlState() = %q, want HY000 on the wire", got)
	}
	// The catalog template in errorMsgRefer supplies the leading
	// "conflict: "; the body equals the historic rendering without the
	// internal-error prefix.
	if got, want := conflict.Error(),
		"conflict: s_cf UPDATE and s_mf UPDATE on pk(1)"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	// Long body: "with different values", used by the insert-insert
	// merge path and the shared pick conflict path.
	conflictDifferent := NewDataBranchConflictNoCtxf(
		"%s %s and %s %s on pk(%v) with different values",
		"s_cf", "INSERT", "s_mf", "INSERT", "101,'a01'")
	if got, want := conflictDifferent.Error(),
		"conflict: s_cf INSERT and s_mf INSERT on pk(101,'a01') with different values"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if conflictDifferent.ErrorCode() != ErrDataBranchConflict {
		t.Fatalf("ErrorCode() = %d, want ErrDataBranchConflict",
			conflictDifferent.ErrorCode())
	}

	// Non-retryable closure: the code must not match any internal,
	// transaction-retry, lock-restart, or duplicate-entry family.
	for _, code := range []uint16{
		ErrInternal,
		ErrTxnWriteConflict, ErrTxnWWConflict, ErrTxnUnknown, ErrTxnClosed,
		ErrDeadLockDetected, ErrLockTableBindChanged,
		ErrDuplicateEntry,
	} {
		if IsMoErrCode(conflict, code) {
			t.Fatalf("data branch conflict must never classify as code %d", code)
		}
	}
}
