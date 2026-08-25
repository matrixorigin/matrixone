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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// TestSessionRollsBackTxnOnError: the opt-in is per session, applies only to a
// duplicate key, and is off unless the session asks for it. Default off is
// MySQL's behaviour -- a duplicate key rolls back the statement, not the
// transaction.
func TestSessionRollsBackTxnOnError(t *testing.T) {
	dup := moerr.NewDuplicateEntryNoCtx("1", "a")
	other := moerr.NewInternalErrorNoCtx("something else")

	// a nil session, or no error, never opts in
	require.False(t, sessionRollsBackTxnOnError(nil, dup))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()

	// default: MySQL behaviour
	require.False(t, sessionRollsBackTxnOnError(ses, dup),
		"the default must be MySQL's: a duplicate key rolls back the statement only")
	require.False(t, sessionRollsBackTxnOnError(ses, nil))

	// opted in
	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_duplicate_key", int64(1)))
	require.True(t, sessionRollsBackTxnOnError(ses, dup))

	// still scoped to the duplicate key: an unrelated error is unaffected, and
	// keeps whatever the static errCodeRollbackWholeTxn set decides
	require.False(t, sessionRollsBackTxnOnError(ses, other))

	// and back off again
	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_duplicate_key", int64(0)))
	require.False(t, sessionRollsBackTxnOnError(ses, dup))
}

// TestDuplicateKeyNotInStaticRollbackSet: the static set is infrastructure
// failures after which a transaction cannot continue. A duplicate key is a
// data error and must not be in it, or the session variable would have nothing
// to control and MySQL compatibility would be lost by default.
func TestDuplicateKeyNotInStaticRollbackSet(t *testing.T) {
	require.NotContains(t, errCodeRollbackWholeTxn, moerr.ErrDuplicateEntry)
	require.False(t, isErrorRollbackWholeTxn(moerr.NewDuplicateEntryNoCtx("1", "a")))
	// a genuine infrastructure failure still ends the transaction
	require.True(t, isErrorRollbackWholeTxn(moerr.NewDeadLockDetectedNoCtx()))
}
