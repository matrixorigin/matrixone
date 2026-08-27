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
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// TestSessionRollsBackTxnOnError: the opt-in is per session, applies to ANY
// real error, and is off unless the session asks for it. Default off is
// MySQL's behaviour -- a failed statement rolls back the statement, not the
// transaction.
func TestSessionRollsBackTxnOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()

	errs := map[string]error{
		"duplicate key":  moerr.NewDuplicateEntryNoCtx("1", "a"),
		"bad type":       moerr.NewInvalidInputNoCtx("not an int"),
		"internal":       moerr.NewInternalErrorNoCtx("something else"),
		"no such table":  moerr.NewNoSuchTableNoCtx("db", "t"),
		"divide by zero": moerr.NewDivByZeroNoCtx(),
		"out of range":   moerr.NewOutOfRangeNoCtx("int8", "300"),
	}

	// default: MySQL behaviour, whatever the error
	for name, err := range errs {
		require.False(t, sessionRollsBackTxnOnError(ses, err),
			"%s: the default must be MySQL's -- statement rollback only", name)
	}
	require.False(t, sessionRollsBackTxnOnError(ses, nil))
	require.False(t, sessionRollsBackTxnOnError(nil, errs["duplicate key"]))

	// opted in: every real error is now fatal to the transaction, not just the
	// handful the static set already covers
	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", int64(1)))
	for name, err := range errs {
		require.True(t, sessionRollsBackTxnOnError(ses, err), "%s must roll back the txn when opted in", name)
	}

	// and back off again
	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", int64(0)))
	require.False(t, sessionRollsBackTxnOnError(ses, errs["duplicate key"]))
}

// TestWarningsNeverRollBackTxn: moerr carries Ok signals, Info codes and
// Warning codes alongside real errors. A truncated value is reported through
// the same type, and it must not discard a transaction even with the strictest
// setting -- only errors do.
func TestWarningsNeverRollBackTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "mo_rollback_txn_on_error", int64(1)))

	// moerr's full taxonomy is covered by TestIsRealError in that package;
	// here we use the exported constructors to prove the frontend honours it.
	for name, notAnError := range map[string]*moerr.Error{
		"warning": moerr.NewWarn(context.Background(), "data truncated"),
		"info":    moerr.NewInfoNoCtx("something worth saying"),
	} {
		require.False(t, notAnError.IsRealError(), "%s is not a real error", name)
		require.False(t, sessionRollsBackTxnOnError(ses, notAnError),
			"%s must never roll back a transaction", name)
	}

	// while a real error, with the same setting, does
	require.True(t, moerr.NewInternalErrorNoCtx("x").IsRealError())
	require.True(t, sessionRollsBackTxnOnError(ses, moerr.NewInternalErrorNoCtx("x")))
}

// TestBackgroundSessionNeverRollsBackWholeTxn: the variable has global scope,
// so a user can set it for the whole instance. Internal work must not inherit
// it -- catalog maintenance and restores treat some errors as benign, and
// letting one of those destroy an enclosing transaction would be a foot-gun no
// user asked for. backSession answers nil for anything outside its allowlist,
// which is what keeps this true.
func TestBackgroundSessionNeverRollsBackWholeTxn(t *testing.T) {
	backSes := &backSession{}
	val, err := backSes.GetSessionSysVar("mo_rollback_txn_on_error")
	require.NoError(t, err)
	require.Nil(t, val, "a background session must not see the setting")
	require.False(t, sessionRollsBackTxnOnError(backSes, moerr.NewDuplicateEntryNoCtx("1", "a")))
}

// TestStaticRollbackSetIsInfrastructureOnly: the static set is failures after
// which a transaction cannot continue. Data errors must stay out of it, or the
// session variable would have nothing to control and MySQL compatibility would
// be lost by default.
func TestStaticRollbackSetIsInfrastructureOnly(t *testing.T) {
	for _, code := range []uint16{moerr.ErrDuplicateEntry, moerr.ErrInvalidInput, moerr.ErrNoSuchTable} {
		require.NotContains(t, errCodeRollbackWholeTxn, code)
	}
	require.False(t, isErrorRollbackWholeTxn(moerr.NewDuplicateEntryNoCtx("1", "a")))
	// a genuine infrastructure failure still ends the transaction, with or
	// without the session variable
	require.True(t, isErrorRollbackWholeTxn(moerr.NewDeadLockDetectedNoCtx()))
}

// unknownVarSession is a FeSession whose system-variable lookup fails, which
// is what a session in a partially-initialized or shutting-down state does.
type unknownVarSession struct {
	FeSession
}

func (s *unknownVarSession) GetSessionSysVar(name string) (interface{}, error) {
	return nil, moerr.NewInternalErrorNoCtx("no such system variable")
}

// TestUnreadableSettingKeepsMySQLBehaviour: if the switch cannot be read, the
// answer must be the DEFAULT, not the strict setting. Failing the other way
// would discard a transaction because of an unrelated lookup failure -- data
// loss caused by a bookkeeping error.
func TestUnreadableSettingKeepsMySQLBehaviour(t *testing.T) {
	ses := &unknownVarSession{}
	require.False(t, sessionRollsBackTxnOnError(ses, moerr.NewDuplicateEntryNoCtx("1", "a")))
	require.False(t, sessionRollsBackTxnOnError(ses, moerr.NewInternalErrorNoCtx("boom")))
}

// TestNonMoerrIsStillAnError: the setting says "any error", and an error that
// MO did not wrap in moerr is still an error. Only moerr can be a warning, so
// only moerr gets the exemption -- otherwise the switch would silently mean
// "any error MO happens to have wrapped", which is not a distinction a user
// can see or predict.
func TestNonMoerrIsStillAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()

	plain := errors.New("driver went away")
	wrapped := fmt.Errorf("while committing: %w", plain)

	// default is still MySQL behaviour for these too
	require.False(t, sessionRollsBackTxnOnError(ses, plain))
	require.False(t, sessionRollsBackTxnOnError(ses, wrapped))

	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", int64(1)))
	require.True(t, sessionRollsBackTxnOnError(ses, plain))
	require.True(t, sessionRollsBackTxnOnError(ses, wrapped))

	// a WRAPPED warning keeps its exemption: the exemption is about what the
	// error IS, not about how many layers it arrived under -- which is why the
	// check is errors.As and not a type assertion
	warning := moerr.NewWarn(ctx, "data truncated")
	require.False(t, warning.IsRealError())
	require.False(t, sessionRollsBackTxnOnError(ses, fmt.Errorf("while inserting: %w", warning)))

	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", int64(0)))
}
