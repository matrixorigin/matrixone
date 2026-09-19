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

package ctl

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestVerifyMoCtlAccess pins the mo_ctl execution-entry authorization backstop. The frontend gate
// (verifyAccountCanExecMoCtrl) scans the built plan, but an mo_ctl evaluated DURING binding (e.g. a
// window frame bound) executes and is erased before that scan runs, so this backstop enforces the
// same sys-account + moadmin-role policy by id at the one point every invocation is executed. It
// must fail CLOSED (an account-less context is refused, not treated as sys).
func TestVerifyMoCtlAccess(t *testing.T) {
	proc := testutil.NewProc(t)

	// sys account + moadmin role -> allowed.
	proc.ReplaceTopCtx(defines.AttachAccount(context.Background(),
		catalog.System_Account, catalog.System_User, catalog.System_Role))
	require.NoError(t, verifyMoCtlAccess(proc))

	// ordinary tenant (even on role id 0) -> refused: the account is not sys.
	proc.ReplaceTopCtx(defines.AttachAccount(context.Background(),
		uint32(42), uint32(1), catalog.System_Role))
	require.Error(t, verifyMoCtlAccess(proc))

	// sys account on a non-moadmin role (e.g. accountadmin=2) -> refused.
	proc.ReplaceTopCtx(defines.AttachAccount(context.Background(),
		catalog.System_Account, catalog.System_User, uint32(2)))
	require.Error(t, verifyMoCtlAccess(proc))

	// no account attached -> fail closed (GetAccountId errors; must NOT default to sys).
	proc.ReplaceTopCtx(context.Background())
	require.Error(t, verifyMoCtlAccess(proc))
}

// TestMoCtlEntryRefusesUnauthorized covers the guard at the MoCtl function entry: an unauthorized
// caller is refused before any argument is read, so a bind-time evaluation (e.g. a window frame
// bound) cannot execute the command regardless of how it reached the executor.
func TestMoCtlEntryRefusesUnauthorized(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.ReplaceTopCtx(defines.AttachAccount(context.Background(),
		uint32(42), uint32(1), catalog.System_Role))
	// nil ivecs/result are never touched: the guard returns first.
	require.Error(t, MoCtl(nil, nil, proc, 1))
}
