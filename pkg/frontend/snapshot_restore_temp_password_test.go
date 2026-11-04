// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// #30752: the bootstrap password used while re-creating a dropped account
// during snapshot/PITR restore must be a CSPRNG random value, never the
// hardcoded factory default "111".
func TestNewSnapshotRestoreTempPassword(t *testing.T) {
	const rounds = 256
	seen := make(map[string]struct{}, rounds)
	for i := 0; i < rounds; i++ {
		pwd, err := newSnapshotRestoreTempPassword()
		require.NoError(t, err)
		require.Len(t, pwd, 48, "192-bit hex encoding")
		require.NotEqual(t, "111", pwd)
		for _, r := range pwd {
			require.Contains(t, "0123456789abcdef", string(r),
				"only lowercase hex may appear, so the literal is SQL-safe")
		}
		require.NotContains(t, pwd, "'")
		_, dup := seen[pwd]
		require.False(t, dup, "duplicate temp password generated")
		seen[pwd] = struct{}{}
	}
}

// The bootstrap statement keeps embedding the one-shot password; the secrecy
// requirement is met by (a) the CSPRNG source and (b) never logging the SQL.
func TestMakeCreateAccountSqlByAccountRecordUsesTempPassword(t *testing.T) {
	pwd, err := newSnapshotRestoreTempPassword()
	require.NoError(t, err)

	rec := accountRecord{
		accountName: "acc30752",
		adminName:   "admin",
		pwd:         pwd,
		comments:    "hello",
	}
	sql := makeCreateAccountSqlByAccountRecord(rec)
	require.Contains(t, sql, "create account IF NOT EXISTS acc30752")
	require.Contains(t, sql, "ADMIN_NAME 'admin'")
	require.Contains(t, sql, "IDENTIFIED BY '"+pwd+"'")
	require.Contains(t, sql, "comment 'hello'")
	require.True(t, strings.HasSuffix(sql, ";"))

	// two independent bootstrap events never share a password
	pwd2, err := newSnapshotRestoreTempPassword()
	require.NoError(t, err)
	require.NotEqual(t, pwd, pwd2)
}

// An entropy-source failure must surface as an error (and never fall back to
// a predictable password) so the restore pipeline refuses to bootstrap with a
// guessable credential.
func TestNewSnapshotRestoreTempPasswordRandFailure(t *testing.T) {
	orig := snapshotRestoreRandRead
	snapshotRestoreRandRead = func(b []byte) (int, error) {
		return 0, errors.New("entropy source unavailable")
	}
	defer func() { snapshotRestoreRandRead = orig }()

	pwd, err := newSnapshotRestoreTempPassword()
	require.Error(t, err)
	require.Empty(t, pwd)
	require.Contains(t, err.Error(), "generate snapshot restore temp password")
}

// The entropy failure must also propagate out of the account-scan loop: the
// restore must re-create the dropped account with a random bootstrap
// password or not at all — never with a fallback.
func TestGetPastExistsAccountsPropagatesRandFailure(t *testing.T) {
	orig := snapshotRestoreRandRead
	snapshotRestoreRandRead = func(b []byte) (int, error) {
		return 0, errors.New("entropy source unavailable")
	}
	t.Cleanup(func() { snapshotRestoreRandRead = orig })

	const ts = 12345
	bh := &backgroundExecTest{sql2result: make(map[string]ExecResult)}
	bh.sql2result[fmt.Sprintf(getPastAccountsFmt, int64(ts))] = newMrsForRestoreStringRows(
		[]string{"account_id", "account_name", "admin_name", "comments"},
		[][]interface{}{{uint64(42), "acc_past_30752", "adm_30752", ""}})

	_, err := getPastExistsAccounts(context.Background(), "svc-test-30752", bh, "snap_30752", ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "generate snapshot restore temp password")
}

func TestGetAccountRecordByTsUsesRandomBootstrapPassword(t *testing.T) {
	const ts = int64(12345)
	const name = "acc_restored"
	bh := &backgroundExecTest{}
	bh.init()
	sql := fmt.Sprintf("select account_id, account_name, admin_name, comments from mo_catalog.mo_account {MO_TS = %d } where account_name = '%s';", ts, name)
	bh.sql2result[sql] = newMrsForRestoreStringRows(
		[]string{"account_id", "account_name", "admin_name", "comments"},
		[][]interface{}{{uint64(42), name, "admin", ""}})
	ses := &Session{}
	record, err := getAccountRecordByTs(t.Context(), ses, bh, "snapshot", ts, name)
	require.NoError(t, err)
	require.Equal(t, name, record.accountName)
	require.Len(t, record.pwd, 48)
	require.NotEqual(t, "111", record.pwd)

	orig := snapshotRestoreRandRead
	snapshotRestoreRandRead = func([]byte) (int, error) { return 0, errors.New("entropy unavailable") }
	t.Cleanup(func() { snapshotRestoreRandRead = orig })
	record, err = getAccountRecordByTs(t.Context(), ses, bh, "snapshot", ts, name)
	require.ErrorContains(t, err, "generate snapshot restore temp password")
	require.Empty(t, record.pwd)
}
