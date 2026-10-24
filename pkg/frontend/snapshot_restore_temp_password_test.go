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
