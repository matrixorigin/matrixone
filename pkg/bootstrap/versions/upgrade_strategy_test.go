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

package versions

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCheckCommonProtocolVersion(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "all CNs ready", value: `{"method":"GETPROTOCOLVERSION","result":"cn-a:14,cn-b:15"}`},
		{name: "older CN blocks", value: `{"method":"GETPROTOCOLVERSION","result":"cn-a:14,cn-b:13"}`, wantErr: true},
		{name: "malformed response blocks", value: `{"method":"GETPROTOCOLVERSION","result":"cn-a"}`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				require.Equal(t, "SELECT mo_ctl('cn', 'GetProtocolVersion', '')", sql)
				return newProtocolResult(t, test.value), nil
			}, nil)
			err := checkCommonProtocolVersion(txn, defines.MORPCVersion14)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, errors.New("query unavailable")
	}, nil)
	require.ErrorContains(t, checkCommonProtocolVersion(txn, defines.MORPCVersion14), "query unavailable")
}

func TestUpgradeEntryWaitsForCommonProtocol(t *testing.T) {
	upgraded := false
	entry := UpgradeEntry{
		TableName:               "CHECK_CONSTRAINTS",
		RequiredProtocolVersion: defines.MORPCVersion14,
		CheckFunc: func(executor.TxnExecutor, uint32) (bool, error) {
			return false, nil
		},
		UpgSql: "CREATE VIEW information_schema.CHECK_CONSTRAINTS AS ...",
	}
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
			return newProtocolResult(t, `{"method":"GETPROTOCOLVERSION","result":"cn-a:14,cn-b:13"}`), nil
		}
		upgraded = true
		return executor.Result{}, nil
	}, nil)

	err := entry.Upgrade(txn, 0)
	require.ErrorContains(t, err, "node")
	require.False(t, upgraded)
}

func TestUpgradeStatementOption(t *testing.T) {
	for _, test := range []struct {
		name      string
		accountID uint32
		userID    uint32
		roleID    uint32
		hasUser   bool
		hasRole   bool
	}{
		{name: "system account", accountID: catalog.System_Account, userID: sysRootID, roleID: sysAdminRoleID},
		{name: "tenant account", accountID: 42, userID: accountAdminUserID, roleID: accountAdminRoleID, hasUser: true, hasRole: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			option := UpgradeStatementOption(test.accountID)
			require.True(t, option.HasAccountID())
			require.Equal(t, test.hasUser, option.HasUserID())
			require.Equal(t, test.hasRole, option.HasRoleID())
			require.Equal(t, test.accountID, option.AccountID())
			require.Equal(t, test.userID, option.UserID())
			require.Equal(t, test.roleID, option.RoleID())
		})
	}
}

func newProtocolResult(t *testing.T, value string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{value}))
	return result.GetResult()
}

// The hidden-object filters in these upgrade probes must be PREFIX tests, not LIKE patterns.
//
// `_` is LIKE's single-character wildcard, so '__mo_index_%' also matches any name shaped
// [?][?]mo[?]index[?]..., and '__mo_cpkey_%' any [?][?]mo[?]cpkey[?]... . Unlike the internal
// ids these filters were written for, the names they are compared against are USER-chosen: a
// table named x1mo2index3foo is excluded from CheckTableDefinition, which then reports a table
// that exists as absent, and the upgrade acts on that answer. prefix_eq is bytes.HasPrefix, so
// the filter matches the hidden objects it names and nothing else.
func TestUpgradeProbesFilterHiddenObjectsByPrefixNotLike(t *testing.T) {
	seen := map[string]string{}
	record := func(name string) func(string) (executor.Result, error) {
		return func(sql string) (executor.Result, error) {
			seen[name] = sql
			return executor.Result{}, nil
		}
	}

	_, _ = CheckTableDefinition(executor.NewMemTxnExecutor(record("table"), nil), 0, "db", "tbl")
	_, _, _ = CheckTableComment(executor.NewMemTxnExecutor(record("comment"), nil), 0, "db", "tbl")
	_, _ = CheckTableColumn(executor.NewMemTxnExecutor(record("column"), nil), 0, "db", "tbl", "col")
	// The System_Account variants are separate statements and need the same filter.
	_, _ = CheckTableDefinition(executor.NewMemTxnExecutor(record("table-sys"), nil), catalog.System_Account, "db", "tbl")
	_, _, _ = CheckTableComment(executor.NewMemTxnExecutor(record("comment-sys"), nil), catalog.System_Account, "db", "tbl")
	_, _ = CheckTableColumn(executor.NewMemTxnExecutor(record("column-sys"), nil), catalog.System_Account, "db", "tbl", "col")

	require.Len(t, seen, 6, "every probe must have run and been captured")
	for name, sql := range seen {
		require.NotContains(t, sql, "LIKE", "%s still filters with a wildcard pattern", name)
	}
	for _, name := range []string{"table", "table-sys", "comment", "comment-sys"} {
		require.Contains(t, seen[name], "NOT prefix_eq(tbl.relname, '__mo_index_')", name)
	}
	for _, name := range []string{"column", "column-sys"} {
		require.Contains(t, seen[name], "NOT prefix_eq(att_relname, '__mo_cpkey_')", name)
	}
}
