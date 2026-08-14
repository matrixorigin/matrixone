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
