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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

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
