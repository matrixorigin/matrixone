// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestInitialRolePrivilegesPreserveSingleRowContract(t *testing.T) {
	for _, role := range []struct {
		name       string
		id         uint32
		privileges []PrivilegeType
	}{
		{moAdminRoleName, moAdminRoleID, entriesOfMoAdminForMoRolePrivsFor},
		{accountAdminRoleName, accountAdminRoleID, entriesOfAccountAdminForMoRolePrivsFor},
		{publicRoleName, publicRoleID, entriesOfPublicForMoRolePrivsFor},
	} {
		t.Run(role.name, func(t *testing.T) {
			parse := func(sql string) *tree.Insert {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				t.Cleanup(stmt.Free)
				return stmt.(*tree.Insert)
			}
			batch := parse(initialRolePrivilegesSQL(role.id, role.name, 42, role.privileges))
			require.Equal(t, "mo_catalog.mo_role_privs", tree.String(batch.Table, dialect.MYSQL))
			require.Equal(t, tree.IdentifierList{"role_id", "role_name", "obj_type", "obj_id", "privilege_id", "privilege_name", "privilege_level", "operation_user_id", "granted_time", "with_grant_option"}, batch.Columns)
			rows := batch.Rows.Select.(*tree.ValuesClause).Rows
			require.Len(t, rows, len(role.privileges))
			for index, privilege := range role.privileges {
				entry := privilegeEntriesMap[privilege]
				legacy := parse(fmt.Sprintf(initMoRolePrivFormat,
					role.id, role.name, entry.objType, entry.objId,
					entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
					uint32(42), "2026-10-02 00:00:00", entry.withGrantOption))
				oldRow := legacy.Rows.Select.(*tree.ValuesClause).Rows[0]
				require.Len(t, rows[index], len(oldRow))
				for column := range oldRow {
					if column == 8 {
						// Each row still uses the original UTC timestamp precision. Calls to
						// the live clock need not fall in the same second.
						timestamp := strings.Trim(tree.String(rows[index][column], dialect.MYSQL), "'\"")
						_, err := time.ParseInLocation("2006-01-02 15:04:05", timestamp, time.UTC)
						require.NoError(t, err)
						continue
					}
					require.Equal(t, tree.String(oldRow[column], dialect.MYSQL), tree.String(rows[index][column], dialect.MYSQL), "row %d column %d", index, column)
				}
			}
		})
	}
	require.Empty(t, initialRolePrivilegesSQL(accountAdminRoleID, accountAdminRoleName, 42, nil))
}

func TestInitialRolePrivilegeBatchFailureStopsTenantInitialization(t *testing.T) {
	for _, failedRole := range []string{accountAdminRoleName, publicRoleName} {
		t.Run(failedRole, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			failure := moerr.NewInternalErrorNoCtx("privilege batch rejected")
			bh.beforeExec = func(sql string) {
				if strings.HasPrefix(sql, initMoRolePrivPrefix) && strings.Contains(sql, "\""+failedRole+"\"") {
					bh.sql2err[sql] = failure
				}
			}
			ctx := defines.AttachAccountId(context.Background(), 42)
			pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
			pu.SV.SetDefaultValues()
			tenant := &TenantInfo{TenantID: 42, UserID: 2, DefaultRoleID: accountAdminRoleID}
			err := createTablesInMoCatalogOfGeneralTenant2(bh, &createAccount{AdminName: "admin", IdentTyp: tree.AccountIdentifiedByPassword, IdentStr: "111"}, ctx, tenant, pu)
			require.ErrorIs(t, err, failure)
			require.NotEmpty(t, bh.executedSQLs)
			require.Contains(t, bh.executedSQLs[len(bh.executedSQLs)-1], "\""+failedRole+"\"")
			for index, sql := range bh.executedSQLs {
				require.Equal(t, uint32(42), bh.executionAccountIDs[index])
				require.NotContains(t, sql, "insert into mo_catalog.mo_user_grant")
			}
		})
	}
}
