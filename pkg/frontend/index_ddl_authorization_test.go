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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestIndexDDLUsesIndexPrivilege(t *testing.T) {
	plans := []*plan.DataDefinition{
		{
			DdlType: plan.DataDefinition_CREATE_INDEX,
			Definition: &plan.DataDefinition_CreateIndex{
				CreateIndex: &plan.CreateIndex{Database: "db", Table: "t"},
			},
		},
		{
			DdlType: plan.DataDefinition_DROP_INDEX,
			Definition: &plan.DataDefinition_DropIndex{
				DropIndex: &plan.DropIndex{Database: "db", Table: "t"},
			},
		},
	}
	for _, ddl := range plans {
		t.Run(ddl.DdlType.String(), func(t *testing.T) {
			tips := extractPrivilegeTipsFromPlan(&plan.Plan{Plan: &plan.Plan_Ddl{Ddl: ddl}})
			require.Len(t, tips, 1)
			require.Equal(t, PrivilegeTypeIndex, tips[0].typ)
			require.Equal(t, "db", tips[0].databaseName)
			require.Equal(t, "t", tips[0].tableName)
		})
	}
}

func TestIndexTransactionOwnershipUsesTheMatchingRoleAndTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner transactionTableOwner
		want  bool
	}{
		{name: "matching ownership", owner: transactionTableOwner{roleID: 5, database: "db", table: "t"}, want: true},
		{name: "different role", owner: transactionTableOwner{roleID: 6, database: "db", table: "t"}},
		{name: "different database", owner: transactionTableOwner{roleID: 5, database: "other", table: "t"}},
		{name: "different table", owner: transactionTableOwner{roleID: 5, database: "db", table: "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", TenantID: 3, UserID: 4, DefaultRoleID: 5})
			bh := &backgroundExecTest{}
			bh.init()
			for _, typ := range []PrivilegeType{PrivilegeTypeIndex, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership} {
				entry := privilegeEntriesMap[typ]
				entry.databaseName, entry.tableName = "db", "t"
				levels, err := getPrivilegeLevelsOfObjectType(t.Context(), entry.objType)
				require.NoError(t, err)
				for _, level := range levels {
					query, err := getSqlForPrivilege2(t.Context(), ses, 5, entry, level)
					require.NoError(t, err)
					bh.sql2result[query] = newMrsForRestoreStringRows([]string{"privilege"}, nil)
				}
			}
			priv := &privilege{objType: objectTypeTable, transactionTableOwner: &tc.owner}
			convertPrivilegeTipsToPrivilege(priv, privilegeTipsArray{{
				typ: PrivilegeTypeIndex, objType: objectTypeTable, databaseName: "db", tableName: "t",
			}})
			roles := &btree.Set[int64]{}
			roles.Insert(5)
			allowed, _, err := determineRoleSetHasPrivilegeSet(t.Context(), bh, ses, roles, priv, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, allowed)

			cache := ses.GetPrivilegeCache()
			levels, err := getPrivilegeLevelsOfObjectType(t.Context(), objectTypeTable)
			require.NoError(t, err)
			for _, level := range levels {
				require.False(t, cache.has(objectTypeTable, level, "db", "t", PrivilegeTypeTableOwnership))
				require.False(t, cache.has(objectTypeTable, level, "db", "t", PrivilegeTypeIndex))
			}
		})
	}
}
