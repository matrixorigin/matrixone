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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestRestoreDDLContext(t *testing.T) {
	ctx := defines.AttachAccount(t.Context(), 20, 1, 0)
	ctx = defines.AttachDDLOwnerRoleId(ctx, 99)
	ordinary, err := restoreDDLContext(ctx, "app", "t")
	require.NoError(t, err)
	require.Equal(t, ctx, ordinary)
	ctx = context.WithValue(ctx, restoreOwnershipKey{}, map[restoreObjectName]restoreOwner{
		{"app", ""}: {3, 4}, {"app", "t"}: {5, 6}, {"other", "t"}: {7, 8},
	})
	for key, expected := range ctx.Value(restoreOwnershipKey{}).(map[restoreObjectName]restoreOwner) {
		result, err := restoreDDLContext(ctx, key.database, key.table)
		require.NoError(t, err)
		account, err := defines.GetAccountId(result)
		require.NoError(t, err)
		require.Equal(t, uint32(20), account)
		require.Equal(t, expected.user, defines.GetUserId(result))
		require.Equal(t, expected.role, defines.GetRoleId(result))
		role, ok := defines.GetDDLOwnerRoleId(result)
		require.True(t, ok)
		require.Equal(t, expected.role, role)
	}
	_, err = restoreDDLContext(ctx, "app", "missing")
	require.ErrorContains(t, err, "missing historical ownership")
}

func TestPartialRestoreRebindsOnlyCurrentScopedIDs(t *testing.T) {
	const databases = "select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 and datname = 'app'"
	const tables = "select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 and reldatabase = 'app'"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"10", "app"}})
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{
		{"100", "app", "t", catalog.SystemOrdinaryRel}, {"101", "app", "v", catalog.SystemViewRel},
	})
	p, err := capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "")
	require.NoError(t, err)
	bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"20", "app"}})
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{{"200", "app", "t", catalog.SystemOrdinaryRel}})
	require.NoError(t, p.rebind(t.Context(), bh))
	require.Equal(t, []string{
		databases, tables, databases, tables,
		"update mo_catalog.mo_role_privs set obj_id = 20 where obj_id = 10 and ((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))",
		"update mo_catalog.mo_role_privs set obj_id = 200 where obj_id = 100 and (obj_type in ('table','view') and privilege_level in ('t','d.t'))",
		"delete from mo_catalog.mo_role_privs where obj_id = 101 and (obj_type in ('table','view') and privilege_level in ('t','d.t'))",
	}, bh.executedSQLs)
	failure := errors.New("write failed")
	bh.sql2err[bh.executedSQLs[4]] = failure
	require.ErrorIs(t, p.rebind(t.Context(), bh), failure)
}

func TestRestorePrincipalMapDoesNotReuseMissingIdentity(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	const query = "select cast(role_id as char), role_name from mo_catalog.mo_role"
	bh.sql2result[query+" {MO_TS = 42}"] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"3", "owner"}, {"4", "deleted"}})
	bh.sql2result[query] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"30", "owner"}, {"4", "unrelated"}})
	ids, err := restorePrincipalMap(t.Context(), bh, 42, 10, 20, "mo_role", "role_id", "role_name")
	require.NoError(t, err)
	require.Equal(t, map[uint32]uint32{3: 30}, ids)
	bh.sql2result[query] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"4294967296", "owner"}})
	_, err = restorePrincipalMap(t.Context(), bh, 42, 10, 20, "mo_role", "role_id", "role_name")
	require.Error(t, err)
}
