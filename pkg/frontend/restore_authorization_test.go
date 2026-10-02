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
	"strings"
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

func TestPrepareRestoreOwnershipRebindsCurrentPrincipals(t *testing.T) {
	const databaseQuery = "select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = 42} where account_id = 10 and datname = 'app'"
	const tableQuery = "select reldatabase, relname, cast(creator as char), cast(owner as char) from mo_catalog.mo_tables {MO_TS = 42} where account_id = 10 and reldatabase = 'app' and relname = 't'"
	const oldUsers = "select cast(user_id as char), user_name from mo_catalog.mo_user {MO_TS = 42}"
	const currentUsers = "select cast(user_id as char), user_name from mo_catalog.mo_user"
	const oldRoles = "select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = 42}"
	const currentRoles = "select cast(role_id as char), role_name from mo_catalog.mo_role"

	bh := &backgroundExecTest{}
	bh.init()
	lockedDatabase, err := getSqlForCheckDatabaseByAccount(defines.AttachAccountId(t.Context(), 20), "app")
	require.NoError(t, err)
	lockedDatabase = strings.TrimSuffix(lockedDatabase, ";") + " for update;"
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase(nil)
	setRows := func(query string, columns []string, rows [][]interface{}) {
		bh.sql2result[query] = newMrsForRestoreStringRows(columns, rows)
	}
	setRows(databaseQuery, []string{"database", "table", "creator", "owner"}, [][]interface{}{{"app", "", "1", "2"}})
	setRows(tableQuery, []string{"database", "table", "creator", "owner"}, [][]interface{}{{"app", "t", "3", "4"}})
	setRows(oldUsers, []string{"id", "name"}, [][]interface{}{{"1", "creator"}, {"3", "editor"}})
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"11", "creator"}, {"33", "editor"}})
	setRows(oldRoles, []string{"id", "name"}, [][]interface{}{{"2", "db_owner"}, {"4", "table_owner"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"22", "db_owner"}, {"44", "table_owner"}})

	ctx, err := prepareRestoreOwnership(t.Context(), bh, 42, 10, 20, "app", "t")
	require.NoError(t, err)
	for _, tc := range []struct {
		table string
		user  uint32
		role  uint32
	}{
		{"", 11, 22},
		{"t", 33, 44},
	} {
		ownerCtx, err := restoreDDLContext(ctx, "app", tc.table)
		require.NoError(t, err)
		require.Equal(t, tc.user, defines.GetUserId(ownerCtx))
		require.Equal(t, tc.role, defines.GetRoleId(ownerCtx))
	}

	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"11", "creator"}})
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 20, "app", "t")
	require.ErrorContains(t, err, "creator no longer exists")
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"11", "creator"}, {"33", "editor"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"22", "db_owner"}})
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 20, "app", "t")
	require.ErrorContains(t, err, "owner role no longer exists")

	// A retained database needs neither its historical creator nor its role.
	// The selected table still needs both, and the existence query is locked
	// in the target account before reading any historical metadata.
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase([][]interface{}{{uint64(100)}})
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"33", "editor"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"44", "table_owner"}})
	start := len(bh.executedSQLs)
	accountStart := len(bh.executionAccountIDs)
	ctx, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 20, "app", "t")
	require.NoError(t, err)
	require.Equal(t, lockedDatabase, bh.executedSQLs[start])
	require.Equal(t, uint32(20), bh.executionAccountIDs[accountStart])
	require.NotContains(t, bh.executedSQLs[start:], databaseQuery)
	_, err = restoreDDLContext(ctx, "app", "")
	require.ErrorContains(t, err, "missing historical ownership")
	tableCtx, err := restoreDDLContext(ctx, "app", "t")
	require.NoError(t, err)
	require.Equal(t, uint32(33), defines.GetUserId(tableCtx))
	ctx = defines.AttachAccountId(ctx, 20)
	require.NoError(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"))
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase(nil)
	require.ErrorContains(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"), "missing historical ownership")
	bh.sql2err[lockedDatabase] = errors.New("database lookup failed")
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 20, "app", "t")
	require.ErrorContains(t, err, "database lookup failed")
	require.ErrorContains(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"), "database lookup failed")
}

func TestPartialRestoreRebindsOnlyCurrentScopedIDs(t *testing.T) {
	const databases = "select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 and datname = 'app'"
	const lockSQL = databases + " for update"
	const tables = "select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 and reldatabase = 'app'"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"10", "app"}})
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{
		{"100", "app", "t", catalog.SystemOrdinaryRel}, {"101", "app", "v", catalog.SystemViewRel},
	})
	const predicate = "(obj_id in (10) and ((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))) or (obj_id in (100,101) and (obj_type in ('table','view') and privilege_level in ('t','d.t')))"
	query := rolePrivilegeRestoreSelectSQL + " where " + predicate + " for update"
	bh.sql2result[query] = newMrsForRestoreStringRows(make([]string, 10), [][]interface{}{
		{"3", "reader", "database", "10", "1", "show tables", "d", "2", "2026-09-30 00:00:00", "0"},
		{"3", "reader", "table", "100", "2", "select", "d.t", "2", "2026-09-30 00:00:00", "1"},
		{"3", "reader", "view", "101", "2", "select", "d.t", "2", "2026-09-30 00:00:00", "0"},
	})
	p, err := capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "")
	require.NoError(t, err)
	bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"20", "app"}})
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{{"200", "app", "t", catalog.SystemOrdinaryRel}})
	require.NoError(t, p.rebind(t.Context(), bh))
	require.Equal(t, []string{
		lockSQL, databases, tables, query, databases, tables,
		"delete from mo_catalog.mo_role_privs where " + predicate,
		"insert into mo_catalog.mo_role_privs(role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,granted_time,with_grant_option) values (3,'reader','database',20,1,'show tables','d',2,'2026-09-30 00:00:00',false),(3,'reader','table',200,2,'select','d.t',2,'2026-09-30 00:00:00',true)",
	}, bh.executedSQLs)
	failure := errors.New("write failed")
	for _, write := range bh.executedSQLs[6:] {
		bh.sql2err[write] = failure
		require.ErrorIs(t, p.rebind(t.Context(), bh), failure)
		delete(bh.sql2err, write)
	}
}

func TestPartialRestoreKeepsUnchangedIdentities(t *testing.T) {
	const databases = "select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 and datname = 'app'"
	const tables = "select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 and reldatabase = 'app'"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"10", "app"}})
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{
		{"100", "app", "t", catalog.SystemOrdinaryRel},
		{"101", "app", "v", catalog.SystemViewRel},
	})
	const predicate = "(obj_id in (10) and ((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))) or (obj_id in (100,101) and (obj_type in ('table','view') and privilege_level in ('t','d.t')))"
	bh.sql2result[rolePrivilegeRestoreSelectSQL+" where "+predicate+" for update"] = newMrsForRestoreStringRows(make([]string, 10), nil)
	p, err := capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "")
	require.NoError(t, err)
	require.NoError(t, p.rebind(t.Context(), bh))
	require.Equal(t, []string{databases + " for update", databases, tables, rolePrivilegeRestoreSelectSQL + " where " + p.predicate + " for update", databases, tables}, bh.executedSQLs)
}

func TestPartialRestoreReplaysGrantsAfterDropWithUnchangedLogicalID(t *testing.T) {
	const lockSQL = "select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 and datname = 'app' for update"
	const tables = "select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 and reldatabase = 'app' and relname = 't'"
	const predicate = "(obj_id in (100) and (obj_type in ('table','view') and privilege_level in ('t','d.t')))"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{{"100", "app", "t", catalog.SystemOrdinaryRel}})
	query := rolePrivilegeRestoreSelectSQL + " where " + predicate + " for update"
	bh.sql2result[query] = newMrsForRestoreStringRows(make([]string, 10), [][]interface{}{
		{"3", "reader", "table", "100", "2", "select", "d.t", "2", "2026-09-30 00:00:00", "0"},
	})
	p, err := capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "t")
	require.NoError(t, err)
	// DROP deleted the live grant catalog row; replay must use the saved grant.
	bh.sql2result[query] = newMrsForRestoreStringRows(make([]string, 10), nil)
	require.NoError(t, p.rebind(t.Context(), bh))
	require.Equal(t, []string{
		lockSQL, tables, query, tables,
		"delete from mo_catalog.mo_role_privs where " + predicate,
		"insert into mo_catalog.mo_role_privs(role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,granted_time,with_grant_option) values (3,'reader','table',100,2,'select','d.t',2,'2026-09-30 00:00:00',false)",
	}, bh.executedSQLs)
}

func TestPartialRestorePropagatesCatalogReadFailures(t *testing.T) {
	const databases = "select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = 20 and datname = 'app'"
	const lockSQL = databases + " for update"
	const tables = "select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = 20 and reldatabase = 'app'"
	const predicate = "(obj_id in (10) and ((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))) or (obj_id in (100) and (obj_type in ('table','view') and privilege_level in ('t','d.t')))"
	query := rolePrivilegeRestoreSelectSQL + " where " + predicate + " for update"
	for _, tc := range []struct {
		name   string
		query  string
		rebind bool
	}{
		{"capture lifecycle lock", lockSQL, false},
		{"capture databases", databases, false},
		{"capture tables", tables, false},
		{"capture grants", query, false},
		{"rebind databases", databases, true},
		{"rebind tables", tables, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[databases] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"10", "app"}})
			bh.sql2result[tables] = newMrsForRestoreStringRows([]string{"id", "db", "name", "kind"}, [][]interface{}{
				{"100", "app", "t", catalog.SystemOrdinaryRel},
			})
			bh.sql2result[query] = newMrsForRestoreStringRows(make([]string, 10), nil)
			var p *partialRestorePrivileges
			if tc.rebind {
				var err error
				p, err = capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "")
				require.NoError(t, err)
				bh.executedSQLs = nil
			}
			failure := errors.New("catalog read failed")
			bh.sql2err[tc.query] = failure
			if tc.rebind {
				require.ErrorIs(t, p.rebind(t.Context(), bh), failure)
			} else {
				_, err := capturePartialRestorePrivileges(t.Context(), bh, 20, "app", "")
				require.ErrorIs(t, err, failure)
			}
			var expected []string
			if !tc.rebind {
				expected = append(expected, lockSQL)
			}
			if tc.query != lockSQL {
				expected = append(expected, databases)
			}
			if tc.query == tables || tc.query == query {
				expected = append(expected, tables)
			}
			if tc.query == query {
				expected = append(expected, query)
			}
			require.Equal(t, expected, bh.executedSQLs)
		})
	}
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
