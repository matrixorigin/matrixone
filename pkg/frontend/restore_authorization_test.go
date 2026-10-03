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
	"fmt"
	"strconv"
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

func TestPrepareRestoreOwnershipRetainsPrincipalIDs(t *testing.T) {
	const databaseQuery = "select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = 42} where account_id = 10 and datname = 'app'"
	const tableQuery = "select reldatabase, relname, cast(creator as char), cast(owner as char), relkind from mo_catalog.mo_tables {MO_TS = 42} where account_id = 10 and reldatabase = 'app' and relname = 't'"
	const oldUsers = "select cast(user_id as char), user_name from mo_catalog.mo_user {MO_TS = 42} where user_id in (1,3)"
	const currentUsers = "select cast(user_id as char), user_name from mo_catalog.mo_user where user_id in (1,3) order by user_id for update"
	const oldRoles = "select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = 42} where role_id in (2,4)"
	const currentRoles = "select cast(role_id as char), role_name from mo_catalog.mo_role where role_id in (2,4) order by role_id for update"

	bh := &backgroundExecTest{}
	bh.init()
	lockedDatabase, err := getSqlForCheckDatabaseByAccount(defines.AttachAccountId(t.Context(), 10), "app")
	require.NoError(t, err)
	lockedDatabase = strings.TrimSuffix(lockedDatabase, ";") + " for update;"
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase(nil)
	setRows := func(query string, columns []string, rows [][]interface{}) {
		bh.sql2result[query] = newMrsForRestoreStringRows(columns, rows)
	}
	setRows(databaseQuery, []string{"database", "table", "creator", "owner"}, [][]interface{}{{"app", "", "1", "2"}})
	setRows(tableQuery, []string{"database", "table", "creator", "owner", "relkind"}, [][]interface{}{{"app", "t", "3", "4", catalog.SystemOrdinaryRel}})
	setRows(oldUsers, []string{"id", "name"}, [][]interface{}{{"1", "creator"}, {"3", "editor"}})
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"1", "creator"}, {"3", "editor"}})
	setRows(oldRoles, []string{"id", "name"}, [][]interface{}{{"2", "db_owner"}, {"4", "table_owner"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"2", "db_owner"}, {"4", "renamed_owner"}})

	for _, catalogTable := range []string{"mo_user", "mo_role"} {
		filter := " where account_id = 10 and reldatabase = 'mo_catalog' and relname = '" + catalogTable + "'"
		setRows("select cast(rel_id as char) from mo_catalog.mo_tables {MO_TS = 42}"+filter, []string{"id"}, [][]interface{}{{"100"}})
		setRows("select cast(rel_id as char) from mo_catalog.mo_tables"+filter+" for update", []string{"id"}, [][]interface{}{{"100"}})
	}
	ctx, err := prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "t", map[restoreObjectName]struct{}{{"app", "t"}: {}})
	require.NoError(t, err)
	for _, tc := range []struct {
		table string
		user  uint32
		role  uint32
	}{
		{"", 1, 2},
		{"t", 3, 4},
	} {
		ownerCtx, err := restoreDDLContext(ctx, "app", tc.table)
		require.NoError(t, err)
		require.Equal(t, tc.user, defines.GetUserId(ownerCtx))
		require.Equal(t, tc.role, defines.GetRoleId(ownerCtx))
	}

	// Logical selection, including servable views and sequences, owns the
	// requirements. Excluded catalog rows must not contribute principals.
	bulkQuery := strings.TrimSuffix(tableQuery, " and relname = 't'")
	columns := []string{"database", "table", "creator", "owner", "relkind"}
	for _, external := range [][]interface{}{
		{"app", "ext", "99", "4", catalog.SystemExternalRel},
		{"app", "ext", "3", "99", catalog.SystemExternalRel},
	} {
		setRows(bulkQuery, columns, [][]interface{}{
			{"app", "t", "3", "4", catalog.SystemOrdinaryRel},
			{"app", "v", "3", "4", catalog.SystemViewRel},
			{"app", "s", "3", "4", catalog.SystemSequenceRel}, external,
			{"app", "skipped_view", "invalid", "99", catalog.SystemViewRel},
			{"app", "hidden_index", "99", "invalid", catalog.SystemOrdinaryRel},
		})
		bulkCtx, err := prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "", map[restoreObjectName]struct{}{{"app", "t"}: {}, {"app", "v"}: {}, {"app", "s"}: {}})
		require.NoError(t, err)
		for _, table := range []string{"t", "v", "s"} {
			ownerCtx, err := restoreDDLContext(bulkCtx, "app", table)
			require.NoError(t, err)
			require.Equal(t, uint32(3), defines.GetUserId(ownerCtx))
			require.Equal(t, uint32(4), defines.GetRoleId(ownerCtx))
		}
		_, err = restoreDDLContext(bulkCtx, "app", "ext")
		require.ErrorContains(t, err, "missing historical ownership")
	}
	bh.sql2err[bulkQuery] = errors.New("ownership read failed")
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "", map[restoreObjectName]struct{}{{"app", "t"}: {}, {"app", "v"}: {}, {"app", "s"}: {}})
	require.ErrorContains(t, err, "ownership read failed")
	delete(bh.sql2err, bulkQuery)
	setRows(bulkQuery, columns, [][]interface{}{{"app", "t", "3", "4", catalog.SystemOrdinaryRel}})
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "", map[restoreObjectName]struct{}{{"app", "t"}: {}, {"app", "v"}: {}, {"app", "s"}: {}})
	require.ErrorContains(t, err, "missing historical ownership")

	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"1", "creator"}})
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "t", map[restoreObjectName]struct{}{{"app", "t"}: {}})
	require.ErrorContains(t, err, "creator no longer exists")
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"1", "creator"}, {"3", "editor"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"2", "db_owner"}})
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "t", map[restoreObjectName]struct{}{{"app", "t"}: {}})
	require.ErrorContains(t, err, "owner role no longer exists")

	// A retained database needs neither its historical creator nor its role.
	// The selected table still needs both, and the existence query is locked
	// in the target account before reading any historical metadata.
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase([][]interface{}{{uint64(100)}})
	setRows(currentUsers, []string{"id", "name"}, [][]interface{}{{"3", "editor"}})
	setRows(currentRoles, []string{"id", "name"}, [][]interface{}{{"4", "table_owner"}})
	for _, query := range []string{oldUsers, currentUsers, oldRoles, currentRoles} {
		single := strings.ReplaceAll(strings.ReplaceAll(query, "(1,3)", "(3)"), "(2,4)", "(4)")
		bh.sql2result[single] = bh.sql2result[query]
	}
	start := len(bh.executedSQLs)
	accountStart := len(bh.executionAccountIDs)
	ctx, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "t", map[restoreObjectName]struct{}{{"app", "t"}: {}})
	require.NoError(t, err)
	require.Equal(t, lockedDatabase, bh.executedSQLs[start])
	require.Equal(t, uint32(10), bh.executionAccountIDs[accountStart])
	require.NotContains(t, bh.executedSQLs[start:], databaseQuery)
	_, err = restoreDDLContext(ctx, "app", "")
	require.ErrorContains(t, err, "missing historical ownership")
	tableCtx, err := restoreDDLContext(ctx, "app", "t")
	require.NoError(t, err)
	require.Equal(t, uint32(3), defines.GetUserId(tableCtx))
	ctx = defines.AttachAccountId(ctx, 10)
	require.NoError(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"))
	bh.sql2result[lockedDatabase] = newMrsForCheckDatabase(nil)
	require.ErrorContains(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"), "missing historical ownership")
	bh.sql2err[lockedDatabase] = errors.New("database lookup failed")
	_, err = prepareRestoreOwnership(t.Context(), bh, 42, 10, 10, "app", "t", map[restoreObjectName]struct{}{{"app", "t"}: {}})
	require.ErrorContains(t, err, "database lookup failed")
	require.ErrorContains(t, execRestoreCreateDatabase(ctx, bh, "app", "create database if not exists app"), "database lookup failed")
}

// FK traversal must not re-enumerate physical objects outside the logical
// source, or replace the pointers classified by view preflight.
func TestPartialRestoreFKSourceScope(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 10)
	info := &tableInfo{dbName: "app", tblName: "t"}
	source := &partialRestoreSource{tables: []*tableInfo{info}, byName: map[restoreObjectName]*tableInfo{{"app", "t"}: info}}
	bh := &backgroundExecTest{}
	bh.init()
	keys := []string{genKey("app", "t"), genKey("app", "hidden"), genKey("other", "t")}
	snapshot, err := getTableInfoMap(ctx, "", bh, nil, "app", "", keys, source)
	require.NoError(t, err)
	pitr, err := getTableInfoMapInPitrRestore(ctx, "", bh, "p", 42, "app", "", keys, source)
	require.NoError(t, err)
	for _, result := range []map[string]*tableInfo{snapshot, pitr} {
		require.Len(t, result, 1)
		require.Same(t, info, result[genKey("app", "t")])
	}
	require.Empty(t, bh.executedSQLs)
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

func TestRestorePrincipalMapIdentityContinuity(t *testing.T) {
	const historic = "select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = 42} where role_id in (3,4)"
	const current = "select cast(role_id as char), role_name from mo_catalog.mo_role where role_id in (3,4) order by role_id for update"
	const generation = "select cast(rel_id as char) from mo_catalog.mo_tables"
	const filter = " where account_id = 10 and reldatabase = 'mo_catalog' and relname = 'mo_role'"
	for _, tc := range []struct {
		name       string
		live       [][]interface{}
		generation [][]interface{}
		expected   map[uint32]uint32
		error      string
	}{
		{"rename", [][]interface{}{{"3", "renamed"}, {"5", "owner"}}, [][]interface{}{{"100"}}, map[uint32]uint32{3: 3}, ""},
		{"deleted and name reused", [][]interface{}{{"5", "owner"}}, [][]interface{}{{"100"}}, map[uint32]uint32{}, ""},
		{"catalog rollback collision", [][]interface{}{{"3", "owner"}}, [][]interface{}{{"101"}}, nil, "catalog was rebuilt"},
		{"missing catalog", nil, nil, nil, "invalid restore principal catalog identity"},
		{"duplicate catalog", nil, [][]interface{}{{"100"}, {"100"}}, nil, "invalid restore principal catalog identity"},
		{"invalid catalog", nil, [][]interface{}{{"0"}}, nil, "invalid restore principal catalog identity"},
		{"invalid principal", [][]interface{}{{"4294967296", "owner"}}, [][]interface{}{{"100"}}, nil, "catalog identity exceeds uint32"},
		{"duplicate principal", [][]interface{}{{"3", "owner"}, {"3", "renamed"}}, [][]interface{}{{"100"}}, nil, "duplicate restore principal identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[historic] = newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{"3", "owner"}, {"4", "deleted"}})
			bh.sql2result[current] = newMrsForRestoreStringRows([]string{"id", "name"}, tc.live)
			bh.sql2result[generation+" {MO_TS = 42}"+filter] = newMrsForRestoreStringRows([]string{"id"}, [][]interface{}{{"100"}})
			bh.sql2result[generation+filter+" for update"] = newMrsForRestoreStringRows([]string{"id"}, tc.generation)
			ids, err := restorePrincipalMap(t.Context(), bh, 42, 10, "mo_role", "role_id", "role_name", map[uint32]struct{}{3: {}, 4: {}})
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ids)
			}
			for _, query := range bh.executedSQLs {
				require.True(t, strings.HasPrefix(query, "select "), query)
			}
		})
	}
}

func TestRestorePrincipalMapReservedSlotsAcrossGenerations(t *testing.T) {
	for _, tc := range []struct {
		name, table, id, principal string
		account                    uint32
		value                      uint32
		protected                  bool
		allowed                    bool
	}{
		{"tenant admin role", "mo_role", "role_id", accountAdminRoleName, 10, accountAdminRoleID, false, true},
		{"public role", "mo_role", "role_id", publicRoleName, 10, publicRoleID, false, true},
		{"system admin role zero", "mo_role", "role_id", moAdminRoleName, sysAccountID, moAdminRoleID, false, true},
		{"wrong reserved role name", "mo_role", "role_id", "replacement", 10, accountAdminRoleID, false, false},
		{"wrong tenant role", "mo_role", "role_id", moAdminRoleName, 10, moAdminRoleID, false, false},
		{"bootstrap custom admin name", "mo_user", "user_id", "bootstrap_admin", 10, GetAdminUserId(), true, true},
		{"bootstrap without protection", "mo_user", "user_id", "bootstrap_admin", 10, GetAdminUserId(), false, false},
		{"additional admin", "mo_user", "user_id", "other_admin", 10, GetAdminUserId() + 1, true, false},
		{"root zero", "mo_user", "user_id", rootName, sysAccountID, rootID, true, true},
		{"dump", "mo_user", "user_id", dumpName, sysAccountID, dumpID, true, true},
		{"wrong bootstrap name", "mo_user", "user_id", "replacement", sysAccountID, rootID, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			nameColumn := "role_name"
			if tc.table == "mo_user" {
				nameColumn = "user_name"
			}
			query := "select cast(" + tc.id + " as char), " + nameColumn + " from mo_catalog." + tc.table
			predicate := fmt.Sprintf(" where %s in (%d)", tc.id, tc.value)
			rows := newMrsForRestoreStringRows([]string{"id", "name"}, [][]interface{}{{strconv.FormatUint(uint64(tc.value), 10), tc.principal}})
			bh.sql2result[query+" {MO_TS = 42}"+predicate] = rows
			bh.sql2result[query+predicate+" order by "+tc.id+" for update"] = rows
			generation := "select cast(rel_id as char) from mo_catalog.mo_tables"
			filter := fmt.Sprintf(" where account_id = %d and reldatabase = 'mo_catalog' and relname = '%s'", tc.account, tc.table)
			bh.sql2result[generation+" {MO_TS = 42}"+filter] = newMrsForRestoreStringRows([]string{"id"}, [][]interface{}{{"100"}})
			bh.sql2result[generation+filter+" for update"] = newMrsForRestoreStringRows([]string{"id"}, [][]interface{}{{"101"}})
			adminRole := uint32(accountAdminRoleID)
			if tc.account == sysAccountID {
				adminRole = moAdminRoleID
			}
			grantQuery := "select cast(user_id as char) from mo_catalog.mo_user_grant"
			grantFilter := fmt.Sprintf(" where user_id = %d and role_id = %d", tc.value, adminRole)
			var grants [][]interface{}
			if tc.protected {
				grants = [][]interface{}{{strconv.FormatUint(uint64(tc.value), 10)}}
			}
			for _, query := range []string{grantQuery + " {MO_TS = 42}" + grantFilter, grantQuery + grantFilter} {
				bh.sql2result[query] = newMrsForRestoreStringRows([]string{"user"}, grants)
			}
			ids, err := restorePrincipalMap(t.Context(), bh, 42, tc.account, tc.table, tc.id, nameColumn, map[uint32]struct{}{tc.value: {}})
			if !tc.allowed {
				require.ErrorContains(t, err, "catalog was rebuilt")
				return
			}
			require.NoError(t, err)
			require.Equal(t, map[uint32]uint32{tc.value: tc.value}, ids)
			// Check each protection independently, so validating only one side
			// cannot pass. Every catalog/grant read also propagates its failure.
			queries := append([]string(nil), bh.executedSQLs...)
			if tc.table == "mo_user" {
				for _, query := range []string{grantQuery + " {MO_TS = 42}" + grantFilter, grantQuery + grantFilter} {
					protected := bh.sql2result[query]
					bh.sql2result[query] = newMrsForRestoreStringRows([]string{"user"}, nil)
					_, err = restorePrincipalMap(t.Context(), bh, 42, tc.account, tc.table, tc.id, nameColumn, map[uint32]struct{}{tc.value: {}})
					require.ErrorContains(t, err, "catalog was rebuilt")
					bh.sql2result[query] = protected
				}
			}
			for _, query := range queries {
				failure := errors.New("catalog read failed")
				bh.sql2err[query] = failure
				_, err = restorePrincipalMap(t.Context(), bh, 42, tc.account, tc.table, tc.id, nameColumn, map[uint32]struct{}{tc.value: {}})
				require.ErrorIs(t, err, failure)
				delete(bh.sql2err, query)
			}
		})
	}
}
