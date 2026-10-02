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
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

type restoreOwnershipKey struct{}
type restoreObjectName struct{ database, table string }
type restoreOwner struct{ user, role uint32 }

// Install historical DDL identities before recreating objects. Full account
// restore also restores the principal catalogs. Partial restore keeps live
// principal IDs within their catalog generation; names are not identities.
func prepareRestoreOwnership(ctx context.Context, bh BackgroundExec, ts int64, source, target uint32, database, table string) (context.Context, error) {
	if database != "" && source != target {
		return nil, moerr.NewInternalErrorNoCtx("partial restore requires the same account")
	}
	sourceCtx := defines.AttachAccountId(ctx, source)
	dbFilter := fmt.Sprintf("account_id = %d", source)
	tableFilter := dbFilter
	if database != "" {
		dbFilter += " and datname = " + quoteSQLStringLiteral(database)
		tableFilter += " and reldatabase = " + quoteSQLStringLiteral(database)
	}
	if table != "" {
		tableFilter += " and relname = " + quoteSQLStringLiteral(table)
	}
	// Table restore retains an existing database. Lock its current identity
	// before deciding which historical principals the DDL actually needs.
	recreateDatabase := true
	if database != "" {
		exists, err := checkDatabaseExistsOrNotWithLock(defines.AttachAccountId(ctx, target), bh, database, true)
		if err != nil {
			return nil, err
		}
		recreateDatabase = table == "" || !exists
	}
	var dbs [][]string
	if recreateDatabase {
		var err error
		dbs, err = getStringColsListFromTS(sourceCtx, bh, fmt.Sprintf(
			"select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = %d} where %s", ts, dbFilter), source, target, 0, 1, 2, 3)
		if err != nil {
			return nil, err
		}
	}
	tables, err := getStringColsListFromTS(sourceCtx, bh, fmt.Sprintf(
		"select reldatabase, relname, cast(creator as char), cast(owner as char) from mo_catalog.mo_tables {MO_TS = %d} where %s", ts, tableFilter), source, target, 0, 1, 2, 3)
	if err != nil {
		return nil, err
	}
	owners := make(map[restoreObjectName]restoreOwner, len(dbs)+len(tables))
	neededUsers, neededRoles := make(map[uint32]struct{}), make(map[uint32]struct{})
	ownershipRows := append(dbs, tables...)
	for _, row := range ownershipRows {
		if len(row) != 4 {
			return nil, moerr.NewInternalErrorNoCtx("invalid restore ownership row")
		}
		user, err := strconv.ParseUint(row[2], 10, 32)
		if err != nil {
			return nil, err
		}
		role, err := strconv.ParseUint(row[3], 10, 32)
		if err != nil {
			return nil, err
		}
		owner := restoreOwner{uint32(user), uint32(role)}
		if database != "" {
			neededUsers[owner.user] = struct{}{}
			neededRoles[owner.role] = struct{}{}
		}
		owners[restoreObjectName{row[0], row[1]}] = owner
	}
	if database != "" && len(owners) != 0 {
		users, err := restorePrincipalMap(ctx, bh, ts, source, "mo_user", "user_id", "user_name", neededUsers)
		if err != nil {
			return nil, err
		}
		roles, err := restorePrincipalMap(ctx, bh, ts, source, "mo_role", "role_id", "role_name", neededRoles)
		if err != nil {
			return nil, err
		}
		for _, row := range ownershipRows {
			object := restoreObjectName{row[0], row[1]}
			owner := owners[object]
			if _, ok := users[owner.user]; !ok {
				return nil, moerr.NewInternalErrorf(ctx, "cannot restore %s.%s: creator no longer exists", object.database, object.table)
			}
			if _, ok := roles[owner.role]; !ok {
				return nil, moerr.NewInternalErrorf(ctx, "cannot restore %s.%s: owner role no longer exists", object.database, object.table)
			}
		}
	}
	return context.WithValue(ctx, restoreOwnershipKey{}, owners), nil
}

// Principal IDs are not reused by ordinary DROP/CREATE, but account restore
// replaces these auto-increment catalogs and can rewind their allocators. Across
// generations only reserved slots with their bootstrap protections are provable;
// ordinary principals fail before DROP even if names and numeric IDs match.
func restorePrincipalMap(ctx context.Context, bh BackgroundExec, ts int64, account uint32, table, id, name string, needed map[uint32]struct{}) (map[uint32]uint32, error) {
	if len(needed) == 0 {
		return map[uint32]uint32{}, nil
	}
	ctx = defines.AttachAccountId(ctx, account)
	filter := fmt.Sprintf(" where account_id = %d and reldatabase = 'mo_catalog' and relname = %s", account, quoteSQLStringLiteral(table))
	generationQuery := "select cast(rel_id as char) from mo_catalog.mo_tables"
	oldGeneration, err := getStringColsListFromTS(ctx, bh, fmt.Sprintf("%s {MO_TS = %d}%s", generationQuery, ts, filter), account, account, 0)
	if err != nil {
		return nil, err
	}
	newGeneration, err := getStringColsList(ctx, bh, generationQuery+filter+" for update", 0)
	if err != nil {
		return nil, err
	}
	parseGeneration := func(rows [][]string) (uint64, error) {
		if len(rows) != 1 || len(rows[0]) != 1 {
			return 0, moerr.NewInternalErrorNoCtx("invalid restore principal catalog identity")
		}
		value, err := parseCatalogID(rows[0], 1)
		if err != nil {
			return 0, err
		}
		if value == 0 {
			return 0, moerr.NewInternalErrorNoCtx("invalid restore principal catalog identity")
		}
		return value, nil
	}
	oldID, err := parseGeneration(oldGeneration)
	if err != nil {
		return nil, err
	}
	newID, err := parseGeneration(newGeneration)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0, len(needed))
	for value := range needed {
		ids = append(ids, value)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	values := make([]string, len(ids))
	for i, value := range ids {
		values[i] = strconv.FormatUint(uint64(value), 10)
	}
	predicate := " where " + id + " in (" + strings.Join(values, ",") + ")"
	query := fmt.Sprintf("select cast(%s as char), %s from mo_catalog.%s", id, name, table)
	oldRows, err := getStringColsListFromTS(ctx, bh, fmt.Sprintf("%s {MO_TS = %d}%s", query, ts, predicate), account, account, 0, 1)
	if err != nil {
		return nil, err
	}
	newRows, err := getStringColsList(ctx, bh, query+predicate+" order by "+id+" for update", 0, 1)
	if err != nil {
		return nil, err
	}
	parseRows := func(rows [][]string) (map[uint32]string, error) {
		result := make(map[uint32]string, len(rows))
		for _, row := range rows {
			key, err := parseCatalogUint32ID(row)
			if err != nil {
				return nil, err
			}
			if _, duplicate := result[key]; duplicate {
				return nil, moerr.NewInternalErrorNoCtx("duplicate restore principal identity")
			}
			result[key] = row[1]
		}
		return result, nil
	}
	oldNames, err := parseRows(oldRows)
	if err != nil {
		return nil, err
	}
	newNames, err := parseRows(newRows)
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]uint32, len(ids))
	for _, value := range ids {
		oldName, existed := oldNames[value]
		newName, exists := newNames[value]
		if !existed || !exists {
			continue
		}
		if oldID != newID {
			reserved := false
			if oldName == newName {
				if table == "mo_role" {
					reserved = value == publicRoleID && oldName == publicRoleName ||
						account == sysAccountID && value == moAdminRoleID && oldName == moAdminRoleName ||
						account != sysAccountID && value == accountAdminRoleID && oldName == accountAdminRoleName
				} else {
					reserved = account != sysAccountID && value == GetAdminUserId() ||
						account == sysAccountID && (value == rootID && oldName == rootName || value == dumpID && oldName == dumpName)
					if reserved {
						adminRole := uint32(accountAdminRoleID)
						if account == sysAccountID {
							adminRole = moAdminRoleID
						}
						grantQuery := "select cast(user_id as char) from mo_catalog.mo_user_grant"
						grantFilter := fmt.Sprintf(" where user_id = %d and role_id = %d", value, adminRole)
						oldGrants, err := getStringColsListFromTS(ctx, bh, fmt.Sprintf("%s {MO_TS = %d}%s", grantQuery, ts, grantFilter), account, account, 0)
						if err != nil {
							return nil, err
						}
						newGrants, err := getStringColsList(ctx, bh, grantQuery+grantFilter, 0)
						if err != nil {
							return nil, err
						}
						isProtected := func(rows [][]string) bool {
							return len(rows) == 1 && len(rows[0]) == 1 && rows[0][0] == strconv.FormatUint(uint64(value), 10)
						}
						reserved = isProtected(oldGrants) && isProtected(newGrants)
					}
				}
			}
			if !reserved {
				return nil, moerr.NewInternalErrorf(ctx, "cannot restore: %s principal catalog was rebuilt; identity continuity cannot be established", table)
			}
		}
		if table == "mo_user" && oldName != newName {
			continue
		}
		result[value] = value
	}
	return result, nil
}

func restoreDDLContext(ctx context.Context, database, table string) (context.Context, error) {
	// CLONE also reuses the view reconstruction helpers, but creates objects
	// owned by the invoking role rather than restoring historical ownership.
	owners, restoring := ctx.Value(restoreOwnershipKey{}).(map[restoreObjectName]restoreOwner)
	if !restoring {
		return ctx, nil
	}
	if owner, ok := owners[restoreObjectName{database, table}]; ok {
		ctx = defines.AttachUserId(ctx, owner.user)
		ctx = defines.AttachRoleId(ctx, owner.role)
		return defines.AttachDDLOwnerRoleId(ctx, owner.role), nil
	}
	return nil, moerr.NewInternalErrorf(ctx, "missing historical ownership for %s.%s", database, table)
}

func execRestoreCreateDatabase(ctx context.Context, bh BackgroundExec, database, sql string) error {
	if owners, restoring := ctx.Value(restoreOwnershipKey{}).(map[restoreObjectName]restoreOwner); restoring {
		if _, recreated := owners[restoreObjectName{database, ""}]; !recreated {
			// Only a locked, existing database permits the no-op DDL without a
			// historical identity. Missing metadata must never create a database
			// owned by the invoking SYS role.
			exists, err := checkDatabaseExistsOrNotWithLock(ctx, bh, database, true)
			if err != nil {
				return err
			}
			if exists {
				return bh.Exec(ctx, sql)
			}
		}
	}
	ctx, err := restoreDDLContext(ctx, database, "")
	if err != nil {
		return err
	}
	return bh.Exec(ctx, sql)
}

type partialRestorePrivileges struct {
	account            uint32
	database, table    string
	databases, objects [][]string
	rows               []rolePrivilegeRestoreRow
	predicate          string
}

var partialRestorePrivilegesCapturedHook atomic.Pointer[func()]

// SetPartialRestorePrivilegesCapturedHookForTest installs a cross-session
// barrier after capture, while the restore transaction still owns its locks.
func SetPartialRestorePrivilegesCapturedHookForTest(hook func()) func() {
	previous := partialRestorePrivilegesCapturedHook.Load()
	if hook == nil {
		partialRestorePrivilegesCapturedHook.Store(nil)
	} else {
		partialRestorePrivilegesCapturedHook.Store(&hook)
	}
	return func() { partialRestorePrivilegesCapturedHook.Store(previous) }
}

// DROP cleans object grants. Save only CURRENT scoped grants before DROP, not
// snapshot grants: partial restore must not revive revoked authorizations.
func capturePartialRestorePrivileges(ctx context.Context, bh BackgroundExec, account uint32, database, table string) (*partialRestorePrivileges, error) {
	p := &partialRestorePrivileges{account: account, database: database, table: table}
	// Share the database-before-relation lifecycle order with GRANT/REVOKE
	// and DROP. Keep replacement and grant replay atomic with name resolution.
	lockSQL := fmt.Sprintf("select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = %d and datname = %s for update", account, quoteSQLStringLiteral(database))
	if err := bh.Exec(defines.AttachAccountId(ctx, account), lockSQL); err != nil {
		return nil, err
	}
	var err error
	p.databases, p.objects, err = p.identities(ctx, bh)
	if err != nil {
		return nil, err
	}
	var predicates []string
	for _, group := range []struct {
		rows      [][]string
		predicate string
	}{
		{p.databases, "((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))"},
		{p.objects, "(obj_type in ('table','view') and privilege_level in ('t','d.t'))"},
	} {
		ids := make([]string, 0, len(group.rows))
		for _, row := range group.rows {
			id, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				return nil, err
			}
			if id != 0 {
				ids = append(ids, strconv.FormatUint(id, 10))
			}
		}
		if len(ids) != 0 {
			predicates = append(predicates, "(obj_id in ("+strings.Join(ids, ",")+") and "+group.predicate+")")
		}
	}
	if len(predicates) == 0 {
		return p, nil
	}
	p.predicate = strings.Join(predicates, " or ")
	cols, err := getStringColsList(defines.AttachAccountId(ctx, account), bh,
		rolePrivilegeRestoreSelectSQL+" where "+p.predicate+" for update", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	if err != nil {
		return nil, err
	}
	p.rows, err = parseRolePrivilegeRestoreRows(ctx, cols)
	if err == nil {
		if hook := partialRestorePrivilegesCapturedHook.Load(); hook != nil {
			(*hook)()
		}
	}
	return p, err
}

func (p *partialRestorePrivileges) identities(ctx context.Context, bh BackgroundExec) ([][]string, [][]string, error) {
	ctx = defines.AttachAccountId(ctx, p.account)
	var databases [][]string
	var err error
	if p.table == "" {
		databases, err = getStringColsList(ctx, bh, fmt.Sprintf("select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = %d and datname = %s", p.account, quoteSQLStringLiteral(p.database)), 0, 1)
		if err != nil {
			return nil, nil, err
		}
	}
	query := fmt.Sprintf("select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind from mo_catalog.mo_tables where account_id = %d and reldatabase = %s", p.account, quoteSQLStringLiteral(p.database))
	if p.table != "" {
		query += " and relname = " + quoteSQLStringLiteral(p.table)
	}
	objects, err := getStringColsList(ctx, bh, query, 0, 1, 2, 3)
	return databases, objects, err
}

func (p *partialRestorePrivileges) rebind(ctx context.Context, bh BackgroundExec) error {
	databases, objects, err := p.identities(ctx, bh)
	if err != nil {
		return err
	}
	ids, err := buildCatalogRestoreIdentityMap(p.databases, databases, p.objects, objects)
	if err != nil {
		return err
	}
	kept := make([]rolePrivilegeRestoreRow, 0, len(p.rows))
	for _, row := range p.rows {
		id, exists, err := remapRolePrivilegeObjectID(row, ids)
		if err != nil {
			return err
		}
		if exists {
			row.objectID = id
			kept = append(kept, row)
		}
	}
	if len(p.rows) == 0 {
		return nil
	}
	ctx = defines.AttachAccountId(ctx, p.account)
	if err = bh.Exec(ctx, "delete from mo_catalog.mo_role_privs where "+p.predicate); err != nil {
		return err
	}
	return insertRolePrivilegeRestoreRows(ctx, bh, kept)
}
