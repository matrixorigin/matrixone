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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

type restoreOwnershipKey struct{}
type restoreObjectName struct{ database, table string }
type restoreOwner struct{ user, role uint32 }

// Install historical DDL identities before recreating objects. Full account
// restore also restores the principal catalogs; partial restore retains IDs
// only for principals that still exist; a reused name is a different identity.
func prepareRestoreOwnership(ctx context.Context, bh BackgroundExec, ts int64, source, target uint32, database, table string) (context.Context, error) {
	sourceCtx := defines.AttachAccountId(ctx, source)
	dbFilter := fmt.Sprintf("account_id = %d", source)
	tableFilter := dbFilter
	if database != "" {
		dbFilter += " and datname = " + quoteSQLStringLiteral(database)
		tableFilter += " and reldatabase = " + quoteSQLStringLiteral(database)
	}
	if table == "" {
		tableFilter += " and relkind != " + quoteSQLStringLiteral(catalog.SystemExternalRel)
	}
	if table != "" {
		tableFilter += " and relname = " + quoteSQLStringLiteral(table)
	}
	dbs, err := getStringColsListFromTS(sourceCtx, bh, fmt.Sprintf(
		"select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = %d} where %s", ts, dbFilter), source, target, 0, 1, 2, 3)
	if err != nil {
		return nil, err
	}
	tables, err := getStringColsListFromTS(sourceCtx, bh, fmt.Sprintf(
		"select reldatabase, relname, cast(creator as char), cast(owner as char) from mo_catalog.mo_tables {MO_TS = %d} where %s", ts, tableFilter), source, target, 0, 1, 2, 3)
	if err != nil {
		return nil, err
	}
	var users, roles map[uint32]struct{}
	databaseExists := false
	if table != "" {
		rows, lookupErr := getStringColsList(defines.AttachAccountId(ctx, target), bh, fmt.Sprintf("select cast(dat_id as char) from mo_catalog.mo_database where account_id = %d and datname = %s for update", target, quoteSQLStringLiteral(database)), 0)
		if lookupErr != nil {
			return nil, lookupErr
		}
		databaseExists = len(rows) != 0
	}
	if database != "" {
		users, err = loadRestorePrincipalIDs(ctx, bh, target, "mo_user", "user_id")
		if err != nil {
			return nil, err
		}
		roles, err = loadRestorePrincipalIDs(ctx, bh, target, "mo_role", "role_id")
		if err != nil {
			return nil, err
		}
	}
	owners := make(map[restoreObjectName]restoreOwner, len(dbs)+len(tables))
	for _, row := range append(dbs, tables...) {
		owner, err := parseRestoreOwner(row)
		if err != nil {
			return nil, err
		}
		if database != "" && !(databaseExists && row[1] == "") {
			_, ok := users[owner.user]
			if !ok {
				return nil, moerr.NewInternalErrorf(ctx, "cannot restore %s.%s: creator no longer exists", row[0], row[1])
			}
			_, ok = roles[owner.role]
			if !ok {
				return nil, moerr.NewInternalErrorf(ctx, "cannot restore %s.%s: owner role no longer exists", row[0], row[1])
			}
		}
		owners[restoreObjectName{row[0], row[1]}] = owner
	}
	return context.WithValue(ctx, restoreOwnershipKey{}, owners), nil
}

func parseRestoreOwner(row []string) (restoreOwner, error) {
	if len(row) != 4 {
		return restoreOwner{}, moerr.NewInternalErrorNoCtx("invalid restore ownership row")
	}
	user, err := strconv.ParseUint(row[2], 10, 32)
	if err != nil {
		return restoreOwner{}, err
	}
	role, err := strconv.ParseUint(row[3], 10, 32)
	return restoreOwner{uint32(user), uint32(role)}, err
}

// Deferred subscriptions recreate only their database, not every object in
// their source account. Full account restore already restored principal IDs.
func prepareSubscriptionRestoreOwnership(ctx context.Context, bh BackgroundExec, ts int64, source, target uint32, database string) (context.Context, error) {
	rows, err := getStringColsListFromTS(defines.AttachAccountId(ctx, source), bh,
		fmt.Sprintf("select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = %d} where account_id = %d and datname = %s", ts, source, quoteSQLStringLiteral(database)), source, target, 0, 1, 2, 3)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, moerr.NewInternalErrorf(ctx, "missing historical ownership for %s", database)
	}
	owner, err := parseRestoreOwner(rows[0])
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, restoreOwnershipKey{}, map[restoreObjectName]restoreOwner{{database, ""}: owner}), nil
}

// Partial restore is within one account. Validate current IDs, not names;
// ALTER ROLE keeps its ID, whereas DROP/CREATE with the same name does not.
func loadRestorePrincipalIDs(ctx context.Context, bh BackgroundExec, account uint32, table, id string) (map[uint32]struct{}, error) {
	rows, err := getStringColsList(defines.AttachAccountId(ctx, account), bh,
		fmt.Sprintf("select cast(%s as char) from mo_catalog.%s", id, table), 0)
	if err != nil {
		return nil, err
	}
	ids := make(map[uint32]struct{}, len(rows))
	for _, row := range rows {
		if len(row) != 1 {
			return nil, moerr.NewInternalErrorNoCtx("invalid restore principal row")
		}
		id, err := strconv.ParseUint(row[0], 10, 32)
		if err != nil {
			return nil, err
		}
		ids[uint32(id)] = struct{}{}
	}
	return ids, nil
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
}

// Capture CURRENT identities before DROP, not identities at the snapshot:
// grants may have changed or the object may already have been restored once.
func capturePartialRestorePrivileges(ctx context.Context, bh BackgroundExec, account uint32, database, table string) (*partialRestorePrivileges, error) {
	p := &partialRestorePrivileges{account: account, database: database, table: table}
	var err error
	p.databases, p.objects, err = p.identities(ctx, bh)
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
	ctx = defines.AttachAccountId(ctx, p.account)
	for _, group := range []struct {
		rows      [][]string
		ids       map[uint64]uint64
		predicate string
	}{
		{p.databases, ids.databaseIDs, "((obj_type = 'database' and privilege_level = 'd') or (obj_type in ('table','view') and privilege_level in ('*','d.*')))"},
		{p.objects, ids.objectIDs, "(obj_type in ('table','view') and privilege_level in ('t','d.t'))"},
	} {
		for _, row := range group.rows {
			old, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				return err
			}
			if old == 0 {
				continue
			}
			id, exists := group.ids[old]
			if exists && id == old {
				continue
			}
			where := fmt.Sprintf(" where obj_id = %d and %s", old, group.predicate)
			query := "delete from mo_catalog.mo_role_privs" + where
			if exists {
				query = fmt.Sprintf("update mo_catalog.mo_role_privs set obj_id = %d", id) + where
			}
			if err = bh.Exec(ctx, query); err != nil {
				return err
			}
		}
	}
	return nil
}
