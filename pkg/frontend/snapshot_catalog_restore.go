// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type systemCatalogRestorePolicy uint8

// A pre-upgrade system snapshot can replace the current CDC watermark table
// with an older schema while leaving the upgraded catalog version ready. Such
// a restore would break even newly created CDC tasks, so reject it before any
// account or catalog mutations. Non-system accounts do not own this catalog.
func preflightHistoricalCDCAccountRestore(ctx context.Context, bh BackgroundExec, ts int64, sourceAccount uint32) error {
	if sourceAccount != catalog.System_Account {
		return nil
	}
	query := fmt.Sprintf("select attname from mo_catalog.mo_columns {MO_TS = %d} where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_cdc_watermark' and attname in ('source_table_id','owner_generation','pending_source_table_id','target_identity')", ts)
	rows, err := getStringColsList(defines.AttachAccountId(ctx, catalog.System_Account), bh, query, 0)
	if err != nil {
		return err
	}
	columns := make(map[string]bool, len(rows))
	for _, row := range rows {
		columns[row[0]] = true
	}
	for _, name := range []string{"source_table_id", "owner_generation", "pending_source_table_id", "target_identity"} {
		if !columns[name] {
			return moerr.NewNotSupported(ctx, "historical system account restore would replace the CDC watermark catalog with an incompatible schema")
		}
	}
	return nil
}

func preflightRestoreAccountMetadata(ctx context.Context, bh BackgroundExec, ts int64, sourceAccount, targetAccount uint32, targetExists bool) error {
	if err := preflightHistoricalCDCAccountRestore(ctx, bh, ts, sourceAccount); err != nil {
		return err
	}
	return preflightIcebergAccountRestore(ctx, bh, ts, sourceAccount, targetAccount, targetExists)
}

// Whole-account restore recreates object identities, while Iceberg metadata is
// otherwise cloned verbatim. Reject only histories whose identities cannot be
// preserved; do this before account creation, deletion, or object mutation.
func preflightIcebergAccountRestore(ctx context.Context, bh BackgroundExec, ts int64, sourceAccount, targetAccount uint32, targetExists bool) error {
	sameAccount := targetExists && sourceAccount == targetAccount
	tables := []string{icebergsql.TableCatalogs, icebergsql.TablePrincipalMap, icebergsql.TableResidencyPolicy,
		icebergsql.TableTables, icebergsql.TableRefs, icebergsql.TablePublishJobs, icebergsql.TableOrphanFiles, icebergsql.TableMaintenanceJobs}
	names := make([]string, len(tables))
	for i, name := range tables {
		names[i] = quoteSQLStringLiteral(name)
	}
	sourceCtx := defines.AttachAccountId(ctx, sourceAccount)
	query := fmt.Sprintf("select relname from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d and reldatabase = 'mo_catalog' and relname in (%s)", ts, sourceAccount, strings.Join(names, ","))
	present, err := getStringColsList(sourceCtx, bh, query, 0)
	if err != nil {
		return err
	}
	historical := make(map[string]bool, len(present))
	for _, row := range present {
		table := row[0]
		historical[table] = true
		// Only names returned by the constant allowlist above can reach the SQL.
		query = fmt.Sprintf("select '1' from %s {MO_TS = %d}", qualifiedTableName(moCatalog, table), ts)
		if sameAccount && table == icebergsql.TableCatalogs {
			query += fmt.Sprintf(" where account_id <> %d", sourceAccount)
		}
		query += " limit 1"
		rows, err := getStringColsList(sourceCtx, bh, query, 0)
		if err != nil {
			return err
		}
		if len(rows) != 0 {
			return moerr.NewNotSupportedf(ctx, "account restore cannot safely rebind historical Iceberg metadata in %s for account %d", table, sourceAccount)
		}
	}
	// Tables absent from the source are not replaced by catalog restore. Reject
	// existing target metadata that would otherwise survive an older snapshot.
	if targetExists {
		targetCtx := defines.AttachAccountId(ctx, targetAccount)
		for _, table := range tables {
			if historical[table] {
				continue
			}
			query = fmt.Sprintf("select '1' from %s limit 1", qualifiedTableName(moCatalog, table))
			rows, err := getStringColsList(targetCtx, bh, query, 0)
			if err != nil {
				return err
			}
			if len(rows) != 0 {
				return moerr.NewNotSupportedf(ctx, "account restore cannot preserve current Iceberg metadata in %s absent from the historical snapshot", table)
			}
		}
	}
	return nil
}

// Partial restore recreates a database or table. Iceberg external mappings are
// skipped by the generic restore and must not be silently lost or overwritten.
func preflightIcebergPartialRestore(ctx context.Context, bh BackgroundExec, ts int64, account uint32, database, table string) error {
	accountCtx := defines.AttachAccountId(ctx, account)
	for _, historical := range []bool{true, false} {
		snapshot := ""
		if historical {
			snapshot = fmt.Sprintf(" {MO_TS = %d}", ts)
		}
		query := fmt.Sprintf("select relname, rel_createsql from mo_catalog.mo_tables%s where account_id = %d and reldatabase = %s and relkind = %s", snapshot, account, quoteSQLStringLiteral(database), quoteSQLStringLiteral(catalog.SystemExternalRel))
		if table != "" {
			query += " and relname = " + quoteSQLStringLiteral(table)
		}
		rows, err := getStringColsList(accountCtx, bh, query, 0, 1)
		if err != nil {
			return err
		}
		for _, row := range rows {
			_, iceberg, err := icebergsql.ParseCreateSQLEnvelope(ctx, row[1])
			if err != nil {
				return err
			}
			if iceberg {
				return moerr.NewNotSupportedf(ctx, "restore cannot safely restore or replace Iceberg table %s.%s", database, row[0])
			}
		}
	}
	return nil
}

func restoreTargetAccountID(ctx context.Context, bh BackgroundExec, targetName string) (uint32, bool, error) {
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	query := "select cast(account_id as varchar(20)) from mo_catalog.mo_account where account_name = " + quoteSQLStringLiteral(targetName)
	rows, err := getStringColsList(systemCtx, bh, query, 0)
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	if len(rows) != 1 {
		return 0, false, moerr.NewInternalErrorNoCtx("duplicate restore target account")
	}
	id, err := strconv.ParseUint(rows[0][0], 10, 32)
	if err != nil {
		return 0, false, err
	}
	return uint32(id), true, nil
}

// A historical clone may predate the index required by the running Iceberg
// catalog. Repair it in the same transaction without rewriting historical IDs.
func ensureRestoredIcebergAllocator(ctx context.Context, bh BackgroundExec, table *tableInfo) error {
	if table.dbName != moCatalog || table.tblName != icebergsql.TableCatalogs {
		return nil
	}
	rows, err := getStringColsList(ctx, bh, "show create table mo_catalog.mo_iceberg_catalogs", 1)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return moerr.NewInternalErrorNoCtx("missing Iceberg catalog definition after restore")
	}
	statements, err := mysql.Parse(ctx, rows[0][0], 1)
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()
	if len(statements) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid Iceberg catalog definition after restore")
	}
	definition, ok := statements[0].(*tree.CreateTable)
	if !ok {
		return moerr.NewInternalErrorNoCtx("invalid Iceberg catalog definition after restore")
	}
	for _, def := range definition.Defs {
		switch index := def.(type) {
		case *tree.Index:
			if !strings.EqualFold(index.Name, "catalog_id_allocator") {
				continue
			}
			if (index.KeyType != tree.INDEX_TYPE_INVALID && index.KeyType != tree.INDEX_TYPE_BTREE) || len(index.KeyParts) != 1 || index.KeyParts[0] == nil || index.KeyParts[0].ColName == nil || index.KeyParts[0].Expr != nil || index.KeyParts[0].Length != 0 || index.KeyParts[0].Direction == tree.Descending || !strings.EqualFold(index.KeyParts[0].ColName.ColName(), "catalog_id") {
				return moerr.NewInvalidStateNoCtxf("unexpected Iceberg catalog_id_allocator definition")
			}
			return nil
		case *tree.UniqueIndex:
			if strings.EqualFold(index.GetIndexName(), "catalog_id_allocator") {
				return moerr.NewInvalidStateNoCtxf("unexpected Iceberg catalog_id_allocator definition")
			}
		}
	}
	internalCtx := context.WithValue(ctx, defines.InternalExecutorKey{}, true)
	return bh.Exec(internalCtx, "create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(catalog_id)")
}

const (
	// systemCatalogRestoreSkip means that DDL or another subsystem owns the
	// target state, so snapshot restore must not copy the historical table.
	systemCatalogRestoreSkip systemCatalogRestorePolicy = iota
	// systemCatalogRestoreCopy is safe only for catalog rows whose identifiers
	// retain their meaning in the target account.
	systemCatalogRestoreCopy
	// systemCatalogRestoreRebuild skips the generic table copy and lets the table
	// owner rebuild its rows after every referenced object is restored.
	systemCatalogRestoreRebuild
)

func (policy systemCatalogRestorePolicy) skipsBulkRestore() bool {
	return policy == systemCatalogRestoreSkip || policy == systemCatalogRestoreRebuild
}

type systemCatalogRestoreContext struct {
	ctx           context.Context
	sid           string
	bh            BackgroundExec
	snapshotTS    int64
	sourceAccount uint32
	targetAccount uint32
}

// catalogRestoreAccountPair preserves the source identity used for historical
// reads and the target identity used for reconstructed catalog rows.
type catalogRestoreAccountPair struct {
	sourceAccount uint32
	targetAccount uint32
}

type systemCatalogPostRestoreHandler struct {
	tableName string
	handler   func(*systemCatalogRestoreContext) error
}

// The slice is deliberately ordered. To add another identity-bearing catalog:
//
//  1. mark it systemCatalogRestoreRebuild in the policy table;
//  2. register its owner-specific handler here, after any catalog it consumes;
//  3. test both its ID semantics and every account-restore entry point.
//
// The runtime policy/handler cross-check below makes a half-registered catalog
// fail before any target catalog rows are rewritten.
var systemCatalogPostRestoreHandlers = []systemCatalogPostRestoreHandler{
	{tableName: "mo_role_grant", handler: restoreRoleGrantsAfterObjects},
	{tableName: "mo_role_privs", handler: restoreRolePrivilegesAfterObjects},
}

const rolePrivilegeRestoreInsertBatchSize = 256

const userDefinedFunctionCatalogColumns = `function_id, name, owner, args, arg_types, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

const userDefinedFunctionCatalogSourceColumns = `function_id, name, owner, args, ` +
	catalog.UserDefinedFunctionArgumentTypesSQL + `, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

const storedProcedureCatalogColumns = `proc_id, name, creator, args, lang, body, sql_mode, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation`

// The 4.1 routine catalogs do not have sql_mode. Use the 4.2 catalog default
// when projecting their rows into the current schema.
const legacyRoutineSQLMode = "PIPES_AS_CONCAT"

func isCurrentSchemaRoutineCatalog(tblInfo *tableInfo) bool {
	return tblInfo != nil && tblInfo.dbName == moCatalog &&
		(tblInfo.tblName == "mo_user_defined_function" || tblInfo.tblName == "mo_stored_procedure")
}

// historicalRoutineSQLModeExpression checks the source account at the same
// snapshot as the copied rows. An absent column is distinct from a failed
// metadata query: only the former uses the released 4.2 default.
func historicalRoutineSQLModeExpression(
	ctx context.Context,
	bh BackgroundExec,
	tableName, sourceSnapshot string,
	sourceAccount uint32,
) (string, error) {
	query := fmt.Sprintf("select attname from mo_catalog.mo_columns%s where account_id = %d and att_database = 'mo_catalog' and att_relname = %s and attname = 'sql_mode' limit 2",
		sourceSnapshot, sourceAccount, quoteSQLStringLiteral(tableName))
	rows, err := getStringColsList(defines.AttachAccountId(ctx, sourceAccount), bh, query, 0)
	if err != nil {
		return "", err
	}
	if len(rows) > 1 {
		return "", moerr.NewInvalidStateNoCtxf("duplicate historical sql_mode columns in %s", tableName)
	}
	if len(rows) == 0 {
		return quoteSQLStringLiteral(legacyRoutineSQLMode), nil
	}
	return "sql_mode", nil
}

// Restore routine rows into the running binary's schema. UDF arg_types is
// derived from historical args, as in the catalog migration.
func restoreRoutineCatalogWithCurrentSchema(
	ctx context.Context,
	bh BackgroundExec,
	catalogName string,
	sourceSnapshot string,
	sourceAccount uint32,
	targetAccount uint32,
) error {
	mode, err := historicalRoutineSQLModeExpression(ctx, bh, catalogName, sourceSnapshot, sourceAccount)
	if err != nil {
		return err
	}
	var ddl, targetColumns, sourceColumns string
	switch catalogName {
	case "mo_user_defined_function":
		ddl = MoCatalogMoUserDefinedFunctionDDL
		targetColumns = userDefinedFunctionCatalogColumns
		sourceColumns = strings.TrimSuffix(userDefinedFunctionCatalogSourceColumns, "sql_mode") + mode
	case "mo_stored_procedure":
		ddl = MoCatalogMoStoredProcedureDDL
		targetColumns = storedProcedureCatalogColumns
		sourceColumns = strings.Replace(storedProcedureCatalogColumns, "sql_mode", mode, 1)
	default:
		return moerr.NewInvalidInputNoCtxf("unsupported routine catalog %q", catalogName)
	}
	targetCtx := defines.AttachAccountId(ctx, targetAccount)
	tableName := qualifiedTableName(moCatalog, catalogName)
	if err := bh.Exec(targetCtx, dropTableIfExistsSQL(moCatalog, catalogName)); err != nil {
		return err
	}
	// The running binary owns this catalog schema, including historical widths.
	internalDDL := context.WithValue(targetCtx, defines.InternalExecutorKey{}, true)
	if err := bh.Exec(internalDDL, ddl); err != nil {
		return err
	}

	copySQL := fmt.Sprintf(
		"insert into %s (%s) select %s from %s%s",
		tableName,
		targetColumns,
		sourceColumns,
		tableName,
		sourceSnapshot,
	)
	if sourceAccount == targetAccount {
		return bh.Exec(targetCtx, copySQL)
	}
	return bh.ExecRestore(targetCtx, copySQL, sourceAccount, targetAccount)
}

func restoreSystemCatalogsAfterObjects(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotTS int64,
	sourceAccount uint32,
	targetAccount uint32,
) error {
	if err := validateSystemCatalogRestoreHandlers(ctx); err != nil {
		return err
	}
	restoreCtx := &systemCatalogRestoreContext{
		ctx:           ctx,
		sid:           sid,
		bh:            bh,
		snapshotTS:    snapshotTS,
		sourceAccount: sourceAccount,
		targetAccount: targetAccount,
	}
	for _, entry := range systemCatalogPostRestoreHandlers {
		if err := entry.handler(restoreCtx); err != nil {
			return err
		}
	}
	return nil
}

func validateSystemCatalogRestoreHandlers(ctx context.Context) error {
	registered := make(map[string]struct{}, len(systemCatalogPostRestoreHandlers))
	for _, entry := range systemCatalogPostRestoreHandlers {
		if systemCatalogRestorePolicies[entry.tableName] != systemCatalogRestoreRebuild {
			return moerr.NewInternalErrorf(ctx, "catalog restore handler for %s has no rebuild policy", entry.tableName)
		}
		if _, exists := registered[entry.tableName]; exists {
			return moerr.NewInternalErrorf(ctx, "catalog restore rebuild for %s has multiple handlers", entry.tableName)
		}
		registered[entry.tableName] = struct{}{}
	}
	for tableName, policy := range systemCatalogRestorePolicies {
		if policy == systemCatalogRestoreRebuild {
			if _, ok := registered[tableName]; !ok {
				return moerr.NewInternalErrorf(ctx, "catalog restore rebuild for %s has no handler", tableName)
			}
		}
	}
	return nil
}

type catalogObjectName struct {
	database   string
	name       string
	objectType string
}

type catalogObjectIdentity struct {
	id uint64
	catalogObjectName
}

type catalogRestoreIdentityMap struct {
	databaseIDs map[uint64]uint64
	objectIDs   map[uint64]uint64
}

// catalogRestorePrincipalIdentityMap binds tenant-local identities from the
// historical account to the account reconstructed by restore. User and role
// IDs normally survive because their catalog tables are copied, but restore
// must not rely on that incidental equality when an account is re-created.
type catalogRestorePrincipalIdentityMap struct {
	userIDs map[uint32]uint32
	roleIDs map[uint32]uint32
}

type publicationRestoreIdentity struct {
	accountID uint32
	userID    uint32
	roleID    uint32
}

type rolePrivilegeRestoreRow struct {
	roleID          int64
	roleName        string
	objectType      string
	objectID        uint64
	privilegeID     int64
	privilegeName   string
	privilegeLevel  string
	operationUserID uint64
	grantedTime     string
	withGrantOption bool
}

// Restore rows into the current table so the current role-grant indexes remain
// available even when the snapshot predates their creation.
func restoreRoleGrantsAfterObjects(restoreCtx *systemCatalogRestoreContext) error {
	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)
	if err := restoreCtx.bh.Exec(targetCtx, "delete from mo_catalog.mo_role_grant"); err != nil {
		return err
	}
	const columns = "granted_id,grantee_id,operation_role_id,operation_user_id,granted_time,with_grant_option"
	statement := fmt.Sprintf("insert into mo_catalog.mo_role_grant (%s) select %s from mo_catalog.mo_role_grant {MO_TS = %d}", columns, columns, restoreCtx.snapshotTS)
	if restoreCtx.sourceAccount != restoreCtx.targetAccount {
		return restoreCtx.bh.ExecRestore(targetCtx, statement, restoreCtx.sourceAccount, restoreCtx.targetAccount)
	}
	return restoreCtx.bh.Exec(targetCtx, statement)
}

func restoreRolePrivilegesAfterObjects(restoreCtx *systemCatalogRestoreContext) error {
	identityMap, err := loadCatalogRestoreIdentityMap(restoreCtx)
	if err != nil {
		return err
	}
	rows, err := loadRolePrivilegesAtSnapshot(restoreCtx)
	if err != nil {
		return err
	}

	kept := rows[:0]
	for _, row := range rows {
		newObjectID, found, remapErr := remapRolePrivilegeObjectID(row, identityMap)
		if remapErr != nil {
			return remapErr
		}
		if !found {
			// Some source objects (for example, subscription databases) are
			// deliberately omitted by bulk account restore. Their grants must be
			// omitted too; retaining the source ID could authorize an unrelated
			// target object after a future ID allocation.
			continue
		}
		row.objectID = newObjectID
		kept = append(kept, row)
	}

	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)
	if err = restoreCtx.bh.Exec(targetCtx, "delete from mo_catalog.mo_role_privs"); err != nil {
		return err
	}
	return insertRolePrivilegeRestoreRows(targetCtx, restoreCtx.bh, kept)
}

func insertRolePrivilegeRestoreRows(ctx context.Context, bh BackgroundExec, rows []rolePrivilegeRestoreRow) error {
	insertPrefix := "insert into mo_catalog.mo_role_privs(" +
		"role_id,role_name,obj_type,obj_id,privilege_id,privilege_name," +
		"privilege_level,operation_user_id,granted_time,with_grant_option) values "
	for start := 0; start < len(rows); start += rolePrivilegeRestoreInsertBatchSize {
		end := min(start+rolePrivilegeRestoreInsertBatchSize, len(rows))
		values := make([]string, 0, end-start)
		for _, row := range rows[start:end] {
			values = append(values, fmt.Sprintf(
				"(%d,%s,%s,%d,%d,%s,%s,%d,%s,%t)",
				row.roleID,
				quoteSQLStringLiteral(row.roleName),
				quoteSQLStringLiteral(row.objectType),
				row.objectID,
				row.privilegeID,
				quoteSQLStringLiteral(row.privilegeName),
				quoteSQLStringLiteral(row.privilegeLevel),
				row.operationUserID,
				quoteSQLStringLiteral(row.grantedTime),
				row.withGrantOption,
			))
		}
		if err := bh.Exec(ctx, insertPrefix+strings.Join(values, ",")); err != nil {
			return err
		}
	}
	return nil
}

func loadCatalogRestoreIdentityMap(restoreCtx *systemCatalogRestoreContext) (*catalogRestoreIdentityMap, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)

	sourceDatabaseSQL := fmt.Sprintf(
		"select cast(dat_id as char), datname from mo_catalog.mo_database {MO_TS = %d} "+
			"where account_id = %d order by dat_id",
		restoreCtx.snapshotTS, restoreCtx.sourceAccount,
	)
	sourceDatabases, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sourceDatabaseSQL, restoreCtx.sourceAccount, restoreCtx.targetAccount, 0, 1,
	)
	if err != nil {
		return nil, err
	}
	targetDatabases, err := getStringColsList(
		targetCtx, restoreCtx.bh,
		fmt.Sprintf(
			"select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = %d order by dat_id",
			restoreCtx.targetAccount,
		), 0, 1,
	)
	if err != nil {
		return nil, err
	}

	sourceObjectSQL := fmt.Sprintf(
		"select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind "+
			"from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d order by rel_id",
		restoreCtx.snapshotTS, restoreCtx.sourceAccount,
	)
	sourceObjects, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sourceObjectSQL, restoreCtx.sourceAccount, restoreCtx.targetAccount, 0, 1, 2, 3,
	)
	if err != nil {
		return nil, err
	}
	targetObjects, err := getStringColsList(
		targetCtx, restoreCtx.bh,
		fmt.Sprintf(
			"select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind "+
				"from mo_catalog.mo_tables where account_id = %d order by rel_id",
			restoreCtx.targetAccount,
		), 0, 1, 2, 3,
	)
	if err != nil {
		return nil, err
	}

	return buildCatalogRestoreIdentityMap(sourceDatabases, targetDatabases, sourceObjects, targetObjects)
}

func loadCatalogRestorePrincipalIdentityMap(
	restoreCtx *systemCatalogRestoreContext,
) (*catalogRestorePrincipalIdentityMap, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)

	sourceUsers, err := getStringColsListFromTS(
		sourceCtx,
		restoreCtx.bh,
		fmt.Sprintf(
			"select cast(user_id as char), user_name from mo_catalog.mo_user {MO_TS = %d} order by user_id",
			restoreCtx.snapshotTS,
		),
		restoreCtx.sourceAccount,
		restoreCtx.targetAccount,
		0,
		1,
	)
	if err != nil {
		return nil, err
	}
	targetUsers, err := getStringColsList(
		targetCtx,
		restoreCtx.bh,
		"select cast(user_id as char), user_name from mo_catalog.mo_user order by user_id",
		0,
		1,
	)
	if err != nil {
		return nil, err
	}

	sourceRoles, err := getStringColsListFromTS(
		sourceCtx,
		restoreCtx.bh,
		fmt.Sprintf(
			"select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = %d} order by role_id",
			restoreCtx.snapshotTS,
		),
		restoreCtx.sourceAccount,
		restoreCtx.targetAccount,
		0,
		1,
	)
	if err != nil {
		return nil, err
	}
	targetRoles, err := getStringColsList(
		targetCtx,
		restoreCtx.bh,
		"select cast(role_id as char), role_name from mo_catalog.mo_role order by role_id",
		0,
		1,
	)
	if err != nil {
		return nil, err
	}

	userIDs, err := buildCatalogRestoreNamedIdentityMap(sourceUsers, targetUsers)
	if err != nil {
		return nil, err
	}
	roleIDs, err := buildCatalogRestoreNamedIdentityMap(sourceRoles, targetRoles)
	if err != nil {
		return nil, err
	}
	return &catalogRestorePrincipalIdentityMap{userIDs: userIDs, roleIDs: roleIDs}, nil
}

func buildCatalogRestoreNamedIdentityMap(sourceRows, targetRows [][]string) (map[uint32]uint32, error) {
	targetIDs := make(map[string]uint32, len(targetRows))
	for _, row := range targetRows {
		id, err := parseCatalogUint32ID(row)
		if err != nil {
			return nil, err
		}
		targetIDs[row[1]] = id
	}

	identityMap := make(map[uint32]uint32, len(sourceRows))
	for _, row := range sourceRows {
		sourceID, err := parseCatalogUint32ID(row)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetIDs[row[1]]; ok {
			identityMap[sourceID] = targetID
		}
	}
	return identityMap, nil
}

func parseCatalogUint32ID(row []string) (uint32, error) {
	id, err := parseCatalogID(row, 2)
	if err != nil {
		return 0, err
	}
	if id > uint64(^uint32(0)) {
		return 0, moerr.NewInternalErrorNoCtx("catalog identity exceeds uint32")
	}
	return uint32(id), nil
}

func resolvePublicationRestoreIdentity(
	ctx context.Context,
	pubInfo *pubsub.PubInfo,
	targetAccounts map[string]*pubsub.AccountInfo,
	principalMap *catalogRestorePrincipalIdentityMap,
) (publicationRestoreIdentity, error) {
	targetAccount, ok := targetAccounts[pubInfo.PubAccountName]
	if !ok || targetAccount == nil || targetAccount.Id < 0 {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: target account %s does not exist",
			pubInfo.PubName,
			pubInfo.PubAccountName,
		)
	}
	targetUserID, ok := principalMap.userIDs[pubInfo.Creator]
	if !ok {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: creator user %d does not exist in target account %s",
			pubInfo.PubName,
			pubInfo.Creator,
			pubInfo.PubAccountName,
		)
	}
	targetRoleID, ok := principalMap.roleIDs[pubInfo.Owner]
	if !ok {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: owner role %d does not exist in target account %s",
			pubInfo.PubName,
			pubInfo.Owner,
			pubInfo.PubAccountName,
		)
	}
	return publicationRestoreIdentity{
		accountID: uint32(targetAccount.Id),
		userID:    targetUserID,
		roleID:    targetRoleID,
	}, nil
}

func buildCatalogRestoreIdentityMap(
	sourceDatabases [][]string,
	targetDatabases [][]string,
	sourceObjects [][]string,
	targetObjects [][]string,
) (*catalogRestoreIdentityMap, error) {
	targetDatabaseIDs := make(map[string]uint64, len(targetDatabases))
	for _, row := range targetDatabases {
		id, err := parseCatalogID(row, 2)
		if err != nil {
			return nil, err
		}
		targetDatabaseIDs[row[1]] = id
	}
	databaseIDs := make(map[uint64]uint64, len(sourceDatabases))
	for _, row := range sourceDatabases {
		sourceID, err := parseCatalogID(row, 2)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetDatabaseIDs[row[1]]; ok {
			databaseIDs[sourceID] = targetID
		}
	}

	targetObjectIDs := make(map[catalogObjectName]uint64, len(targetObjects))
	for _, row := range targetObjects {
		identity, err := parseCatalogObjectIdentity(row)
		if err != nil {
			return nil, err
		}
		targetObjectIDs[identity.catalogObjectName] = identity.id
	}
	objectIDs := make(map[uint64]uint64, len(sourceObjects))
	for _, row := range sourceObjects {
		identity, err := parseCatalogObjectIdentity(row)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetObjectIDs[identity.catalogObjectName]; ok {
			objectIDs[identity.id] = targetID
		}
	}

	return &catalogRestoreIdentityMap{databaseIDs: databaseIDs, objectIDs: objectIDs}, nil
}

func parseCatalogID(row []string, expectedColumns int) (uint64, error) {
	if len(row) != expectedColumns {
		return 0, moerr.NewInternalErrorNoCtx("invalid catalog identity row")
	}
	id, err := strconv.ParseUint(row[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func parseCatalogObjectIdentity(row []string) (catalogObjectIdentity, error) {
	id, err := parseCatalogID(row, 4)
	if err != nil {
		return catalogObjectIdentity{}, err
	}
	objectType := objectTypeTable.String()
	if row[3] == catalog.SystemViewRel {
		objectType = objectTypeView.String()
	}
	return catalogObjectIdentity{
		id: id,
		catalogObjectName: catalogObjectName{
			database:   row[1],
			name:       row[2],
			objectType: objectType,
		},
	}, nil
}

const rolePrivilegeRestoreSelectSQL = "select cast(role_id as char), role_name, obj_type, cast(obj_id as char), " +
	"cast(privilege_id as char), privilege_name, privilege_level, " +
	"cast(coalesce(operation_user_id, 0) as char), cast(granted_time as char), " +
	"cast(with_grant_option as char) from mo_catalog.mo_role_privs"

func loadRolePrivilegesAtSnapshot(restoreCtx *systemCatalogRestoreContext) ([]rolePrivilegeRestoreRow, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	sql := fmt.Sprintf(
		rolePrivilegeRestoreSelectSQL+" {MO_TS = %d} "+
			"order by role_id, obj_type, obj_id, privilege_id, privilege_level",
		restoreCtx.snapshotTS,
	)
	cols, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sql, restoreCtx.sourceAccount, restoreCtx.targetAccount,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	)
	if err != nil {
		return nil, err
	}

	return parseRolePrivilegeRestoreRows(restoreCtx.ctx, cols)
}

func parseRolePrivilegeRestoreRows(ctx context.Context, cols [][]string) ([]rolePrivilegeRestoreRow, error) {
	rows := make([]rolePrivilegeRestoreRow, 0, len(cols))
	for _, col := range cols {
		if len(col) != 10 {
			return nil, moerr.NewInternalError(ctx, "invalid mo_role_privs restore row")
		}
		roleID, err := strconv.ParseInt(col[0], 10, 64)
		if err != nil {
			return nil, err
		}
		objectID, err := strconv.ParseUint(col[3], 10, 64)
		if err != nil {
			return nil, err
		}
		privilegeID, err := strconv.ParseInt(col[4], 10, 64)
		if err != nil {
			return nil, err
		}
		operationUserID, err := strconv.ParseUint(col[7], 10, 64)
		if err != nil {
			return nil, err
		}
		withGrantOption, err := strconv.ParseBool(col[9])
		if err != nil {
			if col[9] == "0" || col[9] == "1" {
				withGrantOption = col[9] == "1"
			} else {
				return nil, err
			}
		}
		rows = append(rows, rolePrivilegeRestoreRow{
			roleID:          roleID,
			roleName:        col[1],
			objectType:      col[2],
			objectID:        objectID,
			privilegeID:     privilegeID,
			privilegeName:   col[5],
			privilegeLevel:  col[6],
			operationUserID: operationUserID,
			grantedTime:     col[8],
			withGrantOption: withGrantOption,
		})
	}
	return rows, nil
}

func remapRolePrivilegeObjectID(
	row rolePrivilegeRestoreRow,
	identityMap *catalogRestoreIdentityMap,
) (newObjectID uint64, found bool, err error) {
	// Wildcard account/database/table privileges deliberately use object ID 0.
	// Zero is a sentinel, not an identity, and therefore must never be remapped.
	if row.objectID == objectIDAll {
		return row.objectID, true, nil
	}

	switch row.objectType {
	case objectTypeDatabase.String():
		if row.privilegeLevel != privilegeLevelDatabase.String() {
			return 0, false, moerr.NewInternalErrorNoCtx("nonzero database privilege has an invalid level")
		}
		newObjectID, found = identityMap.databaseIDs[row.objectID]
		return newObjectID, found, nil
	case objectTypeTable.String(), objectTypeView.String():
		switch row.privilegeLevel {
		case privilegeLevelStar.String(), privilegeLevelDatabaseStar.String():
			newObjectID, found = identityMap.databaseIDs[row.objectID]
			return newObjectID, found, nil
		case privilegeLevelDatabaseTable.String(), privilegeLevelTable.String():
			newObjectID, found = identityMap.objectIDs[row.objectID]
			return newObjectID, found, nil
		default:
			return 0, false, moerr.NewInternalErrorNoCtx("nonzero table or view privilege has an invalid level")
		}
	case objectTypeFunction.String():
		// UDF metadata is copied verbatim from mo_user_defined_function, whose
		// function_id is tenant-local. Unlike databases and relations, no DDL
		// recreation allocates a new target identity for this catalog row.
		return row.objectID, true, nil
	default:
		return 0, false, moerr.NewInternalErrorNoCtxf(
			"cannot restore nonzero object ID for privilege object type %s", row.objectType,
		)
	}
}
