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
)

type systemCatalogRestorePolicy uint8

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
	{tableName: "mo_role_privs", handler: restoreRolePrivilegesAfterObjects},
}

const rolePrivilegeRestoreInsertBatchSize = 256

const userDefinedFunctionCatalogColumns = `function_id, name, owner, args, arg_types, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

const userDefinedFunctionCatalogSourceColumns = `function_id, name, owner, args, ` +
	catalog.UserDefinedFunctionArgumentTypesSQL + `, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

// isCurrentSchemaUserDefinedFunctionCatalog identifies the catalog whose
// schema is owned by the running binary. Restoring a historical CREATE TABLE
// for it would remove arg_types even when the current tenant-upgrade state is
// already complete.
func isCurrentSchemaUserDefinedFunctionCatalog(tblInfo *tableInfo) bool {
	return tblInfo != nil && tblInfo.dbName == moCatalog && tblInfo.tblName == "mo_user_defined_function"
}

// restoreUserDefinedFunctionCatalogWithCurrentSchema restores historical UDF
// rows into the current catalog shape. The source may predate arg_types, so
// the copy deliberately derives it from args through the same ByteJson SQL
// expression used by the v4.0.6 backfill. This keeps restore independent of
// the snapshot's DDL generation and preserves exact overload identities.
func restoreUserDefinedFunctionCatalogWithCurrentSchema(
	ctx context.Context,
	bh BackgroundExec,
	sourceSnapshot string,
	sourceAccount uint32,
	targetAccount uint32,
) error {
	targetCtx := defines.AttachAccountId(ctx, targetAccount)
	tableName := qualifiedTableName(moCatalog, "mo_user_defined_function")
	if err := bh.Exec(targetCtx, dropTableIfExistsSQL(moCatalog, "mo_user_defined_function")); err != nil {
		return err
	}
	if err := bh.Exec(targetCtx, MoCatalogMoUserDefinedFunctionDDL); err != nil {
		return err
	}

	copySQL := fmt.Sprintf(
		"insert into %s (%s) select %s from %s%s",
		tableName,
		userDefinedFunctionCatalogColumns,
		userDefinedFunctionCatalogSourceColumns,
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
	if len(kept) == 0 {
		return nil
	}

	insertPrefix := "insert into mo_catalog.mo_role_privs(" +
		"role_id,role_name,obj_type,obj_id,privilege_id,privilege_name," +
		"privilege_level,operation_user_id,granted_time,with_grant_option) values "
	for start := 0; start < len(kept); start += rolePrivilegeRestoreInsertBatchSize {
		end := min(start+rolePrivilegeRestoreInsertBatchSize, len(kept))
		values := make([]string, 0, end-start)
		for _, row := range kept[start:end] {
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
		if err = restoreCtx.bh.Exec(targetCtx, insertPrefix+strings.Join(values, ",")); err != nil {
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

func loadRolePrivilegesAtSnapshot(restoreCtx *systemCatalogRestoreContext) ([]rolePrivilegeRestoreRow, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	sql := fmt.Sprintf(
		"select cast(role_id as char), role_name, obj_type, cast(obj_id as char), "+
			"cast(privilege_id as char), privilege_name, privilege_level, "+
			"cast(coalesce(operation_user_id, 0) as char), cast(granted_time as char), "+
			"cast(with_grant_option as char) from mo_catalog.mo_role_privs {MO_TS = %d} "+
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

	rows := make([]rolePrivilegeRestoreRow, 0, len(cols))
	for _, col := range cols {
		if len(col) != 10 {
			return nil, moerr.NewInternalError(restoreCtx.ctx, "invalid mo_role_privs restore row")
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
