// Copyright 2021 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// verifyAccountCanOperateClusterTable determines the account can operate
// the cluster table
func verifyAccountCanOperateClusterTable(account *TenantInfo,
	dbName string,
	clusterTableOperation clusterTableOperationType) bool {
	if account.IsSysTenant() {
		//sys account can do anything on the cluster table.
		if dbName == moCatalog {
			return true
		}
	} else {
		//the general account can only read the cluster table
		if dbName == moCatalog {
			switch clusterTableOperation {
			case clusterTableNone, clusterTableSelect:
				return true
			}
		}
	}
	return false
}

// verifyLightPrivilege checks the privilege that does not need to
// access the privilege tables.
// case1 : checks if a real user from client is modifying the catalog databases (mo_catalog,information_schema,system,
// system_metric,mysql).
// case2 : checks if the user operates the cluster table.
func verifyLightPrivilege(ses *Session,
	dbName string,
	writeDBTableDirect bool,
	isClusterTable bool,
	clusterTableOperation clusterTableOperationType) bool {
	var ok bool
	if ses.GetFromRealUser() && writeDBTableDirect {
		if len(dbName) == 0 {
			dbName = ses.GetDatabaseName()
		}
		if !canWriteProtectedDatabase(ses) && isProtectedDatabase(ses, dbName) {
			return false
		}
		dbName = strings.ToLower(dbName)
		if ok2 := isBannedDatabase(dbName); ok2 {
			if isClusterTable {
				ok = verifyAccountCanOperateClusterTable(ses.GetTenantInfo(), dbName, clusterTableOperation)
			} else {
				ok = false
			}
		} else {
			ok = !isClusterTable
		}
	} else {
		ok = true
	}
	return ok
}

// getDefaultAccount returns the internal account
func getDefaultAccount() *TenantInfo {
	return &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
		delimiter:     ':',
	}
}

// verifyPrivilegeEntryInMultiPrivilegeLevelsInCache checks privilege entry
// in the cache only.
func verifyPrivilegeEntryInMultiPrivilegeLevelsInCache(
	ses *Session,
	cache *privilegeCache,
	entry privilegeEntry,
	pls []privilegeLevelType) (bool, error) {
	var yes bool
	dbName := entry.databaseName
	if len(dbName) == 0 {
		dbName = ses.GetDatabaseName()
	}
	if cache != nil {
		for _, pl := range pls {
			yes = cache.has(entry.objType, pl, dbName, entry.tableName, entry.privilegeId)
			if yes {
				return true, nil
			}
		}
	}
	return false, nil
}

// checkPrivilegeInCache checks the privilege in the cache first.
var checkPrivilegeInCache = func(ctx context.Context, ses *Session, priv *privilege, enableCache bool) (bool, error) {
	var err error
	var pls []privilegeLevelType
	var yes2, yes bool
	cache := ses.GetPrivilegeCache()
	if cache != nil && enableCache {
		for _, entry := range priv.entries {
			if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
				pls, err = getPrivilegeLevelsOfObjectType(ctx, entry.objType)
				if err != nil {
					return false, err
				}

				yes2 = verifyLightPrivilege(ses,
					entry.databaseName,
					priv.writeDatabaseAndTableDirectly,
					priv.isClusterTable,
					priv.clusterTableOperation)

				if yes2 {
					yes, err = verifyPrivilegeEntryInMultiPrivilegeLevelsInCache(ses, cache, entry, pls)
					if err != nil {
						return false, err
					}
				}

				if yes {
					return true, nil
				}
			} else if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
				if entry.compound != nil {
					allTrue := true
					//multi privileges take effect together
					for _, mi := range entry.compound.items {
						if mi.privilegeTyp == PrivilegeTypeCanGrantRoleToOthersInCreateUser {
							//TODO: normalize the name
							//TODO: simplify the logic
							// yes, err = determineUserCanGrantRolesToOthersInternal(ctx, bh, ses, []*tree.Role{mi.role})
							// if err != nil {
							// 	return false, err
							// }
							// if yes {
							// 	from := &verifiedRole{
							// 		typ:  roleType,
							// 		name: mi.role.UserName,
							// 	}
							// 	for _, user := range mi.users {
							// 		to := &verifiedRole{
							// 			typ:  userType,
							// 			name: user.Username,
							// 		}
							// 		err = verifySpecialRolesInGrant(ctx, ses.GetTenantInfo(), from, to)
							// 		if err != nil {
							// 			return false, err
							// 		}
							// 	}
							// }
							yes = false
						} else {
							if len(mi.originViews) > 0 || mi.directView != "" {
								// View chains require metadata checks; skip cache-only evaluation.
								return false, nil
							}
							tempEntry := privilegeEntriesMap[mi.privilegeTyp]
							tempEntry.databaseName = mi.dbName
							tempEntry.tableName = mi.tableName
							tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
							tempEntry.compound = nil
							pls, err = getPrivilegeLevelsOfObjectType(ctx, tempEntry.objType)
							if err != nil {
								return false, err
							}

							writeDirectly := priv.writeDatabaseAndTableDirectly
							if (tempEntry.objType == objectTypeTable || tempEntry.objType == objectTypeView) && mi.privilegeTyp == PrivilegeTypeSelect {
								writeDirectly = false
							}
							yes2 = verifyLightPrivilege(ses,
								tempEntry.databaseName,
								writeDirectly,
								mi.isClusterTable,
								mi.clusterTableOperation)

							if yes2 {
								//At least there is one success
								yes, err = verifyPrivilegeEntryInMultiPrivilegeLevelsInCache(ses, cache, tempEntry, pls)
								if err != nil {
									return false, err
								}
							}
						}
						if !yes {
							allTrue = false
							break
						}
					}

					if allTrue {
						return allTrue, nil
					}
				}
			}
		}
	}
	return false, nil
}

// privilegeCacheIsEnabled checks if the privilege cache is enabled.
var privilegeCacheIsEnabled = func(ctx context.Context, ses *Session) (bool, error) {
	var err error
	var value interface{}
	var newValue bool
	value, err = ses.GetSessionSysVar("enable_privilege_cache")
	if err != nil {
		return false, err
	}

	newValue, err = valueIsBoolTrue(value)
	if err != nil {
		return false, err
	}

	return newValue, err
}

// hasMoCtrl checks whether the plan has mo_ctrl
func hasMoCtrl(p *plan2.Plan) bool {
	if p != nil && p.GetQuery() != nil { //select,insert select, update, delete
		q := p.GetQuery()
		if q.StmtType == plan.Query_INSERT || q.StmtType == plan.Query_SELECT {
			for _, node := range q.Nodes {
				if node != nil && node.NodeType == plan.Node_PROJECT {
					//restrict :
					//	select mo_ctrl ...
					//	insert into ... select mo_ctrl ...
					for _, proj := range node.ProjectList {
						if plan2.HasMoCtrl(proj) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// isTargetSysWhiteList checks if ALL DML target tables are in the whitelist.
// Returns true only when all target tables are in the whitelist.
// Returns false if any target table is not in the whitelist, or if there are no DML target tables.
func isTargetSysWhiteList(p *plan2.Plan) bool {
	if p == nil || p.GetQuery() == nil {
		return false
	}
	q := p.GetQuery()

	isInWhiteList := func(dbname, name string) bool {
		return dbname == catalog.MO_CATALOG && sysWhiteListTables[name] > 0
	}

	foundTarget := false
	for _, node := range q.Nodes {
		if node == nil {
			continue
		}
		// Only check actual DML target tables, not tables that are just being read
		switch node.NodeType {
		case plan.Node_MULTI_UPDATE:
			// For UPDATE/DELETE via MULTI_UPDATE, check all target tables in UpdateCtxList
			for _, updateCtx := range node.UpdateCtxList {
				if ref := updateCtx.ObjRef; ref != nil {
					foundTarget = true
					if !isInWhiteList(ref.SchemaName, ref.ObjName) {
						return false
					}
				}
			}
		case plan.Node_DELETE:
			// For DELETE, check the target table in DeleteCtx
			if node.DeleteCtx != nil && node.DeleteCtx.Ref != nil {
				ref := node.DeleteCtx.Ref
				foundTarget = true
				if !isInWhiteList(ref.SchemaName, ref.ObjName) {
					return false
				}
			}
		case plan.Node_INSERT:
			// For INSERT, check the target table in InsertCtx
			if node.InsertCtx != nil && node.InsertCtx.Ref != nil {
				ref := node.InsertCtx.Ref
				foundTarget = true
				if !isInWhiteList(ref.SchemaName, ref.ObjName) {
					return false
				}
			}
		}
	}
	// Return true only if we found at least one target table and all are in whitelist
	return foundTarget
}

// verifyAccountCanExecMoCtrl only sys account and moadmin role.
func verifyAccountCanExecMoCtrl(account *TenantInfo) bool {
	return account.IsSysTenant() && account.IsMoAdminRole()
}

func canWriteProtectedDatabase(ses *Session) bool {
	if ses == nil || ses.GetTenantInfo() == nil {
		return false
	}
	tenant := ses.GetTenantInfo()
	return tenant.IsAccountAdminRole() || tenant.IsMoAdminRole()
}

func normalizeProtectedDatabaseName(ses *Session, dbName string) string {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" && ses != nil {
		dbName = ses.GetDatabaseName()
	}
	return dbName
}

func getProtectedDatabaseSet(ses *Session) map[string]struct{} {
	if ses == nil {
		return nil
	}
	value, err := ses.GetGlobalSysVar(ProtectedDatabases)
	if err != nil {
		return nil
	}
	raw, ok := value.(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	protected := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		dbName := strings.TrimSpace(part)
		if dbName != "" {
			protected[dbName] = struct{}{}
		}
	}
	return protected
}

func isProtectedDatabase(ses *Session, dbName string) bool {
	dbName = normalizeProtectedDatabaseName(ses, dbName)
	if dbName == "" {
		return false
	}
	_, ok := getProtectedDatabaseSet(ses)[dbName]
	return ok
}

func checkProtectedDatabaseWrite(ctx context.Context, ses *Session, dbNames ...string) bool {
	if len(dbNames) == 0 {
		return true
	}
	if ses == nil || !ses.GetFromRealUser() {
		return true
	}
	if canWriteProtectedDatabase(ses) {
		return true
	}

	pDbs := getProtectedDatabaseSet(ses)
	if len(pDbs) == 0 {
		return true
	}

	return checkProtectedDatabaseWriteWithSet(ctx, ses, pDbs, dbNames...)
}

func checkProtectedDatabaseWriteWithSet(ctx context.Context, ses *Session, protectedDatabases map[string]struct{}, dbNames ...string) bool {
	if len(protectedDatabases) == 0 || len(dbNames) == 0 {
		return true
	}
	for _, dbName := range dbNames {
		dbName = normalizeProtectedDatabaseName(ses, dbName)
		if dbName == "" {
			continue
		}
		if _, ok := protectedDatabases[dbName]; ok {
			return false
		}
	}
	return true
}

func checkProtectedDatabaseWriteByStmt(ctx context.Context, ses *Session, stmt tree.Statement) bool {
	if ses == nil || !ses.GetFromRealUser() {
		return true
	}
	if canWriteProtectedDatabase(ses) {
		return true
	}
	protectedDatabases := getProtectedDatabaseSet(ses)
	if len(protectedDatabases) == 0 {
		return true
	}
	return checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, protectedDatabaseWriteTargetsFromStmt(stmt)...)
}

func privilegeTipWritesDatabase(tip privilegeTips) bool {
	switch tip.typ {
	case PrivilegeTypeSelect, PrivilegeTypeValues:
		return false
	default:
		return true
	}
}

func checkProtectedDatabaseWriteByPrivilegeTips(ctx context.Context, ses *Session, tips privilegeTipsArray) bool {
	if ses == nil || !ses.GetFromRealUser() {
		return true
	}
	if canWriteProtectedDatabase(ses) {
		return true
	}
	protectedDatabases := getProtectedDatabaseSet(ses)
	if len(protectedDatabases) == 0 {
		return true
	}
	dbNames := make([]string, 0, len(tips))
	for _, tip := range tips {
		if privilegeTipWritesDatabase(tip) {
			dbNames = append(dbNames, tip.databaseName)
		}
	}
	return checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, dbNames...)
}

func appendTableNameDatabaseName(dbNames []string, name *tree.TableName) []string {
	if name == nil {
		return dbNames
	}
	return append(dbNames, string(name.SchemaName))
}

func appendTableNamesDatabaseNames(dbNames []string, names tree.TableNames) []string {
	for _, name := range names {
		dbNames = appendTableNameDatabaseName(dbNames, name)
	}
	return dbNames
}

func functionNameDatabaseName(name *tree.FunctionName) string {
	if name == nil {
		return ""
	}
	return string(name.Name.SchemaName)
}

func procedureNameDatabaseName(name *tree.ProcedureName) string {
	if name == nil {
		return ""
	}
	return string(name.Name.SchemaName)
}

func protectedDatabaseWriteTargetsFromStmt(stmt tree.Statement) []string {
	dbNames := make([]string, 0, 2)
	switch st := stmt.(type) {
	case *tree.CreateDatabase:
		dbNames = append(dbNames, string(st.Name))
	case *tree.DropDatabase:
		dbNames = append(dbNames, string(st.Name))
	case *tree.AlterDataBaseConfig:
		if !st.IsAccountLevel {
			dbNames = append(dbNames, st.DbName)
		}
	case *tree.CreateTable:
		dbNames = appendTableNameDatabaseName(dbNames, &st.Table)
	case *tree.CreateView:
		dbNames = appendTableNameDatabaseName(dbNames, st.Name)
	case *tree.CreateSource:
		dbNames = appendTableNameDatabaseName(dbNames, st.SourceName)
	case *tree.CreateConnector:
		dbNames = appendTableNameDatabaseName(dbNames, st.TableName)
	case *tree.CreateSequence:
		dbNames = appendTableNameDatabaseName(dbNames, st.Name)
	case *tree.AlterSequence:
		dbNames = appendTableNameDatabaseName(dbNames, st.Name)
	case *tree.AlterView:
		dbNames = appendTableNameDatabaseName(dbNames, st.Name)
	case *tree.AlterTable:
		dbNames = appendTableNameDatabaseName(dbNames, st.Table)
	case *tree.RenameTable:
		for _, alter := range st.AlterTables {
			if alter == nil {
				continue
			}
			dbNames = appendTableNameDatabaseName(dbNames, alter.Table)
			for _, opt := range alter.Options {
				if renameOpt, ok := opt.(*tree.AlterOptionTableName); ok && renameOpt.Name != nil {
					target := renameOpt.Name.ToTableName()
					dbNames = append(dbNames, string(target.SchemaName))
				}
			}
		}
	case *tree.CreateFunction:
		dbNames = append(dbNames, functionNameDatabaseName(st.Name))
	case *tree.DropFunction:
		dbNames = append(dbNames, functionNameDatabaseName(st.Name))
	case *tree.CreateProcedure:
		dbNames = append(dbNames, procedureNameDatabaseName(st.Name))
	case *tree.DropProcedure:
		dbNames = append(dbNames, procedureNameDatabaseName(st.Name))
	case *tree.DropTable:
		dbNames = appendTableNamesDatabaseNames(dbNames, st.Names)
	case *tree.DropView:
		dbNames = appendTableNamesDatabaseNames(dbNames, st.Names)
	case *tree.DropSequence:
		dbNames = appendTableNamesDatabaseNames(dbNames, st.Names)
	case *tree.Load:
		dbNames = appendTableNameDatabaseName(dbNames, st.Table)
	case *tree.CreateIndex:
		dbNames = appendTableNameDatabaseName(dbNames, st.Table)
	case *tree.DropIndex:
		dbNames = appendTableNameDatabaseName(dbNames, st.TableName)
	case *tree.TruncateTable:
		dbNames = appendTableNameDatabaseName(dbNames, st.Name)
	case *tree.CloneTable:
		dbNames = appendTableNameDatabaseName(dbNames, &st.CreateTable.Table)
	case *tree.CloneDatabase:
		dbNames = append(dbNames, string(st.DstDatabase))
	case *tree.RestoreSnapShot:
		if st.Level == tree.RESTORELEVELDATABASE || st.Level == tree.RESTORELEVELTABLE {
			dbNames = append(dbNames, string(st.DatabaseName))
		}
	case *tree.RestorePitr:
		if st.Level == tree.RESTORELEVELDATABASE || st.Level == tree.RESTORELEVELTABLE {
			dbNames = append(dbNames, string(st.DatabaseName))
		}
	case *tree.DataBranchCreateTable:
		dbNames = appendTableNameDatabaseName(dbNames, &st.CreateTable.Table)
	case *tree.DataBranchDeleteTable:
		dbNames = appendTableNameDatabaseName(dbNames, &st.TableName)
	case *tree.DataBranchCreateDatabase:
		dbNames = append(dbNames, string(st.DstDatabase))
	case *tree.DataBranchDeleteDatabase:
		dbNames = append(dbNames, string(st.DatabaseName))
	case *tree.DataBranchMerge:
		dbNames = appendTableNameDatabaseName(dbNames, &st.DstTable)
	case *tree.DataBranchPick:
		dbNames = appendTableNameDatabaseName(dbNames, &st.DstTable)
	}
	return dbNames
}
