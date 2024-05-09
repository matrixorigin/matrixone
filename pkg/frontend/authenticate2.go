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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
func checkPrivilegeInCache(ctx context.Context, ses *Session, priv *privilege, enableCache bool) (bool, error) {
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
							//yes, err = determineUserCanGrantRolesToOthersInternal(ctx, bh, ses, []*tree.Role{mi.role})
							//if err != nil {
							//	return false, err
							//}
							//if yes {
							//	from := &verifiedRole{
							//		typ:  roleType,
							//		name: mi.role.UserName,
							//	}
							//	for _, user := range mi.users {
							//		to := &verifiedRole{
							//			typ:  userType,
							//			name: user.Username,
							//		}
							//		err = verifySpecialRolesInGrant(ctx, ses.GetTenantInfo(), from, to)
							//		if err != nil {
							//			return false, err
							//		}
							//	}
							//}
						} else {
							tempEntry := privilegeEntriesMap[mi.privilegeTyp]
							tempEntry.databaseName = mi.dbName
							tempEntry.tableName = mi.tableName
							tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
							tempEntry.compound = nil
							pls, err = getPrivilegeLevelsOfObjectType(ctx, tempEntry.objType)
							if err != nil {
								return false, err
							}

							yes2 = verifyLightPrivilege(ses,
								tempEntry.databaseName,
								priv.writeDatabaseAndTableDirectly,
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
func privilegeCacheIsEnabled(ctx context.Context, ses *Session) (bool, error) {
	var err error
	var value interface{}
	var newValue bool
	value, err = ses.GetSessionVar(ctx, "enable_privilege_cache")
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

// verifyAccountCanExecMoCtrl only sys account and moadmin role.
func verifyAccountCanExecMoCtrl(account *TenantInfo) bool {
	return account.IsSysTenant() && account.IsMoAdminRole()
}
