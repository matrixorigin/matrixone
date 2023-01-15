package frontend

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
