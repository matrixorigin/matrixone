// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_6

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	moRolePrivsObjectIDIndexName = "idx_mo_role_privs_obj_id"
	orphanPrivilegePageSize      = uint64(1000)
)

const deleteOrphanDatabasePrivilegesSQL = `delete from mo_catalog.mo_role_privs
where obj_id != 0
  and ((obj_type = 'database' and privilege_level = 'd')
    or (obj_type in ('table','view') and privilege_level in ('d.*','*')))
  and not exists (
    select 1 from mo_catalog.mo_database d
    where d.account_id = current_account_id()
      and d.dat_id = mo_role_privs.obj_id)
limit 1000`

const deleteOrphanRelationPrivilegesSQL = `delete from mo_catalog.mo_role_privs
where obj_id != 0
  and obj_type in ('table','view')
  and privilege_level in ('d.t','t')
  and not exists (
    select 1 from mo_catalog.mo_tables t
    where t.account_id = current_account_id()
      and t.rel_logical_id = mo_role_privs.obj_id)
limit 1000`

func addMoRolePrivsObjectIDIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_role_privs",
		UpgType:   versions.ADD_INDEX,
		UpgSql: "create index " + moRolePrivsObjectIDIndexName +
			" on mo_catalog.mo_role_privs(obj_id)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn,
				accountID,
				catalog.MO_CATALOG,
				"mo_role_privs",
				moRolePrivsObjectIDIndexName,
			)
		},
	}
}

// MaintainOrphanObjectPrivilegesPage performs at most one bounded destructive
// page. It has no completion marker: callers revisit every tenant indefinitely,
// so a mixed-version writer can only create work for a later pass.
func MaintainOrphanObjectPrivilegesPage(
	txn executor.TxnExecutor,
	accountID uint32,
) (tenantClean bool, err error) {
	indexEntry := addMoRolePrivsObjectIDIndex()
	exists, err := indexEntry.CheckFunc(txn, accountID)
	if err != nil {
		return false, err
	}
	option := versions.UpgradeStatementOption(accountID)
	if !exists {
		res, err := txn.Exec(indexEntry.UpgSql, option)
		if err != nil {
			return false, err
		}
		res.Close()
		return false, nil
	}

	res, err := txn.Exec(deleteOrphanDatabasePrivilegesSQL, option)
	if err != nil {
		return false, err
	}
	affected := res.AffectedRows
	res.Close()
	if affected != 0 {
		return false, nil
	}

	res, err = txn.Exec(deleteOrphanRelationPrivilegesSQL, option)
	if err != nil {
		return false, err
	}
	affected = res.AffectedRows
	res.Close()
	return affected < orphanPrivilegePageSize, nil
}
