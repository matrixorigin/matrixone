// Copyright 2024 Matrix Origin
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

package versions

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func GetUpgradeVersions(
	finalVersion string,
	txn executor.TxnExecutor,
	forUpdate bool,
	mustHave bool) ([]VersionUpgrade, error) {
	sql := fmt.Sprintf(`select 
			id,
			from_version, 
			to_version, 
			final_version, 
			state, 
			upgrade_order,
			upgrade_cluster,
			upgrade_tenant,
			total_tenant,
			ready_tenant from %s 
			where final_version = '%s' 
			order by upgrade_order asc`,
		catalog.MOUpgradeTable,
		finalVersion)
	if forUpdate {
		sql = fmt.Sprintf("%s for update", sql)
	}
	values, err := getVersionUpgradesBySQL(sql, txn)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 && mustHave {
		panic("BUG: missing version upgrade")
	}
	return values, nil
}

func GetUpgradeVersionsByOrder(
	finalVersion string,
	order int32,
	txn executor.TxnExecutor) ([]VersionUpgrade, error) {
	sql := fmt.Sprintf(`select 
			id,
			from_version, 
			to_version, 
			final_version, 
			state, 
			upgrade_order,
			upgrade_cluster,
			upgrade_tenant,
			total_tenant,
			ready_tenant from %s 
			where final_version = '%s' and upgrade_order = %d`,
		catalog.MOUpgradeTable,
		finalVersion,
		order)
	values, err := getVersionUpgradesBySQL(sql, txn)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func GetUpgradingTenantVersion(txn executor.TxnExecutor) (VersionUpgrade, bool, error) {
	sql := fmt.Sprintf(`select 
			id,
			from_version, 
			to_version, 
			final_version, 
			state, 
			upgrade_order,
			upgrade_cluster,
			upgrade_tenant,
			total_tenant,
			ready_tenant
			from %s 
			where state = %d 
			order by upgrade_order asc`,
		catalog.MOUpgradeTable,
		StateUpgradingTenant)
	values, err := getVersionUpgradesBySQL(sql, txn)
	if err != nil {
		return VersionUpgrade{}, false, err
	}
	if len(values) == 0 {
		return VersionUpgrade{}, false, nil
	}
	if len(values) != 1 {
		panic("BUG: invalid version upgrade")
	}
	return values[0], true, nil
}

func GetUpgradeVersionForUpdateByID(
	id uint64,
	txn executor.TxnExecutor) (VersionUpgrade, error) {
	sql := fmt.Sprintf(`select 
			id,
			from_version, 
			to_version, 
			final_version, 
			state, 
			upgrade_order,
			upgrade_cluster,
			upgrade_tenant,
			total_tenant,
			ready_tenant
			from %s 
			where id = %d for update`,
		catalog.MOUpgradeTable,
		id)
	values, err := getVersionUpgradesBySQL(sql, txn)
	if err != nil {
		return VersionUpgrade{}, err
	}
	if len(values) != 1 {
		panic(fmt.Sprintf("BUG: can not get version upgrade by primary key: %d", id))
	}
	return values[0], nil
}

func AddVersionUpgrades(
	values []VersionUpgrade,
	txn executor.TxnExecutor) error {
	for _, v := range values {
		sql := GetVersionUpgradeSQL(v)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		res.Close()
	}
	return nil
}

func UpdateVersionUpgradeState(
	upgrade VersionUpgrade,
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("update %s set state = %d, update_at = current_timestamp() where final_version = '%s' and upgrade_order = %d",
		catalog.MOUpgradeTable,
		state,
		upgrade.FinalVersion,
		upgrade.UpgradeOrder)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func UpdateVersionUpgradeTasks(
	upgrade VersionUpgrade,
	txn executor.TxnExecutor) error {
	updateState := ""
	if upgrade.TotalTenant == upgrade.ReadyTenant {
		updateState = fmt.Sprintf("state = %d,", StateReady)
	}
	sql := fmt.Sprintf("update %s set total_tenant = %d, ready_tenant = %d,%s update_at = current_timestamp() where final_version = '%s' and upgrade_order = %d",
		catalog.MOUpgradeTable,
		upgrade.TotalTenant,
		upgrade.ReadyTenant,
		updateState,
		upgrade.FinalVersion,
		upgrade.UpgradeOrder)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func GetVersionUpgradeSQL(v VersionUpgrade) string {
	return fmt.Sprintf(`insert into %s (from_version, 
		to_version, 
		final_version, 
		state, 
		upgrade_cluster, 
		upgrade_tenant, 
		upgrade_order, 
		total_tenant, 
		ready_tenant, 
		create_at, 
		update_at) values ('%s', '%s', '%s', %d, %d, %d, %d, %d, %d, current_timestamp(), current_timestamp())`,
		catalog.MOUpgradeTable,
		v.FromVersion,
		v.ToVersion,
		v.FinalVersion,
		v.State,
		v.UpgradeCluster,
		v.UpgradeTenant,
		v.UpgradeOrder,
		v.TotalTenant,
		v.ReadyTenant)
}

func getVersionUpgradesBySQL(
	sql string,
	txn executor.TxnExecutor) ([]VersionUpgrade, error) {
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var values []VersionUpgrade
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			value := VersionUpgrade{}
			value.ID = vector.GetFixedAt[uint64](cols[0], i)
			value.FromVersion = cols[1].GetStringAt(i)
			value.ToVersion = cols[2].GetStringAt(i)
			value.FinalVersion = cols[3].GetStringAt(i)
			value.State = vector.GetFixedAt[int32](cols[4], i)
			value.UpgradeOrder = vector.GetFixedAt[int32](cols[5], i)
			value.UpgradeCluster = vector.GetFixedAt[int32](cols[6], i)
			value.UpgradeTenant = vector.GetFixedAt[int32](cols[7], i)
			value.TotalTenant = vector.GetFixedAt[int32](cols[8], i)
			value.ReadyTenant = vector.GetFixedAt[int32](cols[9], i)
			values = append(values, value)
		}

		return true
	})
	return values, nil
}
