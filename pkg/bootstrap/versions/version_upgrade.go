package versions

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func MustGetLatestReadyVersion(
	txn executor.TxnExecutor) (string, error) {
	sql := fmt.Sprintf(`select version from %s 
			where state = %d 
			order by create_at desc limit 1`,
		catalog.MOUpgradeTable,
		StateReady)

	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return "", err
	}
	defer res.Close()

	version := ""
	res.ReadRows(func(cols []*vector.Vector) bool {
		version = executor.GetStringRows(cols[0])[0]
		return true
	})
	if version == "" {
		panic("missing latest ready version")
	}
	return version, nil
}

func GetUpgradeVersions(
	finalVersion string,
	txn executor.TxnExecutor,
	forUpdate bool) ([]VersionUpgrade, error) {
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
	if len(values) == 0 {
		panic("BUG: missing version upgrade")
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
			where id = %d`,
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
		res, err := txn.Exec(GetVersionUpgradeSQL(v), executor.StatementOption{})
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
	state := No
	if upgrade.TotalTenant == upgrade.ReadyTenant {
		state = Yes
	}
	sql := fmt.Sprintf("update %s set total_tenant = %d, ready_tenant = %d, state = %d, update_at = current_timestamp() where final_version = '%s' and upgrade_order = %d",
		catalog.MOUpgradeTable,
		upgrade.TotalTenant,
		upgrade.ReadyTenant,
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
	res.ReadRows(func(cols []*vector.Vector) bool {
		value := VersionUpgrade{}
		value.ID = executor.GetFixedRows[uint64](cols[0])[0]
		value.FromVersion = executor.GetStringRows(cols[1])[0]
		value.ToVersion = executor.GetStringRows(cols[2])[0]
		value.FinalVersion = executor.GetStringRows(cols[3])[0]
		value.State = executor.GetFixedRows[int32](cols[4])[0]
		value.UpgradeOrder = executor.GetFixedRows[int32](cols[5])[0]
		value.UpgradeCluster = executor.GetFixedRows[int32](cols[6])[0]
		value.UpgradeTenant = executor.GetFixedRows[int32](cols[7])[0]
		value.TotalTenant = executor.GetFixedRows[int32](cols[8])[0]
		value.ReadyTenant = executor.GetFixedRows[int32](cols[9])[0]
		values = append(values, value)
		return true
	})
	return values, nil
}
