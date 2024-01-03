package v1_2_0

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	Handler = &upgradeHandle{
		metadata: versions.Version{
			Version:           "1.2.0",
			MinUpgradeVersion: "1.1.0",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.No,
		},
	}
)

type upgradeHandle struct {
	metadata versions.Version
}

func (u *upgradeHandle) Metadata() versions.Version {
	return u.metadata
}

func (u *upgradeHandle) Prepare(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	txn.Use(catalog.MO_CATALOG)
	created, err := u.isFrameworkTablesCreated(txn)
	if err != nil {
		return err
	}
	if !created {
		// Many cn maybe create framework tables parallel, only one can create success.
		// Just return error, and upgrade framework will retry.
		return u.createFrameworkTables(txn)
	}
	return nil
}

func (u *upgradeHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor) error {
	return nil
}

func (u *upgradeHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	return nil
}

func (u *upgradeHandle) isFrameworkTablesCreated(txn executor.TxnExecutor) (bool, error) {
	res, err := txn.Exec("show tables")
	if err != nil {
		return false, err
	}
	defer res.Close()

	var tables []string
	res.ReadRows(func(cols []*vector.Vector) bool {
		tables = append(tables, executor.GetStringRows(cols[0])...)
		return true
	})
	for _, t := range tables {
		if strings.EqualFold(t, catalog.MOVersionTable) {
			return true, nil
		}
	}
	return false, nil
}

func (u *upgradeHandle) createFrameworkTables(txn executor.TxnExecutor) error {
	values := append(versions.FrameworkInitSQLs,
		u.metadata.GetInsertSQL(),
		versions.GetVersionUpgradeSQL(versions.VersionUpgrade{
			FromVersion:    u.metadata.MinUpgradeVersion,
			ToVersion:      u.metadata.Version,
			FinalVersion:   u.metadata.Version,
			UpgradeOrder:   0,
			UpgradeCluster: u.metadata.UpgradeCluster,
			UpgradeTenant:  u.metadata.UpgradeTenant,
			State:          versions.StateCreated,
		}))
	for _, sql := range values {
		r, err := txn.Exec(sql)
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
