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
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "1.2.0",
			MinUpgradeVersion: "1.1.0",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.No,
		},
	}
)

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	txn.Use(catalog.MO_CATALOG)
	created, err := v.isFrameworkTablesCreated(txn)
	if err != nil {
		return err
	}
	if !created {
		// Many cn maybe create framework tables parallel, only one can create success.
		// Just return error, and upgrade framework will retry.
		return v.createFrameworkTables(txn)
	}
	return nil
}

func (v *versionHandle) HandleCreateTenant(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	// TODO: move create tenant logic here
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) isFrameworkTablesCreated(txn executor.TxnExecutor) (bool, error) {
	res, err := txn.Exec("show tables", executor.StatementOption{})
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

func (v *versionHandle) createFrameworkTables(txn executor.TxnExecutor) error {
	values := append(versions.FrameworkInitSQLs,
		v.metadata.GetInsertSQL(),
		versions.GetVersionUpgradeSQL(versions.VersionUpgrade{
			FromVersion:    v.metadata.MinUpgradeVersion,
			ToVersion:      v.metadata.Version,
			FinalVersion:   v.metadata.Version,
			UpgradeOrder:   0,
			UpgradeCluster: v.metadata.UpgradeCluster,
			UpgradeTenant:  v.metadata.UpgradeTenant,
			State:          versions.StateCreated,
		}))
	for _, sql := range values {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
