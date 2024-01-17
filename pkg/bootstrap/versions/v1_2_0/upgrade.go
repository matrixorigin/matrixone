package v1_2_0

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	txn executor.TxnExecutor,
	final bool) error {
	txn.Use(catalog.MO_CATALOG)
	created, err := versions.IsFrameworkTablesCreated(txn)
	if err != nil {
		return err
	}
	if !created {
		// Many cn maybe create framework tables parallel, only one can create success.
		// Just return error, and upgrade framework will retry.
		return v.createFrameworkTables(txn, final)
	}
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

func (v *versionHandle) createFrameworkTables(
	txn executor.TxnExecutor,
	final bool) error {
	values := versions.FrameworkInitSQLs
	if final {
		values = append(values, v.metadata.GetInsertSQL(versions.StateReady))
	}

	for _, sql := range values {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
