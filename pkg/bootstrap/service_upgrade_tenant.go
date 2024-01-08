package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// MaybeUpgradeTenant used to check the tenant need upgrade or not. If need upgrade, it will
// upgrade the tenant immediately in current txn.
func (s *service) MaybeUpgradeTenant(
	ctx context.Context,
	tenantFetchFunc func() (int32, string, error),
	txnOp client.TxnOperator) (bool, error) {
	upgraded := false
	opts := executor.Options{}.WithTxn(txnOp)
	err := s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			tenantID, version, err := tenantFetchFunc()
			if err != nil {
				return err
			}

			// tenant create at current cn, can work correctly
			currentCN := getFinalVersionHandle().Metadata()
			if currentCN.Version == version {
				return nil
			} else if versions.Compare(currentCN.Version, version) < 0 {
				// tenant create at 1.4.0, current tenant version 1.5.0, it must be cannot work
				return moerr.NewInvalidInputNoCtx("tenant version %s is greater than current cn version %s",
					version, currentCN.Version)
			}

			// arrive here means tenant version < current cn version, need upgrade.
			// and currentCN.Version == last cluster version

			latestVersion, err := versions.GetLatestVersion(txn)
			if err != nil {
				return err
			}
			if latestVersion.Version != currentCN.Version {
				panic("BUG: current cn's version(" +
					currentCN.Version +
					") must equal cluster latest version(" +
					latestVersion.Version +
					")")
			}

			upgraded = true
			for {
				// upgrade completed
				if finalVersionCompleted.Load() {
					break
				}

				upgrades, err := versions.GetUpgradeVersions(latestVersion.Version, txn, false)
				if err != nil {
					return err
				}
				// latest cluster is already upgrade completed
				if upgrades[len(upgrades)-1].State == versions.StateUpgradingTenant ||
					upgrades[len(upgrades)-1].State == versions.StateReady {
					break
				}

				time.Sleep(time.Second)
			}

			// upgrade in current goroutine immediately
			version, err = versions.GetTenantCreateVersionForUpdate(tenantID, txn)
			if err != nil {
				return err
			}
			from := version
			for _, v := range handles {
				if versions.Compare(v.Metadata().Version, from) > 0 &&
					v.Metadata().CanDirectUpgrade(from) {
					if err := v.HandleTenantUpgrade(ctx, tenantID, txn); err != nil {
						return err
					}
					from = v.Metadata().Version
				}
			}
			return nil
		},
		opts)
	if err != nil {
		return false, err
	}
	return upgraded, nil
}

// asyncUpgradeTenantTask is a task to execute the tenant upgrade logic in
// parallel based on the grouped tenant batch.
func (s *service) asyncUpgradeTenantTask(ctx context.Context) {
	fn := func() error {
		ctx, cancel := context.WithTimeout(ctx, time.Hour*24)
		defer cancel()

		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied()
		return s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				upgrade, ok, err := versions.GetUpgradingTenantVersion(txn)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				if upgrade.TotalTenant == upgrade.ReadyTenant {
					panic("BUG: invalid upgrade tenant")
				}

				// no upgrade logic on current cn, skip
				v := getFinalVersionHandle().Metadata().Version
				if versions.Compare(upgrade.ToVersion, v) > 0 {
					return nil
				}

				// select task and tenants for update
				taskID, tenants, createVersions, err := versions.GetUpgradeTenantTasks(upgrade.ID, txn)
				if err != nil {
					return err
				}
				if len(tenants) == 0 {
					return nil
				}
				h := getVersionHandle(upgrade.ToVersion)
				for i, id := range tenants {
					createVersion := createVersions[i]
					// createVersion >= upgrade.ToVersion already upgrade
					if versions.Compare(createVersion, upgrade.ToVersion) >= 0 {
						continue
					}
					if !h.Metadata().CanDirectUpgrade(createVersion) {
						panic("BUG: invalid upgrade")
					}
					if err := h.HandleTenantUpgrade(ctx, id, txn); err != nil {
						return err
					}
				}
				if err := versions.UpdateUpgradeTenantTaskState(taskID, versions.Yes, txn); err != nil {
					return err
				}

				// update count, we need using select for update to avoid concurrent update
				upgrade, err = versions.GetUpgradeVersionForUpdateByID(upgrade.ID, txn)
				if err != nil {
					return err
				}
				upgrade.ReadyTenant += int32(len(tenants))
				if upgrade.TotalTenant < upgrade.ReadyTenant {
					panic("BUG: invalid upgrade tenant")
				}
				return versions.UpdateVersionUpgradeTasks(upgrade, txn)
			},
			opts)
	}

	timer := time.NewTimer(checkUpgradeTenantDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if finalVersionCompleted.Load() {
				return
			}

			if err := fn(); err != nil {
				continue
			}
			timer.Reset(checkUpgradeTenantDuration)
		}
	}
}

func fetchTenants(
	batch int,
	fn func([]int32) error,
	txn executor.TxnExecutor) error {
	last := int32(0)
	var ids []int32
	for {
		ids = ids[:0]
		sql := fmt.Sprintf("select account_id from mo_account where account_id > %d order by account_id limit %d",
			last,
			batch)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		n := 0
		res.ReadRows(func(cols []*vector.Vector) bool {
			last = executor.GetFixedRows[int32](cols[0])[0]
			ids = append(ids, last)
			n++
			return true
		})
		res.Close()
		if n == 0 {
			return nil
		}
		if err := fn(ids); err != nil {
			return err
		}
	}
}
