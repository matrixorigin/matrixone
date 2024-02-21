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
	"go.uber.org/zap"
)

// MaybeUpgradeTenant used to check the tenant need upgrade or not. If need upgrade, it will
// upgrade the tenant immediately in current txn.
func (s *service) MaybeUpgradeTenant(
	ctx context.Context,
	tenantFetchFunc func() (int32, string, error),
	txnOp client.TxnOperator) (bool, error) {
	tenantID, version, err := tenantFetchFunc()
	if err != nil {
		return false, err
	}

	s.mu.RLock()
	checked := s.mu.tenants[tenantID]
	s.mu.RUnlock()
	if checked {
		return false, nil
	}

	upgraded := false
	opts := executor.Options{}.WithTxn(txnOp)
	err = s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			// tenant create at current cn, can work correctly
			currentCN := s.getFinalVersionHandle().Metadata()
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
				if s.upgrade.finalVersionCompleted.Load() {
					break
				}

				upgrades, err := versions.GetUpgradeVersions(latestVersion.Version, txn, false, true)
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
			for _, v := range s.handles {
				if versions.Compare(v.Metadata().Version, from) > 0 &&
					v.Metadata().CanDirectUpgrade(from) {
					if err := v.HandleTenantUpgrade(ctx, tenantID, txn); err != nil {
						return err
					}
					if err := versions.UpgradeTenantVersion(tenantID, v.Metadata().Version, txn); err != nil {
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
	s.mu.Lock()
	s.mu.tenants[tenantID] = true
	s.mu.Unlock()
	return upgraded, nil
}

// asyncUpgradeTenantTask is a task to execute the tenant upgrade logic in
// parallel based on the grouped tenant batch.
func (s *service) asyncUpgradeTenantTask(ctx context.Context) {
	fn := func() (bool, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Hour*24)
		defer cancel()

		hasUpgradeTenants := false
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied()
		err := s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				upgrade, ok, err := versions.GetUpgradingTenantVersion(txn)
				if err != nil {
					getUpgradeLogger().Error("failed to get upgrading tenant version",
						zap.Error(err))
					return err
				}

				getUpgradeLogger().Info("get upgrading tenant version",
					zap.String("upgrade", upgrade.String()),
					zap.Bool("has", ok))
				if !ok || upgrade.TotalTenant == upgrade.ReadyTenant {
					return nil
				}

				// no upgrade logic on current cn, skip
				v := s.getFinalVersionHandle().Metadata().Version
				if versions.Compare(upgrade.ToVersion, v) > 0 {
					getUpgradeLogger().Info("skip upgrade tenant",
						zap.String("final", v),
						zap.String("to", upgrade.ToVersion))
					return nil
				}

				// select task and tenants for update
				taskID, tenants, createVersions, err := versions.GetUpgradeTenantTasks(upgrade.ID, txn)
				if err != nil {
					getUpgradeLogger().Error("failed to load upgrade tenants",
						zap.String("upgrade", upgrade.String()),
						zap.Error(err))
					return err
				}

				getUpgradeLogger().Info("load upgrade tenants",
					zap.Int("count", len(tenants)),
					zap.String("upgrade", upgrade.String()))
				if len(tenants) == 0 {
					return nil
				}

				hasUpgradeTenants = true
				h := s.getVersionHandle(upgrade.ToVersion)
				updated := int32(0)
				for i, id := range tenants {
					createVersion := createVersions[i]

					getUpgradeLogger().Info("upgrade tenant",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))

					// createVersion >= upgrade.ToVersion already upgrade
					if versions.Compare(createVersion, upgrade.ToVersion) >= 0 {
						continue
					}

					getUpgradeLogger().Info("execute upgrade tenant",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))

					if err := h.HandleTenantUpgrade(ctx, id, txn); err != nil {
						getUpgradeLogger().Error("failed to execute upgrade tenant",
							zap.Int32("tenant", id),
							zap.String("tenant-version", createVersion),
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}

					if err := versions.UpgradeTenantVersion(id, h.Metadata().Version, txn); err != nil {
						getUpgradeLogger().Error("failed to update upgrade tenant create version",
							zap.Int32("tenant", id),
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}

					getUpgradeLogger().Info("execute upgrade tenant completed",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))
					updated++
				}

				if err := versions.UpdateUpgradeTenantTaskState(taskID, versions.Yes, txn); err != nil {
					getUpgradeLogger().Error("failed to update upgrade tenant state",
						zap.String("upgrade", upgrade.String()))
					return err
				}
				getUpgradeLogger().Info("tenant state updated",
					zap.Int32("from", tenants[0]),
					zap.Int32("to", tenants[len(tenants)-1]),
					zap.String("upgrade", upgrade.String()))

				// update count, we need using select for update to avoid concurrent update
				upgrade, err = versions.GetUpgradeVersionForUpdateByID(upgrade.ID, txn)
				if err != nil {
					getUpgradeLogger().Error("failed to get latest upgrade info",
						zap.String("upgrade", upgrade.String()))
					return err
				}

				upgrade.ReadyTenant += updated
				if upgrade.TotalTenant < upgrade.ReadyTenant {
					panic(fmt.Sprintf("BUG: invalid upgrade tenant, upgrade %s, updated %d", upgrade.String(), updated))
				}

				getUpgradeLogger().Info("upgrade tenant ready count changed",
					zap.String("upgrade", upgrade.String()))

				if upgrade.State == versions.StateReady {
					return nil
				}
				return versions.UpdateVersionUpgradeTasks(upgrade, txn)
			},
			opts)
		if err != nil {
			getUpgradeLogger().Error("tenant task handle failed",
				zap.Error(err))
			return false, err
		}
		return hasUpgradeTenants, nil
	}

	timer := time.NewTimer(s.upgrade.checkUpgradeTenantDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if s.upgrade.finalVersionCompleted.Load() {
				return
			}

			for {
				if hasUpgradeTenants, err := fn(); err != nil ||
					hasUpgradeTenants {
					continue
				}
				break
			}
			timer.Reset(s.upgrade.checkUpgradeTenantDuration)
		}
	}
}

func fetchTenants(
	batch int,
	fn func([]int32) error,
	txn executor.TxnExecutor) error {
	last := int32(-1)
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
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				last = vector.GetFixedAt[int32](cols[0], i)
				ids = append(ids, last)
				n++
			}
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
