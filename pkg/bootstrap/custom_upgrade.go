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
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
	"time"
)

func (s *service) UpgradeTenant(ctx context.Context, tenantName string, isALLAccount bool) (bool, error) {
	// Before manually upgrade, check if there are unready upgrade tasks in system upgrade environment
	err := s.UpgradePreCheck(ctx)
	if err != nil {
		return true, err
	}

	if isALLAccount {
		return true, s.BootstrapUpgrade(ctx)
	} else {
		err = s.CheckAndUpgradeCluster(ctx)
		if err != nil {
			return true, err
		}

		tenantID, err := s.CheckUpgradeAccount(ctx, tenantName)
		if err != nil {
			return true, err
		}

		if err = s.UpgradeOneTenant(ctx, tenantID); err != nil {
			return true, err
		}
	}
	return true, nil
}

// CheckAndUpgradeCluster Before manually upgrade, it is necessary to ensure that the cluster upgrade is completed.
// When performing the cluster upgrade, the tenant upgrade task will be initialized
func (s *service) CheckAndUpgradeCluster(ctx context.Context) error {
	s.adjustUpgrade()

	if err := retryRun(ctx, "doCheckUpgrade", s.doCheckUpgrade); err != nil {
		getUpgradeLogger().Error("check upgrade failed", zap.Error(err))
		return err
	}
	if err := s.stopper.RunTask(s.asyncUpgradeTask); err != nil {
		return err
	}
	return nil
}

func (s *service) UpgradeOneTenant(ctx context.Context, tenantID int32) error {
	s.mu.RLock()
	checked := s.mu.tenants[tenantID]
	s.mu.RUnlock()
	if checked {
		return nil
	}

	opts := executor.Options{}.
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	err := s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)

			version, err := versions.GetTenantVersion(tenantID, txn)
			if err != nil {
				return err
			}

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

			for {
				// upgrade completed
				if s.upgrade.finalVersionCompleted.Load() {
					break
				}

				upgrades, err := versions.GetUpgradeVersions(latestVersion.Version, latestVersion.VersionOffset, txn, false, true)
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
		return err
	}
	s.mu.Lock()
	s.mu.tenants[tenantID] = true
	s.mu.Unlock()
	return nil
}

// CheckUpgradeAccount Custom upgrade account Check if the tenant name exists and is legal
func (s *service) CheckUpgradeAccount(ctx context.Context, accountName string) (int32, error) {
	var accountId int32

	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	err := s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			var err error = nil
			accountId, err = GetAccountIdByName(accountName, txn)
			if err != nil {
				getUpgradeLogger().Error("failed to get accountId by accountName when upgrade account",
					zap.Error(err))
				return err
			}
			return nil
		},
		opts)
	return accountId, err
}

// GetAccountIdByName get accountId by accountName when upgrade account
func GetAccountIdByName(accountName string, txn executor.TxnExecutor) (int32, error) {
	sql := fmt.Sprintf("select account_id, account_name from %s.%s where account_name = '%s'",
		catalog.MO_CATALOG, catalog.MOAccountTable, accountName)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return -1, err
	}

	// Check if the group account name exists
	var accountId int32 = -1
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		accountId = vector.GetFixedAt[int32](cols[0], 0)
		return true
	})

	if accountId == -1 {
		return -1, moerr.NewInvalidInputNoCtx("The input account name '%s' is invalid, please check input!", accountName)
	}
	return accountId, nil
}

// UpgradePreCheck Manual upgrade environment pre check, check if there are any unready upgrade tasks in upgrade environment.
// If there are, the unready upgrade tasks need to be processed first
func (s *service) UpgradePreCheck(ctx context.Context) error {
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	err := s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			created, err := versions.IsFrameworkTablesCreated(txn)
			if err != nil {
				getUpgradeLogger().Error("failed to check upgrade framework",
					zap.Error(err))
				return err
			}
			if !created {
				return nil
			}

			unReady, err := checkUpgradeEnvUnReady(txn)
			if err != nil {
				getUpgradeLogger().Error("failed to check task status in pgrade environment", zap.Error(err))
				return err
			}

			if unReady {
				getUpgradeLogger().Info("There are unexecuted tenant upgrade tasks in upgrade environment, start asynchronous supplementary execution")
				s.adjustUpgrade()
				if err := s.stopper.RunTask(s.asyncUpgradeTask); err != nil {
					return err
				}
				for i := 0; i < s.upgrade.upgradeTenantTasks; i++ {
					if err := s.stopper.RunTask(s.asyncUpgradeTenantTask); err != nil {
						return err
					}
				}
				return moerr.NewInternalError(ctx, "There is an untrigged upgrade tasks in the system, execution started, Please try again later")
			}
			return nil
		},
		opts)
	return err
}

// checkUpgradeEnvUnReady Check if the upgrade environment is ready
func checkUpgradeEnvUnReady(txn executor.TxnExecutor) (bool, error) {
	sql := fmt.Sprintf("select id, from_version, to_version, final_version, final_version_offset from %s.%s where state = 1",
		catalog.MO_CATALOG, catalog.MOUpgradeTable)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return false, err
	}

	var loaded bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		loaded = true
		return false
	})
	return loaded, nil
}
