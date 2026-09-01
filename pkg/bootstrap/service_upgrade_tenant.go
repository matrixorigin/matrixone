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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// MaybeUpgradeTenant checks and upgrades tenant metadata on demand. A required
// upgrade is rejected while a caller-owned transaction is active; the caller
// must end that transaction and retry so migration pages can commit safely.
func (s *service) MaybeUpgradeTenant(
	ctx context.Context,
	tenantFetchFunc func() (int32, string, error),
	txnOp client.TxnOperator) (bool, error) {
	tenantID, _, err := tenantFetchFunc()
	if err != nil {
		return false, err
	}

	s.mu.RLock()
	checked := s.mu.tenants[tenantID]
	s.mu.RUnlock()
	if checked {
		return false, nil
	}

	currentCN := s.getFinalVersionHandle().Metadata()
	persistedVersion := ""
	tenantExists := true
	shouldUpgrade := false
	err = s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)
		persistedVersion, tenantExists, err = versions.GetTenantVersionIfExists(tenantID, txn)
		if err != nil || !tenantExists {
			return err
		}
		if versions.Compare(currentCN.Version, persistedVersion) < 0 {
			return moerr.NewInvalidInputNoCtxf(
				"tenant version %s is greater than current cn version %s",
				persistedVersion, currentCN.Version)
		}
		shouldUpgrade = versions.Compare(persistedVersion, currentCN.Version) < 0
		latestVersion, err := versions.GetLatestVersion(txn)
		if err != nil {
			return err
		}
		if latestVersion.Version != currentCN.Version {
			s.logger.Fatal("BUG: current cn's version(" + currentCN.Version +
				") must equal cluster latest version(" + latestVersion.Version + ")")
		}
		if persistedVersion == currentCN.Version {
			if conditional, ok := s.getFinalVersionHandle().(conditionalTenantUpgrade); ok {
				shouldUpgrade, err = conditional.TenantUpgradeRequired(tenantID, txn)
				if err != nil {
					return err
				}
			}
			if !shouldUpgrade {
				upgrades, err := versions.GetUpgradeVersions(
					latestVersion.Version, latestVersion.VersionOffset, txn, false, false,
				)
				if err != nil {
					return err
				}
				shouldUpgrade = len(upgrades) > 0
			}
		}
		return nil
	}, executor.Options{}.WithTxn(txnOp))
	if err != nil {
		return false, err
	}
	if !tenantExists {
		return false, nil
	}
	if !shouldUpgrade {
		// A caller-owned transaction may still roll back the state observed above.
		// Cache only committed, independently observed readiness.
		if txnOp == nil {
			s.markTenantUpgradeChecked(tenantID)
		}
		return false, nil
	}
	if txnOp != nil {
		return false, moerr.NewInvalidStateNoCtx(
			"tenant upgrade requires retry without a caller-owned transaction")
	}
	if err := s.waitTenantUpgradeReady(ctx); err != nil {
		return false, err
	}
	tenantExists, err = s.upgradeTenantDirectly(
		ctx, tenantID, persistedVersion == currentCN.Version,
	)
	if err != nil || !tenantExists {
		return false, err
	}
	s.markTenantUpgradeChecked(tenantID)
	return true, nil
}

func (s *service) markTenantUpgradeChecked(tenantID int32) {
	s.mu.Lock()
	s.mu.tenants[tenantID] = true
	s.mu.Unlock()
}

// incrementalTenantUpgrade is implemented by migrations that must commit
// bounded pages before a tenant task can be marked complete.
type conditionalTenantUpgrade interface {
	TenantUpgradeRequired(tenantID int32, txn executor.TxnExecutor) (bool, error)
}

type incrementalTenantUpgrade interface {
	HandleTenantUpgradeStep(
		ctx context.Context,
		tenantID int32,
		txn executor.TxnExecutor,
	) (completed bool, err error)
}

func (s *service) waitTenantUpgradeReady(ctx context.Context) error {
	for !s.upgrade.finalVersionCompleted.Load() {
		ready := false
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied().
			WithTimeZone(time.Local)
		err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			latestVersion, err := versions.GetLatestVersion(txn)
			if err != nil {
				return err
			}
			state, ok, err := versions.GetVersionState(
				latestVersion.Version, latestVersion.VersionOffset, txn, false,
			)
			if err != nil {
				return err
			}
			if ok && state == versions.StateReady {
				ready = true
				return nil
			}
			upgrades, err := versions.GetUpgradeVersions(
				latestVersion.Version, latestVersion.VersionOffset, txn, false, true,
			)
			if err != nil {
				return err
			}
			ready = len(upgrades) == 0 ||
				upgrades[len(upgrades)-1].State == versions.StateUpgradingTenant ||
				upgrades[len(upgrades)-1].State == versions.StateReady
			return nil
		}, opts)
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}

// upgradeTenantDirectly drives one eligible handler at a time. Every incomplete
// incremental step commits before retrying the same handler; tenant version is
// published only with the completed step.
func (s *service) upgradeTenantDirectly(
	ctx context.Context,
	tenantID int32,
	includeEqual bool,
) (bool, error) {
	for {
		completed := true
		done := false
		tenantExists := true
		selectedEqual := false
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied().
			WithTimeZone(time.Local)
		err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			current, exists, err := versions.GetTenantCreateVersionForUpdateIfExists(tenantID, txn)
			if err != nil {
				return err
			}
			if !exists {
				tenantExists = false
				done = true
				return nil
			}

			var selected VersionHandle
			for _, h := range s.handles {
				compare := versions.Compare(h.Metadata().Version, current)
				if compare < 0 || (compare == 0 && !includeEqual) ||
					!h.Metadata().CanDirectUpgrade(current) {
					continue
				}
				selected = h
				selectedEqual = compare == 0
				break
			}
			if selected == nil {
				if versions.Compare(current, s.getFinalVersionHandle().Metadata().Version) < 0 {
					return moerr.NewInvalidStateNoCtxf(
						"no direct tenant upgrade path from %s", current)
				}
				done = true
				return nil
			}

			if incremental, ok := selected.(incrementalTenantUpgrade); ok {
				completed, err = incremental.HandleTenantUpgradeStep(ctx, tenantID, txn)
				if err != nil || !completed {
					return err
				}
			} else if err := selected.HandleTenantUpgrade(ctx, tenantID, txn); err != nil {
				return err
			}
			return versions.UpgradeTenantVersion(tenantID, selected.Metadata().Version, txn)
		}, opts)
		if err != nil {
			return false, err
		}
		if done {
			return tenantExists, nil
		}
		if completed && selectedEqual {
			includeEqual = false
		}
	}
}

// shouldRunTenantUpgrade keeps the normal version-transition behavior (a tenant
// already at the target version is skipped), but reruns an offset-only upgrade.
// mo_account.create_version does not store a version offset, so FromVersion ==
// ToVersion is the signal that an equal-version tenant still needs this step.
func shouldRunTenantUpgrade(createVersion string, upgrade versions.VersionUpgrade) bool {
	tenantVersionCompare := versions.Compare(createVersion, upgrade.ToVersion)
	if tenantVersionCompare < 0 {
		return true
	}
	return tenantVersionCompare == 0 &&
		versions.Compare(upgrade.FromVersion, upgrade.ToVersion) == 0
}

// asyncUpgradeTenantTask is a task to execute the tenant upgrade logic in
// parallel based on the grouped tenant batch.
func (s *service) asyncUpgradeTenantTask(ctx context.Context) {
	fn := func() (bool, error) {
		ctx, cancel := context.WithTimeoutCause(ctx, time.Hour*24, moerr.CauseAsyncUpgradeTenantTask)
		defer cancel()

		hasUpgradeTenants := false
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied().
			WithTimeZone(time.Local)
		err := s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				upgrade, ok, err := versions.GetUpgradingTenantVersion(txn)
				if err != nil {
					s.logger.Error("failed to get upgrading tenant version",
						zap.Error(err))
					return err
				}

				s.logger.Debug("get upgrading tenant version",
					zap.String("upgrade", upgrade.String()),
					zap.Bool("has", ok))
				if !ok || upgrade.TotalTenant == upgrade.ReadyTenant {
					return nil
				}

				// no upgrade logic on current cn, skip
				v := s.getFinalVersionHandle().Metadata().Version
				if versions.Compare(upgrade.ToVersion, v) > 0 {
					s.logger.Info("skip upgrade tenant",
						zap.String("final", v),
						zap.String("to", upgrade.ToVersion))
					return nil
				}

				// select task and tenants for update
				taskID, tenants, createVersions, hasDeletedTenantTasks, hasConflictTenantTasks, err := versions.GetUpgradeTenantTasks(upgrade.ID, txn)
				if err != nil {
					s.logger.Error("failed to load upgrade tenants",
						zap.String("upgrade", upgrade.String()),
						zap.Error(err))
					return err
				}

				s.logger.Debug("load upgrade tenants",
					zap.Int("count", len(tenants)),
					zap.String("upgrade", upgrade.String()))
				if len(tenants) == 0 {
					hasUnreadyTasks, err := versions.HasUnreadyUpgradeTenantTasks(upgrade.ID, txn)
					if err != nil {
						s.logger.Error("failed to check remaining tenant upgrade tasks",
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}
					if hasConflictTenantTasks {
						return nil
					}

					reconciledDeletedTaskCount := int64(0)
					if hasUnreadyTasks {
						reconciledDeletedTaskCount, err = versions.ReconcileDeletedUpgradeTenantTasks(upgrade.ID, txn)
						if err != nil {
							s.logger.Error("failed to reconcile deleted tenant upgrade tasks",
								zap.String("upgrade", upgrade.String()),
								zap.Error(err))
							return err
						}
						hasUnreadyTasks, err = versions.HasUnreadyUpgradeTenantTasks(upgrade.ID, txn)
						if err != nil {
							s.logger.Error("failed to recheck remaining tenant upgrade tasks",
								zap.String("upgrade", upgrade.String()),
								zap.Error(err))
							return err
						}
						if hasUnreadyTasks {
							return nil
						}
					}

					reloadedUpgrade, err := versions.GetUpgradeVersionForUpdateByID(upgrade.ID, txn)
					if err != nil {
						s.logger.Error("failed to reload upgrade after tenant task reconciliation",
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}
					if reloadedUpgrade.ReadyTenant >= reloadedUpgrade.TotalTenant {
						return nil
					}
					reloadedUpgrade.ReadyTenant = reloadedUpgrade.TotalTenant
					fields := []zap.Field{
						zap.String("upgrade", reloadedUpgrade.String()),
						zap.Int64("reconciled-deleted-task-count", reconciledDeletedTaskCount),
						zap.Bool("deleted-tenant-tasks-detected", hasDeletedTenantTasks),
					}
					if reconciledDeletedTaskCount > 0 {
						s.logger.Warn("tenant upgrade task counters reconciled to current tenants", fields...)
					} else {
						s.logger.Info("tenant upgrade task counters reconciled to current tenants", fields...)
					}
					return versions.UpdateVersionUpgradeTasks(reloadedUpgrade, txn)
				}

				hasUpgradeTenants = true
				h := s.getVersionHandle(upgrade.ToVersion)
				if incremental, ok := h.(incrementalTenantUpgrade); ok {
					return s.upgradeTenantIncrementally(
						ctx, upgrade, taskID, tenants[0], createVersions[0], incremental, txn,
					)
				}
				updated := int32(0)
				for i, id := range tenants {
					createVersion := createVersions[i]

					s.logger.Info("upgrade tenant",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))

					if !shouldRunTenantUpgrade(createVersion, upgrade) {
						continue
					}

					s.logger.Info("execute upgrade tenant",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))

					if err = h.HandleTenantUpgrade(ctx, id, txn); err != nil {
						s.logger.Error("failed to execute upgrade tenant",
							zap.Int32("tenant", id),
							zap.String("tenant-version", createVersion),
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}

					if err = versions.UpgradeTenantVersion(id, h.Metadata().Version, txn); err != nil {
						s.logger.Error("failed to update upgrade tenant create version",
							zap.Int32("tenant", id),
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
					}

					s.logger.Info("execute upgrade tenant completed",
						zap.Int32("tenant", id),
						zap.String("tenant-version", createVersion),
						zap.String("upgrade", upgrade.String()))
					updated++
				}

				if err = versions.UpdateUpgradeTenantTaskState(taskID, versions.Yes, txn); err != nil {
					s.logger.Error("failed to update upgrade tenant state",
						zap.String("upgrade", upgrade.String()))
					return err
				}
				s.logger.Info("tenant state updated",
					zap.Int32("from", tenants[0]),
					zap.Int32("to", tenants[len(tenants)-1]),
					zap.String("upgrade", upgrade.String()))

				// update count, we need using select for update to avoid concurrent update
				upgrade, err = versions.GetUpgradeVersionForUpdateByID(upgrade.ID, txn)
				if err != nil {
					s.logger.Error("failed to get latest upgrade info",
						zap.String("upgrade", upgrade.String()))
					return err
				}

				upgrade.ReadyTenant += updated
				if upgrade.TotalTenant < upgrade.ReadyTenant {
					s.logger.Error("invalid upgrade tenant",
						zap.String("upgrade", upgrade.String()),
						zap.Int32("updated", updated),
					)
					return moerr.NewInvalidStateNoCtx("orphan txn or pre lock released by lock table changed")
				}

				s.logger.Info("upgrade tenant ready count changed",
					zap.String("upgrade", upgrade.String()))

				if upgrade.State == versions.StateReady {
					return nil
				}
				return versions.UpdateVersionUpgradeTasks(upgrade, txn)
			},
			opts)
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			s.logger.Error("tenant task handle failed",
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

			drainUpgradeTenants(ctx, fn)
			timer.Reset(s.upgrade.checkUpgradeTenantDuration)
		}
	}
}

func (s *service) upgradeTenantIncrementally(
	ctx context.Context,
	upgrade versions.VersionUpgrade,
	taskID uint64,
	tenantID int32,
	createVersion string,
	h incrementalTenantUpgrade,
	txn executor.TxnExecutor,
) error {
	if shouldRunTenantUpgrade(createVersion, upgrade) {
		completed, err := h.HandleTenantUpgradeStep(ctx, tenantID, txn)
		if err != nil {
			s.logger.Error("failed to execute incremental tenant upgrade",
				zap.Int32("tenant", tenantID), zap.String("upgrade", upgrade.String()), zap.Error(err))
			return err
		}
		if !completed {
			s.logger.Info("incremental tenant upgrade page ready to commit",
				zap.Int32("tenant", tenantID), zap.String("upgrade", upgrade.String()))
			return nil
		}
		if err := versions.UpgradeTenantVersion(tenantID, upgrade.ToVersion, txn); err != nil {
			return err
		}
	}

	advanced, err := versions.AdvanceUpgradeTenantTask(taskID, tenantID, txn)
	if err != nil || !advanced {
		return err
	}
	current, err := versions.GetUpgradeVersionForUpdateByID(upgrade.ID, txn)
	if err != nil {
		return err
	}
	current.ReadyTenant++
	if current.ReadyTenant > current.TotalTenant {
		return moerr.NewInvalidStateNoCtx("incremental tenant upgrade ready count exceeds total")
	}
	return versions.UpdateVersionUpgradeTasks(current, txn)
}

func drainUpgradeTenants(ctx context.Context, fn func() (bool, error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		hasUpgradeTenants, err := fn()
		if err != nil || !hasUpgradeTenants {
			return
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
				last = vector.GetFixedAtWithTypeCheck[int32](cols[0], i)
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
