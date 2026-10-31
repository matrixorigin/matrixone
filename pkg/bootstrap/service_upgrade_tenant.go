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
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// MaybeUpgradeTenant used to check the tenant need upgrade or not. If need upgrade, it will
// upgrade the tenant immediately in current txn. The callback's version is only
// a hint: mo_account.create_version is authoritative, including for accounts an
// old CN creates after the background upgrade's account-range snapshot.
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

	upgraded := false
	opts := executor.Options{}.WithTxn(txnOp).
		WithAccountID(catalog.System_Account).
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	// Release the planning transaction and account lock before the paged
	// 4.0.7 walk, then recheck the committed account version before caching it.
	for {
		pendingV407 := false
		err = s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				txn.Use(catalog.MO_CATALOG)
				version, err := versions.GetTenantVersion(tenantID, txn)
				if err != nil {
					return err
				}
				// Check the persisted account version, not the serving CN's version.
				currentCN := s.getFinalVersionHandle().Metadata()
				if currentCN.Version == version {
					return nil
				} else if versions.Compare(currentCN.Version, version) < 0 {
					// tenant create at 1.4.0, current tenant version 1.5.0, it must be cannot work
					return moerr.NewInvalidInputNoCtxf("tenant version %s is greater than current cn version %s",
						version, currentCN.Version)
				}

				// arrive here means tenant version < current cn version, need upgrade.
				// and currentCN.Version == last cluster version

				latestVersion, err := versions.GetLatestVersion(txn)
				if err != nil {
					return err
				}
				if latestVersion.Version != currentCN.Version {
					return moerr.NewInvalidStateNoCtxf(
						"tenant upgrade requires current cn version %s to match cluster latest version %s",
						currentCN.Version, latestVersion.Version)
				}

				for {
					// A restarted CN may not have observed completion locally. The
					// persisted ready state also permits late-account compensation.
					if latestVersion.IsReady() || s.upgrade.finalVersionCompleted.Load() {
						break
					}

					upgrades, err := versions.GetUpgradeVersions(latestVersion.Version, latestVersion.VersionOffset, txn, false, true)
					if err != nil {
						return err
					}
					if len(upgrades) == 0 {
						return moerr.NewInvalidStateNoCtx("tenant upgrade route is unavailable")
					}
					// latest cluster is already upgrade completed
					if upgrades[len(upgrades)-1].State == versions.StateUpgradingTenant ||
						upgrades[len(upgrades)-1].State == versions.StateReady {
						break
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Second):
					}
				}

				// upgrade in current goroutine immediately
				version, err = versions.GetTenantCreateVersionForUpdate(tenantID, txn)
				if err != nil {
					return err
				}
				if versions.Compare(version, currentCN.Version) > 0 {
					return moerr.NewInvalidInputNoCtxf("tenant version %s is greater than current cn version %s",
						version, currentCN.Version)
				}
				from := version
				for _, v := range s.handles {
					if versions.Compare(v.Metadata().Version, from) > 0 &&
						v.Metadata().CanDirectUpgrade(from) {
						if v == v4_0_7.Handler {
							if txnOp == nil {
								pendingV407 = true
								break // Release the planning transaction/account lock before paging.
							}
							_, err := v4_0_7.Handler.UpgradeTenant(ctx, tenantID, s.upgrade.catalogObjectLimit,
								func(operation string, limit uint64, adjustable bool, work func(executor.TxnExecutor) error) (bool, error) {
									err := work(txn)
									if err != nil {
										err = catalogOperationError(err, fmt.Sprintf("tenant %d", tenantID), operation, limit, adjustable)
									}
									return true, err
								})
							if err != nil {
								return err
							}
						} else if err := v.HandleTenantUpgrade(ctx, tenantID, txn); err != nil {
							return err
						}
						if err := versions.UpgradeTenantVersion(tenantID, v.Metadata().Version, txn); err != nil {
							return err
						}
						from = v.Metadata().Version
						upgraded = true
					}
				}
				if !pendingV407 && from != currentCN.Version {
					return moerr.NewInvalidInputNoCtxf("cannot upgrade tenant version %s to current cn version %s",
						from, currentCN.Version)
				}
				return nil
			},
			opts)
		if err != nil {
			return false, err
		}
		if !pendingV407 {
			break
		}
		changed, err := s.upgradeV407TenantOnDemand(ctx, tenantID)
		if err != nil {
			return false, err
		}
		upgraded = upgraded || changed
	}
	// A caller-owned transaction can still roll back; only cache committed checks.
	if txnOp == nil {
		s.mu.Lock()
		s.mu.tenants[tenantID] = true
		s.mu.Unlock()
	}
	return upgraded, nil
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

// newTenantUpgradePass returns one transactional tenant-upgrade pass. Keeping
// the pass independent from the periodic task makes completion observable
// without coupling callers (or tests) to the task's wall clock.
func (s *service) newTenantUpgradePass(ctx context.Context) func() (bool, error) {
	// This cursor is only a contention hint. Durable progress remains in the
	// upgrade tables, and a complete scan always starts again at zero.
	var cursorUpgradeID uint64
	var cursor int32
	return func() (bool, error) {
		passCtx := ctx
		if !s.upgrade.finalVersionCompleted.Load() {
			passCtx = executor.WithLocalExecution(passCtx)
		}
		operationCtx, cancel := context.WithTimeoutCause(passCtx, time.Hour*24, moerr.CauseAsyncUpgradeTenantTask)

		hasUpgradeTenants := false
		resetCursor := false
		var selected *tenantUpgradeSelection
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied().
			WithTimeZone(time.Local)
		err := s.exec.ExecTxn(
			operationCtx,
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
				if !ok {
					cursorUpgradeID, cursor = 0, 0
					return nil
				}
				if cursorUpgradeID != upgrade.ID {
					cursorUpgradeID, cursor = upgrade.ID, 0
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
				taskID, tenants, createVersions, hasDeletedTenantTasks, err := versions.GetUpgradeTenantTasks(upgrade.ID, cursor, txn)
				if err != nil {
					return err
				}

				s.logger.Debug("load upgrade tenants",
					zap.Int("count", len(tenants)),
					zap.String("upgrade", upgrade.String()))
				if len(tenants) == 0 {
					if cursor != 0 {
						// This scan skipped a previously contended range. Start a
						// fresh full scan before reconciling counts or task states.
						resetCursor = true
						return nil
					}
					hasUnreadyTasks, err := versions.HasUnreadyUpgradeTenantTasks(upgrade.ID, txn)
					if err != nil {
						s.logger.Error("failed to check remaining tenant upgrade tasks",
							zap.String("upgrade", upgrade.String()),
							zap.Error(err))
						return err
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
				if h == v4_0_7.Handler {
					selected = &tenantUpgradeSelection{upgrade: upgrade, taskID: taskID, tenants: tenants}
					return nil
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

					if err = h.HandleTenantUpgrade(operationCtx, id, txn); err != nil {
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
			var conflict *versions.TenantTaskConflict
			if errors.As(err, &conflict) {
				cursor = conflict.NextAccountID
			}
			err = moerr.AttachCause(operationCtx, err)
			cancel()
			return false, err
		}
		cancel()
		if selected != nil {
			if err := s.upgradeV407TenantRange(passCtx, *selected); err != nil {
				return false, err
			}
		}
		if resetCursor {
			cursor = 0
		}
		if !hasUpgradeTenants && s.upgrade.finalVersionCompleted.Load() {
			maintenanceCtx, cancel := context.WithTimeoutCause(passCtx, time.Hour*24, moerr.CauseAsyncUpgradeTenantTask)
			defer cancel()
			if err := s.maintainInformationSchemaViews(maintenanceCtx); err != nil {
				return false, wrapUpgradeError(moerr.AttachCause(maintenanceCtx, err), "information schema views maintenance")
			}
			if err := s.maintainOrphanObjectPrivileges(maintenanceCtx); err != nil {
				return false, wrapUpgradeError(moerr.AttachCause(maintenanceCtx, err), "orphan object privilege maintenance")
			}
		}
		return hasUpgradeTenants, nil
	}
}

// asyncUpgradeTenantTask is a task to execute the tenant upgrade logic in
// parallel based on the grouped tenant batch.
func (s *service) asyncUpgradeTenantTask(ctx context.Context) {
	fn := s.newTenantUpgradePass(ctx)
	timer := time.NewTimer(s.upgrade.checkUpgradeTenantDuration)
	defer timer.Stop()

	maintenanceOwner := false
	defer func() {
		if maintenanceOwner {
			s.upgrade.orphanPrivilegeMaintenanceWorkerRunning.Store(false)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if s.upgrade.finalVersionCompleted.Load() && !maintenanceOwner {
				// Tenant upgrade workers used to exit at this point. Keep only one
				// of them as the process-local periodic maintenance owner; this also
				// prevents manual upgrade pre-checks from accumulating permanent
				// maintenance workers.
				if !s.upgrade.orphanPrivilegeMaintenanceWorkerRunning.CompareAndSwap(false, true) {
					return
				}
				maintenanceOwner = true
			}
			err := drainUpgradeTenants(ctx, fn)
			var terminal *terminalUpgradeError
			if errors.As(err, &terminal) && s.recordTerminalUpgradeFailure(ctx, err) {
				return
			}
			if err != nil && ctx.Err() == nil {
				s.logger.Error("tenant upgrade or catalog maintenance failed; will retry", zap.Error(err))
			}
			timer.Reset(s.upgrade.checkUpgradeTenantDuration)
		}
	}
}

func drainUpgradeTenants(ctx context.Context, fn func() (bool, error)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hasUpgradeTenants, err := fn()
		if err != nil || !hasUpgradeTenants {
			return err
		}
	}
}

func fetchTenantPage(after int32, txn executor.TxnExecutor) ([]int32, error) {
	const pageSize = 128
	sql := fmt.Sprintf("select account_id from mo_account where account_id > %d order by account_id limit %d", after, pageSize)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	ids := make([]int32, 0, pageSize)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			ids = append(ids, vector.GetFixedAtWithTypeCheck[int32](cols[0], i))
		}
		return true
	})
	return ids, nil
}
