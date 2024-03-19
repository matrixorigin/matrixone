// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	defaultUpgradeTenantBatch         = 256
	defaultCheckUpgradeDuration       = time.Second * 5
	defaultCheckUpgradeTenantDuration = time.Second * 10
	defaultUpgradeTenantTasks         = 4
)

func (s *service) BootstrapUpgrade(ctx context.Context) error {
	getUpgradeLogger().Info("start bootstrap upgrade")
	s.adjustUpgrade()
	// MO's upgrade framework is automated, requiring no manual execution of any
	// upgrade commands, and supports cross-version upgrades. All upgrade processes
	// are executed at the CN node. Currently, rollback upgrade is not supported.
	//
	// When a new version of the CN node is started, it will first get the current
	// version of the cluster running, and determine the upgrade route before this
	// version and the current new version of the CN. When upgrading across versions,
	// this upgrade route will go through multiple versions of upgrades, and finally
	// upgrade to the current version of the CN.
	//
	// Each version upgrade, for the previous version, contains 2 parts, one is to
	// upgrade the cluster metadata and the other is to upgrade the tenant metadata.
	//
	// For upgrading cluster metadata, it is usually very fast, usually it is creating
	// some new metadata tables or updating the structure of some metadata tables, and
	// this process is performed on one CN.
	//
	// For upgrading tenant metadata, the time consuming upgrade depends on the number
	// of tenants, and since MO is a meta-native multi-tenant database, our default
	// number of tenants is huge. So the whole tenant upgrade is asynchronous and will
	// be grouped for all tenants and concurrently executed on multiple CNs at the same
	// time.
	if err := retryRun(ctx, "doCheckUpgrade", s.doCheckUpgrade); err != nil {
		getUpgradeLogger().Error("check upgrade failed", zap.Error(err))
		return err
	}
	if err := s.stopper.RunTask(s.asyncUpgradeTask); err != nil {
		return err
	}
	for i := 0; i < s.upgrade.upgradeTenantTasks; i++ {
		if err := s.stopper.RunTask(s.asyncUpgradeTenantTask); err != nil {
			return err
		}
	}
	return nil
}

// doCheckUpgrade get the current version of the cluster running, and determine the upgrade
// route before this version and the current new version of the CN.
//
// Note that this logic will execute concurrently if more than one CN starts at the same
// time, but it doesn't matter, we use select for update to make it so that only one CN can
// create the upgrade step.
func (s *service) doCheckUpgrade(ctx context.Context) error {
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			final := s.getFinalVersionHandle().Metadata()

			// First version as a genesis version, always need to be PREPARE.
			// Because the first version need to init upgrade framework tables.
			if len(s.handles) == 1 {
				getUpgradeLogger().Info("init upgrade framework",
					zap.String("final-version", final.Version))
				return s.handles[0].Prepare(ctx, txn, true)
			}

			// Deploy mo first time without 1.2.0, init framework first.
			// And upgrade to current version.
			created, err := versions.IsFrameworkTablesCreated(txn)
			if err != nil {
				getUpgradeLogger().Error("failed to check upgrade framework",
					zap.Error(err))
				return err
			}
			if !created {
				getUpgradeLogger().Info("init upgrade framework",
					zap.String("final-version", final.Version))

				if err := s.handles[0].Prepare(ctx, txn, false); err != nil {
					getUpgradeLogger().Error("failed to init upgrade framework",
						zap.Error(err))
					return err
				}

				res, err := txn.Exec(final.GetInsertSQL(versions.StateReady), executor.StatementOption{})
				if err == nil {
					res.Close()
				}
				return err
			}

			// lock version table
			if err := txn.LockTable(catalog.MOVersionTable); err != nil {
				getUpgradeLogger().Error("failed to lock table",
					zap.String("table", catalog.MOVersionTable),
					zap.Error(err))
				return err
			}

			v, err := versions.GetLatestVersion(txn)
			if err != nil {
				getUpgradeLogger().Error("failed to get latest version",
					zap.Error(err))
				return err
			}

			getUpgradeLogger().Info("get current mo cluster latest version",
				zap.String("latest", v.Version),
				zap.String("final", final.Version))

			// cluster is upgrading to v1, only v1's cn can start up.
			if !v.IsReady() && v.Version != final.Version {
				panic(fmt.Sprintf("cannot upgrade to version %s, because version %s is in upgrading",
					final.Version,
					v.Version))
			}
			// cluster is running at v1, cannot startup a old version to join cluster.
			if v.IsReady() && versions.Compare(final.Version, v.Version) < 0 {
				panic(fmt.Sprintf("cannot startup a old version %s to join cluster, current version is %s",
					final.Version,
					v.Version))
			}

			// check upgrade has 2 step:
			// 1: already checked, version exists
			// 2: add upgrades from latest version to final version
			checker := func() (bool, error) {
				if v.Version == final.Version && v.VersionOffset >= final.VersionOffset {
					return true, nil
				}

				state, ok, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, false)
				if err == nil && ok && state == versions.StateReady {
					s.upgrade.finalVersionCompleted.Store(true)
				}
				if err != nil {
					getUpgradeLogger().Error("failed to get final version state",
						zap.String("final", final.Version),
						zap.Error(err))
				}
				return ok, err
			}

			addUpgradesToFinalVersion := func() error {
				if err := versions.AddVersion(final.Version, final.VersionOffset, versions.StateCreated, txn); err != nil {
					getUpgradeLogger().Error("failed to add final version",
						zap.String("final", final.Version),
						zap.Error(err))
					return err
				}

				getUpgradeLogger().Error("final version added",
					zap.String("final", final.Version))

				latest, err := versions.MustGetLatestReadyVersion(txn)
				if err != nil {
					getUpgradeLogger().Error("failed to get latest ready version",
						zap.String("latest", latest),
						zap.Error(err))
					return err
				}

				getUpgradeLogger().Info("current latest ready version loaded",
					zap.String("latest", latest),
					zap.String("final", final.Version),
					zap.Int32("versionOffset", int32(final.VersionOffset)))

				var upgrades []versions.VersionUpgrade
				from := latest
				append := func(v versions.Version) {
					order := int32(len(upgrades))
					u := versions.VersionUpgrade{
						FromVersion:        from,
						ToVersion:          v.Version,
						FinalVersion:       final.Version,
						FinalVersionOffset: final.VersionOffset,
						State:              versions.StateCreated,
						UpgradeOrder:       order,
						UpgradeCluster:     v.UpgradeCluster,
						UpgradeTenant:      v.UpgradeTenant,
					}
					upgrades = append(upgrades, u)

					getUpgradeLogger().Info("version upgrade added",
						zap.String("upgrade", u.String()),
						zap.String("final", final.Version))
				}

				// can upgrade to final version directly.
				if final.CanDirectUpgrade(latest) {
					append(final)
				} else {
					for _, v := range s.handles {
						if versions.Compare(v.Metadata().Version, from) > 0 &&
							v.Metadata().CanDirectUpgrade(from) {
							append(v.Metadata())
							from = v.Metadata().Version
						}
					}
				}
				return versions.AddVersionUpgrades(upgrades, txn)
			}

			// step 1
			if versionAdded, err := checker(); err != nil || versionAdded {
				return err
			}

			// step 2
			return addUpgradesToFinalVersion()
		},
		opts)
}

// asyncUpgradeTask is a task that executes the upgrade logic step by step
// according to the created upgrade steps
func (s *service) asyncUpgradeTask(ctx context.Context) {
	fn := func() (bool, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Hour*24)
		defer cancel()

		var err error
		var completed bool
		opts := executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied().
			WithTimeZone(time.Local)
		err = s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				completed, err = s.performUpgrade(ctx, txn)
				return err
			},
			opts)
		return completed, err
	}

	timer := time.NewTimer(s.upgrade.checkUpgradeDuration)
	defer timer.Stop()

	defer func() {
		getUpgradeLogger().Info("upgrade task exit",
			zap.String("final", s.getFinalVersionHandle().Metadata().Version))
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if s.upgrade.finalVersionCompleted.Load() {
				return
			}

			completed, err := fn()
			if err == nil && completed {
				s.upgrade.finalVersionCompleted.Store(true)
				return
			}
			timer.Reset(s.upgrade.checkUpgradeDuration)
		}
	}
}

func (s *service) performUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) (bool, error) {
	final := s.getFinalVersionHandle().Metadata()

	// make sure only one cn can execute upgrade logic
	state, ok, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, true)
	if err != nil {
		getUpgradeLogger().Error("failed to load final version state",
			zap.String("final", final.Version),
			zap.Int32("versionOffset", int32(final.VersionOffset)),
			zap.Error(err))
		return false, err
	}
	if !ok {
		getUpgradeLogger().Info("final version not found, retry later",
			zap.String("final", final.Version),
			zap.Int32("versionOffset", int32(final.VersionOffset)))
		return false, nil
	}

	getUpgradeLogger().Info("final version state loaded",
		zap.String("final", final.Version),
		zap.Int32("versionOffset", int32(final.VersionOffset)),
		zap.Int32("state", state))

	if state == versions.StateReady {
		return true, nil
	}

	// get upgrade steps, and perform upgrade one by one
	upgrades, err := versions.GetUpgradeVersions(final.Version, final.VersionOffset, txn, true, true)
	if err != nil {
		getUpgradeLogger().Error("failed to load upgrades",
			zap.String("final", final.Version),
			zap.Error(err))
		return false, err
	}

	for _, u := range upgrades {
		getUpgradeLogger().Info("handle version upgrade",
			zap.String("upgrade", u.String()))

		state, err := s.doUpgrade(ctx, u, txn)
		if err != nil {
			getUpgradeLogger().Error("failed to handle version upgrade",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version),
				zap.Error(err))
			return false, err
		}

		switch state {
		case versions.StateReady:
			// upgrade was completed
			getUpgradeLogger().Info("upgrade version completed",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version))
		case versions.StateUpgradingTenant:
			// we must wait all tenant upgrade completed, and then upgrade to
			// next version
			getUpgradeLogger().Info("upgrade version in tenant upgrading",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version))
			return false, nil
		default:
			panic(fmt.Sprintf("BUG: invalid state %d", state))
		}
	}

	// all upgrades completed, update final version to ready state.
	if err := versions.UpdateVersionState(final.Version, final.VersionOffset, versions.StateReady, txn); err != nil {
		getUpgradeLogger().Error("failed to update state",
			zap.String("final", final.Version),
			zap.Error(err))

		return false, err
	}

	getUpgradeLogger().Info("upgrade to final version completed",
		zap.String("final", final.Version))
	return true, nil
}

// doUpgrade Corresponding to one upgrade step in a version upgrade
func (s *service) doUpgrade(
	ctx context.Context,
	upgrade versions.VersionUpgrade,
	txn executor.TxnExecutor) (int32, error) {
	if upgrade.State == versions.StateReady {
		return upgrade.State, nil
	}

	if (upgrade.UpgradeCluster == versions.No && upgrade.UpgradeTenant == versions.No) ||
		(upgrade.State == versions.StateUpgradingTenant && upgrade.TotalTenant == upgrade.ReadyTenant) {
		if err := versions.UpdateVersionUpgradeState(upgrade, versions.StateReady, txn); err != nil {
			return 0, err
		}
		return versions.StateReady, nil
	}

	if upgrade.State == versions.StateUpgradingTenant {
		return upgrade.State, nil
	}

	state := versions.StateReady
	h := s.getVersionHandle(upgrade.ToVersion)

	getUpgradeLogger().Info("execute upgrade prepare",
		zap.String("upgrade", upgrade.String()))
	if err := h.Prepare(ctx, txn, h.Metadata().Version == s.getFinalVersionHandle().Metadata().Version); err != nil {
		return 0, err
	}
	getUpgradeLogger().Info("execute upgrade prepare completed",
		zap.String("upgrade", upgrade.String()))

	if upgrade.UpgradeCluster == versions.Yes {
		getUpgradeLogger().Info("execute upgrade cluster",
			zap.String("upgrade", upgrade.String()))
		if err := h.HandleClusterUpgrade(ctx, txn); err != nil {
			return 0, err
		}
		getUpgradeLogger().Info("execute upgrade cluster completed",
			zap.String("upgrade", upgrade.String()))
	}

	if upgrade.UpgradeTenant == versions.Yes {
		state = versions.StateUpgradingTenant
		err := fetchTenants(
			s.upgrade.upgradeTenantBatch,
			func(ids []int32) error {
				upgrade.TotalTenant += int32(len(ids))
				getUpgradeLogger().Info("add tenants to upgrade",
					zap.String("upgrade", upgrade.String()),
					zap.Int32("from", ids[0]),
					zap.Int32("to", ids[len(ids)-1]))
				return versions.AddUpgradeTenantTask(upgrade.ID, upgrade.ToVersion, ids[0], ids[len(ids)-1], txn)
			},
			txn)
		if err != nil {
			return 0, err
		}
		if err := versions.UpdateVersionUpgradeTasks(upgrade, txn); err != nil {
			return 0, err
		}
		getUpgradeLogger().Info("upgrade tenants task updated",
			zap.String("upgrade", upgrade.String()))
		if upgrade.TotalTenant == upgrade.ReadyTenant {
			state = versions.StateReady
		}
	}

	getUpgradeLogger().Info("upgrade update state",
		zap.String("upgrade", upgrade.String()),
		zap.Int32("state", state))
	return state, versions.UpdateVersionUpgradeState(upgrade, state, txn)
}

func retryRun(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) error) error {
	wait := time.Second
	maxWait := time.Second * 10
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		getUpgradeLogger().Error("execute task failed, retry later",
			zap.String("task", name),
			zap.Duration("wait", wait),
			zap.Error(err))
		time.Sleep(wait)
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (s *service) adjustUpgrade() {
	if s.upgrade.upgradeTenantBatch == 0 {
		s.upgrade.upgradeTenantBatch = defaultUpgradeTenantBatch
	}
	if s.upgrade.checkUpgradeDuration == 0 {
		s.upgrade.checkUpgradeDuration = defaultCheckUpgradeDuration
	}
	if s.upgrade.checkUpgradeTenantDuration == 0 {
		s.upgrade.checkUpgradeTenantDuration = defaultCheckUpgradeTenantDuration
	}
	if s.upgrade.upgradeTenantTasks == 0 {
		s.upgrade.upgradeTenantTasks = defaultUpgradeTenantTasks
	}
	getUpgradeLogger().Info("upgrade config",
		zap.Duration("check-upgrade-duration", s.upgrade.checkUpgradeDuration),
		zap.Duration("check-upgrade-tenant-duration", s.upgrade.checkUpgradeTenantDuration),
		zap.Int("upgrade-tenant-tasks", s.upgrade.upgradeTenantTasks),
		zap.Int("tenant-batch", s.upgrade.upgradeTenantBatch))
}
