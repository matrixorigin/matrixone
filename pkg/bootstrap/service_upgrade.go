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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	upgradeTenantBatch         = 256
	checkUpgradeDuration       = time.Second * 5
	checkUpgradeTenantDuration = time.Second * 10
	upgradeTenantTasks         = 4
	finalVersionCompleted      atomic.Bool
)

func (s *service) BootstrapUpgrade(ctx context.Context) error {
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
		return err
	}
	if err := s.stopper.RunTask(s.asyncUpgradeTask); err != nil {
		return err
	}
	for i := 0; i < upgradeTenantTasks; i++ {
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
		WithWaitCommittedLogApplied()
	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			// First version as a genesis version, always need to be PREPARE.
			// Because the first version need to init upgrade framework tables.
			if len(handles) == 1 {
				return handles[0].Prepare(ctx, txn)
			}

			// lock version table
			if err := txn.LockTable(catalog.MOVersionTable); err != nil {
				return nil
			}

			final := getFinalVersionHandle().Metadata()
			v, err := versions.GetLatestVersion(txn)
			if err != nil {
				return err
			}

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
				if v.Version == final.Version {
					return true, nil
				}

				state, ok, err := versions.GetVersionState(final.Version, txn, false)
				if err == nil && ok && state == versions.StateReady {
					finalVersionCompleted.Store(true)
				}
				return ok, err
			}

			addUpgradesToFinalVersion := func() error {
				if err := versions.AddVersion(final.Version, versions.StateCreated, txn); err != nil {
					return err
				}

				latest, err := versions.MustGetLatestReadyVersion(txn)
				if err != nil {
					return err
				}

				var upgrades []versions.VersionUpgrade
				append := func(v versions.Version) {
					upgrades = append(upgrades, versions.VersionUpgrade{
						FromVersion:    latest,
						ToVersion:      v.Version,
						FinalVersion:   v.Version,
						State:          versions.StateCreated,
						UpgradeOrder:   0,
						UpgradeCluster: v.UpgradeCluster,
						UpgradeTenant:  v.UpgradeTenant,
					})
				}

				// can upgrade to final version directly.
				if final.CanDirectUpgrade(latest) {
					append(final)
				} else {
					from := latest
					for _, v := range handles {
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
			WithWaitCommittedLogApplied()
		err = s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				completed, err = s.performUpgrade(ctx, txn)
				return err
			},
			opts)
		return completed, err
	}

	timer := time.NewTimer(checkUpgradeDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if finalVersionCompleted.Load() {
				return
			}

			completed, err := fn()
			if err == nil && completed {
				finalVersionCompleted.Store(true)
				return
			}
			timer.Reset(checkUpgradeDuration)
		}
	}
}

func (s *service) performUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) (bool, error) {
	final := getFinalVersionHandle().Metadata()

	// make sure only one cn can execute upgrade logic
	state, ok, err := versions.GetVersionState(final.Version, txn, true)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if state == versions.StateReady {
		return true, nil
	}

	// get upgrade steps, and perform upgrade one by one
	upgrades, err := versions.GetUpgradeVersions(final.Version, txn, true)
	if err != nil {
		return false, err
	}

	for _, u := range upgrades {
		state, err := s.doUpgrade(ctx, u, txn)
		if err != nil {
			return false, err
		}
		switch state {
		case versions.StateReady:
			// upgrade was completed
		case versions.StateUpgradingTenant:
			// we must wait all tenant upgrade completed, and then upgrade to
			// next version
			return false, nil
		default:
			panic(fmt.Sprintf("BUG: invalid state %d", state))
		}
	}

	// all upgrades completed, update final version to ready state.
	if err := versions.UpdateVersionState(final.Version, versions.StateReady, txn); err != nil {
		return false, err
	}
	return true, nil
}

func (s *service) doUpgrade(
	ctx context.Context,
	upgrade versions.VersionUpgrade,
	txn executor.TxnExecutor) (int32, error) {
	if upgrade.State == versions.StateReady ||
		upgrade.State == versions.StateUpgradingTenant {
		return upgrade.State, nil
	}

	if upgrade.UpgradeCluster == versions.No &&
		upgrade.UpgradeTenant == versions.No {
		if err := versions.UpdateVersionUpgradeState(upgrade, versions.StateReady, txn); err != nil {
			return 0, err
		}
		return versions.StateReady, nil
	}

	state := versions.StateReady
	h := getVersionHandle(upgrade.ToVersion)
	if err := h.Prepare(ctx, txn); err != nil {
		return 0, err
	}

	if upgrade.UpgradeCluster == versions.Yes {
		if err := h.HandleClusterUpgrade(ctx, txn); err != nil {
			return 0, err
		}
	}

	if upgrade.UpgradeTenant == versions.Yes {
		state = versions.StateUpgradingTenant
		err := fetchTenants(
			upgradeTenantBatch,
			func(ids []int32) error {
				upgrade.TotalTenant += int32(len(ids))
				return versions.AddUpgradeTenantTask(upgrade.ID, upgrade.ToVersion, ids[0], ids[len(ids)-1], txn)
			},
			txn)
		if err != nil {
			return 0, err
		}
		if err := versions.UpdateVersionUpgradeTasks(upgrade, txn); err != nil {
			return 0, err
		}
	}

	return state, versions.UpdateVersionUpgradeState(upgrade, state, txn)
}

func retryRun(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) error) error {
	wait := time.Second
	maxWait := time.Second
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		getLogger().Error("execute task failed, retry later",
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
