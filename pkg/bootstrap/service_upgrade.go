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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	upgradeTenantBatch         = 512
	checkUpgradeDuration       = time.Second * 5
	checkUpgradeTenantDuration = time.Second * 10
	upgradeTenantTasks         = 4
)

func (s *service) upgrade(ctx context.Context) error {
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

func (s *service) doCheckUpgrade(ctx context.Context) error {
	opts := executor.Options{}.
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			// first version as a genesis version, always need to be PREPARE.
			// Because the first version need to init upgrade framework tables.
			if len(handles) == 1 {
				return handles[0].Prepare(ctx, txn)
			}

			final := getFinalUpgradeHandle().Metadata()

			// check upgrade has 3 step:
			// 1: already checked, version exists
			// 2: add new final version
			// 3: add upgrades from latest version to final version
			checker := func() (bool, error) {
				_, ok, err := versions.GetVersionStateForUpdate(final.Version, txn)
				return ok, err
			}

			addUpgradesToFinalVersion := func() error {
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
						if v.Metadata().CanDirectUpgrade(from) {
							append(v.Metadata())
							from = v.Metadata().Version
						}
					}
				}
				return versions.AddVersionUpgrades(upgrades, txn)
			}

			// step 1
			if checked, err := checker(); err != nil || checked {
				return err
			}

			// step 2
			if err := versions.AddVersion(final.Version, versions.StateCreated, txn); err != nil {
				return err
			}

			// step 3
			return addUpgradesToFinalVersion()
		},
		opts)
}

func (s *service) asyncUpgradeTask(ctx context.Context) {
	fn := func() error {
		var err error
		var state int32
		opts := executor.Options{}.
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied()
		err = s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				state, err = s.upgradeTask(ctx, txn)
				return err
			},
			opts)
		if err != nil {
			return err
		}

		switch state {
		case versions.StateCreated:
		case versions.StateUpgradingTenant:
		default:
			panic(fmt.Sprintf("BUG: invalid state %d", state))
		}
		return nil
	}

	timer := time.NewTimer(checkUpgradeDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := fn(); err != nil {
				continue
			}
			timer.Reset(checkUpgradeDuration)
		}
	}
}

func (s *service) upgradeTask(
	ctx context.Context,
	txn executor.TxnExecutor) (int32, error) {
	final := getFinalUpgradeHandle().Metadata()

	upgrades, err := versions.GetUpgradeVersions(final.Version, txn, true)
	if err != nil {
		return 0, err
	}

	var state int32
	for _, u := range upgrades {
		state, err = s.doUpgrade(ctx, u, txn)
		if err != nil {
			return 0, err
		}
		switch state {
		case versions.StateReady:
		case versions.StateUpgradingTenant:
			return state, nil
		default:
			panic(fmt.Sprintf("BUG: invalid state %d", state))
		}
	}
	return state, nil
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
	h := getUpgradeHandle(upgrade.ToVersion)
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
		total := 0
		err := fetchTenants(
			upgradeTenantBatch,
			func(ids []int32) error {
				total += len(ids)
				return versions.AddUpgradeTenantTask(upgrade.ID, upgrade.ToVersion, ids[0], ids[len(ids)-1], txn)
			},
			txn)
		if err != nil {
			return 0, err
		}

		if err := versions.UpdateVersionUpgradeTasks(upgrade, int32(total), 0, txn); err != nil {
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
