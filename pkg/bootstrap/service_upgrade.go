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
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v2_0_0"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	defaultUpgradeTenantBatch         = 16
	defaultCheckUpgradeDuration       = time.Second * 5
	defaultCheckUpgradeTenantDuration = time.Second * 10
	defaultUpgradeTenantTasks         = 4
)

// Each catalog transaction needs its own deadline. A complete upgrade may
// contain many transactions and remains governed by its caller's context.
const upgradeTransactionTimeout = 24 * time.Hour

type upgradeFailure struct{ err error }

// terminalUpgradeError indicates that a retry cannot succeed without operator
// action. Transient catalog, RPC and transaction errors continue to retry.
// upgradeContextError adds an operation diagnostic without erasing the original
// retry/terminal classification or errors.Is/errors.As identity.
type upgradeContextError struct {
	message string
	cause   error
}

func (e *upgradeContextError) Error() string { return e.message + ": " + e.cause.Error() }
func (e *upgradeContextError) Unwrap() error { return e.cause }

func wrapUpgradeError(cause error, format string, args ...any) error {
	return &upgradeContextError{message: fmt.Sprintf(format, args...), cause: cause}
}

type terminalUpgradeError struct{ error }

func (e *terminalUpgradeError) Unwrap() error { return e.error }

// crossingPending reports whether the frozen-4.0.7 crossing has not durably
// committed for the persisted latest version row v. Intermediate route
// progress is persisted only as mo_upgrade step rows, so a latest row beyond
// 4.0.7 needs one scan of its own route; rows on or below 4.0.7 are answered
// from the row itself with no added read.
func crossingPending(v versions.Version, txn executor.TxnExecutor) (bool, error) {
	if versions.Compare(v.Version, "4.0.7") < 0 {
		return true, nil
	}
	if v.Version == "4.0.7" {
		return !v.IsReady(), nil
	}
	steps, err := versions.GetUpgradeVersions(v.Version, v.VersionOffset, txn, false, false)
	if err != nil {
		return false, err
	}
	return crossingPendingInSteps(steps), nil
}

func crossingPendingInSteps(steps []versions.VersionUpgrade) bool {
	for _, step := range steps {
		if step.ToVersion == "4.0.7" && step.State != versions.StateReady {
			return true
		}
	}
	return false
}

func (s *service) UpgradeStatus() (bool, error) {
	if s.upgrade.finalVersionCompleted.Load() {
		return true, nil
	}
	if failure := s.upgrade.failure.Load(); failure != nil {
		return false, failure.err
	}
	return false, nil
}

func (s *service) CheckManualUpgradeReadiness(ctx context.Context) error {
	ctx = executor.WithLocalExecution(ctx)
	ctx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	return s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		latest, err := versions.GetLatestVersion(txn)
		if err != nil {
			return err
		}
		final := s.getFinalVersionHandle().Metadata()
		if latest.Version == final.Version && latest.VersionOffset == final.VersionOffset && latest.IsReady() {
			return nil
		}
		return moerr.NewInvalidStateNoCtxf(
			"catalog version %s/%d is not ready for %s/%d; enable auto-upgrade for the stopped upgrade and restart",
			latest.Version, latest.VersionOffset, final.Version, final.VersionOffset)
	}, opts)
}

func (s *service) BootstrapUpgrade(ctx context.Context) error {
	ctx = executor.WithLocalExecution(ctx)
	s.logger.Info("start bootstrap upgrade")
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
	if err := retryRun(ctx, s.logger, "doCheckUpgrade", s.doCheckUpgrade); err != nil {
		s.logger.Error("check upgrade failed", zap.Error(err))
		return err
	}
	if err := s.stopper.RunTask(func(ctx context.Context) {
		s.asyncUpgradeTask(executor.WithLocalExecution(ctx))
	}); err != nil {
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
	return s.checkUpgrade(ctx, false)
}

// recheck is used only after a worker commits completion of a retained target.
// Another CN may already have created (or completed) our route in the meantime.
func (s *service) checkUpgrade(ctx context.Context, recheck bool) error {
	ctx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	completed := false
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	err := s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			final := s.getFinalVersionHandle().Metadata()

			s.logger.Info("start doCheckUpgrade",
				zap.String("final-version", final.Version),
				zap.String("txn-id", hex.EncodeToString(txn.Txn().Txn().ID)),
			)

			// Deploy mo first time without 1.2.0, init framework first.
			// And upgrade to current version.
			created, err := versions.IsFrameworkTablesCreated(txn)
			if err != nil {
				s.logger.Error("failed to check upgrade framework",
					zap.Error(err))
				return err
			}

			// First version as a genesis version, always need to be PREPARE.
			// Because the first version need to init upgrade framework tables.
			if !created {
				// Get the founder version of the upgrade framework
				founder := s.getFounderVersionHandle().Metadata()
				s.logger.Info("init upgrade framework",
					zap.String("founder-version", founder.Version),
					zap.String("final-version", final.Version))

				// create new upgrade framework tables for the first time,
				// which means using v1.2.0 for the first time
				//err = s.getFinalVersionHandle().HandleCreateFrameworkDeps(txn)
				err = s.getFounderVersionHandle().HandleCreateFrameworkDeps(txn)
				if err != nil {
					s.logger.Error("execute pre dependencies error when creating a new upgrade framework", zap.Error(err))
					return err
				}

				// Many cn maybe create framework tables parallel, only one can create success.
				// Just return error, and upgrade framework will retry.

				//err = createFrameworkTables(txn, final)
				err = createFrameworkTables(txn, founder)
				if err != nil {
					s.logger.Error("create upgrade framework tables error", zap.Error(err))
					return err
				}
				s.logger.Info("create upgrade framework tables success")
			}

			// lock version table
			startTime := time.Now()
			if err := txn.LockTable(catalog.MOVersionTable); err != nil {
				s.logger.Error("failed to lock table",
					zap.String("table", catalog.MOVersionTable),
					zap.String("txn-id", hex.EncodeToString(txn.Txn().Txn().ID)),
					zap.Duration("lock_duration", time.Since(startTime)),
					zap.Error(err))
				return err
			}
			s.logger.Info("lock table successfully",
				zap.String("table", catalog.MOVersionTable),
				zap.String("txn-id", hex.EncodeToString(txn.Txn().Txn().ID)),
				zap.Duration("lock_duration", time.Since(startTime)))

			if recheck {
				state, exists, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, false)
				if err != nil || exists {
					if err == nil && exists && state != versions.StateReady && versions.Compare(final.Version, "4.0.7") >= 0 {
						if final.Version == "4.0.7" {
							if err = rejectLegacyCDCTasks(txn); err != nil {
								return err
							}
						} else if err = s.checkPendingV407Upgrade(final, txn); err != nil {
							return err
						}
					}
					completed = err == nil && exists && state == versions.StateReady
					return err
				}
			}

			v, err := versions.GetLatestVersion(txn)
			if err != nil {
				s.logger.Error("failed to get latest version",
					zap.Error(err))
				return err
			}
			if err := s.checkRetiredCatalogSource(v.Version); err != nil {
				return err
			}

			// A previously created offset-only route is equally unsupported. Its
			// target can have this binary's offset even though account versions
			// cannot record which accounts completed that route.
			if !v.IsReady() && versions.Compare(final.Version, "4.0.7") >= 0 && v.Version == "4.0.7" && v.VersionOffset == v4_0_7.Handler.Metadata().VersionOffset {
				steps, err := versions.GetUpgradeVersions(v.Version, v.VersionOffset, txn, false, false)
				if err != nil {
					return err
				}
				for _, step := range steps {
					if step.FromVersion == "4.0.7" && step.ToVersion == "4.0.7" {
						return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("unsupported in-progress 4.0.7 offset-only upgrade; 4.0.7 is frozen and further migrations require a new catalog version")}
					}
				}
			}

			// 4.0.7 uses account version as its only durable per-account progress
			// marker. That marker cannot distinguish two offsets of the same
			// version. Reject that route before any catalog mutation instead of
			// repeatedly advancing the tenant counter or hanging recovery. Once
			// released, further catalog migrations must use a newer version.
			if v.IsReady() && versions.Compare(final.Version, "4.0.7") >= 0 && v.Version == "4.0.7" && v.VersionOffset != v4_0_7.Handler.Metadata().VersionOffset {
				return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf(
					"catalog 4.0.7 offset %d differs from binary offset %d; 4.0.7 is frozen and further migrations require a new catalog version",
					v.VersionOffset, v4_0_7.Handler.Metadata().VersionOffset)}
			}
			// Validate retained recovery before any catalog setup. That validation
			// owns CDC admission for this route; do not run its preflight twice.
			retainedRecovery := !v.IsReady() && v.Version != final.Version && s.canResumeUpgrade(v)
			if retainedRecovery {
				if err := s.checkUpgradeRecovery(v, txn); err != nil {
					return err
				}
			}
			if created && !retainedRecovery && versions.Compare(final.Version, "4.0.7") >= 0 {
				pending, err := crossingPending(v, txn)
				if err != nil {
					return err
				}
				if pending {
					if err := rejectLegacyCDCTasks(txn); err != nil {
						return err
					}
				}
			}

			// Admission needs these catalogs before the ordinary upgrade route can
			// reach 4.0.7. Serialize their idempotent creation with route creation
			// under the existing version-table lock, and commit both together.
			if versions.Compare(final.Version, "4.0.7") >= 0 &&
				(versions.Compare(v.Version, "4.0.7") < 0 ||
					(v.Version == "4.0.7" && !v.IsReady())) {
				if err := v4_0_7.EnsureViewMetadataCatalogs(txn); err != nil {
					return err
				}
			}

			s.logger.Info("get current mo cluster latest version",
				zap.String("latest", v.Version),
				zap.Uint32("latest versionOffset", v.VersionOffset),
				zap.String("final", final.Version),
				zap.Uint32("final versionOffset", final.VersionOffset))

			// Retain an unfinished historical route. Do not create a second route
			// until its cluster and tenant stages have all committed completion.
			if !v.IsReady() {
				if v.Version != final.Version {
					if retainedRecovery {
						return nil
					}
					return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot upgrade to version %s, because version %s is in upgrading",
						final.Version, v.Version)}
				} else if v.VersionOffset != final.VersionOffset {
					return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot upgrade to version %s with versionOffset[%d], because version %s with versionOffset[%d] is in upgrading",
						final.Version, final.VersionOffset, v.Version, v.VersionOffset)}
				}
			}

			// cluster is running at v1, cannot startup a old version to join cluster.
			if v.IsReady() && versions.Compare(final.Version, v.Version) < 0 {
				return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot start older version %s, current cluster version is %s",
					final.Version, v.Version)}
			}

			// check upgrade has 2 step:
			// 1: already checked, version exists
			// 2: add upgrades from latest version to final version
			checker := func() (bool, error) {
				if v.Version == final.Version && v.VersionOffset >= final.VersionOffset {
					completed = v.IsReady()
					return true, nil
				}

				state, ok, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, false)
				completed = err == nil && ok && state == versions.StateReady
				if err != nil {
					s.logger.Error("failed to get final version state",
						zap.String("final", final.Version),
						zap.Error(err))
				}
				return ok, err
			}

			addUpgradesToFinalVersion := func() error {
				if err := versions.AddVersion(final.Version, final.VersionOffset, versions.StateCreated, txn); err != nil {
					s.logger.Error("failed to add final version",
						zap.String("final", final.Version),
						zap.Error(err))
					return err
				}

				s.logger.Info("final version added",
					zap.String("final", final.Version), zap.Uint32("versionOffset", final.VersionOffset))

				latest, err := versions.MustGetLatestReadyVersion(txn)
				if err != nil {
					s.logger.Error("failed to get latest ready version",
						zap.String("latest", latest),
						zap.Error(err))
					return err
				}

				s.logger.Info("current latest ready version loaded",
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

					s.logger.Info("version upgrade added",
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
	if err == nil && completed {
		s.upgrade.finalVersionCompleted.Store(true)
	}
	return err
}

// A semantic version with an unchanged handler offset can be resumed by a newer
// CN. Offset-only recovery remains unsupported: an old worker cannot distinguish
// different handler generations for the same ToVersion.
func (s *service) canResumeUpgrade(target versions.Version) bool {
	if versions.Compare(target.Version, s.getFinalVersionHandle().Metadata().Version) >= 0 {
		return false
	}
	for _, h := range s.handles {
		if h.Metadata().Version == target.Version {
			return h.Metadata().VersionOffset == target.VersionOffset
		}
	}
	return false
}

func (s *service) checkUpgradeRecovery(target versions.Version, txn executor.TxnExecutor) error {
	upgrades, err := versions.GetUpgradeVersions(target.Version, target.VersionOffset, txn, false, false)
	if err != nil {
		return err
	}
	if len(upgrades) == 0 || upgrades[len(upgrades)-1].ToVersion != target.Version {
		return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot resume incomplete upgrade route to %s", target.Version)}
	}
	for i, u := range upgrades {
		if err := s.checkRetiredCatalogSource(u.FromVersion); err != nil {
			return err
		}
		if u.UpgradeOrder != int32(i) ||
			(i > 0 && u.FromVersion != upgrades[i-1].ToVersion) ||
			versions.Compare(u.FromVersion, u.ToVersion) > 0 ||
			versions.Compare(u.ToVersion, target.Version) > 0 ||
			(u.State != versions.StateCreated && u.State != versions.StateUpgradingTenant && u.State != versions.StateReady) {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot resume invalid upgrade step %s", u.String())}
		}
		// Offset-only legs target only the frozen 4.0.7 stage, whose only
		// durable per-account progress marker cannot identify two offsets
		// of the same version. Reject resumption here exactly as the
		// pre-mutation check rejects creating it.
		if u.FromVersion == v4_0_7.Handler.Metadata().Version &&
			u.ToVersion == v4_0_7.Handler.Metadata().Version {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("unsupported in-progress 4.0.7 offset-only upgrade; 4.0.7 is frozen and further migrations require a new catalog version")}
		}
		supported := false
		for _, h := range s.handles {
			v := h.Metadata()
			if v.Version == u.ToVersion {
				supported = v.CanDirectUpgrade(u.FromVersion) &&
					v.UpgradeCluster == u.UpgradeCluster && v.UpgradeTenant == u.UpgradeTenant
				break
			}
		}
		if !supported {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("cannot resume unsupported upgrade step %s", u.String())}
		}
	}
	// Preserve the frozen-stage admission fence even when a later catalog
	// repair is the retained route's final target.
	pending, err := validateV407UpgradeSteps(upgrades)
	if err != nil {
		return err
	}
	if pending {
		return rejectLegacyCDCTasks(txn)
	}
	return nil
}

// Each pass chooses its target from committed catalog state, so a restart or a
// competing CN can take over without a process-local recovery generation.
func (s *service) newUpgradePass(ctx context.Context) func() (bool, error) {
	var phasedUpgradeID uint64
	return func() (bool, error) {
		for {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			var err error
			var completed bool
			var progressed bool
			var phase *versions.VersionUpgrade
			final := s.getFinalVersionHandle().Metadata()
			target := final
			opts := executor.Options{}.
				WithDatabase(catalog.MO_CATALOG).
				WithTxnMode(txn.TxnMode_Pessimistic).
				WithTxnIsolation(txn.TxnIsolation_RC).
				WithMinCommittedTS(s.now()).
				WithWaitCommittedLogApplied().
				WithTimeZone(time.Local)
			operationCtx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
			err = s.exec.ExecTxn(
				operationCtx,
				func(txn executor.TxnExecutor) error {
					_, exists, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, false)
					if err != nil {
						return err
					}
					if !exists {
						target, err = versions.GetLatestVersion(txn)
						if err != nil {
							return err
						}
						if target.Version == "" || versions.Compare(target.Version, final.Version) >= 0 ||
							(!target.IsReady() && !s.canResumeUpgrade(target)) {
							return moerr.NewInvalidStateNoCtxf("cannot resume upgrade target %s offset %d", target.Version, target.VersionOffset)
						}
						if !target.IsReady() {
							if err := s.checkUpgradeRecovery(target, txn); err != nil {
								return err
							}
						}
					}
					completed, progressed, phase, err = s.performUpgrade(operationCtx, target, txn, phasedUpgradeID)
					return err
				},
				opts)
			if err != nil {
				err = moerr.AttachCause(operationCtx, err)
			}
			cancel()
			if err != nil {
				return false, err
			}
			if phase != nil {
				// The transaction above committed all preceding released steps
				// before the snapshot preflight and independently committed work.
				if err := s.runV407ClusterPhase(ctx, *phase); err != nil {
					return false, err
				}
				phasedUpgradeID = phase.ID
				continue
			}
			if progressed {
				continue
			}
			if completed && target.Version != final.Version {
				// Release the old target's row/step locks before taking the version
				// table lock to create our route. Never wait for tenants under locks.
				return false, s.checkUpgrade(ctx, true)
			}
			return completed, nil
		}
	}
}

// asyncUpgradeTask is a task that executes the upgrade logic step by step
// according to the created upgrade steps
func (s *service) asyncUpgradeTask(ctx context.Context) {
	fn := s.newUpgradePass(ctx)
	timer := time.NewTimer(s.upgrade.checkUpgradeDuration)
	defer timer.Stop()

	defer func() {
		s.logger.Info("upgrade task exit",
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
			var terminal *terminalUpgradeError
			if errors.As(err, &terminal) {
				if s.recordTerminalUpgradeFailure(ctx, err) {
					return
				}
			}
			if err != nil {
				s.logger.Error("upgrade attempt failed; will retry", zap.Error(err))
			}
			timer.Reset(s.upgrade.checkUpgradeDuration)
		}
	}
}

// Check persisted progress before publishing a terminal failure: another CN
// may have completed the upgrade while this worker was discovering the error.
func (s *service) recordTerminalUpgradeFailure(ctx context.Context, failure error) bool {
	ctx = executor.WithLocalExecution(ctx)
	opCtx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	final := s.getFinalVersionHandle().Metadata()
	var ready bool
	err := s.exec.ExecTxn(opCtx, func(txn executor.TxnExecutor) error {
		state, exists, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, true)
		ready = err == nil && exists && state == versions.StateReady
		return err
	}, executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied())
	if err != nil {
		s.logger.Error("failed to recheck upgrade after terminal error", zap.Error(err))
		return false
	}
	if ready {
		s.upgrade.finalVersionCompleted.Store(true)
	} else {
		s.upgrade.failure.CompareAndSwap(nil, &upgradeFailure{err: failure})
	}
	return true
}

func (s *service) performUpgrade(
	ctx context.Context,
	final versions.Version,
	txn executor.TxnExecutor,
	phasedUpgradeID uint64) (bool, bool, *versions.VersionUpgrade, error) {

	// make sure only one cn can execute upgrade logic
	state, ok, err := versions.GetVersionState(final.Version, final.VersionOffset, txn, true)
	if err != nil {
		s.logger.Error("failed to load final version state",
			zap.String("final", final.Version),
			zap.Int32("versionOffset", int32(final.VersionOffset)),
			zap.Error(err))
		return false, false, nil, err
	}
	if !ok {
		s.logger.Info("final version not found, retry later",
			zap.String("final", final.Version),
			zap.Int32("versionOffset", int32(final.VersionOffset)))
		return false, false, nil, nil
	}

	s.logger.Info("final version state loaded",
		zap.String("final", final.Version),
		zap.Int32("versionOffset", int32(final.VersionOffset)),
		zap.Int32("state", state))

	if state == versions.StateReady {
		return true, false, nil, nil
	}

	// get upgrade steps, and perform upgrade one by one
	upgrades, err := versions.GetUpgradeVersions(final.Version, final.VersionOffset, txn, true, true)
	if err != nil {
		s.logger.Error("failed to load upgrades",
			zap.String("final", final.Version),
			zap.Error(err))
		return false, false, nil, err
	}

	for _, u := range upgrades {
		if err := s.checkRetiredCatalogSource(u.FromVersion); err != nil {
			return false, false, nil, err
		}
	}
	for _, u := range upgrades {
		s.logger.Info("handle version upgrade",
			zap.String("upgrade", u.String()))

		if u.State == versions.StateCreated && u.UpgradeCluster == versions.Yes &&
			(phasedUpgradeID == 0 || u.ID != phasedUpgradeID) && s.getVersionHandle(u.ToVersion) == v4_0_7.Handler {
			if u.ID == 0 {
				return false, false, nil, &terminalUpgradeError{moerr.NewInvalidStateNoCtx("catalog upgrade step has no ID")}
			}
			// Commit any preceding steps before starting a separately committed
			// reconciliation of the unpublished 4.0.7 catalog.
			phase := u
			return false, false, &phase, nil
		}
		state, err := s.doUpgrade(ctx, u, txn, phasedUpgradeID != 0 && u.ID == phasedUpgradeID)
		if err != nil {
			s.logger.Error("failed to handle version upgrade",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version),
				zap.Error(err))
			return false, false, nil, err
		}

		switch state {
		case versions.StateCreated:
			// Task enumeration has more pages. Commit this page before continuing.
			return false, true, nil, nil
		case versions.StateReady:
			// upgrade was completed
			s.logger.Info("upgrade version completed",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version))
		case versions.StateUpgradingTenant:
			// we must wait all tenant upgrade completed, and then upgrade to
			// next version
			s.logger.Debug("upgrade version in tenant upgrading",
				zap.String("upgrade", u.String()),
				zap.String("final", final.Version))
			return false, false, nil, nil
		default:
			s.logger.Fatal(fmt.Sprintf("BUG: invalid state %d", state))
		}
	}

	// all upgrades completed, update final version to ready state.
	if err := versions.UpdateVersionState(final.Version, final.VersionOffset, versions.StateReady, txn); err != nil {
		s.logger.Error("failed to update state",
			zap.String("final", final.Version),
			zap.Error(err))

		return false, false, nil, err
	}

	s.logger.Info("upgrade to final version completed",
		zap.String("final", final.Version))
	return true, false, nil, nil
}

// doUpgrade Corresponding to one upgrade step in a version upgrade
func (s *service) doUpgrade(
	ctx context.Context,
	upgrade versions.VersionUpgrade,
	txn executor.TxnExecutor,
	clusterPhased bool) (int32, error) {
	if upgrade.State == versions.StateReady {
		return upgrade.State, nil
	}

	if upgrade.UpgradeCluster == versions.No && upgrade.UpgradeTenant == versions.No {
		if err := versions.UpdateVersionUpgradeState(upgrade, versions.StateReady, txn); err != nil {
			return 0, err
		}
		return versions.StateReady, nil
	}

	if upgrade.State == versions.StateUpgradingTenant {
		if upgrade.TotalTenant == upgrade.ReadyTenant {
			unready, err := versions.HasUnreadyUpgradeTenantTasks(upgrade.ID, txn)
			if err != nil {
				return 0, err
			}
			if !unready {
				if err := versions.UpdateVersionUpgradeState(upgrade, versions.StateReady, txn); err != nil {
					return 0, err
				}
				return versions.StateReady, nil
			}
		}
		return upgrade.State, nil
	}

	state := versions.StateReady
	h := s.getVersionHandle(upgrade.ToVersion)

	s.logger.Info("execute upgrade prepare",
		zap.String("upgrade", upgrade.String()))
	if err := h.Prepare(ctx, txn, h.Metadata().Version == upgrade.FinalVersion); err != nil {
		return 0, err
	}
	s.logger.Info("execute upgrade prepare completed",
		zap.String("upgrade", upgrade.String()))

	if upgrade.UpgradeCluster == versions.Yes && !clusterPhased {
		s.logger.Info("execute upgrade cluster",
			zap.String("upgrade", upgrade.String()))
		ctx = context.WithValue(ctx, v2_0_0.KekKey{}, s.upgrade.kek)
		if err := h.HandleClusterUpgrade(ctx, txn); err != nil {
			return 0, err
		}
		s.logger.Info("execute upgrade cluster completed",
			zap.String("upgrade", upgrade.String()))
	}

	if upgrade.UpgradeTenant == versions.Yes {
		if required := h.Metadata().RequiredProtocolVersion; required > 0 {
			if err := versions.CheckCommonProtocolVersion(txn, required); err != nil {
				return 0, err
			}
		}
		last, err := versions.LastUpgradeTenantTaskAccountID(upgrade.ID, txn)
		if err != nil {
			return 0, err
		}
		ids, err := fetchTenantPage(last, txn)
		if err != nil {
			return 0, err
		}
		if len(ids) != 0 {
			const maxInt32 = int32(^uint32(0) >> 1)
			if s.upgrade.upgradeTenantBatch <= 0 {
				return 0, moerr.NewInvalidStateNoCtx("upgrade tenant batch must be positive")
			}
			if upgrade.TotalTenant < 0 || upgrade.ReadyTenant < 0 || upgrade.ReadyTenant > upgrade.TotalTenant ||
				upgrade.TotalTenant > maxInt32-int32(len(ids)) {
				return 0, moerr.NewInvalidStateNoCtx("invalid or overflowing upgrade tenant counts")
			}
			for start := 0; start < len(ids); start += s.upgrade.upgradeTenantBatch {
				end := min(start+s.upgrade.upgradeTenantBatch, len(ids))
				if err := versions.AddUpgradeTenantTask(upgrade.ID, upgrade.ToVersion, ids[start], ids[end-1], txn); err != nil {
					return 0, err
				}
			}
			upgrade.TotalTenant += int32(len(ids))
			if err := versions.UpdateVersionUpgradeTasks(upgrade, txn); err != nil {
				return 0, err
			}
			return versions.StateCreated, nil
		}
		state = versions.StateUpgradingTenant
		if upgrade.TotalTenant == 0 {
			state = versions.StateReady
		}
	}

	s.logger.Info("upgrade update state",
		zap.String("upgrade", upgrade.String()),
		zap.Int32("state", state))
	return state, versions.UpdateVersionUpgradeState(upgrade, state, txn)
}

func retryRun(
	ctx context.Context,
	logger *log.MOLogger,
	name string,
	fn func(ctx context.Context) error) error {
	wait := time.Second
	maxWait := time.Second * 10
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		var terminal *terminalUpgradeError
		if errors.As(err, &terminal) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		logger.Error("execute task failed, retry later",
			zap.String("task", name),
			zap.Duration("wait", wait),
			zap.Error(err))
		if err := waitBootstrapRetry(ctx, wait); err != nil {
			return err
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
}

func (s *service) adjustUpgrade() {
	if s.upgrade.catalogObjectLimit == 0 {
		s.upgrade.catalogObjectLimit = DefaultCatalogUpgradeObjectBytes
	}
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
	s.logger.Info("upgrade config",
		zap.Duration("check-upgrade-duration", s.upgrade.checkUpgradeDuration),
		zap.Duration("check-upgrade-tenant-duration", s.upgrade.checkUpgradeTenantDuration),
		zap.Int("upgrade-tenant-tasks", s.upgrade.upgradeTenantTasks),
		zap.Int("tenant-batch", s.upgrade.upgradeTenantBatch))
}

// createFrameworkTables When init upgrade framework for the first time,
// create the tables that the upgrade framework depends on
func createFrameworkTables(
	txn executor.TxnExecutor,
	final versions.Version) error {
	values := versions.FrameworkInitSQLs
	values = append(values, final.GetInitVersionSQL(versions.StateReady))

	for _, sql := range values {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
