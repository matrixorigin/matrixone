// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

const (
	lifecycleDiscoveryPageObjects       = 64
	lifecycleDiscoveryMetaBytes         = 8 << 20
	lifecycleTargetObjectBytes          = 128 << 20
	lifecycleMaxCreatedObjects          = 32
	lifecycleMaxCertifiedBlockReadBytes = 256 << 20
	lifecycleWholeBatchMaxSources       = 64
	lifecycleWholeBatchMaxSourceBytes   = 4 << 30
	lifecycleFullScanInterval           = 24 * time.Hour
	lifecycleMetadataCompactionInterval = 5 * time.Minute
	lifecycleMaxClusterChildren         = 2
	lifecycleCleanupSweepBudget         = time.Minute
	lifecycleObjectAttemptTimeout       = 30 * time.Minute
	lifecycleCoordinatorRunTimeout      = lifecycleObjectAttemptTimeout + 5*time.Minute
)

func lifecycleMetadataCompactionDue(last, now time.Time) bool {
	return last.IsZero() || !now.Before(last.Add(lifecycleMetadataCompactionInterval))
}

type lifecycleObjectPlanInput struct {
	Source objectio.ObjectEntry
	Whole  bool
}

type lifecycleObjectPlan struct {
	Sources     []objectio.ObjectEntry
	Whole       bool
	SourceBytes uint64
}

// planLifecycleObjectTasks keeps Mixed Rewrite strictly single-source while
// coalescing adjacent Whole Objects into one bounded exact-retire transaction.
// This avoids one Dataset/Root per 128 MiB Object on TB-scale ordered tables
// without introducing persistent candidate state or an Object index.
func planLifecycleObjectTasks(
	inputs []lifecycleObjectPlanInput,
) []lifecycleObjectPlan {
	plans := make([]lifecycleObjectPlan, 0, len(inputs))
	wholeSources := make([]objectio.ObjectEntry, 0, lifecycleWholeBatchMaxSources)
	var wholeBytes uint64
	flushWhole := func() {
		if len(wholeSources) == 0 {
			return
		}
		plans = append(plans, lifecycleObjectPlan{
			Sources:     wholeSources,
			Whole:       true,
			SourceBytes: wholeBytes,
		})
		wholeSources = make(
			[]objectio.ObjectEntry,
			0,
			lifecycleWholeBatchMaxSources,
		)
		wholeBytes = 0
	}
	for _, input := range inputs {
		sourceBytes := lifecycleObjectPressureBytes(input.Source.ObjectStats)
		if !input.Whole {
			flushWhole()
			plans = append(plans, lifecycleObjectPlan{
				Sources:     []objectio.ObjectEntry{input.Source},
				SourceBytes: sourceBytes,
			})
			continue
		}
		if len(wholeSources) > 0 &&
			(len(wholeSources) == lifecycleWholeBatchMaxSources ||
				wholeBytes+sourceBytes > lifecycleWholeBatchMaxSourceBytes) {
			flushWhole()
		}
		wholeSources = append(wholeSources, input.Source)
		wholeBytes += sourceBytes
	}
	flushWhole()
	return plans
}

type lifecycleBindingExecutor struct {
	engine       engine.Engine
	txnClient    client.TxnClient
	sqlExecutor  executor.SQLExecutor
	taeFS        fileservice.FileService
	release      lifecyclepkg.SQLReleaseConfig
	pager        lifecyclepkg.SQLBindingPager
	admission    *lifecyclepkg.RewriteAdmission
	rewriteSlots chan struct{}
	faults       lifecyclepkg.FaultInjector
	now          func() time.Time
	epoch        uint64
}

func lifecycleCoordinatorConfig() lifecyclepkg.CoordinatorConfig {
	return lifecyclepkg.CoordinatorConfig{
		Enabled:             true,
		PageSize:            64,
		MaxPagesPerRun:      4,
		MaxBindingsPerRun:   1000,
		MaxClusterChildren:  lifecycleMaxClusterChildren,
		MaxAccountChildren:  4,
		MaxDatabaseChildren: 2,
		MaxTableChildren:    1,
	}
}

func resolveLifecycleTAEFileService(
	fileServices fileservice.FileService,
) (fileservice.FileService, error) {
	if fileServices == nil {
		// Unit tests that only exercise disabled/maintenance paths do not touch
		// TAE objects and may intentionally omit a FileService.
		return nil, nil
	}
	return fileservice.Get[fileservice.FileService](
		fileServices,
		defines.SharedFileServiceName,
	)
}

// LifecycleTaskExecutorFactory wires the existing TaskService, transaction
// engine, FileService, Merge producer, and GC SyncProtection path. Ordinary
// tables are untouched because the coordinator pages only explicit Bindings.
func LifecycleTaskExecutorFactory(
	txnEngine engine.Engine,
	txnClient client.TxnClient,
	sqlExecutor executor.SQLExecutor,
	fileServices fileservice.FileService,
	faults lifecyclepkg.FaultInjector,
) func(context.Context, taskpb.Task) error {
	taeFS, taeFSErr := resolveLifecycleTAEFileService(fileServices)
	release := lifecyclepkg.SQLReleaseConfig{Executor: sqlExecutor}
	pager := lifecyclepkg.SQLBindingPager{Executor: sqlExecutor}
	admission, admissionErr := lifecyclepkg.NewRewriteAdmission(
		lifecyclepkg.RewriteReleaseProfile{
			Window:                   24 * time.Hour,
			MaxAmplification:         20,
			MaxSourceBytesPerAccount: 1 << 40,
			MaxSourceBytesPerCluster: 4 << 40,
		},
	)
	cleanupReconcileCursor := ""
	var metadataAccountCursor uint32
	var restoreCleanupCursor lifecyclepkg.ExpiredRestoreCursor
	rewriteSlots := make(chan struct{}, 1)
	runSlots := make(chan struct{}, 1)
	var lastMetadataCompaction time.Time
	var activeRunner *lifecycleBindingExecutor
	coordinator := lifecyclepkg.NewCoordinator(
		lifecycleCoordinatorConfig(),
		pager,
		func(ctx context.Context, binding lifecyclepkg.Binding) error {
			// runSlots keeps activeRunner stable until every child from this
			// tick has completed. The Coordinator itself is retained so its
			// in-process Binding cursor survives across TaskService ticks.
			if activeRunner == nil {
				return fmt.Errorf("Lifecycle coordinator runner is not installed")
			}
			return activeRunner.run(ctx, binding)
		},
	)
	return func(ctx context.Context, scheduled taskpb.Task) error {
		// TaskService runner contexts do not necessarily carry a deadline, while
		// every internal SQL transaction requires one. Keep this boundary local
		// to Lifecycle and allow an earlier caller deadline to win.
		ctx, cancelRun := context.WithTimeout(ctx, lifecycleCoordinatorRunTimeout)
		defer cancelRun()
		// TaskService declares coordinator concurrency one, and this local guard
		// also protects cursors if a duplicate invocation is delivered during
		// runner handoff. A duplicate tick is skipped instead of queued behind a
		// potentially long Lifecycle run. Ordinary transaction and Merge paths
		// never access this slot.
		releaseRun, acquired := tryAcquireLifecycleCoordinatorRunSlot(runSlots)
		if !acquired {
			return nil
		}
		defer releaseRun()
		if admissionErr != nil {
			return admissionErr
		}
		if taeFSErr != nil {
			return fmt.Errorf("resolve Lifecycle SHARED FileService: %w", taeFSErr)
		}
		var cleanupErr error
		var cleanupScanComplete bool
		cleanupReconcileCursor, cleanupScanComplete, cleanupErr = sweepLifecycleCleanupRoots(
			ctx,
			sqlExecutor,
			taeFS,
			faults,
			cleanupReconcileCursor,
		)
		if !cleanupScanComplete {
			return cleanupErr
		}
		var restoreCleanupErr error
		restoreCleanupCursor, restoreCleanupErr =
			cleanupExpiredLifecycleRestores(
				ctx,
				sqlExecutor,
				restoreCleanupCursor,
			)
		var metadataErr error
		now := time.Now()
		if lifecycleMetadataCompactionDue(lastMetadataCompaction, now) {
			metadataAccountCursor, _, metadataErr =
				(lifecyclepkg.SQLMetadataCompactor{Executor: sqlExecutor}).
					CompactPage(
						ctx,
						metadataAccountCursor,
						now,
						30*24*time.Hour,
						8,
						256,
					)
			if metadataErr == nil {
				lastMetadataCompaction = now
			}
		}
		enabled, err := release.Enabled(ctx)
		if err != nil || !enabled {
			return errors.Join(
				cleanupErr,
				restoreCleanupErr,
				metadataErr,
				err,
			)
		}
		epoch := lifecycleTaskEpoch(scheduled)
		activeRunner = &lifecycleBindingExecutor{
			engine:       txnEngine,
			txnClient:    txnClient,
			sqlExecutor:  sqlExecutor,
			taeFS:        taeFS,
			release:      release,
			pager:        pager,
			admission:    admission,
			rewriteSlots: rewriteSlots,
			faults:       faults,
			now:          time.Now,
			epoch:        epoch,
		}
		return errors.Join(
			cleanupErr,
			restoreCleanupErr,
			metadataErr,
			coordinator.Run(ctx),
		)
	}
}

func tryAcquireLifecycleCoordinatorRunSlot(
	slots chan struct{},
) (func(), bool) {
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return nil, false
	}
}

func cleanupExpiredLifecycleRestores(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	cursor lifecyclepkg.ExpiredRestoreCursor,
) (lifecyclepkg.ExpiredRestoreCursor, error) {
	attempts, next, err := (lifecyclepkg.SQLExpiredRestorePager{
		Executor: sqlExecutor,
	}).Next(
		ctx,
		cursor,
		time.Now(),
		8,
		64,
	)
	if err != nil {
		return cursor, err
	}
	var cleanupErr error
	for _, attempt := range attempts {
		cleanupErr = errors.Join(
			cleanupErr,
			(SQLRestoreRepository{
				AccountID:          attempt.AccountID,
				TargetDatabaseName: attempt.TargetDatabaseName,
				Executor:           sqlExecutor,
			}).CleanupHidden(ctx, attempt.RestoreID),
		)
	}
	return next, cleanupErr
}

func sweepLifecycleCleanupRoots(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	taeFS fileservice.FileService,
	faults lifecyclepkg.FaultInjector,
	reconcileCursor string,
) (string, bool, error) {
	roots := lifecyclepkg.SQLCleanupRootRepository{Executor: sqlExecutor}
	temporary, err := roots.ListPublishedTemporary(ctx, 64)
	if err != nil {
		return reconcileCursor, false, err
	}
	reconcileable, nextCursor, _, err := roots.ListReconcileable(
		ctx,
		reconcileCursor,
		64,
	)
	if err != nil {
		return reconcileCursor, false, err
	}
	due, err := roots.ListSweepable(ctx, time.Now(), 64)
	if err != nil {
		return reconcileCursor, false, err
	}
	if len(due) == 0 &&
		len(temporary) == 0 &&
		len(reconcileable) == 0 {
		return nextCursor, true, nil
	}
	// Cleanup is maintenance work. Bound one processing pass so a slow
	// Provider cannot monopolize the single Lifecycle coordinator and starve
	// later reconciliation or Restore cleanup. An unfinished idempotent page is
	// replayed on the next tick.
	sweepDeadline := time.Now().Add(lifecycleCleanupSweepBudget)
	taeStore := lifecyclepkg.FileServiceArchiveStore{
		FileService:    taeFS,
		MaxListEntries: 100_000,
	}
	var sweepErr error
	for _, root := range temporary {
		rootTimeout, ok := lifecycleCleanupRootTimeout(
			time.Now(),
			sweepDeadline,
		)
		if !ok {
			break
		}
		cleaned, cleanupErr := func() (lifecyclepkg.CleanupRoot, error) {
			rootCtx, cancelRoot := context.WithTimeout(
				ctx,
				rootTimeout,
			)
			defer cancelRoot()
			return lifecyclepkg.CleanupPublishedTemporary(
				rootCtx,
				roots,
				taeStore,
				root,
			)
		}()
		if cleanupErr != nil {
			root.LastError = cleanupErr.Error()
			_, updateErr := roots.UpdateCleanup(ctx, root, root.StateVersion)
			sweepErr = errors.Join(sweepErr, cleanupErr, updateErr)
			continue
		}
		if cleaned.Mode == lifecyclepkg.CleanupModeTTLRewrite {
			_, transitionErr := roots.Transition(
				ctx,
				cleaned.RootID,
				cleaned.AttemptID,
				cleaned.ExecutorEpoch,
				lifecyclepkg.CleanupRootPublished,
				cleaned.StateVersion,
				lifecyclepkg.CleanupRootDeletePending,
			)
			sweepErr = errors.Join(sweepErr, transitionErr)
		}
	}
	reconcileCatalog := lifecyclepkg.SQLCleanupReconcileCatalog{
		Executor: sqlExecutor,
	}
	reconciler := lifecyclepkg.CleanupReconciler{
		Roots:   roots,
		Catalog: reconcileCatalog,
	}
	now := time.Now()
	processedReconcileable := 0
	for _, root := range reconcileable {
		rootTimeout, ok := lifecycleCleanupRootTimeout(
			time.Now(),
			sweepDeadline,
		)
		if !ok {
			break
		}
		rootCtx, cancelRoot := context.WithTimeout(ctx, rootTimeout)
		_, reconcileErr := reconciler.ReconcileOne(rootCtx, root, now)
		cancelRoot()
		sweepErr = errors.Join(sweepErr, reconcileErr)
		processedReconcileable++
	}
	now = time.Now()
	for _, root := range due {
		rootTimeout, ok := lifecycleCleanupRootTimeout(
			time.Now(),
			sweepDeadline,
		)
		if !ok {
			break
		}
		var archiveFS fileservice.FileService
		sweeper := lifecyclepkg.CleanupSweeper{
			Roots: roots,
			ResolveArchive: func(
				resolveCtx context.Context,
				root lifecyclepkg.CleanupRoot,
			) (lifecyclepkg.CleanupObjectStore, error) {
				if root.ArchivePrefix == "" {
					return nil, nil
				}
				target, parseErr := lifecyclepkg.ParseFrozenArchiveTarget(
					[]byte(root.ArchiveNamespace),
				)
				if parseErr != nil {
					return nil, parseErr
				}
				created, createErr := lifecyclepkg.NewArchiveFileService(
					resolveCtx,
					target,
				)
				if createErr != nil {
					return nil, createErr
				}
				archiveFS = created
				return lifecyclepkg.FileServiceArchiveStore{
					FileService:    created,
					MaxListEntries: 100_000,
				}, nil
			},
			ResolveTAE: func(
				context.Context,
				lifecyclepkg.CleanupRoot,
			) (lifecyclepkg.CleanupObjectStore, error) {
				return taeStore, nil
			},
			FinalizePublication: reconcileCatalog.FinalizeCleanup,
			QuiescenceWindow:    10 * time.Minute,
			Faults:              faults,
		}
		rootErr := func() error {
			rootCtx, cancelRoot := context.WithTimeout(
				ctx,
				rootTimeout,
			)
			defer cancelRoot()
			return sweeper.SweepOne(rootCtx, root.RootID, now)
		}()
		if archiveFS != nil {
			closeCtx, cancelClose := context.WithTimeout(
				context.WithoutCancel(ctx),
				lifecycleProtectionReleaseTimeout,
			)
			archiveFS.Close(closeCtx)
			cancelClose()
		}
		if rootErr != nil {
			_, deferErr := lifecyclepkg.DeferCleanupRoot(
				ctx,
				roots,
				root.RootID,
				now,
				rootErr,
			)
			sweepErr = errors.Join(sweepErr, rootErr, deferErr)
		}
	}
	if processedReconcileable != len(reconcileable) {
		// Do not advance past Roots that were not visited before the pass
		// budget expired. Reconciliation is idempotent, so replaying already
		// processed rows is safe and keeps the cursor contract simple.
		nextCursor = reconcileCursor
	}
	return nextCursor, true, sweepErr
}

func lifecycleCleanupRootTimeout(
	now time.Time,
	sweepDeadline time.Time,
) (time.Duration, bool) {
	remaining := sweepDeadline.Sub(now)
	if remaining <= 0 {
		return 0, false
	}
	return min(remaining, lifecycleTemporaryCleanupTimeout), true
}

func lifecycleTaskEpoch(scheduled taskpb.Task) uint64 {
	switch value := scheduled.(type) {
	case *taskpb.AsyncTask:
		return max(uint64(value.GetEpoch()), 1)
	default:
		return 1
	}
}

func (runner *lifecycleBindingExecutor) run(
	ctx context.Context,
	binding lifecyclepkg.Binding,
) (err error) {
	if runner.engine == nil ||
		runner.txnClient == nil ||
		runner.sqlExecutor == nil ||
		runner.taeFS == nil ||
		runner.now == nil {
		return fmt.Errorf("Lifecycle binding executor dependencies are incomplete")
	}
	enabled, err := runner.release.Enabled(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		// The coordinator may have loaded and queued this Binding before an
		// operator closed the release gate. Do not start a new child after the
		// gate closes; an Object already past this check is allowed to finish.
		return nil
	}
	archiveAction := strings.EqualFold(binding.Action, "ARCHIVE")
	deleteAction := strings.EqualFold(binding.Action, "DELETE")
	if !archiveAction && !deleteAction {
		return fmt.Errorf(
			"Lifecycle action %q is not enabled",
			binding.Action,
		)
	}
	if archiveAction && binding.PurgeAfterDays <= binding.ExpireAfterDays {
		return fmt.Errorf("Lifecycle Archive retention window is invalid")
	}
	var target lifecyclepkg.FrozenArchiveTarget
	var archiveFS fileservice.FileService
	if archiveAction {
		target, err = runner.release.ResolveArchiveTarget(
			ctx,
			binding.AccountID,
			binding.StageID,
			binding.StageIdentityDigest,
		)
		if err != nil {
			return err
		}
		archiveFS, err = lifecyclepkg.NewArchiveFileService(ctx, target)
		if err != nil {
			return err
		}
		defer func() {
			closeCtx, cancelClose := context.WithTimeout(
				context.WithoutCancel(ctx),
				lifecycleProtectionReleaseTimeout,
			)
			defer cancelClose()
			archiveFS.Close(closeCtx)
		}()
	}

	accountCtx := defines.AttachAccount(
		ctx,
		binding.AccountID,
		catalog.System_User,
		catalog.System_Role,
	)
	operator, err := runner.txnClient.New(
		accountCtx,
		runner.engine.LatestLogtailAppliedTime(),
		client.WithTxnCreateBy(
			binding.AccountID,
			"",
			"tae object lifecycle reader",
			0,
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancelRollback := lifecycleRollbackContext(accountCtx)
		defer cancelRollback()
		err = errors.Join(
			err,
			operator.Rollback(rollbackCtx),
		)
	}()
	if err = runner.engine.New(accountCtx, operator); err != nil {
		return err
	}
	_, _, relation, err := runner.engine.GetRelationById(
		accountCtx,
		operator,
		binding.PhysicalTableID,
	)
	if err != nil {
		return err
	}
	table, ok := relation.(LifecycleTable)
	if !ok {
		return fmt.Errorf(
			"table %d does not expose Lifecycle capabilities",
			binding.PhysicalTableID,
		)
	}
	tableDef := relation.GetTableDef(accountCtx)
	if tableDef == nil || tableDef.TblId != binding.PhysicalTableID {
		return fmt.Errorf("Lifecycle table definition identity changed")
	}
	bindingSchemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	if !strings.EqualFold(
		binding.SchemaDigest,
		hex.EncodeToString(bindingSchemaDigest[:]),
	) {
		return fmt.Errorf("Lifecycle Binding schema fence changed")
	}
	schema, schemaDigest, err := lifecyclepkg.BuildSchemaDescriptor(
		accountCtx,
		tableDef,
	)
	if err != nil {
		return err
	}
	columnOrdinal, columnSeqnum, columnType, err := lifecycleColumn(
		tableDef,
		binding.LifecycleColumnID,
	)
	if err != nil {
		return err
	}
	evaluation := runner.now()
	cutoff, encodedCutoff, err := lifecycleCutoff(
		evaluation,
		binding.ExpireAfterDays,
		binding.LateArrivalGraceDays,
		binding.EvaluationTimezone,
		columnType,
	)
	if err != nil {
		return err
	}
	snapshot := types.TimestampToTS(operator.SnapshotTS())
	cursor := lifecycleDiscoveryCursor(binding)
	discoveryCtx, cancelDiscovery := context.WithTimeout(
		accountCtx,
		30*time.Second,
	)
	page, err := table.LifecycleDiscoverObjectPage(
		discoveryCtx,
		lifecycleDiscoveryRequest(binding, snapshot, evaluation, cursor),
	)
	if err != nil {
		cancelDiscovery()
		return err
	}
	planInputs, nextCursor, completedFullScanAt, err :=
		classifyLifecycleDiscoveryPage(
			discoveryCtx,
			page,
			table.LifecycleSortKeyOrdinal(),
			columnOrdinal,
			columnSeqnum,
			columnType,
			encodedCutoff,
			lifecycleDiscoveryMetaBytes,
			func(
				loadCtx context.Context,
				stats objectio.ObjectStats,
				seqnum uint16,
			) (objectio.ZoneMap, error) {
				return loadLifecycleObjectColumnZoneMap(
					loadCtx,
					runner.taeFS,
					stats,
					seqnum,
				)
			},
		)
	cancelDiscovery()
	if err != nil {
		return err
	}
	fullScanAt := page.StartedFullScanAt
	if !completedFullScanAt.IsZero() {
		fullScanAt = completedFullScanAt
	}
	binding, err = runner.pager.SaveCursor(
		accountCtx,
		binding,
		nextCursor,
		fullScanAt,
	)
	if err != nil {
		return err
	}
	if len(planInputs) == 0 {
		return nil
	}

	var archiveStore lifecyclepkg.ArchiveStore
	if archiveAction {
		archiveStore = lifecyclepkg.FileServiceArchiveStore{
			FileService:    archiveFS,
			MaxListEntries: 100_000,
		}
	}
	processor := &LifecycleProcessor{
		Config: LifecycleProcessorConfig{
			TAENamespace:          "shared",
			MaxRestoreChunkRows:   65_536,
			MaxChunkBytes:         64 << 20,
			MaxActiveCleanupRoots: 4096,
			MaxActiveCleanupBytes: 64 << 40,
			CleanupGrace:          10 * time.Minute,
		},
		Roots: lifecyclepkg.SQLCleanupRootRepository{
			Executor: runner.sqlExecutor,
		},
		CleanupCapacity: lifecyclepkg.SQLCleanupRootRepository{
			Executor: runner.sqlExecutor,
		},
		Store: archiveStore,
		TemporaryStore: lifecyclepkg.FileServiceArchiveStore{
			FileService:    runner.taeFS,
			MaxListEntries: 100_000,
		},
		Protection: lifecyclepkg.SQLSyncProtectionClient{
			Executor:    runner.sqlExecutor,
			FileService: runner.taeFS,
			TaskID:      lifecyclepkg.CoordinatorTaskID,
		},
		RewriteAdmission: runner.admission,
		Finalizer: TxnLifecycleFinalCommitter{
			Engine:      runner.engine,
			TxnClient:   runner.txnClient,
			SQLExecutor: runner.sqlExecutor,
		},
		Faults: runner.faults,
	}
	for _, objectPlan := range planLifecycleObjectTasks(planInputs) {
		enabled, gateErr := runner.release.Enabled(accountCtx)
		if gateErr != nil {
			return gateErr
		}
		if !enabled {
			// Recheck between Objects so a long Binding page cannot continue
			// starting retirements after the kill switch is closed.
			return nil
		}
		var maxCreated uint32
		var deltaRows uint64
		var deltaBytes uint64
		var deltaBlocks uint32
		if !objectPlan.Whole {
			source := objectPlan.Sources[0]
			maxCreated = uint32(math.Ceil(
				float64(lifecycleObjectPressureBytes(source.ObjectStats))/
					float64(lifecycleTargetObjectBytes),
			)) + 1
			maxCreated = min(maxCreated, lifecycleMaxCreatedObjects)
			deltaRows = 100_000
			deltaBytes = 32 << 20
			deltaBlocks = source.ObjectStats.BlkCnt()
		}
		objectTask := LifecycleObjectTask{
			Binding:             binding,
			Table:               table,
			Sources:             objectPlan.Sources,
			SourceSnapshot:      snapshot,
			Schema:              schema,
			SchemaDigest:        schemaDigest,
			BindingSchemaDigest: bindingSchemaDigest,
			Classifier: lifecyclepkg.ExpirationClassifier{
				ColumnOrdinal: columnOrdinal,
				ColumnType:    columnType,
				Cutoff:        encodedCutoff,
			}.Classify,
			Whole:                      objectPlan.Whole,
			Cutoff:                     cutoff,
			Now:                        evaluation,
			Deadline:                   evaluation.Add(lifecycleObjectAttemptTimeout),
			ExecutorEpoch:              runner.epoch,
			TargetObjectSize:           lifecycleTargetObjectBytes,
			MaxCreatedObjects:          maxCreated,
			MaxCertifiedBlockReadBytes: lifecycleMaxCertifiedBlockReadBytes,
			DeltaRows:                  deltaRows,
			DeltaBytes:                 deltaBytes,
			DeltaBlocks:                deltaBlocks,
			ProtectionLimits: logtailreplay.LifecycleTombstoneSelectionLimits{
				MaxScannedObjects:  10_000,
				MaxSelectedObjects: 1_024,
				MaxMetaBytes:       64 << 20,
			},
		}
		committed := false
		objectTask.OnFinalCommit = func() { committed = true }
		processErr := func() error {
			releaseRewrite := func() {}
			if !objectPlan.Whole {
				var acquireErr error
				releaseRewrite, acquireErr =
					tryAcquireLifecycleRewriteSlot(
						accountCtx,
						runner.rewriteSlots,
					)
				if acquireErr != nil {
					return acquireErr
				}
			}
			defer releaseRewrite()
			if archiveAction {
				objectTask.ArchiveTarget = target
				objectTask.DatasetID = uuid.NewString()
				objectTask.PurgeAfter =
					time.Duration(binding.PurgeAfterDays) * 24 * time.Hour
				_, processErr := processor.ProcessArchiveObject(
					accountCtx,
					objectTask,
				)
				return processErr
			}
			objectTask.ReceiptID = uuid.NewString()
			_, processErr := processor.ProcessTTLObject(
				accountCtx,
				objectTask,
			)
			return processErr
		}()
		if processErr != nil {
			if isLifecycleDeferredObjectError(processErr) {
				continue
			}
			return processErr
		}
		if !applyLifecycleObjectOutcome(&binding, committed) {
			continue
		}
		operation := "ttl_whole"
		if archiveAction {
			operation = "archive_whole"
		}
		if !objectPlan.Whole {
			if archiveAction {
				operation = "archive_rewrite"
			} else {
				operation = "ttl_rewrite"
			}
		}
		metricv2.LifecycleObjectCounter.WithLabelValues(operation).Add(
			float64(len(objectPlan.Sources)),
		)
		metricv2.LifecycleBytesCounter.WithLabelValues(
			"retired_source",
		).Add(float64(objectPlan.SourceBytes))
	}
	return nil
}

func applyLifecycleObjectOutcome(
	binding *lifecyclepkg.Binding,
	committed bool,
) bool {
	if binding == nil || !committed {
		return false
	}
	// The final Binding fence increments the same row version. A complete
	// no-op scan publishes no fence and therefore must not advance this local
	// copy before the next Object in the same metadata page.
	binding.Version++
	return true
}

func isLifecycleDeferredObjectError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "MIXED_LAYOUT_BLOCKED:") ||
		strings.Contains(message, "RESOURCE_BLOCKED:")
}

// tryAcquireLifecycleRewriteSlot is a CN-local Scheduler guard. It applies
// only to Lifecycle Mixed Rewrite and never enters the TN, ordinary Merge, or
// transaction paths. Fail-fast admission avoids keeping a read transaction
// open while waiting; a later bounded metadata scan may retry the Object.
func tryAcquireLifecycleRewriteSlot(
	ctx context.Context,
	slots chan struct{},
) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if slots == nil || cap(slots) == 0 {
		return nil, fmt.Errorf(
			"Lifecycle Rewrite concurrency is not configured",
		)
	}
	select {
	case slots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-slots
			})
		}, nil
	default:
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"rewrite_concurrency",
		).Inc()
		return nil, fmt.Errorf(
			"RESOURCE_BLOCKED: Lifecycle Rewrite concurrency is exhausted",
		)
	}
}

func lifecycleColumn(
	table *plan.TableDef,
	columnID uint64,
) (int, uint16, types.T, error) {
	ordinal := 0
	for _, column := range table.Cols {
		if column == nil || column.Hidden {
			continue
		}
		if column.ColId == columnID {
			if column.Seqnum > math.MaxUint16 {
				return 0, 0, 0, fmt.Errorf(
					"Lifecycle column seqnum %d exceeds Object metadata encoding",
					column.Seqnum,
				)
			}
			oid := types.T(column.Typ.Id)
			switch oid {
			case types.T_date, types.T_datetime, types.T_timestamp:
				return ordinal, uint16(column.Seqnum), oid, nil
			default:
				return 0, 0, 0, fmt.Errorf(
					"Lifecycle column type %s is no longer supported",
					oid,
				)
			}
		}
		ordinal++
	}
	return 0, 0, 0, fmt.Errorf("Lifecycle column %d no longer exists", columnID)
}

func lifecycleCutoff(
	evaluation time.Time,
	expireDays uint32,
	graceDays uint32,
	timezone string,
	columnType types.T,
) (time.Time, int64, error) {
	if evaluation.IsZero() || expireDays == 0 {
		return time.Time{}, 0, fmt.Errorf("Lifecycle cutoff input is incomplete")
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, 0, err
	}
	localCutoff := evaluation.In(location).AddDate(
		0,
		0,
		-int(expireDays)-int(graceDays),
	)
	switch columnType {
	case types.T_date:
		year, month, day := localCutoff.Date()
		return localCutoff, int64(types.DateFromCalendar(
			int32(year),
			uint8(month),
			uint8(day),
		)), nil
	case types.T_datetime:
		year, month, day := localCutoff.Date()
		hour, minute, second := localCutoff.Clock()
		return localCutoff, int64(types.DatetimeFromClock(
			int32(year),
			uint8(month),
			uint8(day),
			uint8(hour),
			uint8(minute),
			uint8(second),
			uint32(localCutoff.Nanosecond()/1_000),
		)), nil
	case types.T_timestamp:
		return localCutoff, int64(types.UnixNanoToTimestamp(
			localCutoff.UTC().UnixNano(),
		)), nil
	default:
		return time.Time{}, 0, fmt.Errorf(
			"unsupported Lifecycle column type %s",
			columnType,
		)
	}
}

// lifecycleObjectExpirationByZoneMap uses the ObjectStats fast path only when
// the lifecycle column is the physical sort key.
func lifecycleObjectExpirationByZoneMap(
	stats objectio.ObjectStats,
	sortKeyOrdinal int,
	columnOrdinal int,
	columnType types.T,
	cutoff int64,
) (whole bool, skip bool) {
	if sortKeyOrdinal != columnOrdinal {
		return false, false
	}
	return lifecycleExpirationByZoneMap(
		stats.SortKeyZoneMap(),
		columnType,
		cutoff,
	)
}

// lifecycleExpirationByZoneMap returns whole=true only when max < cutoff.
// skip=true only when min >= cutoff proves that no row is expired. Unknown or
// legacy metadata deliberately falls back to the exact Reader classifier.
func lifecycleExpirationByZoneMap(
	zoneMap objectio.ZoneMap,
	columnType types.T,
	cutoff int64,
) (whole bool, skip bool) {
	if !zoneMap.IsInited() ||
		zoneMap.GetType() != columnType ||
		zoneMap.MaxTruncated() {
		return false, false
	}
	minimum, minOK := lifecycleTemporalValue(zoneMap.GetMin())
	maximum, maxOK := lifecycleTemporalValue(zoneMap.GetMax())
	if !minOK || !maxOK {
		return false, false
	}
	return maximum < cutoff, minimum >= cutoff
}

type lifecycleColumnZoneMapLoader func(
	context.Context,
	objectio.ObjectStats,
	uint16,
) (objectio.ZoneMap, error)

// classifyLifecycleDiscoveryPage keeps arbitrary lifecycle columns on the
// existing Object metadata path. It never creates an Object index or reads
// data rows merely to decide Whole/Mixed/not-yet-expired. If the metadata
// budget fills, only the ordered prefix is consumed and the cursor remains at
// the last classified Object so the tail is not skipped.
func classifyLifecycleDiscoveryPage(
	ctx context.Context,
	page lifecyclepkg.DiscoveryPage,
	sortKeyOrdinal int,
	columnOrdinal int,
	columnSeqnum uint16,
	columnType types.T,
	cutoff int64,
	maxMetaBytes uint64,
	load lifecycleColumnZoneMapLoader,
) (
	[]lifecycleObjectPlanInput,
	lifecyclepkg.DiscoveryCursor,
	time.Time,
	error,
) {
	if maxMetaBytes == 0 || page.MetaBytes > maxMetaBytes {
		return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, fmt.Errorf(
			"RESOURCE_BLOCKED: Lifecycle discovery metadata budget is exhausted",
		)
	}
	inputs := make(
		[]lifecycleObjectPlanInput,
		0,
		len(page.Candidates),
	)
	consumed := len(page.Candidates)
	usedMetaBytes := page.MetaBytes
	if sortKeyOrdinal != columnOrdinal {
		if load == nil {
			return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, fmt.Errorf(
				"Lifecycle non-sort-key metadata loader is unavailable",
			)
		}
		consumed = 0
		for _, candidate := range page.Candidates {
			if err := ctx.Err(); err != nil {
				return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, err
			}
			charge, err := lifecycleObjectMetaCharge(candidate.Source.ObjectStats)
			if err != nil {
				return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, err
			}
			if charge > maxMetaBytes-usedMetaBytes {
				if consumed == 0 {
					return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, fmt.Errorf(
						"RESOURCE_BLOCKED: Lifecycle Object metadata exceeds the certified page budget",
					)
				}
				break
			}
			zoneMap, err := load(
				ctx,
				candidate.Source.ObjectStats,
				columnSeqnum,
			)
			if err != nil {
				return nil, lifecyclepkg.DiscoveryCursor{}, time.Time{}, err
			}
			usedMetaBytes += charge
			consumed++
			whole, skip := lifecycleExpirationByZoneMap(
				zoneMap,
				columnType,
				cutoff,
			)
			if !skip {
				inputs = append(inputs, lifecycleObjectPlanInput{
					Source: candidate.Source,
					Whole:  whole,
				})
			}
		}
	} else {
		for _, candidate := range page.Candidates {
			whole, skip := lifecycleObjectExpirationByZoneMap(
				candidate.Source.ObjectStats,
				sortKeyOrdinal,
				columnOrdinal,
				columnType,
				cutoff,
			)
			if !skip {
				inputs = append(inputs, lifecycleObjectPlanInput{
					Source: candidate.Source,
					Whole:  whole,
				})
			}
		}
	}

	next := page.Next
	completedAt := page.CompletedFullScanAt
	if consumed < len(page.Candidates) {
		last := page.Candidates[consumed-1]
		next = lifecyclepkg.DiscoveryCursor{
			Snapshot:       last.Snapshot,
			LastObjectName: *last.Source.ObjectShortName(),
			HasLastObject:  true,
		}
		completedAt = time.Time{}
	}
	return inputs, next, completedAt, nil
}

func lifecycleObjectMetaCharge(stats objectio.ObjectStats) (uint64, error) {
	extent := stats.Extent()
	compressed := uint64(extent.Length())
	decoded := uint64(extent.OriginSize())
	if compressed == 0 || decoded == 0 || decoded > (math.MaxUint64-compressed)/2 {
		return 0, fmt.Errorf(
			"RESOURCE_BLOCKED: Lifecycle Object metadata size is not certified",
		)
	}
	return compressed + 2*decoded, nil
}

func loadLifecycleObjectColumnZoneMap(
	ctx context.Context,
	fs fileservice.FileService,
	stats objectio.ObjectStats,
	seqnum uint16,
) (objectio.ZoneMap, error) {
	location := stats.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, err
	}
	data, ok := meta.DataMeta()
	if !ok {
		return nil, fmt.Errorf("Lifecycle source Object has no data metadata")
	}
	return data.MustGetColumn(seqnum).ZoneMap().Clone(), nil
}

func lifecycleTemporalValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case types.Date:
		return int64(typed), true
	case types.Datetime:
		return int64(typed), true
	case types.Timestamp:
		return int64(typed), true
	default:
		return 0, false
	}
}

func lifecycleDiscoveryCursor(
	binding lifecyclepkg.Binding,
) lifecyclepkg.DiscoveryCursor {
	cursor := lifecyclepkg.DiscoveryCursor{Wrapped: binding.ScanWrapped}
	if encoded, err := hex.DecodeString(binding.ScanSnapshotHex); err == nil &&
		len(encoded) == len(cursor.Snapshot) {
		copy(cursor.Snapshot[:], encoded)
	}
	if encoded, err := hex.DecodeString(binding.ScanLastObjectNameHex); err == nil &&
		len(encoded) == objectio.ObjectNameShortLen {
		copy(cursor.LastObjectName[:], encoded)
		cursor.HasLastObject = true
	}
	return cursor
}

func lifecycleDiscoveryRequest(
	binding lifecyclepkg.Binding,
	snapshot types.TS,
	now time.Time,
	cursor lifecyclepkg.DiscoveryCursor,
) lifecyclepkg.DiscoveryRequest {
	return lifecyclepkg.DiscoveryRequest{
		Snapshot:         snapshot,
		Now:              now,
		Cursor:           cursor,
		LastFullScanAt:   binding.LastFullScanAt,
		FullScanInterval: lifecycleFullScanInterval,
		Limits: lifecyclepkg.DiscoveryLimits{
			MaxObjects:   lifecycleDiscoveryPageObjects,
			MaxMetaBytes: lifecycleDiscoveryMetaBytes,
			MaxDuration:  30 * time.Second,
		},
	}
}

func lifecycleSchemaDigestString(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}
