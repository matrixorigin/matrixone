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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

const (
	lifecycleCommitProtocolVersion    uint32 = 1
	lifecycleProtectionReleaseTimeout        = 30 * time.Second
	lifecycleRollbackTimeout                 = 30 * time.Second
	lifecycleTemporaryCleanupTimeout         = 2 * time.Minute
)

func lifecycleProtectionReleaseContext(parent context.Context) (
	context.Context,
	context.CancelFunc,
) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		lifecycleProtectionReleaseTimeout,
	)
}

func lifecycleTemporaryCleanupContext(parent context.Context) (
	context.Context,
	context.CancelFunc,
) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		lifecycleTemporaryCleanupTimeout,
	)
}

func lifecycleRollbackContext(parent context.Context) (
	context.Context,
	context.CancelFunc,
) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		lifecycleRollbackTimeout,
	)
}

type LifecycleFinalizeRequest struct {
	Root                lifecyclepkg.CleanupRoot
	Binding             lifecyclepkg.Binding
	Manifest            *lifecyclepkg.ArchiveManifest
	ManifestKey         string
	ManifestDigest      [sha256.Size]byte
	Control             *api.LifecycleCommitEntry
	SyncProtectionJobID string
	FinalTxnID          string
	PurgeEligibleAt     time.Time
	Cutoff              time.Time
	EvaluationTime      time.Time
	ExpiredRows         uint64
	RetiredBytes        uint64
}

// LifecycleFinalCommitter owns the private ordinary transaction that inserts
// Dataset/Receipt, installs the thin retire entry, and commits immediately.
type LifecycleFinalCommitter interface {
	Finalize(context.Context, LifecycleFinalizeRequest) error
}

type LifecycleObjectTask struct {
	Binding lifecyclepkg.Binding
	Table   LifecycleTable
	Sources []objectio.ObjectEntry

	SourceSnapshot      types.TS
	Schema              lifecyclepkg.SchemaDescriptor
	SchemaDigest        [sha256.Size]byte
	BindingSchemaDigest [sha256.Size]byte
	Classifier          lifecyclepkg.RewriteBlockClassifier
	Whole               bool

	ArchiveTarget lifecyclepkg.FrozenArchiveTarget
	DatasetID     string
	ReceiptID     string
	Cutoff        time.Time
	PurgeAfter    time.Duration
	Now           time.Time
	Deadline      time.Time
	ExecutorEpoch uint64

	TargetObjectSize           uint32
	MaxCreatedObjects          uint32
	MaxCertifiedBlockReadBytes uint64
	DeltaRows                  uint64
	DeltaBytes                 uint64
	DeltaBlocks                uint32
	ProtectionLimits           logtailreplay.LifecycleTombstoneSelectionLimits
	// OnFinalCommit is a CN-local observation hook used only by the scheduler
	// to advance its in-memory Binding version after the ordinary final
	// transaction has actually committed. No-op scans must not look committed.
	OnFinalCommit func()
}

type LifecycleProcessorConfig struct {
	TAENamespace          string
	MaxRestoreChunkRows   uint64
	MaxChunkBytes         uint64
	MaxActiveCleanupRoots int
	MaxActiveCleanupBytes uint64
	CleanupGrace          time.Duration
}

type LifecycleProcessor struct {
	Config           LifecycleProcessorConfig
	Roots            lifecyclepkg.CleanupRootRepository
	CleanupCapacity  lifecyclepkg.CleanupCapacityChecker
	Store            lifecyclepkg.ArchiveStore
	Protection       lifecyclepkg.SyncProtectionClient
	RewriteAdmission *lifecyclepkg.RewriteAdmission
	Finalizer        LifecycleFinalCommitter
	TemporaryStore   lifecyclepkg.CleanupObjectStore
	Faults           lifecyclepkg.FaultInjector
	ID               func() string
}

func (processor *LifecycleProcessor) ProcessArchiveObject(
	ctx context.Context,
	task LifecycleObjectTask,
) (result lifecyclepkg.CleanupRoot, err error) {
	if err := processor.validateTask(ctx, task); err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	attemptCtx, cancelAttempt := context.WithDeadline(ctx, task.Deadline)
	defer cancelAttempt()
	ctx = attemptCtx
	id := processor.ID
	if id == nil {
		id = func() string { return uuid.NewString() }
	}
	faults := processor.Faults
	if faults == nil {
		faults = lifecyclepkg.NoLifecycleFaults{}
	}
	rootID := id()
	attemptID := id()
	finalTxnID := id()
	archivePrefix := path.Join(
		"lifecycle",
		rootID,
		attemptID,
	)
	bookingPrefix := ""
	if !task.Whole {
		bookingPrefix = path.Join(
			"lifecycle-staging",
			rootID,
			attemptID,
			"booking",
		)
	}
	targetIdentity, err := task.ArchiveTarget.Marshal()
	if err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	reservedCleanupBytes, archivePhysicalBytes, err :=
		lifecycleCleanupReservation(task, true)
	if err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	if err := processor.CleanupCapacity.CheckCreateCapacity(
		ctx,
		processor.Config.MaxActiveCleanupRoots,
		processor.Config.MaxActiveCleanupBytes,
		reservedCleanupBytes,
	); err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	sourceSetDigest := lifecycleSourceDigest(task.Sources)
	unresolved, err := processor.Roots.HasUnresolvedSource(
		ctx,
		task.Binding.AccountID,
		task.Binding.PhysicalTableID,
		sourceSetDigest,
	)
	if err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	if unresolved {
		return lifecyclepkg.CleanupRoot{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle table has an unresolved final transaction",
		)
	}
	if !task.Whole && processor.RewriteAdmission != nil {
		if err := processor.RewriteAdmission.ReserveSource(
			task.Binding.AccountID,
			lifecycleObjectPressureBytes(task.Sources[0].ObjectStats),
			task.Now,
		); err != nil {
			return lifecyclepkg.CleanupRoot{}, err
		}
	}
	mode := lifecyclepkg.CleanupModeArchiveRewrite
	if task.Whole {
		mode = lifecyclepkg.CleanupModeArchiveWhole
	}
	root := lifecyclepkg.CleanupRoot{
		RootID:               rootID,
		AttemptID:            attemptID,
		Mode:                 mode,
		OwnerAccountID:       task.Binding.AccountID,
		LogicalTableID:       task.Binding.LogicalTableID,
		PhysicalTableID:      task.Binding.PhysicalTableID,
		ExecutorEpoch:        task.ExecutorEpoch,
		WorkerDeadline:       task.Deadline,
		ArchiveNamespace:     string(targetIdentity),
		CredentialHandle:     task.ArchiveTarget.CredentialHandle,
		ArchivePrefix:        archivePrefix,
		TAENamespace:         processor.Config.TAENamespace,
		BookingPrefix:        bookingPrefix,
		OrdinalUpperBound:    task.MaxCreatedObjects,
		ReservedCleanupBytes: reservedCleanupBytes,
		SourceSetDigest:      sourceSetDigest,
		FinalTxnID:           finalTxnID,
		State:                lifecyclepkg.CleanupRootRegistered,
		StateVersion:         1,
		CleanupAfter: lifecycleCleanupAfter(
			task.Now,
			task.Deadline,
			processor.Config.CleanupGrace,
		),
		TemporaryCleanupDone: task.Whole,
	}
	var liveSegmentID objectio.Segmentid
	if !task.Whole {
		liveSegmentID = *objectio.NewSegmentid()
		root.SegmentID = liveSegmentID.String()
	}
	if err := processor.Roots.Register(ctx, root); err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterRootRegister); err != nil {
		return processor.abandon(ctx, root, err)
	}
	root, err = processor.transitionRoot(
		ctx,
		root,
		lifecyclepkg.CleanupRootUploading,
	)
	if err != nil {
		return root, err
	}

	protection, err := task.Table.LifecycleSelectProtectionSet(
		ctx,
		task.SourceSnapshot,
		task.Sources,
		task.ProtectionLimits,
	)
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	if !bytes.Equal(protection.SourceSetDigest[:], root.SourceSetDigest[:]) {
		return processor.abandon(
			ctx,
			root,
			moerr.NewInternalErrorNoCtxf("Lifecycle protected source set changed"),
		)
	}
	lease, err := lifecyclepkg.AcquireProtection(
		ctx,
		processor.Protection,
		root.AttemptID,
		protection,
		task.Deadline,
	)
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	defer func() {
		releaseCtx, cancelRelease := lifecycleProtectionReleaseContext(ctx)
		defer cancelRelease()
		if releaseErr := lease.Release(releaseCtx); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterProtection); err != nil {
		return processor.abandon(ctx, root, err)
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultBeforeSourceRead); err != nil {
		return processor.abandon(ctx, root, err)
	}

	writer, err := lifecyclepkg.NewArchiveWriter(
		ctx,
		lifecyclepkg.ArchiveWriterConfig{
			RootID:               root.RootID,
			AttemptID:            root.AttemptID,
			Prefix:               root.ArchivePrefix,
			WriteID:              id(),
			Schema:               task.Schema,
			SchemaDigest:         task.SchemaDigest,
			MaxRestoreChunkRows:  processor.Config.MaxRestoreChunkRows,
			MaxChunkLogicalBytes: processor.Config.MaxChunkBytes,
			MaxPhysicalBytes:     archivePhysicalBytes,
			Faults:               faults,
		},
		processor.Store,
		lifecyclepkg.NewCleanupRootSideEffectGuard(processor.Roots),
	)
	if err != nil {
		return processor.abandon(ctx, root, err)
	}

	var rewrite LifecycleRewriteResult
	var scanReport lifecyclepkg.ObjectScanReport
	if task.Whole {
		scanReport, err = processor.archiveWhole(ctx, task, writer)
	} else {
		if err := faults.Inject(
			ctx,
			lifecyclepkg.FaultBeforeRewriteStaging,
		); err != nil {
			return processor.abandon(ctx, root, err)
		}
		rewrite, err = task.Table.LifecycleRewriteObject(
			ctx,
			LifecycleRewriteOptions{
				SourceSnapshot:             task.SourceSnapshot,
				Source:                     task.Sources[0].ObjectStats,
				TargetObjSize:              task.TargetObjectSize,
				LiveSegmentID:              liveSegmentID,
				MaxCertifiedBlockReadBytes: task.MaxCertifiedBlockReadBytes,
				Classify:                   task.Classifier,
				Archive: func(
					callbackCtx context.Context,
					value *batch.Batch,
					expired *nulls.Nulls,
				) error {
					return writer.WriteBatch(callbackCtx, value, expired)
				},
				BeforeLiveWrite: func(
					callbackCtx context.Context,
					_ objectio.Segmentid,
				) error {
					return lifecyclepkg.NewCleanupRootSideEffectGuard(
						processor.Roots,
					).EnsureDurable(
						callbackCtx,
						root.RootID,
						root.AttemptID,
					)
				},
				BookingPath: func(ordinal uint32) (string, error) {
					return path.Join(
						root.BookingPrefix,
						fmt.Sprintf("booking-%06d", ordinal),
					), nil
				},
			},
		)
		scanReport = rewrite.ScanReport
		if errors.Is(err, ErrLifecycleNoExpiredRows) {
			return processor.completeNoop(ctx, root)
		}
		if errors.Is(err, ErrLifecycleRewriteHasNoLiveRows) {
			task.Whole = true
			rewrite.CreatedObjectStats = nil
			rewrite.TransferBookingLocation = nil
			rewrite.TransferMappingDigest = [sha256.Size]byte{}
			rewrite.MergeLevel = 0
			err = nil
		}
	}
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	if err := scanReport.ValidateComplete(); err != nil {
		return processor.abandon(ctx, root, err)
	}
	if scanReport.ExpiredRows == 0 {
		return processor.completeNoop(ctx, root)
	}
	if !task.Whole {
		if err := validateLifecycleRewriteOwnership(root, rewrite); err != nil {
			return processor.abandon(ctx, root, err)
		}
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterRewriteStaging); err != nil {
		return processor.abandon(ctx, root, err)
	}
	manifest, manifestKey, err := writer.Close(ctx)
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	if manifest.RowCount == 0 || manifest.RowCount != scanReport.ExpiredRows {
		return processor.abandon(
			ctx,
			root,
			moerr.NewInternalErrorNoCtxf(
				"Lifecycle Archive row count %d does not cover %d expired rows",
				manifest.RowCount,
				scanReport.ExpiredRows,
			),
		)
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterPayloadWrite); err != nil {
		return processor.abandon(ctx, root, err)
	}
	if !task.Whole && processor.RewriteAdmission != nil {
		sourceBytes := lifecycleObjectPressureBytes(task.Sources[0].ObjectStats)
		retiredBytes, estimateErr := lifecycleEstimatedExpiredPressureBytes(
			sourceBytes,
			scanReport,
		)
		if estimateErr != nil {
			return processor.abandon(ctx, root, estimateErr)
		}
		lifecycleObserveRewritePressure(sourceBytes, retiredBytes)
		if err := processor.RewriteAdmission.CheckAmplification(
			sourceBytes,
			retiredBytes,
		); err != nil {
			return processor.abandon(ctx, root, err)
		}
	}
	manifestBytes, manifestDigest, err := lifecyclepkg.MarshalArchiveManifest(manifest)
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	if len(manifestBytes) == 0 {
		return processor.abandon(
			ctx,
			root,
			moerr.NewInternalErrorNoCtxf("Lifecycle verified Manifest is empty"),
		)
	}
	root.ManifestKey = manifestKey
	root.ManifestDigest = manifestDigest
	root, err = processor.Roots.UpdateCleanup(ctx, root, root.StateVersion)
	if err != nil {
		return root, err
	}
	root, err = processor.transitionRoot(
		ctx,
		root,
		lifecyclepkg.CleanupRootVerified,
	)
	if err != nil {
		return root, err
	}
	root, err = processor.transitionRoot(
		ctx,
		root,
		lifecyclepkg.CleanupRootFinalizing,
	)
	if err != nil {
		return root, err
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultBeforeFinalCommit); err != nil {
		return processor.abandon(ctx, root, err)
	}
	control := processor.commitControl(task, root, rewrite)
	err = processor.Finalizer.Finalize(
		ctx,
		LifecycleFinalizeRequest{
			Root:                root,
			Binding:             task.Binding,
			Manifest:            manifest,
			ManifestKey:         manifestKey,
			ManifestDigest:      manifestDigest,
			Control:             control,
			SyncProtectionJobID: lease.JobID(),
			FinalTxnID:          finalTxnID,
			// Purge retention is measured from the Lifecycle value. Every
			// archived row is <= the frozen effective cutoff, so cutoff plus
			// the full policy interval is a conservative eligibility bound
			// without adding a second min/max tracking protocol.
			PurgeEligibleAt: task.Cutoff.Add(task.PurgeAfter),
			Cutoff:          task.Cutoff,
			EvaluationTime:  task.Now,
		},
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrTxnUnknown) {
			updated, transitionErr := processor.transition(
				ctx,
				root,
				lifecyclepkg.CleanupRootCommitUnknown,
				err,
			)
			if transitionErr != nil {
				return updated, errors.Join(err, transitionErr)
			}
			return updated, err
		}
		return processor.abandon(ctx, root, err)
	}
	if task.OnFinalCommit != nil {
		task.OnFinalCommit()
	}
	// The committed created Objects are now owned by the ordinary TAE
	// Catalog/WAL/GC path. Remove their deterministic segment range from the
	// Root before it can become PUBLISHED and later eligible for Purge.
	root.SegmentID = ""
	root.OrdinalUpperBound = 0
	if root.BookingPrefix == "" {
		root.TemporaryCleanupDone = true
	}
	root, err = processor.Roots.UpdateCleanup(ctx, root, root.StateVersion)
	if err != nil {
		return root, err
	}
	root, err = processor.transitionRoot(
		ctx,
		root,
		lifecyclepkg.CleanupRootPublished,
	)
	if err != nil {
		return root, err
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterFinalCommit); err != nil {
		return root, err
	}
	if !root.TemporaryCleanupDone {
		cleanupCtx, cancelCleanup := lifecycleTemporaryCleanupContext(ctx)
		defer cancelCleanup()
		cleaned, cleanupErr := lifecyclepkg.CleanupPublishedTemporary(
			cleanupCtx,
			processor.Roots,
			processor.TemporaryStore,
			root,
		)
		if cleanupErr != nil {
			root.LastError = cleanupErr.Error()
			updated, updateErr := processor.Roots.UpdateCleanup(
				cleanupCtx,
				root,
				root.StateVersion,
			)
			if updateErr == nil {
				root = updated
			}
			return root, nil
		}
		root = cleaned
	}
	return root, nil
}

// lifecycleCleanupAfter keeps cleanup behind every side effect the current
// attempt is still allowed to start. A short cleanup grace must never let a
// sweeper declare the namespace quiescent while a provider or FileService
// operation can still complete before the frozen worker deadline.
func lifecycleCleanupAfter(
	now time.Time,
	workerDeadline time.Time,
	grace time.Duration,
) time.Time {
	cleanupAfter := now.Add(grace)
	if workerDeadline.After(cleanupAfter) {
		return workerDeadline
	}
	return cleanupAfter
}

const (
	lifecycleArchivePhysicalOverhead = uint64(32 << 20)
	lifecycleBookingPhysicalOverhead = uint64(1 << 20)
	// External booking stores src_blk(int32), src_row(uint32),
	// dest_obj(uint8), dest_blk(uint16), and dest_row(uint32).
	lifecycleTransferEntryBytes = uint64(15)
)

// lifecycleCleanupReservation freezes a conservative, executable upper bound
// before Root registration. ArchiveWriter enforces archivePhysicalBytes before
// every PUT. Live Object and booking bounds come from the already-certified
// Merge output count, target size, Block-read ceiling, and transfer encoding.
func lifecycleCleanupReservation(
	task LifecycleObjectTask,
	archive bool,
) (reservedBytes uint64, archivePhysicalBytes uint64, err error) {
	var sourceBytes uint64
	var sourceRows uint64
	for _, source := range task.Sources {
		sourceBytes, err = lifecycleCheckedAdd(
			sourceBytes,
			lifecycleObjectPressureBytes(source.ObjectStats),
		)
		if err != nil {
			return 0, 0, err
		}
		sourceRows, err = lifecycleCheckedAdd(
			sourceRows,
			uint64(source.ObjectStats.Rows()),
		)
		if err != nil {
			return 0, 0, err
		}
	}
	if archive {
		archivePhysicalBytes, err = lifecycleCheckedAdd(
			sourceBytes*2,
			lifecycleArchivePhysicalOverhead,
		)
		if err != nil {
			return 0, 0, err
		}
		reservedBytes = archivePhysicalBytes
	}
	if task.Whole {
		return reservedBytes, archivePhysicalBytes, nil
	}
	if task.MaxCreatedObjects == 0 || task.TargetObjectSize == 0 {
		return 0, 0, moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Rewrite output bound is incomplete",
		)
	}
	liveBytes := uint64(task.MaxCreatedObjects) * uint64(task.TargetObjectSize)
	liveBytes, err = lifecycleCheckedAdd(
		liveBytes,
		task.MaxCertifiedBlockReadBytes,
	)
	if err != nil {
		return 0, 0, err
	}
	bookingBytes := sourceRows * lifecycleTransferEntryBytes
	bookingBytes, err = lifecycleCheckedAdd(
		bookingBytes,
		lifecycleBookingPhysicalOverhead,
	)
	if err != nil {
		return 0, 0, err
	}
	reservedBytes, err = lifecycleCheckedAdd(reservedBytes, liveBytes)
	if err != nil {
		return 0, 0, err
	}
	reservedBytes, err = lifecycleCheckedAdd(reservedBytes, bookingBytes)
	if err != nil {
		return 0, 0, err
	}
	return reservedBytes, archivePhysicalBytes, nil
}

func lifecycleCheckedAdd(left uint64, right uint64) (uint64, error) {
	value := left + right
	if value < left {
		return 0, moerr.NewInternalErrorNoCtxf("RESOURCE_BLOCKED: Lifecycle cleanup byte bound overflow")
	}
	return value, nil
}

func (processor *LifecycleProcessor) archiveWhole(
	ctx context.Context,
	task LifecycleObjectTask,
	writer *lifecyclepkg.ArchiveWriter,
) (lifecyclepkg.ObjectScanReport, error) {
	var aggregate lifecyclepkg.ObjectScanReport
	sources := append([]objectio.ObjectEntry(nil), task.Sources...)
	slices.SortFunc(sources, func(left, right objectio.ObjectEntry) int {
		return bytes.Compare(
			left.ObjectStats.ObjectName(),
			right.ObjectStats.ObjectName(),
		)
	})
	for _, source := range sources {
		report, err := task.Table.LifecycleReadObject(
			ctx,
			task.SourceSnapshot,
			source.ObjectStats,
			task.MaxCertifiedBlockReadBytes,
			func(value *batch.Batch, snapshotDeleted *nulls.Nulls) error {
				selected := nulls.NewWithSize(value.RowCount())
				for row := 0; row < value.RowCount(); row++ {
					if snapshotDeleted == nil ||
						!snapshotDeleted.Contains(uint64(row)) {
						selected.Add(uint64(row))
					}
				}
				return writer.WriteBatch(ctx, value, selected)
			},
		)
		if err != nil {
			return aggregate, err
		}
		expiredRows := report.ScannedRows - report.SnapshotDeletedRows
		if err := report.SetVisibleClassification(expiredRows, 0); err != nil {
			return aggregate, err
		}
		if err := report.ValidateComplete(); err != nil {
			return aggregate, err
		}
		if err := aggregate.Add(report); err != nil {
			return aggregate, err
		}
	}
	return aggregate, aggregate.ValidateComplete()
}

// completeNoop records that the exact scan found no visible expired rows.
// No Dataset/Receipt or retirement entry is published; Root-owned staging is
// left to the ordinary bounded sweeper and the scheduler may advance its
// cursor without treating a normal boundary Object as a failed Binding.
func (processor *LifecycleProcessor) completeNoop(
	ctx context.Context,
	root lifecyclepkg.CleanupRoot,
) (lifecyclepkg.CleanupRoot, error) {
	cleanupCtx, cancelCleanup := lifecycleTemporaryCleanupContext(ctx)
	defer cancelCleanup()
	return processor.transition(
		cleanupCtx,
		root,
		lifecyclepkg.CleanupRootDeletePending,
		nil,
	)
}

func (processor *LifecycleProcessor) commitControl(
	task LifecycleObjectTask,
	root lifecyclepkg.CleanupRoot,
	rewrite LifecycleRewriteResult,
) *api.LifecycleCommitEntry {
	sourceStats := make([][]byte, len(task.Sources))
	for index := range task.Sources {
		sourceStats[index] = append(
			[]byte(nil),
			task.Sources[index].ObjectStats.Marshal()...,
		)
	}
	mode := api.LifecycleCommitEntry_Whole
	if !task.Whole {
		mode = api.LifecycleCommitEntry_Rewrite
	}
	control := &api.LifecycleCommitEntry{
		ProtocolVersion:              lifecycleCommitProtocolVersion,
		RetireMode:                   mode,
		RootId:                       root.RootID,
		AttemptId:                    root.AttemptID,
		DatasetId:                    task.DatasetID,
		ReceiptId:                    task.ReceiptID,
		DatabaseId:                   task.Binding.DatabaseID,
		LogicalTableId:               task.Binding.LogicalTableID,
		PhysicalTableId:              task.Binding.PhysicalTableID,
		BindingGeneration:            task.Binding.Generation,
		SchemaDigest:                 append([]byte(nil), task.SchemaDigest[:]...),
		SourceSnapshotTs:             ptrTimestamp(task.SourceSnapshot.ToTimestamp()),
		SourceSetDigest:              append([]byte(nil), root.SourceSetDigest[:]...),
		DataSourceObjectStats:        sourceStats,
		FinalPrepareDeadlineUnixNano: task.Deadline.UnixNano(),
	}
	if !task.Whole {
		control.CreatedObjectStats = rewrite.CreatedObjectStats
		control.TransferBookingLocations = rewrite.TransferBookingLocation
		control.TransferMappingDigest = append(
			[]byte(nil),
			rewrite.TransferMappingDigest[:]...,
		)
		control.MaxDeltaRows = task.DeltaRows
		control.MaxDeltaBytes = task.DeltaBytes
		control.MaxDeltaBlocks = task.DeltaBlocks
		control.MergeLevel = rewrite.MergeLevel
	}
	return control
}

func ptrTimestamp(value timestamp.Timestamp) *timestamp.Timestamp {
	return &value
}

func (processor *LifecycleProcessor) validateTask(
	ctx context.Context,
	task LifecycleObjectTask,
) error {
	if processor.Roots == nil ||
		processor.CleanupCapacity == nil ||
		processor.Store == nil ||
		processor.Protection == nil ||
		processor.Finalizer == nil ||
		task.Table == nil ||
		task.SourceSnapshot.IsEmpty() ||
		len(task.Sources) == 0 ||
		task.DatasetID == "" ||
		task.Cutoff.IsZero() ||
		task.PurgeAfter <= 0 ||
		task.Binding.AccountID == 0 ||
		task.Binding.DatabaseID == 0 ||
		task.Binding.LogicalTableID == 0 ||
		task.Binding.PhysicalTableID == 0 ||
		task.Binding.Generation == 0 ||
		task.Binding.Version == 0 ||
		task.ExecutorEpoch == 0 ||
		task.MaxCertifiedBlockReadBytes == 0 ||
		task.Now.IsZero() ||
		!task.Deadline.After(task.Now) ||
		processor.Config.TAENamespace == "" ||
		processor.Config.MaxRestoreChunkRows == 0 ||
		processor.Config.MaxChunkBytes == 0 ||
		processor.Config.MaxActiveCleanupRoots <= 0 ||
		processor.Config.MaxActiveCleanupBytes == 0 ||
		processor.Config.CleanupGrace < 0 {
		return moerr.NewInvalidInput(ctx, "Lifecycle Object task is incomplete")
	}
	if !task.Whole && (len(task.Sources) != 1 ||
		task.Classifier == nil ||
		processor.TemporaryStore == nil ||
		task.MaxCreatedObjects == 0 ||
		task.DeltaRows == 0 ||
		task.DeltaBytes == 0 ||
		task.DeltaBlocks == 0) {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle Rewrite must have one source, classifier, and delta budgets",
		)
	}
	if task.Whole && len(task.Sources) > 64 {
		return moerr.NewInvalidInput(ctx, "Lifecycle Whole source set exceeds certified limit")
	}
	if task.Whole {
		var sourceBytes uint64
		for _, source := range task.Sources {
			sourceBytes += lifecycleObjectPressureBytes(source.ObjectStats)
			if sourceBytes > lifecycleWholeBatchMaxSourceBytes {
				return moerr.NewInvalidInput(
					ctx,
					"Lifecycle Whole source bytes exceed certified limit",
				)
			}
		}
	}
	digest, err := task.Schema.Digest()
	if err != nil {
		return err
	}
	if digest != task.SchemaDigest {
		return moerr.NewInvalidInput(ctx, "Lifecycle schema digest changed")
	}
	if !strings.EqualFold(
		task.Binding.SchemaDigest,
		hex.EncodeToString(task.BindingSchemaDigest[:]),
	) {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle Binding schema digest changed",
		)
	}
	if err := task.ArchiveTarget.Validate(); err != nil {
		return err
	}
	return nil
}

func (processor *LifecycleProcessor) abandon(
	ctx context.Context,
	root lifecyclepkg.CleanupRoot,
	cause error,
) (lifecyclepkg.CleanupRoot, error) {
	if root.State == lifecyclepkg.CleanupRootCommitUnknown ||
		root.State == lifecyclepkg.CleanupRootPublished ||
		root.State == lifecyclepkg.CleanupRootDeletePending ||
		root.State == lifecyclepkg.CleanupRootDeleting ||
		root.State == lifecyclepkg.CleanupRootCleaned {
		return root, cause
	}
	cleanupCtx, cancelCleanup := lifecycleTemporaryCleanupContext(ctx)
	defer cancelCleanup()
	updated, transitionErr := processor.transition(
		cleanupCtx,
		root,
		lifecyclepkg.CleanupRootDeletePending,
		cause,
	)
	if transitionErr != nil {
		return updated, errors.Join(cause, transitionErr)
	}
	return updated, cause
}

func (processor *LifecycleProcessor) transition(
	ctx context.Context,
	root lifecyclepkg.CleanupRoot,
	to lifecyclepkg.CleanupRootState,
	cause error,
) (lifecyclepkg.CleanupRoot, error) {
	root.LastError = ""
	if cause != nil {
		root.LastError = cause.Error()
	}
	updated, updateErr := processor.Roots.UpdateCleanup(
		ctx,
		root,
		root.StateVersion,
	)
	if updateErr != nil {
		return root, updateErr
	}
	result, transitionErr := processor.transitionRoot(
		ctx,
		updated,
		to,
	)
	return result, transitionErr
}

func (processor *LifecycleProcessor) transitionRoot(
	ctx context.Context,
	root lifecyclepkg.CleanupRoot,
	to lifecyclepkg.CleanupRootState,
) (lifecyclepkg.CleanupRoot, error) {
	faults := processor.Faults
	if faults == nil {
		faults = lifecyclepkg.NoLifecycleFaults{}
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultBeforeRootCAS); err != nil {
		return root, err
	}
	updated, err := processor.Roots.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		root.State,
		root.StateVersion,
		to,
	)
	if err != nil {
		return root, err
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterRootCAS); err != nil {
		return updated, err
	}
	return updated, nil
}

func lifecycleSourceDigest(sources []objectio.ObjectEntry) [sha256.Size]byte {
	stats := make([]objectio.ObjectStats, len(sources))
	for index := range sources {
		stats[index] = sources[index].ObjectStats
	}
	slices.SortFunc(stats, func(left, right objectio.ObjectStats) int {
		return bytes.Compare(left.ObjectName(), right.ObjectName())
	})
	sum := sha256.New()
	_, _ = sum.Write([]byte("matrixone/lifecycle/data-sources/v1"))
	for index := range stats {
		_, _ = sum.Write(stats[index][:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], sum.Sum(nil))
	return digest
}
