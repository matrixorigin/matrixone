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
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
)

// ProcessTTLObject applies DELETE lifecycle without creating Archive payload.
// Whole retirement has no external side effect and therefore no Cleanup Root.
// Mixed retirement reuses the same single-source Rewrite producer and creates
// one Root solely for live staging and external booking ownership.
func (processor *LifecycleProcessor) ProcessTTLObject(
	ctx context.Context,
	task LifecycleObjectTask,
) (result lifecyclepkg.CleanupRoot, err error) {
	if err := processor.validateTTLTask(ctx, task); err != nil {
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
	attemptID := id()
	finalTxnID := id()
	sourceSetDigest := lifecycleSourceDigest(task.Sources)
	if !task.Whole {
		unresolved, checkErr := processor.Roots.HasUnresolvedSource(
			ctx,
			task.Binding.AccountID,
			task.Binding.PhysicalTableID,
			sourceSetDigest,
		)
		if checkErr != nil {
			return lifecyclepkg.CleanupRoot{}, checkErr
		}
		if unresolved {
			return lifecyclepkg.CleanupRoot{}, moerr.NewInternalErrorNoCtxf(
				"Lifecycle table has an unresolved final transaction",
			)
		}
		if processor.RewriteAdmission != nil {
			if err := processor.RewriteAdmission.ReserveSource(
				task.Binding.AccountID,
				lifecycleObjectPressureBytes(task.Sources[0].ObjectStats),
				task.Now,
			); err != nil {
				return lifecyclepkg.CleanupRoot{}, err
			}
		}
	}
	protection, err := task.Table.LifecycleSelectProtectionSet(
		ctx,
		task.SourceSnapshot,
		task.Sources,
		task.ProtectionLimits,
	)
	if err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	if !bytes.Equal(protection.SourceSetDigest[:], sourceSetDigest[:]) {
		return lifecyclepkg.CleanupRoot{}, moerr.NewInternalErrorNoCtxf(
			"Lifecycle protected source set changed",
		)
	}
	lease, err := lifecyclepkg.AcquireProtection(
		ctx,
		processor.Protection,
		attemptID,
		protection,
		task.Deadline,
	)
	if err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	defer func() {
		releaseCtx, cancelRelease := lifecycleProtectionReleaseContext(ctx)
		defer cancelRelease()
		err = errors.Join(
			err,
			lease.Release(releaseCtx),
		)
	}()
	if err := faults.Inject(ctx, lifecyclepkg.FaultAfterProtection); err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}
	if err := faults.Inject(ctx, lifecyclepkg.FaultBeforeSourceRead); err != nil {
		return lifecyclepkg.CleanupRoot{}, err
	}

	if task.Whole {
		expiredRows, retiredBytes, measureErr := processor.measureWholeTTL(
			ctx,
			task,
		)
		if errors.Is(measureErr, ErrLifecycleNoExpiredRows) {
			return lifecyclepkg.CleanupRoot{}, nil
		}
		if measureErr != nil {
			return lifecyclepkg.CleanupRoot{}, measureErr
		}
		controlRoot := lifecyclepkg.CleanupRoot{
			AttemptID:       attemptID,
			SourceSetDigest: sourceSetDigest,
		}
		control := processor.commitControl(task, controlRoot, LifecycleRewriteResult{})
		if err := faults.Inject(
			ctx,
			lifecyclepkg.FaultBeforeFinalCommit,
		); err != nil {
			return lifecyclepkg.CleanupRoot{}, err
		}
		if err := processor.Finalizer.Finalize(
			ctx,
			LifecycleFinalizeRequest{
				Binding:             task.Binding,
				Control:             control,
				SyncProtectionJobID: lease.JobID(),
				FinalTxnID:          finalTxnID,
				Cutoff:              task.Cutoff,
				EvaluationTime:      task.Now,
				ExpiredRows:         expiredRows,
				RetiredBytes:        retiredBytes,
			},
		); err != nil {
			return lifecyclepkg.CleanupRoot{}, err
		}
		if task.OnFinalCommit != nil {
			task.OnFinalCommit()
		}
		if err := faults.Inject(
			ctx,
			lifecyclepkg.FaultAfterFinalCommit,
		); err != nil {
			return lifecyclepkg.CleanupRoot{}, err
		}
		return lifecyclepkg.CleanupRoot{}, nil
	}

	rootID := id()
	reservedCleanupBytes, _, err := lifecycleCleanupReservation(task, false)
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
	bookingPrefix := path.Join(
		"lifecycle-staging",
		rootID,
		attemptID,
		"booking",
	)
	liveSegmentID := *objectio.NewSegmentid()
	root := lifecyclepkg.CleanupRoot{
		RootID:               rootID,
		AttemptID:            attemptID,
		Mode:                 lifecyclepkg.CleanupModeTTLRewrite,
		OwnerAccountID:       task.Binding.AccountID,
		LogicalTableID:       task.Binding.LogicalTableID,
		PhysicalTableID:      task.Binding.PhysicalTableID,
		ExecutorEpoch:        task.ExecutorEpoch,
		WorkerDeadline:       task.Deadline,
		TAENamespace:         processor.Config.TAENamespace,
		SegmentID:            liveSegmentID.String(),
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
	}
	if err := processor.Roots.Register(ctx, root); err != nil {
		return root, err
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

	encoder := lifecyclepkg.NewCanonicalBatchEncoder(task.SchemaDigest)
	if err := faults.Inject(
		ctx,
		lifecyclepkg.FaultBeforeRewriteStaging,
	); err != nil {
		return processor.abandon(ctx, root, err)
	}
	rewrite, err := task.Table.LifecycleRewriteObject(
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
				return encoder.WriteBatch(callbackCtx, value, expired)
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
	if errors.Is(err, ErrLifecycleNoExpiredRows) {
		return processor.completeNoop(ctx, root)
	}
	if errors.Is(err, ErrLifecycleRewriteHasNoLiveRows) {
		task.Whole = true
		rewrite.CreatedObjectStats = nil
		rewrite.TransferBookingLocation = nil
		rewrite.TransferMappingDigest = [32]byte{}
		rewrite.MergeLevel = 0
		err = nil
	}
	if err != nil {
		return processor.abandon(ctx, root, err)
	}
	if err := rewrite.ScanReport.ValidateComplete(); err != nil {
		return processor.abandon(ctx, root, err)
	}
	if rewrite.ScanReport.ExpiredRows == 0 {
		return processor.completeNoop(ctx, root)
	}
	if !task.Whole {
		if err := validateLifecycleRewriteOwnership(root, rewrite); err != nil {
			return processor.abandon(ctx, root, err)
		}
	}
	if err := faults.Inject(
		ctx,
		lifecyclepkg.FaultAfterRewriteStaging,
	); err != nil {
		return processor.abandon(ctx, root, err)
	}
	if encoder.RowCount() == 0 ||
		encoder.RowCount() != rewrite.ScanReport.ExpiredRows {
		return processor.abandon(
			ctx,
			root,
			moerr.NewInternalErrorNoCtxf(
				"Lifecycle TTL measured %d of %d expired rows",
				encoder.RowCount(),
				rewrite.ScanReport.ExpiredRows,
			),
		)
	}
	if processor.RewriteAdmission != nil {
		sourceBytes := lifecycleObjectPressureBytes(task.Sources[0].ObjectStats)
		retiredBytes, estimateErr := lifecycleEstimatedExpiredPressureBytes(
			sourceBytes,
			rewrite.ScanReport,
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
	if err := faults.Inject(
		ctx,
		lifecyclepkg.FaultBeforeFinalCommit,
	); err != nil {
		return processor.abandon(ctx, root, err)
	}
	control := processor.commitControl(task, root, rewrite)
	if err := processor.Finalizer.Finalize(
		ctx,
		LifecycleFinalizeRequest{
			Root:                root,
			Binding:             task.Binding,
			Control:             control,
			SyncProtectionJobID: lease.JobID(),
			FinalTxnID:          finalTxnID,
			Cutoff:              task.Cutoff,
			EvaluationTime:      task.Now,
			ExpiredRows:         encoder.RowCount(),
			RetiredBytes:        encoder.LogicalBytes(),
		},
	); err != nil {
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

	root.SegmentID = ""
	root.OrdinalUpperBound = 0
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
	if err := faults.Inject(
		ctx,
		lifecyclepkg.FaultAfterFinalCommit,
	); err != nil {
		return root, err
	}
	cleanupCtx, cancelCleanup := lifecycleTemporaryCleanupContext(ctx)
	defer cancelCleanup()
	root, err = lifecyclepkg.CleanupPublishedTemporary(
		cleanupCtx,
		processor.Roots,
		processor.TemporaryStore,
		root,
	)
	if err != nil {
		root.LastError = err.Error()
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
	return processor.transitionRoot(
		cleanupCtx,
		root,
		lifecyclepkg.CleanupRootDeletePending,
	)
}

func (processor *LifecycleProcessor) measureWholeTTL(
	ctx context.Context,
	task LifecycleObjectTask,
) (uint64, uint64, error) {
	encoder := lifecyclepkg.NewCanonicalBatchEncoder(task.SchemaDigest)
	for _, source := range task.Sources {
		var sourceExpiredRows uint64
		report, err := task.Table.LifecycleReadObject(
			ctx,
			task.SourceSnapshot,
			source.ObjectStats,
			task.MaxCertifiedBlockReadBytes,
			func(value *batch.Batch, snapshotDeleted *nulls.Nulls) error {
				expired, err := task.Classifier(ctx, value, snapshotDeleted)
				if err != nil {
					return err
				}
				var visible uint64
				for row := 0; row < value.RowCount(); row++ {
					if snapshotDeleted == nil ||
						!snapshotDeleted.Contains(uint64(row)) {
						visible++
					}
				}
				if uint64(expired.Count()) != visible {
					return moerr.NewInternalErrorNoCtxf(
						"Lifecycle Whole TTL source contains live rows",
					)
				}
				sourceExpiredRows += uint64(expired.Count())
				return encoder.WriteBatch(ctx, value, expired)
			},
		)
		if err != nil {
			return 0, 0, err
		}
		if err := report.SetVisibleClassification(sourceExpiredRows, 0); err != nil {
			return 0, 0, err
		}
		if err := report.ValidateComplete(); err != nil {
			return 0, 0, err
		}
	}
	if encoder.RowCount() == 0 {
		return 0, 0, ErrLifecycleNoExpiredRows
	}
	return encoder.RowCount(), encoder.LogicalBytes(), nil
}

func (processor *LifecycleProcessor) validateTTLTask(
	ctx context.Context,
	task LifecycleObjectTask,
) error {
	if processor.Protection == nil ||
		processor.Finalizer == nil ||
		task.Table == nil ||
		task.SourceSnapshot.IsEmpty() ||
		len(task.Sources) == 0 ||
		task.ReceiptID == "" ||
		task.DatasetID != "" ||
		task.Classifier == nil ||
		task.Cutoff.IsZero() ||
		task.Binding.AccountID == 0 ||
		task.Binding.DatabaseID == 0 ||
		task.Binding.LogicalTableID == 0 ||
		task.Binding.PhysicalTableID == 0 ||
		task.Binding.Generation == 0 ||
		task.Binding.Version == 0 ||
		task.ExecutorEpoch == 0 ||
		task.MaxCertifiedBlockReadBytes == 0 ||
		task.Now.IsZero() ||
		!task.Deadline.After(task.Now) {
		return moerr.NewInvalidInput(ctx, "Lifecycle TTL Object task is incomplete")
	}
	if task.Whole && len(task.Sources) > 64 {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle Whole TTL source set exceeds certified limit",
		)
	}
	if !task.Whole && (processor.Roots == nil ||
		processor.CleanupCapacity == nil ||
		processor.TemporaryStore == nil ||
		processor.Config.TAENamespace == "" ||
		processor.Config.MaxActiveCleanupRoots <= 0 ||
		processor.Config.MaxActiveCleanupBytes == 0 ||
		processor.Config.CleanupGrace < 0 ||
		len(task.Sources) != 1 ||
		task.MaxCreatedObjects == 0 ||
		task.DeltaRows == 0 ||
		task.DeltaBytes == 0 ||
		task.DeltaBlocks == 0) {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle TTL Rewrite task is incomplete",
		)
	}
	digest, err := task.Schema.Digest()
	if err != nil {
		return err
	}
	if digest != task.SchemaDigest {
		return moerr.NewInvalidInput(ctx, "Lifecycle TTL schema digest changed")
	}
	if !strings.EqualFold(
		task.Binding.SchemaDigest,
		hex.EncodeToString(task.BindingSchemaDigest[:]),
	) {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle TTL Binding schema digest changed",
		)
	}
	return nil
}
