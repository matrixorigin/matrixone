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

package lifecycle

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type CleanupPublicationState uint8

const (
	CleanupPublicationMissing CleanupPublicationState = iota
	CleanupPublicationPublished
	CleanupPublicationDeletePending
)

// CleanupReconcileCatalog is the tenant-Catalog side of Root reconciliation.
// It is intentionally read-mostly and only runs in the Lifecycle coordinator.
// Ordinary transactions, DML, queries, and Merge never implement this
// interface or access Cleanup Roots.
type CleanupReconcileCatalog interface {
	MatchingPublication(
		context.Context,
		CleanupRoot,
		time.Time,
	) (CleanupPublicationState, error)
	OwnerExists(context.Context, CleanupRoot) (bool, error)
	RequestCleanup(context.Context, CleanupRoot, time.Time) (bool, error)
}

type CleanupReconciler struct {
	Roots   CleanupRootRepository
	Catalog CleanupReconcileCatalog
}

func (reconciler CleanupReconciler) ReconcileOne(
	ctx context.Context,
	root CleanupRoot,
	now time.Time,
) (CleanupRoot, error) {
	if reconciler.Roots == nil || reconciler.Catalog == nil || now.IsZero() {
		return root, moerr.NewInternalErrorNoCtxf("Lifecycle Cleanup reconciler is incomplete")
	}

	switch root.State {
	case CleanupRootRegistered, CleanupRootUploading, CleanupRootVerified:
		if root.WorkerDeadline.IsZero() || now.Before(root.WorkerDeadline) {
			return root, nil
		}
		return reconciler.Roots.Transition(
			ctx,
			root.RootID,
			root.AttemptID,
			root.ExecutorEpoch,
			root.State,
			root.StateVersion,
			CleanupRootDeletePending,
		)

	case CleanupRootFinalizing:
		if root.WorkerDeadline.IsZero() || now.Before(root.WorkerDeadline) {
			return root, nil
		}
		// A final transaction may have been sent before its owner disappeared.
		// Never infer abort from a lease timeout.
		return reconciler.Roots.Transition(
			ctx,
			root.RootID,
			root.AttemptID,
			root.ExecutorEpoch,
			CleanupRootFinalizing,
			root.StateVersion,
			CleanupRootCommitUnknown,
		)

	case CleanupRootCommitUnknown:
		publication, err := reconciler.Catalog.MatchingPublication(ctx, root, now)
		if err != nil {
			return root, err
		}
		if publication != CleanupPublicationMissing {
			// Matching Dataset/Receipt proves that the final transaction
			// committed. TAE now owns every created live Object.
			root.SegmentID = ""
			root.OrdinalUpperBound = 0
			updated, err := reconciler.Roots.UpdateCleanup(
				ctx,
				root,
				root.StateVersion,
			)
			if err != nil {
				return root, err
			}
			updated, err = reconciler.Roots.Transition(
				ctx,
				updated.RootID,
				updated.AttemptID,
				updated.ExecutorEpoch,
				CleanupRootCommitUnknown,
				updated.StateVersion,
				CleanupRootPublished,
			)
			if err != nil {
				return updated, err
			}
			if publication == CleanupPublicationDeletePending &&
				updated.TemporaryCleanupDone {
				return reconciler.Roots.Transition(
					ctx,
					updated.RootID,
					updated.AttemptID,
					updated.ExecutorEpoch,
					CleanupRootPublished,
					updated.StateVersion,
					CleanupRootDeletePending,
				)
			}
			return updated, nil
		}
		// Absence of the owner does not prove that the final transaction
		// aborted. It may have committed before DROP removed the tenant
		// publication row, in which case TAE owns the created live Objects.
		// Without a generic authoritative MO transaction-status result, keep
		// the Root fail-closed and let the bounded UNKNOWN backlog stop new
		// Lifecycle work instead of deleting possibly committed files.
		return root, nil

	case CleanupRootPublished:
		if !root.TemporaryCleanupDone {
			return root, nil
		}
		cleanup, err := reconciler.Catalog.RequestCleanup(ctx, root, now)
		if err != nil {
			return root, err
		}
		if !cleanup {
			return root, nil
		}
		return reconciler.Roots.Transition(
			ctx,
			root.RootID,
			root.AttemptID,
			root.ExecutorEpoch,
			CleanupRootPublished,
			root.StateVersion,
			CleanupRootDeletePending,
		)

	default:
		return root, nil
	}
}
