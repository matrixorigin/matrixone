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
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type CleanupRootState string

const (
	CleanupRootRegistered    CleanupRootState = "REGISTERED"
	CleanupRootUploading     CleanupRootState = "UPLOADING"
	CleanupRootVerified      CleanupRootState = "VERIFIED"
	CleanupRootFinalizing    CleanupRootState = "FINALIZING"
	CleanupRootPublished     CleanupRootState = "PUBLISHED"
	CleanupRootCommitUnknown CleanupRootState = "COMMIT_UNKNOWN"
	CleanupRootDeletePending CleanupRootState = "DELETE_PENDING"
	CleanupRootDeleting      CleanupRootState = "DELETING"
	CleanupRootCleaned       CleanupRootState = "CLEANED"
)

type CleanupMode string

const (
	CleanupModeArchiveWhole   CleanupMode = "ARCHIVE_WHOLE"
	CleanupModeArchiveRewrite CleanupMode = "ARCHIVE_REWRITE"
	CleanupModeTTLRewrite     CleanupMode = "TTL_REWRITE"
	cleanupRetryBackoff                   = 5 * time.Minute
	maxCleanupLastErrorBytes              = 4096
)

type CleanupRoot struct {
	RootID          string
	AttemptID       string
	Mode            CleanupMode
	OwnerAccountID  uint32
	LogicalTableID  uint64
	PhysicalTableID uint64
	ExecutorEpoch   uint64
	WorkerDeadline  time.Time

	ArchiveNamespace string
	CredentialHandle string
	ArchivePrefix    string
	ManifestKey      string
	ManifestDigest   [32]byte

	TAENamespace      string
	SegmentID         string
	BookingPrefix     string
	OrdinalUpperBound uint32
	// ReservedCleanupBytes is the worst-case physical footprint this Root is
	// allowed to create while it can become cleanup backlog. It is frozen
	// before the first side effect; writers must fail before exceeding it.
	ReservedCleanupBytes uint64

	SourceSetDigest      [32]byte
	FinalTxnID           string
	State                CleanupRootState
	StateVersion         uint64
	CleanupAfter         time.Time
	TemporaryCleanupDone bool
	QuiescenceSince      time.Time
	LastListAt           time.Time
	LastError            string
}

func ValidateCleanupRoot(root CleanupRoot) error {
	if root.RootID == "" || root.AttemptID == "" {
		return fmt.Errorf("Lifecycle cleanup root identity is incomplete")
	}
	if root.StateVersion == 0 || root.CleanupAfter.IsZero() ||
		root.ReservedCleanupBytes == 0 {
		return fmt.Errorf("Lifecycle cleanup root state identity is incomplete")
	}
	switch root.Mode {
	case CleanupModeArchiveWhole:
		if !lifecycleRootScopedPrefix(
			root.ArchivePrefix,
			root.RootID,
			root.AttemptID,
		) {
			return fmt.Errorf("Lifecycle Archive prefix is not Root scoped")
		}
	case CleanupModeArchiveRewrite:
		if !lifecycleRootScopedPrefix(
			root.ArchivePrefix,
			root.RootID,
			root.AttemptID,
		) {
			return fmt.Errorf("Lifecycle Rewrite namespaces are not Root scoped")
		}
		if root.TemporaryCleanupDone {
			if root.BookingPrefix != "" ||
				root.SegmentID != "" ||
				root.OrdinalUpperBound != 0 {
				return fmt.Errorf(
					"Lifecycle completed temporary cleanup still owns TAE files",
				)
			}
		} else if !lifecycleRootScopedPrefix(
			root.BookingPrefix,
			root.RootID,
			root.AttemptID,
		) {
			return fmt.Errorf("Lifecycle Rewrite Booking prefix is not Root scoped")
		}
	case CleanupModeTTLRewrite:
		if root.TemporaryCleanupDone {
			if root.BookingPrefix != "" ||
				root.SegmentID != "" ||
				root.OrdinalUpperBound != 0 {
				return fmt.Errorf(
					"Lifecycle completed temporary cleanup still owns TAE files",
				)
			}
		} else if !lifecycleRootScopedPrefix(
			root.BookingPrefix,
			root.RootID,
			root.AttemptID,
		) {
			return fmt.Errorf("Lifecycle TTL Rewrite namespace is not Root scoped")
		}
	default:
		return fmt.Errorf("unknown Lifecycle cleanup mode %s", root.Mode)
	}
	return nil
}

func lifecycleRootScopedPrefix(prefix, rootID, attemptID string) bool {
	if prefix == "" || rootID == "" || attemptID == "" ||
		strings.HasPrefix(prefix, "/") ||
		strings.HasSuffix(prefix, "/") ||
		path.Clean(prefix) != prefix {
		return false
	}
	parts := strings.Split(prefix, "/")
	for index := 0; index+1 < len(parts); index++ {
		if parts[index] == rootID && parts[index+1] == attemptID {
			return true
		}
	}
	return false
}

func cleanupKeyWithinPrefix(key, prefix string) bool {
	if key == "" || prefix == "" ||
		strings.HasPrefix(key, "/") ||
		path.Clean(key) != key ||
		path.Clean(prefix) != prefix {
		return false
	}
	return strings.HasPrefix(key, strings.TrimSuffix(prefix, "/")+"/")
}

type CleanupRootRepository interface {
	Register(ctx context.Context, root CleanupRoot) error
	Get(ctx context.Context, rootID string) (CleanupRoot, error)
	HasUnresolvedSource(
		ctx context.Context,
		ownerAccountID uint32,
		physicalTableID uint64,
		sourceSetDigest [32]byte,
	) (bool, error)
	Transition(
		ctx context.Context,
		rootID string,
		attemptID string,
		executorEpoch uint64,
		from CleanupRootState,
		expectedVersion uint64,
		to CleanupRootState,
	) (CleanupRoot, error)
	UpdateCleanup(
		ctx context.Context,
		root CleanupRoot,
		expectedVersion uint64,
	) (CleanupRoot, error)
}

// CleanupCapacityChecker is advisory admission for Lifecycle workers. Root
// count, scheduler concurrency, and per-Root reservation together provide a
// deterministic upper bound; this is intentionally not a distributed lock or
// a new transaction protocol.
type CleanupCapacityChecker interface {
	CheckCreateCapacity(
		context.Context,
		int,
		uint64,
		uint64,
	) error
}

type cleanupRootSideEffectGuard struct {
	roots CleanupRootRepository
}

func NewCleanupRootSideEffectGuard(
	roots CleanupRootRepository,
) ArchiveSideEffectGuard {
	return &cleanupRootSideEffectGuard{roots: roots}
}

func (guard *cleanupRootSideEffectGuard) EnsureDurable(
	ctx context.Context,
	rootID string,
	attemptID string,
) error {
	root, err := guard.roots.Get(ctx, rootID)
	if err != nil {
		return ErrCleanupRootNotDurable
	}
	if root.AttemptID != attemptID ||
		(root.State != CleanupRootRegistered &&
			root.State != CleanupRootUploading) {
		return ErrCleanupRootNotDurable
	}
	return nil
}

func validateCleanupRootTransition(from, to CleanupRootState) bool {
	switch from {
	case CleanupRootRegistered:
		return to == CleanupRootUploading || to == CleanupRootDeletePending
	case CleanupRootUploading:
		return to == CleanupRootVerified || to == CleanupRootDeletePending
	case CleanupRootVerified:
		return to == CleanupRootFinalizing || to == CleanupRootDeletePending
	case CleanupRootFinalizing:
		return to == CleanupRootPublished ||
			to == CleanupRootCommitUnknown ||
			to == CleanupRootDeletePending
	case CleanupRootCommitUnknown:
		return to == CleanupRootPublished || to == CleanupRootDeletePending
	case CleanupRootPublished:
		return to == CleanupRootDeletePending
	case CleanupRootDeletePending:
		return to == CleanupRootDeleting
	case CleanupRootDeleting:
		return to == CleanupRootCleaned
	default:
		return false
	}
}

func CanSweepCleanupRoot(root CleanupRoot) bool {
	return root.State == CleanupRootDeletePending ||
		root.State == CleanupRootDeleting
}

type CleanupObjectStore interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// CleanupPublishedTemporary removes only Root-owned temporary files after a
// successful final commit. The live Object segment must already have been
// transferred to TAE ownership and cleared from the Root.
func CleanupPublishedTemporary(
	ctx context.Context,
	roots CleanupRootRepository,
	store CleanupObjectStore,
	root CleanupRoot,
) (CleanupRoot, error) {
	if roots == nil {
		return root, fmt.Errorf("Lifecycle Cleanup Root repository is nil")
	}
	if err := ValidateCleanupRoot(root); err != nil {
		return root, err
	}
	if root.State != CleanupRootPublished {
		return root, fmt.Errorf(
			"Lifecycle temporary cleanup requires PUBLISHED Root, got %s",
			root.State,
		)
	}
	if root.TemporaryCleanupDone {
		return root, nil
	}
	if root.SegmentID != "" || root.OrdinalUpperBound != 0 {
		return root, fmt.Errorf(
			"Lifecycle live Object ownership was not transferred to TAE",
		)
	}
	if root.BookingPrefix != "" {
		if store == nil {
			return root, fmt.Errorf("Lifecycle temporary cleanup store is nil")
		}
		keys, err := listCleanupKeys(ctx, store, root.BookingPrefix)
		if err != nil {
			return root, err
		}
		for _, key := range keys {
			if err := store.Delete(ctx, key); err != nil {
				return root, err
			}
		}
		remaining, err := listCleanupKeys(ctx, store, root.BookingPrefix)
		if err != nil {
			return root, err
		}
		if len(remaining) != 0 {
			return root, fmt.Errorf(
				"Lifecycle temporary cleanup still has %d Booking files",
				len(remaining),
			)
		}
	}
	root.BookingPrefix = ""
	root.TemporaryCleanupDone = true
	root.LastError = ""
	return roots.UpdateCleanup(ctx, root, root.StateVersion)
}

type CleanupSweeper struct {
	Roots          CleanupRootRepository
	Archive        CleanupObjectStore
	TAE            CleanupObjectStore
	ResolveArchive func(context.Context, CleanupRoot) (CleanupObjectStore, error)
	ResolveTAE     func(context.Context, CleanupRoot) (CleanupObjectStore, error)
	// FinalizePublication advances the tenant Dataset only after Provider and
	// TAE-owned temporary files have passed the empty quiescence window. An
	// error keeps the Root in DELETING so the ordinary coordinator retry can
	// converge without recreating or re-uploading the Archive.
	FinalizePublication func(context.Context, CleanupRoot) error
	QuiescenceWindow    time.Duration
	Faults              FaultInjector
}

// DeferCleanupRoot prevents one permanently failing provider/credential item
// from monopolizing the bounded oldest-first cleanup page. It changes only the
// next retry time and diagnostics; ownership and state remain unchanged.
func DeferCleanupRoot(
	ctx context.Context,
	roots CleanupRootRepository,
	rootID string,
	now time.Time,
	cause error,
) (CleanupRoot, error) {
	if roots == nil || rootID == "" || now.IsZero() || cause == nil {
		return CleanupRoot{}, fmt.Errorf("Lifecycle cleanup deferral is incomplete")
	}
	root, err := roots.Get(ctx, rootID)
	if err != nil {
		return CleanupRoot{}, err
	}
	if !CanSweepCleanupRoot(root) {
		return root, nil
	}
	retryAt := now.Add(cleanupRetryBackoff)
	if root.CleanupAfter.Before(retryAt) {
		root.CleanupAfter = retryAt
	}
	root.LastError = cause.Error()
	if len(root.LastError) > maxCleanupLastErrorBytes {
		root.LastError = root.LastError[:maxCleanupLastErrorBytes]
	}
	return roots.UpdateCleanup(ctx, root, root.StateVersion)
}

func (sweeper CleanupSweeper) SweepOne(
	ctx context.Context,
	rootID string,
	now time.Time,
) error {
	if sweeper.Roots == nil || sweeper.QuiescenceWindow <= 0 {
		return fmt.Errorf("Lifecycle cleanup sweeper is not configured")
	}
	root, err := sweeper.Roots.Get(ctx, rootID)
	if err != nil {
		return err
	}
	// Root metadata is the authority for destructive external cleanup. Recheck
	// its immutable namespace after every durable read so Catalog corruption or
	// a future decoder bug fails closed before any LIST or DELETE.
	if err := ValidateCleanupRoot(root); err != nil {
		return err
	}
	if !CanSweepCleanupRoot(root) || now.Before(root.CleanupAfter) {
		return nil
	}
	if root.State == CleanupRootDeletePending {
		root, err = sweeper.transitionRoot(ctx, root, CleanupRootDeleting)
		if err != nil {
			return err
		}
	}

	archive, err := sweeper.archiveStore(ctx, root)
	if err != nil {
		return err
	}
	tae, err := sweeper.taeStore(ctx, root)
	if err != nil {
		return err
	}
	faults := sweeper.Faults
	if faults == nil {
		faults = NoLifecycleFaults{}
	}
	if err := faults.Inject(ctx, FaultBeforeCleanupList); err != nil {
		return err
	}
	keys, err := listCleanupKeys(ctx, archive, root.ArchivePrefix)
	if err != nil {
		return err
	}
	if tae != nil && root.BookingPrefix != "" {
		bookingKeys, listErr := listCleanupKeys(ctx, tae, root.BookingPrefix)
		if listErr != nil {
			return listErr
		}
		keys = append(keys, bookingKeys...)
	}
	root.LastListAt = now
	if len(keys) > 0 {
		if err := faults.Inject(ctx, FaultBeforeCleanupDelete); err != nil {
			return err
		}
		for _, key := range keys {
			store := archive
			if root.BookingPrefix != "" &&
				cleanupKeyWithinPrefix(key, root.BookingPrefix) {
				store = tae
			}
			if store == nil {
				return fmt.Errorf("Lifecycle cleanup store is unavailable for %s", key)
			}
			if err := store.Delete(ctx, key); err != nil {
				return err
			}
			// Model a provider delete that completed but whose response was
			// lost before the Root progress update. A retry must reconcile
			// from LIST and converge without assuming the key still exists.
			if err := faults.Inject(ctx, FaultAfterCleanupDelete); err != nil {
				return err
			}
		}
		root.QuiescenceSince = time.Time{}
		_, err = sweeper.Roots.UpdateCleanup(ctx, root, root.StateVersion)
		return err
	}
	if err := faults.Inject(ctx, FaultBeforeCleanupDelete); err != nil {
		return err
	}
	if err := deleteLifecycleLiveStaging(ctx, tae, root); err != nil {
		return err
	}
	if err := faults.Inject(ctx, FaultAfterCleanupDelete); err != nil {
		return err
	}
	if root.QuiescenceSince.IsZero() {
		root.QuiescenceSince = now
		_, err = sweeper.Roots.UpdateCleanup(ctx, root, root.StateVersion)
		return err
	}
	if now.Sub(root.QuiescenceSince) < sweeper.QuiescenceWindow {
		_, err = sweeper.Roots.UpdateCleanup(ctx, root, root.StateVersion)
		return err
	}
	if sweeper.FinalizePublication != nil {
		if err := sweeper.FinalizePublication(ctx, root); err != nil {
			return err
		}
	}
	_, err = sweeper.transitionRoot(ctx, root, CleanupRootCleaned)
	return err
}

func (sweeper CleanupSweeper) transitionRoot(
	ctx context.Context,
	root CleanupRoot,
	to CleanupRootState,
) (CleanupRoot, error) {
	faults := sweeper.Faults
	if faults == nil {
		faults = NoLifecycleFaults{}
	}
	if err := faults.Inject(ctx, FaultBeforeRootCAS); err != nil {
		return root, err
	}
	updated, err := sweeper.Roots.Transition(
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
	if err := faults.Inject(ctx, FaultAfterRootCAS); err != nil {
		return updated, err
	}
	return updated, nil
}

func (sweeper CleanupSweeper) archiveStore(
	ctx context.Context,
	root CleanupRoot,
) (CleanupObjectStore, error) {
	if sweeper.ResolveArchive != nil {
		return sweeper.ResolveArchive(ctx, root)
	}
	if root.ArchivePrefix == "" {
		return nil, nil
	}
	if sweeper.Archive == nil {
		return nil, fmt.Errorf("Lifecycle Archive cleanup store is nil")
	}
	return sweeper.Archive, nil
}

func (sweeper CleanupSweeper) taeStore(
	ctx context.Context,
	root CleanupRoot,
) (CleanupObjectStore, error) {
	if sweeper.ResolveTAE != nil {
		return sweeper.ResolveTAE(ctx, root)
	}
	if root.BookingPrefix == "" && root.SegmentID == "" {
		return nil, nil
	}
	if sweeper.TAE == nil {
		return nil, fmt.Errorf("Lifecycle TAE cleanup store is nil")
	}
	return sweeper.TAE, nil
}

func listCleanupKeys(
	ctx context.Context,
	store CleanupObjectStore,
	prefix string,
) ([]string, error) {
	if prefix == "" {
		return nil, nil
	}
	if store == nil {
		return nil, fmt.Errorf("Lifecycle cleanup store is nil")
	}
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if !cleanupKeyWithinPrefix(key, prefix) {
			return nil, fmt.Errorf(
				"Lifecycle cleanup key %q is outside prefix %q",
				key,
				prefix,
			)
		}
	}
	return keys, nil
}

func deleteLifecycleLiveStaging(
	ctx context.Context,
	store CleanupObjectStore,
	root CleanupRoot,
) error {
	if root.SegmentID == "" || root.OrdinalUpperBound == 0 {
		return nil
	}
	if store == nil {
		return fmt.Errorf("Lifecycle TAE staging cleanup store is nil")
	}
	segmentID, err := types.ParseUuid(root.SegmentID)
	if err != nil {
		return fmt.Errorf("invalid Lifecycle staging Segment ID: %w", err)
	}
	for ordinal := uint32(0); ordinal < root.OrdinalUpperBound; ordinal++ {
		if ordinal > uint32(^uint16(0)) {
			return fmt.Errorf("Lifecycle staging Object ordinal overflows uint16")
		}
		name := objectio.BuildObjectName(&segmentID, uint16(ordinal)).String()
		if err := store.Delete(ctx, name); err != nil {
			return err
		}
	}
	return nil
}
