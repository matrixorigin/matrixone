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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRestoreCoordinatorImportsStableChunksAndPublishes(t *testing.T) {
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)
	manifest, err := ReadArchiveManifest(context.Background(), store, manifestKey)
	require.NoError(t, err)
	repository := newMemoryRestoreRepository()
	coordinator := RestoreCoordinator{
		Store:      store,
		Repository: repository,
		Config: RestoreConfig{
			MaxChunkRows:         10,
			MaxChunkLogicalBytes: 1 << 20,
			Deadline:             time.Minute,
		},
	}
	attempt := RestoreAttempt{
		RestoreID:          "restore-1",
		LeaseID:            "lease-1",
		StagingDatabaseID:  7,
		StagingTableID:     8,
		HiddenName:         "__mo_lifecycle_restore_1",
		TargetDatabaseID:   7,
		TargetDatabaseName: "restore_db",
		TargetName:         "events_history",
	}
	dataset := restoreTestDataset(t, "dataset-1", manifestKey, manifest)
	err = coordinator.Restore(context.Background(), dataset, attempt)
	require.NoError(t, err)
	require.True(t, repository.published)
	require.Len(t, repository.receipts, int(manifest.TotalChunkCount))
	require.Len(t, repository.rows, int(manifest.RowCount))

	// A takeover sees committed receipts and does not insert the rows twice.
	err = coordinator.Restore(context.Background(), dataset, attempt)
	require.NoError(t, err)
	require.Len(t, repository.rows, int(manifest.RowCount))
}

func TestRestoreRejectsDatasetManifestIdentityMismatch(t *testing.T) {
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)
	manifest, err := ReadArchiveManifest(context.Background(), store, manifestKey)
	require.NoError(t, err)
	dataset := restoreTestDataset(t, "dataset-identity", manifestKey, manifest)
	require.NoError(t, validateRestoreDatasetManifestIdentity(dataset, manifest))

	for _, test := range []struct {
		name   string
		mutate func(*RestoreDataset, *ArchiveManifest)
	}{
		{
			name: "root",
			mutate: func(value *RestoreDataset, _ *ArchiveManifest) {
				value.RootID = "other-root"
			},
		},
		{
			name: "attempt",
			mutate: func(value *RestoreDataset, _ *ArchiveManifest) {
				value.AttemptID = "other-attempt"
			},
		},
		{
			name: "manifest digest",
			mutate: func(value *RestoreDataset, _ *ArchiveManifest) {
				value.ManifestDigest[0] ^= 1
			},
		},
		{
			name: "schema digest",
			mutate: func(value *RestoreDataset, _ *ArchiveManifest) {
				value.SchemaDigest[0] ^= 1
			},
		},
		{
			name: "verification",
			mutate: func(_ *RestoreDataset, value *ArchiveManifest) {
				value.VerificationStatus = "SOURCE_ENCODED"
			},
		},
		{
			name: "payload namespace",
			mutate: func(_ *RestoreDataset, value *ArchiveManifest) {
				value.Files[0].Key = "archive/other/root/payload.parquet"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			copyDataset := dataset
			copyManifest := *manifest
			copyManifest.Files = append([]ArchiveFile(nil), manifest.Files...)
			test.mutate(&copyDataset, &copyManifest)
			require.Error(t, validateRestoreDatasetManifestIdentity(
				copyDataset,
				&copyManifest,
			))
		})
	}
}

func TestRestoreDeadlineCoversManifestRead(t *testing.T) {
	deadline := time.Now().Add(time.Minute).Round(0)
	store := &restoreDeadlineStore{err: errors.New("stop after deadline capture")}
	coordinator := RestoreCoordinator{
		Store:      store,
		Repository: newMemoryRestoreRepository(),
		Config: RestoreConfig{
			MaxChunkRows:         10,
			MaxChunkLogicalBytes: 1 << 20,
			Deadline:             time.Hour,
		},
	}
	err := coordinator.Restore(
		context.Background(),
		RestoreDataset{
			DatasetID:   "dataset-deadline",
			ManifestKey: "manifest-deadline",
			State:       "PUBLISHED",
		},
		RestoreAttempt{
			RestoreID:  "restore-deadline",
			HiddenName: "__mo_lifecycle_restore_deadline",
			TargetName: "restored_deadline",
			Deadline:   deadline,
		},
	)
	require.ErrorIs(t, err, store.err)
	require.True(t, deadline.Equal(store.deadline))
}

type restoreDeadlineStore struct {
	deadline time.Time
	err      error
}

func (*restoreDeadlineStore) Put(context.Context, string, []byte) error {
	return nil
}

func (store *restoreDeadlineStore) Get(ctx context.Context, _ string) ([]byte, error) {
	store.deadline, _ = ctx.Deadline()
	return nil, store.err
}

func (store *restoreDeadlineStore) Stat(ctx context.Context, _ string) (int64, error) {
	store.deadline, _ = ctx.Deadline()
	return 0, store.err
}

func (store *restoreDeadlineStore) GetExact(
	ctx context.Context,
	_ string,
	_ int64,
) ([]byte, error) {
	store.deadline, _ = ctx.Deadline()
	return nil, store.err
}

func TestRestoreChunkOrdinalRejectsDifferentDigest(t *testing.T) {
	repository := newMemoryRestoreRepository()
	attempt := RestoreAttempt{
		RestoreID:         "restore-2",
		DatasetID:         "dataset-2",
		LeaseID:           "lease-2",
		Deadline:          time.Now().Add(time.Minute),
		HiddenName:        "__hidden",
		TargetName:        "target",
		StagingTableID:    10,
		StagingDatabaseID: 11,
	}
	repository.attempts[attempt.RestoreID] = attempt
	first := RestoreChunkReceipt{RestoreID: attempt.RestoreID, ChunkOrdinal: 0}
	first.ChunkDigest[0] = 1
	_, err := repository.ImportChunk(
		context.Background(),
		attempt,
		first,
		SchemaDescriptor{},
		nil,
	)
	require.NoError(t, err)
	second := first
	second.ChunkDigest[0] = 2
	_, err = repository.ImportChunk(
		context.Background(),
		attempt,
		second,
		SchemaDescriptor{},
		nil,
	)
	require.Error(t, err)
}

func TestPurgeRefusesActiveRestoreLease(t *testing.T) {
	repository := newMemoryRestoreRepository()
	coordinator := RestoreCoordinator{Repository: repository}
	now := time.Now()
	err := coordinator.Purge(context.Background(), RestoreDataset{
		DatasetID:       "dataset",
		Version:         1,
		RestoreLeaseID:  "lease",
		RestoreDeadline: now.Add(time.Minute),
	}, now)
	require.ErrorIs(t, err, ErrRestoreInProgress)
	require.Zero(t, repository.purgeCount)
}

func TestPurgeRefusesExpiredRestoreLeaseUntilCleanupReleasesIt(t *testing.T) {
	repository := newMemoryRestoreRepository()
	coordinator := RestoreCoordinator{Repository: repository}
	now := time.Now()
	err := coordinator.Purge(context.Background(), RestoreDataset{
		DatasetID:       "dataset",
		Version:         1,
		RestoreLeaseID:  "expired-lease",
		RestoreDeadline: now.Add(-time.Minute),
	}, now)
	require.ErrorIs(t, err, ErrRestoreInProgress)
	require.Zero(t, repository.purgeCount)
}

func TestRestoreFaultAfterCommittedChunkResumesWithoutDuplicateRows(t *testing.T) {
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)
	manifest, err := ReadArchiveManifest(context.Background(), store, manifestKey)
	require.NoError(t, err)
	repository := newMemoryRestoreRepository()
	injected := errors.New("injected restore crash")
	faults := &oneShotRestoreFault{
		point: FaultAfterRestoreChunk,
		err:   injected,
	}
	coordinator := RestoreCoordinator{
		Store:      store,
		Repository: repository,
		Faults:     faults,
		Config: RestoreConfig{
			MaxChunkRows:         10,
			MaxChunkLogicalBytes: 1 << 20,
			Deadline:             time.Minute,
		},
	}
	attempt := RestoreAttempt{
		RestoreID:          "restore-resume",
		LeaseID:            "lease-resume",
		StagingDatabaseID:  7,
		StagingTableID:     8,
		HiddenName:         "__mo_lifecycle_restore_resume",
		TargetDatabaseID:   7,
		TargetDatabaseName: "restore_db",
		TargetName:         "events_history",
	}
	dataset := restoreTestDataset(t, "dataset-resume", manifestKey, manifest)
	require.ErrorIs(t, coordinator.Restore(
		context.Background(),
		dataset,
		attempt,
	), injected)
	require.NotEmpty(t, repository.rows)
	faults.err = nil
	require.NoError(t, coordinator.Restore(
		context.Background(),
		dataset,
		attempt,
	))
	require.Len(t, repository.rows, int(manifest.RowCount))
}

func TestRestorePreSideEffectFaultsResumeWithoutDuplicateRows(t *testing.T) {
	for _, point := range []FaultPoint{
		FaultBeforeRestoreChunk,
		FaultBeforeRestorePublish,
	} {
		t.Run(string(point), func(t *testing.T) {
			store := newMemoryArchiveStore()
			manifestKey := writeArchiveTestDataset(t, store)
			manifest, err := ReadArchiveManifest(
				context.Background(),
				store,
				manifestKey,
			)
			require.NoError(t, err)
			repository := newMemoryRestoreRepository()
			faults := &oneShotRestoreFault{
				point: point,
				err:   errors.New("injected " + string(point)),
			}
			coordinator := RestoreCoordinator{
				Store:      store,
				Repository: repository,
				Faults:     faults,
				Config: RestoreConfig{
					MaxChunkRows:         10,
					MaxChunkLogicalBytes: 1 << 20,
					Deadline:             time.Minute,
				},
			}
			attempt := RestoreAttempt{
				RestoreID:          "restore-" + string(point),
				LeaseID:            "lease-" + string(point),
				StagingDatabaseID:  7,
				StagingTableID:     8,
				HiddenName:         "__mo_lifecycle_" + string(point),
				TargetDatabaseID:   7,
				TargetDatabaseName: "restore_db",
				TargetName:         "events_history",
			}
			dataset := restoreTestDataset(
				t,
				"dataset-"+string(point),
				manifestKey,
				manifest,
			)

			require.EqualError(
				t,
				coordinator.Restore(
					context.Background(),
					dataset,
					attempt,
				),
				"injected "+string(point),
			)
			if point == FaultBeforeRestoreChunk {
				require.Empty(t, repository.rows)
			} else {
				require.Len(t, repository.rows, int(manifest.RowCount))
				require.Zero(t, repository.publishCount)
			}

			require.NoError(t, coordinator.Restore(
				context.Background(),
				dataset,
				attempt,
			))
			require.Len(t, repository.rows, int(manifest.RowCount))
			require.Equal(t, 1, repository.publishCount)
		})
	}
}

func TestRestoreInitializationFaultsPreserveSingleHiddenOwner(t *testing.T) {
	for _, point := range []FaultPoint{
		FaultBeforeRestoreInitialize,
		FaultAfterRestoreInitialize,
	} {
		t.Run(string(point), func(t *testing.T) {
			store := newMemoryArchiveStore()
			manifestKey := writeArchiveTestDataset(t, store)
			manifest, err := ReadArchiveManifest(
				context.Background(),
				store,
				manifestKey,
			)
			require.NoError(t, err)
			repository := newMemoryRestoreRepository()
			faults := NewProgrammableFaultInjector(
				map[FaultPoint]FaultAction{
					point: FailOnHit(1, "restore-init-crash"),
				},
			)
			coordinator := RestoreCoordinator{
				Store:      store,
				Repository: repository,
				Faults:     faults,
				Config: RestoreConfig{
					MaxChunkRows:         10,
					MaxChunkLogicalBytes: 1 << 20,
					Deadline:             time.Minute,
				},
			}
			attempt := RestoreAttempt{
				RestoreID:          "restore-init-" + string(point),
				LeaseID:            "lease-init-" + string(point),
				StagingDatabaseID:  7,
				StagingTableID:     8,
				HiddenName:         "__mo_lifecycle_restore_init",
				TargetDatabaseID:   7,
				TargetDatabaseName: "restore_db",
				TargetName:         "events_history",
			}
			dataset := restoreTestDataset(
				t,
				"dataset-init-"+string(point),
				manifestKey,
				manifest,
			)

			require.ErrorContains(t, coordinator.Restore(
				context.Background(),
				dataset,
				attempt,
			), "restore-init-crash")
			if point == FaultBeforeRestoreInitialize {
				require.Empty(t, repository.attempts)
			} else {
				require.Len(t, repository.attempts, 1)
			}
			require.Empty(t, repository.rows)

			require.NoError(t, coordinator.Restore(
				context.Background(),
				dataset,
				attempt,
			))
			require.Len(t, repository.attempts, 1)
			require.Len(t, repository.rows, int(manifest.RowCount))
			require.Equal(t, 1, repository.publishCount)
		})
	}
}

func TestRestoreFaultAfterPublishReconcilesWithoutDuplicatePublish(t *testing.T) {
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)
	manifest, err := ReadArchiveManifest(context.Background(), store, manifestKey)
	require.NoError(t, err)
	repository := newMemoryRestoreRepository()
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultAfterRestorePublish: FailOnHit(1, "publish-response-lost"),
	})
	coordinator := RestoreCoordinator{
		Store:      store,
		Repository: repository,
		Faults:     faults,
		Config: RestoreConfig{
			MaxChunkRows:         10,
			MaxChunkLogicalBytes: 1 << 20,
			Deadline:             time.Minute,
		},
	}
	attempt := RestoreAttempt{
		RestoreID:          "restore-publish-unknown",
		LeaseID:            "lease-publish-unknown",
		StagingDatabaseID:  7,
		StagingTableID:     8,
		HiddenName:         "__mo_lifecycle_restore_publish_unknown",
		TargetDatabaseID:   7,
		TargetDatabaseName: "restore_db",
		TargetName:         "events_history",
	}
	dataset := restoreTestDataset(
		t,
		"dataset-publish-unknown",
		manifestKey,
		manifest,
	)
	require.EqualError(t,
		coordinator.Restore(context.Background(), dataset, attempt),
		"publish-response-lost",
	)
	require.True(t, repository.published)
	require.Equal(t, 1, repository.publishCount)

	require.NoError(t,
		coordinator.Restore(context.Background(), dataset, attempt),
	)
	require.Equal(t, 1, repository.publishCount)
	require.Len(t, repository.rows, int(manifest.RowCount))
}

func restoreTestDataset(
	t *testing.T,
	datasetID string,
	manifestKey string,
	manifest *ArchiveManifest,
) RestoreDataset {
	t.Helper()
	digest, err := manifestDigestFromKey(manifestKey)
	require.NoError(t, err)
	return RestoreDataset{
		DatasetID:      datasetID,
		RootID:         manifest.RootID,
		AttemptID:      manifest.AttemptID,
		ManifestKey:    manifestKey,
		ManifestDigest: digest,
		SchemaDigest:   manifest.SchemaDigest,
		ContentHash:    manifest.ContentHash,
		RowCount:       manifest.RowCount,
		LogicalBytes:   manifest.LogicalBytes,
		Version:        1,
		State:          "PUBLISHED",
		StageID:        1,
		StageIdentity:  []byte("test-frozen-stage"),
	}
}

type oneShotRestoreFault struct {
	point FaultPoint
	err   error
}

func (fault *oneShotRestoreFault) Inject(
	_ context.Context,
	point FaultPoint,
) error {
	if point != fault.point || fault.err == nil {
		return nil
	}
	err := fault.err
	fault.err = nil
	return err
}

type memoryRestoreRepository struct {
	mu       sync.Mutex
	attempts map[string]RestoreAttempt
	receipts map[uint64]RestoreChunkReceipt
	rows     [][]CanonicalCell

	published    bool
	publishCount int
	purgeCount   int
}

func newMemoryRestoreRepository() *memoryRestoreRepository {
	return &memoryRestoreRepository{
		attempts: make(map[string]RestoreAttempt),
		receipts: make(map[uint64]RestoreChunkReceipt),
	}
}

func (repository *memoryRestoreRepository) Initialize(
	_ context.Context,
	request RestoreInitializeRequest,
) (RestoreAttempt, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if existing, ok := repository.attempts[request.Attempt.RestoreID]; ok {
		if existing.DatasetID != request.Dataset.DatasetID ||
			existing.HiddenName != request.Attempt.HiddenName ||
			existing.StagingTableID != request.Attempt.StagingTableID {
			return RestoreAttempt{}, fmt.Errorf("Restore initialization identity mismatch")
		}
		return existing, nil
	}
	attempt := request.Attempt
	attempt.State = "IMPORTING"
	repository.attempts[attempt.RestoreID] = attempt
	return attempt, nil
}

func (repository *memoryRestoreRepository) GetAttempt(
	_ context.Context,
	restoreID string,
) (RestoreAttempt, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	attempt, ok := repository.attempts[restoreID]
	if !ok {
		return RestoreAttempt{}, fmt.Errorf("Restore Attempt not found")
	}
	return attempt, nil
}

func (repository *memoryRestoreRepository) ImportChunk(
	_ context.Context,
	attempt RestoreAttempt,
	receipt RestoreChunkReceipt,
	_ SchemaDescriptor,
	rows [][]CanonicalCell,
) (RestoreAttempt, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current, ok := repository.attempts[attempt.RestoreID]
	if !ok || current.LeaseID != attempt.LeaseID {
		return RestoreAttempt{}, fmt.Errorf("Restore Attempt lease mismatch")
	}
	if existing, ok := repository.receipts[receipt.ChunkOrdinal]; ok {
		if existing.ChunkDigest != receipt.ChunkDigest {
			return RestoreAttempt{}, fmt.Errorf("Restore Chunk digest corruption")
		}
		return current, nil
	}
	if receipt.ChunkOrdinal != current.NextChunkOrdinal {
		return RestoreAttempt{}, fmt.Errorf("Restore Chunk ordinal is not next")
	}
	repository.rows = append(repository.rows, rows...)
	repository.receipts[receipt.ChunkOrdinal] = receipt
	current.NextChunkOrdinal++
	current.RestoredRows += receipt.RowCount
	repository.attempts[current.RestoreID] = current
	return current, nil
}

func (repository *memoryRestoreRepository) ListChunkReceipts(
	_ context.Context,
	restoreID string,
) ([]RestoreChunkReceipt, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	values := make([]RestoreChunkReceipt, len(repository.receipts))
	for ordinal, receipt := range repository.receipts {
		if receipt.RestoreID == restoreID {
			values[ordinal] = receipt
		}
	}
	return values, nil
}

func (repository *memoryRestoreRepository) Publish(
	_ context.Context,
	attempt RestoreAttempt,
	verifiedHash [32]byte,
	_ SchemaDescriptor,
	_ []AutoIncrementMax,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	current := repository.attempts[attempt.RestoreID]
	if current.State == "DONE" && current.VerifiedHash == verifiedHash {
		return nil
	}
	if current.LeaseID != attempt.LeaseID ||
		current.NextChunkOrdinal != uint64(len(repository.receipts)) {
		return fmt.Errorf("Restore publish CAS failed")
	}
	current.State = "DONE"
	current.VerifiedHash = verifiedHash
	repository.attempts[current.RestoreID] = current
	repository.published = true
	repository.publishCount++
	return nil
}

func (*memoryRestoreRepository) CleanupHidden(context.Context, string) error {
	return nil
}

func (repository *memoryRestoreRepository) RequestPurge(
	context.Context,
	RestoreDataset,
	time.Time,
) error {
	repository.purgeCount++
	return nil
}
