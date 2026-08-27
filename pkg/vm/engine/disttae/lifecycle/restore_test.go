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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestFilterCanonicalRowsByLifecycleRangeUsesHalfOpenBounds(t *testing.T) {
	schema := SchemaDescriptor{Columns: []SchemaColumn{
		{Ordinal: 0, SourceColumnID: 1, TypeID: int32(types.T_int64)},
		{Ordinal: 1, SourceColumnID: 7, TypeID: int32(types.T_timestamp), NotNull: true},
	}}
	rows := [][]CanonicalCell{
		{{Type: types.T_int64.ToType(), Value: int64(1)}, {Type: types.T_timestamp.ToType(), Value: types.Timestamp(99)}},
		{{Type: types.T_int64.ToType(), Value: int64(2)}, {Type: types.T_timestamp.ToType(), Value: types.Timestamp(100)}},
		{{Type: types.T_int64.ToType(), Value: int64(3)}, {Type: types.T_timestamp.ToType(), Value: types.Timestamp(199)}},
		{{Type: types.T_int64.ToType(), Value: int64(4)}, {Type: types.T_timestamp.ToType(), Value: types.Timestamp(200)}},
	}
	filtered, err := FilterCanonicalRowsByLifecycleRange(
		context.Background(),
		schema,
		ArchiveLifecycleRange{SourceColumnID: 7, TypeID: int32(types.T_timestamp)},
		100,
		200,
		rows,
	)
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.Equal(t, int64(2), filtered[0][0].Value)
	require.Equal(t, int64(3), filtered[1][0].Value)
}

func TestSelectRestoreDatasetsForRangeScopesGenerationCheckToOverlap(t *testing.T) {
	ctx := context.Background()
	oldMin, err := ParseLifecycleRestoreBoundary(ctx, "2024-01-01", types.T_date)
	require.NoError(t, err)
	oldMax, err := ParseLifecycleRestoreBoundary(ctx, "2024-01-31", types.T_date)
	require.NoError(t, err)
	currentMin, err := ParseLifecycleRestoreBoundary(ctx, "2025-01-01", types.T_date)
	require.NoError(t, err)
	currentMax, err := ParseLifecycleRestoreBoundary(ctx, "2025-01-31", types.T_date)
	require.NoError(t, err)
	datasets := []RestoreDataset{
		{
			DatasetID: "old-generation",
			LifecycleRange: ArchiveLifecycleRange{
				SourceColumnID: 3,
				TypeID:         int32(types.T_date),
				Min:            oldMin,
				Max:            oldMax,
			},
			HasLifecycleRange: true,
		},
		{
			DatasetID: "current-generation",
			LifecycleRange: ArchiveLifecycleRange{
				SourceColumnID: 7,
				TypeID:         int32(types.T_date),
				Min:            currentMin,
				Max:            currentMax,
			},
			HasLifecycleRange: true,
		},
	}

	selected, start, end, err := SelectRestoreDatasetsForRange(
		ctx,
		datasets,
		"2025-01-01",
		"2025-02-01",
	)
	require.NoError(t, err)
	require.Equal(t, []RestoreDataset{datasets[1]}, selected)
	require.Equal(t, currentMin, start)
	require.Greater(t, end, currentMax)

	_, _, _, err = SelectRestoreDatasetsForRange(
		ctx,
		datasets,
		"2024-01-01",
		"2025-02-01",
	)
	require.ErrorContains(t, err, "across Lifecycle column generations")
}

func TestLifecycleRangeFailsClosedOnInvalidMetadataAndRows(t *testing.T) {
	ctx := context.Background()
	schema := SchemaDescriptor{Columns: []SchemaColumn{
		{SourceColumnID: 1, TypeID: int32(types.T_int64)},
		{SourceColumnID: 7, TypeID: int32(types.T_timestamp), NotNull: true},
	}}
	lifecycleRange := ArchiveLifecycleRange{
		SourceColumnID: 7,
		TypeID:         int32(types.T_timestamp),
	}
	validRow := []CanonicalCell{
		{Type: types.T_int64.ToType(), Value: int64(1)},
		{Type: types.T_timestamp.ToType(), Value: types.Timestamp(150)},
	}

	_, err := lifecycleRangeColumnOrdinal(schema, ArchiveLifecycleRange{
		SourceColumnID: 7,
		TypeID:         int32(types.T_int64),
	})
	require.ErrorContains(t, err, "column identity is invalid")
	_, err = lifecycleRangeColumnOrdinal(schema, ArchiveLifecycleRange{
		SourceColumnID: 99,
		TypeID:         int32(types.T_timestamp),
	})
	require.ErrorContains(t, err, "does not exist")

	for _, test := range []struct {
		name  string
		ctx   context.Context
		start int64
		end   int64
		rows  [][]CanonicalCell
		want  string
	}{
		{name: "empty interval", ctx: ctx, start: 100, end: 100, rows: [][]CanonicalCell{validRow}, want: "must be non-empty"},
		{name: "short row", ctx: ctx, start: 100, end: 200, rows: [][]CanonicalCell{{validRow[0]}}, want: "does not match the frozen schema"},
		{name: "null range value", ctx: ctx, start: 100, end: 200, rows: [][]CanonicalCell{{validRow[0], {Type: types.T_timestamp.ToType(), Null: true}}}, want: "contains NULL"},
		{name: "wrong physical value", ctx: ctx, start: 100, end: 200, rows: [][]CanonicalCell{{validRow[0], {Type: types.T_timestamp.ToType(), Value: int64(150)}}}, want: "value type TIMESTAMP is invalid"},
		{name: "cancelled", ctx: func() context.Context { cancelled, cancel := context.WithCancel(ctx); cancel(); return cancelled }(), start: 100, end: 200, rows: [][]CanonicalCell{validRow}, want: context.Canceled.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := FilterCanonicalRowsByLifecycleRange(
				test.ctx,
				schema,
				lifecycleRange,
				test.start,
				test.end,
				test.rows,
			)
			require.ErrorContains(t, err, test.want)
		})
	}

	dateValue, err := lifecycleRangeCellValue(CanonicalCell{
		Type: types.T_date.ToType(), Value: types.Date(12),
	})
	require.NoError(t, err)
	require.Equal(t, int64(12), dateValue)
	datetimeValue, err := lifecycleRangeCellValue(CanonicalCell{
		Type: types.T_datetime.ToType(), Value: types.Datetime(34),
	})
	require.NoError(t, err)
	require.Equal(t, int64(34), datetimeValue)

	dataset := RestoreDataset{
		DatasetID: "dataset-1",
		LifecycleRange: ArchiveLifecycleRange{
			SourceColumnID: 7,
			TypeID:         int32(types.T_date),
			Min:            10,
			Max:            20,
		},
		HasLifecycleRange: true,
	}
	for _, test := range []struct {
		name    string
		dataset RestoreDataset
		from    string
		to      string
		want    string
	}{
		{name: "unverified dataset range", dataset: RestoreDataset{DatasetID: "missing-range"}, from: "2025-01-01", to: "2025-02-01", want: "has no verified range identity"},
		{name: "invalid lower boundary", dataset: dataset, from: "not-a-date", to: "2025-02-01", want: "date"},
		{name: "invalid upper boundary", dataset: dataset, from: "2025-01-01", to: "not-a-date", want: "date"},
		{name: "reversed interval", dataset: dataset, from: "2025-02-01", to: "2025-01-01", want: "must be non-empty"},
		{name: "no overlap", dataset: dataset, from: "2025-01-01", to: "2025-02-01", want: "no Dataset overlapping"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := SelectRestoreDatasetsForRange(
				ctx,
				[]RestoreDataset{test.dataset},
				test.from,
				test.to,
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestRestoreRangeFiltersBoundaryDatasetsAndResumesIdempotently(t *testing.T) {
	store := newMemoryArchiveStore()
	const timestampSecond = int64(time.Second / time.Microsecond)
	firstKey := writeArchiveRangeTestDataset(
		t,
		store,
		"first",
		types.Timestamp(50*timestampSecond),
		types.Timestamp(100*timestampSecond),
		types.Timestamp(150*timestampSecond),
	)
	secondKey := writeArchiveRangeTestDataset(
		t,
		store,
		"second",
		types.Timestamp(199*timestampSecond),
		types.Timestamp(200*timestampSecond),
		types.Timestamp(250*timestampSecond),
	)
	firstManifest, err := ReadArchiveManifest(context.Background(), store, firstKey)
	require.NoError(t, err)
	secondManifest, err := ReadArchiveManifest(context.Background(), store, secondKey)
	require.NoError(t, err)
	first := restoreTestDataset(t, "dataset-first", firstKey, firstManifest)
	second := restoreTestDataset(t, "dataset-second", secondKey, secondManifest)
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
		RestoreID:          "restore-range",
		LeaseID:            "lease-range",
		StagingDatabaseID:  7,
		StagingTableID:     8,
		HiddenName:         "__mo_lifecycle_restore_range",
		TargetDatabaseID:   7,
		TargetDatabaseName: "restore_db",
		TargetName:         "events_history",
	}
	require.NoError(t, coordinator.RestoreRange(
		context.Background(),
		[]RestoreDataset{first, second},
		attempt,
		100*timestampSecond,
		200*timestampSecond,
	))
	require.True(t, repository.published)
	require.Equal(t, 1, repository.publishCount)
	require.Len(t, repository.rows, 3)
	require.Equal(t, types.Timestamp(100*timestampSecond), repository.rows[0][1].Value)
	require.Equal(t, types.Timestamp(150*timestampSecond), repository.rows[1][1].Value)
	require.Equal(t, types.Timestamp(199*timestampSecond), repository.rows[2][1].Value)

	// A retry reuses the frozen selection and committed Chunk receipts.
	require.NoError(t, coordinator.RestoreRange(
		context.Background(),
		[]RestoreDataset{first, second},
		attempt,
		100*timestampSecond,
		200*timestampSecond,
	))
	require.Equal(t, 1, repository.publishCount)
	require.Len(t, repository.rows, 3)
}

func TestRestoreRangeManifestMemoryBudgetFailsBeforeUnboundedGrowth(t *testing.T) {
	manifest := archiveManifestV1GoldenFixture()
	encoded, _, err := MarshalArchiveManifest(manifest)
	require.NoError(t, err)

	used, err := addRestoreRangeManifestBytes(0, manifest)
	require.NoError(t, err)
	require.Equal(t, uint64(len(encoded)), used)

	_, err = addRestoreRangeManifestBytes(
		maxRestoreRangeManifestBytes-used+1,
		manifest,
	)
	require.ErrorContains(t, err, "Manifest bytes exceed the certified limit")
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

			require.ErrorContains(
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
	require.ErrorContains(t,
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
	dataset := RestoreDataset{
		DatasetID:      datasetID,
		LogicalTableID: manifest.Schema.SourceTableID,
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
	if manifest.LifecycleRange != nil {
		dataset.LifecycleRange = *manifest.LifecycleRange
		dataset.HasLifecycleRange = true
	}
	return dataset
}

func writeArchiveRangeTestDataset(
	t *testing.T,
	store *memoryArchiveStore,
	identity string,
	values ...types.Timestamp,
) string {
	t.Helper()
	ctx := context.Background()
	schema := SchemaDescriptor{
		FormatVersion:      schemaDescriptorFormatVersion,
		SourceTableID:      42,
		SourceTableVersion: 7,
		SourceDatabaseName: "db",
		SourceTableName:    "events",
		Columns: []SchemaColumn{
			{Ordinal: 0, SourceColumnID: 1, Name: "id", TypeID: int32(types.T_int64), NotNull: true},
			{Ordinal: 1, SourceColumnID: 7, Name: "created_at", TypeID: int32(types.T_timestamp), NotNull: true},
		},
	}
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:                 "root-" + identity,
		AttemptID:              "attempt-" + identity,
		Prefix:                 "archive/root-" + identity + "/attempt-" + identity,
		WriteID:                "write-" + identity,
		Schema:                 schema,
		SchemaDigest:           schemaDigest,
		MaxRestoreChunkRows:    1,
		MaxChunkLogicalBytes:   1 << 20,
		MaxPhysicalBytes:       archiveTestMaxPhysicalBytes,
		TrackLifecycleRange:    true,
		LifecycleColumnOrdinal: 1,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := batch.New([]string{"id", "created_at"})
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	value.Vecs[1] = vector.NewVec(types.T_timestamp.ToType())
	for index, timestamp := range values {
		require.NoError(t, vector.AppendFixed(value.Vecs[0], int64(index+1), false, mp))
		require.NoError(t, vector.AppendFixed(value.Vecs[1], timestamp, false, mp))
	}
	value.SetRowCount(len(values))
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	_, key, err := writer.Close(ctx)
	require.NoError(t, err)
	return key
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
