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
	"crypto/sha256"
	"errors"
	"fmt"
	"path"
	"time"

	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var ErrRestoreInProgress = errors.New("Lifecycle Archive Restore is in progress")

type RestoreDataset struct {
	DatasetID       string
	AccountID       uint32
	RootID          string
	AttemptID       string
	ManifestKey     string
	ManifestDigest  [sha256.Size]byte
	SchemaDigest    [sha256.Size]byte
	ContentHash     [sha256.Size]byte
	RowCount        uint64
	LogicalBytes    uint64
	Version         uint64
	State           string
	StageID         uint64
	StageIdentity   []byte
	PurgeEligibleAt time.Time
	RestoreLeaseID  string
	RestoreDeadline time.Time
}

type RestoreAttempt struct {
	RestoreID          string
	DatasetID          string
	LeaseID            string
	Deadline           time.Time
	StagingDatabaseID  uint64
	StagingTableID     uint64
	HiddenName         string
	TargetDatabaseID   uint64
	TargetDatabaseName string
	TargetName         string
	State              string
	NextChunkOrdinal   uint64
	RestoredRows       uint64
	VerifiedHash       [sha256.Size]byte
}

type RestoreChunkReceipt struct {
	RestoreID            string
	ChunkOrdinal         uint64
	FileOrdinal          uint32
	RowGroupOrdinal      uint32
	ChunkDigest          [sha256.Size]byte
	RowCount             uint64
	LogicalBytes         uint64
	CanonicalContentHash [sha256.Size]byte
}

type RestoreInitializeRequest struct {
	Dataset         RestoreDataset
	Attempt         RestoreAttempt
	HiddenCreateSQL string
}

// RestoreRepository is implemented with ordinary MO transactions. Each
// method's atomicity is part of the interface contract; Lifecycle does not add
// a private transaction protocol.
type RestoreRepository interface {
	Initialize(
		ctx context.Context,
		request RestoreInitializeRequest,
	) (RestoreAttempt, error)
	GetAttempt(ctx context.Context, restoreID string) (RestoreAttempt, error)
	ImportChunk(
		ctx context.Context,
		attempt RestoreAttempt,
		receipt RestoreChunkReceipt,
		schema SchemaDescriptor,
		rows [][]CanonicalCell,
	) (RestoreAttempt, error)
	ListChunkReceipts(
		ctx context.Context,
		restoreID string,
	) ([]RestoreChunkReceipt, error)
	Publish(
		ctx context.Context,
		attempt RestoreAttempt,
		verifiedHash [sha256.Size]byte,
		schema SchemaDescriptor,
		autoIncrementMaxima []AutoIncrementMax,
	) error
	CleanupHidden(ctx context.Context, restoreID string) error
	RequestPurge(
		ctx context.Context,
		dataset RestoreDataset,
		now time.Time,
	) error
}

type RestoreConfig struct {
	MaxChunkRows         uint64
	MaxChunkLogicalBytes uint64
	Deadline             time.Duration
}

type RestoreCoordinator struct {
	Store      ArchiveStore
	Repository RestoreRepository
	Config     RestoreConfig
	Faults     FaultInjector
}

func (coordinator RestoreCoordinator) Restore(
	ctx context.Context,
	dataset RestoreDataset,
	attempt RestoreAttempt,
) (err error) {
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		metricv2.LifecycleRestoreCounter.WithLabelValues(
			"restore",
			result,
		).Inc()
	}()
	if coordinator.Store == nil ||
		coordinator.Repository == nil ||
		coordinator.Config.MaxChunkRows == 0 ||
		coordinator.Config.MaxChunkLogicalBytes == 0 ||
		coordinator.Config.Deadline <= 0 ||
		dataset.State != "PUBLISHED" ||
		dataset.DatasetID == "" ||
		attempt.RestoreID == "" ||
		attempt.HiddenName == "" ||
		attempt.TargetName == "" {
		return fmt.Errorf("Lifecycle Restore input is incomplete")
	}
	if attempt.Deadline.IsZero() {
		attempt.Deadline = time.Now().Add(coordinator.Config.Deadline)
	}
	restoreCtx, cancelRestore := context.WithDeadline(ctx, attempt.Deadline)
	defer cancelRestore()
	ctx = restoreCtx
	if err := ctx.Err(); err != nil {
		return err
	}
	manifest, err := ReadArchiveManifest(ctx, coordinator.Store, dataset.ManifestKey)
	if err != nil {
		return err
	}
	if err := validateRestoreDatasetManifestIdentity(dataset, manifest); err != nil {
		return err
	}
	if manifest.ContentHash != dataset.ContentHash ||
		manifest.RowCount != dataset.RowCount ||
		manifest.LogicalBytes != dataset.LogicalBytes {
		return fmt.Errorf("Lifecycle Dataset and Manifest identity mismatch")
	}
	createSQL, err := manifest.Schema.BuildRestoreCreateTableSQL(
		ctx,
		attempt.TargetDatabaseName,
		attempt.HiddenName,
	)
	if err != nil {
		return err
	}
	attempt.DatasetID = dataset.DatasetID
	faults := coordinator.Faults
	if faults == nil {
		faults = NoLifecycleFaults{}
	}
	if err := faults.Inject(ctx, FaultBeforeRestoreInitialize); err != nil {
		return err
	}
	attempt, err = coordinator.Repository.Initialize(
		ctx,
		RestoreInitializeRequest{
			Dataset:         dataset,
			Attempt:         attempt,
			HiddenCreateSQL: createSQL,
		},
	)
	if err != nil {
		return err
	}
	if err := faults.Inject(ctx, FaultAfterRestoreInitialize); err != nil {
		return err
	}

	for attempt.NextChunkOrdinal < manifest.TotalChunkCount {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !attempt.Deadline.After(time.Now()) {
			return fmt.Errorf("Lifecycle Restore deadline expired")
		}
		ordinal := attempt.NextChunkOrdinal
		if err := faults.Inject(ctx, FaultBeforeRestoreChunk); err != nil {
			return err
		}
		rows, chunk, err := ReadArchiveChunk(
			ctx,
			coordinator.Store,
			manifest,
			ordinal,
			coordinator.Config.MaxChunkRows,
			coordinator.Config.MaxChunkLogicalBytes,
		)
		if err != nil {
			return err
		}
		receipt := RestoreChunkReceipt{
			RestoreID:            attempt.RestoreID,
			ChunkOrdinal:         chunk.ChunkOrdinal,
			FileOrdinal:          chunk.FileOrdinal,
			RowGroupOrdinal:      chunk.RowGroupOrdinal,
			ChunkDigest:          chunkReceiptDigest(chunk),
			RowCount:             chunk.RowCount,
			LogicalBytes:         chunk.LogicalBytes,
			CanonicalContentHash: chunk.CanonicalContentHash,
		}
		attempt, err = coordinator.Repository.ImportChunk(
			ctx,
			attempt,
			receipt,
			manifest.Schema,
			rows,
		)
		if err != nil {
			return err
		}
		if err := faults.Inject(ctx, FaultAfterRestoreChunk); err != nil {
			return err
		}
	}
	receipts, err := coordinator.Repository.ListChunkReceipts(
		ctx,
		attempt.RestoreID,
	)
	if err != nil {
		return err
	}
	chunks := make([]ArchiveChunk, len(receipts))
	var restoredRows uint64
	for ordinal, receipt := range receipts {
		if receipt.ChunkOrdinal != uint64(ordinal) ||
			receipt.FileOrdinal != uint32(ordinal) ||
			receipt.RowGroupOrdinal != 0 ||
			receipt.ChunkDigest != chunkReceiptDigest(ArchiveChunk{
				ChunkOrdinal:         receipt.ChunkOrdinal,
				FileOrdinal:          receipt.FileOrdinal,
				RowGroupOrdinal:      receipt.RowGroupOrdinal,
				RowCount:             receipt.RowCount,
				LogicalBytes:         receipt.LogicalBytes,
				CanonicalContentHash: receipt.CanonicalContentHash,
			}) {
			return fmt.Errorf("Lifecycle Restore Chunk Receipt sequence is corrupt")
		}
		chunks[ordinal] = ArchiveChunk{
			ChunkOrdinal:         receipt.ChunkOrdinal,
			FileOrdinal:          receipt.FileOrdinal,
			RowGroupOrdinal:      receipt.RowGroupOrdinal,
			RowCount:             receipt.RowCount,
			LogicalBytes:         receipt.LogicalBytes,
			CanonicalContentHash: receipt.CanonicalContentHash,
		}
		restoredRows += receipt.RowCount
	}
	if uint64(len(chunks)) != manifest.TotalChunkCount ||
		restoredRows != manifest.RowCount {
		return fmt.Errorf("Lifecycle Restore Chunk Receipts are incomplete")
	}
	verifiedHash := computeArchiveDatasetHash(manifest.SchemaDigest, chunks)
	if verifiedHash != manifest.ContentHash {
		return fmt.Errorf("Lifecycle Restore aggregate content hash mismatch")
	}
	attempt, err = coordinator.Repository.GetAttempt(ctx, attempt.RestoreID)
	if err != nil {
		return err
	}
	if err := faults.Inject(ctx, FaultBeforeRestorePublish); err != nil {
		return err
	}
	if err := coordinator.Repository.Publish(
		ctx,
		attempt,
		verifiedHash,
		manifest.Schema,
		manifest.AutoIncrementMaxima,
	); err != nil {
		return err
	}
	return faults.Inject(ctx, FaultAfterRestorePublish)
}

func validateRestoreDatasetManifestIdentity(
	dataset RestoreDataset,
	manifest *ArchiveManifest,
) error {
	if manifest == nil ||
		dataset.RootID == "" ||
		dataset.AttemptID == "" ||
		manifest.RootID != dataset.RootID ||
		manifest.AttemptID != dataset.AttemptID ||
		manifest.SchemaDigest != dataset.SchemaDigest ||
		manifest.VerificationStatus != "FULL_READBACK_VERIFIED" {
		return fmt.Errorf("Lifecycle Dataset and Manifest identity mismatch")
	}
	manifestDigest, err := manifestDigestFromKey(dataset.ManifestKey)
	if err != nil || manifestDigest != dataset.ManifestDigest {
		return fmt.Errorf("Lifecycle Dataset Manifest digest mismatch")
	}
	prefix := path.Dir(dataset.ManifestKey)
	if !lifecycleRootScopedPrefix(
		prefix,
		dataset.RootID,
		dataset.AttemptID,
	) || !cleanupKeyWithinPrefix(dataset.ManifestKey, prefix) {
		return fmt.Errorf("Lifecycle Dataset Manifest namespace mismatch")
	}
	for _, file := range manifest.Files {
		if !cleanupKeyWithinPrefix(file.Key, prefix) {
			return fmt.Errorf("Lifecycle Dataset Payload namespace mismatch")
		}
	}
	return nil
}

func (coordinator RestoreCoordinator) Purge(
	ctx context.Context,
	dataset RestoreDataset,
	now time.Time,
) (err error) {
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		metricv2.LifecycleRestoreCounter.WithLabelValues(
			"purge",
			result,
		).Inc()
	}()
	if coordinator.Repository == nil || dataset.DatasetID == "" || now.IsZero() {
		return fmt.Errorf("Lifecycle Purge input is incomplete")
	}
	if dataset.RestoreLeaseID != "" {
		return ErrRestoreInProgress
	}
	return coordinator.Repository.RequestPurge(
		ctx,
		dataset,
		now,
	)
}

func chunkReceiptDigest(chunk ArchiveChunk) [sha256.Size]byte {
	return computeArchiveDatasetHash([sha256.Size]byte{}, []ArchiveChunk{chunk})
}
