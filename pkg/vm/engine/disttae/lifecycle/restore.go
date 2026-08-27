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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/big"
	"path"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var ErrRestoreInProgress = moerr.NewInternalErrorNoCtx("Lifecycle Archive Restore is in progress")

const (
	RestoreScopeDataset          = "DATASET"
	RestoreScopeRange            = "RANGE"
	maxRestoreRangeChunks        = 262_144
	maxRestoreRangeManifestBytes = 64 << 20
)

type RestoreDataset struct {
	DatasetID         string
	AccountID         uint32
	LogicalTableID    uint64
	RootID            string
	AttemptID         string
	ManifestKey       string
	ManifestDigest    [sha256.Size]byte
	SchemaDigest      [sha256.Size]byte
	ContentHash       [sha256.Size]byte
	RowCount          uint64
	LogicalBytes      uint64
	Version           uint64
	State             string
	StageID           uint64
	StageIdentity     []byte
	PurgeEligibleAt   time.Time
	RestoreLeaseID    string
	RestoreDeadline   time.Time
	LifecycleRange    ArchiveLifecycleRange
	HasLifecycleRange bool
}

type RestoreAttempt struct {
	RestoreID            string
	DatasetID            string
	LeaseID              string
	Deadline             time.Time
	StagingDatabaseID    uint64
	StagingTableID       uint64
	HiddenName           string
	TargetDatabaseID     uint64
	TargetDatabaseName   string
	TargetName           string
	State                string
	NextChunkOrdinal     uint64
	RestoredRows         uint64
	VerifiedHash         [sha256.Size]byte
	Scope                string
	DatasetIDs           []string
	SelectionDigest      [sha256.Size]byte
	SourceLogicalTableID uint64
	RangeStart           int64
	RangeEnd             int64
	LifecycleRange       ArchiveLifecycleRange
	TotalChunkCount      uint64
	SelectedLogicalBytes uint64
}

type RestoreChunkReceipt struct {
	RestoreID            string
	DatasetID            string
	DatasetChunkOrdinal  uint64
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
	Datasets        []RestoreDataset
	Attempt         RestoreAttempt
	HiddenCreateSQL string
}

type restoreDatasetPlan struct {
	dataset   RestoreDataset
	manifest  *ArchiveManifest
	chunkBase uint64
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
		return moerr.NewInternalErrorNoCtxf("Lifecycle Restore input is incomplete")
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
		return moerr.NewInternalErrorNoCtxf("Lifecycle Dataset and Manifest identity mismatch")
	}
	attempt.Scope = RestoreScopeDataset
	attempt.DatasetID = dataset.DatasetID
	attempt.DatasetIDs = []string{dataset.DatasetID}
	attempt.SourceLogicalTableID = dataset.LogicalTableID
	attempt.LifecycleRange = dataset.LifecycleRange
	attempt.TotalChunkCount = manifest.TotalChunkCount
	attempt.SelectedLogicalBytes = dataset.LogicalBytes
	attempt.SelectionDigest = BuildRestoreSelectionDigest(
		attempt.Scope,
		attempt.SourceLogicalTableID,
		0,
		0,
		attempt.DatasetIDs,
	)
	createSQL, err := manifest.Schema.BuildRestoreCreateTableSQL(
		ctx,
		attempt.TargetDatabaseName,
		attempt.HiddenName,
	)
	if err != nil {
		return err
	}
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
			Datasets:        []RestoreDataset{dataset},
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
			return moerr.NewInternalErrorNoCtxf("Lifecycle Restore deadline expired")
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
			DatasetID:            dataset.DatasetID,
			DatasetChunkOrdinal:  chunk.ChunkOrdinal,
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
			receipt.DatasetID != dataset.DatasetID ||
			receipt.DatasetChunkOrdinal != uint64(ordinal) ||
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
			return moerr.NewInternalErrorNoCtxf("Lifecycle Restore Chunk Receipt sequence is corrupt")
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
		return moerr.NewInternalErrorNoCtxf("Lifecycle Restore Chunk Receipts are incomplete")
	}
	verifiedHash := computeArchiveDatasetHash(manifest.SchemaDigest, chunks)
	if verifiedHash != manifest.ContentHash {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Restore aggregate content hash mismatch")
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

// RestoreRange restores the exact half-open Lifecycle interval [start,end)
// from a bounded immutable Dataset selection into one hidden table. The
// selection is leased and persisted by the existing Restore Attempt before the
// hidden table becomes visible; no Object retirement or generic transaction
// behavior is involved.
func (coordinator RestoreCoordinator) RestoreRange(
	ctx context.Context,
	datasets []RestoreDataset,
	attempt RestoreAttempt,
	start int64,
	end int64,
) (err error) {
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		metricv2.LifecycleRestoreCounter.WithLabelValues(
			"restore_range",
			result,
		).Inc()
	}()
	if coordinator.Store == nil || coordinator.Repository == nil ||
		coordinator.Config.MaxChunkRows == 0 ||
		coordinator.Config.MaxChunkLogicalBytes == 0 ||
		coordinator.Config.Deadline <= 0 || start >= end ||
		attempt.RestoreID == "" || attempt.HiddenName == "" ||
		attempt.TargetName == "" {
		return moerr.NewInternalErrorNoCtxf("Lifecycle range Restore input is incomplete")
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
	plans, schema, err := coordinator.prepareRangePlans(ctx, datasets, start, end)
	if err != nil {
		return err
	}
	datasetIDs := make([]string, len(plans))
	selectedDatasets := make([]RestoreDataset, len(plans))
	var totalChunks uint64
	var selectedBytes uint64
	for index, plan := range plans {
		datasetIDs[index] = plan.dataset.DatasetID
		selectedDatasets[index] = plan.dataset
		totalChunks += plan.manifest.TotalChunkCount
		selectedBytes += plan.dataset.LogicalBytes
	}
	attempt.Scope = RestoreScopeRange
	attempt.DatasetID = datasetIDs[0]
	attempt.DatasetIDs = datasetIDs
	attempt.SourceLogicalTableID = plans[0].dataset.LogicalTableID
	attempt.RangeStart = start
	attempt.RangeEnd = end
	attempt.LifecycleRange = plans[0].dataset.LifecycleRange
	attempt.TotalChunkCount = totalChunks
	attempt.SelectedLogicalBytes = selectedBytes
	attempt.SelectionDigest = BuildRestoreSelectionDigest(
		attempt.Scope,
		attempt.SourceLogicalTableID,
		start,
		end,
		datasetIDs,
	)
	createSQL, err := schema.BuildRestoreCreateTableSQL(
		ctx,
		attempt.TargetDatabaseName,
		attempt.HiddenName,
	)
	if err != nil {
		return err
	}
	faults := coordinator.Faults
	if faults == nil {
		faults = NoLifecycleFaults{}
	}
	if err := faults.Inject(ctx, FaultBeforeRestoreInitialize); err != nil {
		return err
	}
	attempt, err = coordinator.Repository.Initialize(ctx, RestoreInitializeRequest{
		Dataset:         selectedDatasets[0],
		Datasets:        selectedDatasets,
		Attempt:         attempt,
		HiddenCreateSQL: createSQL,
	})
	if err != nil {
		return err
	}
	if err := faults.Inject(ctx, FaultAfterRestoreInitialize); err != nil {
		return err
	}

	planIndex := 0
	for attempt.NextChunkOrdinal < totalChunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		globalOrdinal := attempt.NextChunkOrdinal
		for planIndex+1 < len(plans) &&
			globalOrdinal >= plans[planIndex+1].chunkBase {
			planIndex++
		}
		plan := plans[planIndex]
		localOrdinal := globalOrdinal - plan.chunkBase
		if err := faults.Inject(ctx, FaultBeforeRestoreChunk); err != nil {
			return err
		}
		rows, sourceChunk, err := ReadArchiveChunk(
			ctx,
			coordinator.Store,
			plan.manifest,
			localOrdinal,
			coordinator.Config.MaxChunkRows,
			coordinator.Config.MaxChunkLogicalBytes,
		)
		if err != nil {
			return err
		}
		rows, err = FilterCanonicalRowsByLifecycleRange(
			ctx,
			plan.manifest.Schema,
			*plan.manifest.LifecycleRange,
			start,
			end,
			rows,
		)
		if err != nil {
			return err
		}
		receipt, err := rangeRestoreChunkReceipt(
			ctx,
			attempt.RestoreID,
			plan.dataset.DatasetID,
			globalOrdinal,
			localOrdinal,
			sourceChunk,
			plan.manifest.SchemaDigest,
			rows,
		)
		if err != nil {
			return err
		}
		attempt, err = coordinator.Repository.ImportChunk(
			ctx,
			attempt,
			receipt,
			plan.manifest.Schema,
			rows,
		)
		if err != nil {
			return err
		}
		if err := faults.Inject(ctx, FaultAfterRestoreChunk); err != nil {
			return err
		}
	}
	receipts, err := coordinator.Repository.ListChunkReceipts(ctx, attempt.RestoreID)
	if err != nil {
		return err
	}
	chunks, restoredRows, err := validateRangeRestoreReceipts(
		plans,
		receipts,
		start,
		end,
	)
	if err != nil {
		return err
	}
	verifiedHash := computeArchiveDatasetHash(plans[0].manifest.SchemaDigest, chunks)
	attempt, err = coordinator.Repository.GetAttempt(ctx, attempt.RestoreID)
	if err != nil {
		return err
	}
	if attempt.RestoredRows != restoredRows ||
		attempt.NextChunkOrdinal != totalChunks {
		return moerr.NewInternalErrorNoCtxf("Lifecycle range Restore progress is incomplete")
	}
	if err := faults.Inject(ctx, FaultBeforeRestorePublish); err != nil {
		return err
	}
	if err := coordinator.Repository.Publish(
		ctx,
		attempt,
		verifiedHash,
		schema,
		mergeRestoreAutoIncrementMaxima(plans),
	); err != nil {
		return err
	}
	return faults.Inject(ctx, FaultAfterRestorePublish)
}

func (coordinator RestoreCoordinator) prepareRangePlans(
	ctx context.Context,
	datasets []RestoreDataset,
	start int64,
	end int64,
) ([]restoreDatasetPlan, SchemaDescriptor, error) {
	plans := make([]restoreDatasetPlan, 0, len(datasets))
	var schema SchemaDescriptor
	var schemaDigest [sha256.Size]byte
	var stageID uint64
	var stageIdentity []byte
	var totalChunks uint64
	var selectedBytes uint64
	var manifestBytes uint64
	var logicalTableID uint64
	var lifecycleRange ArchiveLifecycleRange
	for _, dataset := range datasets {
		if !dataset.HasLifecycleRange {
			return nil, SchemaDescriptor{}, moerr.NewInternalErrorNoCtxf(
				"Lifecycle Dataset has no verified range identity",
			)
		}
		if dataset.State != "PUBLISHED" || dataset.LifecycleRange.Max < start ||
			dataset.LifecycleRange.Min >= end {
			continue
		}
		manifest, err := ReadArchiveManifest(ctx, coordinator.Store, dataset.ManifestKey)
		if err != nil {
			return nil, SchemaDescriptor{}, err
		}
		if err := validateRestoreDatasetManifestIdentity(dataset, manifest); err != nil {
			return nil, SchemaDescriptor{}, err
		}
		if manifest.ContentHash != dataset.ContentHash ||
			manifest.RowCount != dataset.RowCount ||
			manifest.LogicalBytes != dataset.LogicalBytes ||
			manifest.LifecycleRange == nil ||
			*manifest.LifecycleRange != dataset.LifecycleRange {
			return nil, SchemaDescriptor{}, moerr.NewInternalErrorNoCtxf(
				"Lifecycle Dataset and Manifest range identity mismatch",
			)
		}
		manifestBytes, err = addRestoreRangeManifestBytes(manifestBytes, manifest)
		if err != nil {
			return nil, SchemaDescriptor{}, err
		}
		if len(plans) == 0 {
			schema = manifest.Schema
			schemaDigest = manifest.SchemaDigest
			stageID = dataset.StageID
			stageIdentity = append([]byte(nil), dataset.StageIdentity...)
			logicalTableID = dataset.LogicalTableID
			lifecycleRange = dataset.LifecycleRange
		} else if manifest.SchemaDigest != schemaDigest ||
			dataset.StageID != stageID ||
			!bytes.Equal(dataset.StageIdentity, stageIdentity) ||
			dataset.LogicalTableID != logicalTableID ||
			dataset.LifecycleRange.SourceColumnID != lifecycleRange.SourceColumnID ||
			dataset.LifecycleRange.TypeID != lifecycleRange.TypeID {
			return nil, SchemaDescriptor{}, moerr.NewNotSupportedNoCtx(
				"Lifecycle range Restore across Schema or Stage generations",
			)
		}
		if totalChunks > math.MaxUint64-manifest.TotalChunkCount ||
			selectedBytes > math.MaxUint64-dataset.LogicalBytes {
			return nil, SchemaDescriptor{}, moerr.NewInternalErrorNoCtxf(
				"Lifecycle range Restore size overflows uint64",
			)
		}
		plans = append(plans, restoreDatasetPlan{
			dataset:   dataset,
			manifest:  manifest,
			chunkBase: totalChunks,
		})
		totalChunks += manifest.TotalChunkCount
		selectedBytes += dataset.LogicalBytes
		if totalChunks > maxRestoreRangeChunks {
			return nil, SchemaDescriptor{}, moerr.NewInternalErrorNoCtxf(
				"RESOURCE_BLOCKED: Lifecycle range Restore exceeds the certified Chunk limit %d",
				maxRestoreRangeChunks,
			)
		}
	}
	if len(plans) == 0 {
		return nil, SchemaDescriptor{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle Archive has no Dataset overlapping the requested range",
		)
	}
	return plans, schema, nil
}

func addRestoreRangeManifestBytes(
	current uint64,
	manifest *ArchiveManifest,
) (uint64, error) {
	encoded, _, err := MarshalArchiveManifest(manifest)
	if err != nil {
		return 0, err
	}
	size := uint64(len(encoded))
	if current > math.MaxUint64-size ||
		current+size > maxRestoreRangeManifestBytes {
		return 0, moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle range Restore Manifest bytes exceed the certified limit %d",
			maxRestoreRangeManifestBytes,
		)
	}
	return current + size, nil
}

func rangeRestoreChunkReceipt(
	ctx context.Context,
	restoreID string,
	datasetID string,
	globalOrdinal uint64,
	localOrdinal uint64,
	sourceChunk ArchiveChunk,
	schemaDigest [sha256.Size]byte,
	rows [][]CanonicalCell,
) (RestoreChunkReceipt, error) {
	encoder := NewCanonicalValueEncoder(schemaDigest)
	for _, row := range rows {
		if err := encoder.WriteRow(ctx, row); err != nil {
			return RestoreChunkReceipt{}, err
		}
	}
	filtered := ArchiveChunk{
		ChunkOrdinal:         globalOrdinal,
		FileOrdinal:          sourceChunk.FileOrdinal,
		RowGroupOrdinal:      sourceChunk.RowGroupOrdinal,
		RowCount:             encoder.RowCount(),
		LogicalBytes:         encoder.LogicalBytes(),
		CanonicalContentHash: encoder.Sum(),
	}
	return RestoreChunkReceipt{
		RestoreID:            restoreID,
		DatasetID:            datasetID,
		DatasetChunkOrdinal:  localOrdinal,
		ChunkOrdinal:         globalOrdinal,
		FileOrdinal:          filtered.FileOrdinal,
		RowGroupOrdinal:      filtered.RowGroupOrdinal,
		ChunkDigest:          chunkReceiptDigest(filtered),
		RowCount:             filtered.RowCount,
		LogicalBytes:         filtered.LogicalBytes,
		CanonicalContentHash: filtered.CanonicalContentHash,
	}, nil
}

func validateRangeRestoreReceipts(
	plans []restoreDatasetPlan,
	receipts []RestoreChunkReceipt,
	start int64,
	end int64,
) ([]ArchiveChunk, uint64, error) {
	if len(plans) == 0 || start >= end {
		return nil, 0, moerr.NewInternalErrorNoCtxf("Lifecycle range Restore receipt input is invalid")
	}
	chunks := make([]ArchiveChunk, len(receipts))
	var restoredRows uint64
	planIndex := 0
	for ordinal, receipt := range receipts {
		globalOrdinal := uint64(ordinal)
		for planIndex+1 < len(plans) &&
			globalOrdinal >= plans[planIndex+1].chunkBase {
			planIndex++
		}
		plan := plans[planIndex]
		localOrdinal := globalOrdinal - plan.chunkBase
		if localOrdinal >= plan.manifest.TotalChunkCount {
			return nil, 0, moerr.NewInternalErrorNoCtxf("Lifecycle range Restore receipt sequence is corrupt")
		}
		source := plan.manifest.Files[localOrdinal].Chunks[0]
		chunk := ArchiveChunk{
			ChunkOrdinal:         globalOrdinal,
			FileOrdinal:          receipt.FileOrdinal,
			RowGroupOrdinal:      receipt.RowGroupOrdinal,
			RowCount:             receipt.RowCount,
			LogicalBytes:         receipt.LogicalBytes,
			CanonicalContentHash: receipt.CanonicalContentHash,
		}
		if receipt.ChunkOrdinal != globalOrdinal ||
			receipt.DatasetID != plan.dataset.DatasetID ||
			receipt.DatasetChunkOrdinal != localOrdinal ||
			receipt.FileOrdinal != source.FileOrdinal ||
			receipt.RowGroupOrdinal != source.RowGroupOrdinal ||
			receipt.ChunkDigest != chunkReceiptDigest(chunk) {
			return nil, 0, moerr.NewInternalErrorNoCtxf("Lifecycle range Restore receipt sequence is corrupt")
		}
		if restoredRows > math.MaxUint64-receipt.RowCount {
			return nil, 0, moerr.NewInternalErrorNoCtxf("Lifecycle range Restore row count overflows uint64")
		}
		restoredRows += receipt.RowCount
		chunks[ordinal] = chunk
	}
	if len(receipts) == 0 ||
		uint64(len(receipts)) != plans[len(plans)-1].chunkBase+
			plans[len(plans)-1].manifest.TotalChunkCount {
		return nil, 0, moerr.NewInternalErrorNoCtxf("Lifecycle range Restore receipts are incomplete")
	}
	return chunks, restoredRows, nil
}

func mergeRestoreAutoIncrementMaxima(plans []restoreDatasetPlan) []AutoIncrementMax {
	values := make(map[uint32]*big.Int)
	for _, plan := range plans {
		for _, maximum := range plan.manifest.AutoIncrementMaxima {
			value, ok := new(big.Int).SetString(maximum.Value, 10)
			if !ok {
				continue
			}
			if current := values[maximum.ColumnOrdinal]; current == nil ||
				value.Cmp(current) > 0 {
				values[maximum.ColumnOrdinal] = value
			}
		}
	}
	return encodeAutoIncrementMaxima(values)
}

func BuildRestoreSelectionDigest(
	scope string,
	logicalTableID uint64,
	start int64,
	end int64,
	datasetIDs []string,
) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("matrixone/lifecycle/restore-selection/v1"))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], logicalTableID)
	hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(start))
	hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(end))
	hash.Write(number[:])
	hash.Write([]byte(scope))
	for _, datasetID := range datasetIDs {
		binary.BigEndian.PutUint64(number[:], uint64(len(datasetID)))
		hash.Write(number[:])
		hash.Write([]byte(datasetID))
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// ParseLifecycleRestoreBoundary converts a user boundary into the same frozen
// physical value used by the Archive Dataset range. TIMESTAMP is interpreted
// in UTC; DATE and DATETIME preserve their SQL wall-clock representation.
func ParseLifecycleRestoreBoundary(
	ctx context.Context,
	value string,
	oid types.T,
) (int64, error) {
	switch oid {
	case types.T_date:
		parsed, err := types.ParseDateCast(value)
		return int64(parsed), err
	case types.T_datetime:
		parsed, err := types.ParseDatetime(value, 6)
		return int64(parsed), err
	case types.T_timestamp:
		parsed, err := types.ParseTimestamp(time.UTC, value, 6)
		return int64(parsed), err
	default:
		return 0, moerr.NewInvalidInputf(
			ctx,
			"Lifecycle Restore range does not support column type %s",
			oid,
		)
	}
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
		(dataset.HasLifecycleRange && (manifest.LifecycleRange == nil ||
			*manifest.LifecycleRange != dataset.LifecycleRange)) ||
		manifest.VerificationStatus != "FULL_READBACK_VERIFIED" {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Dataset and Manifest identity mismatch")
	}
	manifestDigest, err := manifestDigestFromKey(dataset.ManifestKey)
	if err != nil || manifestDigest != dataset.ManifestDigest {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Dataset Manifest digest mismatch")
	}
	prefix := path.Dir(dataset.ManifestKey)
	if !lifecycleRootScopedPrefix(
		prefix,
		dataset.RootID,
		dataset.AttemptID,
	) || !cleanupKeyWithinPrefix(dataset.ManifestKey, prefix) {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Dataset Manifest namespace mismatch")
	}
	for _, file := range manifest.Files {
		if !cleanupKeyWithinPrefix(file.Key, prefix) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Dataset Payload namespace mismatch")
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
		return moerr.NewInternalErrorNoCtxf("Lifecycle Purge input is incomplete")
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
