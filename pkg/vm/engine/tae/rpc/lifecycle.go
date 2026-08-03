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

package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
)

const (
	lifecycleCommitProtocolVersion    = 1
	lifecycleWholeMaxSources          = 64
	lifecycleWholeMaxSourceBytes      = uint64(4 << 30)
	lifecycleRewriteMaxCreatedObjects = 32
	lifecycleRewriteMaxDeltaRows      = uint64(100_000)
	lifecycleRewriteMaxDeltaBytes     = uint64(32 << 20)
)

func validateLifecycleCommitControl(
	entry *api.LifecycleCommitEntry,
	now time.Time,
) error {
	if entry == nil {
		return fmt.Errorf("nil Lifecycle commit control")
	}
	if entry.ProtocolVersion != lifecycleCommitProtocolVersion {
		return fmt.Errorf("unsupported Lifecycle protocol version %d", entry.ProtocolVersion)
	}
	if entry.AttemptId == "" ||
		entry.DatabaseId == 0 ||
		entry.PhysicalTableId == 0 ||
		entry.BindingGeneration == 0 ||
		entry.SourceSnapshotTs == nil ||
		entry.SourceSnapshotTs.PhysicalTime == 0 ||
		len(entry.SchemaDigest) != sha256.Size ||
		len(entry.SourceSetDigest) != sha256.Size {
		return fmt.Errorf("Lifecycle commit identity is incomplete")
	}
	if entry.FinalPrepareDeadlineUnixNano <= now.UnixNano() {
		return fmt.Errorf("Lifecycle final prepare deadline has expired")
	}
	isArchive := entry.DatasetId != ""
	isTTL := entry.ReceiptId != ""
	if isArchive == isTTL {
		return fmt.Errorf("Lifecycle commit must contain exactly one Dataset or TTL Receipt")
	}
	if (isArchive || entry.RetireMode == api.LifecycleCommitEntry_Rewrite) &&
		entry.RootId == "" {
		return fmt.Errorf("Lifecycle commit with external state has no Cleanup Root")
	}
	sourceLimit := lifecycleWholeMaxSources
	if entry.RetireMode == api.LifecycleCommitEntry_Rewrite {
		sourceLimit = 1
		if len(entry.CreatedObjectStats) == 0 ||
			len(entry.CreatedObjectStats) > lifecycleRewriteMaxCreatedObjects ||
			len(entry.TransferBookingLocations) == 0 ||
			len(entry.TransferMappingDigest) != sha256.Size ||
			entry.MaxDeltaRows == 0 ||
			entry.MaxDeltaRows > lifecycleRewriteMaxDeltaRows ||
			entry.MaxDeltaBytes == 0 ||
			entry.MaxDeltaBytes > lifecycleRewriteMaxDeltaBytes ||
			entry.MaxDeltaBlocks == 0 ||
			entry.MergeLevel < 0 ||
			entry.MergeLevel > 7 {
			return fmt.Errorf("Lifecycle Rewrite control is incomplete")
		}
	} else if entry.RetireMode != api.LifecycleCommitEntry_Whole {
		return fmt.Errorf("unknown Lifecycle retire mode %d", entry.RetireMode)
	} else if len(entry.CreatedObjectStats) != 0 ||
		len(entry.TransferBookingLocations) != 0 ||
		len(entry.TransferMappingDigest) != 0 ||
		entry.MaxDeltaRows != 0 ||
		entry.MaxDeltaBytes != 0 ||
		entry.MaxDeltaBlocks != 0 ||
		entry.MergeLevel != 0 {
		return fmt.Errorf("Lifecycle Whole control contains Rewrite state")
	}
	if len(entry.DataSourceObjectStats) == 0 ||
		len(entry.DataSourceObjectStats) > sourceLimit {
		return fmt.Errorf("Lifecycle source Object count is outside the certified limit")
	}
	seen := make(map[objectio.ObjectId]struct{}, len(entry.DataSourceObjectStats))
	var sourceBlockCount uint32
	var sourceBytes uint64
	for _, raw := range entry.DataSourceObjectStats {
		if len(raw) != objectio.ObjectStatsLen {
			return fmt.Errorf("Lifecycle source ObjectStats has invalid length %d", len(raw))
		}
		var stats objectio.ObjectStats
		copy(stats[:], raw)
		if stats.GetAppendable() {
			return fmt.Errorf("Lifecycle cannot retire an appendable Object")
		}
		objectID := *stats.ObjectName().ObjectId()
		if _, exists := seen[objectID]; exists {
			return fmt.Errorf("Lifecycle source Object is duplicated")
		}
		seen[objectID] = struct{}{}
		sourceBlockCount += uint32(stats.BlkCnt())
		objectBytes := uint64(max(stats.OriginSize(), stats.Size(), 1))
		sourceBytes += objectBytes
		if sourceBytes > lifecycleWholeMaxSourceBytes {
			return fmt.Errorf("Lifecycle source bytes exceed the certified limit")
		}
	}
	if entry.RetireMode == api.LifecycleCommitEntry_Rewrite &&
		entry.MaxDeltaBlocks > sourceBlockCount {
		return fmt.Errorf("Lifecycle delta block limit exceeds source layout")
	}
	if !bytes.Equal(
		entry.SourceSetDigest,
		lifecycleSourceSetDigest(entry.DataSourceObjectStats),
	) {
		return fmt.Errorf("Lifecycle source set digest mismatch")
	}
	for _, raw := range entry.CreatedObjectStats {
		if len(raw) != objectio.ObjectStatsLen {
			return fmt.Errorf("Lifecycle created ObjectStats has invalid length %d", len(raw))
		}
	}
	return nil
}

func validateLifecycleProtectionJobID(attemptID, jobID string) error {
	if attemptID == "" || jobID == "" {
		return fmt.Errorf("Lifecycle SyncProtection identity is incomplete")
	}
	// Production jobs are named <attempt-id>-<digest>. A job from another
	// attempt must be rejected before Booking I/O or any TAE mutation.
	if !strings.HasPrefix(jobID, attemptID+"-") {
		return fmt.Errorf("Lifecycle SyncProtection does not belong to the attempt")
	}
	return nil
}

func lifecycleSourceSetDigest(values [][]byte) []byte {
	sorted := make([]objectio.ObjectStats, len(values))
	for index, raw := range values {
		if len(raw) != objectio.ObjectStatsLen {
			return nil
		}
		copy(sorted[index][:], raw)
	}
	slices.SortFunc(sorted, func(left, right objectio.ObjectStats) int {
		return bytes.Compare(left.ObjectName(), right.ObjectName())
	})
	sum := sha256.New()
	_, _ = sum.Write([]byte("matrixone/lifecycle/data-sources/v1"))
	for index := range sorted {
		_, _ = sum.Write(sorted[index][:])
	}
	return sum.Sum(nil)
}

func (h *Handle) HandleLifecycleCommit(
	ctx context.Context,
	txn txnif.AsyncTxn,
	entry *api.LifecycleCommitEntry,
) error {
	if err := validateLifecycleCommitControl(entry, time.Now()); err != nil {
		return moerr.NewInvalidInputf(ctx, "%v", err)
	}
	if err := validateLifecycleProtectionJobID(
		entry.AttemptId,
		txn.GetSyncProtectionJobID(),
	); err != nil {
		return moerr.NewInvalidInputf(ctx, "%v", err)
	}
	database, err := txn.GetDatabaseByID(entry.DatabaseId)
	if err != nil {
		return err
	}
	relation, err := database.GetRelationByID(entry.PhysicalTableId)
	if err != nil {
		return err
	}

	type exactSource struct {
		id       types.Objectid
		expected objectio.ObjectStats
		object   handle.Object
	}
	sources := make([]exactSource, 0, len(entry.DataSourceObjectStats))
	closeSources := func() {
		for index := range sources {
			if sources[index].object != nil {
				_ = sources[index].object.Close()
			}
		}
	}
	defer closeSources()

	// Validate the complete source set before installing the first DropIntent.
	// Object-not-found and any ObjectStats drift are conflicts, never success.
	for _, raw := range entry.DataSourceObjectStats {
		var expected objectio.ObjectStats
		copy(expected[:], raw)
		objectID := *expected.ObjectName().ObjectId()
		object, err := relation.GetObject(&objectID, false)
		if err != nil {
			return err
		}
		meta, ok := object.GetMeta().(*catalog.ObjectEntry)
		if !ok || meta == nil ||
			!bytes.Equal(meta.GetObjectStats()[:], expected[:]) {
			_ = object.Close()
			return moerr.NewTxnWWConflictNoCtx(
				entry.PhysicalTableId,
				"Lifecycle exact source ObjectStats changed",
			)
		}
		sources = append(sources, exactSource{
			id:       objectID,
			expected: expected,
			object:   object,
		})
	}
	if entry.RetireMode == api.LifecycleCommitEntry_Rewrite {
		schema, ok := relation.Schema(false).(*catalog.Schema)
		if !ok || schema == nil || schema.Extra == nil {
			return moerr.NewInternalError(
				ctx,
				"Lifecycle Rewrite cannot resolve the physical schema",
			)
		}
		var sourceStats objectio.ObjectStats
		copy(sourceStats[:], entry.DataSourceObjectStats[0])
		if err := validateLifecycleBookingHeader(
			entry.TransferBookingLocations,
			int(sourceStats.BlkCnt()),
			schema.Extra.BlockMaxRows,
		); err != nil {
			return moerr.NewInvalidInputf(ctx, "%v", err)
		}
		mergeEntry := &api.MergeCommitEntry{
			DbId:        entry.DatabaseId,
			TblId:       entry.PhysicalTableId,
			StartTs:     *entry.SourceSnapshotTs,
			MergedObjs:  cloneByteSlices(entry.DataSourceObjectStats),
			CreatedObjs: cloneByteSlices(entry.CreatedObjectStats),
			BookingLoc: append(
				[]string(nil),
				entry.TransferBookingLocations...,
			),
			Level: entry.MergeLevel,
		}
		transferMaps, err := marshalTransferMapsWithOptions(
			ctx,
			mergeEntry,
			h.db.Runtime.SID(),
			h.db.Runtime.Fs,
			transferMapLoadOptions{
				deleteAfterRead:    false,
				strictSourceBounds: true,
			},
		)
		if err != nil {
			return err
		}
		transferTable := mergesort.NewTransferTableFromMaps(transferMaps)
		if err := validateLifecycleTransferTable(
			entry.CreatedObjectStats,
			transferTable,
			schema.Extra.BlockMaxRows,
		); err != nil {
			transferTable.Release()
			return moerr.NewInvalidInputf(ctx, "%v", err)
		}
		actualDigest := mergesort.TransferMappingDigest(
			entry.CreatedObjectStats,
			transferTable,
		)
		if !bytes.Equal(actualDigest[:], entry.TransferMappingDigest) {
			transferTable.Release()
			return moerr.NewInvalidInput(
				ctx,
				"Lifecycle Rewrite transfer mapping digest mismatch",
			)
		}
		_, err = jobs.HandleLifecycleMergeEntryInTxn(
			ctx,
			txn,
			fmt.Sprintf("lifecycle-rewrite-%s", entry.AttemptId),
			mergeEntry,
			transferTable,
			h.db.Runtime,
			types.TimestampToTS(*entry.SourceSnapshotTs),
			time.Unix(0, entry.FinalPrepareDeadlineUnixNano),
			entry.MaxDeltaRows,
			entry.MaxDeltaBytes,
			entry.MaxDeltaBlocks,
		)
		return err
	}
	droppedObjects := make([]*catalog.ObjectEntry, 0, len(sources))
	for index := range sources {
		if err := relation.SoftDeleteObject(&sources[index].id, false); err != nil {
			return err
		}
		droppedObjects = append(
			droppedObjects,
			sources[index].object.GetMeta().(*catalog.ObjectEntry).GetLatestNode(),
		)
	}
	txnEntry, err := txnentries.NewLifecycleWholeObjectsEntry(
		txn,
		fmt.Sprintf("lifecycle-whole-%s", entry.AttemptId),
		relation,
		droppedObjects,
		types.TimestampToTS(*entry.SourceSnapshotTs),
		time.Unix(0, entry.FinalPrepareDeadlineUnixNano),
		h.db.Runtime,
	)
	if err != nil {
		return err
	}
	readSet := make([]*common.ID, 0, len(droppedObjects))
	for _, object := range droppedObjects {
		readSet = append(readSet, object.AsCommonID())
	}
	if err := txn.LogTxnEntry(
		entry.DatabaseId,
		entry.PhysicalTableId,
		txnEntry,
		readSet,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func validateLifecycleBookingHeader(
	locations []string,
	expectedBlockCount int,
	blockMaxRows uint32,
) error {
	if len(locations) == 0 || len(locations[0]) != 4 {
		return fmt.Errorf("Lifecycle Rewrite booking header is missing")
	}
	blockCount := int(types.DecodeInt32([]byte(locations[0])))
	if blockCount <= 0 ||
		blockCount != expectedBlockCount ||
		len(locations) <= blockCount+1 {
		return fmt.Errorf("Lifecycle Rewrite booking block layout is invalid")
	}
	for block := 0; block < blockCount; block++ {
		if len(locations[block+1]) != 4 {
			return fmt.Errorf("Lifecycle Rewrite booking row header is invalid")
		}
		rows := types.DecodeInt32([]byte(locations[block+1]))
		if rows < 0 || uint32(rows) > blockMaxRows {
			return fmt.Errorf("Lifecycle Rewrite booking row count is outside schema limits")
		}
	}
	return nil
}

func validateLifecycleTransferTable(
	createdValues [][]byte,
	table *mergesort.TransferTable,
	blockMaxRows uint32,
) error {
	if table == nil || blockMaxRows == 0 {
		return fmt.Errorf("Lifecycle Rewrite transfer layout is empty")
	}
	created := make([]objectio.ObjectStats, len(createdValues))
	var expectedDestinations uint64
	for index, raw := range createdValues {
		if len(raw) != objectio.ObjectStatsLen {
			return fmt.Errorf("Lifecycle Rewrite created ObjectStats is malformed")
		}
		copy(created[index][:], raw)
		expectedDestinations += uint64(created[index].Rows())
	}
	var mappedDestinations uint64
	for block := 0; block < table.Len(); block++ {
		for _, value := range table.GetBlockMap(block) {
			if value.ObjIdx == api.NoTransfer {
				continue
			}
			if int(value.ObjIdx) >= len(created) ||
				uint32(value.BlkIdx) >= created[value.ObjIdx].BlkCnt() ||
				value.RowIdx >= blockMaxRows {
				return fmt.Errorf("Lifecycle Rewrite destination is out of bounds")
			}
			mappedDestinations++
		}
	}
	if mappedDestinations != expectedDestinations {
		return fmt.Errorf(
			"Lifecycle Rewrite destination count %d does not match created rows %d",
			mappedDestinations,
			expectedDestinations,
		)
	}
	return nil
}

func cloneByteSlices(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index := range values {
		cloned[index] = append([]byte(nil), values[index]...)
	}
	return cloned
}
