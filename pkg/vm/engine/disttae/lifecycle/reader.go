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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type lifecycleTombstoneSelector interface {
	SelectLifecycleTombstoneObjects(
		context.Context,
		types.TS,
		[]objectio.ObjectId,
		logtailreplay.LifecycleTombstoneSelectionLimits,
	) ([]objectio.ObjectEntry, int, error)
}

type ProtectionSet struct {
	DataSources         []objectio.ObjectStats
	ProtectedTombstones []objectio.ObjectStats
	ProtectedObjects    []objectio.ObjectStats
	SourceSetDigest     [32]byte
	ProtectionSetDigest [32]byte
}

// SelectProtectionSet derives a conservative superset of physical Tombstone
// Objects that MO's existing snapshot reader may consume. The snapshot reader
// continues to use the same PartitionState and its ordinary visibility logic;
// Lifecycle uses the selected identities only for SyncProtection. Tombstones
// are never added to DataSources and therefore can never enter a retirement
// entry.
func SelectProtectionSet(
	ctx context.Context,
	selector lifecycleTombstoneSelector,
	snapshot types.TS,
	dataSources []objectio.ObjectEntry,
	limits logtailreplay.LifecycleTombstoneSelectionLimits,
) (ProtectionSet, error) {
	if selector == nil || len(dataSources) == 0 {
		return ProtectionSet{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle protection selection requires a selector and Data Objects",
		)
	}
	sourceIDs := make([]objectio.ObjectId, len(dataSources))
	set := ProtectionSet{
		DataSources: make([]objectio.ObjectStats, len(dataSources)),
	}
	for index, entry := range dataSources {
		if !entry.Visible(snapshot) || entry.GetAppendable() {
			return ProtectionSet{}, moerr.NewInvalidInput(
				ctx,
				"Lifecycle protection source is not an exact visible Data Object",
			)
		}
		set.DataSources[index] = entry.ObjectStats
		sourceIDs[index] = *entry.ObjectStats.ObjectName().ObjectId()
	}
	tombstones, _, err := selector.SelectLifecycleTombstoneObjects(
		ctx,
		snapshot,
		sourceIDs,
		limits,
	)
	if err != nil {
		return ProtectionSet{}, err
	}
	set.ProtectedTombstones = make([]objectio.ObjectStats, len(tombstones))
	for index := range tombstones {
		set.ProtectedTombstones[index] = tombstones[index].ObjectStats
	}
	set.ProtectedObjects = make(
		[]objectio.ObjectStats,
		0,
		len(set.DataSources)+len(set.ProtectedTombstones),
	)
	set.ProtectedObjects = append(set.ProtectedObjects, set.DataSources...)
	set.ProtectedObjects = append(set.ProtectedObjects, set.ProtectedTombstones...)
	set.SourceSetDigest = digestObjectStats(
		"matrixone/lifecycle/data-sources/v1",
		set.DataSources,
	)
	set.ProtectionSetDigest = digestObjectStats(
		"matrixone/lifecycle/protection-set/v1",
		set.ProtectedObjects,
	)
	return set, nil
}

func digestObjectStats(domain string, values []objectio.ObjectStats) [32]byte {
	sorted := append([]objectio.ObjectStats(nil), values...)
	slices.SortFunc(sorted, func(left, right objectio.ObjectStats) int {
		return bytes.Compare(left.ObjectName(), right.ObjectName())
	})
	sum := sha256.New()
	_, _ = sum.Write([]byte(domain))
	for index := range sorted {
		_, _ = sum.Write(sorted[index][:])
	}
	var digest [32]byte
	copy(digest[:], sum.Sum(nil))
	return digest
}

type SyncProtectionClient interface {
	Register(
		ctx context.Context,
		jobID string,
		objects []objectio.ObjectStats,
		validUntil time.Time,
	) error
	StatExact(ctx context.Context, objects []objectio.ObjectStats) error
	Renew(ctx context.Context, jobID string, validUntil time.Time) error
	Release(ctx context.Context, jobID string) error
}

type ProtectionLease struct {
	client   SyncProtectionClient
	jobID    string
	released bool
	mu       sync.Mutex
}

func AcquireProtection(
	ctx context.Context,
	client SyncProtectionClient,
	attemptID string,
	set ProtectionSet,
	validUntil time.Time,
) (*ProtectionLease, error) {
	if client == nil || attemptID == "" || len(set.ProtectedObjects) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle SyncProtection identity is incomplete")
	}
	if !validUntil.After(time.Now()) {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle SyncProtection deadline has expired")
	}
	jobDigest := sha256.Sum256(append(
		append([]byte("matrixone/lifecycle/sync-protection/v1"), []byte(attemptID)...),
		set.ProtectionSetDigest[:]...,
	))
	jobID := attemptID + "-" + hex.EncodeToString(jobDigest[:8])
	if err := client.Register(ctx, jobID, set.ProtectedObjects, validUntil); err != nil {
		return nil, err
	}
	lease := &ProtectionLease{client: client, jobID: jobID}
	if err := client.StatExact(ctx, set.ProtectedObjects); err != nil {
		releaseErr := lease.Release(ctx)
		return nil, errors.Join(err, releaseErr)
	}
	return lease, nil
}

func (lease *ProtectionLease) JobID() string {
	return lease.jobID
}

func (lease *ProtectionLease) Renew(
	ctx context.Context,
	validUntil time.Time,
) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return fmt.Errorf("Lifecycle SyncProtection %s is already released", lease.jobID)
	}
	if !validUntil.After(time.Now()) {
		return fmt.Errorf("Lifecycle SyncProtection renewal deadline has expired")
	}
	return lease.client.Renew(ctx, lease.jobID, validUntil)
}

func (lease *ProtectionLease) Release(ctx context.Context) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return nil
	}
	if err := lease.client.Release(ctx, lease.jobID); err != nil {
		return err
	}
	lease.released = true
	return nil
}

type ExactBlockConsumer func(*batch.Batch, *nulls.Bitmap) error

// ObjectScanReport is an in-memory proof that one Lifecycle attempt consumed
// one exact source Object completely. It is deliberately not persisted or
// sent to TN: the final transaction still fences the immutable ObjectStats,
// while this report prevents CN from retiring an Object after a short read.
type ObjectScanReport struct {
	ExpectedBlocks      uint32
	ScannedBlocks       uint32
	ExpectedRows        uint64
	ScannedRows         uint64
	SnapshotDeletedRows uint64
	ExpiredRows         uint64
	LiveRows            uint64

	classified bool
}

func NewObjectScanReport(expectedBlocks uint32, expectedRows uint64) ObjectScanReport {
	return ObjectScanReport{
		ExpectedBlocks: expectedBlocks,
		ExpectedRows:   expectedRows,
	}
}

// ObservePhysicalBlock accounts for one complete physical Block before the
// caller classifies its visible rows. The bitmap is validated instead of
// trusting Count(), because an out-of-range bit would otherwise make the
// D/E/L conservation equation meaningless.
func (report *ObjectScanReport) ObservePhysicalBlock(
	rowCount int,
	snapshotDeleted *nulls.Nulls,
) error {
	if report == nil || rowCount < 0 {
		return fmt.Errorf("Lifecycle Object scan report input is invalid")
	}
	deletedRows, err := lifecycleBitmapRowCount(rowCount, snapshotDeleted)
	if err != nil {
		return err
	}
	report.ScannedBlocks++
	report.ScannedRows += uint64(rowCount)
	report.SnapshotDeletedRows += deletedRows
	return nil
}

// ObserveClassifiedBlock accounts for D/E/L in the same physical Batch that
// is passed to DoMergeAndWrite. Lifecycle must never pre-filter L into a new
// Batch before calling this method.
func (report *ObjectScanReport) ObserveClassifiedBlock(
	rowCount int,
	snapshotDeleted *nulls.Nulls,
	expired *nulls.Nulls,
) error {
	if report == nil || rowCount < 0 {
		return fmt.Errorf("Lifecycle Object scan report input is invalid")
	}
	deletedRows, err := lifecycleBitmapRowCount(rowCount, snapshotDeleted)
	if err != nil {
		return err
	}
	expiredRows, err := lifecycleBitmapRowCount(rowCount, expired)
	if err != nil {
		return err
	}
	if snapshotDeleted != nil && expired != nil {
		for _, row := range expired.GetBitmap().ToArray() {
			if snapshotDeleted.Contains(row) {
				return fmt.Errorf(
					"Lifecycle row cannot be both snapshot-deleted and expired",
				)
			}
		}
	}
	visibleRows := uint64(rowCount) - deletedRows
	if expiredRows > visibleRows {
		return fmt.Errorf("Lifecycle expired rows exceed visible rows")
	}
	report.ScannedBlocks++
	report.ScannedRows += uint64(rowCount)
	report.SnapshotDeletedRows += deletedRows
	report.ExpiredRows += expiredRows
	report.LiveRows += visibleRows - expiredRows
	report.classified = true
	return nil
}

// SetVisibleClassification completes a physical-only report produced by the
// exact Whole reader. The caller must derive E/L from the same borrowed
// Batches before they are released.
func (report *ObjectScanReport) SetVisibleClassification(
	expiredRows uint64,
	liveRows uint64,
) error {
	if report == nil || report.classified {
		return fmt.Errorf("Lifecycle Object scan classification is duplicated")
	}
	visibleRows := report.ScannedRows - report.SnapshotDeletedRows
	if expiredRows > visibleRows || liveRows != visibleRows-expiredRows {
		return fmt.Errorf("Lifecycle Object scan classification is incomplete")
	}
	report.ExpiredRows = expiredRows
	report.LiveRows = liveRows
	report.classified = true
	return nil
}

func (report ObjectScanReport) ValidatePhysicalComplete() error {
	if report.ExpectedBlocks == 0 || report.ExpectedRows == 0 ||
		report.ScannedBlocks != report.ExpectedBlocks ||
		report.ScannedRows != report.ExpectedRows ||
		report.SnapshotDeletedRows > report.ScannedRows {
		return fmt.Errorf(
			"Lifecycle Object scan is incomplete: blocks=%d/%d rows=%d/%d deleted=%d",
			report.ScannedBlocks,
			report.ExpectedBlocks,
			report.ScannedRows,
			report.ExpectedRows,
			report.SnapshotDeletedRows,
		)
	}
	return nil
}

func (report ObjectScanReport) ValidateComplete() error {
	if err := report.ValidatePhysicalComplete(); err != nil {
		return err
	}
	if !report.classified ||
		report.SnapshotDeletedRows+report.ExpiredRows+report.LiveRows != report.ScannedRows {
		return fmt.Errorf(
			"Lifecycle Object scan D/E/L conservation failed: D=%d E=%d L=%d rows=%d",
			report.SnapshotDeletedRows,
			report.ExpiredRows,
			report.LiveRows,
			report.ScannedRows,
		)
	}
	return nil
}

func (report *ObjectScanReport) Add(other ObjectScanReport) error {
	if report == nil {
		return fmt.Errorf("Lifecycle Object scan aggregate is incomplete")
	}
	if err := other.ValidateComplete(); err != nil {
		return err
	}
	report.ExpectedBlocks += other.ExpectedBlocks
	report.ScannedBlocks += other.ScannedBlocks
	report.ExpectedRows += other.ExpectedRows
	report.ScannedRows += other.ScannedRows
	report.SnapshotDeletedRows += other.SnapshotDeletedRows
	report.ExpiredRows += other.ExpiredRows
	report.LiveRows += other.LiveRows
	report.classified = true
	return nil
}

func lifecycleBitmapRowCount(rowCount int, bitmap *nulls.Nulls) (uint64, error) {
	if bitmap == nil {
		return 0, nil
	}
	for _, row := range bitmap.GetBitmap().ToArray() {
		if row >= uint64(rowCount) {
			return 0, fmt.Errorf("Lifecycle row bitmap contains an out-of-range row")
		}
	}
	return uint64(bitmap.Count()), nil
}
