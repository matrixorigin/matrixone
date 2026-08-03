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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

// RewriteBlockClassifier classifies rows of one complete physical source
// Block. The returned bitmap contains E (expired) rows only. The input
// snapshotDeleted bitmap contains D and must not be changed.
type RewriteBlockClassifier func(
	ctx context.Context,
	physicalBlock *batch.Batch,
	snapshotDeleted *nulls.Nulls,
) (expired *nulls.Nulls, err error)

// RewriteExpiredConsumer consumes E before the same physical Block is handed
// to DoMergeAndWrite. It must finish synchronously and must not retain Batch
// vectors after returning.
type RewriteExpiredConsumer func(
	ctx context.Context,
	physicalBlock *batch.Batch,
	expired *nulls.Nulls,
) error

// RewriteHost is the thin Lifecycle wrapper around MO's existing Merge
// producer. It does not produce Objects, reorder rows, or construct
// destination mappings. The wrapped MergeTaskHost remains the sole owner of
// those operations.
type RewriteHost struct {
	mergesort.MergeTaskHost
	classify RewriteBlockClassifier
	archive  RewriteExpiredConsumer
	report   ObjectScanReport
}

func NewRewriteHost(
	base mergesort.MergeTaskHost,
	classify RewriteBlockClassifier,
	archive RewriteExpiredConsumer,
) (*RewriteHost, error) {
	if base == nil || classify == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite requires a Merge host and classifier",
		)
	}
	if base.GetObjectCnt() != 1 {
		return nil, moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite must have exactly one source Object",
		)
	}
	blockCounts := base.GetBlkCnts()
	if len(blockCounts) != 1 || blockCounts[0] <= 0 || base.GetTotalRowCnt() == 0 {
		return nil, moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite source layout is incomplete",
		)
	}
	return &RewriteHost{
		MergeTaskHost: base,
		classify:      classify,
		archive:       archive,
		report: NewObjectScanReport(
			uint32(blockCounts[0]),
			uint64(base.GetTotalRowCnt()),
		),
	}, nil
}

// DoTransfer is deliberately unconditional. A table comment may disable
// transfer for an ordinary CN Merge, but Lifecycle must transfer every post-S
// DELETE that targets an L row.
func (host *RewriteHost) DoTransfer() bool {
	return true
}

// LoadNextBatch preserves the physical Batch and its Object/Block/row order.
// D and E are expressed solely through the delete bitmap supplied to the
// existing Merge producer. Lifecycle never filters L into a new Batch.
func (host *RewriteHost) LoadNextBatch(
	ctx context.Context,
	objIdx uint32,
	reuseBatch *batch.Batch,
) (*batch.Batch, *nulls.Nulls, func(), error) {
	value, snapshotDeleted, release, err := host.MergeTaskHost.LoadNextBatch(
		ctx,
		objIdx,
		reuseBatch,
	)
	if err != nil {
		return nil, nil, release, err
	}
	if value == nil || release == nil {
		if release != nil {
			release()
		}
		return nil, nil, nil, moerr.NewInternalError(
			ctx,
			"Lifecycle Rewrite reader returned incomplete ownership",
		)
	}
	expired, err := host.classify(ctx, value, snapshotDeleted)
	if err != nil {
		release()
		return nil, nil, nil, err
	}
	if err := validateRewriteClasses(value.RowCount(), snapshotDeleted, expired); err != nil {
		release()
		return nil, nil, nil, err
	}
	if err := host.report.ObserveClassifiedBlock(
		value.RowCount(),
		snapshotDeleted,
		expired,
	); err != nil {
		release()
		return nil, nil, nil, err
	}
	if nulls.Any(expired) && host.archive != nil {
		if err := host.archive(ctx, value, expired); err != nil {
			release()
			return nil, nil, nil, err
		}
	}
	deletedOrExpired := nulls.NewWithSize(value.RowCount())
	if snapshotDeleted != nil {
		deletedOrExpired.Or(snapshotDeleted)
	}
	if expired != nil {
		deletedOrExpired.Or(expired)
	}
	return value, deletedOrExpired, release, nil
}

// ScanReport is called only after DoMergeAndWrite has reached EOF. A short,
// duplicate, reordered, or row-incomplete read is a hard failure even if the
// Archive payload written so far passed full readback.
func (host *RewriteHost) ScanReport() (ObjectScanReport, error) {
	report := host.report
	if err := report.ValidateComplete(); err != nil {
		return report, err
	}
	return report, nil
}

func validateRewriteClasses(
	rowCount int,
	snapshotDeleted *nulls.Nulls,
	expired *nulls.Nulls,
) error {
	probe := NewObjectScanReport(1, uint64(rowCount))
	return probe.ObserveClassifiedBlock(rowCount, snapshotDeleted, expired)
}
