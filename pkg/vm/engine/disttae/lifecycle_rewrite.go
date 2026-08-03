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
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	taeoptions "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

var ErrLifecycleRewriteHasNoLiveRows = moerr.NewInternalErrorNoCtx(
	"Lifecycle Rewrite has no live rows; finalize as Whole retirement",
)

var ErrLifecycleNoExpiredRows = moerr.NewInternalErrorNoCtx(
	"Lifecycle source Object has no expired visible rows",
)

// LifecycleRewriteOptions describes the Root-owned physical output of one
// single-source Mixed Rewrite. The Root and its namespace must be durable
// before this method is called.
type LifecycleRewriteOptions struct {
	SourceSnapshot types.TS
	Source         objectio.ObjectStats
	TargetObjSize  uint32
	LiveSegmentID  objectio.Segmentid
	// MaxCertifiedBlockReadBytes is a conservative peak-memory ceiling for
	// one physical source Block. It is checked from Object metadata before
	// BlockDataReadNoCopy starts.
	MaxCertifiedBlockReadBytes uint64

	Classify lifecyclepkg.RewriteBlockClassifier
	Archive  lifecyclepkg.RewriteExpiredConsumer

	BeforeLiveWrite func(context.Context, objectio.Segmentid) error
	BookingPath     func(pageOrdinal uint32) (string, error)
}

type LifecycleRewriteResult struct {
	CreatedObjectStats      [][]byte
	TransferBookingLocation []string
	TransferMappingDigest   [sha256.Size]byte
	MergeLevel              int32
	ScanReport              lifecyclepkg.ObjectScanReport
}

// lifecycleObjectPressureBytes is the single resource-accounting size for a
// TAE Object. OriginSize is the preferred logical source size; Size protects
// legacy or unusually encoded Objects whose physical representation is larger.
// Exact identity still uses the complete ObjectStats bytes, not this value.
func lifecycleObjectPressureBytes(stats objectio.ObjectStats) uint64 {
	return uint64(max(stats.OriginSize(), stats.Size(), 1))
}

func lifecycleEstimatedExpiredPressureBytes(
	sourceBytes uint64,
	report lifecyclepkg.ObjectScanReport,
) (uint64, error) {
	visibleRows := report.ExpiredRows + report.LiveRows
	if sourceBytes == 0 || report.ExpiredRows == 0 || visibleRows == 0 {
		return 0, moerr.NewInternalErrorNoCtxf("MIXED_LAYOUT_BLOCKED: expired Rewrite pressure is zero")
	}
	// Mixed is strictly one Object, so both factors are bounded by uint32 and
	// their product fits uint64. Round up so a tiny expired tail is not charged
	// as zero bytes.
	return max(
		(sourceBytes*report.ExpiredRows+visibleRows-1)/visibleRows,
		uint64(1),
	), nil
}

func lifecycleObserveRewritePressure(sourceBytes, expiredBytes uint64) {
	metricv2.LifecycleBytesCounter.WithLabelValues(
		"rewrite_source_pressure",
	).Add(float64(sourceBytes))
	metricv2.LifecycleBytesCounter.WithLabelValues(
		"rewrite_estimated_expired_pressure",
	).Add(float64(expiredBytes))
	if sourceBytes > expiredBytes {
		metricv2.LifecycleBytesCounter.WithLabelValues(
			"rewrite_estimated_live_pressure",
		).Add(float64(sourceBytes - expiredBytes))
	}
}

func validateLifecycleRewriteOwnership(
	root lifecyclepkg.CleanupRoot,
	result LifecycleRewriteResult,
) error {
	if root.SegmentID == "" ||
		root.BookingPrefix == "" ||
		root.OrdinalUpperBound == 0 ||
		len(result.CreatedObjectStats) == 0 ||
		uint32(len(result.CreatedObjectStats)) > root.OrdinalUpperBound {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite Root ownership is incomplete")
	}
	segmentID, err := types.ParseUuid(root.SegmentID)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite Root segment is invalid: %v", err)
	}
	seenOrdinals := make(map[uint16]struct{}, len(result.CreatedObjectStats))
	for _, raw := range result.CreatedObjectStats {
		if len(raw) != objectio.ObjectStatsLen {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite created ObjectStats is malformed")
		}
		var stats objectio.ObjectStats
		copy(stats[:], raw)
		name := stats.ObjectName()
		if name.SegmentId() != segmentID {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite created Object escaped Root segment")
		}
		ordinal := name.Num()
		if uint32(ordinal) >= root.OrdinalUpperBound {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite created Object escaped Root ordinal range")
		}
		if _, exists := seenOrdinals[ordinal]; exists {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite created Object ordinal is duplicated")
		}
		seenOrdinals[ordinal] = struct{}{}
	}
	bookingFiles := 0
	prefix := strings.TrimSuffix(root.BookingPrefix, "/") + "/"
	for _, location := range result.TransferBookingLocation {
		// Existing external transfer encoding prefixes the physical file list
		// with 4-byte block/row-count headers.
		if len(location) == 4 {
			continue
		}
		bookingFiles++
		if !strings.HasPrefix(location, prefix) {
			return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite Booking escaped Root prefix")
		}
	}
	if bookingFiles == 0 {
		return moerr.NewInternalErrorNoCtxf("Lifecycle Rewrite has no Root-owned Booking file")
	}
	return nil
}

// LifecycleObjectRewriter is the narrow engine capability used by the
// Lifecycle worker. It deliberately exposes only the single-source build
// phase and never the ordinary Merge scheduler or commit handler.
type LifecycleObjectRewriter interface {
	LifecycleRewriteObject(
		context.Context,
		LifecycleRewriteOptions,
	) (LifecycleRewriteResult, error)
}

func validateLifecycleRewriteOptions(options LifecycleRewriteOptions) error {
	if options.SourceSnapshot.IsEmpty() ||
		*options.Source.ObjectName().ObjectId() == (types.Objectid{}) ||
		options.Source.GetAppendable() ||
		options.LiveSegmentID == (types.Uuid{}) ||
		options.MaxCertifiedBlockReadBytes == 0 ||
		options.Classify == nil ||
		options.BeforeLiveWrite == nil ||
		options.BookingPath == nil {
		return moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite options do not define an exact Root-owned source/output",
		)
	}
	return nil
}

// validateLifecycleRewriteLayout keeps the first GA inside the layout already
// produced by cnMergeTask. Supporting a non-default physical layout requires a
// common Merge change; Lifecycle must fail before reading instead of growing a
// second producer or guessing a different transfer-slab shape.
func validateLifecycleRewriteLayout(extra *api.SchemaExtra) error {
	if extra == nil ||
		extra.BlockMaxRows != taeoptions.DefaultBlockMaxRows ||
		extra.ObjectMaxBlocks != uint32(taeoptions.DefaultBlocksPerObject) {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Rewrite supports only the certified default Object layout",
		)
	}
	return nil
}

// LifecycleRewriteObject executes only the build phase. It writes Root-owned
// live Objects and an immutable external booking, but it does not mutate the
// source Catalog Object. The finalizer later publishes these outputs through a
// tagged Lifecycle commit entry.
func (tbl *txnTable) LifecycleRewriteObject(
	ctx context.Context,
	options LifecycleRewriteOptions,
) (_ LifecycleRewriteResult, err error) {
	if err := validateLifecycleRewriteOptions(options); err != nil {
		return LifecycleRewriteResult{}, err
	}
	if err := validateLifecycleRewriteLayout(tbl.GetExtraInfo()); err != nil {
		return LifecycleRewriteResult{}, err
	}
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	current, exists := state.GetObject(*options.Source.ObjectShortName())
	if !exists ||
		(!current.DeleteTime.IsEmpty() &&
			current.DeleteTime.LE(&options.SourceSnapshot)) ||
		current.ObjectStats != options.Source {
		return LifecycleRewriteResult{}, moerr.NewTxnWWConflictNoCtx(
			tbl.tableId,
			"Lifecycle Rewrite source Object identity changed",
		)
	}

	tbl.ensureSeqnumsAndTypesExpectRowid()
	sortKeyPos, sortKeyIsPK := tbl.getSortKeyPosAndSortKeyIsPK()
	base, err := newCNMergeTask(
		ctx,
		tbl,
		options.SourceSnapshot,
		sortKeyPos,
		sortKeyIsPK,
		[]objectio.ObjectStats{options.Source},
		options.TargetObjSize,
	)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	defer base.Release()
	if err := base.configureLifecycleBlockReadBudget(
		ctx,
		options.MaxCertifiedBlockReadBytes,
	); err != nil {
		return LifecycleRewriteResult{}, err
	}
	base.segmentID = &options.LiveSegmentID
	base.GetCommitEntry().Level = int32(options.Source.GetLevel())

	host, err := lifecyclepkg.NewRewriteHost(
		base,
		options.Classify,
		options.Archive,
	)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	if err := options.BeforeLiveWrite(ctx, options.LiveSegmentID); err != nil {
		return LifecycleRewriteResult{}, err
	}
	if err := mergesort.DoMergeAndWrite(
		ctx,
		fmt.Sprintf("lifecycle-rewrite-%d", tbl.tableId),
		sortKeyPos,
		host,
	); err != nil {
		base.commitEntry.Err = err.Error()
		return LifecycleRewriteResult{}, err
	}
	report, err := host.ScanReport()
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	result := LifecycleRewriteResult{ScanReport: report}
	if report.ExpiredRows == 0 {
		return result, ErrLifecycleNoExpiredRows
	}
	if report.LiveRows == 0 {
		return result, ErrLifecycleRewriteHasNoLiveRows
	}
	if base.transferTable == nil {
		return LifecycleRewriteResult{}, moerr.NewInternalError(
			ctx,
			"Lifecycle Rewrite producer returned no TransferTable",
		)
	}
	if len(base.commitEntry.CreatedObjs) == 0 {
		return LifecycleRewriteResult{}, moerr.NewInternalError(
			ctx,
			"Lifecycle Rewrite producer omitted live Objects",
		)
	}
	digest := mergesort.TransferMappingDigest(
		base.commitEntry.CreatedObjs,
		base.transferTable,
	)
	commitEntry, err := dumpTransferInfoWithOptions(
		ctx,
		base,
		&lifecycleTransferBookingWriteOptions{
			forceExternal:                true,
			preservePhysicalFilesOnError: true,
			pathAllocator:                options.BookingPath,
		},
	)
	if err != nil {
		return LifecycleRewriteResult{}, err
	}
	result.CreatedObjectStats = append(
		[][]byte(nil),
		commitEntry.CreatedObjs...,
	)
	result.TransferBookingLocation = append(
		[]string(nil),
		commitEntry.BookingLoc...,
	)
	result.TransferMappingDigest = digest
	result.MergeLevel = commitEntry.Level
	return result, nil
}

const lifecycleBlockReadFixedOverhead = uint64(
	64<<20 + // bounded Parquet encoder Chunk
		16<<20 + // maximum single-source transfer slab tier
		16<<20, // vector/framing safety margin
)

// lifecycleBlockReadPeakBytes estimates the maximum simultaneous memory held
// while Lifecycle classifies one complete physical Block. The source vectors
// and the live Merge output can each be as large as the source logical bytes;
// Archive encoding and the transfer slab have independent fixed hard caps.
func lifecycleBlockReadPeakBytes(sourceLogicalBytes uint64) (uint64, error) {
	maxUint64 := ^uint64(0)
	if sourceLogicalBytes == 0 ||
		sourceLogicalBytes > (maxUint64-lifecycleBlockReadFixedOverhead)/2 {
		return 0, moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Block read cannot be estimated safely",
		)
	}
	return sourceLogicalBytes*2 + lifecycleBlockReadFixedOverhead, nil
}

func validateLifecycleBlockReadPeak(
	sourceLogicalBytes uint64,
	maxCertifiedBytes uint64,
) error {
	if maxCertifiedBytes == 0 {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle certified Block read limit is zero",
		)
	}
	peak, err := lifecycleBlockReadPeakBytes(sourceLogicalBytes)
	if err != nil {
		return err
	}
	if peak > maxCertifiedBytes {
		return moerr.NewInternalErrorNoCtxf(
			"RESOURCE_BLOCKED: Lifecycle Block read peak %d exceeds certified limit %d",
			peak,
			maxCertifiedBytes,
		)
	}
	return nil
}
