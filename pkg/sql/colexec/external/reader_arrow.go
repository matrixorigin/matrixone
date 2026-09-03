// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/arrowbridge"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external/arrowio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	arrowRangeAllocationSite mpool.AllocationSite = 1
	arrowVectorDataSite      mpool.AllocationSite = 3
	arrowVectorAreaSite      mpool.AllocationSite = 4
	arrowVectorNullsSite     mpool.AllocationSite = 5
	arrowVectorGroupingSite  mpool.AllocationSite = 6
	arrowMaxOutputRows                            = 100_000
)

var arrowPinnedMetricState struct {
	sync.Mutex
	current int64
	peak    int64
}

// These wrappers observe the same capacity reservation that owns the backing;
// they do not introduce a second quota or release path. Only a successful
// Commit changes the gauge, and the returned lease's idempotent Release removes
// that exact capacity after the FileService/Arrow backing is no longer live.
type meteredArrowRangeAdmission struct {
	inner fileservice.RangeReadAdmission
}

type meteredArrowCapacityReservation struct {
	inner fileservice.CapacityReservation
}

type meteredArrowCapacityLease struct {
	inner    fileservice.CapacityLease
	capacity int64
	released atomic.Bool
}

func (a meteredArrowRangeAdmission) Reserve(
	ctx context.Context,
	upperBound int64,
) (fileservice.CapacityReservation, error) {
	reservation, err := a.inner.Reserve(ctx, upperBound)
	if err != nil {
		return nil, err
	}
	return meteredArrowCapacityReservation{inner: reservation}, nil
}

func (r meteredArrowCapacityReservation) Commit(
	actualCapacity int64,
) (fileservice.CapacityLease, error) {
	lease, err := r.inner.Commit(actualCapacity)
	if err != nil {
		return nil, err
	}
	adjustArrowPinnedBytes(actualCapacity)
	return &meteredArrowCapacityLease{inner: lease, capacity: actualCapacity}, nil
}

func (r meteredArrowCapacityReservation) Abort() {
	r.inner.Abort()
}

func (l *meteredArrowCapacityLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	defer adjustArrowPinnedBytes(-l.capacity)
	l.inner.Release()
}

func adjustArrowPinnedBytes(delta int64) {
	arrowPinnedMetricState.Lock()
	defer arrowPinnedMetricState.Unlock()
	arrowPinnedMetricState.current += delta
	// Capacity leases are idempotent, so underflow would indicate a metric-only
	// bookkeeping defect. Keep the exported gauge valid while lifecycle tests
	// assert that every reader returns to its starting value.
	if arrowPinnedMetricState.current < 0 {
		arrowPinnedMetricState.current = 0
	}
	metric.ArrowLoadPinnedBytesGauge.Set(float64(arrowPinnedMetricState.current))
	if arrowPinnedMetricState.current > arrowPinnedMetricState.peak {
		arrowPinnedMetricState.peak = arrowPinnedMetricState.current
		metric.ArrowLoadPinnedBytesHighWaterGauge.Set(float64(arrowPinnedMetricState.peak))
	}
}

func observeArrowPhase(start time.Time, phase string, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	metric.ArrowLoadPhaseDurationHistogram.WithLabelValues(phase, outcome).Observe(time.Since(start).Seconds())
}

func observeArrowConvertStats(stats arrowbridge.ConvertStats) {
	metric.ArrowLoadPayloadBytesCounter.WithLabelValues("eligible").Add(float64(stats.EligiblePayloadBytes))
	metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed").Add(float64(stats.BorrowedPayloadBytes))
	metric.ArrowLoadPayloadBytesCounter.WithLabelValues("retained_capacity").Add(float64(stats.RetainedCapacityBytes))
	metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo").Add(float64(stats.MaterializedPayloadBytes))
	metric.ArrowLoadConversionColumnCounter.WithLabelValues("borrowed").Add(float64(stats.BorrowedColumns))
	metric.ArrowLoadConversionColumnCounter.WithLabelValues("materialized").Add(float64(stats.MaterializedColumns))
	metric.ArrowLoadFallbackCounter.WithLabelValues("pin_amplification").Add(float64(stats.PinAmplificationFallbacks))
	metric.ArrowLoadFallbackCounter.WithLabelValues("unaligned").Add(float64(stats.UnalignedFallbacks))
}

func arrowLoadErrorCategory(err error) string {
	var moError *moerr.Error
	moCode := uint16(0)
	if errors.As(err, &moError) {
		moCode = moError.ErrorCode()
	}
	switch {
	case errors.Is(err, context.Canceled) || moCode == moerr.ErrQueryInterrupted:
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded) || moCode == moerr.ErrQueryTimeout:
		return "deadline_exceeded"
	case errors.Is(err, fileservice.ErrObjectChanged):
		return "object_changed"
	case errors.Is(err, mpool.ErrAllocationAccountCapacity) ||
		moCode == moerr.ErrOOM || moCode == moerr.ErrMPoolCapacity:
		return "resource_exhausted"
	case moCode == moerr.ErrNotSupported || moCode == moerr.ErrNYI:
		return "not_supported"
	case moCode == moerr.ErrConstraintViolation:
		return "constraint_violation"
	case moCode == moerr.ErrInvalidInput || moCode == moerr.ErrOutOfRange:
		return "invalid_input"
	case moCode == moerr.ErrInternal:
		return "internal"
	default:
		return "io"
	}
}

func observeArrowError(err error) {
	if err != nil {
		metric.ArrowLoadErrorCounter.WithLabelValues(arrowLoadErrorCategory(err)).Inc()
	}
}

// ArrowReader is the LOAD-only adapter between the format-neutral External
// operator and the canonical Arrow-to-MO bridge. The IPC reader owns the
// current record; borrowed MO vectors retain only the ArrayData/range leases
// they reference and therefore safely outlive the next IPC Next call.
type ArrowReader struct {
	param                 *ExternalParam
	reader                arrowio.Reader
	plan                  *arrowbridge.Plan
	pending               arrow.RecordBatch
	rowOffset             int64
	admission             fileservice.RangeReadAdmission
	allocation            *vector.AllocationAccountSelection
	conversionFingerprint [sha256.Size]byte
	fingerprintSet        bool
}

func NewArrowReader(
	param *ExternalParam,
	_ *process.Process,
	account *mpool.AllocationAccount,
) (*ArrowReader, error) {
	if param == nil || param.Extern == nil || account == nil {
		return nil, moerr.NewInvalidInputNoCtx("Arrow LOAD requires a statement allocation account")
	}
	if param.ArrowConversionPlanVersion != arrowbridge.ConversionPlanVersion {
		return nil, moerr.NewInvalidInputNoCtxf(
			"unsupported Arrow conversion plan version %d", param.ArrowConversionPlanVersion,
		)
	}
	if len(param.ArrowSchemaFingerprint) != 0 && len(param.ArrowSchemaFingerprint) != sha256.Size {
		return nil, moerr.NewInvalidInputNoCtxf(
			"invalid Arrow schema fingerprint length %d", len(param.ArrowSchemaFingerprint),
		)
	}
	admission, err := fileservice.NewAllocationAccountRangeAdmission(
		account,
		mpool.AllocationOwnerExternal,
		arrowRangeAllocationSite,
		mpool.AllocationCapacityClassDefault,
	)
	if err != nil {
		return nil, err
	}
	allocation, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerExternal,
		arrowVectorDataSite,
		arrowVectorAreaSite,
		arrowVectorNullsSite,
		arrowVectorGroupingSite,
	)
	if err != nil {
		return nil, err
	}
	return &ArrowReader{
		param: param, admission: meteredArrowRangeAdmission{inner: admission}, allocation: allocation,
	}, nil
}

func (r *ArrowReader) Open(param *ExternalParam, proc *process.Process) (_ bool, retErr error) {
	startTime := time.Now()
	defer func() {
		observeArrowPhase(startTime, "open", retErr)
		outcome := "success"
		if retErr != nil {
			outcome = "error"
			observeArrowError(retErr)
		}
		metric.ArrowLoadObjectCounter.WithLabelValues(outcome).Inc()
	}()
	if r == nil || param == nil || param.Extern == nil || proc == nil {
		return false, moerr.NewInvalidInputNoCtx("invalid Arrow reader open")
	}
	if err := r.Close(); err != nil {
		return false, err
	}
	r.param = param
	fileIndex := param.Fileparam.FileIndex - 1
	if fileIndex < 0 || fileIndex >= len(param.FileSize) {
		return false, moerr.NewInvalidInputf(proc.Ctx, "Arrow file size is missing for file index %d", fileIndex)
	}
	size := param.FileSize[fileIndex]
	if size < 0 {
		return false, moerr.NewInvalidInputf(proc.Ctx, "Arrow file size %d is invalid", size)
	}
	fs, readPath, err := plan2.GetForETLWithType(param.Extern, param.Fileparam.Filepath)
	if err != nil {
		return false, err
	}
	container, err := arrowContainer(param.Extern.ArrowContainer)
	if err != nil {
		return false, err
	}
	identity, err := arrowObjectIdentity(proc.Ctx, param, fileIndex, fs, readPath, size)
	if err != nil {
		return false, err
	}
	fileShard, err := arrowFileShard(proc.Ctx, param, fileIndex, container)
	if err != nil {
		return false, err
	}
	reader, err := arrowio.Open(
		proc.Ctx, fs, readPath, size, container, r.admission,
		arrowio.Options{ExpectedIdentity: identity, FileShard: fileShard},
	)
	if err != nil {
		return false, err
	}
	r.reader = reader

	targets, err := BuildArrowTargets(proc.Ctx, param.Attrs, param.Cols)
	if err != nil {
		r.Close()
		return false, err
	}
	mode := arrowbridge.MatchByName
	if param.Extern.ArrowMatchByPosition {
		mode = arrowbridge.MatchByPosition
	}
	// LOAD uses a deliberately more permissive type policy than exact result
	// protocols such as Python UDF. Keep the policy explicit at this boundary.
	r.plan, err = arrowbridge.BindLoad(proc.Ctx, reader.Schema(), targets, mode)
	if err != nil {
		r.Close()
		return false, err
	}
	fileFingerprint := r.plan.Fingerprint()
	if len(param.ArrowSchemaFingerprint) != 0 &&
		!bytes.Equal(param.ArrowSchemaFingerprint, fileFingerprint[:]) {
		r.Close()
		return false, moerr.NewInvalidInputf(proc.Ctx,
			"Arrow schema and conversion contract for file index %d does not match the planned contract", fileIndex)
	}
	if r.fingerprintSet && r.conversionFingerprint != fileFingerprint {
		r.Close()
		return false, moerr.NewInvalidInputf(proc.Ctx,
			"Arrow schema and conversion contract for file index %d differs from earlier files", fileIndex)
	}
	r.conversionFingerprint = fileFingerprint
	r.fingerprintSet = true
	finished, err := r.advanceToNextNonEmptyRecord()
	if err != nil {
		r.Close()
		return false, err
	}
	if finished {
		if err = r.Close(); err != nil {
			return false, err
		}
	}
	if fileShard != nil {
		metric.ArrowLoadShardCounter.Inc()
	}
	return finished, nil
}

// advanceToNextNonEmptyRecord hides legal zero-row IPC record batches from
// External's non-empty output contract. Calling Next also releases the IPC
// reader's previous record; any published borrowed MO vectors hold their own
// ArrayData references and remain valid.
func (r *ArrowReader) advanceToNextNonEmptyRecord() (bool, error) {
	startTime := time.Now()
	var retErr error
	defer func() { observeArrowPhase(startTime, "next_record", retErr) }()
	if r == nil || r.reader == nil {
		retErr = moerr.NewInternalErrorNoCtx("Arrow reader is not open")
		return false, retErr
	}
	for r.reader.Next() {
		record := r.reader.RecordBatch()
		if record == nil {
			retErr = moerr.NewInvalidInputNoCtx("Arrow reader returned a nil record batch")
			return false, retErr
		}
		if record.NumRows() == 0 {
			continue
		}
		r.pending = record
		r.rowOffset = 0
		metric.ArrowLoadRecordCounter.Inc()
		return false, nil
	}
	if err := r.reader.Err(); err != nil {
		retErr = err
		return false, retErr
	}
	r.pending = nil
	r.rowOffset = 0
	return true, nil
}

func arrowFileShard(
	ctx context.Context,
	param *ExternalParam,
	fileIndex int,
	container arrowio.Container,
) (*arrowio.FileShard, error) {
	var planned *pipeline.ArrowRecordBatchShard
	for _, shard := range param.ArrowRecordBatchShards {
		if shard == nil || int(shard.FileIndex) != fileIndex {
			continue
		}
		if planned != nil {
			return nil, moerr.NewInvalidInputf(ctx,
				"multiple Arrow record-batch shards target file index %d in one reader", fileIndex)
		}
		planned = shard
	}
	if planned == nil {
		return nil, nil
	}
	if container == arrowio.ContainerStream {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC Stream cannot use record-batch shards")
	}
	if planned.RecordBatchStart < 0 || planned.RecordBatchStart >= planned.RecordBatchEnd ||
		planned.EstimatedRows < 0 || planned.EstimatedWireBytes < 0 {
		return nil, moerr.NewInvalidInputf(ctx,
			"invalid Arrow record-batch shard [%d,%d)", planned.RecordBatchStart, planned.RecordBatchEnd)
	}
	return &arrowio.FileShard{
		RecordBatchStart: planned.RecordBatchStart,
		RecordBatchEnd:   planned.RecordBatchEnd,
		RequiredDictionaryBlockIndices: append(
			[]int32(nil), planned.RequiredDictionaryBlockIndices...,
		),
	}, nil
}

func arrowObjectIdentity(
	ctx context.Context,
	param *ExternalParam,
	fileIndex int,
	fs fileservice.FileService,
	readPath string,
	size int64,
) (*fileservice.ObjectIdentity, error) {
	var planned *pipeline.ArrowObjectIdentity
	for _, identity := range param.ArrowObjectIdentities {
		if identity == nil || int(identity.FileIndex) != fileIndex {
			continue
		}
		if planned != nil {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate Arrow object identity for file index %d", fileIndex)
		}
		planned = identity
	}
	if planned != nil {
		identity := &fileservice.ObjectIdentity{
			VersionID: planned.VersionId,
			ETag:      planned.Etag,
			Size:      planned.Size,
		}
		if planned.LastModifiedUnixNano != 0 {
			identity.LastModified = time.Unix(0, planned.LastModifiedUnixNano).UTC()
		}
		if err := identity.Validate(); err != nil {
			return nil, err
		}
		if identity.Size != size {
			return nil, fmt.Errorf("%w: Arrow object size changed from %d to %d",
				fileservice.ErrObjectChanged, identity.Size, size)
		}
		return identity, nil
	}
	identityFS, ok := fs.(fileservice.ObjectIdentityFileService)
	if !ok {
		return nil, nil
	}
	identity, err := identityFS.StatFileIdentity(ctx, readPath)
	if err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if identity.Size != size {
		return nil, fmt.Errorf("%w: Arrow object size changed from %d to %d",
			fileservice.ErrObjectChanged, size, identity.Size)
	}
	return &identity, nil
}

func (r *ArrowReader) ReadBatch(
	ctx context.Context,
	buf *batch.Batch,
	proc *process.Process,
	_ process.Analyzer,
) (_ bool, retErr error) {
	defer func() { observeArrowError(retErr) }()
	if r == nil || r.reader == nil || r.plan == nil || r.pending == nil || buf == nil || proc == nil {
		return false, moerr.NewInvalidInput(ctx, "Arrow reader is not open")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	record := r.pending
	start := r.rowOffset
	maxRows := min(arrowMaxOutputRows, int(record.NumRows()-start))
	rows, err := r.plan.MaxOutputRows(ctx, record, start, maxRows, r.param.maxBatchSize)
	if err != nil {
		return false, err
	}
	end := start + int64(rows)
	if start < 0 || start >= end {
		return false, moerr.NewInvalidInput(ctx, "Arrow record batch has an invalid row window")
	}
	view := record
	if start != 0 || end != record.NumRows() {
		view = record.NewSlice(start, end)
		defer view.Release()
	}

	location := time.UTC
	if proc.GetSessionInfo() != nil && proc.GetSessionInfo().TimeZone != nil {
		location = proc.GetSessionInfo().TimeZone
	}
	convertStart := time.Now()
	converted, stats, err := r.plan.Convert(ctx, view, proc.Mp(), arrowbridge.ConvertOptions{
		Location: location, Allocation: r.allocation,
	})
	observeArrowPhase(convertStart, "convert", err)
	if err != nil {
		return false, err
	}
	observeArrowConvertStats(stats)
	wireBudgetStart := time.Now()
	converted, actualRows, err := fitArrowBatchToWireBudget(ctx, converted, r.param.maxBatchSize, proc.Mp())
	observeArrowPhase(wireBudgetStart, "wire_budget", err)
	if err != nil {
		converted.Clean(proc.Mp())
		return false, err
	}
	if actualRows < rows {
		rows = actualRows
		end = start + int64(rows)
	}
	fileFinished := false
	if end < record.NumRows() {
		r.rowOffset = end
	} else {
		fileFinished, err = r.advanceToNextNonEmptyRecord()
		if err != nil {
			converted.Clean(proc.Mp())
			return false, err
		}
	}
	publishStart := time.Now()
	if err = replaceArrowBatch(buf, converted, proc.Mp()); err != nil {
		observeArrowPhase(publishStart, "publish", err)
		converted.Clean(proc.Mp())
		return false, err
	}
	observeArrowPhase(publishStart, "publish", nil)
	metric.ArrowLoadBatchCounter.Inc()
	metric.ArrowLoadRowCounter.Add(float64(rows))
	return fileFinished, nil
}

func fitArrowBatchToWireBudget(
	ctx context.Context,
	converted *batch.Batch,
	maxBytes uint64,
	mp *mpool.MPool,
) (*batch.Batch, int, error) {
	if converted == nil || converted.RowCount() <= 0 {
		return converted, 0, moerr.NewInvalidInput(ctx, "invalid converted Arrow batch")
	}
	if maxBytes == 0 {
		return converted, converted.RowCount(), nil
	}
	size, err := converted.MarshalBinarySize()
	if err != nil {
		return converted, 0, err
	}
	if uint64(size) <= maxBytes {
		return converted, converted.RowCount(), nil
	}
	low, high := 1, converted.RowCount()-1
	best := 0
	for low <= high {
		if err := ctx.Err(); err != nil {
			return converted, 0, err
		}
		middle := low + (high-low)/2
		window, err := converted.Window(0, middle)
		if err != nil {
			return converted, 0, err
		}
		windowSize, sizeErr := window.MarshalBinarySize()
		window.Clean(mp)
		if sizeErr != nil {
			return converted, 0, sizeErr
		}
		if uint64(windowSize) <= maxBytes {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if best == 0 {
		one, err := converted.Window(0, 1)
		if err != nil {
			return converted, 0, err
		}
		oneSize, sizeErr := one.MarshalBinarySize()
		one.Clean(mp)
		if sizeErr != nil {
			return converted, 0, sizeErr
		}
		return converted, 0, moerr.NewConstraintViolationf(
			ctx, "Arrow row canonical wire size %d exceeds batch limit %d", oneSize, maxBytes,
		)
	}
	stable, err := stableArrowBatchPrefix(converted, best, mp)
	if err != nil {
		return converted, 0, err
	}
	converted.Clean(mp)
	return stable, best, nil
}

// stableArrowBatchPrefix keeps the zero-copy contract column-local while
// making a prefix independent of the source batch lifetime. Borrowed columns
// retain their leased payloads; materialized columns copy only selected rows.
func stableArrowBatchPrefix(source *batch.Batch, rows int, mp *mpool.MPool) (_ *batch.Batch, err error) {
	if source == nil || mp == nil || rows <= 0 || rows > source.RowCount() {
		return nil, moerr.NewInvalidInputNoCtx("invalid Arrow batch prefix")
	}
	stable := batch.NewOffHeap(append([]string(nil), source.Attrs...))
	stable.Recursive = source.Recursive
	stable.ShuffleIDX = source.ShuffleIDX
	defer func() {
		if err != nil {
			stable.Clean(mp)
		}
	}()
	for index, sourceVector := range source.Vecs {
		if sourceVector == nil {
			return nil, moerr.NewInvalidInputNoCtxf("Arrow source batch column %d is nil", index)
		}
		var snapshot *vector.Vector
		if sourceVector.HasBorrowedBacking() {
			snapshot, err = sourceVector.RetainedReadonlyWindowWithMP(0, rows, mp)
		} else {
			var window *vector.Vector
			selection := sourceVector.AllocationAccountSelection()
			if selection == nil {
				window, err = sourceVector.WindowByLogicalRows(0, rows)
			} else {
				window, err = sourceVector.WindowByLogicalRowsWithAllocation(0, rows, mp, selection)
			}
			if err == nil {
				snapshot, err = window.Dup(mp)
				window.Free(mp)
			}
		}
		if err != nil {
			return nil, err
		}
		stable.SetVector(int32(index), snapshot)
	}
	stable.SetRowCount(rows)
	return stable, nil
}

func (r *ArrowReader) Close() error {
	if r == nil {
		return nil
	}
	r.pending = nil
	r.rowOffset = 0
	r.plan = nil
	if r.reader == nil {
		return nil
	}
	reader := r.reader
	r.reader = nil
	return reader.Close()
}

func arrowContainer(value string) (arrowio.Container, error) {
	switch value {
	case "", tree.ARROW_CONTAINER_AUTO:
		return arrowio.ContainerAuto, nil
	case tree.ARROW_CONTAINER_FILE:
		return arrowio.ContainerFile, nil
	case tree.ARROW_CONTAINER_STREAM:
		return arrowio.ContainerStream, nil
	default:
		return 0, moerr.NewBadConfigNoCtxf("the arrow_container '%s' is not supported", value)
	}
}

// BuildArrowTargets builds the exact table-side conversion contract shared by
// compile-time fingerprinting and execution-time binding.
func BuildArrowTargets(
	ctx context.Context,
	attrs []plan2.ExternAttr,
	cols []*plan2.ColDef,
) ([]arrowbridge.TargetColumn, error) {
	targets := make([]arrowbridge.TargetColumn, len(attrs))
	for outputIndex, attr := range attrs {
		colIndex := int(attr.ColIndex)
		if colIndex < 0 || colIndex >= len(cols) || cols[colIndex] == nil {
			return nil, moerr.NewInvalidInputf(ctx, "Arrow target column index %d is invalid", colIndex)
		}
		col := cols[colIndex]
		targets[outputIndex] = arrowbridge.TargetColumn{
			Name:     col.Name,
			Type:     makeType(&col.Typ, false),
			NotNull:  col.Typ.NotNullable,
			MOIndex:  outputIndex,
			AttrName: attr.ColName,
		}
	}
	return targets, nil
}

func replaceArrowBatch(dst, src *batch.Batch, mp *mpool.MPool) error {
	if dst == nil || src == nil || len(dst.Vecs) != len(src.Vecs) {
		return fmt.Errorf("Arrow conversion produced an incompatible MatrixOne batch")
	}
	for i := range src.Vecs {
		if src.Vecs[i] == nil {
			return fmt.Errorf("Arrow conversion produced a nil vector at column %d", i)
		}
		if dst.Vecs[i] != nil {
			dst.Vecs[i].Free(mp)
		}
		dst.SetVector(int32(i), src.Vecs[i])
		src.SetVector(int32(i), nil)
	}
	dst.SetRowCount(src.RowCount())
	src.Clean(mp)
	return nil
}

var _ ExternalFileReader = (*ArrowReader)(nil)
