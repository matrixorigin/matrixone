// Copyright 2024 Matrix Origin
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

package group

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func validateDecodedAggregateGroupCount(
	agg aggexec.GroupAggFuncExec,
	expected int,
) error {
	actual := agg.GetNumGroups()
	if actual != expected {
		return moerr.NewInvalidInputNoCtxf(
			"decoded aggregate %d row count %d does not match record row count %d",
			agg.AggID(), actual, expected)
	}
	return nil
}

func appendSpillGroupByRows(
	writer io.Writer,
	gbBatch *batch.Batch,
	rows []int32,
) error {
	if writer == nil || gbBatch == nil {
		return io.ErrClosedPipe
	}
	if len(gbBatch.Vecs) > int(^uint32(0)>>1) {
		return moerr.NewInvalidInputNoCtx(
			"spilled group-by column count exceeds wire format")
	}
	if err := types.WriteInt32(writer, int32(len(gbBatch.Vecs))); err != nil {
		return err
	}
	for _, vec := range gbBatch.Vecs {
		if err := vec.MarshalSelectedRowsTo(writer, rows); err != nil {
			return err
		}
	}
	return nil
}

func writeSpillBool(writer io.Writer, value bool) error {
	encoded := types.EncodeBool(&value)
	n, err := writer.Write(encoded)
	if err != nil {
		return err
	}
	if n != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}

type spillRecordWriter struct {
	target  io.Writer
	written int64
}

func (w *spillRecordWriter) Write(value []byte) (int, error) {
	if w == nil || w.target == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := w.target.Write(value)
	w.written += int64(n)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *spillRecordWriter) WriteSelectedFixedRows(
	data []byte,
	width int,
	rows []int32,
) (int, error) {
	if w == nil || w.target == nil {
		return 0, io.ErrClosedPipe
	}
	fastWriter, ok := w.target.(interface {
		WriteSelectedFixedRows([]byte, int, []int32) (int, error)
	})
	if !ok {
		if width < 0 || (width != 0 && len(data)%width != 0) {
			return 0, moerr.NewInvalidInputNoCtx(
				"invalid fixed-width group spill selection")
		}
		if width == 0 {
			return 0, nil
		}
		written := 0
		rowCount := len(data) / width
		for _, selected := range rows {
			row := int(selected)
			if row < 0 || row >= rowCount {
				return written, moerr.NewInvalidInputNoCtx(
					"invalid fixed-width group spill row")
			}
			n, err := w.Write(data[row*width : (row+1)*width])
			written += n
			if err != nil {
				return written, err
			}
		}
		return written, nil
	}
	n, err := fastWriter.WriteSelectedFixedRows(data, width, rows)
	w.written += int64(n)
	if err == nil && width >= 0 && width <= math.MaxInt/max(1, len(rows)) &&
		n != width*len(rows) {
		err = io.ErrShortWrite
	}
	return n, err
}

func newGroupSpillBuffer(
	ctr *container,
	site mpool.AllocationSite,
) (reusableSpillBuffer, error) {
	if ctr == nil || ctr.mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if ctr.allocationAccount == nil {
		return &unaccountedSpillBuffer{}, nil
	}
	// Read-ahead and write coalescing are optional optimizations. Charge them
	// to ordinary statement capacity so they cannot consume the bounded
	// recovery floor required by hash/selection spill scratch. If ordinary
	// capacity is exhausted, the reader/writer falls back to direct I/O.
	return mpool.NewAccountedBuffer(
		ctr.mp,
		ctr.allocationAccount,
		mpool.AllocationOwnerGroup,
		site,
	)
}

func unmarshalSpillGroupByRows(
	r io.Reader,
	gbBatch *batch.Batch,
	rows int,
	mp *mpool.MPool,
) error {
	if r == nil || gbBatch == nil || mp == nil || rows < 0 {
		return moerr.NewInvalidInputNoCtx("invalid spilled group-by rows")
	}
	columns, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if columns != len(gbBatch.Vecs) {
		return moerr.NewInvalidInputNoCtx("spilled group-by column count mismatch")
	}
	for _, vec := range gbBatch.Vecs {
		if err := vec.UnmarshalSelectedRowsFrom(r, rows, mp); err != nil {
			return err
		}
	}
	gbBatch.SetRowCount(rows)
	return nil
}

type ResHashRelated struct {
	mp         *mpool.MPool
	Hash       hashmap.HashMap
	TxnItr     hashmap.TransactionalIterator
	insertPlan hashmap.InsertPlan
}

type groupInsertPreview struct {
	values    []uint64
	inserted  []uint8
	newGroups int
}

type groupKeySourcePublication struct {
	overrides    [][]types.StringSource
	destinations []*vector.Vector
}

func (publication *groupKeySourcePublication) addDestination(destination *vector.Vector) {
	for _, existing := range publication.destinations {
		if existing == destination {
			return
		}
	}
	publication.destinations = append(publication.destinations, destination)
}

func (publication *groupKeySourcePublication) finalize() {
	for _, destination := range publication.destinations {
		destination.FinalizeStringSourcePreflight()
	}
}

type groupPrePublicationError struct {
	cause error
}

func (err *groupPrePublicationError) Error() string { return err.cause.Error() }
func (err *groupPrePublicationError) Unwrap() error { return err.cause }

func isGroupPrePublicationError(err error) bool {
	_, ok := err.(*groupPrePublicationError)
	return ok
}

func (ctr *container) commitGroupByChunk(
	vectors []*vector.Vector,
	offset, rows int,
	preview groupInsertPreview,
) ([]uint64, int, error) {
	hasStringSourceMetadata := ctr.groupKeyStringSourceMetadata
	if !hasStringSourceMetadata {
		for _, vec := range vectors {
			if vec.HasStringSourceMetadata() {
				hasStringSourceMetadata = true
				break
			}
		}
	}
	var sourcePublication groupKeySourcePublication
	if hasStringSourceMetadata {
		defer sourcePublication.finalize()
		if err := ctr.preflightPreviewGroupKeyStringSources(
			vectors, offset, preview.values, preview.inserted,
			&sourcePublication); err != nil {
			return nil, 0, &groupPrePublicationError{cause: err}
		}
	}
	values, _, err := ctr.hr.TxnItr.CommitPreview(&ctr.hr.insertPlan)
	if err != nil {
		return nil, 0, &groupPrePublicationError{cause: err}
	}

	more, err := ctr.appendGroupByBatchWithStringSources(
		vectors, offset, preview.inserted, sourcePublication.overrides, 0)
	if err != nil {
		return nil, 0, err
	}
	if more != preview.newGroups {
		return nil, 0, mpool.ErrAllocationAccountInvariant
	}
	if hasStringSourceMetadata {
		if err := ctr.applyPreviewGroupKeyStringSources(vectors, offset, values); err != nil {
			return nil, 0, err
		}
		ctr.groupKeyStringSourceMetadata = true
	}
	return values, more, nil
}

func (ctr *container) previewGroupDestination(groupIndex int) (*batch.Batch, int, error) {
	batchIndex := groupIndex / aggBatchSize
	batchRow := groupIndex % aggBatchSize
	if batchIndex < len(ctr.groupByBatches) {
		return ctr.groupByBatches[batchIndex], batchRow, nil
	}
	if batchIndex == len(ctr.groupByBatches) && ctr.groupByStandby != nil {
		return ctr.groupByStandby, batchRow, nil
	}
	return nil, 0, mpool.ErrAllocationAccountInvariant
}

func (ctr *container) forEachPreviewGroupKeyStringSource(
	vectors []*vector.Vector,
	offset int,
	groups []uint64,
	fn func(destination *vector.Vector, row int, source types.StringSource) error,
) error {
	for row, group := range groups {
		if group == 0 {
			continue
		}
		seen := false
		for previous := 0; previous < row; previous++ {
			if groups[previous] == group {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		groupIndex := int(group - 1)
		destinationBatch, destinationRow, err := ctr.previewGroupDestination(groupIndex)
		if err != nil {
			return err
		}
		for column, sourceVector := range vectors {
			destination := destinationBatch.Vecs[column]
			merged := sourceVector.GetStringSourceAt(offset + row)
			if destinationRow < destination.Length() {
				merged, err = types.MergeStringSources(
					destination.GetStringSourceAt(destinationRow), merged)
				if err != nil {
					return err
				}
			}
			for candidate := row + 1; candidate < len(groups); candidate++ {
				if groups[candidate] != group {
					continue
				}
				merged, err = types.MergeStringSources(
					merged, sourceVector.GetStringSourceAt(offset+candidate))
				if err != nil {
					return err
				}
			}
			if err := fn(destination, destinationRow, merged); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) preflightPreviewGroupKeyStringSources(
	vectors []*vector.Vector,
	offset int,
	groups []uint64,
	inserted []uint8,
	publication *groupKeySourcePublication,
) error {
	if len(groups) > hashmap.UnitLimit || len(inserted) != len(groups) || publication == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := ctr.forEachPreviewGroupKeyStringSource(
		vectors, offset, groups,
		func(destination *vector.Vector, row int, source types.StringSource) error {
			publication.addDestination(destination)
			if err := destination.PreflightSetStringSourceAtLength(
				row, max(destination.Length(), row+1), source, ctr.mp); err != nil {
				return err
			}
			// Existing-row preflight has finalLength == Length and therefore does
			// not infer a deferred publication. Keep both current and standby
			// reservations alive until groupKeySourcePublication.finalize.
			destination.RetainStringSourcePreflight()
			return nil
		},
	); err != nil {
		return err
	}
	// Keep preview results in operator-owned, UnitLimit-bounded scratch. The
	// selected append consumes these overrides without modifying borrowed input.
	publication.overrides = make([][]types.StringSource, len(vectors))
	for column := range publication.overrides {
		publication.overrides[column] = make([]types.StringSource, len(groups))
	}
	for row, flag := range inserted {
		if flag == 0 {
			continue
		}
		group := groups[row]
		for column, sourceVector := range vectors {
			merged := sourceVector.GetStringSourceAt(offset + row)
			for candidate := row + 1; candidate < len(groups); candidate++ {
				if groups[candidate] != group {
					continue
				}
				var err error
				merged, err = types.MergeStringSources(
					merged, sourceVector.GetStringSourceAt(offset+candidate))
				if err != nil {
					return err
				}
			}
			publication.overrides[column][row] = merged
		}
	}
	return nil
}

func (ctr *container) applyPreviewGroupKeyStringSources(
	vectors []*vector.Vector,
	offset int,
	groups []uint64,
) error {
	return ctr.forEachPreviewGroupKeyStringSource(
		vectors, offset, groups,
		func(destination *vector.Vector, row int, source types.StringSource) error {
			return destination.SetStringSourceAtWithMP(row, source, ctr.mp)
		},
	)
}

func (group *Group) configureH0OrderedAggSpill(proc *process.Process) {
	if !group.NeedEval || group.ctr.mtyp != H0 {
		return
	}
	group.configureOrderedAggSpill(proc, group.ctr.aggList)
}

func (group *Group) configureOrderedAggSpill(proc *process.Process, aggs []aggexec.GroupAggFuncExec) {
	group.ctr.configureOrderedAggSpill(proc, group.OpAnalyzer, aggs)
}

func (ctr *container) configureOrderedAggSpill(
	proc *process.Process,
	opAnalyzer process.Analyzer,
	aggs []aggexec.GroupAggFuncExec,
) {
	for _, agg := range aggs {
		aggexec.ConfigureGroupConcatH0Spill(
			agg,
			ctr.spillMem,
			proc.Ctx,
			func() (*os.File, error) {
				spillFS, err := proc.GetSpillFileService()
				if err != nil {
					return nil, err
				}
				id, _ := uuid.NewV7()
				return spillFS.CreateAndRemoveFile(
					proc.Ctx,
					fmt.Sprintf("group_concat_run_%s", id.String()),
				)
			},
			func(bytes, rows, retainedMemory int64) {
				opAnalyzer.Spill(bytes)
				opAnalyzer.SpillRows(rows)
				opAnalyzer.SetMemUsed(max(ctr.memUsed(), retainedMemory))
			},
		)
		aggexec.ConfigureOrderedPercentileSpill(
			agg,
			ctr.spillMem,
			proc.Ctx,
			func() (*os.File, error) {
				spillFS, err := proc.GetSpillFileService()
				if err != nil {
					return nil, err
				}
				id, _ := uuid.NewV7()
				return spillFS.CreateAndRemoveFile(
					proc.Ctx,
					fmt.Sprintf("ordered_percentile_run_%s", id.String()),
				)
			},
			func(bytes, rows, retainedMemory int64) {
				opAnalyzer.Spill(bytes)
				opAnalyzer.SpillRows(rows)
				opAnalyzer.SetMemUsed(max(ctr.memUsed(), retainedMemory))
			},
		)
	}
}

func (hr *ResHashRelated) IsEmpty() bool {
	return hr.Hash == nil || hr.TxnItr == nil
}

func (hr *ResHashRelated) BuildHashTable(
	ctx context.Context, mp *mpool.MPool,
	rebuild bool,
	isStrHash bool, keyNullable bool, groupingAware bool, preAllocated uint64,
	hashAllocation *hashtable.AllocationAccountSelection,
	iteratorAllocation *hashmap.IteratorAllocation,
) error {

	if hr.mp == nil {
		hr.mp = mp
	}

	if hr.mp != mp {
		return moerr.NewInternalError(ctx, "hr.map mpool reset to different mpool")
	}
	if rebuild {
		if hr.Hash != nil {
			hr.Hash.Free()
			hr.Hash = nil
		}
	}

	if hr.Hash != nil {
		return nil
	}

	reuseIterator := hr.TxnItr != nil
	if isStrHash {
		var h *hashmap.StrHashMap
		var err error
		if hashAllocation == nil {
			h, err = hashmap.NewStrHashMap(keyNullable, hr.mp)
		} else {
			h, err = hashmap.NewStrHashMapWithAllocations(
				keyNullable,
				hr.mp,
				hashAllocation,
				iteratorAllocation,
			)
		}
		if err != nil {
			return err
		}
		if groupingAware {
			if err = h.SetGroupingAware(); err != nil {
				h.Free()
				return err
			}
		}
		hr.Hash = h
		if !reuseIterator {
			hr.TxnItr = h.NewTransactionalIterator()
		}
	} else {
		var h *hashmap.IntHashMap
		var err error
		if hashAllocation == nil {
			h, err = hashmap.NewIntHashMap(keyNullable, hr.mp)
		} else {
			h, err = hashmap.NewIntHashMapWithAllocation(
				keyNullable,
				hr.mp,
				hashAllocation,
			)
		}
		if err != nil {
			return err
		}
		hr.Hash = h
		if !reuseIterator {
			hr.TxnItr = h.NewTransactionalIterator()
		}
	}

	if reuseIterator {
		hashmap.IteratorChangeOwner(hr.TxnItr, hr.Hash)
	}
	if preAllocated > 0 {
		if err := hr.Hash.PreAlloc(preAllocated); err != nil {
			return err
		}
	}
	return nil
}
func (hr *ResHashRelated) Free0() {
	if hr.Hash != nil {
		hr.Hash.Free()
		hr.Hash = nil
	}
	if hr.TxnItr != nil {
		hashmap.IteratorClearOwner(hr.TxnItr)
	}
	hr.insertPlan = hashmap.InsertPlan{}
	hr.mp = nil
}

// countNonZeroAndFindKth is a helper function to count the number of non-zero values
// and find index of values, that is the kth non-zero, -1 if there are less than k
// non-zero values.
func countNonZeroAndFindKth(values []uint8, k int) (count int, kth int) {
	count = 0
	kth = -1
	if len(values) < k {
		for _, v := range values {
			if v == 0 {
				continue
			}
			count++
		}
		return count, kth
	}

	for i, v := range values {
		if v == 0 {
			continue
		}

		count++
		if count == k {
			kth = i
			break
		}
	}

	if kth != -1 {
		for i := kth + 1; i < len(values); i++ {
			if values[i] == 0 {
				continue
			}
			count++
		}
	}
	return count, kth
}

func resizeGroupScratch[T any](
	ctr *container,
	values []T,
	length int,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < 0 || ctr == nil || ctr.mp == nil {
		return values, mpool.ErrAllocationAccountInvalid
	}
	if cap(values) >= length {
		values = values[:length]
		clear(values)
		return values, nil
	}
	var (
		next []T
		err  error
	)
	if ctr.allocationAccount == nil {
		next = make([]T, length)
	} else {
		next, err = mpool.MakeSliceAccountedWithCapacityClass[T](
			length,
			ctr.mp,
			ctr.allocationAccount,
			mpool.AllocationOwnerGroup,
			site,
			ctr.recoveryCapacityClass,
		)
		if err != nil {
			// The caller assigns the result back to its owning field. Preserve the
			// old allocation on failure so cleanup retains the only reference.
			return values, err
		}
	}
	if cap(values) > 0 && ctr.allocationAccount != nil {
		mpool.FreeSlice(ctr.mp, values)
	}
	return next, nil
}

// resizeDiscardableGroupScratch grows scratch without retaining the old
// allocation while acquiring its replacement. The contents are disposable;
// releasing first keeps the transient recovery-capacity requirement equal to
// the final physical size, including after an interrupted earlier spill wave.
func resizeDiscardableGroupScratch[T any](
	ctr *container,
	values []T,
	length int,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < 0 || ctr == nil || ctr.mp == nil {
		return values, mpool.ErrAllocationAccountInvalid
	}
	if cap(values) < length {
		freeGroupScratch(ctr, values)
		values = nil
	}
	return resizeGroupScratch(ctr, values, length, site)
}

func freeGroupScratch[T any](ctr *container, values []T) {
	if ctr != nil && ctr.mp != nil && ctr.allocationAccount != nil && cap(values) > 0 {
		mpool.FreeSlice(ctr.mp, values)
	}
}

func (ctr *container) computeBucketIndex(hashCodes []uint64, myLv uint64) {
	// Fibonacci hashing: multiply by a level-dependent odd constant and extract
	// the top bits. This replaces a per-element xxhash call with a single
	// multiply+shift while still providing good bucket distribution even when the
	// source hash has poor low-bit entropy (e.g. int32 keys that produce only
	// 32-bit hash values). Different levels use different multipliers so groups
	// landing in the same bucket at level N get split at level N+1.
	mult := uint64(0x9e3779b97f4a7c15) + myLv*2
	bucketCount := len(ctr.currentSpillBkt)
	if bucketCount == 0 {
		bucketCount = ctr.spillPartitionCount()
	}
	maskBits := uint(spillMaskBits)
	if bucketCount == spillDistinctNumBuckets {
		maskBits = spillDistinctMaskBits
	}
	for i := range hashCodes {
		hashCodes[i] = (hashCodes[i] * mult) >> (64 - maskBits)
	}
}

func canRepartitionGroupSpill(parent *spillBucket) bool {
	return parent == nil || parent.lv < spillMaxPass
}

func (ctr *container) openSpillBucket(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	bkt *spillBucket,
) error {
	if bkt == nil {
		return moerr.NewInternalErrorNoCtx("nil group spill bucket")
	}
	if bkt.file != nil {
		return nil
	}
	var (
		fdToken   *process.ExecutionSpillFDReservation
		diskToken *process.ExecutionSpillDiskReservation
		err       error
	)
	if ctr.budget != nil {
		fdToken, err = ctr.budget.ReserveSpillFD(1)
		if err != nil {
			return err
		}
		diskToken, err = ctr.budget.ReserveSpillDisk(0)
		if err != nil {
			fdToken.Release()
			return err
		}
	}
	file, err := spillfs.CreateAndRemoveFile(proc.Ctx, bkt.name)
	if err != nil {
		if fdToken != nil {
			fdToken.Release()
		}
		if diskToken != nil {
			diskToken.Release()
		}
		return err
	}
	bkt.file = file
	bkt.writer, err = newGroupSpillWriter(ctr, file, proc.Ctx, diskToken)
	if err != nil {
		_ = file.Close()
		bkt.file = nil
		if fdToken != nil {
			fdToken.Release()
		}
		if diskToken != nil {
			diskToken.Release()
		}
		return err
	}
	bkt.fdToken = fdToken
	bkt.diskToken = diskToken
	return nil
}

func (ctr *container) writeSpillRecord(
	proc *process.Process,
	spillfs fileservice.MutableFileService,
	opStats *process.OperatorStats,
	gb *batch.Batch,
	nthBatch int,
	bucket int,
	rows []int32,
	bktFlags []uint8,
	prepareParamKindSources []prepareParamKindRowsSource,
) (int64, int64, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}
	if nthBatch < 0 || nthBatch >= len(ctr.groupByBatches) {
		return 0, 0, moerr.NewInternalErrorNoCtx(
			"group spill aggregate chunk out of range")
	}
	legacyFlags := ctr.allocationAccount == nil
	if legacyFlags && len(bktFlags) != gb.RowCount() {
		return 0, 0, moerr.NewInternalErrorNoCtx(
			"group spill flags do not match group rows")
	}
	for _, row := range rows {
		if row < 0 || int(row) >= gb.RowCount() {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"group spill row out of range")
		}
		if legacyFlags {
			bktFlags[row] = 1
		}
	}
	if legacyFlags {
		defer func() {
			for _, row := range rows {
				bktFlags[row] = 0
			}
		}()
	}

	bkt := ctr.currentSpillBkt[bucket]
	if bkt.file == nil {
		opStats.AddExtraStat("GroupSpillBucketsCreated", 1)
		if err := ctr.openSpillBucket(proc, spillfs, bkt); err != nil {
			return 0, 0, err
		}
	}
	record := spillRecordWriter{target: bkt.writer}
	cnt := int64(len(rows))
	if err := types.WriteInt64(&record, cnt); err != nil {
		return 0, 0, err
	}
	if err := appendSpillGroupByRows(&record, gb, rows); err != nil {
		return 0, 0, err
	}

	const firstMagic = uint64(0x12345678DEADBEEF)
	if err := types.WriteInt64(&record, cnt); err != nil {
		return 0, 0, err
	}
	if err := types.WriteUint64(&record, firstMagic); err != nil {
		return 0, 0, err
	}

	if err := types.WriteInt32(&record, int32(len(ctr.aggList))); err != nil {
		return 0, 0, err
	}
	var fullFlags [][]uint8
	if legacyFlags {
		fullFlags = make([][]uint8, len(ctr.groupByBatches))
		fullFlags[nthBatch] = bktFlags
	}
	clear(prepareParamKindSources)
	hasPrepareParamKinds := false
	for i, ag := range ctr.aggList {
		if fullFlags != nil {
			// The stable intermediate format predates the bounded spill codec and
			// accepts one flag slice per aggregate chunk. Legacy callers do not
			// install a hard allocation account, so keep this compatibility shape
			// off the accounted execution path.
			if err := ag.SaveIntermediateResult(cnt, fullFlags, &record); err != nil {
				return 0, 0, err
			}
		} else {
			if err := ag.SaveSpillIntermediateRows(nthBatch, rows, &record); err != nil {
				return 0, 0, err
			}
		}
		if i < len(ctr.aggExprs) && ctr.aggExprs[i].PreservesFirstArgPrepareParamKind() {
			var err error
			prepareParamKindSources[i], err = newPrepareParamKindSelectedRowsSource(
				ag.PrepareParamKindVectorForChunk(nthBatch), rows)
			if err != nil {
				return 0, 0, err
			}
			hasPrepareParamKinds = hasPrepareParamKinds ||
				prepareParamKindSources[i].summary.seen
		}
	}
	if err := writeSpillBool(&record, hasPrepareParamKinds); err != nil {
		return 0, 0, err
	}
	if hasPrepareParamKinds {
		if err := writePrepareParamKindTrailer(proc.Ctx, &record, ctr.aggExprs,
			&ctr.prepareParamKind, prepareParamKindSources); err != nil {
			return 0, 0, err
		}
	}

	const lastMagic = uint64(0xDEADBEEF12345678)
	if err := types.WriteInt64(&record, cnt); err != nil {
		return 0, 0, err
	}
	if err := types.WriteUint64(&record, lastMagic); err != nil {
		return 0, 0, err
	}
	opStats.AddExtraStat("GroupSpillRecords", 1)
	bkt.cnt += cnt
	return record.written, cnt, nil
}

func (ctr *container) spillDataToDisk(proc *process.Process, opAnalyzer process.Analyzer, parentBkt *spillBucket) (int64, int64, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return 0, 0, err
	}
	// Once exact-key spill owns any COUNT(DISTINCT) state, no generic group
	// record may reintroduce a complete hot-group argument set. Drain the current
	// resident work set before every root or recursive group-spill write.
	if ctr.distinctSpill != nil && !ctr.distinctContributionsPrepared {
		if _, err := ctr.drainExactCountDistinct(proc, opAnalyzer); err != nil {
			return 0, 0, err
		}
	}
	if ctr.recoveryCapacity != nil && opAnalyzer != nil {
		reserved, _ := ctr.recoveryCapacity.Snapshot()
		opAnalyzer.GetOpStats().SetMaxExtraStat(
			"GroupSpillRecoveryReservedBytes",
			int64(min(reserved, math.MaxInt64)),
		)
	}

	var totalBytes, totalRows int64
	var parentLv int
	if parentBkt != nil {
		parentLv = parentBkt.lv
	}
	myLv := parentLv + 1

	// if current spill bucket is not created, create a new one.
	if ctr.currentSpillBkt == nil {
		// The local spill threshold is only a policy hint. At maximum depth,
		// callers may finish a terminal leaf as long as every physical allocation
		// continues to pass the independent statement account.
		if !canRepartitionGroupSpill(parentBkt) {
			return 0, 0, nil
		}

		var parentName string
		if parentBkt != nil {
			parentName = parentBkt.name
		} else {
			uuid, _ := uuid.NewV7()
			parentName = fmt.Sprintf("spill_%s", uuid.String())
		}

		logutil.Infof("spilling data to disk, level %d, parent file %s", myLv, parentName)
		// Create bucket objects; files are created lazily on first write.
		ctr.currentSpillBkt = make([]*spillBucket, ctr.spillPartitionCount())
		for i := range ctr.currentSpillBkt {
			child := &spillBucket{
				lv:   myLv,
				name: fmt.Sprintf("%s_%d", parentName, i),
			}
			if parentBkt != nil {
				child.path = parentBkt.path
				child.pathLen = parentBkt.pathLen
			}
			if child.pathLen >= len(child.path) {
				return 0, 0, moerr.NewInternalErrorNoCtx(
					"group spill path exceeds maximum depth")
			}
			child.path[child.pathLen] = uint8(i)
			child.pathLen++
			ctr.currentSpillBkt[i] = child
		}
	}

	// nothing to spill,
	if ctr.hr.IsEmpty() {
		return 0, 0, nil
	}
	spillStart := time.Now()
	opStats := opAnalyzer.GetOpStats()
	opStats.AddExtraStat("GroupSpillWriteCalls", 1)
	opStats.SetMaxExtraStat("GroupSpillMaxLevel", int64(myLv))
	defer func() {
		opStats.AddExtraStat("GroupSpillWriteNanos", time.Since(spillStart).Nanoseconds())
	}()

	// compute spill bucket.
	var err error
	n := int(ctr.hr.Hash.GroupCount())
	opStats.SetMaxExtraStat("GroupSpillMaxGroups", int64(n))
	if uint64(n) > ctr.spillHashPreAllocSize {
		ctr.spillHashPreAllocSize = uint64(n)
	}
	ctr.spillHashCodes, err = resizeDiscardableGroupScratch(
		ctr,
		ctr.spillHashCodes,
		n,
		GroupAllocationSiteSpillHashCodes,
	)
	if err != nil {
		return 0, 0, err
	}
	hashCodes := ctr.hr.Hash.FillGroupHashes(ctr.spillHashCodes[:n])
	// our hash code from Hash is NOT random, esp, int32/uint32 will hash to a 32 bit value,
	// bummer.
	ctr.computeBucketIndex(hashCodes, uint64(myLv))

	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return 0, 0, err
	}

	// Process one groupByBatch at a time. A record never contains more than one
	// hash-map unit, so serialization and reload scratch stay bounded even when
	// every group hashes to the same bucket.
	//
	nBatches := len(ctr.groupByBatches)
	var compactedAggChunks int64
	prepareParamKindSources := make([]prepareParamKindRowsSource, len(ctr.aggList))
	maxBatchRows := 0
	totalBatchRows := 0
	for _, gb := range ctr.groupByBatches {
		if gb == nil {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"group spill contains a nil group batch")
		}
		rows := gb.RowCount()
		if rows < 0 || rows > aggBatchSize || totalBatchRows > n-rows {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"group spill batch rows do not match hash groups")
		}
		totalBatchRows += rows
		maxBatchRows = max(maxBatchRows, rows)
	}
	if totalBatchRows != n {
		return 0, 0, moerr.NewInternalErrorNoCtx(
			"group spill batch rows do not match hash groups")
	}
	// The stable, unaccounted compatibility codec still selects with flags.
	// Accounted spill passes the partition rows directly and does not admit or
	// scan a duplicate row-parallel selection.
	if ctr.allocationAccount == nil {
		ctr.spillFlagFlat, err = resizeDiscardableGroupScratch(
			ctr, ctr.spillFlagFlat, maxBatchRows, GroupAllocationSiteSpillFlags)
		if err != nil {
			return 0, 0, err
		}
	}
	// Allocate the row borrower once for the largest chunk. A later uneven
	// chunk only reslices it, avoiding overlapping replacement capacity.
	ctr.spillBucketRows, err = resizeDiscardableGroupScratch(
		ctr, ctr.spillBucketRows, maxBatchRows, GroupAllocationSiteSpillRows)
	if err != nil {
		return 0, 0, err
	}

	hcOffset := 0
	bucketCount := len(ctr.currentSpillBkt)
	for nthBatch, gb := range ctr.groupByBatches {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return 0, 0, err
		}

		rc := gb.RowCount()
		if rc == 0 {
			continue
		}
		batchHC := hashCodes[hcOffset : hcOffset+rc]
		hcOffset += rc

		var bktFlags []uint8
		if ctr.allocationAccount == nil {
			bktFlags = ctr.spillFlagFlat[:rc]
			clear(bktFlags)
		}

		// Partition the batch in two linear passes. Only the counts/cursors are
		// fixed-size; row ids occupy exactly O(rc) accounted recovery scratch.
		var bucketCounts [spillMaxNumBuckets]int
		var bucketOffsets [spillMaxNumBuckets + 1]int
		var bucketCursors [spillMaxNumBuckets]int
		for _, hash := range batchHC {
			bucketCounts[int(hash&uint64(bucketCount-1))]++
		}
		for bucket, count := range bucketCounts[:bucketCount] {
			bucketOffsets[bucket+1] = bucketOffsets[bucket] + count
			bucketCursors[bucket] = bucketOffsets[bucket]
		}
		for row, hash := range batchHC {
			bucket := int(hash & uint64(bucketCount-1))
			ctr.spillBucketRows[bucketCursors[bucket]] = int32(row)
			bucketCursors[bucket]++
		}
		writeRows := func(bucket int, rows []int32) error {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return err
			}
			written, spilled, err := ctr.writeSpillRecord(
				proc, spillfs, opStats, gb, nthBatch, bucket,
				rows, bktFlags,
				prepareParamKindSources,
			)
			if err != nil {
				return err
			}
			totalBytes += written
			totalRows += spilled
			if nBatches > 1 {
				compactedAggChunks += int64(nBatches-1) * int64(len(ctr.aggList))
			}
			return nil
		}
		for bucket, selected := range bucketCounts[:bucketCount] {
			if selected > 0 {
				end := bucketOffsets[bucket+1]
				for start := bucketOffsets[bucket]; start < end; start += hashmap.UnitLimit {
					rows := ctr.spillBucketRows[start:min(start+hashmap.UnitLimit, end)]
					if err := writeRows(bucket, rows); err != nil {
						return 0, 0, err
					}
				}
			}
		}

		// The last bucket has no following loop boundary. Check once more so
		// cancellation during its serialization or write is still observed
		// before starting another spill phase.
		if err, canceled := vm.CancelCheck(proc); canceled {
			return 0, 0, err
		}
	}
	opStats.AddExtraStat("GroupSpillSerializedBytes", totalBytes)
	opStats.AddExtraStat("GroupSpillAggChunkHeadersOmitted", compactedAggChunks)

	if ctr.allocationAccount != nil {
		// Return the write-side borrowers before reload starts. The reserved
		// floor remains owned by the operator and can now be reused by bounded
		// decode staging without increasing the query-wide charge. Legacy Group
		// retains its historical reusable scratch until terminal cleanup.
		freeGroupScratch(ctr, ctr.spillHashCodes)
		ctr.spillHashCodes = nil
		freeGroupScratch(ctr, ctr.spillFlagFlat)
		ctr.spillFlagFlat = nil
		freeGroupScratch(ctr, ctr.spillBucketRows)
		ctr.spillBucketRows = nil
	}

	// reset ctr for next spill
	ctr.resetForSpill()
	return totalBytes, totalRows, nil
}

type preparedSpillReloadRecord struct {
	rows              int
	reusedAggExec     bool
	readNanos         int64
	aggUnmarshalNanos int64
}

// prepareSpillReloadRecord consumes and admits one record up to, but not
// including, the first hash-table mutation. Every error from this method is
// therefore safe to retry from the caller's saved record boundary.
func (ctr *container) prepareSpillReloadRecord(
	proc *process.Process,
	opAnalyzer process.Analyzer,
	opStats *process.OperatorStats,
	bkt *spillBucket,
	reader *groupSpillReader,
	aggExprs []aggexec.AggFuncExecExpression,
) (record preparedSpillReloadRecord, eof bool, retErr error) {
	readStart := time.Now()
	cnt, err := types.ReadInt64(reader)
	if err != nil {
		if err == io.EOF {
			return record, true, nil
		}
		return record, false, err
	}
	if cnt == 0 {
		record.readNanos = time.Since(readStart).Nanoseconds()
		return record, false, nil
	}
	if cnt < 0 || cnt > hashmap.UnitLimit {
		return record, false, moerr.NewInvalidInputNoCtxf(
			"invalid group spill record row count %d", cnt)
	}
	record.rows = int(cnt)
	if err = ctr.ensureRecoveryCapacity(record.rows, opAnalyzer); err != nil {
		return record, false, err
	}

	record.reusedAggExec = len(ctr.spillAggList) == len(aggExprs) &&
		len(ctr.spillAggList) > 0
	if len(ctr.aggList) != len(aggExprs) {
		ctr.aggList, err = ctr.makeAggList(aggExprs)
		if err != nil {
			return record, false, err
		}
		if bkt.lv >= spillMaxPass {
			ctr.configureOrderedAggSpill(proc, opAnalyzer, ctr.aggList)
		}
	}
	if len(ctr.spillAggList) != len(aggExprs) {
		ctr.spillAggList, err = ctr.makeSpillAggList(aggExprs)
		if err != nil {
			return record, false, err
		}
	}

	if ctr.spillGbBatch == nil {
		ctr.spillGbBatch, err = ctr.createNewGroupByBatchWithAllocation(
			nil,
			record.rows,
			ctr.spillGroupByAllocation,
		)
		if err != nil {
			return record, false, err
		}
	}
	gbBatch := ctr.spillGbBatch
	gbBatch.CleanOnlyData()
	if err = unmarshalSpillGroupByRows(reader, gbBatch, record.rows, ctr.mp); err != nil {
		return record, false, err
	}

	checkMagic, err := types.ReadUint64(reader)
	if err != nil {
		return record, false, err
	}
	if checkMagic != uint64(cnt) {
		return record, false, moerr.NewInternalError(proc.Ctx, "spill groupby cnt mismatch")
	}
	checkMagic, err = types.ReadUint64(reader)
	if err != nil {
		return record, false, err
	}
	if checkMagic != 0x12345678DEADBEEF {
		return record, false, moerr.NewInternalError(proc.Ctx, "spill groupby magic number mismatch")
	}

	nAggs, err := types.ReadInt32(reader)
	if err != nil {
		return record, false, err
	}
	if nAggs != int32(len(ctr.spillAggList)) {
		return record, false, moerr.NewInternalError(proc.Ctx, "spill agg cnt mismatch")
	}
	record.readNanos = time.Since(readStart).Nanoseconds()

	aggStart := time.Now()
	aggErr := func() error {
		for _, ag := range ctr.spillAggList {
			if ctr.allocationAccount == nil {
				if err := ag.UnmarshalFromReader(reader, ctr.mp); err != nil {
					return err
				}
			} else {
				if err := ag.UnmarshalSpillFromReader(reader, ctr.mp); err != nil {
					return err
				}
			}
			if err := validateDecodedAggregateGroupCount(ag, record.rows); err != nil {
				return err
			}
		}
		return nil
	}()
	record.aggUnmarshalNanos = time.Since(aggStart).Nanoseconds()
	if aggErr != nil {
		return record, false, aggErr
	}

	hasPrepareParamKinds, err := types.ReadBool(reader)
	if err != nil {
		return record, false, err
	}
	if hasPrepareParamKinds {
		var spillPrepareParamKind aggexec.PrepareParamKindStates
		spillPrepareParamKind.Reset(aggExprs)
		targets := make([]prepareParamKindRowsTarget, len(ctr.spillAggList))
		for i, ag := range ctr.spillAggList {
			targets[i] = prepareParamKindFlatTarget(ag)
		}
		prepareParamKindSummaries, err := readPrepareParamKindTrailer(
			proc.Ctx,
			reader,
			int32(len(ctr.spillAggList)),
			&spillPrepareParamKind,
			targets,
			ctr.mp,
			true,
			true,
		)
		if err != nil {
			return record, false, err
		}
		for i, ag := range ctr.spillAggList {
			if i >= len(aggExprs) || !aggExprs[i].PreservesFirstArgPrepareParamKind() {
				continue
			}
			if !prepareParamKindSummaries[i].rows {
				if prepareParamKindSummaries[i].seen {
					ag.SetPrepareParamKind(prepareParamKindSummaries[i].kind)
				}
			}
		}
	}

	checkMagic, err = types.ReadUint64(reader)
	if err != nil {
		return record, false, err
	}
	if checkMagic != uint64(cnt) {
		return record, false, moerr.NewInternalError(proc.Ctx, "spill agg cnt mismatch")
	}
	checkMagic, err = types.ReadUint64(reader)
	if err != nil {
		return record, false, err
	}
	if checkMagic != 0xDEADBEEF12345678 {
		return record, false, moerr.NewInternalError(proc.Ctx, "spill agg magic number mismatch")
	}

	if ctr.hr.IsEmpty() {
		// bkt.cnt is an upper bound: records from different spill waves can
		// contain duplicate keys. Cap the reload allocation at a hash-table
		// cardinality this operator has already held under the same spill limit.
		rawPreAllocated := min(uint64(bkt.cnt), ctr.spillHashPreAllocSize)
		preAllocated := ctr.boundedSpillReloadPreAlloc(bkt.cnt)
		opStats.SetMaxExtraStat("GroupSpillPreallocRows", int64(preAllocated))
		if preAllocated < rawPreAllocated {
			opStats.AddExtraStat("GroupSpillPreallocCapped", 1)
		}
		if err = ctr.buildSpillReloadHashTable(
			proc.Ctx, preAllocated, opStats); err != nil {
			return record, false, err
		}
	}
	return record, false, nil
}

func (ctr *container) retrySpillReloadRecord(
	proc *process.Process,
	opAnalyzer process.Analyzer,
	opStats *process.OperatorStats,
	bkt *spillBucket,
	reader *groupSpillReader,
	recordStart int64,
	cause error,
) (bool, error) {
	if ctr.allocationAccount == nil ||
		!mpool.IsRetryableAllocationCapacity(cause) {
		return false, cause
	}
	ctr.freeSpillReloadStaging()
	if dropped, err := reader.DisableReadAheadAndRewind(recordStart); err != nil {
		return false, err
	} else if dropped {
		if opStats != nil {
			opStats.AddExtraStat("GroupSpillReadAheadFallbacks", 1)
			opStats.AddExtraStat("GroupSpillReloadRetries", 1)
		}
		return true, nil
	}
	if ctr.hr.IsEmpty() || ctr.hr.Hash.GroupCount() == 0 {
		return false, cause
	}
	// A capacity rejection, unlike the local spill threshold, proves that the
	// current terminal work set cannot grow safely. Preserve the original
	// requested/used/limit error once no further partition can release it.
	if !canRepartitionGroupSpill(bkt) {
		return false, cause
	}

	// prepareSpillReloadRecord has not mutated the hash table. Drop only its
	// incoming staging, externalize the resident prefix, and replay the record
	// from its logical boundary. Rewind accounts for read-ahead on the file.
	before := ctr.hr.Hash.GroupCount()
	bytes, rows, err := ctr.spillDataToDisk(proc, opAnalyzer, bkt)
	if err != nil {
		return false, err
	}
	if rows <= 0 || uint64(rows) < before {
		return false, moerr.NewInternalErrorNoCtx(
			"group reload capacity recovery made no measurable spill progress")
	}
	if err = reader.Rewind(recordStart); err != nil {
		return false, err
	}
	opAnalyzer.Spill(bytes)
	opAnalyzer.SpillRows(rows)
	if opStats != nil {
		opStats.AddExtraStat("GroupSpillReloadRetries", 1)
	}
	return true, nil
}

// load spilled data from the spill bucket queue.
func (ctr *container) loadSpilledData(proc *process.Process, opAnalyzer process.Analyzer, aggExprs []aggexec.AggFuncExecExpression) (_ bool, retErr error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}

	// first, if there is current spill bucket, transfer it to the spill bucket queue.
	if ctr.currentSpillBkt != nil {
		if ctr.spillBkts == nil {
			ctr.spillBkts = list.New[*spillBucket]()
		}
		for i, bkt := range ctr.currentSpillBkt {
			if bkt.cnt > 0 {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return false, err
				}
				if err := bkt.flushWriter(); err != nil {
					bkt.free()
					ctr.currentSpillBkt[i] = nil
					return false, err
				}
				ctr.spillBkts.PushBack(bkt)
			} else {
				bkt.free()
			}
			// Ownership has moved to spillBkts or ended at free(). Do not leave
			// a second source reference behind if a later flush fails/cancels.
			ctr.currentSpillBkt[i] = nil
		}
		ctr.currentSpillBkt = nil
		if err, canceled := vm.CancelCheck(proc); canceled {
			return false, err
		}
	}

	// then, if there is no spill bucket in the queue, done.
	if ctr.spillBkts == nil || ctr.spillBkts.Len() == 0 {
		// done
		return false, nil
	}

	// popped bkt must be defer freed.
	bkt := ctr.spillBkts.PopBack().Value
	defer func() {
		if err := bkt.free(); err != nil && retErr == nil {
			retErr = err
		}
		ctr.freeSpillAggList()
	}()
	opStats := opAnalyzer.GetOpStats()
	var reloadRecords, reusedAggExecRecords int64
	var readNanos, aggUnmarshalNanos, hashMergeNanos int64
	defer func() {
		opStats.AddExtraStat("GroupSpillReloadRecords", reloadRecords)
		opStats.AddExtraStat("GroupSpillAggExecReuseRecords", reusedAggExecRecords)
		opStats.AddExtraStat("GroupSpillReloadReadNanos", readNanos)
		opStats.AddExtraStat("GroupSpillAggUnmarshalNanos", aggUnmarshalNanos)
		opStats.AddExtraStat("GroupSpillHashMergeNanos", hashMergeNanos)
	}()
	opStats.AddExtraStat("GroupSpillReloadBuckets", 1)
	opStats.AddExtraStat("GroupSpillReloadRows", bkt.cnt)
	opStats.SetMaxExtraStat("GroupSpillMaxBucketRows", bkt.cnt)
	opStats.SetMaxExtraStat("GroupSpillMaxLevel", int64(bkt.lv))
	reloadStart := time.Now()
	reloadRecorded := false
	recordReloadTime := func() {
		if !reloadRecorded {
			opStats.AddExtraStat("GroupSpillReloadNanos", time.Since(reloadStart).Nanoseconds())
			reloadRecorded = true
		}
	}
	defer recordReloadTime()

	// reposition to the start of the file.
	if _, err := bkt.file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	// Reset the current hash/group state. Reload staging is created lazily for
	// each record because it must be released before a recursive respill can
	// borrow the mandatory recovery floor.
	ctr.resetForSpill()

	if ctr.spillReader == nil {
		reader, err := newGroupSpillReader(ctr, bkt.file, proc.Ctx)
		if err != nil {
			return false, err
		}
		ctr.spillReader = reader
	} else {
		if err := ctr.spillReader.Reset(bkt.file); err != nil {
			return false, err
		}
	}
	bufferedFile := ctr.spillReader

reloadLoop:
	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return false, err
		}

		recordStart := bufferedFile.Position()
		var record preparedSpillReloadRecord
		for {
			prepared, eof, prepareErr := ctr.prepareSpillReloadRecord(
				proc, opAnalyzer, opStats, bkt, bufferedFile, aggExprs)
			record = prepared
			readNanos += record.readNanos
			aggUnmarshalNanos += record.aggUnmarshalNanos
			if prepareErr == nil {
				if eof {
					break reloadLoop
				}
				break
			}
			retried, retryErr := ctr.retrySpillReloadRecord(
				proc, opAnalyzer, opStats, bkt, bufferedFile, recordStart, prepareErr)
			if !retried {
				return false, retryErr
			}
		}
		if record.rows == 0 {
			continue
		}
		mergeStart := time.Now()
		gbBatch := ctr.spillGbBatch
		rowCount := record.rows
		hashBytesBefore := ctr.hr.Hash.Size()
		hashKeyVecs := ctr.hashKeyVectors(gbBatch.Vecs)
		var preview groupInsertPreview
		err := ctr.hr.TxnItr.PreviewInsert(
			0, rowCount, hashKeyVecs,
			ctr.hr.Hash.GroupCount(), &ctr.hr.insertPlan)
		if err == nil {
			preview.values = ctr.hr.insertPlan.Values()
			preview.inserted = ctr.hr.insertPlan.Inserted()
			preview.newGroups = int(ctr.hr.insertPlan.NewGroups())
			err = ctr.hr.Hash.PreAlloc(ctr.hr.insertPlan.NewGroups())
		}
		if err == nil {
			err = ctr.preflightBuildChunk(
				gbBatch.Vecs, 0, rowCount,
				preview.inserted, preview.newGroups)
		}
		if err == nil {
			for j, agg := range ctr.aggList {
				if err = agg.PreflightBatchMerge(
					ctr.spillAggList[j], 0,
					preview.values); err != nil {
					break
				}
			}
		}
		if err != nil {
			ctr.cancelGroupByPreflights()
			retried, retryErr := ctr.retrySpillReloadRecord(
				proc, opAnalyzer, opStats, bkt, bufferedFile, recordStart, err)
			if !retried {
				return false, retryErr
			}
			continue reloadLoop
		}
		vals, more, err := ctr.commitGroupByChunk(
			gbBatch.Vecs, 0, rowCount, preview)
		if err != nil {
			if !isGroupPrePublicationError(err) {
				return false, err
			}
			ctr.cancelGroupByPreflights()
			retried, retryErr := ctr.retrySpillReloadRecord(
				proc, opAnalyzer, opStats, bkt, bufferedFile, recordStart, err)
			if !retried {
				return false, retryErr
			}
			continue reloadLoop
		}

		if len(ctr.aggList) > 0 {
			if more > 0 {
				for j := range ctr.aggList {
					if err := ctr.aggList[j].GroupGrow(more); err != nil {
						return false, err
					}
				}
			}
			for j, ag := range ctr.aggList {
				if err := ag.BatchMerge(ctr.spillAggList[j], 0, vals[:rowCount]); err != nil {
					return false, err
				}
			}
		}
		reloadRecords++
		if record.reusedAggExec {
			reusedAggExecRecords++
		}
		observeHashGrowth(opStats, "GroupHashReload", hashBytesBefore, ctr.hr.Hash.Size())
		hashMergeNanos += time.Since(mergeStart).Nanoseconds()

		if ctr.needSpill(opAnalyzer) && canRepartitionGroupSpill(bkt) {
			ctr.freeSpillReloadStaging()
			if bytes, rows, err := ctr.spillDataToDisk(proc, opAnalyzer, bkt); err != nil {
				return false, err
			} else {
				opAnalyzer.Spill(bytes)
				opAnalyzer.SpillRows(rows)
			}
		}
	}

	recordReloadTime()
	if ctr.allocationAccount != nil {
		// Accounted reload returns optional read-ahead between buckets so the
		// statement can reuse that capacity. Legacy Group historically retains
		// and resets the buffer across buckets.
		bufferedFile.DropReadAhead()
	}

	// respilling happened, so we finish the last batch and recursive down
	if ctr.isSpilling() {
		opStats.AddExtraStat("GroupSpillRespills", 1)
		ctr.freeSpillReloadStaging()
		if bytes, rows, err := ctr.spillDataToDisk(proc, opAnalyzer, bkt); err != nil {
			return false, err
		} else {
			opAnalyzer.Spill(bytes)
			opAnalyzer.SpillRows(rows)
		}
		return ctr.loadSpilledData(proc, opAnalyzer, aggExprs)
	}
	if ctr.distinctContributionsPrepared {
		if err := ctr.applyDistinctContributions(proc, bkt); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (ctr *container) getNextFinalResult(
	proc *process.Process,
) (vm.CallResult, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now we need to flush the final result of agg to output batches.
	if ctr.currBatchIdx >= len(ctr.groupByBatches) ||
		(ctr.currBatchIdx == len(ctr.groupByBatches)-1 &&
			ctr.groupByBatches[ctr.currBatchIdx].RowCount() == 0) {
		// exhauseed all batches, or, last group by batch has no data,
		// done.
		return vm.CancelResult, nil
	}

	curr := ctr.currBatchIdx
	ctr.currBatchIdx += 1

	if curr == 0 {
		// flush aggs final result to vectors, all aggs follow groupby columns.
		for i, ag := range ctr.aggList {
			vecs, err := aggexec.FlushWithContext(proc.Ctx, ag)
			if err != nil {
				return vm.CancelResult, err
			}
			kind := ctr.prepareParamKind.Get(i)
			for _, vec := range vecs {
				// Preserving aggregates (MIN/MAX/ANY/MAX_BY and value
				// windows) attach the winner's row provenance directly to
				// their state vector. Keep that exact metadata; the scalar
				// execution summary is only a compatibility fallback for
				// aggregate implementations that materialize ordinary state.
				if !vec.HasPrepareParamKind() {
					vec.SetPrepareParamKind(kind)
				}
			}
			for j := range vecs {
				ctr.groupByBatches[j].Vecs = append(
					ctr.groupByBatches[j].Vecs, vecs[j])
			}
		}

		ctr.freeAggList()
	}

	// get the groupby batch
	batch := ctr.groupByBatches[curr]
	res := vm.NewCallResult()
	res.Batch = batch
	return res, nil
}

func (ctr *container) outputOneBatchFinal(proc *process.Process, opAnalyzer process.Analyzer, aggExprs []aggexec.AggFuncExecExpression) (vm.CallResult, error) {
	if err := ctr.releaseFinalRecoveryCapacity(); err != nil {
		return vm.CancelResult, err
	}
	// read next result batch
	res, err := ctr.getNextFinalResult(proc)
	if err != nil {
		return vm.CancelResult, err
	}

	// or should we check res.Status == vm.ExecStop
	if res.Batch != nil {
		return res, nil
	}

	loaded, err := ctr.loadSpilledData(proc, opAnalyzer, aggExprs)
	if err != nil {
		return vm.CancelResult, err
	}
	if loaded {
		return ctr.outputOneBatchFinal(proc, opAnalyzer, aggExprs)
	}
	ctr.finishDistinctContributions()
	if err := ctr.releaseFinalRecoveryCapacity(); err != nil {
		return vm.CancelResult, err
	}
	return res, nil
}

// newRuntimeEmptyGroupingSetBatch builds the key rows required by SQL when an
// all-rolled grouping set receives no input. A nil setIDs slice describes one
// legacy/static grouping set whose every key is rolled up. A non-nil slice
// describes dynamic grouping sets, whose final key column carries the set id.
func (ctr *container) newRuntimeEmptyGroupingSetBatch(
	groupTypes []types.Type,
	setIDs []int64,
) (*batch.Batch, error) {
	rows := 1
	rollupColumns := len(groupTypes)
	if setIDs != nil {
		rows = len(setIDs)
		rollupColumns--
	}
	output := batch.NewOffHeapWithSize(len(groupTypes))
	if err := output.SetAllocationAccount(ctr.groupByAllocation); err != nil {
		output.Clean(ctr.mp)
		return nil, err
	}
	for i := 0; i < rollupColumns; i++ {
		vec, err := vector.NewRollupConstWithAllocation(
			groupTypes[i], rows, ctr.mp, ctr.groupByAllocation)
		if err != nil {
			output.Clean(ctr.mp)
			return nil, err
		}
		output.Vecs[i] = vec
	}
	if setIDs != nil {
		setIDVector, err := vector.NewOffHeapVecWithTypeAndAllocation(
			groupTypes[len(groupTypes)-1], ctr.groupByAllocation)
		if err != nil {
			output.Clean(ctr.mp)
			return nil, err
		}
		output.Vecs[len(output.Vecs)-1] = setIDVector
		if err = vector.AppendFixedList(setIDVector, setIDs, nil, ctr.mp); err != nil {
			output.Clean(ctr.mp)
			return nil, err
		}
	}
	output.SetRowCount(rows)
	return output, nil
}

func (ctr *container) memUsed() int64 {
	sz := ctr.mp.CurrNB()
	for _, agg := range ctr.aggList {
		sz += agg.AdditionalMemorySize()
	}
	return sz
}

func (ctr *container) needSpill(opAnalyzer process.Analyzer) bool {

	memUsed := ctr.memUsed()
	opAnalyzer.SetMemUsed(memUsed)

	// Generic group spill partitions groups using the grouping hash table. H0
	// has exactly one aggregate group and no grouping hash table. Aggregates
	// that support H0 spilling (for example ordered GROUP_CONCAT) manage it in
	// their own executors.
	if ctr.mtyp == H0 {
		return false
	}

	// Values below 10K are the debug group-count threshold. Otherwise the
	// threshold is measured in bytes.
	if ctr.spillMem < 10000 {
		return ctr.hr.Hash.GroupCount() >= uint64(ctr.spillMem)
	}
	return memUsed > ctr.spillMem
}

func (ctr *container) makeAggList(aggExprs []aggexec.AggFuncExecExpression) ([]aggexec.GroupAggFuncExec, error) {
	return ctr.makeAggListWithAllocation(aggExprs, ctr.aggregateAllocation)
}

func (ctr *container) makeSpillAggList(
	aggExprs []aggexec.AggFuncExecExpression,
) ([]aggexec.GroupAggFuncExec, error) {
	return ctr.makeAggListWithAllocation(aggExprs, ctr.spillAggregateAllocation)
}

func (ctr *container) buildSpillReloadHashTable(
	ctx context.Context,
	preAllocated uint64,
	opStats *process.OperatorStats,
) error {
	err := ctr.buildHashTable(ctx, preAllocated)
	if err == nil || preAllocated <= aggHtPreAllocSize ||
		!mpool.IsRetryableAllocationCapacity(err) {
		return err
	}

	// Proven-cardinality preallocation is only an optimization. If decode
	// staging makes its transient peak too large, release the partial target and
	// retry from the historical minimum instead of failing a recoverable bucket.
	ctr.hr.Free0()
	for _, agg := range ctr.aggList {
		agg.Free()
	}
	if opStats != nil {
		opStats.AddExtraStat("GroupSpillPreallocFallbacks", 1)
	}
	return ctr.buildHashTable(ctx, 0)
}

func (ctr *container) makeAggListWithAllocation(
	aggExprs []aggexec.AggFuncExecExpression,
	allocation *aggexec.AllocationAccount,
) ([]aggexec.GroupAggFuncExec, error) {
	var err error
	aggList := make([]aggexec.GroupAggFuncExec, len(aggExprs))
	for i, agExpr := range aggExprs {
		typs := make([]types.Type, len(agExpr.GetArgExpressions()))
		for j, arg := range agExpr.GetArgExpressions() {
			typs[j] = types.NewWithCharset(
				types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale, uint8(arg.Typ.Charset),
			)
		}
		singleGroup := ctr.mtyp == H0
		if ctr.legacyTextMinMax || ctr.legacyVarianceState {
			if singleGroup {
				aggList[i], err = aggexec.MakeSingleGroupAggWithLegacyRemoteState(
					ctr.mp, agExpr.GetAggID(), agExpr.IsDistinct(), ctr.legacyTextMinMax,
					ctr.legacyVarianceState, allocation, agExpr.GetExtraInformation(), typs...)
			} else {
				aggList[i], err = aggexec.MakeGroupAggWithLegacyRemoteState(
					ctr.mp, agExpr.GetAggID(), agExpr.IsDistinct(), ctr.legacyTextMinMax,
					ctr.legacyVarianceState, allocation, agExpr.GetExtraInformation(), typs...)
			}
		} else if singleGroup {
			aggList[i], err = aggexec.MakeSingleGroupAgg(
				ctr.mp, agExpr.GetAggID(), agExpr.IsDistinct(), allocation,
				agExpr.GetExtraInformation(), typs...)
		} else {
			aggList[i], err = aggexec.MakeGroupAgg(
				ctr.mp, agExpr.GetAggID(), agExpr.IsDistinct(), allocation,
				agExpr.GetExtraInformation(), typs...)
		}
		if err != nil {
			freeAggListPartial(aggList, i)
			return nil, err
		}
	}

	if ctr.mtyp != H0 {
		aggexec.SyncAggregatorsToChunkSize(aggList, aggBatchSize)
	} else {
		aggexec.SyncAggregatorsToChunkSize(aggList, 1)
		for _, ag := range aggList {
			if err := ag.GroupGrow(1); err != nil {
				freeAggList(aggList)
				return nil, err
			}
		}
	}
	return aggList, nil
}

func useLegacyTextMinMaxForRemote(proc *process.Process) bool {
	if proc == nil || proc.Ctx == nil {
		return false
	}
	remote, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool)
	if !remote {
		return false
	}
	value, ok := moruntime.ServiceRuntime(proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return !ok || !valid || version < defines.MORPCVersion14
}

func useLegacyVarianceStateForRemote(proc *process.Process) bool {
	if proc == nil || proc.Ctx == nil {
		return false
	}
	remote, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool)
	if !remote {
		return false
	}
	value, ok := moruntime.ServiceRuntime(proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return !ok || !valid || version < defines.MORPCVersion35
}

// freeAggListPartial frees the first n aggregators in the list.
func freeAggListPartial(aggList []aggexec.GroupAggFuncExec, n int) {
	for i := 0; i < n && i < len(aggList); i++ {
		if aggList[i] != nil {
			aggList[i].Free()
		}
	}
}

// freeAggList frees all aggregators in the list.
func freeAggList(aggList []aggexec.GroupAggFuncExec) {
	freeAggListPartial(aggList, len(aggList))
}

func (ctr *container) sanityCheck() {
	if util.Debug {
		originGroupCount := ctr.hr.Hash.GroupCount()
		batchRowCount := 0
		for _, batch := range ctr.groupByBatches {
			batchRowCount += batch.RowCount()
		}
		if batchRowCount != int(originGroupCount) {
			panic(moerr.NewInternalErrorNoCtx("group count mismatch"))
		}
	}
}
