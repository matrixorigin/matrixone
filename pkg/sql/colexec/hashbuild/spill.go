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

package hashbuild

import (
	"fmt"
	"io"
	"math"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMagic      = 0x12345678DEADBEEF
	spillBufferSize = 8192 // Buffer 8192 rows before flushing
	// Serialized records are accumulated per bucket across source batches.
	// Allocation is admitted lazily against the lifecycle scratch lease and
	// falls back to direct writes when the hard budget has no headroom.
	spillWriteCoalesceSize = 64 << 10
)

func (ctr *container) dropSpillScratchBuffers() {
	if cap(ctr.spillHashValues) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillHashValues)
	}
	if cap(ctr.spillBucketRowIds) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillBucketRowIds)
	}
	if ctr.spillAccountedWrite != nil {
		ctr.spillAccountedWrite.Free()
		ctr.spillAccountedWrite = nil
	}
	for i := range ctr.spillAccountedBuckets {
		if ctr.spillAccountedBuckets[i] != nil {
			ctr.spillAccountedBuckets[i].Free()
			ctr.spillAccountedBuckets[i] = nil
		}
	}
	for bucket := range ctr.spillBucketWriteRows {
		ctr.spillBucketWriteRows[bucket] = 0
	}
	ctr.spillHashValues = nil
	ctr.spillBucketRowIds = nil
	for i := range ctr.spillBucketCounts {
		ctr.spillBucketCounts[i] = 0
	}
	for i := range ctr.spillBucketOffsets {
		ctr.spillBucketOffsets[i] = 0
	}
	ctr.spillKeyVecs = nil
	ctr.spillAllocationMP = nil
	ctr.spillCoalesceDisabled = false
}

func growHashBuildSpillSlice[T any](
	values []T,
	length int,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	site mpool.AllocationSite,
	capacityClass mpool.AllocationCapacityClass,
) ([]T, error) {
	if length < 0 || account == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if length <= cap(values) {
		return values[:length], nil
	}
	capacity := cap(values)
	if capacity == 0 {
		capacity = 1
	}
	for capacity < length {
		if capacity > math.MaxInt/2 {
			capacity = length
			break
		}
		capacity *= 2
	}
	next, err := mpool.MakeSliceAccountedWithCapacityClass[T](
		capacity,
		mp,
		account,
		HashBuildAllocationOwner,
		site,
		capacityClass,
	)
	if err != nil {
		return nil, err
	}
	copy(next, values)
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
	return next[:length], nil
}

func marshalSpillRecordAccounted(
	bat *batch.Batch,
	buf *mpool.AccountedBuffer,
) (int64, error) {
	if bat == nil || bat.RowCount() == 0 || buf == nil {
		return 0, nil
	}
	cnt := int64(bat.RowCount())
	buf.Reset()
	batchSize, err := bat.MarshalBinaryWithGroupingSize()
	if err != nil || batchSize > math.MaxInt-24 {
		if err != nil {
			return 0, err
		}
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if err := buf.EnsureCapacity(batchSize + 24); err != nil {
		return 0, err
	}
	if err := buf.WriteInt64(cnt); err != nil {
		return 0, err
	}
	batchSizePos := buf.Len()
	if err := buf.WriteInt64(0); err != nil {
		return 0, err
	}
	batchStart := buf.Len()
	if err := bat.MarshalBinaryWithGroupingTo(buf); err != nil {
		return 0, err
	}
	serializedSize := int64(buf.Len() - batchStart)
	if err := buf.SetInt64(batchSizePos, serializedSize); err != nil {
		return 0, err
	}
	if err := buf.WriteUint64(uint64(spillMagic)); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (ctr *container) writeSpillPayload(
	proc *process.Process,
	file *os.File,
	payload []byte,
	rows int64,
	analyzer process.Analyzer,
) error {
	if file == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	// All initial-spill writes funnel through this helper. Check after any
	// vector projection/marshal and immediately before the physical write so a
	// cancellation that raced lazy file creation or serialization does not
	// start stale I/O. An already-running os.File.Write is not interruptible.
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}

	if ctr.hashmapBuilder.budget == nil || ctr.spillBundle == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	_, _, err := ctr.spillBundle.growDisk(file, ctr.hashmapBuilder.budget, uint64(len(payload)))
	if err != nil {
		return err
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	written, err := file.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	// The exact payload length was admitted. Record logical ownership only
	// after the full write; partial writes retain the conservative charge
	// until the enclosing bundle closes the file.
	ctr.spillBundle.recordDiskWrite(file, rows, uint64(written))
	if analyzer != nil {
		analyzer.Spill(int64(written))
		analyzer.SpillRows(rows)
	}

	return nil
}

func (ctr *container) getSpillFS(proc *process.Process) (fileservice.MutableFileService, error) {
	if ctr.spillFS != nil {
		return ctr.spillFS, nil
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	ctr.spillFS = fs
	return fs, nil
}

// ensureSpillFile lazily creates an anonymous spill file for the given bucket.
func (ctr *container) ensureSpillFile(proc *process.Process, files []*os.File, bucket int) (*os.File, error) {
	if bucket < 0 || bucket >= len(files) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if files[bucket] != nil {
		return files[bucket], nil
	}
	if ctr.spillUUID == "" {
		return nil, moerr.NewInternalErrorNoCtx("ensureSpillFile: spillUUID not initialized")
	}
	spillfs, err := ctr.getSpillFS(proc)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("join_%s_%d_build", ctr.spillUUID, bucket)
	if ctr.hashmapBuilder.budget == nil {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	fdToken, err := ctr.hashmapBuilder.budget.ReserveSpillFD(1)
	if err != nil {
		return nil, err
	}
	f, err := spillfs.CreateAndRemoveFile(proc.Ctx, name)
	if err != nil {
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	files[bucket] = f
	if ctr.spillBundle == nil {
		ctr.spillBundle = &spillFileBundle{}
	}
	ctr.spillBundle.addFD(f, bucket, fdToken)
	return f, nil
}

// spillBatchBounded partitions one input batch without retaining 32 bucket
// vectors. Hash values are classified with two linear passes (count, then
// scatter after prefix offsets), and a single row-id array describes every
// bucket. One selected batch is reused as each bucket is materialized and
// marshaled before advancing; serialized records are coalesced until the
// bounded buffers or final handoff flush.
func (ctr *container) spillBatchBounded(proc *process.Process, bat *batch.Batch, files []*os.File, executors []colexec.ExpressionExecutor, analyzer process.Analyzer, sourceAlreadyCharged bool) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	if ctr.spillBatchAllocation == nil ||
		ctr.hashmapBuilder.mapAllocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.spillAllocationMP != nil && ctr.spillAllocationMP != proc.Mp() {
		return mpool.ErrAllocationAccountInvalid
	}
	ctr.spillAllocationMP = proc.Mp()
	if !sourceAlreadyCharged {
		externalBytes := bat.Allocated()
		if size := bat.Size(); size > externalBytes {
			externalBytes = size
		}
		analyzer.GetOpStats().SetMaxExtraStat(
			"HashBuildSpillBorrowedSourceBytes",
			int64(externalBytes),
		)
	}

	rows := bat.RowCount()
	if !keycodec.ValidVectors(bat.Vecs, rows) {
		return process.ErrHashBuildBudgetInvalid
	}
	var err error

	if cap(ctr.spillKeyVecs) < len(executors) {
		ctr.spillKeyVecs = make([]*vector.Vector, len(executors))
	}
	ctr.spillHashValues, err = growHashBuildSpillSlice(
		ctr.spillHashValues,
		rows,
		proc.Mp(),
		ctr.hashmapBuilder.mapAllocationAccount,
		HashBuildSpillAllocationSiteHashValues,
		ctr.recoveryCapacityClass,
	)
	if err != nil {
		return err
	}
	ctr.spillBucketRowIds, err = growHashBuildSpillSlice(
		ctr.spillBucketRowIds,
		rows,
		proc.Mp(),
		ctr.hashmapBuilder.mapAllocationAccount,
		HashBuildSpillAllocationSiteRowIDs,
		ctr.recoveryCapacityClass,
	)
	if err != nil {
		return err
	}
	keyVecs := ctr.spillKeyVecs[:len(executors)]
	var selected *batch.Batch
	defer func() {
		if selected != nil {
			selected.Clean(proc.Mp())
		}
		for i := range ctr.spillKeyVecs {
			ctr.spillKeyVecs[i] = nil
		}
	}()
	evalOne := func(i int) error {
		vec, evalErr := executors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if evalErr == nil {
			keyVecs[i] = vec
		}
		return evalErr
	}
	for i := range executors {
		if err = evalOne(i); err != nil {
			break
		}
	}
	if err != nil {
		// Eval may leave child/result allocations cached. Destroy the tree so a
		// pressure retry starts from the exact post-rollback account state.
		ctr.freeSpillExprExecs()
		return err
	}
	if !keycodec.ValidVectors(keyVecs, rows) {
		return process.ErrHashBuildBudgetInvalid
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	ctr.hashmapBuilder.observeNullKeys(keyVecs)

	// Reuse hashValues buffer
	hashes := ctr.spillHashValues[:rows]
	computeXXHash(keyVecs, hashes)
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	counts := ctr.spillBucketCounts[:]
	for i := range counts {
		counts[i] = 0
	}
	for _, hash := range hashes {
		counts[int(hash&(spillNumBuckets-1))]++
	}
	offsets := ctr.spillBucketOffsets[:]
	offsets[0] = 0
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		offsets[bucket+1] = offsets[bucket] + counts[bucket]
	}
	// Reuse counts as write cursors only through a stack-local copy. The
	// original prefix offsets remain stable while each bucket is materialized.
	var writePos [spillNumBuckets]int32
	copy(writePos[:], offsets[:spillNumBuckets])
	for row, hash := range hashes {
		bucket := int(hash & (spillNumBuckets - 1))
		pos := writePos[bucket]
		ctr.spillBucketRowIds[pos] = int32(row)
		writePos[bucket] = pos + 1
	}

	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		start, end := offsets[bucket], offsets[bucket+1]
		if start == end {
			continue
		}
		if selected == nil {
			selected = batch.NewOffHeapWithSize(len(bat.Vecs))
			if err := selected.SetAllocationAccount(
				ctr.spillBatchAllocation,
			); err != nil {
				return err
			}
			for i, vec := range bat.Vecs {
				if vec == nil {
					return process.ErrHashBuildBudgetInvalid
				}
				selected.Vecs[i], err =
					vector.NewOffHeapVecWithTypeAndAllocation(
						*vec.GetType(),
						ctr.spillBatchAllocation,
					)
				if err != nil {
					return err
				}
			}
		}
		cursor := start
		for cursor < end {
			attemptEnd := end
			reclaimedMinimum := false
			for {
				selected.CleanOnlyData()
				sels := ctr.spillBucketRowIds[cursor:attemptEnd]
				n := int(attemptEnd - cursor)
				var spillErr error
				for i, vec := range bat.Vecs {
					if vec == nil {
						spillErr = process.ErrHashBuildBudgetInvalid
						break
					}
					if spillErr = selected.Vecs[i].PreExtend(n, proc.Mp()); spillErr != nil {
						break
					}
					if spillErr = selected.Vecs[i].UnionInt32(vec, sels, proc.Mp()); spillErr != nil {
						break
					}
				}
				if spillErr == nil {
					selected.SetRowCount(n)
					var file *os.File
					file, spillErr = ctr.ensureSpillFile(proc, files, int(bucket))
					if spillErr == nil {
						spillErr = ctr.appendSpillRecord(
							proc,
							file,
							int(bucket),
							selected,
							analyzer,
						)
					}
				}
				selected.CleanOnlyData()
				if spillErr == nil {
					cursor = attemptEnd
					break
				}
				if !IsRetryableMemoryCapacity(spillErr) {
					return spillErr
				}
				if err := checkHashBuildCanceled(proc); err != nil {
					return err
				}
				if n > 1 {
					attemptEnd = cursor + int32((n+1)/2)
					analyzer.GetOpStats().AddExtraStat(
						"HashBuildSpillBatchReductions",
						1,
					)
					continue
				}
				if !reclaimedMinimum {
					before := ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used
					if err := ctr.reclaimOptionalSpillBuffers(
						proc,
						files,
						analyzer,
					); err != nil {
						return err
					}
					reclaimedMinimum = true
					after := ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used
					if after >= before {
						analyzer.GetOpStats().AddExtraStat(
							"HashBuildSpillMinimumUnitErrors",
							1,
						)
						return NewMinimumAllocationPressureError(
							"hashbuild",
							"spill-selected-or-codec",
							ctr.hashmapBuilder.mapAllocationAccount,
						)
					}
					analyzer.GetOpStats().AddExtraStat(
						"HashBuildSpillOptionalReclaims",
						1,
					)
					continue
				}
				analyzer.GetOpStats().AddExtraStat(
					"HashBuildSpillMinimumUnitErrors",
					1,
				)
				return NewMinimumAllocationPressureError(
					"hashbuild",
					"spill-selected-or-codec",
					ctr.hashmapBuilder.mapAllocationAccount,
				)
			}
		}
	}
	return nil
}

// reclaimOptionalSpillBuffers publishes already completed coalesced records,
// then drops codec/coalesce capacity. The current selected record has not been
// published when this is called, so retrying that one record is idempotent.
func (ctr *container) reclaimOptionalSpillBuffers(
	proc *process.Process,
	files []*os.File,
	analyzer process.Analyzer,
) error {
	for bucket, buffer := range ctr.spillAccountedBuckets {
		if buffer == nil {
			continue
		}
		if buffer.Len() > 0 {
			if bucket >= len(files) || files[bucket] == nil {
				return process.ErrHashBuildBudgetInvalid
			}
			if err := ctr.flushPendingSpillBucket(
				proc,
				files[bucket],
				bucket,
				analyzer,
			); err != nil {
				return err
			}
		}
		buffer.Free()
		ctr.spillAccountedBuckets[bucket] = nil
	}
	ctr.spillCoalesceDisabled = true
	if ctr.spillAccountedWrite != nil {
		ctr.spillAccountedWrite.Free()
		ctr.spillAccountedWrite = nil
	}
	return nil
}

func (ctr *container) releaseSpillComputeScratch() {
	if ctr.spillBatchAllocation == nil || ctr.spillAllocationMP == nil {
		return
	}
	if cap(ctr.spillHashValues) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillHashValues)
	}
	if cap(ctr.spillBucketRowIds) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillBucketRowIds)
	}
	ctr.spillHashValues = nil
	ctr.spillBucketRowIds = nil
}

// dropMandatorySpillRecoveryScratch releases only allocations that borrow the
// retained-state recovery floor. Optional coalescing buffers use ordinary
// admission and may keep their already-produced records across the transition.
func (ctr *container) dropMandatorySpillRecoveryScratch() {
	ctr.freeSpillExprExecs()
	ctr.releaseSpillComputeScratch()
	if ctr.spillAccountedWrite != nil {
		ctr.spillAccountedWrite.Free()
		ctr.spillAccountedWrite = nil
	}
}

// spillBatchWithPressure retries only the unpublished prefix of an exact
// spill operation. Hash/expression capacity failures happen before any bucket
// write; selected/codec failures are handled transactionally inside
// spillBatchBounded. Each retry halves the input or reclaims memory, and a
// one-row failure becomes a controlled minimum-unit error.
func (ctr *container) spillBatchWithPressure(
	proc *process.Process,
	bat *batch.Batch,
	files []*os.File,
	executors []colexec.ExpressionExecutor,
	analyzer process.Analyzer,
	sourceAlreadyCharged bool,
) error {
	if ctr.spillBatchAllocation == nil || bat == nil || bat.RowCount() == 0 {
		return ctr.spillBatchBounded(
			proc,
			bat,
			files,
			executors,
			analyzer,
			sourceAlreadyCharged,
		)
	}
	rows := bat.RowCount()
	chunk := rows
	minimumRetried := false
	guard := NewPressureRetryGuard(PressureProgress{
		Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
		InputUnits:       chunk,
		OptionalDisabled: ctr.spillCoalesceDisabled,
	}, 64)
	for start := 0; start < rows; {
		if len(ctr.hashmapBuilder.executors) == 0 {
			var err error
			executors, err = ctr.initSpillExprExecs(
				proc, ctr.spillConditions)
			if err != nil {
				return err
			}
		}
		end := rows
		if chunk < rows-start {
			end = start + chunk
		}
		current := bat
		if start != 0 || end != rows {
			var err error
			current, err = bat.WindowWithAllocation(
				start, end, proc.Mp(), ctr.spillBatchAllocation,
			)
			if err != nil {
				return err
			}
		}
		err := ctr.spillBatchBounded(
			proc,
			current,
			files,
			executors,
			analyzer,
			sourceAlreadyCharged,
		)
		if current != bat {
			current.Clean(proc.Mp())
		}
		if err == nil {
			start = end
			minimumRetried = false
			nextUnits := chunk
			if remaining := rows - start; remaining < nextUnits {
				nextUnits = remaining
			}
			guard = NewPressureRetryGuard(PressureProgress{
				Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
				InputUnits:       nextUnits,
				OptionalDisabled: ctr.spillCoalesceDisabled,
			}, 64)
			continue
		}
		if !IsRetryableMemoryCapacity(err) {
			return err
		}
		if cancelErr := checkHashBuildCanceled(proc); cancelErr != nil {
			return cancelErr
		}
		ctr.releaseSpillComputeScratch()
		attempted := end - start
		if attempted <= 1 {
			if !minimumRetried {
				if reclaimErr := ctr.reclaimOptionalSpillBuffers(
					proc,
					files,
					analyzer,
				); reclaimErr != nil {
					return reclaimErr
				}
				next := PressureProgress{
					Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
					InputUnits:       attempted,
					OptionalDisabled: ctr.spillCoalesceDisabled,
				}
				if guard.Advance(next) != nil {
					return NewMinimumAllocationPressureError(
						"hashbuild",
						"spill-hash-or-expression",
						ctr.hashmapBuilder.mapAllocationAccount,
					)
				}
				minimumRetried = true
				analyzer.GetOpStats().AddExtraStat(
					"HashBuildSpillMinimumRetries",
					1,
				)
				continue
			}
			return NewMinimumAllocationPressureError(
				"hashbuild",
				"spill-hash-or-expression",
				ctr.hashmapBuilder.mapAllocationAccount,
			)
		}
		chunk = (attempted + 1) / 2
		if err := guard.Advance(PressureProgress{
			Used:       ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
			InputUnits: chunk,
		}); err != nil {
			return err
		}
		analyzer.GetOpStats().AddExtraStat("HashBuildSpillInputReductions", 1)
	}
	return nil
}

// appendSpillRecord appends one framed record to the bucket's bounded write
// buffer. Full buffers are written before accepting the next record. A record
// larger than the coalescing target is written directly, so no unbounded
// temporary copy can be retained.
func (ctr *container) appendSpillRecord(
	proc *process.Process,
	file *os.File,
	bucket int,
	bat *batch.Batch,
	analyzer process.Analyzer,
) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	if ctr.spillAllocationMP != proc.Mp() ||
		ctr.hashmapBuilder.mapAllocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.spillAccountedWrite == nil {
		var err error
		ctr.spillAccountedWrite, err = mpool.NewAccountedBufferWithCapacityClass(
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildAllocationOwner,
			HashBuildSpillAllocationSiteMarshalBuffer,
			ctr.recoveryCapacityClass,
		)
		if err != nil {
			return err
		}
	}
	cnt, err := marshalSpillRecordAccounted(bat, ctr.spillAccountedWrite)
	if err != nil {
		return err
	}
	payload := ctr.spillAccountedWrite.Bytes()
	if ctr.spillCoalesceDisabled {
		return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
	}
	buffer := ctr.spillAccountedBuckets[bucket]
	if buffer != nil && buffer.Len() > 0 &&
		buffer.Len()+len(payload) > spillWriteCoalesceSize {
		if err := ctr.flushPendingSpillBucket(
			proc,
			file,
			bucket,
			analyzer,
		); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
	}
	if buffer == nil {
		buffer, err = mpool.NewAccountedBuffer(
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildAllocationOwner,
			HashBuildSpillAllocationSiteCoalesceBuffer,
		)
		if err != nil {
			return err
		}
		ctr.spillAccountedBuckets[bucket] = buffer
	}
	if buffer.Len() == 0 && buffer.Cap() < spillWriteCoalesceSize {
		if err := buffer.EnsureCapacity(spillWriteCoalesceSize); err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				return ctr.writeSpillPayload(
					proc,
					file,
					payload,
					cnt,
					analyzer,
				)
			}
			return err
		}
	}
	if _, err := buffer.Write(payload); err != nil {
		return err
	}
	ctr.spillBucketWriteRows[bucket] += cnt
	if buffer.Len() >= spillWriteCoalesceSize {
		return ctr.flushPendingSpillBucket(
			proc,
			file,
			bucket,
			analyzer,
		)
	}
	return nil
}

func (ctr *container) flushPendingSpillBucket(
	proc *process.Process,
	file *os.File,
	bucket int,
	analyzer process.Analyzer,
) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	rows := ctr.spillBucketWriteRows[bucket]
	buffer := ctr.spillAccountedBuckets[bucket]
	if buffer == nil || buffer.Len() == 0 {
		return nil
	}
	err := ctr.writeSpillPayload(proc, file, buffer.Bytes(), rows, analyzer)
	// Clear even on a failed/partial write. A caller's enclosing failure path
	// owns cleanup, and retrying the same bytes could duplicate records.
	buffer.Reset()
	ctr.spillBucketWriteRows[bucket] = 0
	return err
}

// flushSpillBuffers writes all pending bucket records before files are rewound
// or handed to JoinMap. Cancellation is checked between physical writes. After
// the first error, the remaining buffers are discarded rather than written, so
// every buffer still reaches a terminal state without doing doomed I/O.
func (ctr *container) flushSpillBuffers(proc *process.Process, files []*os.File, analyzer process.Analyzer) error {
	var firstErr error
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		pending := 0
		if ctr.spillAccountedBuckets[bucket] != nil {
			pending = ctr.spillAccountedBuckets[bucket].Len()
		}
		if pending == 0 {
			continue
		}
		if firstErr != nil {
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		if err := checkHashBuildCanceled(proc); err != nil {
			firstErr = err
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		var file *os.File
		if bucket < len(files) {
			file = files[bucket]
		}
		if file == nil {
			firstErr = process.ErrHashBuildBudgetInvalid
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		if err := ctr.flushPendingSpillBucket(proc, file, bucket, analyzer); err != nil {
			firstErr = err
		}
	}
	return firstErr
}

// initSpillExprExecs reuses the HashmapBuilder key executors. Spill and normal
// build are mutually exclusive after the transition, so a second executor
// tree would only duplicate retained capacity and its lifecycle.
func (ctr *container) initSpillExprExecs(proc *process.Process, conditions []*plan.Expr) ([]colexec.ExpressionExecutor, error) {
	for _, condition := range conditions {
		if condition == nil {
			return nil, &process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid, Message: "nil shuffle spill key"}
		}
	}
	ctr.spillConditions = conditions
	if len(ctr.hashmapBuilder.executors) != len(conditions) {
		ctr.hashmapBuilder.FreeExecutors()
		execs, err := newExpressionExecutorsWithCapacityClass(
			proc,
			conditions,
			ctr.hashmapBuilder.mapAllocationAccount,
			ctr.recoveryCapacityClass,
		)
		if err != nil {
			return nil, err
		}
		ctr.hashmapBuilder.executors = execs
		ctr.hashmapBuilder.keyExprs = conditions
	}
	ctr.spillExprExecs = ctr.hashmapBuilder.executors
	return ctr.spillExprExecs, nil
}

// freeSpillExprExecs clears the spill alias and releases its builder-owned tree.
func (ctr *container) freeSpillExprExecs() {
	ctr.spillExprExecs = nil
	ctr.hashmapBuilder.FreeExecutors()
}

func (ctr *container) memUsed() int64 {
	sz := ctr.hashmapBuilder.GetSize() + ctr.hashmapBuilder.Batches.MemSize
	batches := ctr.hashmapBuilder.Batches.Buf
	// MemSize tracks completed fixed-size batches. Include the one permitted
	// partial tail so a threshold decision cannot lag by almost one batch. If
	// MemSize is zero (including directly assembled test state), sum all batches.
	if ctr.hashmapBuilder.Batches.MemSize == 0 {
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			sz += int64(bat.Size())
		}
	} else if len(batches) > 0 {
		tail := batches[len(batches)-1]
		if tail != nil && tail.RowCount() != colexec.DefaultBatchSize {
			sz += int64(tail.Size())
		}
	}
	return sz
}

func (hashBuild *HashBuild) shouldSpillBatches() bool {
	if !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	return colexec.ShouldSpill(ctr.memUsed(), int64(ctr.hashmapBuilder.InputBatchRowCount), ctr.spillThreshold)
}

// shouldSpillBeforeRetain applies the configured threshold to the retained
// state plus the current upstream batch. InputBatchRowCount already includes
// that batch when this is called. Moving the existing decision before the copy
// prevents the threshold-crossing reservation from consuming the scratch
// headroom needed to start spill; it does not size or reserve spill scratch.
func (hashBuild *HashBuild) shouldSpillBeforeRetain(inputBatchSize int64) bool {
	if !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	predicted := ctr.memUsed()
	if inputBatchSize < 0 || predicted > math.MaxInt64-inputBatchSize {
		predicted = math.MaxInt64
	} else {
		predicted += inputBatchSize
	}
	return colexec.ShouldSpill(
		predicted,
		int64(ctr.hashmapBuilder.InputBatchRowCount),
		ctr.spillThreshold,
	)
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// Each column is processed in a tight loop over all rows, avoiding
// per-row buffer concatenation and giving sequential vector access.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return
	}
	keycodec.ComputeXXHash(keyVecs, hashValues, 0)
}
