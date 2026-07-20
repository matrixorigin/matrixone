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
	"bytes"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

// spillBudgetBytes is intentionally conservative.  The input batch remains
// owned by the upstream operator while HashBuild evaluates and partitions it,
// so its footprint is included in the temporary peak alongside hash values,
// row selections, the selected bucket, and marshal scratch.
func spillBudgetBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	rows := uint64(bat.RowCount())
	source := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > source {
		source = size
	}
	return spillBudgetFor(rows, source)
}

func spillBudgetFor(rows, source uint64) (uint64, error) {
	if rows > math.MaxUint64/12 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	// hash values + one reusable selection array.  The selected batch and its
	// varlen area may overlap the source capacity, hence a full additional
	// source allocation is charged rather than just logical Size().
	total := rows * 12
	if source > math.MaxUint64-total {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	total += source
	// A selected bucket can contain every input row.  MarshalBinary emits
	// vector metadata and varlen area; 2x payload plus fixed headers is a safe
	// upper bound before bytes.Buffer grows.
	if source > (math.MaxUint64-64*1024)/2 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	marshal := source*2 + 64*1024
	if total > math.MaxUint64-marshal {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	total += marshal
	if total > uint64(^uint(0)>>1) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return total, nil
}

// spillEmergencyBudgetBytes covers both the upstream ingress batch and the
// largest physical batch CopyIntoBatches can form by coalescing small inputs.
func spillEmergencyBudgetBytes(bat *batch.Batch) (uint64, error) {
	need, err := spillBudgetBytes(bat)
	if err != nil || bat == nil || bat.RowCount() <= 0 || bat.RowCount() >= colexec.DefaultBatchSize {
		return need, err
	}
	rows := uint64(bat.RowCount())
	source := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > source {
		source = size
	}
	if source > math.MaxUint64/uint64(colexec.DefaultBatchSize) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	scaled := (source*uint64(colexec.DefaultBatchSize) + rows - 1) / rows
	metadata, ok := retainedMetadataAllowance(bat)
	if !ok || metadata > math.MaxUint64/uint64(colexec.DefaultBatchSize) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	scaledMetadata := (metadata*uint64(colexec.DefaultBatchSize) + rows - 1) / rows
	if scaled > math.MaxUint64-scaledMetadata {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	scaled += scaledMetadata
	physicalNeed, err := spillBudgetFor(uint64(colexec.DefaultBatchSize), scaled)
	if err != nil {
		return 0, err
	}
	if physicalNeed > need {
		need = physicalNeed
	}
	return need, nil
}

func (ctr *container) reserveSpillScratch(bat *batch.Batch) (*process.HashBuildReservation, error) {
	if ctr.hashmapBuilder.budget == nil {
		return nil, nil
	}
	bytes, err := spillBudgetBytes(bat)
	if err != nil {
		return nil, err
	}
	if bytes == 0 {
		return nil, nil
	}
	return ctr.hashmapBuilder.budget.Reserve(bytes)
}

func (ctr *container) ensureSpillScratchReservation(bat *batch.Batch) error {
	if ctr.hashmapBuilder.budget == nil {
		return nil
	}
	need, err := spillEmergencyBudgetBytes(bat)
	if err != nil || need == 0 {
		return err
	}
	if ctr.spillScratchReservation == nil {
		ctr.spillScratchReservation, err = ctr.hashmapBuilder.budget.Reserve(need)
		if err == nil {
			ctr.spillScratchEmergency = true
			ctr.spillScratchBase = need
		}
		return err
	}
	if ctr.spillScratchBase >= need {
		ctr.spillScratchEmergency = true
		return nil
	}
	if err := ctr.spillScratchReservation.Grow(need - ctr.spillScratchBase); err != nil {
		return err
	}
	ctr.spillScratchBase = need
	ctr.spillScratchEmergency = true
	return nil
}

func (ctr *container) releaseSpillScratchReservation() {
	if ctr.spillScratchReservation != nil {
		ctr.spillScratchReservation.Release()
		ctr.spillScratchReservation = nil
	}
	ctr.spillScratchEmergency = false
	ctr.spillScratchBase = 0
}

func (ctr *container) dropSpillScratchBuffers() {
	for bucket := range ctr.spillBucketWriteBufs {
		ctr.spillBucketWriteBufs[bucket] = bytes.Buffer{}
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
	ctr.spillSelection = nil
	ctr.spillKeyVecs = nil
	ctr.spillWriteBuf = bytes.Buffer{}
}

func marshalSpillRecord(bat *batch.Batch, buf *bytes.Buffer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	buf.Reset()
	buf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize (filled in after marshalling)
	batchSizePos := buf.Len()
	var zero int64
	buf.Write(types.EncodeInt64(&zero))

	// Write batch data directly to spillWriteBuf.  The bounded partition path
	// reserves this buffer's conservative upper bound before entering here.
	batchStartPos := buf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(buf, false); err != nil {
		return 0, err
	}
	batchSize := int64(buf.Len() - batchStartPos)

	// Write batchSize at reserved position
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(buf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)

	magic := uint64(spillMagic)
	buf.Write(types.EncodeUint64(&magic))
	return cnt, nil
}

func (ctr *container) writeSpillPayload(file *os.File, payload []byte, rows int64, analyzer process.Analyzer) error {
	if file == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
	}

	var err error
	if ctr.hashmapBuilder.budget != nil {
		if ctr.spillBundle == nil {
			return process.ErrHashBuildBudgetInvalid
		}
		_, _, err = ctr.spillBundle.growDisk(file, ctr.hashmapBuilder.budget, uint64(len(payload)))
		if err != nil {
			return err
		}
	}
	written, err := file.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	if ctr.hashmapBuilder.budget != nil {
		// The exact payload length was admitted. Record logical ownership only
		// after the full write; partial writes retain the conservative charge
		// until the enclosing bundle closes the file.
		ctr.spillBundle.recordDiskWrite(file, rows, uint64(written))
	}
	if analyzer != nil {
		analyzer.Spill(int64(written))
		analyzer.SpillRows(rows)
	}

	return nil
}

func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, file *os.File, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}
	cnt, err := marshalSpillRecord(bat, &ctr.spillWriteBuf)
	if err != nil {
		return 0, err
	}
	if err := ctr.writeSpillPayload(file, ctr.spillWriteBuf.Bytes(), cnt, analyzer); err != nil {
		return 0, err
	}
	return cnt, nil
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
	var fdToken *process.HashBuildSpillFDReservation
	if ctr.hashmapBuilder.budget != nil {
		fdToken, err = ctr.hashmapBuilder.budget.ReserveSpillFD(1)
		if err != nil {
			return nil, err
		}
	}
	f, err := spillfs.CreateAndRemoveFile(proc.Ctx, name)
	if err != nil {
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	files[bucket] = f
	if fdToken != nil {
		if ctr.spillBundle == nil {
			ctr.spillBundle = &spillFileBundle{}
		}
		ctr.spillBundle.addFD(f, bucket, fdToken)
	}
	return f, nil
}

// spillBatchBounded partitions one input batch without retaining 32 bucket
// vectors. Hash values are classified with two linear passes (count, then
// scatter after prefix offsets), and a single row-id array describes every
// bucket. One selected batch is reused as each bucket is materialized and
// marshaled before advancing; serialized records are coalesced until the
// bounded buffers or final handoff flush.
func (ctr *container) spillBatchBounded(proc *process.Process, bat *batch.Batch, files []*os.File, executors []colexec.ExpressionExecutor, analyzer process.Analyzer) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	need, err := spillBudgetBytes(bat)
	if err != nil {
		return err
	}
	// Scratch belongs to the execution generation, not to one batch. Build
	// normally pre-admits the emergency lease before calling us; direct callers
	// (including recovery/error paths and unit tests) establish the same lease
	// here. Keep it live while capacities are retained and release it from
	// Reset/Free/build cleanup exactly once.
	if ctr.hashmapBuilder.budget != nil {
		if ctr.spillScratchReservation == nil {
			ctr.spillScratchReservation, err = ctr.hashmapBuilder.budget.Reserve(need)
			if err != nil {
				return err
			}
			ctr.spillScratchBase = need
		} else if need > ctr.spillScratchBase {
			if ctr.spillScratchEmergency {
				return process.ErrHashBuildBudgetAdmission
			}
			if err := ctr.spillScratchReservation.Grow(need - ctr.spillScratchBase); err != nil {
				return err
			}
			ctr.spillScratchBase = need
		}
	}

	if cap(ctr.spillKeyVecs) < len(executors) {
		ctr.spillKeyVecs = make([]*vector.Vector, len(executors))
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
	for i, exec := range executors {
		vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		keyVecs[i] = vec
	}
	ctr.hashmapBuilder.observeNullKeys(keyVecs)

	// Reuse hashValues buffer
	rows := bat.RowCount()
	if cap(ctr.spillHashValues) < rows {
		ctr.spillHashValues = make([]uint64, rows)
	}
	hashes := ctr.spillHashValues[:rows]
	computeXXHash(keyVecs, hashes)
	if cap(ctr.spillBucketRowIds) < rows {
		ctr.spillBucketRowIds = make([]int32, rows)
	}
	// Keep the legacy spillSelection field as an alias for callers/tests that
	// inspect it. It intentionally points at the same backing array: no second
	// row-id allocation is made.
	ctr.spillSelection = ctr.spillBucketRowIds
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
		start, end := offsets[bucket], offsets[bucket+1]
		if start == end {
			continue
		}
		if selected == nil {
			selected = batch.NewOffHeapWithSize(len(bat.Vecs))
			selected.Attrs = bat.Attrs
			for i, vec := range bat.Vecs {
				if vec == nil {
					return process.ErrHashBuildBudgetInvalid
				}
				selected.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
			}
		}
		selected.CleanOnlyData()
		sels := ctr.spillBucketRowIds[start:end]
		n := int(end - start)
		var spillErr error
		for i, vec := range bat.Vecs {
			if vec == nil {
				spillErr = process.ErrHashBuildBudgetInvalid
				break
			}
			if spillErr = selected.Vecs[i].PreExtend(n, proc.Mp()); spillErr != nil {
				break
			}
			if spillErr = selected.Vecs[i].UnionInt32(vec, sels[:n], proc.Mp()); spillErr != nil {
				break
			}
		}
		if spillErr == nil {
			selected.SetRowCount(n)
			var file *os.File
			file, spillErr = ctr.ensureSpillFile(proc, files, int(bucket))
			if spillErr == nil {
				spillErr = ctr.appendSpillRecord(file, int(bucket), selected, analyzer)
			}
		}
		selected.CleanOnlyData()
		if spillErr != nil {
			return spillErr
		}
	}
	return nil
}

// appendSpillRecord appends one framed record to the bucket's bounded write
// buffer. Full buffers are written before accepting the next record. A record
// larger than the coalescing target is written directly, so no unbounded
// temporary copy can be retained.
func (ctr *container) appendSpillRecord(file *os.File, bucket int, bat *batch.Batch, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	cnt, err := marshalSpillRecord(bat, &ctr.spillWriteBuf)
	if err != nil {
		return err
	}
	payload := ctr.spillWriteBuf.Bytes()
	buf := &ctr.spillBucketWriteBufs[bucket]
	if buf.Len() > 0 && buf.Len()+len(payload) > spillWriteCoalesceSize {
		if err := ctr.flushPendingSpillBucket(file, bucket, analyzer); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return ctr.writeSpillPayload(file, payload, cnt, analyzer)
	}
	if buf.Len() == 0 {
		if !ctr.ensureSpillCoalesceCapacity(buf) {
			return ctr.writeSpillPayload(file, payload, cnt, analyzer)
		}
		buf.Grow(spillWriteCoalesceSize)
	}
	_, _ = buf.Write(payload)
	ctr.spillBucketWriteRows[bucket] += cnt
	if buf.Len() >= spillWriteCoalesceSize {
		return ctr.flushPendingSpillBucket(file, bucket, analyzer)
	}
	return nil
}

func (ctr *container) ensureSpillCoalesceCapacity(buf *bytes.Buffer) bool {
	if buf == nil || buf.Cap() >= spillWriteCoalesceSize {
		return true
	}
	if ctr.hashmapBuilder.budget == nil || ctr.spillScratchReservation == nil {
		return ctr.hashmapBuilder.budget == nil
	}
	additional := uint64(spillWriteCoalesceSize-buf.Cap()) * 2
	if err := ctr.spillScratchReservation.Grow(additional); err != nil {
		return false
	}
	return true
}

func (ctr *container) flushPendingSpillBucket(file *os.File, bucket int, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	buf := &ctr.spillBucketWriteBufs[bucket]
	if buf.Len() == 0 {
		return nil
	}
	rows := ctr.spillBucketWriteRows[bucket]
	payload := buf.Bytes()
	err := ctr.writeSpillPayload(file, payload, rows, analyzer)
	// Clear even on a failed/partial write. A caller's enclosing failure path
	// owns cleanup, and retrying the same bytes could duplicate records.
	buf.Reset()
	ctr.spillBucketWriteRows[bucket] = 0
	return err
}

// flushSpillBuffers writes all pending bucket records before files are rewound
// or handed to JoinMap. Continue after an individual failure so every buffer
// reaches a terminal state; the first error is returned to the caller.
func (ctr *container) flushSpillBuffers(files []*os.File, analyzer process.Analyzer) error {
	var firstErr error
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		if ctr.spillBucketWriteBufs[bucket].Len() == 0 {
			continue
		}
		var file *os.File
		if bucket < len(files) {
			file = files[bucket]
		}
		if file == nil {
			if firstErr == nil {
				firstErr = process.ErrHashBuildBudgetInvalid
			}
			ctr.spillBucketWriteBufs[bucket].Reset()
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		if err := ctr.flushPendingSpillBucket(file, bucket, analyzer); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (ctr *container) discardSpillBuffers() {
	for bucket := range ctr.spillBucketWriteBufs {
		ctr.spillBucketWriteBufs[bucket].Reset()
		ctr.spillBucketWriteRows[bucket] = 0
	}
}

func (ctr *container) appendBuildBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, executors []colexec.ExpressionExecutor, analyzer process.Analyzer) error {
	// buffers is retained in the signature for source compatibility with older
	// unit tests and callers.  The implementation intentionally ignores it:
	// every non-empty bucket is selected and flushed before the next bucket is
	// materialized, so no persistent fanout-sized vector set can grow.
	_ = buffers
	return ctr.spillBatchBounded(proc, bat, files, executors, analyzer)
}

// initSpillExprExecs initializes or validates spill expression executors.
// Returns the executors slice ready for use. Called once when entering spill mode.
func (ctr *container) initSpillExprExecs(proc *process.Process, conditions []*plan.Expr) ([]colexec.ExpressionExecutor, error) {
	for _, condition := range conditions {
		if condition == nil {
			return nil, &process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid, Message: "nil shuffle spill key"}
		}
		if _, ok := condition.Expr.(*plan.Expr_Col); !ok {
			return nil, &process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid, Message: "shuffle spill requires simple column keys"}
		}
	}
	if len(ctr.spillExprExecs) != len(conditions) {
		execs, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, conditions)
		if err != nil {
			return nil, err
		}
		ctr.freeSpillExprExecs()
		ctr.spillExprExecs = execs
	}
	return ctr.spillExprExecs, nil
}

// freeSpillExprExecs frees all cached spill expression executors.
func (ctr *container) freeSpillExprExecs() {
	for _, exec := range ctr.spillExprExecs {
		if exec != nil {
			exec.Free()
		}
	}
	ctr.spillExprExecs = nil
}

func (ctr *container) memUsed() int64 {
	sz := ctr.hashmapBuilder.GetSize() + ctr.hashmapBuilder.Batches.MemSize
	// If MemSize is 0 but Buf is non-empty (e.g. set directly in tests), fall back to summing.
	if sz == 0 {
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			sz += int64(bat.Size())
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

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return h ^ (val + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2))
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// Each column is processed in a tight loop over all rows, avoiding
// per-row buffer concatenation and giving sequential vector access.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = 0
	}

	for _, vec := range keyVecs {
		if vec.IsConst() {
			colHash := uint64(0)
			if !vec.IsConstNull() {
				colHash = xxhash.Sum64(vec.GetRawBytesAt(0))
			}
			for i := 0; i < rowCount; i++ {
				hashValues[i] = hashCombine(hashValues[i], colHash)
			}
		} else {
			n := rowCount
			if vec.Length() < n {
				n = vec.Length()
			}
			if vec.GetNulls().Any() {
				nulls := vec.GetNulls()
				for i := 0; i < n; i++ {
					if nulls.Contains(uint64(i)) {
						hashValues[i] = hashCombine(hashValues[i], 0)
					} else {
						hashValues[i] = hashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
					}
				}
			} else {
				for i := 0; i < n; i++ {
					hashValues[i] = hashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
				}
			}
		}
	}

}
