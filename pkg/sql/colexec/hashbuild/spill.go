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
		return err
	}
	current := ctr.spillScratchReservation.Size()
	if current >= need {
		return nil
	}
	return ctr.spillScratchReservation.Grow(need - current)
}

func (ctr *container) releaseSpillScratchReservation() {
	if ctr.spillScratchReservation != nil {
		ctr.spillScratchReservation.Release()
		ctr.spillScratchReservation = nil
	}
}

func (ctr *container) dropSpillScratchBuffers() {
	ctr.spillHashValues = nil
	ctr.spillBucketRowIds = nil
	ctr.spillSelection = nil
	ctr.spillKeyVecs = nil
	ctr.spillWriteBuf = bytes.Buffer{}
}

func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, file *os.File, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize (filled in after marshalling)
	batchSizePos := ctr.spillWriteBuf.Len()
	var zero int64
	ctr.spillWriteBuf.Write(types.EncodeInt64(&zero))

	// Write batch data directly to spillWriteBuf.  The bounded partition path
	// reserves this buffer's conservative upper bound before entering here.
	batchStartPos := ctr.spillWriteBuf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false); err != nil {
		return 0, err
	}
	batchSize := int64(ctr.spillWriteBuf.Len() - batchStartPos)

	// Write batchSize at reserved position
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(ctr.spillWriteBuf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)

	magic := uint64(spillMagic)
	ctr.spillWriteBuf.Write(types.EncodeUint64(&magic))

	payload := ctr.spillWriteBuf.Bytes()
	var err error
	if ctr.hashmapBuilder.budget != nil {
		if ctr.spillBundle == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		_, _, err = ctr.spillBundle.growDisk(file, ctr.hashmapBuilder.budget, uint64(len(payload)))
		if err != nil {
			return 0, err
		}
	}
	written, err := file.Write(payload)
	if err != nil {
		return 0, err
	}
	if written != len(payload) {
		return 0, io.ErrShortWrite
	}
	if ctr.hashmapBuilder.budget != nil {
		// The exact payload length was admitted. Record logical ownership only
		// after the full write; partial writes retain the conservative charge
		// until the enclosing bundle closes the file.
		ctr.spillBundle.recordDiskWrite(file, cnt, uint64(written))
	}
	analyzer.Spill(int64(written))
	analyzer.SpillRows(cnt)

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
// vectors.  Hash values and row selections are admitted once; one bucket is
// materialized, marshaled, and flushed before its memory is cleaned.
func (ctr *container) spillBatchBounded(proc *process.Process, bat *batch.Batch, files []*os.File, executors []colexec.ExpressionExecutor, analyzer process.Analyzer) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	need, err := spillBudgetBytes(bat)
	if err != nil {
		return err
	}
	reservation := ctr.spillScratchReservation
	borrowed := reservation != nil
	if borrowed {
		if reservation.Size() < need {
			return process.ErrHashBuildBudgetAdmission
		}
	} else {
		reservation, err = ctr.reserveSpillScratch(bat)
		if err != nil {
			return err
		}
	}
	defer func() {
		// Go scratch capacities are temporary.  Drop the backing arrays before
		// releasing the admission token; retaining them would make a later,
		// smaller batch reuse uncharged capacity.
		ctr.dropSpillScratchBuffers()
		if reservation != nil && !borrowed {
			// All temporary scratch is gone at this point; retaining a zero-sized
			// token until Release keeps exactly-once accounting on error paths.
			_, _ = reservation.ReconcileDown(0)
			reservation.Release()
		}
	}()

	if cap(ctr.spillKeyVecs) < len(executors) {
		ctr.spillKeyVecs = make([]*vector.Vector, len(executors))
	}
	keyVecs := ctr.spillKeyVecs[:len(executors)]
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
	if cap(ctr.spillSelection) < rows {
		ctr.spillSelection = make([]int32, rows)
	}
	sels := ctr.spillSelection[:rows]
	for bucket := uint64(0); bucket < spillNumBuckets; bucket++ {
		n := 0
		for row, hash := range hashes {
			if hash&(spillNumBuckets-1) == bucket {
				sels[n] = int32(row)
				n++
			}
		}
		if n == 0 {
			continue
		}

		selected := batch.NewOffHeapWithSize(len(bat.Vecs))
		selected.Attrs = bat.Attrs
		var spillErr error
		for i, vec := range bat.Vecs {
			if vec == nil {
				spillErr = process.ErrHashBuildBudgetInvalid
				break
			}
			selected.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
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
				_, spillErr = ctr.flushBucketBuffer(proc, selected, file, analyzer)
			}
		}
		selected.Clean(proc.Mp())
		if spillErr != nil {
			return spillErr
		}
	}
	return nil
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
