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

	// Write batch data directly to spillWriteBuf
	batchStartPos := ctr.spillWriteBuf.Len()
	bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false)
	batchSize := int64(ctr.spillWriteBuf.Len() - batchStartPos)

	// Write batchSize at reserved position
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(ctr.spillWriteBuf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)

	magic := uint64(spillMagic)
	ctr.spillWriteBuf.Write(types.EncodeUint64(&magic))

	written, err := file.Write(ctr.spillWriteBuf.Bytes())
	if err != nil {
		return 0, err
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
	f, err := spillfs.CreateAndRemoveFile(proc.Ctx, name)
	if err != nil {
		return nil, err
	}
	files[bucket] = f
	return f, nil
}

func (ctr *container) appendBuildBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, executors []colexec.ExpressionExecutor, analyzer process.Analyzer) error {
	if bat.RowCount() == 0 {
		return nil
	}

	// Evaluate hash keys using pre-initialized executors (reuse cached slice)
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

	// Reuse hashValues buffer
	rowCount := bat.RowCount()
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]

	if err := computeXXHash(keyVecs, hashValues); err != nil {
		return err
	}

	// Reuse bucketRowIds buffer
	if cap(ctr.spillBucketRowIds) < spillNumBuckets {
		ctr.spillBucketRowIds = make([][]int32, spillNumBuckets)
	}
	bucketRowIds := ctr.spillBucketRowIds[:spillNumBuckets]

	// Single pass: reset and distribute row indices into buckets.
	for i := 0; i < spillNumBuckets; i++ {
		bucketRowIds[i] = bucketRowIds[i][:0]
	}
	for row := 0; row < rowCount; row++ {
		b := hashValues[row] & (spillNumBuckets - 1)
		bucketRowIds[b] = append(bucketRowIds[b], int32(row))
	}

	// Collect non-empty buckets (reuse cached slice).
	ctr.spillNonEmptyBuckets = ctr.spillNonEmptyBuckets[:0]
	for i := 0; i < spillNumBuckets; i++ {
		if len(bucketRowIds[i]) > 0 {
			ctr.spillNonEmptyBuckets = append(ctr.spillNonEmptyBuckets, i)
		}
	}

	// Add rows to buffers and flush when needed
	for _, bucketId := range ctr.spillNonEmptyBuckets {
		sels := bucketRowIds[bucketId]

		buf := buffers[bucketId]
		if buf == nil {
			buf = batch.NewOffHeapWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				typ := *vec.GetType()
				buf.Vecs[i] = vector.NewOffHeapVecWithType(typ)
				buf.Vecs[i].PreExtend(spillBufferSize, proc.Mp())
			}
			buffers[bucketId] = buf
		}

		// Append rows to buffer
		for i, vec := range bat.Vecs {
			if err := buf.Vecs[i].UnionInt32(vec, sels, proc.Mp()); err != nil {
				return err
			}
		}
		buf.SetRowCount(buf.RowCount() + len(sels))

		// Flush if buffer is full
		if buf.RowCount() >= spillBufferSize {
			file, err := ctr.ensureSpillFile(proc, files, bucketId)
			if err != nil {
				return err
			}
			if _, err := ctr.flushBucketBuffer(proc, buf, file, analyzer); err != nil {
				return err
			}
			buf.CleanOnlyData()
		}
	}

	return nil
}

// initSpillExprExecs initializes or validates spill expression executors.
// Returns the executors slice ready for use. Called once when entering spill mode.
func (ctr *container) initSpillExprExecs(proc *process.Process, conditions []*plan.Expr) ([]colexec.ExpressionExecutor, error) {
	if len(ctr.spillExprExecs) != len(conditions) {
		// Clean up old executors if count changed
		ctr.freeSpillExprExecs()
		ctr.spillExprExecs = make([]colexec.ExpressionExecutor, len(conditions))
		for i, expr := range conditions {
			var err error
			if ctr.spillExprExecs[i], err = colexec.NewExpressionExecutor(proc, expr); err != nil {
				// Clean up what we already created
				for j := 0; j < i; j++ {
					ctr.spillExprExecs[j].Free()
				}
				ctr.spillExprExecs = nil
				return nil, err
			}
		}
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

// acquireSpillBuffers returns a slice of spillNumBuckets buffers, reusing from pool if available.
// All returned buffers have CleanOnlyData() called to reset them for fresh use.
func (ctr *container) acquireSpillBuffers(proc *process.Process) []*batch.Batch {
	if len(ctr.spillBuffers) < spillNumBuckets {
		ctr.spillBuffers = append(ctr.spillBuffers, make([]*batch.Batch, spillNumBuckets-len(ctr.spillBuffers))...)
	}
	bufs := ctr.spillBuffers[:spillNumBuckets]
	for i := range bufs {
		if bufs[i] != nil {
			bufs[i].CleanOnlyData()
		}
	}
	return bufs
}

// cleanSpillBufferPool cleans all pooled buffers.
func (ctr *container) cleanSpillBufferPool(proc *process.Process) {
	for _, buf := range ctr.spillBuffers {
		if buf != nil {
			buf.Clean(proc.Mp())
		}
	}
	ctr.spillBuffers = nil
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

func (ctr *container) rowCnt() int64 {
	sz := 0
	for _, bat := range ctr.hashmapBuilder.Batches.Buf {
		sz += bat.RowCount()
	}
	return int64(sz)
}

func (hashBuild *HashBuild) shouldSpillBatches() bool {
	if !hashBuild.CanSpill || !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	if ctr.spillThreshold <= 0 {
		return false
	}
	if ctr.spillThreshold <= 100000 {
		return ctr.rowCnt() >= ctr.spillThreshold
	} else {
		return ctr.memUsed() > ctr.spillThreshold
	}
}

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return h ^ (val + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2))
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// Each column is processed in a tight loop over all rows, avoiding
// per-row buffer concatenation and giving sequential vector access.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) error {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return nil
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = 0
	}

	for _, vec := range keyVecs {
		if vec.IsConst() {
			colHash := xxhash.Sum64(vec.GetRawBytesAt(0))
			for i := 0; i < rowCount; i++ {
				hashValues[i] = hashCombine(hashValues[i], colHash)
			}
		} else {
			n := rowCount
			if vec.Length() < n {
				n = vec.Length()
			}
			for i := 0; i < n; i++ {
				hashValues[i] = hashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
			}
		}
	}

	return nil
}
