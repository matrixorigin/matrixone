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

package hashjoin

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMagic      = 0x12345678DEADBEEF
	spillMaxPass    = 3
	spillBufferSize = 8192
)

type spillBucketReader struct {
	file   *os.File
	reader *bufio.Reader
	buf    []byte
}

func newSpillBucketReader(proc *process.Process, bucketName string) (*spillBucketReader, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}

	file, err := spillfs.OpenFile(proc.Ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return &spillBucketReader{
		file:   file,
		reader: bufio.NewReaderSize(file, spillBufferSize),
		buf:    make([]byte, 8),
	}, nil
}

func (r *spillBucketReader) readBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
	// Read count
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	cnt := types.DecodeInt64(r.buf)

	// Read batch size
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		return nil, err
	}
	batchSize := types.DecodeInt64(r.buf)

	// Clean previous data
	reuseBat.CleanOnlyData()

	// Create limited reader for exact batch size
	limitedReader := io.LimitReader(r.reader, batchSize)

	// Unmarshal directly from reader
	if err := reuseBat.UnmarshalFromReader(limitedReader, proc.Mp()); err != nil {
		return nil, err
	}

	// Verify the batch unmarshal consumed exactly batchSize bytes. Leftover bytes
	// mean the serialized format is inconsistent with what was written, which would
	// silently misalign all subsequent reads in the spill file.
	if n, _ := io.Copy(io.Discard, limitedReader); n > 0 {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", n)
	}

	// Read magic
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		return nil, err
	}
	magic := types.DecodeUint64(r.buf)
	if magic != spillMagic {
		return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}

	if reuseBat.RowCount() != int(cnt) {
		return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}

	return reuseBat, nil
}

func (r *spillBucketReader) close() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

func (hashJoin *HashJoin) getSpilledInputBatch(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var result vm.CallResult
	ctr := &hashJoin.ctr

	for {
		// Try to read next probe batch from current bucket
		if ctr.probeBucketReader != nil {
			// Initialize reusable batch if needed
			if ctr.spillProbeReadBatch == nil {
				ctr.spillProbeReadBatch = batch.NewOffHeapWithSize(0)
			}

			bat, err := ctr.probeBucketReader.readBatch(proc, ctr.spillProbeReadBatch)
			if err != nil && err != io.EOF {
				return result, err
			}

			if bat != nil {
				result.Batch = bat
				return result, nil
			}

			// EOF - done with this bucket's probe file
			ctr.probeBucketReader.close()
			ctr.probeBucketReader = nil
			if spillfs, err := proc.GetSpillFileService(); err == nil {
				spillfs.Delete(proc.Ctx, ctr.probeBucketFileName)
			}
			ctr.probeBucketFileName = ""

			// If we have rightRowsMatched, return nil to trigger finalization
			if ctr.rightRowsMatched != nil {
				return result, nil
			}

			// For non-right joins, clean up immediately
			ctr.cleanHashMap()
		}

		// Load next bucket from queue
		if ctr.mp == nil {
			if len(ctr.spillQueue) == 0 {
				return result, nil
			}

			// Pop from front
			bucket := ctr.spillQueue[0]
			ctr.spillQueue = ctr.spillQueue[1:]

			// Build hashmap; may re-spill and push sub-buckets to front
			tmpJoinMap, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
			if err != nil {
				return result, err
			}

			if tmpJoinMap == nil {
				// Re-spilled: sub-buckets already prepended to spillQueue; loop
				continue
			}

			// Open probe reader
			probeReader, err := newSpillBucketReader(proc, bucket.probeFile)
			if err != nil {
				tmpJoinMap.Free()
				return result, err
			}

			ctr.probeBucketReader = probeReader
			ctr.probeBucketFileName = bucket.probeFile
			ctr.mp = tmpJoinMap
			ctr.itr = nil

			ctr.rightBats = ctr.mp.GetBatches()
			ctr.rightRowCnt = ctr.mp.GetRowCount()

			if hashJoin.IsRightJoin {
				if ctr.rightRowCnt > 0 {
					ctr.rightRowsMatched = &bitmap.Bitmap{}
					ctr.rightRowsMatched.InitWithSize(ctr.rightRowCnt)
					ctr.rightMatchedIter = nil
				}
			}

			ctr.probeState = psNextBatch
			ctr.lastIdx = 0
			ctr.vsIdx = 0
		}
	}
}

func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, file *os.File, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize
	batchSizePos := ctr.spillWriteBuf.Len()
	ctr.spillWriteBuf.Write(types.EncodeInt64(new(int64)))

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

func createRootProbeSpillBucketFiles(proc *process.Process) ([]string, []*os.File, error) {
	uid, _ := uuid.NewV7()
	rootName := fmt.Sprintf("join_probe_%s", uid.String())
	return createSpillBucketChildrenFiles(proc, rootName)
}

func (ctr *container) appendProbeBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {
	ctr.evalJoinCondition(bat, proc)
	return ctr.scatterBatchToFiles(proc, bat, ctr.eqCondVecs, files, buffers, analyzer, seed)
}

// computeXXHash computes xxhash values for partitioning.
// seed allows different hash distributions at each spill depth.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64, buf *[]byte, seed uint64) error {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return nil
	}

	rowCount := len(hashValues)
	if cap(*buf) < 128 {
		*buf = make([]byte, 0, 128)
	}

	for i := 0; i < rowCount; i++ {
		*buf = (*buf)[:0]

		// Prepend seed bytes so different depths produce different distributions
		if seed != 0 {
			var seedBytes [8]byte
			seedBytes[0] = byte(seed)
			seedBytes[1] = byte(seed >> 8)
			seedBytes[2] = byte(seed >> 16)
			seedBytes[3] = byte(seed >> 24)
			seedBytes[4] = byte(seed >> 32)
			seedBytes[5] = byte(seed >> 40)
			seedBytes[6] = byte(seed >> 48)
			seedBytes[7] = byte(seed >> 56)
			*buf = append(*buf, seedBytes[:]...)
		}

		// Encode all key columns for this row
		for _, vec := range keyVecs {
			// For constant vectors, always use index 0
			idx := i
			if vec.IsConst() {
				idx = 0
			} else if i >= vec.Length() {
				continue
			}

			*buf = append(*buf, vec.GetRawBytesAt(idx)...)
		}

		// Compute xxhash
		hashValues[i] = xxhash.Sum64(*buf)
	}

	return nil
}

// rebuildHashmapForBucket loads a spill bucket into a JoinMap.
// If memory exceeds the threshold mid-build and depth < spillMaxPass,
// it re-spills into sub-buckets (prepended to spillQueue) and returns nil, nil.
func (hashJoin *HashJoin) rebuildHashmapForBucket(proc *process.Process, bucket spillBucket, analyzer process.Analyzer) (*message.JoinMap, error) {
	ctr := &hashJoin.ctr

	// Create a temporary hashmap builder
	builder := &hashbuild.HashmapBuilder{}
	cleanupBuilder := func() {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
	}
	if err := builder.Prepare(hashJoin.EqConds[1], -1, proc); err != nil {
		return nil, err
	}

	// Stream batches from build file
	reader, err := newSpillBucketReader(proc, bucket.buildFile)
	if err != nil {
		cleanupBuilder()
		return nil, err
	}
	defer reader.close()

	// Initialize reusable batch if needed
	if ctr.spillBuildReadBatch == nil {
		ctr.spillBuildReadBatch = batch.NewOffHeapWithSize(0)
	}

	nextDepth := bucket.depth + 1

	for {
		bat, err := reader.readBatch(proc, ctr.spillBuildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			cleanupBuilder()
			return nil, err
		}

		if err := builder.Batches.CopyIntoBatches(bat, proc); err != nil {
			cleanupBuilder()
			return nil, err
		}
		builder.InputBatchRowCount += bat.RowCount()

		// Check memory threshold mid-build (only if we can go deeper)
		if bucket.depth < spillMaxPass && ctr.spillThreshold > 0 {
			memUsed := builder.GetSize() + builder.Batches.MemSize
			if memUsed == 0 {
				// fallback: sum batch sizes
				for _, b := range builder.Batches.Buf {
					memUsed += int64(b.Size())
				}
			}
			if memUsed > ctr.spillThreshold {
				// Re-spill: create sub-bucket files at depth+1
				jm, err := hashJoin.reSpillBucket(proc, bucket, builder, reader, nextDepth, analyzer)
				cleanupBuilder()
				return jm, err
			}
		}
	}

	// Delete build and probe files now that we've read them
	spillfs, _ := proc.GetSpillFileService()
	spillfs.Delete(proc.Ctx, bucket.buildFile)

	// Build hashmap
	if err := builder.BuildHashmap(hashJoin.HashOnPK, !hashJoin.HashOnPK, false, proc); err != nil {
		cleanupBuilder()
		return nil, err
	}

	jm := builder.GetJoinMap(proc.Mp())
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)

	// Free only the executors - hashmaps, MultiSels, and batches are now owned by JoinMap
	builder.FreeExecutors()

	return jm, nil
}

// reSpillBucket handles re-spilling when memory threshold is exceeded during rebuild.
// It creates sub-bucket files, partitions data, and uses defer for cleanup.
func (hashJoin *HashJoin) reSpillBucket(proc *process.Process, bucket spillBucket, builder *hashbuild.HashmapBuilder, reader *spillBucketReader, nextDepth int, analyzer process.Analyzer) (*message.JoinMap, error) {
	ctr := &hashJoin.ctr

	// Create sub-bucket files
	subBuildNames, subBuildFiles, err := createSpillBucketChildrenFiles(proc, bucket.buildFile)
	if err != nil {
		return nil, err
	}
	subProbeNames, subProbeFiles, err := createSpillBucketChildrenFiles(proc, bucket.probeFile)
	if err != nil {
		closeAndCleanFiles(subBuildFiles, subBuildNames, proc)
		return nil, err
	}

	// Defer file closing and cleanup on error
	var respillErr error
	defer func() {
		// Close all files
		for _, f := range subBuildFiles {
			if f != nil {
				f.Close()
			}
		}
		for _, f := range subProbeFiles {
			if f != nil {
				f.Close()
			}
		}
		// Delete files only on error
		if respillErr != nil {
			spillfs, _ := proc.GetSpillFileService()
			if spillfs != nil {
				spillfs.Delete(proc.Ctx, subBuildNames...)
				spillfs.Delete(proc.Ctx, subProbeNames...)
			}
		}
		// Free cached expression executors
		ctr.freeSpillBuildExprExecs()
	}()

	// Initialize spill executors once for reuse across all batches
	execs, err := ctr.initSpillBuildExprExecs(proc, hashJoin.EqConds[1])
	if err != nil {
		return nil, err
	}

	subBuildBufs := ctr.acquireReusableSpillBuffers(true)
	subProbeBufs := ctr.acquireReusableSpillBuffers(false)
	seed := uint64(nextDepth)

	// Re-partition all accumulated build batches
	for _, b := range builder.Batches.Buf {
		if respillErr = hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildFiles, subBuildBufs, execs, seed, analyzer); respillErr != nil {
			return nil, respillErr
		}
	}

	// Also partition remaining batches from the build file
	for {
		b, err := reader.readBatch(proc, ctr.spillBuildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			respillErr = err
			return nil, respillErr
		}
		if respillErr = hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildFiles, subBuildBufs, execs, seed, analyzer); respillErr != nil {
			return nil, respillErr
		}
	}

	// Flush build sub-buffers
	for i, buf := range subBuildBufs {
		if _, err := ctr.flushBucketBuffer(proc, buf, subBuildFiles[i], analyzer); err != nil {
			respillErr = err
			return nil, respillErr
		}
		if buf != nil {
			buf.CleanOnlyData()
		}
	}

	// Re-partition the probe file
	probeReader, err := newSpillBucketReader(proc, bucket.probeFile)
	if err != nil {
		respillErr = err
		return nil, respillErr
	}
	defer probeReader.close()

	if ctr.spillProbeReadBatch == nil {
		ctr.spillProbeReadBatch = batch.NewOffHeapWithSize(0)
	}
	for {
		pb, err := probeReader.readBatch(proc, ctr.spillProbeReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			respillErr = err
			return nil, respillErr
		}
		if respillErr = ctr.appendProbeBatchToSpillFiles(proc, pb, subProbeFiles, subProbeBufs, analyzer, seed); respillErr != nil {
			return nil, respillErr
		}
	}

	// Flush probe sub-buffers
	for i, buf := range subProbeBufs {
		if _, err := ctr.flushBucketBuffer(proc, buf, subProbeFiles[i], analyzer); err != nil {
			respillErr = err
			return nil, respillErr
		}
		if buf != nil {
			buf.CleanOnlyData()
		}
	}

	// Delete original files
	spillfs, _ := proc.GetSpillFileService()
	spillfs.Delete(proc.Ctx, bucket.buildFile)
	spillfs.Delete(proc.Ctx, bucket.probeFile)

	// Prepend sub-buckets to spillQueue
	subBuckets := make([]spillBucket, spillNumBuckets)
	for i := 0; i < spillNumBuckets; i++ {
		subBuckets[i] = spillBucket{
			buildFile: subBuildNames[i],
			probeFile: subProbeNames[i],
			depth:     nextDepth,
		}
	}
	ctr.spillQueue = append(subBuckets, ctr.spillQueue...)

	return nil, nil // signal: re-spilled
}

// appendBuildBatchToSubFiles partitions a build batch into sub-bucket files using the given seed.
// It evaluates buildKeyExprs against bat to get the key vectors for hashing, matching
// the same key-based partitioning used by the initial hashbuild spill.
func (hashJoin *HashJoin) appendBuildBatchToSubFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, execs []colexec.ExpressionExecutor, seed uint64, analyzer process.Analyzer) error {
	ctr := &hashJoin.ctr
	// Evaluate build-side key expressions using pre-initialized executors
	keyVecs := make([]*vector.Vector, len(execs))
	for i, exec := range execs {
		vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		keyVecs[i] = vec
	}
	return ctr.scatterBatchToFiles(proc, bat, keyVecs, files, buffers, analyzer, seed)
}

// scatterBatchToFiles distributes rows of bat into per-bucket files based on the hash of keyVecs.
func (ctr *container) scatterBatchToFiles(proc *process.Process, bat *batch.Batch, keyVecs []*vector.Vector, files []*os.File, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {
	rowCount := bat.RowCount()
	if rowCount == 0 {
		return nil
	}
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]
	if err := computeXXHash(keyVecs, hashValues, &ctr.spillHashBuf, seed); err != nil {
		return err
	}
	if cap(ctr.spillBucketRowIds) < spillNumBuckets {
		ctr.spillBucketRowIds = make([][]int32, spillNumBuckets)
	}
	bucketRowIds := ctr.spillBucketRowIds[:spillNumBuckets]

	// Pre-count rows per bucket to pre-allocate slices and avoid repeated append reallocations
	var bktCounts [spillNumBuckets]int32
	for row := 0; row < rowCount; row++ {
		bktCounts[hashValues[row]&(spillNumBuckets-1)]++
	}

	// Pre-allocate bucket slices to exact size needed
	for i := 0; i < spillNumBuckets; i++ {
		if bktCounts[i] > 0 {
			if cap(bucketRowIds[i]) < int(bktCounts[i]) {
				bucketRowIds[i] = make([]int32, 0, bktCounts[i])
			} else {
				bucketRowIds[i] = bucketRowIds[i][:0]
			}
		} else {
			bucketRowIds[i] = bucketRowIds[i][:0]
		}
	}

	// Fill bucket slices with row indices
	for row := 0; row < rowCount; row++ {
		bucketId := hashValues[row] & (spillNumBuckets - 1)
		bucketRowIds[bucketId] = append(bucketRowIds[bucketId], int32(row))
	}

	// Collect non-empty buckets once to avoid iterating all 32 when data is sparse
	nonEmptyBuckets := make([]int, 0, spillNumBuckets)
	for i := 0; i < spillNumBuckets; i++ {
		if bktCounts[i] > 0 {
			nonEmptyBuckets = append(nonEmptyBuckets, i)
		}
	}

	for _, bucketId := range nonEmptyBuckets {
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
		for i, vec := range bat.Vecs {
			if err := buf.Vecs[i].UnionInt32(vec, sels, proc.Mp()); err != nil {
				return err
			}
		}
		buf.SetRowCount(buf.RowCount() + len(sels))
		if buf.RowCount() >= spillBufferSize {
			if _, err := ctr.flushBucketBuffer(proc, buf, files[bucketId], analyzer); err != nil {
				return err
			}
			buf.CleanOnlyData()
		}
	}
	return nil
}

// createSpillBucketChildrenFiles creates spillNumBuckets files with group-style
// hierarchical naming: parentName_0 ... parentName_(spillNumBuckets-1).
func createSpillBucketChildrenFiles(proc *process.Process, parentName string) ([]string, []*os.File, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, nil, err
	}
	if parentName == "" {
		return nil, nil, moerr.NewInternalErrorNoCtx("empty spill parent name")
	}

	names := make([]string, spillNumBuckets)
	files := make([]*os.File, spillNumBuckets)

	for i := 0; i < spillNumBuckets; i++ {
		names[i] = fmt.Sprintf("%s_%d", parentName, i)
		if files[i], err = spillfs.CreateFile(proc.Ctx, names[i]); err != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return nil, nil, err
		}
	}
	return names, files, nil
}

// closeAndCleanFiles closes and deletes a set of spill files.
func closeAndCleanFiles(files []*os.File, names []string, proc *process.Process) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return
	}
	spillfs.Delete(proc.Ctx, names...)
}

func cleanSpillBuffers(buffers []*batch.Batch, proc *process.Process) {
	for _, buf := range buffers {
		if buf != nil {
			buf.Clean(proc.Mp())
		}
	}
}

// initSpillBuildExprExecs initializes or validates cached build expression executors for re-spill.
// Called once per reSpillBucket operation to reuse executors across all batches.
func (ctr *container) initSpillBuildExprExecs(proc *process.Process, conditions []*plan.Expr) ([]colexec.ExpressionExecutor, error) {
	if len(ctr.spillBuildExprExecs) != len(conditions) {
		ctr.freeSpillBuildExprExecs()
		ctr.spillBuildExprExecs = make([]colexec.ExpressionExecutor, len(conditions))
		for i, expr := range conditions {
			var err error
			if ctr.spillBuildExprExecs[i], err = colexec.NewExpressionExecutor(proc, expr); err != nil {
				for j := 0; j < i; j++ {
					ctr.spillBuildExprExecs[j].Free()
				}
				ctr.spillBuildExprExecs = nil
				return nil, err
			}
		}
	}
	return ctr.spillBuildExprExecs, nil
}

// freeSpillBuildExprExecs frees cached build expression executors.
func (ctr *container) freeSpillBuildExprExecs() {
	for _, exec := range ctr.spillBuildExprExecs {
		if exec != nil {
			exec.Free()
		}
	}
	ctr.spillBuildExprExecs = nil
}

func (ctr *container) acquireReusableSpillBuffers(forBuild bool) []*batch.Batch {
	var pool *[]*batch.Batch
	if forBuild {
		pool = &ctr.spillBuildSubBufs
	} else {
		pool = &ctr.spillProbeSubBufs
	}
	if len(*pool) < spillNumBuckets {
		*pool = append(*pool, make([]*batch.Batch, spillNumBuckets-len(*pool))...)
	}
	bufs := (*pool)[:spillNumBuckets]
	for i := range bufs {
		if bufs[i] != nil {
			bufs[i].CleanOnlyData()
		}
	}
	return bufs
}

func (ctr *container) cleanSpillBufferPool(proc *process.Process) {
	cleanSpillBuffers(ctr.spillBuildSubBufs, proc)
	cleanSpillBuffers(ctr.spillProbeSubBufs, proc)
	ctr.spillBuildSubBufs = nil
	ctr.spillProbeSubBufs = nil
}
