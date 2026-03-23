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
	"context"
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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	// spillRowBufferSize is the number of rows accumulated in memory before
	// flushing to a spill file during scatter. Keeping this at 8192 rows
	// balances memory pressure vs write-syscall frequency.
	spillRowBufferSize = 8192
	// spillIOBufferSize is the size of the bufio.Reader read-ahead buffer used
	// when reading spill files. A large buffer (4 MiB) dramatically reduces the
	// number of read() syscalls when scanning multi-hundred-MB spill files during
	// multi-level re-spill, which is the primary source of IO-related performance
	// degradation reported by users.
	spillIOBufferSize = 4 * 1024 * 1024
)

// getSpillFS returns the cached spill file service, initialising it on first call.
// Caching avoids a registry lookup, an EnsureDir syscall, and a subPathFS heap
// allocation on every file operation.
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

type spillBucketReader struct {
	file   *os.File
	reader *bufio.Reader
	buf    []byte
	empty  bool // true when no file exists (lazy-created file was never written)
}

func (r *spillBucketReader) readBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
	if r.empty {
		return nil, io.EOF
	}
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

// resetForFile points r at a new spill file, reusing the existing bufio.Reader
// buffer (spillIOBufferSize) to avoid re-allocating 4 MiB on every bucket.
// Pass bucketName="" for an immediately-EOF (empty) reader.
func (r *spillBucketReader) resetForFile(ctx context.Context, spillfs fileservice.MutableFileService, bucketName string) error {
	r.close() // close previous file if any
	r.empty = false

	if bucketName == "" {
		r.empty = true
		return nil
	}

	file, err := spillfs.OpenFile(ctx, bucketName)
	if err != nil {
		return err
	}
	r.file = file

	if r.reader == nil {
		// First use: allocate the buffer once.
		r.reader = bufio.NewReaderSize(file, spillIOBufferSize)
		r.buf = make([]byte, 8)
	} else {
		// Subsequent uses: reuse the existing 4 MiB buffer.
		r.reader.Reset(file)
	}
	return nil
}

// resetForFd points r at an already-open fd, reusing the existing bufio.Reader
// buffer. Pass fd=nil for an immediately-EOF (empty) reader. The fd must be
// seeked to position 0 by the caller (or use handOffFd which does this).
func (r *spillBucketReader) resetForFd(fd *os.File) {
	r.close()
	r.empty = false
	if fd == nil {
		r.empty = true
		return
	}
	r.file = fd
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(fd, spillIOBufferSize)
		r.buf = make([]byte, 8)
	} else {
		r.reader.Reset(fd)
	}
}

// spillBucketWriter tracks the name and open file handle for a spill sub-bucket.
// Files are created lazily: the first call to flushBucketBuffer with non-empty data
// creates the underlying file. Callers check created() to know whether any data
// was actually written.
type spillBucketWriter struct {
	name string
	file *os.File
}

// created reports whether any data has been flushed to this writer's file.
func (w *spillBucketWriter) created() bool { return w.file != nil }

// close closes the underlying file if it was created.
func (w *spillBucketWriter) close() {
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
}

// handOffFd seeks the file back to position 0 and transfers ownership of the fd
// to the caller. After this call, w.file is nil and close() is a no-op.
// Returns nil if no data was ever written (file was never created).
func (w *spillBucketWriter) handOffFd() *os.File {
	if w.file == nil {
		return nil
	}
	w.file.Seek(0, io.SeekStart)
	fd := w.file
	w.file = nil
	return fd
}

// delete releases the file's resources. Since files are created via
// CreateAndRemoveFile, the directory entry is already gone; closing the fd
// releases the inode. Safe to call when the file was never created.
func (w *spillBucketWriter) delete() {
	w.close()
}

// makeSpillBucketWriters creates spillNumBuckets writers with names "join_<uid>_<i>_<suffix>".
// No files are created on disk; file creation is deferred to the first write.
func makeSpillBucketWriters(uid, suffix string) []spillBucketWriter {
	writers := make([]spillBucketWriter, spillNumBuckets)
	for i := range writers {
		writers[i].name = fmt.Sprintf("join_%s_%d_%s", uid, i, suffix)
	}
	return writers
}

func (hashJoin *HashJoin) getSpilledInputBatch(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var result vm.CallResult
	ctr := &hashJoin.ctr

	for {
		// Try to read next probe batch from current bucket
		if ctr.probeBucketActive {
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

			// EOF - done with this bucket's probe file; closing the fd releases the inode
			ctr.probeBucketReader.close()
			ctr.probeBucketActive = false

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
			tmpJoinMap, reSpilled, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
			if err != nil {
				return result, err
			}

			if reSpilled {
				// Sub-buckets already prepended to spillQueue; loop
				continue
			}

			if tmpJoinMap == nil {
				// Build bucket was empty.
				// For left outer/single/anti: probe file was kept; open it with
				// ctr.mp == nil so the existing emptyProbe path outputs every probe
				// row as a non-match (NULLs on the build side).
				// For all other join types: probe file was deleted; skip bucket.
				if !hashJoin.IsLeftOuter() && !hashJoin.IsLeftSingle() && !hashJoin.IsLeftAnti() {
					continue
				}
			}

			// Open probe reader — reuse the cached reader (and its 4 MiB buffer)
			if ctr.probeBucketReader == nil {
				ctr.probeBucketReader = &spillBucketReader{}
			}
			ctr.probeBucketReader.resetForFd(bucket.probeFd)

			ctr.probeBucketActive = true
			ctr.mp = tmpJoinMap // may be nil for empty-build left outer/single/anti
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

// flushBucketBuffer serializes bat and appends it to w's file.
// If the file has not been created yet, it is opened lazily on the first non-empty write.
// Returns 0 without error if bat is nil or empty.
func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, w *spillBucketWriter, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	if w.file == nil {
		spillfs, err := ctr.getSpillFS(proc)
		if err != nil {
			return 0, err
		}
		f, err := spillfs.CreateAndRemoveFile(proc.Ctx, w.name)
		if err != nil {
			return 0, err
		}
		w.file = f
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

	written, err := w.file.Write(ctr.spillWriteBuf.Bytes())
	if err != nil {
		return 0, err
	}
	analyzer.Spill(int64(written))
	analyzer.SpillRows(cnt)

	return cnt, nil
}

func createRootProbeSpillBucketFiles() []spillBucketWriter {
	uid, _ := uuid.NewV7()
	uidStr := uid.String()
	logutil.Infof("creating probe spill files, base: %s", uidStr)
	return makeSpillBucketWriters(uidStr, "probe")
}

func (ctr *container) appendProbeBatchToSpillFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {
	ctr.evalJoinCondition(bat, proc)
	return ctr.scatterBatchToFiles(proc, bat, ctr.eqCondVecs, writers, buffers, analyzer, seed)
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

// shouldReSpill reports whether the builder's current memory (or row count) exceeds
// the spill threshold. Mirrors hashbuild.shouldSpillBatches exactly:
//   - threshold <= 0: never re-spill
//   - threshold <= 100000: treat as a row-count limit
//   - threshold > 100000: treat as a memory-size limit
func (ctr *container) shouldReSpill(builder *hashbuild.HashmapBuilder) bool {
	if ctr.spillThreshold <= 0 {
		return false
	}
	if ctr.spillThreshold <= 100000 {
		return int64(builder.InputBatchRowCount) >= ctr.spillThreshold
	}
	sz := builder.GetSize() + builder.Batches.MemSize
	if sz == 0 {
		for _, b := range builder.Batches.Buf {
			sz += int64(b.Size())
		}
	}
	return sz > ctr.spillThreshold
}

// rebuildHashmapForBucket loads a spill bucket into a JoinMap.
// If memory exceeds the threshold mid-build and depth < spillMaxPass,
// it re-spills into sub-buckets (prepended to spillQueue) and returns nil, nil.
func (hashJoin *HashJoin) rebuildHashmapForBucket(proc *process.Process, bucket spillBucket, analyzer process.Analyzer) (jm *message.JoinMap, reSpilled bool, err error) {
	ctr := &hashJoin.ctr
	logutil.Infof("rebuilding hashmap for spill bucket: %s, depth: %d", bucket.baseName, bucket.depth+1)

	// Create a temporary hashmap builder
	builder := &hashbuild.HashmapBuilder{}
	cleanupBuilder := func() {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
	}
	if prepErr := builder.Prepare(hashJoin.EqConds[1], -1, proc); prepErr != nil {
		return nil, false, prepErr
	}

	// Stream batches from build file — reuse the cached reader (and its 4 MiB buffer).
	spillfs, fsErr := ctr.getSpillFS(proc)
	if fsErr != nil {
		cleanupBuilder()
		return nil, false, fsErr
	}

	// Delete the named build file on any exit path (normal or error).
	// The bucket was already popped from spillQueue, so cleanupSpillFiles won't find it.
	// fd-based build files are closed by reader.close() (deferred below).
	// Use context.Background() so cleanup succeeds even when proc.Ctx is cancelled.
	ownsBuildFile := bucket.buildFile != ""
	defer func() {
		if ownsBuildFile && bucket.buildFile != "" {
			spillfs.RemoveFile(context.Background(), bucket.buildFile)
		}
	}()

	if ctr.spillBuildReader == nil {
		ctr.spillBuildReader = &spillBucketReader{}
	}
	if bucket.buildFd != nil {
		ctr.spillBuildReader.resetForFd(bucket.buildFd)
	} else if rdErr := ctr.spillBuildReader.resetForFile(proc.Ctx, spillfs, bucket.buildFile); rdErr != nil {
		cleanupBuilder()
		return nil, false, rdErr
	}
	reader := ctr.spillBuildReader
	defer reader.close()

	// Initialize reusable batch if needed
	if ctr.spillBuildReadBatch == nil {
		ctr.spillBuildReadBatch = batch.NewOffHeapWithSize(0)
	}

	nextDepth := bucket.depth + 1

	for {
		bat, batErr := reader.readBatch(proc, ctr.spillBuildReadBatch)
		if batErr == io.EOF {
			break
		}
		if batErr != nil {
			cleanupBuilder()
			return nil, false, batErr
		}

		if copyErr := builder.Batches.CopyIntoBatches(bat, proc); copyErr != nil {
			cleanupBuilder()
			return nil, false, copyErr
		}
		builder.InputBatchRowCount += bat.RowCount()

		// Check threshold mid-build (only if we can go deeper)
		if bucket.depth < spillMaxPass && ctr.shouldReSpill(builder) {
			// Transfer build file ownership to reSpillBucket.
			ownsBuildFile = false
			_, reSpillErr := hashJoin.reSpillBucket(proc, bucket, builder, reader, nextDepth, analyzer)
			cleanupBuilder()
			return nil, true, reSpillErr
		}
	}

	// Build file will be deleted by the defer above.

	// Build hashmap
	if buildErr := builder.BuildHashmap(hashJoin.HashOnPK, !hashJoin.HashOnPK, false, proc); buildErr != nil {
		cleanupBuilder()
		return nil, false, buildErr
	}

	jm = builder.GetJoinMap(proc.Mp())
	if jm == nil {
		// Empty build bucket.
		// For join types other than left outer/single/anti nothing can match and the
		// probe file is also useless — delete it and signal "skip this bucket".
		// For left outer/single/anti the probe rows must still be output as non-matches
		// (NULLs on the build side); keep the probe file and return nil so the caller
		// opens it with ctr.mp == nil — the existing emptyProbe path handles this.
		isLeftOuterOrSingleOrAnti := hashJoin.IsLeftOuter() || hashJoin.IsLeftSingle() || hashJoin.IsLeftAnti()
		if !isLeftOuterOrSingleOrAnti {
			// Close probe fd — the inode is released (file was created via CreateAndRemoveFile).
			if bucket.probeFd != nil {
				bucket.probeFd.Close()
			}
		}
		return nil, false, nil
	}
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)

	// Free only the executors — hashmaps, MultiSels, and batches are now owned by JoinMap
	builder.FreeExecutors()

	return jm, false, nil
}

// reSpillBucket handles re-spilling when memory threshold is exceeded during rebuild.
// Sub-bucket files are created lazily: a file is only written when it actually receives data.
func (hashJoin *HashJoin) reSpillBucket(proc *process.Process, bucket spillBucket, builder *hashbuild.HashmapBuilder, reader *spillBucketReader, nextDepth int, analyzer process.Analyzer) (*message.JoinMap, error) {
	ctr := &hashJoin.ctr
	logutil.Infof("re-spilling bucket: %s, depth: %d -> %d", bucket.baseName, bucket.depth+1, nextDepth+1)

	spillfs, err := ctr.getSpillFS(proc)
	if err != nil {
		return nil, err
	}

	// Generate sub-bucket writers without creating files (lazy creation on first write).
	// Names follow "join_<baseName>_<j>_build/probe", e.g. join_<uuid>_3_5_build for
	// root bucket 3, sub-bucket 5. baseName already encodes the full ancestry.
	subBuildWriters := makeSpillBucketWriters(bucket.baseName, "build")
	subProbeWriters := makeSpillBucketWriters(bucket.baseName, "probe")

	// Defer file closing and cleanup on error.
	var respillErr error
	defer func() {
		for i := range subBuildWriters {
			subBuildWriters[i].close()
		}
		for i := range subProbeWriters {
			subProbeWriters[i].close()
		}
		ctr.freeSpillBuildExprExecs()
		// Delete named build file on any exit path. Use context.Background() so
		// cleanup succeeds even when proc.Ctx is cancelled.
		if bucket.buildFile != "" {
			spillfs.RemoveFile(context.Background(), bucket.buildFile)
		}
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
		if respillErr = hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildWriters, subBuildBufs, execs, seed, analyzer); respillErr != nil {
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
		if respillErr = hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildWriters, subBuildBufs, execs, seed, analyzer); respillErr != nil {
			return nil, respillErr
		}
	}

	// Flush build sub-buffers; use writer.created() as ground truth since a
	// mid-scatter flush may have written data even if the final buffer is empty.
	var validBuckets [spillNumBuckets]bool
	for i, buf := range subBuildBufs {
		if buf != nil && buf.RowCount() > 0 {
			if _, err := ctr.flushBucketBuffer(proc, buf, &subBuildWriters[i], analyzer); err != nil {
				respillErr = err
				return nil, respillErr
			}
		}
		validBuckets[i] = subBuildWriters[i].created()
		if buf != nil {
			buf.CleanOnlyData()
		}
	}

	// Re-partition the probe file.
	// reader is the build-file reader, already at EOF. Reuse its 4 MiB buffer
	// by resetting it to point at the probe fd.
	reader.resetForFd(bucket.probeFd)
	probeReader := reader
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
		if respillErr = ctr.appendProbeBatchToSpillFiles(proc, pb, subProbeWriters, subProbeBufs, analyzer, seed); respillErr != nil {
			return nil, respillErr
		}
	}

	// Flush probe sub-buffers and decide which sub-buckets to enqueue.
	//
	// A sub-bucket can produce output when:
	//   • inner/semi/anti-not-exist: both sides non-empty.
	//   • left outer/single/anti: probe non-empty (build may be empty → emptyProbe path).
	//   • right outer/single/anti: build non-empty (probe may be empty → no probe rows but
	//     unmatched build rows are still output).
	isRightOuterOrSingleOrAnti := hashJoin.IsRightOuter() || hashJoin.IsRightSingle() || hashJoin.IsRightAnti()
	isLeftOuterOrSingleOrAnti := hashJoin.IsLeftOuter() || hashJoin.IsLeftSingle() || hashJoin.IsLeftAnti()

	for i, buf := range subProbeBufs {
		// hasProbeData is true if there is data in the buffer or already flushed to file.
		hasProbeData := (buf != nil && buf.RowCount() > 0) || subProbeWriters[i].created()

		if hasProbeData || isRightOuterOrSingleOrAnti {
			if _, err := ctr.flushBucketBuffer(proc, buf, &subProbeWriters[i], analyzer); err != nil {
				respillErr = err
				return nil, respillErr
			}
			// For left outer/single/anti: probe rows output as non-matches even with empty build.
			if isLeftOuterOrSingleOrAnti && hasProbeData {
				validBuckets[i] = true
			}
			// Sub-bucket still invalid: no output possible, clean up.
			if !validBuckets[i] {
				subProbeWriters[i].delete()
				if buf != nil {
					buf.CleanOnlyData()
				}
				continue
			}
		} else {
			// Probe is empty and not right outer/single/anti: no output possible.
			// Clean up any build file that was lazily created.
			subBuildWriters[i].delete()
			validBuckets[i] = false
			if buf != nil {
				buf.CleanOnlyData()
			}
			continue
		}
		if buf != nil {
			buf.CleanOnlyData()
		}
	}

	// Named build file is deleted by the defer above.
	// buildFd is owned by ctr.spillBuildReader; closed when reader.resetForFd(bucket.probeFd)
	// was called above (r.close() fired then). probeFd is now owned by probeReader (same struct).

	// Prepend only valid sub-buckets to spillQueue.
	// Use probeFd=nil when no probe data was written; resetForFd(nil) gives an empty reader.
	subBuckets := make([]spillBucket, 0, spillNumBuckets)
	for i := 0; i < spillNumBuckets; i++ {
		if validBuckets[i] {
			subBuckets = append(subBuckets, spillBucket{
				buildFd:  subBuildWriters[i].handOffFd(),
				probeFd:  subProbeWriters[i].handOffFd(),
				baseName: fmt.Sprintf("%s_%d", bucket.baseName, i),
				depth:    nextDepth,
			})
		}
	}
	ctr.spillQueue = append(subBuckets, ctr.spillQueue...)

	return nil, nil // signal: re-spilled
}

// appendBuildBatchToSubFiles partitions a build batch into sub-bucket files using the given seed.
// It evaluates buildKeyExprs against bat to get the key vectors for hashing, matching
// the same key-based partitioning used by the initial hashbuild spill.
func (hashJoin *HashJoin) appendBuildBatchToSubFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, execs []colexec.ExpressionExecutor, seed uint64, analyzer process.Analyzer) error {
	ctr := &hashJoin.ctr
	// Evaluate build-side key expressions using pre-initialized executors.
	// Reuse cached slice to avoid a per-batch allocation.
	if cap(ctr.spillKeyVecs) < len(execs) {
		ctr.spillKeyVecs = make([]*vector.Vector, len(execs))
	}
	keyVecs := ctr.spillKeyVecs[:len(execs)]
	for i, exec := range execs {
		vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		keyVecs[i] = vec
	}
	return ctr.scatterBatchToFiles(proc, bat, keyVecs, writers, buffers, analyzer, seed)
}

// scatterBatchToFiles distributes rows of bat into per-bucket files based on the hash of keyVecs.
// Writers may hold pre-created files (root probe spill) or nil files for lazy creation (re-spill).
func (ctr *container) scatterBatchToFiles(proc *process.Process, bat *batch.Batch, keyVecs []*vector.Vector, writers []spillBucketWriter, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {
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

	// Collect non-empty buckets once to avoid iterating all 32 when data is sparse.
	// Reuse cached slice to avoid a per-batch allocation.
	ctr.spillNonEmptyBuckets = ctr.spillNonEmptyBuckets[:0]
	for i := 0; i < spillNumBuckets; i++ {
		if bktCounts[i] > 0 {
			ctr.spillNonEmptyBuckets = append(ctr.spillNonEmptyBuckets, i)
		}
	}

	for _, bucketId := range ctr.spillNonEmptyBuckets {
		sels := bucketRowIds[bucketId]
		buf := buffers[bucketId]
		if buf == nil {
			buf = batch.NewOffHeapWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				typ := *vec.GetType()
				buf.Vecs[i] = vector.NewOffHeapVecWithType(typ)
				buf.Vecs[i].PreExtend(spillRowBufferSize, proc.Mp())
			}
			buffers[bucketId] = buf
		}
		for i, vec := range bat.Vecs {
			if err := buf.Vecs[i].UnionInt32(vec, sels, proc.Mp()); err != nil {
				return err
			}
		}
		buf.SetRowCount(buf.RowCount() + len(sels))
		if buf.RowCount() >= spillRowBufferSize {
			if _, err := ctr.flushBucketBuffer(proc, buf, &writers[bucketId], analyzer); err != nil {
				return err
			}
			buf.CleanOnlyData()
		}
	}
	return nil
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
