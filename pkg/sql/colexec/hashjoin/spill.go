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
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	// when reading spill files. A large buffer reduces the number of read()
	// syscalls when scanning multi-hundred-MB spill files during multi-level
	// re-spill.
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
	frameLimit := int64(1 << 40)
	if proc.Base.ImportMemory != nil {
		frameLimit = proc.Base.ImportMemory.JoinInputBytes
	}
	if cnt <= 0 || cnt > 8192 || batchSize <= 0 || batchSize > frameLimit {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid spill frame length")
	}

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
	if remaining := limitedReader.(*io.LimitedReader).N; remaining != 0 {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", remaining)
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
// buffer (spillIOBufferSize) to avoid re-allocating 1 MiB on every bucket.
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
		r.reader = bufio.NewReaderSize(io.NewSectionReader(fd, 0, 1<<63-1), spillIOBufferSize)
		r.buf = make([]byte, 8)
	} else {
		r.reader.Reset(io.NewSectionReader(fd, 0, 1<<63-1))
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

// handOffFd transfers ownership of the fd; readers use independent ReadAt offsets.
// The caller becomes the owner. After this call, w.file is nil and close() is a no-op.
// Returns nil if no data was ever written (file was never created).
func (w *spillBucketWriter) handOffFd() *os.File {
	if w.file == nil {
		return nil
	}
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
			ctr.activeBucket = &bucket
			ctr.spillQueue = ctr.spillQueue[1:]

			if bucket.depth >= spillMaxPass && proc.Base.ImportMemory != nil {
				if err := hashJoin.startSkew(proc, bucket); err != nil {
					return result, err
				}
				return result, nil
			}
			// Build hashmap; may re-spill and push sub-buckets to front
			tmpJoinMap, reSpilled, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
			if err != nil {
				return result, err
			}

			if reSpilled {
				ctr.activeBucket = nil
				// Sub-buckets already prepended to spillQueue; loop
				continue
			}

			if tmpJoinMap == nil {
				// Build bucket was empty.
				// For joins that emit unmatched probe rows (left outer/single/anti,
				// full outer): probe file was kept; open it with ctr.mp == nil so
				// the existing emptyProbe path outputs every probe row as a
				// non-match (NULLs on the build side).
				// For all other join types: probe file was deleted; skip bucket.
				if !hashJoin.EmitUnmatchedProbe() {
					ctr.activeBucket = nil
					continue
				}
			}

			// Open probe reader — reuse the cached reader (and its 4 MiB buffer)
			if ctr.probeBucketReader == nil {
				ctr.probeBucketReader = &spillBucketReader{}
			}
			ctr.probeBucketReader.resetForFd(bucket.probeFd)
			ctr.activeBucket.probeFd = nil
			ctr.activeBucket = nil

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
	written, err := message.WriteJoinSpillFrame(w.file, bat)
	if err != nil {
		return 0, err
	}
	analyzer.Spill(written)
	analyzer.SpillRows(cnt)

	return cnt, nil
}

func createRootProbeSpillBucketFiles() ([]spillBucketWriter, error) {
	uid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	uidStr := uid.String()
	logutil.Debugf("creating probe spill files, base: %s", uidStr)
	return makeSpillBucketWriters(uidStr, "probe"), nil
}

func (ctr *container) appendProbeBatchToSpillFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {
	target := int64(64 << 20)
	if proc.Base.ImportMemory != nil {
		target = proc.Base.ImportMemory.UnitBytes
	}
	return message.ForEachJoinSpillWindow(bat, target, func(window *batch.Batch) error {
		return ctr.appendProbeWindowToSpillFiles(proc, window, writers, buffers, analyzer, seed)
	})
}
func (ctr *container) appendProbeWindowToSpillFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, analyzer process.Analyzer, seed uint64) error {

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	return ctr.scatterBatchToFiles(proc, bat, ctr.eqCondVecs, writers, buffers, analyzer, seed)
}

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return h ^ (val + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2))
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// seed initialises every hash slot so different spill depths produce
// different bucket distributions without per-row byte encoding.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) error {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return nil
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = seed
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

// rebuildHashmapForBucket loads a spill bucket into a JoinMap.
// If memory exceeds the threshold mid-build and depth < spillMaxPass,
// it re-spills into sub-buckets (prepended to spillQueue) and returns nil, nil.
func (hashJoin *HashJoin) rebuildHashmapForBucket(proc *process.Process, bucket spillBucket, analyzer process.Analyzer) (jm *message.JoinMap, reSpilled bool, err error) {
	ctr := &hashJoin.ctr
	// An uncontrolled query has no admitted singleton workspace. Preserve its
	// existing max-depth behavior without imposing a new SQL row-size limit.
	legacyMaxDepth := bucket.depth >= spillMaxPass && proc.Base.ImportMemory == nil
	if bucket.depth >= spillMaxPass && proc.Base.ImportMemory != nil {
		if err := hashJoin.startSkew(proc, bucket); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	logutil.Debugf("rebuilding hashmap for spill bucket: %s, depth: %d", bucket.baseName, bucket.depth)

	var throttler rscthrottler.RSCThrottler
	if value, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.CNMemoryThrottler); ok {
		throttler, _ = value.(rscthrottler.RSCThrottler)
	}
	reserved := int64(0)
	acquire := func(bytes int64) bool {
		if bytes <= 0 {
			return true
		}
		if throttler != nil {
			if _, ok := throttler.Acquire(bytes); !ok {
				return false
			}
		}
		reserved += bytes
		return true
	}
	release := func(bytes int64) {
		bytes = min(bytes, reserved)
		if bytes <= 0 {
			return
		}
		reserved -= bytes
		if throttler != nil {
			throttler.Release(bytes)
		}
	}
	defer func() { release(reserved) }()
	// Create a temporary hashmap builder
	builder := &hashbuild.HashmapBuilder{}
	defer builder.Free(proc)
	cleanupBuilder := func() {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
	}
	if prepErr := builder.Prepare(hashJoin.EqConds[1], -1, -1, nil, proc); prepErr != nil {
		return nil, false, prepErr
	}

	if !legacyMaxDepth {
		builder.Batches.TrackAllocation()
	}
	// Match initial HashBuild: ordinary queries with a CN budget use a logical
	// payload threshold, while every actual copy still requires admission.
	logicalThreshold := !legacyMaxDepth && proc.Base.ImportMemory == nil && throttler != nil

	// Stream batches from build file — reuse the cached reader (and its 4 MiB buffer).
	if ctr.spillBuildReader == nil {
		ctr.spillBuildReader = &spillBucketReader{}
	}
	ctr.spillBuildReader.resetForFd(bucket.buildFd)
	if ctr.activeBucket != nil {
		ctr.activeBucket.buildFd = nil
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

		if !legacyMaxDepth {
			growth := builder.Batches.AppendAllocation(bat)
			threshold := false
			if ctr.spillThreshold > 0 && ctr.spillThreshold <= 100000 {
				threshold = int64(builder.InputBatchRowCount+bat.RowCount()) >= ctr.spillThreshold
			} else if ctr.spillThreshold > 100000 && !logicalThreshold {
				threshold = growth > ctr.spillThreshold-builder.Batches.RetainedBytes()
			}
			if threshold || !acquire(growth) {
				_, spillErr := hashJoin.reSpillBucket(proc, bucket, builder, reader, nextDepth, analyzer, bat)
				cleanupBuilder()
				return nil, true, spillErr
			}
		}
		if copyErr := builder.Batches.CopyIntoBatches(bat, proc); copyErr != nil {
			cleanupBuilder()
			return nil, false, copyErr
		}
		builder.InputBatchRowCount += bat.RowCount()
		if reserved > 0 {
			release(reserved - builder.Batches.RetainedBytes())
		}
		if logicalThreshold && ctr.spillThreshold > 100000 && builder.CollectedBatchBytes() > ctr.spillThreshold {
			// The current batch is already in builder; passing it as pending
			// would write the same rows a second time.
			_, spillErr := hashJoin.reSpillBucket(proc, bucket, builder, reader, nextDepth, analyzer)
			cleanupBuilder()
			return nil, true, spillErr
		}
	}

	if !legacyMaxDepth {
		bound, ok := builder.BuildMemoryBound(hashJoin.EqConds[1], !hashJoin.HashOnPK, false)
		if !ok && proc.Base.ImportMemory != nil || ok && !acquire(bound) {
			_, spillErr := hashJoin.reSpillBucket(proc, bucket, builder, reader, nextDepth, analyzer)
			cleanupBuilder()
			return nil, true, spillErr
		}
	}
	// Build hashmap
	if buildErr := builder.BuildHashmap(hashJoin.HashOnPK, !hashJoin.HashOnPK, false, proc); buildErr != nil {
		cleanupBuilder()
		return nil, false, buildErr
	}

	jm = builder.GetJoinMap(proc.Mp())
	if jm == nil {
		// Empty build bucket.
		// For joins that emit unmatched probe rows (left outer/single/anti,
		// full outer) the probe rows must still be output as non-matches
		// (NULLs on the build side); keep the probe file and return nil so the
		// caller opens it with ctr.mp == nil — the existing emptyProbe path
		// handles this.
		// For all other join types nothing can match and the probe file is
		// also useless — delete it and signal "skip this bucket".
		needsProbeForEmpty := hashJoin.EmitUnmatchedProbe()
		if !needsProbeForEmpty {
			// Close probe fd — the inode is released (file was created via CreateAndRemoveFile).
			if bucket.probeFd != nil {
				bucket.probeFd.Close()
				if ctr.activeBucket != nil {
					ctr.activeBucket.probeFd = nil
				}
			}
		}
		return nil, false, nil
	}
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)
	if throttler != nil && reserved > 0 {
		lease := reserved
		reserved = 0
		jm.SetMemoryRelease(func() { throttler.Release(lease) })
	}

	// Free only the executors — hashmaps, MultiSels, and batches are now owned by JoinMap
	builder.FreeExecutors()

	return jm, false, nil
}

// reSpillBucket handles re-spilling when memory threshold is exceeded during rebuild.
// Sub-bucket files are created lazily: a file is only written when it actually receives data.
func (hashJoin *HashJoin) reSpillBucket(proc *process.Process, bucket spillBucket, builder *hashbuild.HashmapBuilder, reader *spillBucketReader, nextDepth int, analyzer process.Analyzer, pending ...*batch.Batch) (*message.JoinMap, error) {
	ctr := &hashJoin.ctr
	logutil.Debugf("re-spilling bucket: %s, depth: %d -> %d", bucket.baseName, bucket.depth, nextDepth)

	if _, err := ctr.getSpillFS(proc); err != nil {
		return nil, err
	}

	// Generate sub-bucket writers without creating files (lazy creation on first write).
	// Names follow "join_<baseName>_<j>_build/probe", e.g. join_<uuid>_3_5_build for
	// root bucket 3, sub-bucket 5. baseName already encodes the full ancestry.
	subBuildWriters := makeSpillBucketWriters(bucket.baseName, "build")
	subProbeWriters := makeSpillBucketWriters(bucket.baseName, "probe")

	// Defer file closing and cleanup on error.
	defer func() {
		for i := range subBuildWriters {
			subBuildWriters[i].close()
		}
		for i := range subProbeWriters {
			subProbeWriters[i].close()
		}
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
		if err := hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildWriters, subBuildBufs, execs, seed, analyzer); err != nil {
			return nil, err
		}
	}

	for _, incoming := range pending {
		if err := hashJoin.appendBuildBatchToSubFiles(proc, incoming, subBuildWriters, subBuildBufs, execs, seed, analyzer); err != nil {
			return nil, err
		}
	}
	// Also partition remaining batches from the build file
	for {
		b, err := reader.readBatch(proc, ctr.spillBuildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := hashJoin.appendBuildBatchToSubFiles(proc, b, subBuildWriters, subBuildBufs, execs, seed, analyzer); err != nil {
			return nil, err
		}
	}

	// Flush build sub-buffers.
	for i, buf := range subBuildBufs {
		if buf != nil {
			if buf.RowCount() > 0 {
				if _, err := ctr.flushBucketBuffer(proc, buf, &subBuildWriters[i], analyzer); err != nil {
					return nil, err
				}
			}
			buf.CleanOnlyData()
		}
	}

	// Re-partition the probe file.
	// reader is the build-file reader, already at EOF. Reuse its 4 MiB buffer
	// by resetting it to point at the probe fd.
	//
	// Disable probe writers for empty sub-build buckets so scatter discards
	// those probe rows immediately (same pattern as root probe scatter).
	needsProbeForEmpty := hashJoin.EmitUnmatchedProbe()
	if !needsProbeForEmpty {
		for i := range subProbeWriters {
			if !subBuildWriters[i].created() {
				subProbeWriters[i].name = ""
			}
		}
	}
	reader.resetForFd(bucket.probeFd)
	if ctr.activeBucket != nil {
		ctr.activeBucket.probeFd = nil
	}
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
			return nil, err
		}
		if err := ctr.appendProbeBatchToSpillFiles(proc, pb, subProbeWriters, subProbeBufs, analyzer, seed); err != nil {
			return nil, err
		}
	}

	// Flush probe sub-buffers.
	needsBuildForEmpty := hashJoin.EmitUnmatchedBuild()
	for i, buf := range subProbeBufs {
		if buf != nil {
			if buf.RowCount() > 0 {
				if _, err := ctr.flushBucketBuffer(proc, buf, &subProbeWriters[i], analyzer); err != nil {
					return nil, err
				}
			}
			buf.CleanOnlyData()
		}
	}

	// Prepend only useful sub-buckets to spillQueue. A sub-bucket can produce
	// output when both sides are non-empty, or when one side is empty but the
	// join type requires outputting unmatched rows from the other side.
	subBuckets := make([]spillBucket, 0, spillNumBuckets)
	for i := 0; i < spillNumBuckets; i++ {
		hasBuild := subBuildWriters[i].created()
		hasProbe := subProbeWriters[i].created()
		enqueue := (hasBuild && hasProbe) ||
			(hasBuild && needsBuildForEmpty) ||
			(hasProbe && needsProbeForEmpty)
		if enqueue {
			subBuckets = append(subBuckets, spillBucket{
				buildFd:  subBuildWriters[i].handOffFd(),
				probeFd:  subProbeWriters[i].handOffFd(),
				baseName: fmt.Sprintf("%s_%d", bucket.baseName, i),
				depth:    nextDepth,
			})
		} else {
			subBuildWriters[i].delete()
			subProbeWriters[i].delete()
		}
	}
	ctr.spillQueue = append(subBuckets, ctr.spillQueue...)

	return nil, nil // signal: re-spilled
}

// appendBuildBatchToSubFiles partitions a build batch into sub-bucket files using the given seed.
// It evaluates buildKeyExprs against bat to get the key vectors for hashing, matching
// the same key-based partitioning used by the initial hashbuild spill.
func (hashJoin *HashJoin) appendBuildBatchToSubFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, execs []colexec.ExpressionExecutor, seed uint64, analyzer process.Analyzer) error {
	target := int64(64 << 20)
	if proc.Base.ImportMemory != nil {
		target = proc.Base.ImportMemory.UnitBytes
	}
	return message.ForEachJoinSpillWindow(bat, target, func(window *batch.Batch) error {
		return hashJoin.appendBuildWindowToSubFiles(proc, window, writers, buffers, execs, seed, analyzer)
	})
}
func (hashJoin *HashJoin) appendBuildWindowToSubFiles(proc *process.Process, bat *batch.Batch, writers []spillBucketWriter, buffers []*batch.Batch, execs []colexec.ExpressionExecutor, seed uint64, analyzer process.Analyzer) error {

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
	lease, err := acquireSpillWorkspace(proc)
	if err != nil {
		return err
	}
	defer releaseSpillWorkspace(lease)
	defer func() {
		ctr.spillHashValues = nil
		ctr.spillBucketRowIds = nil
		ctr.spillNonEmptyBuckets = nil
		ctr.spillKeyVecs = nil
	}()
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]
	if err := computeXXHash(keyVecs, hashValues, seed); err != nil {
		return err
	}
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

	// Collect non-empty buckets once to avoid iterating all 32 when data is sparse.
	// Reuse cached slice to avoid a per-batch allocation.
	ctr.spillNonEmptyBuckets = ctr.spillNonEmptyBuckets[:0]
	for i := 0; i < spillNumBuckets; i++ {
		if len(bucketRowIds[i]) > 0 {
			ctr.spillNonEmptyBuckets = append(ctr.spillNonEmptyBuckets, i)
		}
	}

	target := int64(64 << 20)
	if proc.Base.ImportMemory != nil {
		target = proc.Base.ImportMemory.UnitBytes
	}
	for _, bucketId := range ctr.spillNonEmptyBuckets {
		if writers[bucketId].name == "" {
			continue
		}
		if err := message.WriteJoinSpillRows(proc.Mp(), bat, bucketRowIds[bucketId], target, func(compact *batch.Batch) error {
			_, err := ctr.flushBucketBuffer(proc, compact, &writers[bucketId], analyzer)
			return err
		}); err != nil {
			return err
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
