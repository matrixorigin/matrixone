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

// Package spillutil provides shared spill-to-disk primitives for join operators.
package spillutil

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	SpillMagic      = 0x12345678DEADBEEF
	SpillNumBuckets = 32
	SpillMaxPass    = 3
)

// SpillBucket holds file descriptors for one spilled bucket.
type SpillBucket struct {
	BuildFd  *os.File
	ProbeFd  *os.File
	BaseName string
	Depth    int
}

// BucketReader reads serialized batch records from an fd.
type BucketReader struct {
	fd          *os.File
	reader      *bufio.Reader
	buf         []byte
	limitReader io.LimitedReader
	Empty       bool // true when no file exists (lazy-created file was never written)
}

func (r *BucketReader) ReadBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
	if r.Empty {
		return nil, io.EOF
	}
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(r.fd, 4*1024*1024)
	}
	if r.buf == nil {
		r.buf = make([]byte, 16)
	}
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	cnt := types.DecodeInt64(r.buf[:8])
	batchSize := types.DecodeInt64(r.buf[8:16])

	reuseBat.CleanOnlyData()

	r.limitReader.R = r.reader
	r.limitReader.N = batchSize
	if err := reuseBat.UnmarshalFromReader(&r.limitReader, proc.Mp()); err != nil {
		return nil, err
	}

	// Verify the batch unmarshal consumed exactly batchSize bytes.
	if r.limitReader.N > 0 {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", r.limitReader.N)
	}

	// Read magic (8 bytes)
	if _, err := io.ReadFull(r.reader, r.buf[:8]); err != nil {
		return nil, err
	}
	if types.DecodeUint64(r.buf[:8]) != SpillMagic {
		return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}

	if reuseBat.RowCount() != int(cnt) {
		return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}

	return reuseBat, nil
}

func (r *BucketReader) ResetForFd(fd *os.File) {
	r.Close()
	r.Empty = false
	if fd == nil {
		r.Empty = true
		return
	}
	r.fd = fd
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(fd, 4*1024*1024)
		r.buf = make([]byte, 16)
	} else {
		r.reader.Reset(fd)
	}
}

func (r *BucketReader) Close() {
	if r.fd != nil {
		r.fd.Close()
		r.fd = nil
	}
	// Keep r.reader alive so ResetForFd/ResetForFile can reuse the 4 MiB buffer.
}

// BucketWriter writes serialized batch records to an fd.
type BucketWriter struct {
	Name string
	Fd   *os.File
}

func (w *BucketWriter) Created() bool { return w.Fd != nil }

func (w *BucketWriter) Close() {
	if w.Fd != nil {
		w.Fd.Close()
		w.Fd = nil
	}
}

func (w *BucketWriter) HandOffFd() *os.File {
	if w.Fd == nil {
		return nil
	}
	w.Fd.Seek(0, io.SeekStart)
	fd := w.Fd
	w.Fd = nil
	return fd
}

// MakeBucketWriters creates SpillNumBuckets writers with derived names.
func MakeBucketWriters(prefix string) []BucketWriter {
	uid := uuid.New().String()
	writers := make([]BucketWriter, SpillNumBuckets)
	for i := range writers {
		writers[i].Name = fmt.Sprintf("%s_%s_%02d", prefix, uid, i)
	}
	return writers
}

// FlushBucketBatch writes bat to w, creating the spill file on first write.
// If analyzer is non-nil, spill bytes/rows are tracked.
func FlushBucketBatch(proc *process.Process, bat *batch.Batch, w *BucketWriter, bucketBuf *bytes.Buffer, analyzer process.Analyzer) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if !w.Created() {
		fs, err := GetSpillFS(proc)
		if err != nil {
			return err
		}
		f, err := fs.CreateAndRemoveFile(proc.Ctx, w.Name)
		if err != nil {
			return err
		}
		w.Fd = f
	}
	cnt := int64(bat.RowCount())
	bucketBuf.Reset()
	bucketBuf.Write(types.EncodeInt64(&cnt))
	batchSizePos := bucketBuf.Len()
	var zero int64
	bucketBuf.Write(types.EncodeInt64(&zero))
	batchStart := bucketBuf.Len()
	bat.MarshalBinaryWithBuffer(bucketBuf, false)
	batchSize := int64(bucketBuf.Len() - batchStart)
	copy(bucketBuf.Bytes()[batchSizePos:batchSizePos+8], types.EncodeInt64(&batchSize))
	magic := uint64(SpillMagic)
	bucketBuf.Write(types.EncodeUint64(&magic))
	written, err := w.Fd.Write(bucketBuf.Bytes())
	if err != nil {
		return err
	}
	if written != bucketBuf.Len() {
		return io.ErrShortWrite
	}
	if analyzer != nil {
		analyzer.Spill(int64(written))
		analyzer.SpillRows(cnt)
	}
	return nil
}

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return h ^ (val + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2))
}

// ComputeXXHash evaluates key vectors and computes XXHash64 values using
// column-at-a-time processing for better cache locality. seed initialises every
// hash slot so different spill depths produce different bucket distributions.
func ComputeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) error {
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

	return nil
}

// ScatterBatch partitions rows by hash key into bucket writers using a two-pass
// bulk approach: compute hashes once, collect row indices per bucket, then bulk-
// copy via UnionInt32 for better performance.
// If analyzer is non-nil, spill bytes/rows are tracked on flush.
func ScatterBatch(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	buffers []*batch.Batch,
	seed uint64,
	bucketBuf *bytes.Buffer,
	analyzer process.Analyzer,
) error {
	return scatterImpl(proc, bat, keyVecs, writers, buffers, seed, bucketBuf, analyzer, nil, nil)
}

// scatterImpl is the internal implementation that accepts reusable buffers.
func scatterImpl(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	buffers []*batch.Batch,
	seed uint64,
	bucketBuf *bytes.Buffer,
	analyzer process.Analyzer,
	reuseHashValues *[]uint64,
	reuseBucketRowIds *[][]int32,
) error {
	rowCount := bat.RowCount()
	if rowCount == 0 {
		return nil
	}

	var hashValues []uint64
	if reuseHashValues != nil && cap(*reuseHashValues) >= rowCount {
		hashValues = (*reuseHashValues)[:rowCount]
	} else {
		hashValues = make([]uint64, rowCount)
		if reuseHashValues != nil {
			*reuseHashValues = hashValues
		}
	}
	if err := ComputeXXHash(keyVecs, hashValues, seed); err != nil {
		return err
	}

	// Collect row indices per bucket.
	var bucketRowIds [][]int32
	if reuseBucketRowIds != nil {
		bucketRowIds = *reuseBucketRowIds
		if cap(bucketRowIds) < len(writers) {
			bucketRowIds = make([][]int32, len(writers))
			*reuseBucketRowIds = bucketRowIds
		} else {
			bucketRowIds = bucketRowIds[:len(writers)]
		}
	} else {
		bucketRowIds = make([][]int32, len(writers))
	}
	estPerBucket := rowCount/len(writers) + rowCount/len(writers)/4 + 1
	for i := range bucketRowIds {
		if cap(bucketRowIds[i]) < estPerBucket {
			bucketRowIds[i] = make([]int32, 0, estPerBucket)
		} else {
			bucketRowIds[i] = bucketRowIds[i][:0]
		}
	}
	for row := 0; row < rowCount; row++ {
		b := int(hashValues[row] & (uint64(len(writers)) - 1))
		bucketRowIds[b] = append(bucketRowIds[b], int32(row))
	}

	// Only iterate non-empty buckets.
	for bucketId, sels := range bucketRowIds {
		if len(sels) == 0 {
			continue
		}
		if writers[bucketId].Name == "" {
			continue // disabled bucket — discard rows
		}
		buf := buffers[bucketId]
		if buf == nil {
			buf = batch.NewOffHeapWithSize(len(bat.Vecs))
			for j, vec := range bat.Vecs {
				buf.Vecs[j] = vector.NewOffHeapVecWithType(*vec.GetType())
				buf.Vecs[j].PreExtend(8192, proc.Mp())
			}
			buffers[bucketId] = buf
		}
		for j, vec := range bat.Vecs {
			if err := buf.Vecs[j].UnionInt32(vec, sels, proc.Mp()); err != nil {
				return err
			}
		}
		buf.SetRowCount(buf.RowCount() + len(sels))
		if buf.RowCount() >= 8192 {
			if err := FlushBucketBatch(proc, buf, &writers[bucketId], bucketBuf, analyzer); err != nil {
				return err
			}
			buf.CleanOnlyData()
		}
	}

	return nil
}

// scatterBatch scatters bat using the engine's reusable hash/row-id buffers.
func (e *SpillEngine) scatterBatch(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	buffers []*batch.Batch,
	seed uint64,
	analyzer process.Analyzer,
) error {
	return scatterImpl(proc, bat, keyVecs, writers, buffers, seed, &e.writeBuf, analyzer,
		&e.scatterHashValues, &e.scatterBucketRowIds)
}

// ShouldSpill returns true when memory or row count exceeds the spill threshold.
// Matches hashbuild.shouldSpillBatches: threshold ≤ 100000 is row-count mode,
// otherwise byte mode.
func ShouldSpill(memUsed int64, rowCnt int64, threshold int64) bool {
	if threshold <= 0 {
		return false
	}
	if threshold <= 100000 {
		return rowCnt >= threshold
	}
	return memUsed > threshold
}

// GetSpillFS returns a spill file service for the given process.
func GetSpillFS(proc *process.Process) (fileservice.MutableFileService, error) {
	return proc.GetSpillFileService()
}

// ReusableBufferPool maintains a persistent pool of spill buffers, preserving
// vector allocations across uses via CleanOnlyData() to avoid repeated allocation.
type ReusableBufferPool struct {
	bufs []*batch.Batch
}

// Acquire returns a slice of n buffers, growing the pool if needed. Existing
// entries have their data cleaned but retain underlying vector memory.
func (p *ReusableBufferPool) Acquire(n int) []*batch.Batch {
	if len(p.bufs) < n {
		p.bufs = append(p.bufs, make([]*batch.Batch, n-len(p.bufs))...)
	}
	bufs := p.bufs[:n]
	for i := range bufs {
		if bufs[i] != nil {
			bufs[i].CleanOnlyData()
		}
	}
	return bufs
}

// Release fully cleans all buffers and resets the pool.
func (p *ReusableBufferPool) Release(proc *process.Process) {
	for i, b := range p.bufs {
		if b != nil {
			b.Clean(proc.Mp())
			p.bufs[i] = nil
		}
	}
	p.bufs = nil
}

// --- SpillEngine: unified bucket-loop state machine ---

// SpillEngineConfig configures a SpillEngine with operator-specific parameters.
type SpillEngineConfig struct {
	BuildKeyExprs           []*plan.Expr // key exprs for hash partitioning during re-spill
	SpillThreshold          int64        // memory threshold for re-spill; 0 disables
	NeedsProbeForEmptyBuild bool         // keep probe file when build is empty (left outer/anti)
	NeedsBuildForEmptyProbe bool         // keep build sub-buckets when probe is empty (right/full outer)
	HashOnPK                bool         // hashmap build strategy
	// Dedup metadata — passed through to HashmapBuilder during rebuild so that
	// duplicate detection, IGNORE/UPDATE/REPLACE semantics are preserved.
	IsDedup                   bool
	OnDuplicateAction         plan.Node_OnDuplicateAction
	DedupBuildKeepLast        bool
	DedupColName              string
	DedupColTypes             []plan.Type
	DelColIdx                 int32
	DedupDeleteMarkerColIdx   int32
	DedupDeleteKeepColIdxList []int32
}

// BucketResult encodes the outcome of a RebuildHashmap call.
type BucketResult int

const (
	BucketReady      BucketResult = iota // hashmap rebuilt, probe file open
	BucketReSpilled                      // over-memory, sub-buckets prepended to queue
	BucketEmptyBuild                     // build empty, probe file preserved (outer join)
	BucketSkip                           // no useful data, skip entirely
	BucketQueueEmpty                     // all buckets processed
)

// SpillEngine owns the spill bucket queue and drives the probe-batch loop.
type SpillEngine struct {
	cfg     SpillEngineConfig
	buckets []SpillBucket

	// Current bucket state
	buildReader    BucketReader
	probeReader    BucketReader
	buildReadBatch *batch.Batch
	probeReadBatch *batch.Batch
	probingActive  bool

	// Reusable scatter state
	buildPool ReusableBufferPool
	probePool ReusableBufferPool
	writeBuf  bytes.Buffer

	// Cached key executors for re-spill
	keyExecs []colexec.ExpressionExecutor
	keyVecs  []*vector.Vector

	// Reusable scatter buffers to avoid per-batch allocations.
	scatterHashValues   []uint64
	scatterBucketRowIds [][]int32

	// probeKeyEval evaluates probe-side key expressions for probe re-scatter.
	// Stored from ScatterProbeTable. Must use probe-side conditions
	// (EqConds[0]), not build-side (EqConds[1]) to ensure correct bucket assignment.
	probeKeyEval func(*batch.Batch) ([]*vector.Vector, error)
}

// NewSpillEngine creates an engine from configuration. Call InitFromSpilledMap next.
func NewSpillEngine(cfg SpillEngineConfig) *SpillEngine {
	return &SpillEngine{cfg: cfg}
}

// InitFromSpilledMap creates SpillBucket entries from build FDs.
// Empty (nil) FDs become placeholder buckets for outer-join semantics.
func (e *SpillEngine) InitFromSpilledMap(buildFds []*os.File) error {
	e.buckets = make([]SpillBucket, 0, len(buildFds))
	for _, fd := range buildFds {
		e.buckets = append(e.buckets, SpillBucket{
			BuildFd: fd,
			Depth:   1,
		})
	}
	return nil
}

// ScatterProbeTable consumes all probe batches from children, hash-partitions
// them into per-bucket probe files, and pairs probe FDs with their build FDs.
// children() returns the next probe batch or nil when done.
func (e *SpillEngine) ScatterProbeTable(
	proc *process.Process,
	children func() (*batch.Batch, error),
	analyzer process.Analyzer,
	evalKeysFn func(bat *batch.Batch) ([]*vector.Vector, error),
) error {
	e.probeKeyEval = evalKeysFn
	writers := MakeBucketWriters("probe")
	buffers := e.probePool.Acquire(len(writers))

	// Disable writers for empty-build buckets unless outer join requires probe output.
	if !e.cfg.NeedsProbeForEmptyBuild {
		for i, b := range e.buckets {
			if b.BuildFd == nil {
				writers[i].Name = ""
			}
		}
	}

	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()

	// Consume all probe batches.
	for {
		bat, err := children()
		if err != nil {
			return err
		}
		if bat == nil {
			break
		}
		if bat.IsEmpty() {
			continue
		}
		keyVecs, err := evalKeysFn(bat)
		if err != nil {
			return err
		}
		if err := e.scatterBatch(proc, bat, keyVecs, writers, buffers, 0, analyzer); err != nil {
			return err
		}
	}

	// Flush remaining buffers and hand off FDs.
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			if err := FlushBucketBatch(proc, buf, &writers[i], &e.writeBuf, analyzer); err != nil {
				return err
			}
		}
	}
	for i := range e.buckets {
		e.buckets[i].ProbeFd = writers[i].HandOffFd()
	}
	return nil
}

// NextProbeBatch returns the next probe batch from the current bucket's probe file.
// Returns nil when EOF is reached (caller should then call FinishBucket).
func (e *SpillEngine) NextProbeBatch(proc *process.Process) (*batch.Batch, error) {
	if !e.probingActive {
		return nil, nil
	}
	if e.probeReadBatch == nil {
		e.probeReadBatch = batch.NewOffHeapWithSize(0)
	}
	bat, err := e.probeReader.ReadBatch(proc, e.probeReadBatch)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return bat, nil
}

// builderMemSize computes total memory used by a HashmapBuilder during the rebuild
// loop. builder.GetSize() only covers hashmap structures (not yet built), so we add
// builder.Batches.MemSize for the raw accumulated batches, with a per-batch fallback.
func builderMemSize(builder *hashbuild.HashmapBuilder) int64 {
	sz := builder.GetSize() + builder.Batches.MemSize
	if sz == 0 {
		for _, b := range builder.Batches.Buf {
			sz += int64(b.Size())
		}
	}
	return sz
}

// RebuildHashmap rebuilds the hashmap for the next bucket in the queue.
func (e *SpillEngine) RebuildHashmap(proc *process.Process, analyzer process.Analyzer) (*message.JoinMap, BucketResult, error) {
	if len(e.buckets) == 0 {
		return nil, BucketQueueEmpty, nil
	}
	bucket := e.buckets[0]

	if bucket.BuildFd == nil {
		// Empty build bucket.
		e.buckets[0].ProbeFd = nil // transferred to reader below; prevent Cleanup double-close
		e.buckets = e.buckets[1:]
		if e.cfg.NeedsProbeForEmptyBuild && bucket.ProbeFd != nil {
			e.probeReader.ResetForFd(bucket.ProbeFd)
			e.probingActive = true
			return nil, BucketEmptyBuild, nil
		}
		if bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		return nil, BucketSkip, nil
	}

	builder := &hashbuild.HashmapBuilder{}
	builder.IsDedup = e.cfg.IsDedup
	builder.OnDuplicateAction = e.cfg.OnDuplicateAction
	builder.DedupBuildKeepLast = e.cfg.DedupBuildKeepLast
	builder.DedupColName = e.cfg.DedupColName
	builder.DedupColTypes = e.cfg.DedupColTypes
	if err := builder.Prepare(e.cfg.BuildKeyExprs, e.cfg.DelColIdx, e.cfg.DedupDeleteMarkerColIdx, e.cfg.DedupDeleteKeepColIdxList, proc); err != nil {
		builder.Free(proc)
		return nil, BucketSkip, err
	}

	e.buildReader.ResetForFd(bucket.BuildFd)
	e.buckets[0].BuildFd = nil // prevent Cleanup double-close on error
	defer e.buildReader.Close()
	if e.buildReadBatch == nil {
		e.buildReadBatch = batch.NewOffHeapWithSize(0)
	}

	for {
		bat, err := e.buildReader.ReadBatch(proc, e.buildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		if err := builder.Batches.CopyIntoBatches(bat, proc); err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		builder.InputBatchRowCount += bat.RowCount()

		if bucket.Depth < SpillMaxPass && e.cfg.SpillThreshold > 0 &&
			(builderMemSize(builder) > e.cfg.SpillThreshold) {
			e.buckets[0].ProbeFd = nil // reSpillBucket takes ownership via reader.ResetForFd
			subBuckets, err := e.reSpillBucket(proc, analyzer, bucket, builder, &e.buildReader)
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			if err != nil {
				return nil, BucketSkip, err
			}
			e.buckets = append(subBuckets, e.buckets[1:]...)
			return nil, BucketReSpilled, nil
		}
	}

	hashOnNonPK := !e.cfg.HashOnPK
	if err := builder.BuildHashmap(e.cfg.HashOnPK, hashOnNonPK, false, proc); err != nil {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		return nil, BucketSkip, err
	}

	jm := builder.GetJoinMap(proc.Mp())
	if jm == nil {
		e.buckets[0].ProbeFd = nil // transferred to reader below; prevent Cleanup double-close
		e.buckets = e.buckets[1:]
		if e.cfg.NeedsProbeForEmptyBuild && bucket.ProbeFd != nil {
			e.probeReader.ResetForFd(bucket.ProbeFd)
			e.probingActive = true
			return nil, BucketEmptyBuild, nil
		}
		if bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		builder.Free(proc)
		return nil, BucketSkip, nil
	}
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)
	builder.FreeExecutors()

	// Pop the head bucket and open probe reader.
	e.buckets = e.buckets[1:]
	if bucket.ProbeFd != nil {
		e.probeReader.ResetForFd(bucket.ProbeFd)
		e.probingActive = true
	}
	return jm, BucketReady, nil
}

func (e *SpillEngine) reSpillBucket(proc *process.Process, analyzer process.Analyzer, bucket SpillBucket, builder *hashbuild.HashmapBuilder, reader *BucketReader) ([]SpillBucket, error) {
	buildWriters := MakeBucketWriters("build_sub")
	buildBuffers := e.buildPool.Acquire(len(buildWriters))
	probeWriters := MakeBucketWriters("probe_sub")
	probeBuffers := e.probePool.Acquire(len(probeWriters))
	seed := uint64(bucket.Depth + 1)

	defer func() {
		for i := range buildWriters {
			buildWriters[i].Close()
		}
		for i := range probeWriters {
			probeWriters[i].Close()
		}
	}()

	// Cache key executors.
	if len(e.keyExecs) != len(e.cfg.BuildKeyExprs) {
		e.freeKeyExecs()
		e.keyExecs = make([]colexec.ExpressionExecutor, len(e.cfg.BuildKeyExprs))
		for i, expr := range e.cfg.BuildKeyExprs {
			var err error
			e.keyExecs[i], err = colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return nil, err
			}
		}
	}

	// evalAndScatter builds key vectors using the given executors and scatters.
	evalAndScatter := func(bat *batch.Batch, writers []BucketWriter, buffers []*batch.Batch, execs []colexec.ExpressionExecutor) error {
		if cap(e.keyVecs) < len(execs) {
			e.keyVecs = make([]*vector.Vector, len(execs))
		}
		keyVecs := e.keyVecs[:len(execs)]
		for i, exec := range execs {
			vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			if err != nil {
				return err
			}
			keyVecs[i] = vec
		}
		return e.scatterBatch(proc, bat, keyVecs, writers, buffers, seed, analyzer)
	}

	// Scatter builder's accumulated batches + remaining build file.
	for _, b := range builder.Batches.Buf {
		if err := evalAndScatter(b, buildWriters, buildBuffers, e.keyExecs); err != nil {
			return nil, err
		}
	}
	for {
		bat, err := reader.ReadBatch(proc, e.buildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := evalAndScatter(bat, buildWriters, buildBuffers, e.keyExecs); err != nil {
			return nil, err
		}
	}

	// Flush build buffers FIRST so Created() sees the correct state below.
	for i := range buildWriters {
		if buildBuffers[i] != nil && buildBuffers[i].RowCount() > 0 {
			if err := FlushBucketBatch(proc, buildBuffers[i], &buildWriters[i], &e.writeBuf, analyzer); err != nil {
				return nil, err
			}
			buildBuffers[i].CleanOnlyData()
		}
	}

	if e.probeReadBatch == nil {
		e.probeReadBatch = batch.NewOffHeapWithSize(0)
	}

	// Scatter probe file. Reuse reader's 4 MiB buffer from the build pass.
	if bucket.ProbeFd != nil {
		reader.ResetForFd(bucket.ProbeFd)
		// Disable probe writers for empty sub-build buckets (unless outer join).
		if !e.cfg.NeedsProbeForEmptyBuild {
			for i := range probeWriters {
				if !buildWriters[i].Created() {
					probeWriters[i].Name = ""
				}
			}
		}
		for {
			bat, err := reader.ReadBatch(proc, e.probeReadBatch)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if err := scatterProbe(proc, e, bat, probeWriters, probeBuffers, seed, analyzer); err != nil {
				return nil, err
			}
		}
	}

	// Flush probe buffer tails (build already flushed before probe scatter).
	for i := range probeWriters {
		if probeBuffers[i] != nil && probeBuffers[i].RowCount() > 0 {
			if err := FlushBucketBatch(proc, probeBuffers[i], &probeWriters[i], &e.writeBuf, analyzer); err != nil {
				return nil, err
			}
			probeBuffers[i].CleanOnlyData()
		}
	}

	var subBuckets []SpillBucket
	for i := range buildWriters {
		hasBuild := buildWriters[i].Created()
		hasProbe := probeWriters[i].Created()
		enqueue := (hasBuild && hasProbe) ||
			(hasBuild && e.cfg.NeedsBuildForEmptyProbe) ||
			(hasProbe && e.cfg.NeedsProbeForEmptyBuild)
		if enqueue {
			subBuckets = append(subBuckets, SpillBucket{
				BuildFd:  buildWriters[i].HandOffFd(),
				ProbeFd:  probeWriters[i].HandOffFd(),
				BaseName: buildWriters[i].Name,
				Depth:    bucket.Depth + 1,
			})
		}
	}
	return subBuckets, nil
}

// FinishBucket closes the current bucket's probe reader.
func (e *SpillEngine) FinishBucket() {
	e.probeReader.Close()
	e.probingActive = false
}

// IsProbing reports whether a probe file is currently open.
func (e *SpillEngine) IsProbing() bool { return e.probingActive }

// HasMoreBuckets reports whether there are remaining buckets to process.
func (e *SpillEngine) HasMoreBuckets() bool { return len(e.buckets) > 0 }

// AdvanceToNextBucket pops the next bucket from the queue, rebuilds the hashmap,
// and calls onRebuild for operator-specific wiring. Returns true if a bucket was
// loaded (caller should loop back to Probe). On BucketReSpilled, sub-buckets are
// prepended and this should be called again. Returns false when the queue is empty.
func (e *SpillEngine) AdvanceToNextBucket(
	proc *process.Process,
	analyzer process.Analyzer,
	onRebuild func(jm *message.JoinMap, res BucketResult),
) (bool, error) {
	if !e.HasMoreBuckets() {
		return false, nil
	}
	jm, res, err := e.RebuildHashmap(proc, analyzer)
	if err != nil {
		return false, err
	}
	switch res {
	case BucketReSpilled:
		return true, nil // caller should retry
	case BucketQueueEmpty:
		return false, nil
	}
	onRebuild(jm, res)
	return true, nil
}

// scatterProbe evaluates probe-side keys (EqConds[0]) for probe re-scatter.
// Uses probeKeyEval — NOT keyExecs — to get correct column subscripts
// when probe and build batches have different column layouts.
func scatterProbe(proc *process.Process, e *SpillEngine, bat *batch.Batch, writers []BucketWriter, buffers []*batch.Batch, seed uint64, analyzer process.Analyzer) error {
	keyVecs, err := e.probeKeyEval(bat)
	if err != nil {
		return err
	}
	return e.scatterBatch(proc, bat, keyVecs, writers, buffers, seed, analyzer)
}

func (e *SpillEngine) freeKeyExecs() {
	for _, exec := range e.keyExecs {
		if exec != nil {
			exec.Free()
		}
	}
	e.keyExecs = nil
}

// TestSetBucketDepth sets the depth of bucket 0 (test helper).
func (e *SpillEngine) TestSetBucketDepth(idx int, depth int) {
	if idx < len(e.buckets) {
		e.buckets[idx].Depth = depth
	}
}

// TestSetBucketProbeFd sets the probe fd of bucket 0 (test helper).
func (e *SpillEngine) TestSetBucketProbeFd(idx int, fd *os.File) {
	if idx < len(e.buckets) {
		e.buckets[idx].ProbeFd = fd
	}
}

// Cleanup releases all engine resources.
func (e *SpillEngine) Cleanup(proc *process.Process) {
	e.probeReader.Close()
	e.buildReader.Close()
	e.probingActive = false
	for i := range e.buckets {
		if e.buckets[i].BuildFd != nil {
			e.buckets[i].BuildFd.Close()
		}
		if e.buckets[i].ProbeFd != nil {
			e.buckets[i].ProbeFd.Close()
		}
	}
	e.buckets = nil
	if e.buildReadBatch != nil {
		e.buildReadBatch.Clean(proc.Mp())
		e.buildReadBatch = nil
	}
	if e.probeReadBatch != nil {
		e.probeReadBatch.Clean(proc.Mp())
		e.probeReadBatch = nil
	}
	e.buildPool.Release(proc)
	e.probePool.Release(proc)
	e.freeKeyExecs()
}
