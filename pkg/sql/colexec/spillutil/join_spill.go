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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	SpillMagic      = 0x12345678DEADBEEF
	SpillNumBuckets = 32
	SpillMaxPass    = 3
	// Coalesce serialized records across source batches without retaining
	// selected vectors. Retained buffer capacity is charged once.
	spillWriteCoalesceSize = 64 << 10
	// Match the write coalescing boundary so one physical read usually consumes
	// a complete write while keeping per-reader admission bounded.
	spillReadBufferSize = spillWriteCoalesceSize
	// Keep enough decoded-batch headroom to reuse the reservation for ordinary
	// spill records without retaining the pre-admission unmarshal estimate
	// for a large record until the reader closes. The additive bound makes the
	// long-lived charge independent of the largest serialized payload seen.
)

// SpillBucket holds file descriptors for one spilled bucket.
type SpillBucket struct {
	BuildFd   *message.SpillFile
	ProbeFd   *message.SpillFile
	Depth     int
	BuildRows int64
	ProbeRows int64
}

// checkSpillCanceled is intentionally used at batch, bucket, and physical-I/O
// boundaries. Those are frequent enough to bound cancellation latency without
// adding a select to the row-at-a-time hash and vector loops.
func checkSpillCanceled(proc *process.Process) error {
	select {
	case <-proc.Ctx.Done():
		return proc.Ctx.Err()
	default:
		return nil
	}
}

// BucketReader decodes one move-only spill file. A pending header replaces
// bufio.Peek, so all data-scaled decode storage is owned by accounted vectors
// instead of an untracked Go-heap buffer.
type BucketReader struct {
	fd            *os.File
	reader        *accountedFileReader
	header        [16]byte
	headerPending bool
	spillFile     *message.SpillFile
	mergeRecords  bool
	allocation    *SpillAllocationAccount
	cleanRetries  uint64
	schema        []types.Type
}

func (r *BucketReader) ReadBatch(
	proc *process.Process,
	reuseBat *batch.Batch,
) (_ *batch.Batch, retErr error) {
	defer func() {
		if retErr != nil && !errors.Is(retErr, io.EOF) && r.reader != nil {
			_ = r.reader.DisableBufferAt(r.reader.Offset())
		}
	}()
	if err := checkSpillCanceled(proc); err != nil {
		return nil, err
	}
	if r.fd == nil {
		return nil, io.EOF
	}
	if reuseBat == nil {
		return nil, moerr.NewInvalidInput(
			proc.Ctx,
			"spill batch reader requires a reuse batch",
		)
	}
	if r.allocation == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if r.reader == nil {
		var err error
		r.reader, err = newAccountedFileReader(
			proc.Mp(),
			r.allocation,
			r.fd,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := reuseBat.SetAllocationAccount(r.allocation.decoded); err != nil {
		return nil, err
	}
	if _, err := r.readBatchRecord(proc, reuseBat); err != nil {
		reuseBat.Clean(proc.Mp())
		return nil, err
	}
	if err := r.validateSchema(proc, reuseBat); err != nil {
		reuseBat.Clean(proc.Mp())
		return nil, err
	}
	if !r.mergeRecords {
		if err := checkSpillCanceled(proc); err != nil {
			reuseBat.Clean(proc.Mp())
			return nil, err
		}
		return reuseBat, nil
	}

	for reuseBat.RowCount() < colexec.DefaultBatchSize {
		nextRows, err := r.peekRecordRows(proc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, r.mergeReadError(proc, reuseBat, nil, err)
		}
		if nextRows > int64(colexec.DefaultBatchSize-reuseBat.RowCount()) {
			break
		}
		next, err := newSpillBatch(0, r.allocation.decoded)
		if err != nil {
			return nil, r.mergeReadError(proc, reuseBat, nil, err)
		}
		if _, err := r.readBatchRecord(proc, next); err != nil {
			return nil, r.mergeReadError(proc, reuseBat, next, err)
		}
		if err := r.validateSchema(proc, next); err != nil {
			return nil, r.mergeReadError(proc, reuseBat, next, err)
		}
		for i := range next.Vecs {
			if err := reuseBat.Vecs[i].UnionBatch(
				next.Vecs[i],
				0,
				next.RowCount(),
				nil,
				proc.Mp(),
			); err != nil {
				return nil, r.mergeReadError(proc, reuseBat, next, err)
			}
		}
		reuseBat.AddRowCount(next.RowCount())
		next.Clean(proc.Mp())
	}
	if err := checkSpillCanceled(proc); err != nil {
		return nil, r.mergeReadError(proc, reuseBat, nil, err)
	}
	return reuseBat, nil
}

func (r *BucketReader) validateSchema(
	proc *process.Process,
	bat *batch.Batch,
) error {
	if bat == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	if r.schema == nil {
		r.schema = make([]types.Type, len(bat.Vecs))
		for i, vec := range bat.Vecs {
			if vec == nil {
				return moerr.NewInternalError(proc.Ctx, "nil vector in spill batch")
			}
			r.schema[i] = *vec.GetType()
		}
		return nil
	}
	if len(bat.Vecs) != len(r.schema) {
		return moerr.NewInternalError(proc.Ctx, "spill batch schema changed")
	}
	for i, vec := range bat.Vecs {
		if vec == nil || !r.schema[i].Eq(*vec.GetType()) {
			return moerr.NewInternalError(proc.Ctx, "spill batch schema changed")
		}
	}
	return nil
}

func (r *BucketReader) peekRecordRows(proc *process.Process) (int64, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return 0, err
	}
	if !r.headerPending {
		if _, err := io.ReadFull(r.reader, r.header[:]); err != nil {
			return 0, err
		}
		r.headerPending = true
	}
	rows := types.DecodeInt64(r.header[:8])
	batchSize := types.DecodeInt64(r.header[8:])
	if rows < 0 || batchSize < 0 {
		return 0, moerr.NewInternalError(
			proc.Ctx,
			"negative spill batch header",
		)
	}
	return rows, nil
}

func (r *BucketReader) readBatchRecord(
	proc *process.Process,
	reuseBat *batch.Batch,
) (*batch.Batch, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return nil, err
	}
	if !r.headerPending {
		if _, err := io.ReadFull(r.reader, r.header[:]); err != nil {
			return nil, err
		}
	}
	r.headerPending = false
	cnt := types.DecodeInt64(r.header[:8])
	batchSize := types.DecodeInt64(r.header[8:])
	if cnt < 0 || batchSize < 0 {
		return nil, moerr.NewInternalError(
			proc.Ctx,
			"negative spill batch header",
		)
	}
	payloadOffset := r.reader.Offset()
	decode := func() (io.LimitedReader, error) {
		reuseBat.CleanOnlyData()
		if err := checkSpillCanceled(proc); err != nil {
			return io.LimitedReader{}, err
		}
		limited := io.LimitedReader{R: r.reader, N: batchSize}
		return limited, reuseBat.UnmarshalFromReaderWithGrouping(&limited, proc.Mp())
	}
	limited, decodeErr := decode()
	if decodeErr != nil && mpool.IsRetryableAllocationCapacity(decodeErr) {
		// Reuse owns the old capacity while a replacement is allocated. Release
		// it, rewind the unpublished record, and retry against actual allocation.
		reuseBat.Clean(proc.Mp())
		if err := reuseBat.SetAllocationAccount(r.allocation.decoded); err != nil {
			return nil, err
		}
		if err := r.reader.DisableBufferAt(payloadOffset); err != nil {
			return nil, err
		}
		r.cleanRetries++
		limited, decodeErr = decode()
	}
	if decodeErr != nil {
		return nil, decodeErr
	}
	if limited.N != 0 {
		return nil, moerr.NewInternalErrorf(
			proc.Ctx,
			"batch unmarshal did not consume all bytes: %d remaining",
			limited.N,
		)
	}
	if _, err := io.ReadFull(r.reader, r.header[:8]); err != nil {
		return nil, err
	}
	if types.DecodeUint64(r.header[:8]) != SpillMagic {
		return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}
	if reuseBat.RowCount() != int(cnt) {
		return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}
	return reuseBat, checkSpillCanceled(proc)
}

func (r *BucketReader) mergeReadError(
	proc *process.Process,
	dst *batch.Batch,
	src *batch.Batch,
	err error,
) error {
	if src != nil {
		src.Clean(proc.Mp())
	}
	if dst != nil {
		dst.Clean(proc.Mp())
	}
	return err
}

func (r *BucketReader) ResetForSpillFile(file *message.SpillFile) error {
	r.closeCurrentFile()
	if file == nil {
		return nil
	}
	if err := file.Validate(); err != nil {
		_ = file.Close()
		return err
	}
	r.spillFile = file
	r.fd = file.File()
	if r.reader != nil {
		if err := r.reader.Reset(r.fd); err != nil {
			_ = file.Close()
			r.spillFile = nil
			r.fd = nil
			return err
		}
	}
	r.headerPending = false
	r.schema = nil
	return nil
}

func (r *BucketReader) closeCurrentFile() {
	spill := r.spillFile
	r.spillFile = nil
	if spill != nil {
		_ = spill.Close()
		r.fd = nil
	}
	if r.fd != nil {
		_ = r.fd.Close()
		r.fd = nil
	}
	if r.reader != nil {
		_ = r.reader.Reset(nil)
	}
	r.headerPending = false
	r.schema = nil
}

func (r *BucketReader) Close() {
	r.closeCurrentFile()
	if r.reader != nil {
		r.reader.Free()
		r.reader = nil
	}
}

// BucketWriter writes serialized batch records to an fd.
type BucketWriter struct {
	Name            string
	Fd              *os.File
	Budget          *process.HashBuildBudgetGeneration
	Rows            int64
	Bytes           uint64
	spillFS         *spillFileServiceCache
	diskReservation *process.HashBuildSpillDiskReservation
	fdReservation   *process.HashBuildSpillFDReservation
}

// spillFileServiceCache is shared by every writer created by one SpillEngine.
// The service is borrowed from Process: the cache resolves it lazily at the
// existing first-file boundary and never closes it.
type spillFileServiceCache struct {
	once sync.Once
	fs   fileservice.MutableFileService
	err  error
}

func (c *spillFileServiceCache) get(proc *process.Process) (fileservice.MutableFileService, error) {
	if c == nil {
		return proc.GetSpillFileService()
	}
	c.once.Do(func() {
		c.fs, c.err = proc.GetSpillFileService()
	})
	return c.fs, c.err
}

func (w *BucketWriter) getSpillFileService(proc *process.Process) (fileservice.MutableFileService, error) {
	// Directly constructed writers intentionally retain the historical
	// fallback. SpillEngine writers all point at one engine-owned cache.
	if w.spillFS == nil {
		return proc.GetSpillFileService()
	}
	return w.spillFS.get(proc)
}

func (w *BucketWriter) Created() bool { return w.Fd != nil }

func (w *BucketWriter) Close() {
	if w.Fd != nil {
		w.Fd.Close()
		w.Fd = nil
	}
	if w.diskReservation != nil {
		w.diskReservation.Release()
		w.diskReservation = nil
	}
	if w.fdReservation != nil {
		w.fdReservation.Release()
		w.fdReservation = nil
	}
}

func (w *BucketWriter) handOffSpillFile() (*message.SpillFile, error) {
	if w.Fd == nil {
		return nil, nil
	}
	if _, err := w.Fd.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	fd := w.Fd
	w.Fd = nil
	disk := w.diskReservation
	fdToken := w.fdReservation
	w.diskReservation = nil
	w.fdReservation = nil
	release := func() {
		if disk != nil {
			disk.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
	}
	return message.NewSpillFile(fd, w.Rows, w.Bytes, release), nil
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

type spillRecordBuffer interface {
	io.Writer
	Bytes() []byte
	EnsureCapacity(int) error
	Len() int
	Reset()
}

func marshalSpillRecordTo(
	bat *batch.Batch,
	buf spillRecordBuffer,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	cnt := int64(bat.RowCount())
	buf.Reset()
	batchSize, err := bat.MarshalBinaryWithGroupingSize()
	if err != nil || batchSize > math.MaxInt-24 {
		if err != nil {
			return err
		}
		return process.ErrHashBuildBudgetInvalid
	}
	if err := buf.EnsureCapacity(batchSize + 24); err != nil {
		return err
	}
	if err := writeSpillRecordInt64(buf, cnt); err != nil {
		return err
	}
	batchSizePos := buf.Len()
	if err := writeSpillRecordInt64(buf, 0); err != nil {
		return err
	}
	batchStart := buf.Len()
	if err := bat.MarshalBinaryWithGroupingTo(buf); err != nil {
		return err
	}
	serializedSize := int64(buf.Len() - batchStart)
	if setter, ok := buf.(interface{ SetInt64(int, int64) error }); ok {
		if err := setter.SetInt64(batchSizePos, serializedSize); err != nil {
			return err
		}
	} else {
		binary.NativeEndian.PutUint64(
			buf.Bytes()[batchSizePos:batchSizePos+8],
			uint64(serializedSize),
		)
	}
	return writeSpillRecordUint64(buf, uint64(SpillMagic))
}

func writeSpillRecordInt64(w io.Writer, value int64) error {
	if typed, ok := w.(interface{ WriteInt64(int64) error }); ok {
		return typed.WriteInt64(value)
	}
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], uint64(value))
	return writeSpillRecordBytes(w, data[:])
}

func writeSpillRecordUint64(w io.Writer, value uint64) error {
	if typed, ok := w.(interface{ WriteUint64(uint64) error }); ok {
		return typed.WriteUint64(value)
	}
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], value)
	return writeSpillRecordBytes(w, data[:])
}

func writeSpillRecordBytes(w io.Writer, value []byte) error {
	written, err := w.Write(value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

// writeBucketPayload performs disk/FD admission and one physical write for a
// complete sequence of framed records.
func writeBucketPayload(proc *process.Process, payload []byte, rows int64, w *BucketWriter, analyzer process.Analyzer) error {
	if w == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	if w.Budget == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	if err := checkSpillCanceled(proc); err != nil {
		return err
	}
	oldDiskSize := uint64(0)
	newDiskToken := false
	rollbackDisk := func() {
		if w.diskReservation == nil {
			return
		}
		if newDiskToken {
			w.diskReservation.Release()
			w.diskReservation = nil
			return
		}
		_, _ = w.diskReservation.ReconcileDown(oldDiskSize)
	}
	// Keep one growable disk token per file so bookkeeping remains bounded
	// even when the input arrives as millions of tiny batches.
	if w.diskReservation == nil {
		diskToken, err := w.Budget.ReserveSpillDisk(uint64(len(payload)))
		if err != nil {
			return err
		}
		w.diskReservation = diskToken
		newDiskToken = true
	} else {
		oldDiskSize = w.diskReservation.Size()
		if err := w.diskReservation.Grow(uint64(len(payload))); err != nil {
			return err
		}
	}
	if !w.Created() {
		fdToken, err := w.Budget.ReserveSpillFD(1)
		if err != nil {
			rollbackDisk()
			return err
		}
		fs, err := w.getSpillFileService(proc)
		if err != nil {
			if fdToken != nil {
				fdToken.Release()
			}
			rollbackDisk()
			return err
		}
		f, err := fs.CreateAndRemoveFile(proc.Ctx, w.Name)
		if err != nil {
			if fdToken != nil {
				fdToken.Release()
			}
			rollbackDisk()
			return err
		}
		w.Fd = f
		w.fdReservation = fdToken
	}
	if err := checkSpillCanceled(proc); err != nil {
		return err
	}
	written, err := w.Fd.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	if analyzer != nil {
		analyzer.Spill(int64(written))
		analyzer.SpillRows(rows)
	}
	w.Rows += rows
	w.Bytes += uint64(written)
	return nil
}

// ComputeXXHash evaluates key vectors and computes XXHash64 values using
// column-at-a-time processing for better cache locality. seed initialises every
// hash slot so different spill depths produce different bucket distributions.
func ComputeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) {
	keycodec.ComputeXXHash(keyVecs, hashValues, seed)
}

// classifyRows computes bucket counts, prefix offsets, and one contiguous row
// id array in two linear passes. This replaces the historical bucket-by-bucket
// scan of hashValues (which revisited every row once for each bucket).
func classifyRows(hashValues []uint64, bucketCount int, shift uint64, rowIDs []int32, counts []int32, offsets []int32) error {
	if bucketCount <= 0 || bucketCount > SpillNumBuckets ||
		bucketCount&(bucketCount-1) != 0 || shift >= 64 ||
		len(rowIDs) < len(hashValues) || len(counts) < bucketCount ||
		len(offsets) < bucketCount+1 {
		return process.ErrHashBuildBudgetInvalid
	}
	for i := 0; i < bucketCount; i++ {
		counts[i] = 0
	}
	mask := uint64(bucketCount - 1)
	for _, hash := range hashValues {
		counts[int((hash>>shift)&mask)]++
	}
	offsets[0] = 0
	for i := 0; i < bucketCount; i++ {
		offsets[i+1] = offsets[i] + counts[i]
	}
	var writePos [SpillNumBuckets]int32
	copy(writePos[:bucketCount], offsets[:bucketCount])
	for row, hash := range hashValues {
		bucket := int((hash >> shift) & mask)
		pos := writePos[bucket]
		rowIDs[pos] = int32(row)
		writePos[bucket] = pos + 1
	}
	return nil
}

// scatterBatchBounded writes one bucket at a time. It keeps one selected batch
// and one row-id slice, flushing them before advancing to the next bucket.
func (e *SpillEngine) scatterBatchBounded(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	partitionLevel uint64,
	sourceAlreadyCharged bool,
	analyzer process.Analyzer,
) (retErr error) {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if err := checkSpillCanceled(proc); err != nil {
		return err
	}
	if len(writers) == 0 || len(writers) > SpillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	if e.allocation == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	rows := bat.RowCount()
	if !keycodec.ValidVectors(bat.Vecs, rows) ||
		!keycodec.ValidVectors(keyVecs, rows) {
		return process.ErrHashBuildBudgetInvalid
	}
	var selected *batch.Batch
	defer func() {
		if selected != nil {
			selected.Clean(proc.Mp())
			selected = nil
		}
		if retErr != nil && !hashbuild.IsRetryableMemoryCapacity(retErr) {
			e.discardScatterBuffers()
		}
	}()
	if !sourceAlreadyCharged {
		// The child batch is borrowed and already physically live. Rejecting a
		// new logical token cannot reclaim it, so observe it while exact-account
		// admission governs every new scatter allocation.
		externalBytes := bat.Allocated()
		if size := bat.Size(); size > externalBytes {
			externalBytes = size
		}
		analyzer.GetOpStats().SetMaxExtraStat(
			"JoinSpillBorrowedSourceBytes",
			int64(externalBytes),
		)
	}

	if e.allocationMP != nil && e.allocationMP != proc.Mp() {
		return mpool.ErrAllocationAccountInvalid
	}
	e.allocationMP = proc.Mp()
	var err error
	e.scatterHashValues, err = growSpillSlice(
		e.scatterHashValues,
		rows,
		proc.Mp(),
		e.allocation,
		SpillAllocationSiteHashValues,
	)
	if err != nil {
		return err
	}
	hashValues := e.scatterHashValues[:rows]
	// Re-spill must consume fresh hash bits. Merely changing the initial seed
	// leaves the low five bits correlated with the parent partition and can put
	// every parent row into one child, making repartition unable to progress.
	// Level zero uses bits 0..4, level one bits 5..9, and so on.
	ComputeXXHash(keyVecs, hashValues, 0)
	if err := checkSpillCanceled(proc); err != nil {
		return err
	}
	shift := partitionLevel * 5
	if shift >= 64 {
		return process.ErrHashBuildBudgetInvalid
	}
	e.scatterBucketRowIds, err = growSpillSlice(
		e.scatterBucketRowIds,
		rows,
		proc.Mp(),
		e.allocation,
		SpillAllocationSiteRowIDs,
	)
	if err != nil {
		return err
	}
	if cap(e.keyVecs) < len(keyVecs) {
		e.keyVecs = make([]*vector.Vector, len(keyVecs))
	}
	if err := classifyRows(hashValues, len(writers), shift, e.scatterBucketRowIds, e.scatterBucketCounts[:], e.scatterBucketOffsets[:]); err != nil {
		return err
	}
	for bucketID := range writers {
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		start, end := e.scatterBucketOffsets[bucketID], e.scatterBucketOffsets[bucketID+1]
		if start == end || writers[bucketID].Name == "" {
			continue
		}
		if selected == nil {
			selected, err = newSpillBatch(
				len(bat.Vecs),
				e.allocation.selected,
			)
			if err != nil {
				return err
			}
			for j, vec := range bat.Vecs {
				selected.Vecs[j], err = newSpillVector(
					*vec.GetType(),
					e.allocation.selected,
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
				sels := e.scatterBucketRowIds[cursor:attemptEnd]
				var scatterErr error
				for j, vec := range bat.Vecs {
					if scatterErr = selected.Vecs[j].UnionInt32(
						vec,
						sels,
						proc.Mp(),
					); scatterErr != nil {
						break
					}
				}
				if scatterErr == nil {
					selected.SetRowCount(len(sels))
					scatterErr = e.appendScatterRecord(
						proc,
						selected,
						&writers[bucketID],
						bucketID,
						analyzer,
					)
				}
				selected.CleanOnlyData()
				if scatterErr == nil {
					cursor = attemptEnd
					break
				}
				if !hashbuild.IsRetryableMemoryCapacity(scatterErr) {
					return scatterErr
				}
				if err := checkSpillCanceled(proc); err != nil {
					return err
				}
				n := int(attemptEnd - cursor)
				if n > 1 {
					attemptEnd = cursor + int32((n+1)/2)
					analyzer.GetOpStats().AddExtraStat(
						"JoinSpillBatchReductions",
						1,
					)
					continue
				}
				if !reclaimedMinimum {
					before := e.allocation.account.Snapshot().Used
					if err := e.reclaimOptionalScatterBuffers(
						proc,
						writers,
						analyzer,
					); err != nil {
						return err
					}
					reclaimedMinimum = true
					after := e.allocation.account.Snapshot().Used
					if after >= before {
						return hashbuild.NewMinimumAllocationPressureError(
							"join-spill",
							"scatter-selected-or-codec",
							e.allocation.account,
						)
					}
					analyzer.GetOpStats().AddExtraStat(
						"JoinSpillOptionalReclaims",
						1,
					)
					continue
				}
				return hashbuild.NewMinimumAllocationPressureError(
					"join-spill",
					"scatter-selected-or-codec",
					e.allocation.account,
				)
			}
		}
	}
	return nil
}

func (e *SpillEngine) reclaimOptionalScatterBuffers(
	proc *process.Process,
	writers []BucketWriter,
	analyzer process.Analyzer,
) error {
	for bucket, buffer := range e.scatterAccountedWriteBuffers {
		if buffer == nil {
			continue
		}
		if buffer.Len() > 0 {
			if bucket >= len(writers) {
				return process.ErrHashBuildBudgetInvalid
			}
			if err := e.flushPendingScatterBucket(
				proc,
				&writers[bucket],
				bucket,
				analyzer,
			); err != nil {
				return err
			}
		}
		buffer.Free()
		e.scatterAccountedWriteBuffers[bucket] = nil
	}
	e.scatterCoalesceDisabled = true
	if e.scatterAccountedWriteBuf != nil {
		e.scatterAccountedWriteBuf.Free()
		e.scatterAccountedWriteBuf = nil
	}
	return nil
}

func (e *SpillEngine) releaseScatterComputeScratch() {
	if e.allocationMP == nil {
		return
	}
	freeSpillSlice(e.scatterHashValues, e.allocationMP)
	freeSpillSlice(e.scatterBucketRowIds, e.allocationMP)
	e.scatterHashValues = nil
	e.scatterBucketRowIds = nil
}

func (e *SpillEngine) scatterBatchWithPressure(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	partitionLevel uint64,
	sourceAlreadyCharged bool,
	analyzer process.Analyzer,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return e.scatterBatchBounded(
			proc,
			bat,
			keyVecs,
			writers,
			partitionLevel,
			sourceAlreadyCharged,
			analyzer,
		)
	}
	rows := bat.RowCount()
	chunk := rows
	minimumRetried := false
	guard := hashbuild.NewPressureRetryGuard(hashbuild.PressureProgress{
		Used:             e.allocation.account.Snapshot().Used,
		InputUnits:       rows,
		OptionalDisabled: e.scatterCoalesceDisabled,
	}, 64)
	for start := 0; start < rows; {
		end := rows
		if chunk < rows-start {
			end = start + chunk
		}
		current := bat
		currentKeys := keyVecs
		if start != 0 || end != rows {
			var err error
			current, err = bat.WindowWithAllocation(
				start, end, proc.Mp(), e.allocation.selected,
			)
			if err != nil {
				return err
			}
			currentKeys = make([]*vector.Vector, len(keyVecs))
			for i, key := range keyVecs {
				currentKeys[i], err = key.WindowWithAllocation(
					start, end, proc.Mp(), e.allocation.selected,
				)
				if err != nil {
					for j := 0; j < i; j++ {
						currentKeys[j].Free(proc.Mp())
					}
					current.Clean(proc.Mp())
					return err
				}
			}
		}
		err := e.scatterBatchBounded(
			proc,
			current,
			currentKeys,
			writers,
			partitionLevel,
			sourceAlreadyCharged,
			analyzer,
		)
		if current != bat {
			for _, key := range currentKeys {
				key.Free(proc.Mp())
			}
			current.Clean(proc.Mp())
		}
		if err == nil {
			start = end
			minimumRetried = false
			nextUnits := chunk
			if remaining := rows - start; remaining < nextUnits {
				nextUnits = remaining
			}
			guard = hashbuild.NewPressureRetryGuard(hashbuild.PressureProgress{
				Used:             e.allocation.account.Snapshot().Used,
				InputUnits:       nextUnits,
				OptionalDisabled: e.scatterCoalesceDisabled,
			}, 64)
			continue
		}
		if !hashbuild.IsRetryableMemoryCapacity(err) {
			return err
		}
		if cancelErr := checkSpillCanceled(proc); cancelErr != nil {
			return cancelErr
		}
		e.releaseScatterComputeScratch()
		attempted := end - start
		if attempted <= 1 {
			if !minimumRetried {
				if reclaimErr := e.reclaimOptionalScatterBuffers(
					proc,
					writers,
					analyzer,
				); reclaimErr != nil {
					return reclaimErr
				}
				next := hashbuild.PressureProgress{
					Used:             e.allocation.account.Snapshot().Used,
					InputUnits:       attempted,
					OptionalDisabled: e.scatterCoalesceDisabled,
				}
				if guard.Advance(next) != nil {
					return hashbuild.NewMinimumAllocationPressureError(
						"join-spill",
						"scatter-hash",
						e.allocation.account,
					)
				}
				minimumRetried = true
				analyzer.GetOpStats().AddExtraStat(
					"JoinSpillMinimumRetries",
					1,
				)
				continue
			}
			return hashbuild.NewMinimumAllocationPressureError(
				"join-spill",
				"scatter-hash",
				e.allocation.account,
			)
		}
		chunk = (attempted + 1) / 2
		if err := guard.Advance(hashbuild.PressureProgress{
			Used:       e.allocation.account.Snapshot().Used,
			InputUnits: chunk,
		}); err != nil {
			return err
		}
		analyzer.GetOpStats().AddExtraStat("JoinSpillInputReductions", 1)
	}
	return nil
}

// scatterEvaluatedBatchWithPressure extends the same unpublished-input
// checkpoint across key evaluation and scatter. Exact expression executors may
// retain successfully admitted capacities after a later child/result growth
// fails; evaluating a smaller immutable window can then reuse those capacities
// without replaying any bucket record. scatterBatchWithPressure owns the
// transactional boundary after evaluation, so a capacity error returned here
// has not published the current window.
func (e *SpillEngine) scatterEvaluatedBatchWithPressure(
	proc *process.Process,
	bat *batch.Batch,
	writers []BucketWriter,
	partitionLevel uint64,
	sourceAlreadyCharged bool,
	analyzer process.Analyzer,
	eval func(*batch.Batch) ([]*vector.Vector, error),
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if eval == nil {
		return process.ErrHashBuildBudgetInvalid
	}

	rows := bat.RowCount()
	chunk := rows
	minimumRetried := false
	guard := hashbuild.NewPressureRetryGuard(hashbuild.PressureProgress{
		Used:             e.allocation.account.Snapshot().Used,
		InputUnits:       chunk,
		OptionalDisabled: e.scatterCoalesceDisabled,
	}, 64)
	for start := 0; start < rows; {
		end := rows
		if chunk < rows-start {
			end = start + chunk
		}
		current := bat
		if start != 0 || end != rows {
			var err error
			current, err = bat.WindowWithAllocation(
				start, end, proc.Mp(), e.allocation.selected,
			)
			if err != nil {
				return err
			}
		}

		var err error
		if err = checkSpillCanceled(proc); err == nil {
			var keyVecs []*vector.Vector
			keyVecs, err = eval(current)
			if err == nil {
				err = checkSpillCanceled(proc)
			}
			if err == nil {
				err = e.scatterBatchWithPressure(
					proc,
					current,
					keyVecs,
					writers,
					partitionLevel,
					sourceAlreadyCharged,
					analyzer,
				)
			}
		}
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
			guard = hashbuild.NewPressureRetryGuard(hashbuild.PressureProgress{
				Used:             e.allocation.account.Snapshot().Used,
				InputUnits:       nextUnits,
				OptionalDisabled: e.scatterCoalesceDisabled,
			}, 64)
			continue
		}
		if !hashbuild.IsRetryableMemoryCapacity(err) {
			return err
		}
		if cancelErr := checkSpillCanceled(proc); cancelErr != nil {
			return cancelErr
		}

		e.releaseScatterComputeScratch()
		attempted := end - start
		if attempted > 1 {
			chunk = (attempted + 1) / 2
			if err := guard.Advance(hashbuild.PressureProgress{
				Used:             e.allocation.account.Snapshot().Used,
				InputUnits:       chunk,
				OptionalDisabled: e.scatterCoalesceDisabled,
			}); err != nil {
				return err
			}
			analyzer.GetOpStats().AddExtraStat(
				"JoinSpillExpressionInputReductions",
				1,
			)
			continue
		}

		if minimumRetried {
			return hashbuild.NewMinimumAllocationPressureError(
				"join-spill",
				"scatter-expression",
				e.allocation.account,
			)
		}
		if reclaimErr := e.reclaimOptionalScatterBuffers(
			proc,
			writers,
			analyzer,
		); reclaimErr != nil {
			return reclaimErr
		}
		if err := guard.Advance(hashbuild.PressureProgress{
			Used:             e.allocation.account.Snapshot().Used,
			InputUnits:       attempted,
			OptionalDisabled: e.scatterCoalesceDisabled,
		}); err != nil {
			return hashbuild.NewMinimumAllocationPressureError(
				"join-spill",
				"scatter-expression",
				e.allocation.account,
			)
		}
		minimumRetried = true
		analyzer.GetOpStats().AddExtraStat(
			"JoinSpillExpressionMinimumRetries",
			1,
		)
	}
	return nil
}

func (e *SpillEngine) appendScatterRecord(proc *process.Process, bat *batch.Batch, writer *BucketWriter, bucket int, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= SpillNumBuckets || writer == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	return e.appendAccountedScatterRecord(
		proc,
		bat,
		writer,
		bucket,
		int64(bat.RowCount()),
		analyzer,
	)
}

func (e *SpillEngine) appendAccountedScatterRecord(
	proc *process.Process,
	bat *batch.Batch,
	writer *BucketWriter,
	bucket int,
	rows int64,
	analyzer process.Analyzer,
) error {
	if e.allocationMP != nil && e.allocationMP != proc.Mp() {
		return mpool.ErrAllocationAccountInvalid
	}
	e.allocationMP = proc.Mp()
	if e.scatterAccountedWriteBuf == nil {
		var err error
		e.scatterAccountedWriteBuf, err = e.allocation.newBuffer(
			proc.Mp(),
			SpillAllocationSiteMarshalBuffer,
		)
		if err != nil {
			return err
		}
	}
	if err := marshalSpillRecordTo(
		bat,
		e.scatterAccountedWriteBuf,
	); err != nil {
		return err
	}
	payload := e.scatterAccountedWriteBuf.Bytes()
	if e.scatterCoalesceDisabled {
		return writeBucketPayload(proc, payload, rows, writer, analyzer)
	}
	buf := e.scatterAccountedWriteBuffers[bucket]
	if buf != nil && buf.Len() > 0 &&
		buf.Len()+len(payload) > spillWriteCoalesceSize {
		if err := e.flushPendingScatterBucket(
			proc,
			writer,
			bucket,
			analyzer,
		); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return writeBucketPayload(proc, payload, rows, writer, analyzer)
	}
	if buf == nil {
		var err error
		buf, err = e.allocation.newBuffer(
			proc.Mp(),
			SpillAllocationSiteCoalesceBuffer,
		)
		if err != nil {
			return err
		}
		e.scatterAccountedWriteBuffers[bucket] = buf
	}
	if buf.Len() == 0 && buf.Cap() < spillWriteCoalesceSize {
		if err := buf.EnsureCapacity(spillWriteCoalesceSize); err != nil {
			if errors.Is(err, mpool.ErrAllocationAccountCapacity) ||
				errors.Is(err, mpool.ErrAllocationMetadataSlots) {
				return writeBucketPayload(
					proc,
					payload,
					rows,
					writer,
					analyzer,
				)
			}
			return err
		}
	}
	if _, err := buf.Write(payload); err != nil {
		return err
	}
	e.scatterWriteRows[bucket] += rows
	if buf.Len() >= spillWriteCoalesceSize {
		return e.flushPendingScatterBucket(proc, writer, bucket, analyzer)
	}
	return nil
}

func (e *SpillEngine) flushPendingScatterBucket(proc *process.Process, writer *BucketWriter, bucket int, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= SpillNumBuckets || writer == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	buf := e.scatterAccountedWriteBuffers[bucket]
	if buf == nil || buf.Len() == 0 {
		return nil
	}
	err := writeBucketPayload(
		proc,
		buf.Bytes(),
		e.scatterWriteRows[bucket],
		writer,
		analyzer,
	)
	buf.Reset()
	e.scatterWriteRows[bucket] = 0
	return err
}

// flushScatterBuffers is required before writers are handed off or rewound.
// It continues across buckets after an error so no pending buffer remains
// live on a terminal handoff path.
func (e *SpillEngine) flushScatterBuffers(proc *process.Process, writers []BucketWriter, analyzer process.Analyzer) error {
	var firstErr error
	for bucket := 0; bucket < SpillNumBuckets; bucket++ {
		pending := 0
		if e.scatterAccountedWriteBuffers[bucket] != nil {
			pending = e.scatterAccountedWriteBuffers[bucket].Len()
		}
		if pending == 0 {
			continue
		}
		var writer *BucketWriter
		if bucket < len(writers) {
			writer = &writers[bucket]
		}
		if err := e.flushPendingScatterBucket(proc, writer, bucket, analyzer); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *SpillEngine) discardScatterBuffers() {
	for bucket := range e.scatterAccountedWriteBuffers {
		if e.scatterAccountedWriteBuffers[bucket] != nil {
			e.scatterAccountedWriteBuffers[bucket].Reset()
		}
		e.scatterWriteRows[bucket] = 0
	}
}

// releaseScatterScratch terminates one scatter phase. Scatter capacities are
// reusable while a phase consumes multiple source batches, but must not stay
// charged while the next child hashmap is rebuilt. Cleanup calls this method
// as an idempotent fallback for cancellation paths.
func (e *SpillEngine) releaseScatterScratch() {
	freeSpillSlice(
		e.scatterHashValues,
		e.allocationMP,
	)
	freeSpillSlice(
		e.scatterBucketRowIds,
		e.allocationMP,
	)
	e.scatterHashValues = nil
	e.scatterBucketRowIds = nil
	e.keyVecs = nil
	if e.scatterAccountedWriteBuf != nil {
		e.scatterAccountedWriteBuf.Free()
		e.scatterAccountedWriteBuf = nil
	}
	for i := range e.scatterBucketCounts {
		e.scatterBucketCounts[i] = 0
	}
	for i := range e.scatterBucketOffsets {
		e.scatterBucketOffsets[i] = 0
	}
	for i := range e.scatterAccountedWriteBuffers {
		if e.scatterAccountedWriteBuffers[i] != nil {
			e.scatterAccountedWriteBuffers[i].Free()
			e.scatterAccountedWriteBuffers[i] = nil
		}
		e.scatterWriteRows[i] = 0
	}
	e.allocationMP = nil
	e.scatterCoalesceDisabled = false
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
	ProbeKeyExprs           []*plan.Expr // probe keys admitted before expression evaluation
	SpillThreshold          int64        // memory threshold for re-spill; 0 disables
	NeedsProbeForEmptyBuild bool         // keep probe file when build is empty (left outer/anti)
	NeedsBuildForEmptyProbe bool         // keep build sub-buckets when probe is empty (right/full outer)
	HashOnPK                bool         // hashmap build strategy
	NeedAllocateSels        bool         // build per-group row selections
	NeedBatches             bool         // retain build batches in the published JoinMap
	MergeProbeBatches       bool         // merge small adjacent probe records for dedup semantics
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
	// Budget is the statement generation shared with HashBuild. Rebuild and
	// re-spill must charge this exact generation; creating a fresh generation
	// would bypass aggregate admission and make ownership impossible to audit.
	Budget   *process.HashBuildBudgetGeneration
	MaxQueue int
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
	cfg          SpillEngineConfig
	buckets      []SpillBucket
	spillFS      spillFileServiceCache
	allocation   *SpillAllocationAccount
	allocationMP *mpool.MPool

	// Current bucket state
	buildReader      BucketReader
	probeReader      BucketReader
	buildReadBatch   *batch.Batch
	probeReadBatch   *batch.Batch
	probeExpected    int64
	probeDecoded     int64
	probeExpectedSet bool

	// Reusable scatter state
	buildPool ReusableBufferPool
	probePool ReusableBufferPool

	// Cached key executors for re-spill
	keyExecs []colexec.ExpressionExecutor
	keyVecs  []*vector.Vector

	// Reusable scatter buffers to avoid per-batch allocations.
	scatterHashValues            []uint64
	scatterBucketRowIds          []int32
	scatterBucketCounts          [SpillNumBuckets]int32
	scatterBucketOffsets         [SpillNumBuckets + 1]int32
	scatterAccountedWriteBuf     *mpool.AccountedBuffer
	scatterAccountedWriteBuffers [SpillNumBuckets]*mpool.AccountedBuffer
	scatterCoalesceDisabled      bool
	scatterWriteRows             [SpillNumBuckets]int64
	// probeKeyEval evaluates the consuming join's allocation-accounted probe
	// executors during re-scatter.
	probeKeyEval func(*batch.Batch) ([]*vector.Vector, error)
}

// NewSpillEngine binds every spill allocation to one execution generation.
// A spill engine cannot exist outside that account.
func NewSpillEngine(
	cfg SpillEngineConfig,
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
) (*SpillEngine, error) {
	if account == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	allocation, err := NewSpillAllocationAccount(account, owner)
	if err != nil {
		return nil, err
	}
	return newSpillEngine(cfg, allocation)
}

func newSpillEngine(
	cfg SpillEngineConfig,
	allocation *SpillAllocationAccount,
) (*SpillEngine, error) {
	if err := allocation.validate(); err != nil {
		return nil, err
	}
	if cfg.Budget == nil {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if cfg.Budget.Closed() {
		return nil, process.ErrHashBuildBudgetClosed
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = SpillNumBuckets * SpillNumBuckets
	}
	engine := &SpillEngine{
		cfg:        cfg,
		allocation: allocation,
	}
	engine.buildReader.allocation = allocation
	engine.probeReader.allocation = allocation
	return engine, nil
}

func (e *SpillEngine) makeBucketWriters(prefix string) []BucketWriter {
	writers := MakeBucketWriters(prefix)
	for i := range writers {
		writers[i].spillFS = &e.spillFS
		writers[i].Budget = e.cfg.Budget
	}
	return writers
}

// TakeSpillBuildPayload transfers the complete build-side spill dependency
// from a single-consumer JoinMap and resolves its budget generation. Any
// validation failure closes the moved files before returning.
func TakeSpillBuildPayload(
	proc *process.Process,
	jm *message.JoinMap,
) (message.SpillBuildPayload, *process.HashBuildBudgetGeneration, error) {
	if jm == nil {
		return message.SpillBuildPayload{}, nil, moerr.NewInternalError(
			proc.Ctx,
			message.ErrSpillBuildPayloadEmpty.Error(),
		)
	}
	expectedRows := jm.GetRowCount()
	payload, err := jm.TakeSpillBuildPayload()
	if err != nil {
		return message.SpillBuildPayload{}, nil, moerr.NewInternalError(proc.Ctx, err.Error())
	}
	actualRows := int64(0)
	validRows := expectedRows >= 0
	for _, file := range payload.Files {
		if file == nil {
			continue
		}
		rows := file.Rows()
		if rows <= 0 || actualRows > math.MaxInt64-rows {
			validRows = false
			break
		}
		actualRows += rows
	}
	if !validRows || actualRows != expectedRows {
		_ = payload.Close()
		return message.SpillBuildPayload{}, nil, spillRowCountMismatch(
			proc,
			"build payload",
			expectedRows,
			actualRows,
		)
	}
	for _, file := range payload.Files {
		if file == nil {
			continue
		}
		if err := file.Validate(); err != nil {
			_ = payload.Close()
			return message.SpillBuildPayload{}, nil, err
		}
	}

	budget, ok := payload.BudgetRef.(*process.HashBuildBudgetGeneration)
	if !ok || budget == nil {
		_ = payload.Close()
		return message.SpillBuildPayload{}, nil, moerr.NewInternalError(
			proc.Ctx,
			"spilled join map is missing its producer budget generation",
		)
	}
	return payload, budget, nil
}

// InitFromSpilledFiles transfers the sole ownership of each build spill file
// and its resource reservations into the bucket queue.
func (e *SpillEngine) InitFromSpilledFiles(files []*message.SpillFile) {
	e.buckets = make([]SpillBucket, 0, len(files))
	for _, file := range files {
		var rows int64
		if file != nil {
			rows = file.Rows()
		}
		e.buckets = append(e.buckets, SpillBucket{BuildFd: file, BuildRows: rows, Depth: 1})
	}
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
	bucketCount := len(e.buckets)
	if bucketCount == 0 ||
		bucketCount > SpillNumBuckets ||
		bucketCount&(bucketCount-1) != 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	e.probeKeyEval = evalKeysFn
	// The build payload defines the partition fanout. Using the production
	// maximum unconditionally would hash probe rows into writers that have no
	// corresponding build bucket; those files are never handed off and their
	// rows would be silently discarded for reduced-fanout payloads.
	writers := e.makeBucketWriters("probe")[:bucketCount]

	// Disable writers for empty-build buckets unless outer join requires probe output.
	if !e.cfg.NeedsProbeForEmptyBuild {
		for i, b := range e.buckets {
			if b.BuildFd == nil {
				writers[i].Name = ""
			}
		}
	}

	defer func() {
		e.discardScatterBuffers()
		for i := range writers {
			writers[i].Close()
		}
	}()
	defer e.releaseScatterScratch()

	// Consume all probe batches.
	for {
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		bat, err := children()
		if err != nil {
			return err
		}
		// children may have blocked in an upstream operator while cancellation
		// arrived. Do not evaluate or materialize the returned batch afterward.
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		if bat == nil {
			break
		}
		if bat.Last() {
			return moerr.NewNotSupported(proc.Ctx, "join spill does not support recursive input")
		}
		if bat.IsEmpty() {
			continue
		}
		if err := e.scatterEvaluatedBatchWithPressure(
			proc,
			bat,
			writers,
			0,
			false,
			analyzer,
			func(current *batch.Batch) ([]*vector.Vector, error) {
				return e.evalProbeKeys(proc, current, evalKeysFn)
			},
		); err != nil {
			return err
		}
	}

	// Flush remaining buffers and hand off FDs transactionally. A failed rewind
	// must not publish an EOF-positioned file or orphan earlier handoffs.
	if err := checkSpillCanceled(proc); err != nil {
		return err
	}
	if err := e.flushScatterBuffers(proc, writers, analyzer); err != nil {
		return err
	}
	probeFiles := make([]*message.SpillFile, len(e.buckets))
	for i := range e.buckets {
		file, err := writers[i].handOffSpillFile()
		if err != nil {
			for _, handedOff := range probeFiles {
				if handedOff != nil {
					handedOff.Close()
				}
			}
			return err
		}
		probeFiles[i] = file
	}
	for i, file := range probeFiles {
		e.buckets[i].ProbeFd = file
		if file != nil {
			e.buckets[i].ProbeRows = file.Rows()
		}
	}
	return nil
}

// NextProbeBatch returns the next probe batch from the current bucket's probe file.
// Returns nil when EOF is reached (caller should then call FinishBucket).
func (e *SpillEngine) NextProbeBatch(proc *process.Process) (*batch.Batch, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return nil, err
	}
	if e.probeReader.fd == nil {
		return nil, nil
	}
	if e.probeReadBatch == nil {
		var err error
		e.probeReadBatch, err = newSpillBatch(0, e.allocation.decoded)
		if err != nil {
			return nil, err
		}
	}
	e.probeReader.mergeRecords = e.cfg.MergeProbeBatches || e.cfg.IsDedup
	bat, err := e.probeReader.ReadBatch(proc, e.probeReadBatch)
	if err == io.EOF {
		if e.probeExpectedSet && e.probeDecoded != e.probeExpected {
			return nil, spillRowCountMismatch(
				proc,
				"probe",
				e.probeExpected,
				e.probeDecoded,
			)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Cancellation can race the reader's final record-boundary check. Do not
	// hand a freshly decoded batch to the join probe loop after that point.
	if err := checkSpillCanceled(proc); err != nil {
		e.probeReadBatch.Clean(proc.Mp())
		return nil, err
	}
	rows := int64(bat.RowCount())
	if rows < 0 || e.probeDecoded > math.MaxInt64-rows {
		e.probeReadBatch.Clean(proc.Mp())
		return nil, spillRowCountMismatch(
			proc,
			"probe",
			e.probeExpected,
			math.MaxInt64,
		)
	}
	decoded := e.probeDecoded + rows
	if e.probeExpectedSet && decoded > e.probeExpected {
		e.probeReadBatch.Clean(proc.Mp())
		return nil, spillRowCountMismatch(
			proc,
			"probe",
			e.probeExpected,
			decoded,
		)
	}
	e.probeDecoded = decoded
	return bat, nil
}

func (e *SpillEngine) startProbe(file *message.SpillFile, expected int64) error {
	if err := e.probeReader.ResetForSpillFile(file); err != nil {
		return err
	}
	e.probeExpected = expected
	e.probeDecoded = 0
	e.probeExpectedSet = true
	return nil
}

func spillRowCountMismatch(
	proc *process.Process,
	side string,
	expected int64,
	actual int64,
) error {
	return moerr.NewInternalErrorf(
		proc.Ctx,
		"corrupted spill %s row count: expected=%d actual=%d",
		side,
		expected,
		actual,
	)
}

// builderMemSize computes total memory used by a HashmapBuilder during the
// rebuild loop. GetSize covers hashmap structures and Batches.MemSize covers
// the raw accumulated batches maintained by the builder's copy API.
func builderMemSize(builder *hashbuild.HashmapBuilder) int64 {
	return builder.GetSize() + builder.Batches.MemSize
}

func shouldReSpillBeforeRetain(
	builder *hashbuild.HashmapBuilder,
	bat *batch.Batch,
	threshold int64,
) bool {
	if builder == nil || bat == nil {
		return false
	}
	predictedBytes := builderMemSize(builder)
	batchBytes := int64(bat.Size())
	if batchBytes < 0 || predictedBytes > math.MaxInt64-batchBytes {
		predictedBytes = math.MaxInt64
	} else {
		predictedBytes += batchBytes
	}
	predictedRows := int64(builder.InputBatchRowCount)
	batchRows := int64(bat.RowCount())
	if batchRows < 0 || predictedRows > math.MaxInt64-batchRows {
		predictedRows = math.MaxInt64
	} else {
		predictedRows += batchRows
	}
	return colexec.ShouldSpill(predictedBytes, predictedRows, threshold)
}

// RebuildHashmap rebuilds the hashmap for the next bucket in the queue.
func (e *SpillEngine) RebuildHashmap(proc *process.Process, analyzer process.Analyzer) (*message.JoinMap, BucketResult, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return nil, BucketSkip, err
	}
	if len(e.buckets) == 0 {
		return nil, BucketQueueEmpty, nil
	}
	bucket := e.buckets[0]
	if (bucket.BuildFd == nil && bucket.BuildRows != 0) ||
		(bucket.ProbeFd == nil && bucket.ProbeRows != 0) ||
		bucket.BuildRows < 0 || bucket.ProbeRows < 0 {
		return nil, BucketSkip, moerr.NewInternalError(
			proc.Ctx,
			"corrupted spill bucket file/row metadata",
		)
	}

	// A build-only bucket cannot contribute to joins that never emit unmatched
	// build rows. Close and pop it before allocating a reader, copying batches,
	// building a hashmap, or recursively spilling data that will be discarded.
	if bucket.ProbeFd == nil && !e.cfg.NeedsBuildForEmptyProbe {
		e.buckets[0].BuildFd = nil
		e.buckets = e.buckets[1:]
		if bucket.BuildFd != nil {
			_ = bucket.BuildFd.Close()
		}
		return nil, BucketSkip, nil
	}

	if bucket.BuildFd == nil {
		// Empty build bucket.
		e.buckets[0].ProbeFd = nil // transferred to reader below; prevent Cleanup double-close
		e.buckets = e.buckets[1:]
		if e.cfg.NeedsProbeForEmptyBuild && bucket.ProbeFd != nil {
			if err := e.startProbe(bucket.ProbeFd, bucket.ProbeRows); err != nil {
				bucket.ProbeFd = nil
				return nil, BucketSkip, err
			}
			bucket.ProbeFd = nil
			return nil, BucketEmptyBuild, nil
		}
		if bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		return nil, BucketSkip, nil
	}

	builder := &hashbuild.HashmapBuilder{}
	// The rebuild builder is stack-owned until GetJoinMap detaches its durable
	// state. Keep a panic-safe terminal guard: the outer pipeline recover cannot
	// otherwise reach a local builder abandoned by expression, hash, or spill
	// hooks.
	defer builder.Free(proc)
	builder.SetBudget(e.cfg.Budget)
	if err := builder.SetAllocationAccount(e.allocation.account); err != nil {
		return nil, BucketSkip, err
	}
	builder.IsDedup = e.cfg.IsDedup
	builder.OnDuplicateAction = e.cfg.OnDuplicateAction
	builder.DedupBuildKeepLast = e.cfg.DedupBuildKeepLast
	builder.DedupColName = e.cfg.DedupColName
	builder.DedupColTypes = e.cfg.DedupColTypes
	if err := builder.Prepare(e.cfg.BuildKeyExprs, e.cfg.DelColIdx, e.cfg.DedupDeleteMarkerColIdx, e.cfg.DedupDeleteKeepColIdxList, proc); err != nil {
		builder.Free(proc)
		return nil, BucketSkip, err
	}

	if err := e.buildReader.ResetForSpillFile(bucket.BuildFd); err != nil {
		return nil, BucketSkip, err
	}
	e.buckets[0].BuildFd = nil // prevent Cleanup double-close on error
	defer e.buildReader.closeCurrentFile()
	if e.buildReadBatch == nil {
		readBatch, err := newSpillBatch(0, e.allocation.decoded)
		if err != nil {
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		e.buildReadBatch = readBatch
	}
	// A rebuild may pre-admit one scatter workspace so a retained-copy reject
	// can still repartition the batches already owned by the builder. Release it
	// once the hashmap commits, with this defer covering every unhappy path.
	defer e.releaseScatterScratch()
	reSpill := func(pending *batch.Batch) (BucketResult, error) {
		subBuckets, err := e.reSpillBucket(
			proc, analyzer, bucket, builder, &e.buildReader, pending,
		)
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		if err != nil {
			return BucketSkip, err
		}
		e.buckets = append(subBuckets, e.buckets[1:]...)
		return BucketReSpilled, nil
	}

	for {
		if err := checkSpillCanceled(proc); err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		bat, err := e.buildReader.ReadBatch(proc, e.buildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		if err := checkSpillCanceled(proc); err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
		if bucket.Depth < SpillMaxPass {
			if shouldReSpillBeforeRetain(builder, bat, e.cfg.SpillThreshold) {
				if analyzer != nil {
					analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildPreCopyReSpillAttempts", 1)
				}
				result, spillErr := reSpill(bat)
				return nil, result, spillErr
			}
		}
		if err := builder.CopyBuildBatch(bat, proc); err != nil {
			if isBudgetAdmission(err) && bucket.Depth < SpillMaxPass {
				if analyzer != nil {
					analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildCopyAdmissionReSpillAttempts", 1)
				}
				result, spillErr := reSpill(bat)
				return nil, result, spillErr
			}
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			if isBudgetAdmission(err) {
				return nil, BucketSkip, noProgressError(proc, bucket.Depth)
			}
			return nil, BucketSkip, err
		}
		builder.InputBatchRowCount += bat.RowCount()

		// The spill threshold is a heuristic, not a hard memory limit. At the
		// recursion limit, attempt the build and let aggregate budget admission
		// decide whether it is safe; a rejected map allocation remains a
		// controlled query error instead of an OOM.
		if bucket.Depth < SpillMaxPass &&
			colexec.ShouldSpill(builderMemSize(builder), int64(builder.InputBatchRowCount), e.cfg.SpillThreshold) {
			subBuckets, err := e.reSpillBucket(proc, analyzer, bucket, builder, &e.buildReader, nil)
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			if err != nil {
				return nil, BucketSkip, err
			}
			e.buckets = append(subBuckets, e.buckets[1:]...)
			return nil, BucketReSpilled, nil
		}
	}
	if int64(builder.InputBatchRowCount) != bucket.BuildRows {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		return nil, BucketSkip, spillRowCountMismatch(
			proc,
			"build",
			bucket.BuildRows,
			int64(builder.InputBatchRowCount),
		)
	}

	if err := checkSpillCanceled(proc); err != nil {
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		return nil, BucketSkip, err
	}
	// Keep an admitted recovery floor until the hashmap is committed. Releasing
	// it before map admission would let another concurrent build consume the
	// only headroom this bucket has already proven it needs to re-spill.
	if err := builder.BuildHashmap(e.cfg.HashOnPK, e.cfg.NeedAllocateSels, false, proc); err != nil {
		// BuildHashmap may destructively canonicalize Dedup batches before a
		// later allocation is rejected. Only the builder can prove whether its
		// retained batches still represent the original ingress. Read that
		// contract before freeing any partial state: re-spilling a partially
		// rewritten batch can silently lose delete rows or separate them from
		// the survivor whose conflict they describe.
		recoverySafe := builder.RetainedBatchRecoverySafe()
		if isBudgetAdmission(err) && recoverySafe && bucket.Depth < SpillMaxPass {
			// Release the rejected/partial map admission while retaining the
			// original copied batches for transactional re-spill.
			builder.FreeHashMapOnly(proc)
			if analyzer != nil {
				analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildMapAdmissionReSpillAttempts", 1)
			}
			result, spillErr := reSpill(nil)
			return nil, result, spillErr
		}
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		if isBudgetAdmission(err) {
			return nil, BucketSkip, noProgressError(proc, bucket.Depth)
		}
		return nil, BucketSkip, err
	}
	e.releaseScatterScratch()
	if !e.cfg.NeedBatches {
		if err := builder.DrainCopiedBatches(proc, nil); err != nil {
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			return nil, BucketSkip, err
		}
	}

	var jm *message.JoinMap
	joinMapTransferred := false
	defer func() {
		if jm != nil && !joinMapTransferred {
			jm.FreeMemory()
		}
	}()
	jm = builder.GetJoinMap(proc.Mp())
	if jm == nil {
		// GetJoinMap transfers nothing when the decoded build contains no
		// rows. Release executors and every residual builder allocation before
		// handing an empty-build probe file to the caller.
		builder.Free(proc)
		e.buckets[0].ProbeFd = nil // transferred to reader below; prevent Cleanup double-close
		e.buckets = e.buckets[1:]
		if e.cfg.NeedsProbeForEmptyBuild && bucket.ProbeFd != nil {
			if err := e.startProbe(bucket.ProbeFd, bucket.ProbeRows); err != nil {
				bucket.ProbeFd = nil
				return nil, BucketSkip, err
			}
			bucket.ProbeFd = nil
			return nil, BucketEmptyBuild, nil
		}
		if bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		return nil, BucketSkip, nil
	}
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)
	builder.FreeTemporaryVectors(proc)
	builder.FreeExecutors()

	// Pop the head bucket and open probe reader.
	e.buckets = e.buckets[1:]
	if bucket.ProbeFd != nil {
		if err := e.startProbe(bucket.ProbeFd, bucket.ProbeRows); err != nil {
			bucket.ProbeFd = nil
			return nil, BucketSkip, err
		}
		bucket.ProbeFd = nil
	}
	joinMapTransferred = true
	return jm, BucketReady, nil
}

func (e *SpillEngine) reSpillBucket(proc *process.Process, analyzer process.Analyzer, bucket SpillBucket, builder *hashbuild.HashmapBuilder, reader *BucketReader, pending *batch.Batch) ([]SpillBucket, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return nil, err
	}
	// Re-spill only drains the builder's copied batches. Drop the failed
	// hashmap-build executor set before admitting the engine's re-partition
	// executors, so the two equivalent retained working sets never overlap.
	builder.FreeExecutors()
	buildWriters := e.makeBucketWriters("build_sub")
	probeWriters := e.makeBucketWriters("probe_sub")
	partitionLevel := uint64(bucket.Depth)

	probeFdConsumed := false
	committed := false
	var subBuckets []SpillBucket
	defer func() {
		if !committed {
			e.discardScatterBuffers()
		}
		for i := range buildWriters {
			buildWriters[i].Close()
		}
		for i := range probeWriters {
			probeWriters[i].Close()
		}
		if !probeFdConsumed && bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		if !committed {
			for i := range subBuckets {
				if subBuckets[i].BuildFd != nil {
					_ = subBuckets[i].BuildFd.Close()
				}
				if subBuckets[i].ProbeFd != nil {
					_ = subBuckets[i].ProbeFd.Close()
				}
			}
		}
	}()
	defer e.releaseScatterScratch()

	// Cache key executors.
	if len(e.keyExecs) != len(e.cfg.BuildKeyExprs) {
		execs, err := hashbuild.NewExpressionExecutors(
			proc,
			e.cfg.BuildKeyExprs,
			e.allocation.account,
		)
		if err != nil {
			for _, exec := range execs {
				exec.Free()
			}
		}
		if err != nil {
			return nil, err
		}
		e.freeKeyExecs()
		e.keyExecs = execs
	}

	// evalAndScatter builds key vectors using the given executors and scatters.
	evalAndScatter := func(
		bat *batch.Batch,
		writers []BucketWriter,
		execs []colexec.ExpressionExecutor,
		sourceAlreadyCharged bool,
	) error {
		if cap(e.keyVecs) < len(execs) {
			e.keyVecs = make([]*vector.Vector, len(execs))
		}
		keyVecs := e.keyVecs[:len(execs)]
		defer func() {
			for i := range keyVecs {
				keyVecs[i] = nil
			}
		}()
		return e.scatterEvaluatedBatchWithPressure(
			proc,
			bat,
			writers,
			partitionLevel,
			sourceAlreadyCharged,
			analyzer,
			func(current *batch.Batch) ([]*vector.Vector, error) {
				for i := range keyVecs {
					keyVecs[i] = nil
				}
				for i := range execs {
					vec, err := execs[i].Eval(proc, []*batch.Batch{current}, nil)
					if err != nil {
						// Exact capacity pressure keeps the executor tree as the
						// rollback checkpoint: admitted child/result capacities may
						// make a smaller immutable window fit. Every other failure is
						// terminal and can destroy the private tree immediately.
						if !hashbuild.IsRetryableMemoryCapacity(err) {
							e.freeKeyExecs()
						}
						return nil, err
					}
					keyVecs[i] = vec
				}
				return keyVecs, nil
			},
		)
	}

	var buildRows int64
	if err := builder.DrainCopiedBatches(proc, func(b *batch.Batch) error {
		if b != nil {
			buildRows += int64(b.RowCount())
		}
		if err := evalAndScatter(b, buildWriters, e.keyExecs, true); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if pending != nil && pending.RowCount() > 0 {
		// pending is the current BucketReader batch whose copy admission failed;
		// the reader keeps its batch token live until the next ReadBatch.
		if err := evalAndScatter(pending, buildWriters, e.keyExecs, true); err != nil {
			return nil, err
		}
	}

	if pending != nil {
		buildRows += int64(pending.RowCount())
	}
	for {
		if err := checkSpillCanceled(proc); err != nil {
			return nil, err
		}
		bat, err := reader.ReadBatch(proc, e.buildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		buildRows += int64(bat.RowCount())
		if err := evalAndScatter(bat, buildWriters, e.keyExecs, true); err != nil {
			return nil, err
		}
	}
	if buildRows != bucket.BuildRows {
		return nil, spillRowCountMismatch(
			proc,
			"build",
			bucket.BuildRows,
			buildRows,
		)
	}
	if err := e.flushScatterBuffers(proc, buildWriters, analyzer); err != nil {
		return nil, err
	}

	if e.probeReadBatch == nil {
		readBatch, err := newSpillBatch(0, e.allocation.decoded)
		if err != nil {
			return nil, err
		}
		e.probeReadBatch = readBatch
	}

	var probeRows int64
	if bucket.ProbeFd != nil {
		if err := reader.ResetForSpillFile(bucket.ProbeFd); err != nil {
			return nil, err
		}
		probeFdConsumed = true
		// Disable probe writers for empty sub-build buckets (unless outer join).
		if !e.cfg.NeedsProbeForEmptyBuild {
			for i := range probeWriters {
				if !buildWriters[i].Created() {
					probeWriters[i].Name = ""
				}
			}
		}
		for {
			if err := checkSpillCanceled(proc); err != nil {
				return nil, err
			}
			bat, err := reader.ReadBatch(proc, e.probeReadBatch)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			probeRows += int64(bat.RowCount())
			if err := scatterProbe(proc, e, bat, probeWriters, partitionLevel, analyzer); err != nil {
				return nil, err
			}
		}
		if probeRows != bucket.ProbeRows {
			return nil, spillRowCountMismatch(
				proc,
				"probe",
				bucket.ProbeRows,
				probeRows,
			)
		}
		if err := e.flushScatterBuffers(proc, probeWriters, analyzer); err != nil {
			return nil, err
		}
	}

	var childBuildRows, childProbeRows int64
	for i := range buildWriters {
		hasBuild := buildWriters[i].Created()
		hasProbe := probeWriters[i].Created()
		// Validate conservation over all physical children, including build-only
		// partitions that an inner join may later discard.
		allBuildRows := buildWriters[i].Rows
		allProbeRows := probeWriters[i].Rows
		childBuildRows += allBuildRows
		childProbeRows += allProbeRows
		// Keep every non-empty build child even when its probe side is empty.
		// This preserves exact build-row conservation; the next rebuild simply
		// skips the child for an inner join. Probe-only children remain relevant
		// only for outer joins.
		enqueue := hasBuild || (hasProbe && e.cfg.NeedsProbeForEmptyBuild)
		if enqueue {
			if len(e.buckets)-1+len(subBuckets)+1 > e.cfg.MaxQueue {
				return nil, &process.HashBuildBudgetError{
					Kind:    process.HashBuildBudgetErrorInvalid,
					Message: fmt.Sprintf("spill queue limit exceeded: limit=%d", e.cfg.MaxQueue),
				}
			}
			buildFile, err := buildWriters[i].handOffSpillFile()
			if err != nil {
				return nil, err
			}
			probeFile, err := probeWriters[i].handOffSpillFile()
			if err != nil {
				if buildFile != nil {
					buildFile.Close()
				}
				return nil, err
			}
			buildRowsChild := int64(0)
			probeRowsChild := int64(0)
			if buildFile != nil {
				buildRowsChild = buildFile.Rows()
			}
			if probeFile != nil {
				probeRowsChild = probeFile.Rows()
			}
			subBuckets = append(subBuckets, SpillBucket{
				BuildFd:   buildFile,
				ProbeFd:   probeFile,
				BuildRows: buildRowsChild,
				ProbeRows: probeRowsChild,
				Depth:     bucket.Depth + 1,
			})
		}
	}
	// With no probe side and no outer-join retention requirement, this bucket
	// cannot contribute rows to the join. It is safe to discard the partition
	// set after closing the parent; no child progress is required because the
	// entire bucket is semantically unreachable.
	if bucket.ProbeFd == nil && !e.cfg.NeedsBuildForEmptyProbe && !e.cfg.NeedsProbeForEmptyBuild {
		for i := range subBuckets {
			if subBuckets[i].BuildFd != nil {
				_ = subBuckets[i].BuildFd.Close()
			}
			if subBuckets[i].ProbeFd != nil {
				_ = subBuckets[i].ProbeFd.Close()
			}
		}
		return nil, nil
	}
	if childBuildRows != buildRows {
		for i := range subBuckets {
			if subBuckets[i].BuildFd != nil {
				subBuckets[i].BuildFd.Close()
			}
			if subBuckets[i].ProbeFd != nil {
				subBuckets[i].ProbeFd.Close()
			}
		}
		return nil, moerr.NewInternalErrorf(
			proc.Ctx,
			"join spill build-row conservation failed at depth %d (source=%d, children=%d)",
			bucket.Depth, buildRows, childBuildRows,
		)
	}
	if len(subBuckets) == 0 {
		return nil, moerr.NewInternalErrorf(
			proc.Ctx,
			"join spill produced no child partitions at depth %d (build_rows=%d, probe_rows=%d)",
			bucket.Depth, buildRows, bucket.ProbeRows,
		)
	}
	// Inner/right joins deliberately do not create probe files for children
	// with no build rows: those unmatched probe rows cannot affect the result.
	// Full/left outer semantics retain them and therefore require exact probe
	// conservation. Every mode still rejects row creation.
	probeConservationFailed := bucket.ProbeRows != 0 &&
		(childProbeRows > bucket.ProbeRows ||
			(e.cfg.NeedsProbeForEmptyBuild && childProbeRows != bucket.ProbeRows))
	if probeConservationFailed {
		for i := range subBuckets {
			if subBuckets[i].BuildFd != nil {
				subBuckets[i].BuildFd.Close()
			}
			if subBuckets[i].ProbeFd != nil {
				subBuckets[i].ProbeFd.Close()
			}
		}
		return nil, moerr.NewInternalErrorf(
			proc.Ctx,
			"join spill probe-row conservation failed at depth %d (source=%d, children=%d, exact=%t)",
			bucket.Depth, bucket.ProbeRows, childProbeRows, e.cfg.NeedsProbeForEmptyBuild,
		)
	}
	committed = true
	metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", fmt.Sprintf("%d", bucket.Depth+1)).Inc()
	return subBuckets, nil
}

// FinishBucket closes the current bucket's probe reader.
func (e *SpillEngine) FinishBucket() {
	// Keep reader and decoded-batch reservations live with their retained
	// capacities. The next bucket replaces them transactionally; Cleanup is the
	// terminal release point.
	e.probeReader.closeCurrentFile()
}

// IsProbing reports whether a probe file is currently open.
func (e *SpillEngine) IsProbing() bool { return e.probeReader.fd != nil }

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

// scatterProbe evaluates the consuming join's probe-side keys for re-scatter.
func scatterProbe(proc *process.Process, e *SpillEngine, bat *batch.Batch, writers []BucketWriter, seed uint64, analyzer process.Analyzer) error {
	return e.scatterEvaluatedBatchWithPressure(
		proc,
		bat,
		writers,
		seed,
		true,
		analyzer,
		func(current *batch.Batch) ([]*vector.Vector, error) {
			return e.evalProbeKeys(proc, current, e.probeKeyEval)
		},
	)
}

func (e *SpillEngine) evalProbeKeys(
	proc *process.Process,
	bat *batch.Batch,
	eval func(*batch.Batch) ([]*vector.Vector, error),
) ([]*vector.Vector, error) {
	if eval == nil {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	return eval(bat)
}

func (e *SpillEngine) freeKeyExecs() {
	for _, exec := range e.keyExecs {
		if exec != nil {
			exec.Free()
		}
	}
	e.keyExecs = nil
}

func isBudgetAdmission(err error) bool {
	return hashbuild.IsRetryableMemoryCapacity(err)
}

func noProgressError(proc *process.Process, depth int) error {
	_ = proc
	return hashbuild.NewMinimumAllocationPressureError(
		"join-spill",
		fmt.Sprintf("partition-depth-%d", depth),
		nil,
	)
}

// Cleanup releases all engine resources.
func (e *SpillEngine) Cleanup(proc *process.Process) {
	for i := range e.buckets {
		if e.buckets[i].BuildFd != nil {
			_ = e.buckets[i].BuildFd.Close()
		}
		if e.buckets[i].ProbeFd != nil {
			_ = e.buckets[i].ProbeFd.Close()
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
	e.probeReader.Close()
	e.buildReader.Close()
	e.buildPool.Release(proc)
	e.probePool.Release(proc)
	e.freeKeyExecs()
	e.releaseScatterScratch()
}
