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
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	// selected vectors. bytes.Buffer growth is charged at 2x this target.
	spillWriteCoalesceSize = 64 << 10
)

// SpillBucket holds file descriptors for one spilled bucket.
type SpillBucket struct {
	BuildFd   *message.SpillFile
	ProbeFd   *message.SpillFile
	Depth     int
	BuildRows int64
	ProbeRows int64
}

// BucketReader reads serialized batch records from an fd.
type BucketReader struct {
	fd           *os.File
	reader       *bufio.Reader
	buf          [16]byte
	budget       *process.HashBuildBudgetGeneration
	reservation  *process.HashBuildReservation
	batchToken   *process.HashBuildReservation
	spillFile    *message.SpillFile
	mergeRecords bool
}

func (r *BucketReader) ReadBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
	if r.fd == nil {
		return nil, io.EOF
	}
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(r.fd, 4*1024*1024)
	}
	_, token, err := r.readBatchRecord(proc, reuseBat)
	if err != nil {
		r.releaseReadBatch(proc, reuseBat, token)
		return nil, err
	}
	oldToken := r.batchToken
	r.batchToken = token
	if oldToken != nil {
		oldToken.Release()
	}
	if !r.mergeRecords {
		return reuseBat, nil
	}
	// Merge adjacent records up to the bounded historical 8192-row payload.
	// This preserves dedup/outer-join behaviour across small source batches
	// without retaining one selected batch per bucket during scatter.
	for reuseBat.RowCount() < 8192 {
		if _, err := r.reader.Peek(16); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		next := batch.NewOffHeapWithSize(0)
		_, nextToken, err := r.readBatchRecord(proc, next)
		if err != nil {
			next.Clean(proc.Mp())
			if nextToken != nil {
				nextToken.Release()
			}
			return nil, err
		}
		var mergeToken *process.HashBuildReservation
		if r.budget != nil {
			projected, ok := addUint64(r.batchToken.Size(), nextToken.Size())
			if !ok {
				next.Clean(proc.Mp())
				nextToken.Release()
				return nil, process.ErrHashBuildBudgetInvalid
			}
			mergeToken, err = r.budget.Reserve(projected)
			if err != nil {
				next.Clean(proc.Mp())
				nextToken.Release()
				return nil, err
			}
		}
		for i := range next.Vecs {
			if err := reuseBat.Vecs[i].UnionBatch(next.Vecs[i], 0, next.RowCount(), nil, proc.Mp()); err != nil {
				next.Clean(proc.Mp())
				if nextToken != nil {
					nextToken.Release()
				}
				if mergeToken != nil {
					mergeToken.Release()
				}
				reuseBat.Clean(proc.Mp())
				if r.batchToken != nil {
					r.batchToken.Release()
					r.batchToken = nil
				}
				return nil, err
			}
		}
		reuseBat.SetRowCount(reuseBat.RowCount() + next.RowCount())
		next.Clean(proc.Mp())
		if mergeToken != nil {
			actual, ok := batchRetainedBytes(reuseBat)
			if !ok || actual > mergeToken.Size() {
				mergeToken.Release()
				nextToken.Release()
				reuseBat.Clean(proc.Mp())
				r.batchToken.Release()
				r.batchToken = nil
				return nil, process.ErrHashBuildBudgetInvalid
			}
			if _, err := mergeToken.ReconcileDown(actual); err != nil {
				mergeToken.Release()
				nextToken.Release()
				reuseBat.Clean(proc.Mp())
				r.batchToken.Release()
				r.batchToken = nil
				return nil, err
			}
			r.batchToken.Release()
			nextToken.Release()
			r.batchToken = mergeToken
		}
	}
	return reuseBat, nil
}

func addUint64(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, false
	}
	return a + b, true
}

func batchRetainedBytes(bat *batch.Batch) (uint64, bool) {
	if bat == nil || bat.RowCount() < 0 {
		return 0, false
	}
	actual := uint64(bat.Allocated())
	rows := uint64(bat.RowCount())
	cols := uint64(len(bat.Vecs))
	if cols > (math.MaxUint64-16)/8 {
		return 0, false
	}
	metadata := uint64(16) + cols*8
	if rows > 0 && metadata > math.MaxUint64/rows {
		return 0, false
	}
	return addUint64(actual, rows*metadata)
}

func (r *BucketReader) releaseReadBatch(proc *process.Process, bat *batch.Batch, token *process.HashBuildReservation) {
	if bat != nil {
		bat.Clean(proc.Mp())
	}
	if token != nil {
		token.Release()
	}
	if r.batchToken != nil {
		r.batchToken.Release()
		r.batchToken = nil
	}
}

func (r *BucketReader) readBatchRecord(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, *process.HashBuildReservation, error) {
	if _, err := io.ReadFull(r.reader, r.buf[:]); err != nil {
		if err == io.EOF {
			return nil, nil, io.EOF
		}
		return nil, nil, err
	}
	cnt := types.DecodeInt64(r.buf[:8])
	batchSize := types.DecodeInt64(r.buf[8:16])
	if cnt < 0 || batchSize < 0 {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "negative spill batch header")
	}
	var token *process.HashBuildReservation
	if r.budget != nil {
		payload := uint64(batchSize)
		if payload > (math.MaxUint64-(64<<10))/4 {
			return nil, nil, process.ErrHashBuildBudgetInvalid
		}
		projected := payload*4 + 64<<10
		var err error
		token, err = r.budget.Reserve(projected)
		if err != nil {
			return nil, nil, err
		}
	}

	reuseBat.CleanOnlyData()

	limitReader := io.LimitedReader{R: r.reader, N: batchSize}
	if err := reuseBat.UnmarshalFromReader(&limitReader, proc.Mp()); err != nil {
		return nil, token, err
	}

	// Verify the batch unmarshal consumed exactly batchSize bytes.
	if limitReader.N > 0 {
		return nil, token, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", limitReader.N)
	}

	// Read magic (8 bytes)
	if _, err := io.ReadFull(r.reader, r.buf[:8]); err != nil {
		return nil, token, err
	}
	if types.DecodeUint64(r.buf[:8]) != SpillMagic {
		return nil, token, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}

	if reuseBat.RowCount() != int(cnt) {
		return nil, token, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}
	if token != nil {
		actual, ok := batchRetainedBytes(reuseBat)
		if !ok || actual > token.Size() {
			return nil, token, process.ErrHashBuildBudgetInvalid
		}
		if _, err := token.ReconcileDown(actual); err != nil {
			return nil, token, err
		}
	}
	return reuseBat, token, nil
}

func (r *BucketReader) ResetForFd(fd *os.File) {
	r.closeCurrentFile()
	if fd == nil {
		return
	}
	r.fd = fd
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(fd, 4*1024*1024)
	} else {
		r.reader.Reset(fd)
	}
}

func (r *BucketReader) ResetForSpillFile(file *message.SpillFile) {
	r.closeCurrentFile()
	if file == nil {
		return
	}
	r.spillFile = file
	r.fd = file.File()
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(r.fd, 4*1024*1024)
	} else {
		r.reader.Reset(r.fd)
	}
}

// EnsureBuffer admits the reader's fixed backing allocation before creating
// the bufio.Reader. Rebuild and probe readers are shared one at a time, so a
// single reservation bounds their peak rather than charging one per bucket.
func (r *BucketReader) EnsureBuffer(budget *process.HashBuildBudgetGeneration) error {
	r.budget = budget
	if r.reader != nil || budget == nil {
		return nil
	}
	const size = uint64(64 << 10)
	token, err := budget.Reserve(size)
	if err != nil {
		return err
	}
	r.reservation = token
	r.reader = bufio.NewReaderSize(nil, int(size))
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
		r.fd.Close()
		r.fd = nil
	}
}

func (r *BucketReader) Close() {
	r.closeCurrentFile()
	if r.batchToken != nil {
		r.batchToken.Release()
		r.batchToken = nil
	}
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
	// A retained bufio.Reader capacity must remain charged. Drop it when the
	// reservation is released; ResetForFd/ResetForSpillFile keep both alive.
	r.reader = nil
}

// BucketWriter writes serialized batch records to an fd.
type BucketWriter struct {
	Name            string
	Fd              *os.File
	Budget          *process.HashBuildBudgetGeneration
	Rows            int64
	Bytes           uint64
	diskReservation *process.HashBuildSpillDiskReservation
	fdReservation   *process.HashBuildSpillFDReservation
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

func (w *BucketWriter) HandOffFd() *os.File {
	if w.Fd == nil {
		return nil
	}
	// A raw descriptor cannot carry accounting ownership. Budgeted writers
	// must use handOffSpillFile; retain ownership here so Close can unwind it.
	if w.fdReservation != nil || w.diskReservation != nil {
		return nil
	}
	if _, err := w.Fd.Seek(0, io.SeekStart); err != nil {
		return nil
	}
	fd := w.Fd
	w.Fd = nil
	return fd
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

// FlushBucketBatch writes bat to w, creating the spill file on first write.
// If analyzer is non-nil, spill bytes/rows are tracked.
func FlushBucketBatch(proc *process.Process, bat *batch.Batch, w *BucketWriter, bucketBuf *bytes.Buffer, analyzer process.Analyzer) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	// Serialize before creating the file. This admits marshal scratch and the
	// exact disk extent before CreateAndRemoveFile/write, so a rejected write
	// leaves both the writer and source batch intact.
	cnt := int64(bat.RowCount())
	if err := marshalSpillRecord(bat, bucketBuf); err != nil {
		return err
	}
	return writeBucketPayload(proc, bucketBuf.Bytes(), cnt, w, analyzer)
}

func marshalSpillRecord(bat *batch.Batch, buf *bytes.Buffer) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	cnt := int64(bat.RowCount())
	buf.Reset()
	buf.Write(types.EncodeInt64(&cnt))
	batchSizePos := buf.Len()
	var zero int64
	buf.Write(types.EncodeInt64(&zero))
	batchStart := buf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(buf, false); err != nil {
		return err
	}
	batchSize := int64(buf.Len() - batchStart)
	copy(buf.Bytes()[batchSizePos:batchSizePos+8], types.EncodeInt64(&batchSize))
	magic := uint64(SpillMagic)
	buf.Write(types.EncodeUint64(&magic))
	return nil
}

// writeBucketPayload performs disk/FD admission and one physical write for a
// complete sequence of framed records.
func writeBucketPayload(proc *process.Process, payload []byte, rows int64, w *BucketWriter, analyzer process.Analyzer) error {
	if w == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
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
	if w.Budget != nil {
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
	}
	if !w.Created() {
		var fdToken *process.HashBuildSpillFDReservation
		var err error
		if w.Budget != nil {
			fdToken, err = w.Budget.ReserveSpillFD(1)
			if err != nil {
				rollbackDisk()
				return err
			}
		}
		fs, err := proc.GetSpillFileService()
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

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return h ^ (val + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2))
}

// ComputeXXHash evaluates key vectors and computes XXHash64 values using
// column-at-a-time processing for better cache locality. seed initialises every
// hash slot so different spill depths produce different bucket distributions.
func ComputeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) {
	if len(hashValues) == 0 {
		return
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = seed
	}
	if len(keyVecs) == 0 {
		return
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

// classifyRows computes bucket counts, prefix offsets, and one contiguous row
// id array in two linear passes. This replaces the historical bucket-by-bucket
// scan of hashValues (which revisited every row once for each bucket).
func classifyRows(hashValues []uint64, bucketCount int, shift uint64, rowIDs []int32, counts []int32, offsets []int32) error {
	if bucketCount <= 0 || bucketCount&(bucketCount-1) != 0 || shift >= 64 || len(rowIDs) < len(hashValues) || len(counts) < bucketCount || len(offsets) < bucketCount+1 {
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
	if bucketCount <= len(writePos) {
		copy(writePos[:bucketCount], offsets[:bucketCount])
		for row, hash := range hashValues {
			bucket := int((hash >> shift) & mask)
			pos := writePos[bucket]
			rowIDs[pos] = int32(row)
			writePos[bucket] = pos + 1
		}
		return nil
	}
	// SpillNumBuckets is the production fanout. Keep the helper correct for
	// callers using another power-of-two fanout without allocating a second
	// row-id structure.
	positions := make([]int32, bucketCount)
	copy(positions, offsets[:bucketCount])
	for row, hash := range hashValues {
		bucket := int((hash >> shift) & mask)
		pos := positions[bucket]
		rowIDs[pos] = int32(row)
		positions[bucket] = pos + 1
	}
	return nil
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
	ComputeXXHash(keyVecs, hashValues, seed)

	if len(writers) == 0 || len(writers)&(len(writers)-1) != 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	// Build one contiguous row-id array, then expose each bucket as a slice of
	// that array for compatibility with the buffered path.
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
	var rowIDs []int32
	if len(bucketRowIds) > 0 && cap(bucketRowIds[0]) >= rowCount {
		rowIDs = bucketRowIds[0][:rowCount]
	} else {
		rowIDs = make([]int32, rowCount)
	}
	var countsFixed [SpillNumBuckets]int32
	var offsetsFixed [SpillNumBuckets + 1]int32
	counts := countsFixed[:len(writers)]
	offsets := offsetsFixed[:len(writers)+1]
	if len(writers) > SpillNumBuckets {
		counts = make([]int32, len(writers))
		offsets = make([]int32, len(writers)+1)
	}
	if err := classifyRows(hashValues, len(writers), 0, rowIDs, counts, offsets); err != nil {
		return err
	}
	for i := range bucketRowIds {
		bucketRowIds[i] = rowIDs[offsets[i]:offsets[i+1]]
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
	partitionLevel uint64,
	analyzer process.Analyzer,
) error {
	return e.scatterBatchBounded(proc, bat, keyVecs, writers, partitionLevel, analyzer)
}

// scatterBatchBounded writes one bucket at a time. The historical path kept
// SpillNumBuckets selected batches alive for the full input stream; that made
// a repartition pass itself exceed the hash-build budget. This implementation
// keeps one selected batch and one row-id slice, flushing it before advancing
// to the next bucket.
func (e *SpillEngine) scatterBatchBounded(
	proc *process.Process,
	bat *batch.Batch,
	keyVecs []*vector.Vector,
	writers []BucketWriter,
	partitionLevel uint64,
	analyzer process.Analyzer,
) (retErr error) {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(writers) == 0 || len(writers) > SpillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	rows := bat.RowCount()
	var selected *batch.Batch
	defer func() {
		if selected != nil {
			selected.Clean(proc.Mp())
			selected = nil
		}
		reconcileErr := e.reconcileScatterScratch()
		if reconcileErr != nil && retErr == nil {
			retErr = reconcileErr
		}
		if retErr != nil {
			e.discardScatterBuffers()
		}
	}()
	if e.cfg.Budget != nil {
		// Hash values, row IDs, a single selected batch, and marshal growth all
		// coexist briefly. Charge a conservative bound before any allocation.
		allocated := uint64(bat.Allocated())
		if size := uint64(bat.Size()); size > allocated {
			allocated = size
		}
		rowCount := uint64(rows)
		if rowCount > math.MaxUint64/96 {
			return process.ErrHashBuildBudgetInvalid
		}
		rowScratch := rowCount * 96
		const marshalSlack = uint64(64 << 10)
		if rowScratch > math.MaxUint64-marshalSlack ||
			allocated > (math.MaxUint64-rowScratch-marshalSlack)/3 {
			return process.ErrHashBuildBudgetInvalid
		}
		// Worst case keeps the source, a full selected varlen copy, and the
		// serialized bytes.Buffer alive together. Use 3x source so capacity
		// growth and varlen area remain admitted before Union/Marshal.
		need := allocated*3 + rowScratch + marshalSlack
		var err error
		if e.scatterScratchReservation == nil {
			e.scatterScratchReservation, err = e.cfg.Budget.Reserve(need)
		} else {
			// The live token already charges retained hash/row-id/coalesce
			// capacities. A new source batch and its selected/marshal peak must
			// be admitted on top of that retained charge, even when the token's
			// current size is larger than this batch's standalone estimate.
			err = e.scatterScratchReservation.Grow(need)
		}
		if err != nil {
			return err
		}
	}

	if cap(e.scatterHashValues) < rows {
		e.scatterHashValues = make([]uint64, rows)
	}
	hashValues := e.scatterHashValues[:rows]
	// Re-spill must consume fresh hash bits. Merely changing the initial seed
	// leaves the low five bits correlated with the parent partition and can put
	// every parent row into one child, making repartition unable to progress.
	// Level zero uses bits 0..4, level one bits 5..9, and so on.
	ComputeXXHash(keyVecs, hashValues, 0)
	shift := partitionLevel * 5
	if shift >= 64 {
		return process.ErrHashBuildBudgetInvalid
	}
	if cap(e.scatterBucketRowIds) < rows {
		e.scatterBucketRowIds = make([]int32, rows)
	}
	if cap(e.keyVecs) < len(keyVecs) {
		e.keyVecs = make([]*vector.Vector, len(keyVecs))
	}
	if err := classifyRows(hashValues, len(writers), shift, e.scatterBucketRowIds, e.scatterBucketCounts[:], e.scatterBucketOffsets[:]); err != nil {
		return err
	}
	for bucketID := range writers {
		start, end := e.scatterBucketOffsets[bucketID], e.scatterBucketOffsets[bucketID+1]
		if start == end || writers[bucketID].Name == "" {
			continue
		}
		sels := e.scatterBucketRowIds[start:end]
		if selected == nil {
			selected = batch.NewOffHeapWithSize(len(bat.Vecs))
			for j, vec := range bat.Vecs {
				selected.Vecs[j] = vector.NewOffHeapVecWithType(*vec.GetType())
			}
		}
		selected.CleanOnlyData()
		for j, vec := range bat.Vecs {
			if err := selected.Vecs[j].UnionInt32(vec, sels, proc.Mp()); err != nil {
				selected.CleanOnlyData()
				return err
			}
		}
		selected.SetRowCount(len(sels))
		if err := e.appendScatterRecord(proc, selected, &writers[bucketID], bucketID, analyzer); err != nil {
			selected.CleanOnlyData()
			return err
		}
	}
	return nil
}

func (e *SpillEngine) appendScatterRecord(proc *process.Process, bat *batch.Batch, writer *BucketWriter, bucket int, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= SpillNumBuckets || writer == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	cnt := int64(bat.RowCount())
	if err := marshalSpillRecord(bat, &e.scatterWriteBuf); err != nil {
		return err
	}
	payload := e.scatterWriteBuf.Bytes()
	buf := &e.scatterWriteBuffers[bucket]
	if buf.Len() > 0 && buf.Len()+len(payload) > spillWriteCoalesceSize {
		if err := e.flushPendingScatterBucket(proc, writer, bucket, analyzer); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return writeBucketPayload(proc, payload, cnt, writer, analyzer)
	}
	if buf.Len() == 0 {
		if !e.ensureScatterCoalesceCapacity(buf) {
			return writeBucketPayload(proc, payload, cnt, writer, analyzer)
		}
		buf.Grow(spillWriteCoalesceSize)
	}
	_, _ = buf.Write(payload)
	e.scatterWriteRows[bucket] += cnt
	if buf.Len() >= spillWriteCoalesceSize {
		return e.flushPendingScatterBucket(proc, writer, bucket, analyzer)
	}
	return nil
}

func (e *SpillEngine) ensureScatterCoalesceCapacity(buf *bytes.Buffer) bool {
	if buf == nil || buf.Cap() >= spillWriteCoalesceSize {
		return true
	}
	if e.cfg.Budget == nil || e.scatterScratchReservation == nil {
		return e.cfg.Budget == nil
	}
	additional := uint64(spillWriteCoalesceSize-buf.Cap()) * 2
	if err := e.scatterScratchReservation.Grow(additional); err != nil {
		return false
	}
	return true
}

func (e *SpillEngine) flushPendingScatterBucket(proc *process.Process, writer *BucketWriter, bucket int, analyzer process.Analyzer) error {
	if bucket < 0 || bucket >= SpillNumBuckets || writer == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	buf := &e.scatterWriteBuffers[bucket]
	if buf.Len() == 0 {
		return nil
	}
	err := writeBucketPayload(proc, buf.Bytes(), e.scatterWriteRows[bucket], writer, analyzer)
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
		if e.scatterWriteBuffers[bucket].Len() == 0 {
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
	for bucket := range e.scatterWriteBuffers {
		e.scatterWriteBuffers[bucket].Reset()
		e.scatterWriteRows[bucket] = 0
	}
}

// releaseScatterScratch terminates one scatter phase. Scatter capacities are
// reusable while a phase consumes multiple source batches, but must not stay
// charged while the next child hashmap is rebuilt. Cleanup calls this method
// as an idempotent fallback for cancellation paths.
func (e *SpillEngine) releaseScatterScratch() {
	e.scatterHashValues = nil
	e.scatterBucketRowIds = nil
	e.keyVecs = nil
	e.scatterWriteBuf = bytes.Buffer{}
	for i := range e.scatterBucketCounts {
		e.scatterBucketCounts[i] = 0
	}
	for i := range e.scatterBucketOffsets {
		e.scatterBucketOffsets[i] = 0
	}
	for i := range e.scatterWriteBuffers {
		e.scatterWriteBuffers[i] = bytes.Buffer{}
		e.scatterWriteRows[i] = 0
	}
	if e.scatterScratchReservation != nil {
		e.scatterScratchReservation.Release()
		e.scatterScratchReservation = nil
	}
}

// reconcileScatterScratch leaves only the capacities retained by the engine
// charged after a batch completes. The source batch, selected vectors, and
// marshal buffer are transient and must not pin budget across the queue.
func (e *SpillEngine) reconcileScatterScratch() error {
	if e.scatterScratchReservation == nil {
		return nil
	}
	actual := uint64(0)
	add := func(v uint64) bool {
		if actual > math.MaxUint64-v {
			return false
		}
		actual += v
		return true
	}
	mul := func(v, n uint64) (uint64, bool) {
		if n != 0 && v > math.MaxUint64/n {
			return 0, false
		}
		return v * n, true
	}
	hashBytes, hashOK := mul(uint64(cap(e.scatterHashValues)), 8)
	rowIDBytes, rowIDOK := mul(uint64(cap(e.scatterBucketRowIds)), 4)
	keyBytes, keyOK := mul(uint64(cap(e.keyVecs)), 8)
	countBytes, countOK := mul(uint64(len(e.scatterBucketCounts)), 4)
	offsetBytes, offsetOK := mul(uint64(len(e.scatterBucketOffsets)), 4)
	if !hashOK || !rowIDOK || !keyOK || !countOK || !offsetOK ||
		!add(hashBytes) || !add(rowIDBytes) || !add(keyBytes) || !add(countBytes) || !add(offsetBytes) ||
		!add(uint64(e.scatterWriteBuf.Cap())) {
		return process.ErrHashBuildBudgetInvalid
	}
	for i := range e.scatterWriteBuffers {
		if !add(uint64(e.scatterWriteBuffers[i].Cap())) {
			return process.ErrHashBuildBudgetInvalid
		}
	}
	reserved := e.scatterScratchReservation.Size()
	if actual > reserved {
		return process.ErrHashBuildBudgetInvalid
	}
	if actual < reserved {
		if _, err := e.scatterScratchReservation.ReconcileDown(actual); err != nil {
			return err
		}
	}
	return nil
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
	ProbeKeyExprs           []*plan.Expr // bounded probe keys; expressions are rejected before Eval
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
	cfg     SpillEngineConfig
	buckets []SpillBucket

	// Current bucket state
	buildReader    BucketReader
	probeReader    BucketReader
	buildReadBatch *batch.Batch
	probeReadBatch *batch.Batch

	// Reusable scatter state
	buildPool ReusableBufferPool
	probePool ReusableBufferPool

	// Cached key executors for re-spill
	keyExecs []colexec.ExpressionExecutor
	keyVecs  []*vector.Vector

	// Reusable scatter buffers to avoid per-batch allocations.
	scatterHashValues    []uint64
	scatterBucketRowIds  []int32
	scatterBucketCounts  [SpillNumBuckets]int32
	scatterBucketOffsets [SpillNumBuckets + 1]int32
	scatterWriteBuf      bytes.Buffer
	scatterWriteBuffers  [SpillNumBuckets]bytes.Buffer
	scatterWriteRows     [SpillNumBuckets]int64
	// The lease follows the reusable scratch capacities for the engine
	// lifetime. It is released only by Cleanup, after all backing arrays have
	// been dropped.
	scatterScratchReservation *process.HashBuildReservation

	// probeKeyEval evaluates probe-side key expressions for probe re-scatter.
	// Stored from ScatterProbeTable. Must use probe-side conditions
	// (EqConds[0]), not build-side (EqConds[1]) to ensure correct bucket assignment.
	probeKeyEval func(*batch.Batch) ([]*vector.Vector, error)
}

// NewSpillEngine creates an engine from configuration. Call InitFromSpilledMap next.
func NewSpillEngine(cfg SpillEngineConfig) *SpillEngine {
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = SpillNumBuckets * SpillNumBuckets
	}
	return &SpillEngine{cfg: cfg}
}

// InitFromSpilledMap creates SpillBucket entries from build FDs.
// Empty (nil) FDs become placeholder buckets for outer-join semantics.
func (e *SpillEngine) InitFromSpilledMap(buildFds []*os.File) {
	e.buckets = make([]SpillBucket, 0, len(buildFds))
	for _, fd := range buildFds {
		var file *message.SpillFile
		if fd != nil {
			file = message.NewSpillFile(fd, 0, 0, nil)
		}
		e.buckets = append(e.buckets, SpillBucket{
			BuildFd: file,
			Depth:   1,
		})
	}
}

// InitFromSpilledFiles is the ownership-preserving counterpart of the legacy
// descriptor initializer. Each SpillFile remains the sole owner of its fd and
// reservations while it moves through the bucket queue.
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
	if err := validateSimpleSpillKeys(e.cfg.ProbeKeyExprs); err != nil {
		return err
	}
	e.probeKeyEval = evalKeysFn
	writers := MakeBucketWriters("probe")
	for i := range writers {
		writers[i].Budget = e.cfg.Budget
	}

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
		bat, err := children()
		if err != nil {
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
		keyVecs, err := evalKeysFn(bat)
		if err != nil {
			return err
		}
		if err := e.scatterBatch(proc, bat, keyVecs, writers, nil, 0, analyzer); err != nil {
			return err
		}
	}

	// Flush remaining buffers and hand off FDs transactionally. A failed rewind
	// must not publish an EOF-positioned file or orphan earlier handoffs.
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
	if e.probeReader.fd == nil {
		return nil, nil
	}
	if e.probeReadBatch == nil {
		e.probeReadBatch = batch.NewOffHeapWithSize(0)
	}
	e.probeReader.mergeRecords = e.cfg.MergeProbeBatches || e.cfg.IsDedup
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
			if err := e.probeReader.EnsureBuffer(e.cfg.Budget); err != nil {
				bucket.ProbeFd.Close()
				return nil, BucketSkip, err
			}
			e.probeReader.ResetForSpillFile(bucket.ProbeFd)
			bucket.ProbeFd = nil
			return nil, BucketEmptyBuild, nil
		}
		if bucket.ProbeFd != nil {
			bucket.ProbeFd.Close()
		}
		return nil, BucketSkip, nil
	}

	builder := &hashbuild.HashmapBuilder{}
	builder.SetBudget(e.cfg.Budget)
	builder.IsDedup = e.cfg.IsDedup
	builder.OnDuplicateAction = e.cfg.OnDuplicateAction
	builder.DedupBuildKeepLast = e.cfg.DedupBuildKeepLast
	builder.DedupColName = e.cfg.DedupColName
	builder.DedupColTypes = e.cfg.DedupColTypes
	if err := builder.Prepare(e.cfg.BuildKeyExprs, e.cfg.DelColIdx, e.cfg.DedupDeleteMarkerColIdx, e.cfg.DedupDeleteKeepColIdxList, proc); err != nil {
		builder.Free(proc)
		return nil, BucketSkip, err
	}

	if err := e.buildReader.EnsureBuffer(e.cfg.Budget); err != nil {
		builder.Free(proc)
		bucket.BuildFd.Close()
		bucket.BuildFd = nil
		return nil, BucketSkip, err
	}
	e.buildReader.ResetForSpillFile(bucket.BuildFd)
	e.buckets[0].BuildFd = nil // prevent Cleanup double-close on error
	defer e.buildReader.closeCurrentFile()
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
		if err := builder.CopyBuildBatch(bat, proc); err != nil {
			if isBudgetAdmission(err) && bucket.Depth < SpillMaxPass {
				subBuckets, spillErr := e.reSpillBucket(proc, analyzer, bucket, builder, &e.buildReader, bat)
				builder.FreeHashMapAndBatches(proc)
				builder.Free(proc)
				if spillErr != nil {
					return nil, BucketSkip, spillErr
				}
				e.buckets = append(subBuckets, e.buckets[1:]...)
				return nil, BucketReSpilled, nil
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

	if err := builder.BuildHashmap(e.cfg.HashOnPK, e.cfg.NeedAllocateSels, false, proc); err != nil {
		if isBudgetAdmission(err) && bucket.Depth < SpillMaxPass {
			// Release the rejected/partial map admission while retaining the
			// original copied batches for transactional re-spill.
			builder.FreeHashMapOnly(proc)
			subBuckets, spillErr := e.reSpillBucket(proc, analyzer, bucket, builder, &e.buildReader, nil)
			builder.FreeHashMapAndBatches(proc)
			builder.Free(proc)
			if spillErr != nil {
				return nil, BucketSkip, spillErr
			}
			e.buckets = append(subBuckets, e.buckets[1:]...)
			return nil, BucketReSpilled, nil
		}
		builder.FreeHashMapAndBatches(proc)
		builder.Free(proc)
		if isBudgetAdmission(err) {
			return nil, BucketSkip, noProgressError(proc, bucket.Depth)
		}
		return nil, BucketSkip, err
	}
	if !e.cfg.NeedBatches {
		builder.Batches.Clean(proc.Mp())
	}

	jm := builder.GetJoinMap(proc.Mp())
	if jm == nil {
		e.buckets[0].ProbeFd = nil // transferred to reader below; prevent Cleanup double-close
		e.buckets = e.buckets[1:]
		if e.cfg.NeedsProbeForEmptyBuild && bucket.ProbeFd != nil {
			if err := e.probeReader.EnsureBuffer(e.cfg.Budget); err != nil {
				bucket.ProbeFd.Close()
				return nil, BucketSkip, err
			}
			e.probeReader.ResetForSpillFile(bucket.ProbeFd)
			bucket.ProbeFd = nil
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
	builder.FreeTemporaryVectors(proc)
	builder.FreeExecutors()

	// Pop the head bucket and open probe reader.
	e.buckets = e.buckets[1:]
	if bucket.ProbeFd != nil {
		if err := e.probeReader.EnsureBuffer(e.cfg.Budget); err != nil {
			bucket.ProbeFd.Close()
			jm.Free()
			return nil, BucketSkip, err
		}
		e.probeReader.ResetForSpillFile(bucket.ProbeFd)
		bucket.ProbeFd = nil
	}
	return jm, BucketReady, nil
}

func (e *SpillEngine) reSpillBucket(proc *process.Process, analyzer process.Analyzer, bucket SpillBucket, builder *hashbuild.HashmapBuilder, reader *BucketReader, pending *batch.Batch) ([]SpillBucket, error) {
	if err := validateSimpleSpillKeys(e.cfg.BuildKeyExprs); err != nil {
		return nil, err
	}
	buildWriters := MakeBucketWriters("build_sub")
	for i := range buildWriters {
		buildWriters[i].Budget = e.cfg.Budget
	}
	probeWriters := MakeBucketWriters("probe_sub")
	for i := range probeWriters {
		probeWriters[i].Budget = e.cfg.Budget
	}
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
		execs, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, e.cfg.BuildKeyExprs)
		if err != nil {
			return nil, err
		}
		e.freeKeyExecs()
		e.keyExecs = execs
	}

	// evalAndScatter builds key vectors using the given executors and scatters.
	evalAndScatter := func(bat *batch.Batch, writers []BucketWriter, buffers []*batch.Batch, execs []colexec.ExpressionExecutor) error {
		if cap(e.keyVecs) < len(execs) {
			e.keyVecs = make([]*vector.Vector, len(execs))
		}
		keyVecs := e.keyVecs[:len(execs)]
		defer func() {
			for i := range keyVecs {
				keyVecs[i] = nil
			}
		}()
		for i, exec := range execs {
			vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
			if err != nil {
				return err
			}
			keyVecs[i] = vec
		}
		return e.scatterBatch(proc, bat, keyVecs, writers, nil, partitionLevel, analyzer)
	}

	// Detach accumulated batches so each can be released immediately after it
	// has been copied into the sub-bucket buffers.
	buildBatches := builder.Batches.Buf
	builder.Batches.Buf = nil
	builder.Batches.MemSize = 0
	defer func() {
		for _, b := range buildBatches {
			if b != nil {
				b.Clean(proc.Mp())
			}
		}
	}()
	var buildRows int64
	for i, b := range buildBatches {
		if b != nil {
			buildRows += int64(b.RowCount())
		}
		if err := evalAndScatter(b, buildWriters, nil, e.keyExecs); err != nil {
			return nil, err
		}
		b.Clean(proc.Mp())
		buildBatches[i] = nil
	}
	if pending != nil && pending.RowCount() > 0 {
		if err := evalAndScatter(pending, buildWriters, nil, e.keyExecs); err != nil {
			return nil, err
		}
	}

	if pending != nil {
		buildRows += int64(pending.RowCount())
	}
	for {
		bat, err := reader.ReadBatch(proc, e.buildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		buildRows += int64(bat.RowCount())
		if err := evalAndScatter(bat, buildWriters, nil, e.keyExecs); err != nil {
			return nil, err
		}
	}
	if err := e.flushScatterBuffers(proc, buildWriters, analyzer); err != nil {
		return nil, err
	}

	if e.probeReadBatch == nil {
		e.probeReadBatch = batch.NewOffHeapWithSize(0)
	}

	// Scatter probe file. Reuse reader's 4 MiB buffer from the build pass.
	if bucket.ProbeFd != nil {
		if err := reader.EnsureBuffer(e.cfg.Budget); err != nil {
			return nil, err
		}
		reader.ResetForSpillFile(bucket.ProbeFd)
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
			bat, err := reader.ReadBatch(proc, e.probeReadBatch)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if err := scatterProbe(proc, e, bat, probeWriters, nil, partitionLevel, analyzer); err != nil {
				return nil, err
			}
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
					Kind:    process.HashBuildBudgetErrorAdmission,
					Message: fmt.Sprintf("spill queue limit exceeded: %s", process.ErrHashBuildBudgetAdmission),
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
	if childBuildRows != buildRows || len(subBuckets) == 0 {
		for i := range subBuckets {
			if subBuckets[i].BuildFd != nil {
				subBuckets[i].BuildFd.Close()
			}
			if subBuckets[i].ProbeFd != nil {
				subBuckets[i].ProbeFd.Close()
			}
		}
		return nil, noProgressError(proc, bucket.Depth)
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
		return nil, noProgressError(proc, bucket.Depth)
	}
	committed = true
	metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", fmt.Sprintf("%d", bucket.Depth+1)).Inc()
	return subBuckets, nil
}

func validateSimpleSpillKeys(exprs []*plan.Expr) error {
	for _, expr := range exprs {
		if expr == nil {
			return &process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid, Message: "nil join spill key"}
		}
		if _, ok := expr.Expr.(*plan.Expr_Col); !ok {
			return &process.HashBuildBudgetError{
				Kind:    process.HashBuildBudgetErrorInvalid,
				Message: "join spill expression key has no bounded memory estimator",
			}
		}
	}
	return nil
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

func isBudgetAdmission(err error) bool {
	return err != nil && (errors.Is(err, process.ErrHashBuildBudgetAdmission) ||
		errors.Is(err, process.ErrHashBuildBudgetClosed) ||
		errors.Is(err, process.ErrHashBuildBudgetRejected))
}

func noProgressError(proc *process.Process, depth int) error {
	_ = proc
	return &process.HashBuildBudgetError{
		Kind:    process.HashBuildBudgetErrorAdmission,
		Message: fmt.Sprintf("join spill cannot make progress at depth %d: %s", depth, process.ErrHashBuildBudgetAdmission),
	}
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
