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
	// Keep enough decoded-batch headroom to reuse the reservation for ordinary
	// spill records without retaining the pre-admission unmarshal estimate
	// for a large record until the reader closes. The additive bound makes the
	// long-lived charge independent of the largest serialized payload seen.
	decodedBatchLeaseSlack = 1 << 20
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

// BucketReader reads serialized batch records from an fd.
type BucketReader struct {
	fd           *os.File
	reader       *bufio.Reader
	buf          [16]byte
	budget       *process.HashBuildBudgetGeneration
	reservation  *process.HashBuildReservation
	batchToken   *process.HashBuildReservation
	batchCharge  uint64
	spillFile    *message.SpillFile
	mergeRecords bool
	allocation   *SpillAllocationAccount
}

func (r *BucketReader) ReadBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
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
	if r.allocation != nil {
		if err := reuseBat.SetAllocationAccount(
			r.allocation.decoded,
		); err != nil {
			return nil, err
		}
	}
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(r.fd, 4*1024*1024)
	}
	_, token, charge, err := r.readBatchRecord(proc, reuseBat, r.batchToken, r.batchCharge, true)
	if err != nil {
		r.releaseReadBatch(proc, reuseBat, token)
		return nil, err
	}
	oldToken := r.batchToken
	r.batchToken = token
	r.batchCharge = charge
	if oldToken != nil && oldToken != token {
		oldToken.Release()
	}
	if !r.mergeRecords {
		if err := checkSpillCanceled(proc); err != nil {
			r.releaseReadBatch(proc, reuseBat, nil)
			return nil, err
		}
		return reuseBat, nil
	}
	// Merge adjacent records up to the bounded historical batch payload.
	// This preserves dedup/outer-join behaviour across small source batches
	// without retaining one selected batch per bucket during scatter.
	for reuseBat.RowCount() < colexec.DefaultBatchSize {
		if err := checkSpillCanceled(proc); err != nil {
			return nil, r.mergeReadError(proc, reuseBat, nil, nil, err)
		}
		header, err := r.reader.Peek(16)
		if err != nil {
			if err == io.EOF && len(header) == 0 {
				break
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, r.mergeReadError(proc, reuseBat, nil, nil, err)
		}
		if err := checkSpillCanceled(proc); err != nil {
			return nil, r.mergeReadError(proc, reuseBat, nil, nil, err)
		}
		nextRows := types.DecodeInt64(header[:8])
		nextBatchSize := types.DecodeInt64(header[8:16])
		if nextRows < 0 || nextBatchSize < 0 {
			return nil, r.mergeReadError(
				proc,
				reuseBat,
				nil,
				nil,
				moerr.NewInternalError(proc.Ctx, "negative spill batch header"),
			)
		}
		// A source record is an indivisible ownership and budget unit. Leave it
		// for the next ReadBatch rather than consuming it and growing this batch
		// beyond the advertised merge bound.
		if nextRows > int64(colexec.DefaultBatchSize-reuseBat.RowCount()) {
			break
		}
		var selection *vector.AllocationAccountSelection
		if r.allocation != nil {
			selection = r.allocation.decoded
		}
		next, err := newSpillBatch(0, selection)
		if err != nil {
			return nil, r.mergeReadError(
				proc,
				reuseBat,
				nil,
				nil,
				err,
			)
		}
		_, nextToken, _, err := r.readBatchRecord(proc, next, nil, 0, false)
		if err != nil {
			return nil, r.mergeReadError(proc, reuseBat, next, nextToken, err)
		}
		if err := checkSpillCanceled(proc); err != nil {
			return nil, r.mergeReadError(proc, reuseBat, next, nextToken, err)
		}
		var mergeToken *process.HashBuildReservation
		if r.budget != nil {
			// Keep the current destination (O) and the source record (N) live
			// while admitting the final destination (D). UnionBatch may retain
			// rounded capacities larger than O+N, so reserving O+N here is not a
			// safe admission bound.
			predicted, ok := predictMergedRetainedBytes(reuseBat, next)
			if !ok {
				return nil, r.mergeReadError(proc, reuseBat, next, nextToken, process.ErrHashBuildBudgetInvalid)
			}
			mergeToken, err = r.budget.Reserve(predicted)
			if err != nil {
				return nil, r.mergeReadError(proc, reuseBat, next, nextToken, err)
			}
		}
		if len(reuseBat.Vecs) != len(next.Vecs) {
			return nil, r.mergeReadError(proc, reuseBat, next, nextToken, process.ErrHashBuildBudgetInvalid, mergeToken)
		}
		for i := range next.Vecs {
			if err := reuseBat.Vecs[i].UnionBatch(next.Vecs[i], 0, next.RowCount(), nil, proc.Mp()); err != nil {
				return nil, r.mergeReadError(proc, reuseBat, next, nextToken, err, mergeToken)
			}
		}
		reuseBat.SetRowCount(reuseBat.RowCount() + next.RowCount())
		next.Clean(proc.Mp())
		if mergeToken != nil {
			actual, ok := batchRetainedBytes(reuseBat)
			if !ok {
				return nil, r.mergeReadError(proc, reuseBat, nil, nextToken, process.ErrHashBuildBudgetInvalid, mergeToken)
			}
			if err := reconcileReadReservation(mergeToken, actual); err != nil {
				return nil, r.mergeReadError(proc, reuseBat, nil, nextToken, err, mergeToken)
			}
			if r.batchToken != nil {
				r.batchToken.Release()
				r.batchToken = nil
				r.batchCharge = 0
			}
			if nextToken != nil {
				nextToken.Release()
			}
			r.batchToken = mergeToken
			r.batchCharge = actual
		}
	}
	if err := checkSpillCanceled(proc); err != nil {
		return nil, r.mergeReadError(proc, reuseBat, nil, nil, err)
	}
	return reuseBat, nil
}

// mergeReadError unwinds all ownership acquired while appending a source
// record. The destination may have been partially mutated by UnionBatch, so it
// is cleaned as well. Reservations are exactly-once tokens; releasing an
// already released token is harmless and keeps every error path symmetric.
func (r *BucketReader) mergeReadError(proc *process.Process, dst, src *batch.Batch, srcToken *process.HashBuildReservation, err error, extra ...*process.HashBuildReservation) error {
	if src != nil {
		src.Clean(proc.Mp())
	}
	if srcToken != nil {
		srcToken.Release()
	}
	for _, token := range extra {
		if token != nil {
			token.Release()
		}
	}
	if dst != nil {
		dst.Clean(proc.Mp())
	}
	if r.batchToken != nil {
		r.batchToken.Release()
		r.batchToken = nil
		r.batchCharge = 0
	}
	return err
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
	metadata, ok := batchRetainedMetadataBytes(uint64(bat.RowCount()), uint64(len(bat.Vecs)))
	if !ok {
		return 0, false
	}
	return addUint64(actual, metadata)
}

func batchRetainedMetadataBytes(rows, cols uint64) (uint64, bool) {
	if cols > (math.MaxUint64-16)/8 {
		return 0, false
	}
	metadata := uint64(16) + cols*8
	if rows > 0 && metadata > math.MaxUint64/rows {
		return 0, false
	}
	return rows * metadata, true
}

// reconcileReadReservation shrinks a conservative read reservation to the
// retained batch size. ReconcileDown already validates that actual does not
// exceed the reservation, so callers do not need a separate Size call (and a
// second acquisition of the shared hash-build budget mutex).
func reconcileReadReservation(token *process.HashBuildReservation, actual uint64) error {
	if token == nil {
		return nil
	}
	if _, err := token.ReconcileDown(actual); err != nil {
		if errors.Is(err, process.ErrHashBuildReservationUpward) {
			return process.ErrHashBuildBudgetInvalid
		}
		return err
	}
	return nil
}

// predictMergedRetainedBytes computes the retained upper bound after the exact
// full-record UnionBatch append used by ReadBatch. It mirrors Vector.extend's
// data-cap growth and UnionBatch's varlen fast path (which appends a complete
// non-const source area in one operation). No destination mutation is performed.
func predictMergedRetainedBytes(dst, src *batch.Batch) (uint64, bool) {
	if dst == nil || src == nil || dst.RowCount() < 0 || src.RowCount() < 0 || len(dst.Vecs) != len(src.Vecs) {
		return 0, false
	}
	oldRows, ok := intToUint64(dst.RowCount())
	if !ok {
		return 0, false
	}
	srcRows, ok := intToUint64(src.RowCount())
	if !ok {
		return 0, false
	}
	mergedRows, ok := addUint64(oldRows, srcRows)
	if !ok || mergedRows > uint64(maxIntValue()) {
		return 0, false
	}

	var allocated uint64
	for i := range dst.Vecs {
		dv, sv := dst.Vecs[i], src.Vecs[i]
		if dv == nil || sv == nil || *dv.GetType() != *sv.GetType() || dv.Length() != dst.RowCount() || sv.Length() != src.RowCount() {
			return 0, false
		}
		typeSize := dv.GetType().TypeSize()
		if typeSize < 0 {
			return 0, false
		}
		dataRequired, ok := mulUint64(mergedRows, uint64(typeSize))
		if !ok || dataRequired > uint64(math.MaxInt64) {
			return 0, false
		}
		dataCap, ok := predictedCapacity(cap(dv.GetData()), dataRequired)
		if !ok {
			return 0, false
		}
		if allocated, ok = addUint64(allocated, dataCap); !ok {
			return 0, false
		}

		if !dv.GetType().IsVarlen() {
			continue
		}
		areaAdd, ok := mergedVarlenAreaAdd(sv, srcRows)
		if !ok {
			return 0, false
		}
		areaRequired, ok := addUint64(uint64(len(dv.GetArea())), areaAdd)
		if !ok || areaRequired > uint64(math.MaxInt64) {
			return 0, false
		}
		areaCap, ok := predictedCapacity(cap(dv.GetArea()), areaRequired)
		if !ok {
			return 0, false
		}
		if allocated, ok = addUint64(allocated, areaCap); !ok {
			return 0, false
		}
	}

	cols := uint64(len(dst.Vecs))
	if cols > (math.MaxUint64-16)/8 {
		return 0, false
	}
	metadata := 16 + cols*8
	rowMetadata, ok := mulUint64(mergedRows, metadata)
	if !ok {
		return 0, false
	}
	return addUint64(allocated, rowMetadata)
}

func mergedVarlenAreaAdd(src *vector.Vector, rows uint64) (uint64, bool) {
	if src == nil || !src.GetType().IsVarlen() {
		return 0, false
	}
	if rows == 0 {
		return 0, true
	}
	if src.IsConst() {
		if src.IsConstNull() {
			return 0, true
		}
		// UnionBatch materializes one const value and broadcasts its header. An
		// inline value needs no area; a non-inline value appends exactly once.
		if len(src.GetData()) < src.GetType().TypeSize() {
			return 0, false
		}
		values := vector.MustFixedColNoTypeCheck[types.Varlena](src)
		if len(values) != 1 {
			return 0, false
		}
		value := &values[0]
		if value.IsSmall() {
			return 0, true
		}
		off, length := value.OffsetLen()
		end, ok := addUint64(uint64(off), uint64(length))
		if !ok || end > uint64(len(src.GetArea())) {
			return 0, false
		}
		return uint64(length), true
	}

	// The full-record fast path copies the complete source area once, including
	// stale bytes. Header validation remains UnionBatch's responsibility; avoid
	// adding another per-row scan on the spill rebuild hot path.
	return uint64(len(src.GetArea())), true
}

func predictedCapacity(oldCap int, required uint64) (uint64, bool) {
	if oldCap < 0 || uint64(oldCap) > uint64(math.MaxInt64) || required > uint64(math.MaxInt64) {
		return 0, false
	}
	if required <= uint64(oldCap) {
		return uint64(oldCap), true
	}
	cap, ok := mpool.GrowCapacity(int64(oldCap), int64(required))
	if !ok || cap < 0 {
		return 0, false
	}
	return uint64(cap), true
}

func intToUint64(v int) (uint64, bool) {
	if v < 0 {
		return 0, false
	}
	return uint64(v), true
}

func mulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func batchPayloadWithAllocationSlack(payload, columns uint64) (uint64, bool) {
	const perVectorAllocationSlack = uint64(64 << 10)
	if columns >= math.MaxUint64/perVectorAllocationSlack {
		return 0, false
	}
	allocationSlack := (columns + 1) * perVectorAllocationSlack
	if payload > math.MaxUint64-allocationSlack {
		return 0, false
	}
	return payload + allocationSlack, true
}

func decodedBatchProjectedBytes(payload uint64, rows int64, columns int32) (uint64, bool) {
	if rows < 0 || columns < 0 {
		return 0, false
	}
	projected, ok := batchPayloadWithAllocationSlack(payload, uint64(columns))
	if !ok {
		return 0, false
	}
	metadata, ok := batchRetainedMetadataBytes(uint64(rows), uint64(columns))
	if !ok {
		return 0, false
	}
	return addUint64(projected, metadata)
}

func decodedBatchReusePeakBytes(retained, projected, payload uint64) (uint64, bool) {
	// For large buffers mpool.Grow follows Go's 1.25x growth policy. The old
	// allocation remains live until the replacement is allocated and copied.
	// Small-buffer doubling is bounded by the per-vector slack already included
	// in projected.
	growthSlack := payload / 4
	if payload%4 != 0 {
		growthSlack++
	}
	newAllocation, ok := addUint64(projected, growthSlack)
	if !ok {
		return 0, false
	}
	return addUint64(retained, newAllocation)
}

func maxIntValue() int {
	return int(^uint(0) >> 1)
}

func marshalSpillRecordGrowBytes(bat *batch.Batch) (uint64, bool) {
	base := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > base {
		base = size
	}
	columns := uint64(len(bat.Vecs))
	if columns > (math.MaxUint64-24)/128 {
		return 0, false
	}
	return addUint64(base, columns*128+24)
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
		r.batchCharge = 0
	}
}

func (r *BucketReader) readBatchRecord(
	proc *process.Process,
	reuseBat *batch.Batch,
	token *process.HashBuildReservation,
	charge uint64,
	retainLease bool,
) (*batch.Batch, *process.HashBuildReservation, uint64, error) {
	if err := checkSpillCanceled(proc); err != nil {
		return nil, token, charge, err
	}
	if _, err := io.ReadFull(r.reader, r.buf[:]); err != nil {
		if err == io.EOF {
			return nil, token, charge, io.EOF
		}
		return nil, token, charge, err
	}
	cnt := types.DecodeInt64(r.buf[:8])
	batchSize := types.DecodeInt64(r.buf[8:16])
	if cnt < 0 || batchSize < 0 {
		return nil, token, charge, moerr.NewInternalError(proc.Ctx, "negative spill batch header")
	}
	if err := checkSpillCanceled(proc); err != nil {
		return nil, token, charge, err
	}
	if r.budget != nil {
		payload := uint64(batchSize)
		if payload > uint64(maxIntValue())-(64<<10) {
			return nil, token, charge, process.ErrHashBuildBudgetInvalid
		}
		// The batch payload starts with row count and vector count. Peek only the
		// fixed header so allocator rounding can be bounded per decoded vector
		// before UnmarshalFromReader performs any allocation.
		header, err := r.reader.Peek(12)
		if err != nil {
			return nil, token, charge, err
		}
		rows := types.DecodeInt64(header[:8])
		columns := types.DecodeInt32(header[8:12])
		if rows != cnt {
			return nil, token, charge, moerr.NewInternalError(proc.Ctx, "row count mismatch")
		}
		projected, ok := decodedBatchProjectedBytes(payload, rows, columns)
		if !ok {
			return nil, token, charge, process.ErrHashBuildBudgetInvalid
		}
		// The serialized payload already includes every vector's data, area, null
		// bitmap, and headers. Reserve one decoded payload plus bounded allocator
		// slack, then reconcile to the retained capacities reported by the batch.
		// Multiplying the complete payload rejects large spill records before
		// UnmarshalFromReader can establish their actual retained footprint.
		if token == nil {
			// A caller-provided reuse batch has no budget ownership on the first
			// read. Drop it before admitting the decoded payload.
			reuseBat.Clean(proc.Mp())
			if r.allocation != nil {
				if err := reuseBat.SetAllocationAccount(
					r.allocation.decoded,
				); err != nil {
					return nil, nil, 0, err
				}
			}
			var err error
			token, err = r.budget.Reserve(projected)
			if err != nil {
				return nil, nil, 0, err
			}
			charge = projected
		} else {
			// Reusing vectors can briefly keep their old allocation alive while
			// mpool.Grow allocates the replacement. Admit one complete decoded
			// payload above the retained lease before unmarshal. If that transient
			// peak does not fit, release the old batch and decode from a clean
			// batch so a valid single payload is not rejected.
			retained, retainedOK := batchRetainedBytes(reuseBat)
			peak, peakOK := decodedBatchReusePeakBytes(retained, projected, payload)
			var growErr error
			if retainedOK && peakOK && peak > charge {
				growErr = token.Grow(peak - charge)
			}
			if growErr != nil &&
				!errors.Is(growErr, process.ErrHashBuildBudgetAdmission) {
				return nil, token, charge, growErr
			}
			if !retainedOK || !peakOK || growErr != nil {
				reuseBat.Clean(proc.Mp())
				if r.allocation != nil {
					if err := reuseBat.SetAllocationAccount(
						r.allocation.decoded,
					); err != nil {
						token.Release()
						return nil, nil, 0, err
					}
				}
				token.Release()
				token = nil
				var err error
				token, err = r.budget.Reserve(projected)
				if err != nil {
					return nil, nil, 0, err
				}
				charge = projected
			} else if peak > charge {
				charge = peak
			}
		}
	}

	reuseBat.CleanOnlyData()
	if err := checkSpillCanceled(proc); err != nil {
		return nil, token, charge, err
	}

	limitReader := io.LimitedReader{R: r.reader, N: batchSize}
	if err := reuseBat.UnmarshalFromReader(&limitReader, proc.Mp()); err != nil {
		return nil, token, charge, err
	}
	if err := checkSpillCanceled(proc); err != nil {
		return nil, token, charge, err
	}

	// Verify the batch unmarshal consumed exactly batchSize bytes.
	if limitReader.N > 0 {
		return nil, token, charge, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", limitReader.N)
	}

	// Read magic (8 bytes)
	if _, err := io.ReadFull(r.reader, r.buf[:8]); err != nil {
		return nil, token, charge, err
	}
	if types.DecodeUint64(r.buf[:8]) != SpillMagic {
		return nil, token, charge, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}

	if reuseBat.RowCount() != int(cnt) {
		return nil, token, charge, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}
	if token != nil {
		actual, ok := batchRetainedBytes(reuseBat)
		if !ok || actual > charge {
			return nil, token, charge, process.ErrHashBuildBudgetInvalid
		}
		target := actual
		if retainLease {
			withSlack, ok := addUint64(actual, decodedBatchLeaseSlack)
			if !ok {
				return nil, token, charge, process.ErrHashBuildBudgetInvalid
			}
			if withSlack < charge {
				target = withSlack
			} else {
				target = charge
			}
		}
		if target < charge {
			if err := reconcileReadReservation(token, target); err != nil {
				return nil, token, charge, err
			}
			charge = target
		}
	}
	return reuseBat, token, charge, nil
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
		r.batchCharge = 0
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
	grow, ok := marshalSpillRecordGrowBytes(bat)
	if !ok || grow > uint64(maxIntValue()) {
		return process.ErrHashBuildBudgetInvalid
	}
	if uint64(buf.Cap()) < grow {
		// Allocate the final serialization capacity in one step. Retaining a
		// smaller bytes.Buffer while it grows geometrically would invalidate the
		// single-payload admission estimate.
		*buf = *bytes.NewBuffer(make([]byte, 0, int(grow)))
	}
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

// hashCombine merges a new hash value into a running hash state (Boost-style).
func hashCombine(h, val uint64) uint64 {
	return keycodec.HashCombine(h, val)
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
	sourceAlreadyCharged bool,
	analyzer process.Analyzer,
) error {
	return e.scatterBatchBounded(proc, bat, keyVecs, writers, partitionLevel, sourceAlreadyCharged, analyzer)
}

func scatterTransientBudgetBytes(bat *batch.Batch, sourceAlreadyCharged bool) (uint64, error) {
	if bat == nil || bat.RowCount() < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	allocated := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > allocated {
		allocated = size
	}
	return scatterTransientBudgetFor(allocated, uint64(len(bat.Vecs)), sourceAlreadyCharged)
}

func scatterTransientBudgetFor(allocated, columns uint64, sourceAlreadyCharged bool) (uint64, error) {
	oneMaterializedBatch, ok := batchPayloadWithAllocationSlack(allocated, columns)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	// The selected batch and serialized payload are distinct live objects, so
	// each receives one source-sized estimate plus bounded allocator/framing
	// slack. The row/hash arrays are accounted separately from their capacities.
	need, ok := addUint64(oneMaterializedBatch, oneMaterializedBatch)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if !sourceAlreadyCharged {
		if need, ok = addUint64(need, allocated); !ok {
			return 0, process.ErrHashBuildBudgetInvalid
		}
	}
	return need, nil
}

// reserveRebuildScatterScratch protects the one-batch repartition workspace
// before the rebuild retains another decoded batch. The reservation is only an
// accounting lease: no scatter buffers are allocated until re-spill actually
// starts. Keeping this floor lets a copy admission fail early enough that the
// already-retained batches can still be repartitioned under the same hard cap.
func (e *SpillEngine) reserveRebuildScatterScratch(
	builder *hashbuild.HashmapBuilder,
	bat *batch.Batch,
	analyzer process.Analyzer,
) error {
	if e.cfg.Budget == nil {
		return nil
	}
	if builder == nil || bat == nil || bat.RowCount() < 0 {
		return process.ErrHashBuildBudgetInvalid
	}

	allocated := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > allocated {
		allocated = size
	}
	rows := bat.RowCount()
	columns := len(bat.Vecs)

	// CopyIntoBatches may complete a partial physical tail with this record.
	// Bound that resulting batch before the copy; reserving only for either
	// input independently is not enough when two small records coalesce.
	batches := builder.Batches.Buf
	if bat.RowCount() != colexec.DefaultBatchSize && len(batches) > 0 {
		tail := batches[len(batches)-1]
		if tail == nil {
			return process.ErrHashBuildBudgetInvalid
		}
		if tail.RowCount() != colexec.DefaultBatchSize {
			merged, ok := predictMergedRetainedBytes(tail, bat)
			if !ok {
				return process.ErrHashBuildBudgetInvalid
			}
			if merged > allocated {
				allocated = merged
			}
			if tail.RowCount() > math.MaxInt-bat.RowCount() {
				return process.ErrHashBuildBudgetInvalid
			}
			rows += tail.RowCount()
			if len(tail.Vecs) > columns {
				columns = len(tail.Vecs)
			}
		}
	}

	retained, ok := e.scatterRetainedBytes()
	if !ok {
		return process.ErrHashBuildBudgetInvalid
	}
	growth, ok := e.scatterCapacityGrowthBytes(rows, len(e.cfg.BuildKeyExprs))
	if !ok {
		return process.ErrHashBuildBudgetInvalid
	}
	transient, err := scatterTransientBudgetFor(allocated, uint64(columns), true)
	if err != nil {
		return err
	}
	need, ok := addUint64(retained, growth)
	if !ok {
		return process.ErrHashBuildBudgetInvalid
	}
	if need, ok = addUint64(need, transient); !ok {
		return process.ErrHashBuildBudgetInvalid
	}

	return e.reserveRebuildScratchFloor(need, analyzer)
}

func (e *SpillEngine) reserveRebuildScratchFloor(need uint64, analyzer process.Analyzer) error {
	if need == 0 || e.cfg.Budget == nil {
		return nil
	}
	var err error
	if e.scatterScratchReservation == nil {
		e.scatterScratchReservation, err = e.cfg.Budget.Reserve(need)
		if err != nil {
			if analyzer != nil {
				analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildScratchReserveRejects", 1)
			}
			return err
		}
		if analyzer != nil {
			analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildScratchReserveCount", 1)
		}
	} else if current := e.scatterScratchReservation.Size(); need > current {
		grow := need - current
		if err = e.scatterScratchReservation.Grow(grow); err != nil {
			if analyzer != nil {
				analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildScratchGrowRejects", 1)
			}
			return err
		}
		if analyzer != nil {
			analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildScratchGrowCount", 1)
			analyzer.GetOpStats().AddExtraStat("JoinSpillRebuildScratchGrowBytes", spillStatInt64(grow))
		}
	}
	if need > e.scatterScratchFloor {
		e.scatterScratchFloor = need
	}
	if analyzer != nil {
		analyzer.GetOpStats().SetMaxExtraStat(
			"JoinSpillRebuildScratchFloorBytes",
			spillStatInt64(e.scatterScratchReservation.Size()),
		)
	}
	return nil
}

func spillStatInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func (e *SpillEngine) scatterRetainedBytes() (uint64, bool) {
	actual := uint64(0)
	add := func(v uint64) bool {
		var ok bool
		actual, ok = addUint64(actual, v)
		return ok
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
		return 0, false
	}
	for i := range e.scatterWriteBuffers {
		if !add(uint64(e.scatterWriteBuffers[i].Cap())) {
			return 0, false
		}
	}
	return actual, true
}

func (e *SpillEngine) scatterCapacityGrowthBytes(rows, keys int) (uint64, bool) {
	if rows < 0 || keys < 0 {
		return 0, false
	}
	var growth uint64
	addGrowth := func(required, current uint64) bool {
		if required <= current {
			return true
		}
		var ok bool
		// make allocates the complete replacement before assignment drops the
		// old slice. retained already includes current, so admit all of required.
		growth, ok = addUint64(growth, required)
		return ok
	}
	rowCount := uint64(rows)
	keyCount := uint64(keys)
	if rowCount > math.MaxUint64/8 || keyCount > math.MaxUint64/8 {
		return 0, false
	}
	if !addGrowth(rowCount*8, uint64(cap(e.scatterHashValues))*8) ||
		!addGrowth(rowCount*4, uint64(cap(e.scatterBucketRowIds))*4) ||
		!addGrowth(keyCount*8, uint64(cap(e.keyVecs))*8) {
		return 0, false
	}
	return growth, true
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
		// Start with retained capacities already owned by this token, add only
		// row/hash capacity growth, then add each per-batch transient once.
		retained, ok := e.scatterRetainedBytes()
		if !ok {
			return process.ErrHashBuildBudgetInvalid
		}
		growth, ok := e.scatterCapacityGrowthBytes(rows, len(keyVecs))
		if !ok {
			return process.ErrHashBuildBudgetInvalid
		}
		transient, err := scatterTransientBudgetBytes(bat, sourceAlreadyCharged)
		if err != nil {
			return err
		}
		need, ok := addUint64(retained, growth)
		if !ok {
			return process.ErrHashBuildBudgetInvalid
		}
		if need, ok = addUint64(need, transient); !ok {
			return process.ErrHashBuildBudgetInvalid
		}
		if e.scatterScratchReservation == nil {
			e.scatterScratchReservation, err = e.cfg.Budget.Reserve(need)
		} else if current := e.scatterScratchReservation.Size(); need > current {
			// Grow the retained scratch token to the complete batch peak. Its
			// current hash/row-id/coalesce capacities are components of need,
			// not an additional allocation to charge a second time.
			err = e.scatterScratchReservation.Grow(need - current)
		}
		if err != nil {
			return err
		}
	}

	if e.allocation != nil {
		if e.allocationMP != nil && e.allocationMP != proc.Mp() {
			return mpool.ErrAllocationAccountInvalid
		}
		e.allocationMP = proc.Mp()
	}
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
		sels := e.scatterBucketRowIds[start:end]
		if selected == nil {
			var selection *vector.AllocationAccountSelection
			if e.allocation != nil {
				selection = e.allocation.selected
			}
			selected, err = newSpillBatch(len(bat.Vecs), selection)
			if err != nil {
				return err
			}
			for j, vec := range bat.Vecs {
				selected.Vecs[j], err = newSpillVector(
					*vec.GetType(),
					selection,
				)
				if err != nil {
					return err
				}
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
		if buf.Cap() < spillWriteCoalesceSize {
			*buf = *bytes.NewBuffer(make([]byte, 0, spillWriteCoalesceSize))
		}
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
	additional := uint64(spillWriteCoalesceSize - buf.Cap())
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
	freeSpillSlice(
		e.scatterHashValues,
		e.allocationMP,
		e.allocation,
	)
	freeSpillSlice(
		e.scatterBucketRowIds,
		e.allocationMP,
		e.allocation,
	)
	e.scatterHashValues = nil
	e.scatterBucketRowIds = nil
	e.allocationMP = nil
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
	e.scatterScratchFloor = 0
}

// reconcileScatterScratch leaves only the capacities retained by the engine
// charged after a batch completes. The source batch, selected vectors, and
// marshal buffer are transient and must not pin budget across the queue.
func (e *SpillEngine) reconcileScatterScratch() error {
	if e.scatterScratchReservation == nil {
		return nil
	}
	actual, ok := e.scatterRetainedBytes()
	if !ok {
		return process.ErrHashBuildBudgetInvalid
	}
	if actual < e.scatterScratchFloor {
		actual = e.scatterScratchFloor
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
	Budget *process.HashBuildBudgetGeneration
	// ProbeExpressionLease is owned by the consuming join operator and borrowed
	// by SpillEngine while it scatters or re-scatters probe batches. The join
	// must free its probe executors before releasing this lease.
	ProbeExpressionLease *hashbuild.ExpressionMemoryLease
	MaxQueue             int
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
	buildReader    BucketReader
	probeReader    BucketReader
	buildReadBatch *batch.Batch
	probeReadBatch *batch.Batch

	// Reusable scatter state
	buildPool ReusableBufferPool
	probePool ReusableBufferPool

	// Cached key executors for re-spill
	keyExecs       []colexec.ExpressionExecutor
	keyVecs        []*vector.Vector
	buildExprLease *hashbuild.ExpressionMemoryLease

	// Reusable scatter buffers to avoid per-batch allocations.
	scatterHashValues    []uint64
	scatterBucketRowIds  []int32
	scatterBucketCounts  [SpillNumBuckets]int32
	scatterBucketOffsets [SpillNumBuckets + 1]int32
	scatterWriteBuf      bytes.Buffer
	scatterWriteBuffers  [SpillNumBuckets]bytes.Buffer
	scatterWriteRows     [SpillNumBuckets]int64
	// The lease follows reusable scratch capacities within one rebuild/scatter
	// phase. releaseScatterScratch drops both the backing arrays and this token;
	// Cleanup is the idempotent terminal fallback.
	scatterScratchReservation *process.HashBuildReservation
	// scatterScratchFloor is pre-admitted only while rebuilding an already
	// spilled bucket. It keeps one bounded repartition workspace available if the
	// next retained-copy admission or threshold decision requires re-spill. It
	// is a conservative bound, not a measurement of later physical allocations.
	scatterScratchFloor uint64

	// probeKeyEval is the unbudgeted fallback for probe re-scatter. Production
	// spilled joins evaluate the probe executors owned by ProbeExpressionLease.
	probeKeyEval func(*batch.Batch) ([]*vector.Vector, error)
}

// NewSpillEngine creates an engine from configuration. Call InitFromSpilledMap next.
func NewSpillEngine(cfg SpillEngineConfig) *SpillEngine {
	return newSpillEngine(cfg, nil)
}

// NewSpillEngineWithAllocation constructs the dormant allocation-accounted
// spill path. Legacy production callers continue to use NewSpillEngine.
func NewSpillEngineWithAllocation(
	cfg SpillEngineConfig,
	allocation *SpillAllocationAccount,
) (*SpillEngine, error) {
	if err := allocation.validate(); err != nil {
		return nil, err
	}
	return newSpillEngine(cfg, allocation), nil
}

func newSpillEngine(
	cfg SpillEngineConfig,
	allocation *SpillAllocationAccount,
) *SpillEngine {
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = SpillNumBuckets * SpillNumBuckets
	}
	engine := &SpillEngine{
		cfg:        cfg,
		allocation: allocation,
	}
	engine.buildReader.allocation = allocation
	engine.probeReader.allocation = allocation
	return engine
}

func (e *SpillEngine) makeBucketWriters(prefix string) []BucketWriter {
	writers := MakeBucketWriters(prefix)
	for i := range writers {
		writers[i].spillFS = &e.spillFS
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
	payload, err := jm.TakeSpillBuildPayload()
	if err != nil {
		return message.SpillBuildPayload{}, nil, moerr.NewInternalError(proc.Ctx, err.Error())
	}

	var budget *process.HashBuildBudgetGeneration
	if len(payload.Files) > 0 {
		var ok bool
		budget, ok = payload.BudgetRef.(*process.HashBuildBudgetGeneration)
		if !ok || budget == nil {
			_ = payload.Close()
			return message.SpillBuildPayload{}, nil, moerr.NewInternalError(
				proc.Ctx,
				"spilled join map is missing its producer budget generation",
			)
		}
	} else {
		budget, err = proc.GetHashBuildBudget()
		if err != nil {
			_ = payload.Close()
			return message.SpillBuildPayload{}, nil, err
		}
	}
	return payload, budget, nil
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
	// rows would be silently discarded for legacy or reduced-fanout payloads.
	writers := e.makeBucketWriters("probe")[:bucketCount]
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
		keyVecs, err := e.evalProbeKeys(proc, bat, evalKeysFn)
		if err != nil {
			return err
		}
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		if err := e.scatterBatch(proc, bat, keyVecs, writers, nil, 0, false, analyzer); err != nil {
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
		var selection *vector.AllocationAccountSelection
		if e.allocation != nil {
			selection = e.allocation.decoded
		}
		var err error
		e.probeReadBatch, err = newSpillBatch(0, selection)
		if err != nil {
			return nil, err
		}
	}
	e.probeReader.mergeRecords = e.cfg.MergeProbeBatches || e.cfg.IsDedup
	bat, err := e.probeReader.ReadBatch(proc, e.probeReadBatch)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Cancellation can race the reader's final record-boundary check. Do not
	// hand a freshly decoded batch to the join probe loop after that point.
	if err := checkSpillCanceled(proc); err != nil {
		e.probeReader.releaseReadBatch(proc, e.probeReadBatch, nil)
		return nil, err
	}
	return bat, nil
}

// builderMemSize computes total memory used by a HashmapBuilder during the rebuild
// loop. MemSize covers completed fixed-size batches; include the one permitted
// partial tail as well. The full scan is only a fallback for directly assembled
// state where MemSize has not been maintained.
func builderMemSize(builder *hashbuild.HashmapBuilder) int64 {
	sz := builder.GetSize() + builder.Batches.MemSize
	batches := builder.Batches.Buf
	if builder.Batches.MemSize == 0 {
		for _, b := range builder.Batches.Buf {
			sz += int64(b.Size())
		}
	} else if len(batches) > 0 {
		tail := batches[len(batches)-1]
		if tail != nil && tail.RowCount() != colexec.DefaultBatchSize {
			sz += int64(tail.Size())
		}
	}
	return sz
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
		var selection *vector.AllocationAccountSelection
		if e.allocation != nil {
			selection = e.allocation.decoded
		}
		readBatch, err := newSpillBatch(0, selection)
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
			if err := e.reserveRebuildScatterScratch(builder, bat, analyzer); err != nil {
				// Scratch is contingency headroom, not a prerequisite for a
				// bucket that may still rebuild within the cap. Admission misses
				// are observable but best-effort; lifecycle/accounting failures
				// remain terminal and are returned unchanged.
				if !isBudgetAdmission(err) {
					builder.FreeHashMapAndBatches(proc)
					builder.Free(proc)
					return nil, BucketSkip, err
				}
			}
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
				return nil, BucketSkip, noProgressError(bucket.Depth, err)
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
			return nil, BucketSkip, noProgressError(bucket.Depth, err)
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

	jm := builder.GetJoinMap(proc.Mp())
	if jm == nil {
		// GetJoinMap transfers nothing when the decoded build contains no
		// rows. Release executors and every residual builder allocation before
		// handing an empty-build probe file to the caller.
		builder.Free(proc)
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
	if err := checkSpillCanceled(proc); err != nil {
		return nil, err
	}
	// Re-spill only drains the builder's copied batches. Drop the failed
	// hashmap-build executor set before admitting the engine's re-partition
	// executors, so the two equivalent retained working sets never overlap.
	builder.FreeExecutors()
	buildWriters := e.makeBucketWriters("build_sub")
	for i := range buildWriters {
		buildWriters[i].Budget = e.cfg.Budget
	}
	probeWriters := e.makeBucketWriters("probe_sub")
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
		var execs []colexec.ExpressionExecutor
		var lease *hashbuild.ExpressionMemoryLease
		var err error
		if e.allocation == nil {
			execs, lease, err =
				hashbuild.NewBudgetedExpressionExecutors(
					proc,
					e.cfg.Budget,
					e.cfg.BuildKeyExprs,
					false,
				)
		} else {
			execs, err =
				colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(
					proc,
					e.cfg.BuildKeyExprs,
					e.allocation.expression,
				)
			if err == nil {
				lease, err = hashbuild.NewExpressionMemoryLease(
					nil,
					e.cfg.BuildKeyExprs,
					execs,
					false,
				)
			}
			if err != nil {
				for _, exec := range execs {
					exec.Free()
				}
			}
		}
		if err != nil {
			return nil, err
		}
		e.freeKeyExecs()
		e.keyExecs = execs
		e.buildExprLease = lease
	}

	// evalAndScatter builds key vectors using the given executors and scatters.
	evalAndScatter := func(
		bat *batch.Batch,
		writers []BucketWriter,
		buffers []*batch.Batch,
		execs []colexec.ExpressionExecutor,
		sourceAlreadyCharged bool,
	) error {
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		if cap(e.keyVecs) < len(execs) {
			e.keyVecs = make([]*vector.Vector, len(execs))
		}
		keyVecs := e.keyVecs[:len(execs)]
		defer func() {
			for i := range keyVecs {
				keyVecs[i] = nil
			}
		}()
		err := e.buildExprLease.Run(proc, bat.RowCount(), func(i int) error {
			vec, evalErr := execs[i].Eval(proc, []*batch.Batch{bat}, nil)
			if evalErr != nil {
				return evalErr
			}
			keyVecs[i] = vec
			return nil
		})
		if err != nil {
			// Eval may leave newly allocated child/result vectors cached.
			// Destroy the owned executor tree before releasing its lease.
			e.freeKeyExecs()
			return err
		}
		if err := checkSpillCanceled(proc); err != nil {
			return err
		}
		return e.scatterBatch(proc, bat, keyVecs, writers, nil, partitionLevel, sourceAlreadyCharged, analyzer)
	}

	var buildRows int64
	if err := builder.DrainCopiedBatches(proc, func(b *batch.Batch) error {
		if b != nil {
			buildRows += int64(b.RowCount())
		}
		if err := evalAndScatter(b, buildWriters, nil, e.keyExecs, true); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if pending != nil && pending.RowCount() > 0 {
		// pending is the current BucketReader batch whose copy admission failed;
		// the reader keeps its batch token live until the next ReadBatch.
		if err := evalAndScatter(pending, buildWriters, nil, e.keyExecs, true); err != nil {
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
		if err := evalAndScatter(bat, buildWriters, nil, e.keyExecs, true); err != nil {
			return nil, err
		}
	}
	if err := e.flushScatterBuffers(proc, buildWriters, analyzer); err != nil {
		return nil, err
	}

	if e.probeReadBatch == nil {
		var selection *vector.AllocationAccountSelection
		if e.allocation != nil {
			selection = e.allocation.decoded
		}
		readBatch, err := newSpillBatch(0, selection)
		if err != nil {
			return nil, err
		}
		e.probeReadBatch = readBatch
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
					Message: fmt.Sprintf("join spill queue limit exceeded (limit=%d); reduce join-key skew or increase processLimitationSize", e.cfg.MaxQueue),
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

// scatterProbe evaluates probe-side keys (EqConds[0]) for probe re-scatter.
// It uses the borrowed probe lease, not build-side keyExecs; probeKeyEval is
// retained only as the unbudgeted fallback.
func scatterProbe(proc *process.Process, e *SpillEngine, bat *batch.Batch, writers []BucketWriter, buffers []*batch.Batch, seed uint64, analyzer process.Analyzer) error {
	keyVecs, err := e.evalProbeKeys(proc, bat, e.probeKeyEval)
	if err != nil {
		return err
	}
	return e.scatterBatch(proc, bat, keyVecs, writers, buffers, seed, true, analyzer)
}

func (e *SpillEngine) evalProbeKeys(
	proc *process.Process,
	bat *batch.Batch,
	fallback func(*batch.Batch) ([]*vector.Vector, error),
) ([]*vector.Vector, error) {
	if e.cfg.ProbeExpressionLease == nil {
		if fallback == nil {
			return nil, process.ErrHashBuildBudgetInvalid
		}
		return fallback(bat)
	}
	if e.cfg.ProbeExpressionLease.Len() != len(e.cfg.ProbeKeyExprs) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if cap(e.keyVecs) < len(e.cfg.ProbeKeyExprs) {
		e.keyVecs = make([]*vector.Vector, len(e.cfg.ProbeKeyExprs))
	}
	keyVecs := e.keyVecs[:len(e.cfg.ProbeKeyExprs)]
	err := e.cfg.ProbeExpressionLease.Eval(
		proc,
		[]*batch.Batch{bat},
		bat.RowCount(),
		func(index int, vec *vector.Vector) error {
			keyVecs[index] = vec
			return nil
		},
	)
	if err != nil {
		for i := range keyVecs {
			keyVecs[i] = nil
		}
		return nil, err
	}
	return keyVecs, nil
}

func (e *SpillEngine) freeKeyExecs() {
	for _, exec := range e.keyExecs {
		if exec != nil {
			exec.Free()
		}
	}
	e.keyExecs = nil
	if e.buildExprLease != nil {
		e.buildExprLease.Release()
		e.buildExprLease = nil
	}
}

func isBudgetAdmission(err error) bool {
	return err != nil &&
		errors.Is(err, process.ErrHashBuildBudgetAdmission)
}

func noProgressError(depth int, cause error) error {
	budgetErr := &process.HashBuildBudgetError{
		Kind:    process.HashBuildBudgetErrorAdmission,
		Message: fmt.Sprintf("join spill cannot make progress at depth %d; reduce join-key skew or increase processLimitationSize", depth),
	}
	if cause != nil {
		var budgetCause *process.HashBuildBudgetError
		if errors.As(cause, &budgetCause) && budgetCause.Kind == process.HashBuildBudgetErrorAdmission {
			budgetErr.Resource = budgetCause.Resource
			budgetErr.Requested = budgetCause.Requested
			budgetErr.Used = budgetCause.Used
			budgetErr.Cap = budgetCause.Cap
			budgetErr.Message = fmt.Sprintf("join spill cannot make progress at depth %d", depth)
		}
	}
	return budgetErr
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
