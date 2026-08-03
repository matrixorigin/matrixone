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

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMagic      = 0x12345678DEADBEEF
)

func spillCheckedAdd(total, value uint64) (uint64, error) {
	if total > math.MaxUint64-value {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return total + value, nil
}

func spillCheckedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return left * right, nil
}

func spillCapacityReplacementOverlap(rows, keys, hashCap, rowIDCap, keyCap int) (uint64, error) {
	var overlap uint64
	add := func(required, current int, width uint64) error {
		if required < 0 || current < 0 {
			return process.ErrHashBuildBudgetInvalid
		}
		if required <= current {
			return nil
		}
		old, err := spillCheckedMul(uint64(current), width)
		if err != nil {
			return err
		}
		overlap, err = spillCheckedAdd(overlap, old)
		return err
	}
	if err := add(keys, keyCap, 8); err != nil {
		return 0, err
	}
	if err := add(rows, hashCap, 8); err != nil {
		return 0, err
	}
	if err := add(rows, rowIDCap, 4); err != nil {
		return 0, err
	}
	return overlap, nil
}

// spillMaterializedBytes models the batch that spillBatchBounded creates with
// UnionInt32. It follows vector materialization semantics instead of retained
// capacity or stale logical length: fixed-width descriptors are per output
// row, null payload is skipped, and const varlen payload is copied once.
func spillMaterializedBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	liveRows := uint64(bat.RowCount())
	var materialized uint64
	for _, vec := range bat.Vecs {
		if vec == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		typeSize := vec.GetType().TypeSize()
		if typeSize < 0 {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		descriptors, err := spillCheckedMul(liveRows, uint64(typeSize))
		if err != nil {
			return 0, err
		}
		if materialized, err = spillCheckedAdd(materialized, descriptors); err != nil {
			return 0, err
		}
		if !vec.GetType().IsVarlen() || vec.IsConstNull() {
			continue
		}

		values, _ := vector.MustVarlenaRawData(vec)
		valueRows := liveRows
		if vec.IsConst() {
			valueRows = 1
		}
		if valueRows == 0 || valueRows > uint64(len(values)) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		var livePayload uint64
		hasNull := !vec.GetNulls().EmptyByFlag()
		for row := uint64(0); row < valueRows; row++ {
			if hasNull && vec.GetNulls().Contains(row) {
				continue
			}
			if values[row].IsSmall() {
				continue
			}
			_, length := values[row].OffsetLen()
			if livePayload, err = spillCheckedAdd(livePayload, uint64(length)); err != nil {
				return 0, err
			}
		}

		if materialized, err = spillCheckedAdd(materialized, livePayload); err != nil {
			return 0, err
		}
	}
	return materialized, nil
}

func spillMarshalSlack(columns uint64) (uint64, error) {
	const (
		fixedSlack     = uint64(64 << 10)
		perColumnSlack = uint64(128)
	)
	if columns > (math.MaxUint64-fixedSlack)/perColumnSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return fixedSlack + columns*perColumnSlack, nil
}

func spillMaterializationSlack(columns uint64) (uint64, error) {
	const (
		fixedSlack     = uint64(64 << 10)
		perColumnSlack = uint64(16 << 10)
	)
	if columns > (math.MaxUint64-fixedSlack)/perColumnSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return fixedSlack + columns*perColumnSlack, nil
}

// spillPeakBudgetFor accounts each simultaneously live component explicitly.
// inputBytes is zero for a retained batch whose source reservation is already
// owned by HashBuild.
func spillPeakBudgetFor(rows, inputBytes, selectedBytes, columns uint64) (uint64, error) {
	rowScratch, err := spillCheckedMul(rows, 12) // hashes + one row-id array
	if err != nil {
		return 0, err
	}
	total, err := spillCheckedAdd(rowScratch, inputBytes)
	if err != nil {
		return 0, err
	}
	if total, err = spillCheckedAdd(total, selectedBytes); err != nil {
		return 0, err
	}
	// MarshalBinary creates one serialized payload. The selected estimate
	// already includes its fixed-width data and varlen area, so charge that
	// payload once plus bounded framing/allocation slack.
	marshalSlack, err := spillMarshalSlack(columns)
	if err != nil {
		return 0, err
	}
	marshalBytes, err := spillCheckedAdd(selectedBytes, marshalSlack)
	if err != nil {
		return 0, err
	}
	if total, err = spillCheckedAdd(total, marshalBytes); err != nil {
		return 0, err
	}
	if total > uint64(^uint(0)>>1) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return total, nil
}

// spillBudgetBytes admits only the actual direct-spill path for the current
// input. It never projects a hypothetical retained batch.
func spillBudgetBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	rows := uint64(bat.RowCount())
	selected, err := spillMaterializedBytes(bat)
	if err != nil {
		return 0, err
	}
	materializationSlack, err := spillMaterializationSlack(uint64(len(bat.Vecs)))
	if err != nil {
		return 0, err
	}
	if selected, err = spillCheckedAdd(selected, materializationSlack); err != nil {
		return 0, err
	}
	return spillPeakBudgetFor(rows, uint64(bat.Allocated()), selected, uint64(len(bat.Vecs)))
}

// spillScratchBudgetBytes returns the incremental spill charge. A copied
// build batch remains covered by HashmapBuilder.batchReservations while it is
// drained, so charging its source footprint again would double count it. An
// upstream batch has no HashBuild-owned reservation and keeps the full charge.
func spillScratchBudgetBytes(bat *batch.Batch, sourceAlreadyCharged bool) (uint64, error) {
	need, err := spillBudgetBytes(bat)
	if err != nil || !sourceAlreadyCharged || bat == nil || bat.RowCount() <= 0 {
		return need, err
	}
	// copyBuildBatch reconciles its retained reservation against Allocated
	// (plus metadata), so only that proven charge may be subtracted here.
	source := uint64(bat.Allocated())
	if source > need {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return need - source, nil
}

const spillRecoveryReservationQuantum = uint64(64 << 10)

// spillRecoveryReservationBytes rounds a recovery high-water mark to a small,
// fixed allocation quantum. The lease is per HashBuild execution, not per
// batch: rounding avoids rescanning near-identical varlen batches merely
// because their payload differs by a few bytes, while keeping the bounded
// over-reservation independent of row count, fanout, and query shape.
func spillRecoveryReservationBytes(need uint64) (uint64, error) {
	if need == 0 {
		return 0, nil
	}
	if need > math.MaxUint64-(spillRecoveryReservationQuantum-1) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return (need + spillRecoveryReservationQuantum - 1) &^ (spillRecoveryReservationQuantum - 1), nil
}

// spillDirectRecoveryBudgetUpper is a row-scan-free upper bound used to decide
// whether the current recovery lease can already spill an upstream batch.
// Fixed-width and const batches are exact apart from bounded slack. A regular
// varlen vector may have every descriptor reference the same physical area, so
// rows*area is the smallest representation-independent bound available without
// inspecting descriptors. Such batches are scanned exactly only when this
// conservative bound crosses the retained high-water mark; this path is cold
// for normal resident builds.
func spillDirectRecoveryBudgetUpper(bat *batch.Batch) (uint64, bool, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, false, nil
	}
	rows := uint64(bat.RowCount())
	var selected uint64
	hasVarlen := false
	for _, vec := range bat.Vecs {
		if vec == nil {
			return 0, false, process.ErrHashBuildBudgetInvalid
		}
		typeSize := vec.GetType().TypeSize()
		if typeSize < 0 {
			return 0, false, process.ErrHashBuildBudgetInvalid
		}
		descriptors, err := spillCheckedMul(rows, uint64(typeSize))
		if err != nil {
			return 0, false, err
		}
		selected, err = spillCheckedAdd(selected, descriptors)
		if err != nil {
			return 0, false, err
		}
		if vec.GetType().IsVarlen() && !vec.IsConstNull() {
			hasVarlen = true
			payloadUpper := uint64(len(vec.GetArea()))
			if !vec.IsConst() {
				payloadUpper, err = spillCheckedMul(payloadUpper, rows)
				if err != nil {
					return 0, false, err
				}
			}
			selected, err = spillCheckedAdd(selected, payloadUpper)
			if err != nil {
				return 0, false, err
			}
		}
	}
	materializationSlack, err := spillMaterializationSlack(uint64(len(bat.Vecs)))
	if err != nil {
		return 0, false, err
	}
	selected, err = spillCheckedAdd(selected, materializationSlack)
	if err != nil {
		return 0, false, err
	}
	allocated := bat.Allocated()
	if allocated < 0 {
		return 0, false, process.ErrHashBuildBudgetInvalid
	}
	need, err := spillPeakBudgetFor(rows, uint64(allocated), selected, uint64(len(bat.Vecs)))
	return need, hasVarlen, err
}

// spillRetainedRecoveryBudgetBytes turns the allocation projection already
// required by CopyIntoBatches into a future-drain proof. The projection tracks
// logical spill materialization rather than physical retained allocation:
// ordinary non-const descriptors can also share one retained payload while a
// later spill selection copies it once per row. Source memory itself is covered
// separately by the retained-batch reservation.
func spillRetainedRecoveryBudgetBytes(projection batchCopyProjection) (uint64, error) {
	if projection.maxRetainedRows <= 0 || projection.columns < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	materializationSlack, err := spillMaterializationSlack(uint64(projection.columns))
	if err != nil {
		return 0, err
	}
	selected, err := spillCheckedAdd(
		projection.maxRetainedSelected,
		materializationSlack,
	)
	if err != nil {
		return 0, err
	}
	return spillPeakBudgetFor(
		uint64(projection.maxRetainedRows),
		0,
		selected,
		uint64(projection.columns),
	)
}

func (ctr *container) ensureSpillRecoveryReservationBytes(
	need uint64,
	analyzer process.Analyzer,
) error {
	if ctr.hashmapBuilder.budget == nil || need == 0 || need <= ctr.spillScratchBase {
		return nil
	}
	var err error
	if ctr.spillScratchReservation == nil {
		ctr.spillScratchReservation, err = ctr.hashmapBuilder.budget.Reserve(need)
		if err != nil {
			analyzer.GetOpStats().AddExtraStat("HashBuildSpillRecoveryReserveRejects", 1)
			return err
		}
		ctr.spillScratchBase = need
		analyzer.GetOpStats().SetMaxExtraStat(
			"HashBuildSpillRecoveryReservedBytes", hashBuildStatInt64(need))
		analyzer.GetOpStats().SetMaxExtraStat(
			"HashBuildSpillScratchPeakBytes", hashBuildStatInt64(need))
		return nil
	}

	grow := need - ctr.spillScratchBase
	if err = ctr.spillScratchReservation.Grow(grow); err != nil {
		analyzer.GetOpStats().AddExtraStat("HashBuildSpillRecoveryGrowRejects", 1)
		return err
	}
	ctr.spillScratchBase = need
	analyzer.GetOpStats().AddExtraStat("HashBuildSpillRecoveryGrowCount", 1)
	analyzer.GetOpStats().AddExtraStat(
		"HashBuildSpillRecoveryGrowBytes", hashBuildStatInt64(grow))
	analyzer.GetOpStats().SetMaxExtraStat(
		"HashBuildSpillRecoveryReservedBytes", hashBuildStatInt64(need))
	analyzer.GetOpStats().SetMaxExtraStat(
		"HashBuildSpillScratchPeakBytes", hashBuildStatInt64(need))
	return nil
}

func (ctr *container) ensureDirectSpillRecovery(
	bat *batch.Batch,
	analyzer process.Analyzer,
) error {
	upper, hasVarlen, err := spillDirectRecoveryBudgetUpper(bat)
	if err != nil {
		return err
	}
	if upper <= ctr.spillScratchBase {
		return nil
	}
	need := upper
	if hasVarlen {
		// The cheap upper includes dead/null varlena area. Pay the exact live-row
		// scan only when a larger lease may be required, avoiding both false
		// admission failure and a scan on steady-state batches.
		need, err = spillBudgetBytes(bat)
		if err != nil {
			return err
		}
		if need <= ctr.spillScratchBase {
			return nil
		}
	}
	need, err = spillRecoveryReservationBytes(need)
	if err != nil {
		return err
	}
	return ctr.ensureSpillRecoveryReservationBytes(need, analyzer)
}

func (ctr *container) ensureRetainedSpillRecovery(
	projection batchCopyProjection,
	analyzer process.Analyzer,
) error {
	need, err := spillRetainedRecoveryBudgetBytes(projection)
	if err != nil {
		return err
	}
	need, err = spillRecoveryReservationBytes(need)
	if err != nil {
		return err
	}
	return ctr.ensureSpillRecoveryReservationBytes(need, analyzer)
}

func (ctr *container) growSpillScratchTransient(
	required uint64,
	analyzer process.Analyzer,
) (uint64, bool, error) {
	if ctr.hashmapBuilder.budget == nil || ctr.spillScratchReservation == nil ||
		required <= ctr.spillScratchBase {
		return 0, false, nil
	}
	oldSize := ctr.spillScratchReservation.Size()
	if err := ctr.spillScratchReservation.Grow(required - ctr.spillScratchBase); err != nil {
		return 0, false, err
	}
	analyzer.GetOpStats().SetMaxExtraStat(
		"HashBuildSpillScratchPeakBytes",
		hashBuildStatInt64(ctr.spillScratchReservation.Size()),
	)
	return oldSize, true, nil
}

func (ctr *container) restoreSpillScratchTransient(oldSize uint64, grew bool) error {
	if !grew {
		return nil
	}
	_, err := ctr.spillScratchReservation.ReconcileDown(oldSize)
	return err
}

func (ctr *container) releaseSpillScratchReservation() {
	if ctr.spillScratchReservation != nil {
		ctr.spillScratchReservation.Release()
		ctr.spillScratchReservation = nil
	}
	ctr.spillScratchBase = 0
}

func (ctr *container) dropSpillScratchBuffers() {
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

func spillMarshalGrowBytes(bat *batch.Batch) (uint64, error) {
	base := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > base {
		base = size
	}
	columns := uint64(len(bat.Vecs))
	if columns > (math.MaxUint64-24)/128 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return spillCheckedAdd(base, columns*128+24)
}

func marshalSpillRecord(bat *batch.Batch, buf *bytes.Buffer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	buf.Reset()
	grow, err := spillMarshalGrowBytes(bat)
	if err != nil {
		return 0, err
	}
	if grow > uint64(math.MaxInt) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if uint64(buf.Cap()) < grow {
		// Drop a smaller retained buffer before allocating the final capacity;
		// otherwise bytes.Buffer's geometric growth recreates the multiplier
		// that admission intentionally removed.
		*buf = *bytes.NewBuffer(make([]byte, 0, int(grow)))
	}
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

func (ctr *container) writeSpillPayload(
	proc *process.Process,
	file *os.File,
	payload []byte,
	rows int64,
	analyzer process.Analyzer,
) error {
	if file == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	// All initial-spill writes funnel through this helper. Check after any
	// vector projection/marshal and immediately before the physical write so a
	// cancellation that raced lazy file creation or serialization does not
	// start stale I/O. An already-running os.File.Write is not interruptible.
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
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
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
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
// marshaled and written before advancing.
func (ctr *container) spillBatchBounded(
	proc *process.Process,
	bat *batch.Batch,
	files []*os.File,
	analyzer process.Analyzer,
	sourceAlreadyCharged bool,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	expressionLease := ctr.hashmapBuilder.expressionLease
	if expressionLease == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	rows := bat.RowCount()
	keyCount := expressionLease.Len()
	if keyCount == 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	need, err := spillScratchBudgetBytes(bat, sourceAlreadyCharged)
	if err != nil {
		return err
	}
	// Scratch belongs to the execution generation, not to one batch. Establish
	// and grow the lease lazily before allocating spill buffers. Keep it live
	// while capacities are retained and release it from Reset/Free/build cleanup
	// exactly once.
	if ctr.hashmapBuilder.budget != nil {
		if ctr.spillScratchReservation == nil {
			ctr.spillScratchReservation, err = ctr.hashmapBuilder.budget.Reserve(need)
			if err != nil {
				analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchReserveRejects", 1)
				return err
			}
			ctr.spillScratchBase = need
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildSpillScratchPeakBytes",
				hashBuildStatInt64(ctr.spillScratchReservation.Size()),
			)
		} else if need > ctr.spillScratchBase {
			grow := need - ctr.spillScratchBase
			if err := ctr.spillScratchReservation.Grow(grow); err != nil {
				analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowRejects", 1)
				return err
			}
			analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowCount", 1)
			analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowBytes", hashBuildStatInt64(grow))
			ctr.spillScratchBase = need
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildSpillScratchPeakBytes",
				hashBuildStatInt64(ctr.spillScratchReservation.Size()),
			)
		}
	}

	replacementOverlap, err := spillCapacityReplacementOverlap(
		rows,
		keyCount,
		cap(ctr.spillHashValues),
		cap(ctr.spillBucketRowIds),
		cap(ctr.spillKeyVecs),
	)
	if err != nil {
		return err
	}
	replacementPeak, err := spillCheckedAdd(need, replacementOverlap)
	if err != nil {
		return err
	}
	oldScratchSize, grewScratch, err := ctr.growSpillScratchTransient(
		replacementPeak, analyzer)
	if err != nil {
		return err
	}

	if cap(ctr.spillKeyVecs) < keyCount {
		ctr.spillKeyVecs = make([]*vector.Vector, keyCount)
	}
	if cap(ctr.spillHashValues) < rows {
		ctr.spillHashValues = make([]uint64, rows)
	}
	if cap(ctr.spillBucketRowIds) < rows {
		ctr.spillBucketRowIds = make([]int32, rows)
	}
	if err := ctr.restoreSpillScratchTransient(oldScratchSize, grewScratch); err != nil {
		return err
	}
	keyVecs := ctr.spillKeyVecs[:keyCount]
	var selected *batch.Batch
	defer func() {
		if selected != nil {
			selected.Clean(proc.Mp())
		}
		for i := range ctr.spillKeyVecs {
			ctr.spillKeyVecs[i] = nil
		}
	}()
	err = expressionLease.Eval(
		proc,
		[]*batch.Batch{bat},
		bat.RowCount(),
		func(i int, vec *vector.Vector) error {
			keyVecs[i] = vec
			return nil
		},
	)
	if err != nil {
		// Eval may leave child/result vectors cached. The caller that owns the
		// executor set keeps its lease charged until it destroys the complete
		// tree; this function must not guess or duplicate that ownership.
		return err
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	ctr.hashmapBuilder.observeNullKeys(keyVecs)

	// Reuse hashValues buffer
	hashes := ctr.spillHashValues[:rows]
	computeXXHash(keyVecs, hashes)
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
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
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
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
				spillErr = ctr.appendSpillRecord(
					proc, file, int(bucket), selected, need, analyzer)
			}
		}
		selected.CleanOnlyData()
		if spillErr != nil {
			return spillErr
		}
	}
	return nil
}

// appendSpillRecord writes one framed record immediately. HashBuild workers do
// not retain optional file-bound data that could consume a sibling worker's
// mandatory recovery headroom.
func (ctr *container) appendSpillRecord(
	proc *process.Process,
	file *os.File,
	bucket int,
	bat *batch.Batch,
	scratchNeed uint64,
	analyzer process.Analyzer,
) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	grow, err := spillMarshalGrowBytes(bat)
	if err != nil {
		return err
	}
	var oldScratchSize uint64
	var grewScratch bool
	if old := uint64(ctr.spillWriteBuf.Cap()); ctr.hashmapBuilder.budget != nil && old > 0 && old < grow {
		peak, addErr := spillCheckedAdd(scratchNeed, old)
		if addErr != nil {
			return addErr
		}
		oldScratchSize, grewScratch, err = ctr.growSpillScratchTransient(
			peak, analyzer)
		if err != nil {
			return err
		}
	}
	cnt, err := marshalSpillRecord(bat, &ctr.spillWriteBuf)
	if restoreErr := ctr.restoreSpillScratchTransient(oldScratchSize, grewScratch); restoreErr != nil {
		return restoreErr
	}
	if err != nil {
		return err
	}
	payload := ctr.spillWriteBuf.Bytes()
	return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
}

func (ctr *container) memUsed() int64 {
	sz := ctr.hashmapBuilder.GetSize() + ctr.hashmapBuilder.Batches.MemSize
	batches := ctr.hashmapBuilder.Batches.Buf
	// MemSize tracks completed fixed-size batches. Include the one permitted
	// partial tail so a threshold decision cannot lag by almost one batch. If
	// MemSize is zero (including directly assembled test state), sum all batches.
	if ctr.hashmapBuilder.Batches.MemSize == 0 {
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			sz += int64(bat.Size())
		}
	} else if len(batches) > 0 {
		tail := batches[len(batches)-1]
		if tail != nil && tail.RowCount() != colexec.DefaultBatchSize {
			sz += int64(tail.Size())
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

// shouldSpillBeforeRetain applies the configured threshold to the retained
// state plus the current upstream batch. InputBatchRowCount already includes
// that batch when this is called. Moving the existing decision before the copy
// prevents the threshold-crossing reservation from consuming the scratch
// headroom needed to start spill; it does not size or reserve spill scratch.
func (hashBuild *HashBuild) shouldSpillBeforeRetain(inputBatchSize int64) bool {
	if !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	predicted := ctr.memUsed()
	if inputBatchSize < 0 || predicted > math.MaxInt64-inputBatchSize {
		predicted = math.MaxInt64
	} else {
		predicted += inputBatchSize
	}
	return colexec.ShouldSpill(
		predicted,
		int64(ctr.hashmapBuilder.InputBatchRowCount),
		ctr.spillThreshold,
	)
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// Each column is processed in a tight loop over all rows, avoiding
// per-row buffer concatenation and giving sequential vector access.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return
	}
	keycodec.ComputeXXHash(keyVecs, hashValues, 0)
}
