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

package fill

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const fillSpillMagic = uint64(0x46494c4c5350494c)

type fillSpill struct {
	input  *os.File
	output *os.File
	next   *os.File
	buf    bytes.Buffer

	outputReversePos int64
	ready            bool
	inputRecords     int
	outputRecords    int
	replay           *batch.Batch
	segmentPending   []int64
	segmentLeftValid []bool
	segmentStart     int64
	segmentRows      int64
	safeWatermark    int64
	safeRows         int64
	hasSuffix        bool
	linearLeft       []*vector.Vector
	linearLeftValid  []bool
	forwardPart      spillPartitionSnapshot
}

type spillPartitionSnapshot struct {
	keys  [][]byte
	nulls []bool
	set   bool
}

func (s *spillPartitionSnapshot) cloneFrom(src *spillPartitionSnapshot) {
	*s = spillPartitionSnapshot{set: src.set}
	if len(src.keys) > 0 {
		s.keys = make([][]byte, len(src.keys))
		for i := range src.keys {
			s.keys[i] = append([]byte(nil), src.keys[i]...)
		}
	}
	s.nulls = append([]bool(nil), src.nulls...)
}

func addOriginalNullMarkers(bat *batch.Batch, colLen int, mp *mpool.MPool) error {
	rows := bat.RowCount()
	for c := 0; c < colLen; c++ {
		marker := vector.NewVec(types.T_bool.ToType())
		values := make([]bool, rows)
		for r := 0; r < rows; r++ {
			values[r] = bat.Vecs[c].IsNull(uint64(r))
		}
		if err := vector.AppendFixedList(marker, values, nil, mp); err != nil {
			marker.Free(mp)
			return err
		}
		bat.Vecs = append(bat.Vecs, marker)
		bat.Attrs = append(bat.Attrs, "")
	}
	return nil
}

func stripOriginalNullMarkers(bat *batch.Batch, colLen int, mp *mpool.MPool) {
	if bat == nil || colLen == 0 || len(bat.Vecs) < colLen {
		return
	}
	start := len(bat.Vecs) - colLen
	for _, vec := range bat.Vecs[start:] {
		if vec != nil {
			vec.Free(mp)
		}
	}
	bat.Vecs = bat.Vecs[:start]
	if len(bat.Attrs) >= colLen {
		bat.Attrs = bat.Attrs[:len(bat.Attrs)-colLen]
	}
}

func originalNullAt(bat *batch.Batch, colLen, col, row int) bool {
	marker := bat.Vecs[len(bat.Vecs)-colLen+col]
	return vector.GetFixedAtNoTypeCheck[bool](marker, row)
}

func newFillSpill(proc *process.Process) (*fillSpill, error) {
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	input, err := fs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("fill_%s_in", uuid.NewString()))
	if err != nil {
		return nil, err
	}
	return &fillSpill{input: input}, nil
}

func (s *fillSpill) ensureOutput(proc *process.Process) error {
	if s.output != nil {
		return nil
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return err
	}
	s.output, err = fs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("fill_%s_out", uuid.NewString()))
	return err
}

func (s *fillSpill) ensureNext(proc *process.Process) error {
	if s.next != nil {
		return nil
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return err
	}
	s.next, err = fs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("fill_%s_next", uuid.NewString()))
	return err
}

func (s *fillSpill) writeRecord(fd *os.File, bat *batch.Batch) error {
	s.buf.Reset()
	var zero int64
	s.buf.Write(types.EncodeInt64(&zero))
	start := s.buf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(&s.buf, false); err != nil {
		return err
	}
	size := int64(s.buf.Len() - start)
	copy(s.buf.Bytes()[:8], types.EncodeInt64(&size))
	s.buf.Write(types.EncodeInt64(&size))
	magic := fillSpillMagic
	s.buf.Write(types.EncodeUint64(&magic))
	n, err := fd.Write(s.buf.Bytes())
	if err != nil {
		return err
	}
	if n != s.buf.Len() {
		return io.ErrShortWrite
	}
	return nil
}

func readRecordReverse(fd *os.File, pos *int64, mp *mpool.MPool, reuse *batch.Batch) (*batch.Batch, error) {
	if *pos < 0 {
		end, err := fd.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		*pos = end
	}
	if *pos == 0 {
		return nil, io.EOF
	}
	if *pos < 24 {
		return nil, moerr.NewInternalErrorNoCtx("truncated fill spill record")
	}
	var tail [16]byte
	if _, err := fd.ReadAt(tail[:], *pos-16); err != nil {
		return nil, err
	}
	size := types.DecodeInt64(tail[:8])
	if types.DecodeUint64(tail[8:]) != fillSpillMagic || size < 0 {
		return nil, moerr.NewInternalErrorNoCtx("corrupted fill spill record")
	}
	start := *pos - size - 24
	if start < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid fill spill record size")
	}
	var head [8]byte
	if _, err := fd.ReadAt(head[:], start); err != nil {
		return nil, err
	}
	if types.DecodeInt64(head[:]) != size {
		return nil, moerr.NewInternalErrorNoCtx("fill spill record length mismatch")
	}
	section := io.NewSectionReader(fd, start+8, size)
	allocated := reuse == nil
	if reuse == nil {
		reuse = batch.NewWithSize(0)
	} else {
		reuse.CleanOnlyData()
	}
	if err := reuse.UnmarshalFromReader(section, mp); err != nil {
		if allocated {
			reuse.Clean(mp)
		}
		return nil, err
	}
	consumed, err := section.Seek(0, io.SeekCurrent)
	if err != nil {
		if allocated {
			reuse.Clean(mp)
		}
		return nil, err
	}
	if consumed != size {
		if allocated {
			reuse.Clean(mp)
		}
		return nil, moerr.NewInternalErrorNoCtx("fill spill record payload length mismatch")
	}
	*pos = start
	return reuse, nil
}

func (s *fillSpill) close(proc *process.Process) {
	if s == nil {
		return
	}
	if s.input != nil {
		_ = s.input.Close()
		s.input = nil
	}
	if s.output != nil {
		_ = s.output.Close()
		s.output = nil
	}
	if s.next != nil {
		_ = s.next.Close()
		s.next = nil
	}
	if s.replay != nil {
		s.replay.Clean(proc.Mp())
		s.replay = nil
	}
	for _, vec := range s.linearLeft {
		if vec != nil {
			vec.Free(proc.Mp())
		}
	}
	s.linearLeft = nil
	s.linearLeftValid = nil
}

func (ctr *container) cleanupSpill(proc *process.Process) {
	if ctr.spill != nil {
		ctr.spill.close(proc)
		ctr.spill = nil
	}
}

func (s *spillPartitionSnapshot) sameAndSet(partIdx []int32, bat *batch.Batch, row int) bool {
	same := s.set
	if cap(s.keys) < len(partIdx) {
		s.keys = make([][]byte, len(partIdx))
		s.nulls = make([]bool, len(partIdx))
	}
	s.keys = s.keys[:len(partIdx)]
	s.nulls = s.nulls[:len(partIdx)]
	for i, col := range partIdx {
		value, isNull := partKeyAt(bat.Vecs[col], row)
		if s.set && (isNull != s.nulls[i] || (!isNull && !bytes.Equal(value, s.keys[i]))) {
			same = false
		}
		s.nulls[i] = isNull
		s.keys[i] = append(s.keys[i][:0], value...)
	}
	s.set = true
	return same
}

func makeEndpoint(vec *vector.Vector, row int, proc *process.Process) (*vector.Vector, error) {
	result := vector.NewVec(*vec.GetType())
	if err := appendValue(result, vec, row, proc); err != nil {
		result.Free(proc.Mp())
		return nil, err
	}
	return result, nil
}

func setEndpoint(dst **vector.Vector, src *vector.Vector, row int, proc *process.Process) error {
	if *dst == nil {
		var err error
		*dst, err = makeEndpoint(src, row, proc)
		return err
	}
	return setValue(*dst, src, 0, row, proc)
}

func clearEndpoints(valid []bool) {
	for i := range valid {
		valid[i] = false
	}
}

func cloneBatchWindow(bat *batch.Batch, start, end int, mp *mpool.MPool) (*batch.Batch, error) {
	result := batch.NewWithSize(len(bat.Vecs))
	result.Attrs = append(result.Attrs, bat.Attrs...)
	for i, vec := range bat.Vecs {
		cloned, err := vec.CloneWindow(start, end, mp)
		if err != nil {
			result.Clean(mp)
			return nil, err
		}
		result.SetVector(int32(i), cloned)
	}
	result.SetRowCount(end - start)
	return result, nil
}

func (s *fillSpill) transformReverse(ap *Fill, proc *process.Process) error {
	if err := s.ensureOutput(proc); err != nil {
		return err
	}
	pos := int64(-1)
	var reuse *batch.Batch
	next := make([]*vector.Vector, ap.ColLen)
	valid := make([]bool, ap.ColLen)
	defer func() {
		if reuse != nil {
			reuse.Clean(proc.Mp())
		}
		for _, vec := range next {
			if vec != nil {
				vec.Free(proc.Mp())
			}
		}
	}()
	var part spillPartitionSnapshot
	for {
		bat, err := readRecordReverse(s.input, &pos, proc.Mp(), reuse)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		reuse = bat
		for row := bat.RowCount() - 1; row >= 0; row-- {
			if len(ap.PartitionColIdx) > 0 && !part.sameAndSet(ap.PartitionColIdx, bat, row) {
				clearEndpoints(valid)
			}
			for col := 0; col < ap.ColLen; col++ {
				if originalNullAt(bat, ap.ColLen, col, row) {
					if valid[col] {
						if err = setValue(bat.Vecs[col], next[col], row, 0, proc); err != nil {
							return err
						}
					} else {
						bat.Vecs[col].GetNulls().Add(uint64(row))
					}
					continue
				}
				if err = setEndpoint(&next[col], bat.Vecs[col], row, proc); err != nil {
					return err
				}
				valid[col] = true
			}
		}
		if err = s.writeRecord(s.output, bat); err != nil {
			return err
		}
		s.outputRecords++
	}
	if _, err := s.output.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.outputReversePos = -1
	if ap.FillType == plan.Node_LINEAR {
		if len(s.linearLeft) < ap.ColLen {
			s.linearLeft = make([]*vector.Vector, ap.ColLen)
		}
		if len(s.linearLeftValid) < ap.ColLen {
			s.linearLeftValid = make([]bool, ap.ColLen)
		}
	}
	s.ready = true
	return nil
}

func (ctr *container) shouldSpillPending() bool {
	return colexec.ShouldSpill(ctr.pendingBytes, ctr.pendingRows, ctr.spillThreshold)
}

func (s *fillSpill) updateSafeWatermark() {
	watermark := s.segmentRows
	for _, pending := range s.segmentPending {
		if pending >= 0 && pending < watermark {
			watermark = pending
		}
	}
	if watermark > s.safeWatermark {
		s.safeWatermark = watermark
	}
}

// scanSegment advances the earliest unresolved logical row for every fill
// column. The minimum of those positions is a safe output watermark: later
// rows may still be pending, but they cannot change anything before it.
func (s *fillSpill) scanSegment(ctr *container, ap *Fill, bat *batch.Batch) {
	for row := 0; row < bat.RowCount(); row++ {
		logicalRow := s.segmentRows
		if ctr.isNewSegment(ap, bat, row) {
			for col := range s.segmentPending {
				s.segmentPending[col] = -1
				s.segmentLeftValid[col] = false
			}
		}
		for col := 0; col < ap.ColLen; col++ {
			isNull := originalNullAt(bat, ap.ColLen, col, row)
			switch ap.FillType {
			case plan.Node_NEXT:
				if isNull {
					if s.segmentPending[col] < 0 {
						s.segmentPending[col] = logicalRow
					}
				} else {
					s.segmentPending[col] = -1
				}
			case plan.Node_LINEAR:
				if isNull {
					if s.segmentLeftValid[col] && s.segmentPending[col] < 0 {
						s.segmentPending[col] = logicalRow
					}
				} else {
					s.segmentPending[col] = -1
					s.segmentLeftValid[col] = true
				}
			}
		}
		s.segmentRows++
		s.updateSafeWatermark()
	}
}

func (s *fillSpill) finalizeSegment(ctr *container, ap *Fill, proc *process.Process) error {
	s.safeRows = s.safeWatermark - s.segmentStart
	if err := s.transformReverse(ap, proc); err != nil {
		return err
	}
	if s.input != nil {
		_ = s.input.Close()
		s.input = nil
	}
	return nil
}

func (ctr *container) beginSpill(ap *Fill, proc *process.Process) error {
	spill, err := newFillSpill(proc)
	if err != nil {
		return err
	}
	spill.segmentPending = make([]int64, ap.ColLen)
	spill.segmentLeftValid = make([]bool, ap.ColLen)
	for _, bat := range ctr.bats {
		spill.segmentRows += int64(bat.RowCount())
	}
	coordOffset := func(coord fillCoord) int64 {
		var offset int64
		for seq := ctr.baseSeq; seq < coord.seq; seq++ {
			offset += int64(ctr.batAt(seq).RowCount())
		}
		return offset + int64(coord.row)
	}
	for col := 0; col < ap.ColLen; col++ {
		spill.segmentPending[col] = -1
		if ap.FillType == plan.Node_NEXT {
			if len(ctr.nextRun[col]) > 0 {
				coord := ctr.nextRun[col][0]
				spill.segmentPending[col] = coordOffset(coord)
			}
		} else {
			if len(ctr.linRun[col]) > 0 {
				coord := ctr.linRun[col][0]
				spill.segmentPending[col] = coordOffset(coord)
			}
			spill.segmentLeftValid[col] = ctr.linPre[col].seq >= 0 || ctr.linSeedValid[col]
		}
	}
	spill.updateSafeWatermark()
	if ap.FillType == plan.Node_LINEAR && len(ctr.linEntry) > 0 {
		spill.linearLeft = ctr.linEntry
		spill.linearLeftValid = ctr.linEntryValid
		spill.forwardPart.cloneFrom(&ctr.linEntryPart)
		ctr.linEntry = make([]*vector.Vector, ap.ColLen)
		ctr.linEntryValid = make([]bool, ap.ColLen)
		ctr.linEntryPart = spillPartitionSnapshot{}
	}
	for i, bat := range ctr.bats {
		if err = spill.writeRecord(spill.input, bat); err != nil {
			spill.close(proc)
			return err
		}
		spill.inputRecords++
		bat.Clean(proc.Mp())
		ctr.bats[i] = nil
	}
	ctr.bats = ctr.bats[:0]
	ctr.pendingBytes = 0
	ctr.pendingRows = 0
	ctr.flushable = 0
	ctr.baseSeq = 0
	if ap.FillType == plan.Node_NEXT {
		ctr.flushPendingRunsNext(ap)
	} else {
		ctr.flushPendingRunsLinear(ap)
	}
	ctr.spill = spill
	if spill.safeWatermark > spill.segmentStart {
		if err = spill.finalizeSegment(ctr, ap, proc); err != nil {
			ctr.cleanupSpill(proc)
			return err
		}
	}
	return nil
}

func (ctr *container) collectSpill(ap *Fill, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			ctr.childDone = true
			ctr.spill.safeWatermark = ctr.spill.segmentRows
			return ctr.spill.finalizeSegment(ctr, ap, proc)
		}
		dup, err := result.Batch.Dup(proc.Mp())
		if err != nil {
			return err
		}
		if err = addOriginalNullMarkers(dup, ap.ColLen, proc.Mp()); err != nil {
			dup.Clean(proc.Mp())
			return err
		}
		ctr.spill.scanSegment(ctr, ap, dup)
		if err = ctr.spill.writeRecord(ctr.spill.input, dup); err != nil {
			dup.Clean(proc.Mp())
			return err
		}
		ctr.spill.inputRecords++
		if dup.RowCount() > 0 && len(ap.PartitionColIdx) > 0 {
			ctr.snapshotPartKey(ap.PartitionColIdx, dup, dup.RowCount()-1)
		}
		if analyzer != nil {
			analyzer.Spill(int64(ctr.spill.buf.Len()))
			analyzer.SpillRows(int64(dup.RowCount()))
		}
		dup.Clean(proc.Mp())
		if ctr.spill.safeWatermark > ctr.spill.segmentStart {
			return ctr.spill.finalizeSegment(ctr, ap, proc)
		}
	}
}

func (s *fillSpill) replayNext(ctr *container, ap *Fill, proc *process.Process) (*batch.Batch, error) {
	if s.replay != nil {
		// The batch returned by the previous Call had its internal marker columns
		// stripped. Reusing it for a wider spilled record would make batch
		// UnmarshalFromReader clean the batch after setting RowCount, losing the
		// decoded count. Release it and keep the replay footprint at one fresh
		// batch instead.
		s.replay.Clean(proc.Mp())
		s.replay = nil
	}
	for {
		bat, err := readRecordReverse(s.output, &s.outputReversePos, proc.Mp(), nil)
		if err != nil {
			return nil, err
		}
		rows := int64(bat.RowCount())
		if s.safeRows <= 0 {
			if err = s.writeSuffix(proc, bat); err != nil {
				bat.Clean(proc.Mp())
				return nil, err
			}
			bat.Clean(proc.Mp())
			continue
		}
		if s.safeRows >= rows {
			s.safeRows -= rows
			s.replay = bat
			if ap.FillType == plan.Node_LINEAR {
				if err = s.finishLinearBatch(ctr, ap, proc, bat); err != nil {
					return nil, err
				}
			}
			stripOriginalNullMarkers(bat, ap.ColLen, proc.Mp())
			return bat, nil
		}

		end := int(s.safeRows)
		prefix, err := cloneBatchWindow(bat, 0, end, proc.Mp())
		if err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		suffix, err := cloneBatchWindow(bat, end, bat.RowCount(), proc.Mp())
		if err != nil {
			prefix.Clean(proc.Mp())
			bat.Clean(proc.Mp())
			return nil, err
		}
		bat.Clean(proc.Mp())
		if err = s.writeSuffix(proc, suffix); err != nil {
			prefix.Clean(proc.Mp())
			suffix.Clean(proc.Mp())
			return nil, err
		}
		suffix.Clean(proc.Mp())
		s.safeRows = 0
		s.replay = prefix
		if ap.FillType == plan.Node_LINEAR {
			if err = s.finishLinearBatch(ctr, ap, proc, prefix); err != nil {
				return nil, err
			}
		}
		stripOriginalNullMarkers(prefix, ap.ColLen, proc.Mp())
		return prefix, nil
	}
}

func (s *fillSpill) writeSuffix(proc *process.Process, bat *batch.Batch) error {
	if err := s.ensureNext(proc); err != nil {
		return err
	}
	if err := s.writeRecord(s.next, bat); err != nil {
		return err
	}
	s.hasSuffix = true
	return nil
}

func (s *fillSpill) rotateSuffix() {
	if s.output != nil {
		_ = s.output.Close()
		s.output = nil
	}
	s.input = s.next
	s.next = nil
	s.outputReversePos = -1
	s.ready = false
	s.hasSuffix = false
	s.segmentStart = s.safeWatermark
	s.safeRows = 0
	s.inputRecords = 0
	s.outputRecords = 0
}

func (ctr *container) finishSpillReplay(
	ap *Fill,
	proc *process.Process,
) error {
	spill := ctr.spill
	if spill.hasSuffix {
		spill.rotateSuffix()
		return nil
	}
	if ap.FillType == plan.Node_LINEAR {
		seed := make([]*vector.Vector, ap.ColLen)
		seedValid := make([]bool, ap.ColLen)
		for col := 0; col < ap.ColLen; col++ {
			if !spill.linearLeftValid[col] {
				continue
			}
			var err error
			seed[col], err = makeEndpoint(spill.linearLeft[col], 0, proc)
			if err != nil {
				for _, vec := range seed {
					if vec != nil {
						vec.Free(proc.Mp())
					}
				}
				return err
			}
			seedValid[col] = true
		}
		ctr.clearLinearSeeds(proc.Mp())
		ctr.clearLinearEntries(proc.Mp())
		ctr.linSeed = seed
		ctr.linSeedValid = seedValid
		ctr.linEntry = spill.linearLeft
		ctr.linEntryValid = spill.linearLeftValid
		ctr.linEntryPart.cloneFrom(&spill.forwardPart)
		spill.linearLeft = nil
		spill.linearLeftValid = nil
	}
	ctr.cleanupSpill(proc)
	return nil
}

func (s *fillSpill) finishLinearBatch(ctr *container, ap *Fill, proc *process.Process, bat *batch.Batch) error {
	for row := 0; row < bat.RowCount(); row++ {
		if len(ap.PartitionColIdx) > 0 {
			wasSet := s.forwardPart.set
			if !s.forwardPart.sameAndSet(ap.PartitionColIdx, bat, row) && wasSet {
				clearEndpoints(s.linearLeftValid)
			}
		}
		for col := 0; col < ap.ColLen; col++ {
			if !originalNullAt(bat, ap.ColLen, col, row) {
				if err := setEndpoint(&s.linearLeft[col], bat.Vecs[col], row, proc); err != nil {
					return err
				}
				s.linearLeftValid[col] = true
				continue
			}
			if bat.Vecs[col].IsNull(uint64(row)) || !s.linearLeftValid[col] {
				bat.Vecs[col].GetNulls().Add(uint64(row))
				continue
			}
			leftBatch := batch.NewWithSize(col + 1)
			leftBatch.SetVector(int32(col), s.linearLeft[col])
			leftBatch.SetRowCount(1)
			rightBatch := batch.NewWithSize(col + 1)
			rightBatch.SetVector(int32(col), bat.Vecs[col])
			rightBatch.SetRowCount(bat.RowCount())
			value, owned, err := linearFillValue(ctr, proc, col, leftBatch, 0, rightBatch, row)
			if err != nil {
				return err
			}
			if err = setValue(bat.Vecs[col], value, row, 0, proc); err != nil {
				if owned {
					value.Free(proc.Mp())
				}
				return err
			}
			if owned {
				value.Free(proc.Mp())
			}
		}
	}
	return nil
}
