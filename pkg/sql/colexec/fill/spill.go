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
	buf    bytes.Buffer

	outputReversePos int64
	ready            bool
	inputRecords     int
	outputRecords    int
	replay           *batch.Batch
	carry            *batch.Batch
	segmentWait      []bool
	segmentLeftValid []bool
	linearLeft       []*vector.Vector
	linearLeftValid  []bool
	forwardPart      spillPartitionSnapshot
}

type spillPartitionSnapshot struct {
	keys  [][]byte
	nulls []bool
	set   bool
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
	if s.replay != nil {
		s.replay.Clean(proc.Mp())
		s.replay = nil
	}
	if s.carry != nil {
		s.carry.Clean(proc.Mp())
		s.carry = nil
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

func allResolved(wait []bool) bool {
	for _, pending := range wait {
		if pending {
			return false
		}
	}
	return true
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

// scanSegment returns the exclusive end row that closes the current spill
// segment. A partition boundary closes the old segment before its first row;
// otherwise the segment closes after a row once no fill column has a pending
// dependency on unseen input.
func (s *fillSpill) scanSegment(ctr *container, ap *Fill, bat *batch.Batch) int {
	for row := 0; row < bat.RowCount(); row++ {
		if ctr.isNewSegment(ap, bat, row) {
			return row
		}
		for col := 0; col < ap.ColLen; col++ {
			isNull := originalNullAt(bat, ap.ColLen, col, row)
			switch ap.FillType {
			case plan.Node_NEXT:
				s.segmentWait[col] = isNull
			case plan.Node_LINEAR:
				if isNull {
					s.segmentWait[col] = s.segmentLeftValid[col]
				} else {
					s.segmentWait[col] = false
					s.segmentLeftValid[col] = true
				}
			}
		}
		if allResolved(s.segmentWait) {
			return row + 1
		}
	}
	return -1
}

func (s *fillSpill) finalizeSegment(ctr *container, ap *Fill, proc *process.Process) error {
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
	spill.segmentWait = make([]bool, ap.ColLen)
	spill.segmentLeftValid = make([]bool, ap.ColLen)
	for col := 0; col < ap.ColLen; col++ {
		if ap.FillType == plan.Node_NEXT {
			spill.segmentWait[col] = len(ctr.nextRun[col]) > 0
		} else {
			spill.segmentWait[col] = len(ctr.linRun[col]) > 0
			spill.segmentLeftValid[col] = ctr.linPre[col].seq >= 0 || ctr.linSeedValid[col]
		}
	}
	if ap.FillType == plan.Node_LINEAR && len(ctr.linSeed) > 0 {
		spill.linearLeft = ctr.linSeed
		spill.linearLeftValid = ctr.linSeedValid
		ctr.linSeed = make([]*vector.Vector, ap.ColLen)
		ctr.linSeedValid = make([]bool, ap.ColLen)
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
	if allResolved(spill.segmentWait) {
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
		end := ctr.spill.scanSegment(ctr, ap, dup)
		toSpill := dup
		if end >= 0 && end < dup.RowCount() {
			if end == 0 {
				ctr.spill.carry = dup
				toSpill = nil
			} else {
				prefix, cloneErr := cloneBatchWindow(dup, 0, end, proc.Mp())
				if cloneErr != nil {
					dup.Clean(proc.Mp())
					return cloneErr
				}
				carry, cloneErr := cloneBatchWindow(dup, end, dup.RowCount(), proc.Mp())
				if cloneErr != nil {
					prefix.Clean(proc.Mp())
					dup.Clean(proc.Mp())
					return cloneErr
				}
				dup.Clean(proc.Mp())
				toSpill = prefix
				ctr.spill.carry = carry
			}
		}
		if toSpill != nil {
			if err = ctr.spill.writeRecord(ctr.spill.input, toSpill); err != nil {
				toSpill.Clean(proc.Mp())
				return err
			}
			ctr.spill.inputRecords++
			if toSpill.RowCount() > 0 && len(ap.PartitionColIdx) > 0 {
				ctr.snapshotPartKey(ap.PartitionColIdx, toSpill, toSpill.RowCount()-1)
			}
		}
		if analyzer != nil && toSpill != nil {
			analyzer.Spill(int64(ctr.spill.buf.Len()))
			analyzer.SpillRows(int64(toSpill.RowCount()))
		}
		if toSpill != nil {
			toSpill.Clean(proc.Mp())
		}
		if end >= 0 {
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
	bat, err := readRecordReverse(s.output, &s.outputReversePos, proc.Mp(), s.replay)
	if err != nil {
		return nil, err
	}
	s.replay = bat
	if ap.FillType == plan.Node_LINEAR {
		if err = s.finishLinearBatch(ctr, ap, proc, bat); err != nil {
			return nil, err
		}
	}
	stripOriginalNullMarkers(bat, ap.ColLen, proc.Mp())
	return bat, nil
}

func (ctr *container) finishSpillReplay(
	ap *Fill,
	proc *process.Process,
	consume func(*container, *Fill, *batch.Batch, int, *process.Process) error,
) error {
	spill := ctr.spill
	carry := spill.carry
	spill.carry = nil
	if ap.FillType == plan.Node_LINEAR {
		ctr.clearLinearSeeds(proc.Mp())
		ctr.linSeed = spill.linearLeft
		ctr.linSeedValid = spill.linearLeftValid
		spill.linearLeft = nil
		spill.linearLeftValid = nil
	}
	ctr.cleanupSpill(proc)
	if carry == nil {
		return nil
	}
	seq := ctr.baseSeq + len(ctr.bats)
	ctr.bats = append(ctr.bats, carry)
	ctr.pendingBytes += int64(carry.Size())
	ctr.pendingRows += int64(carry.RowCount())
	if err := consume(ctr, ap, carry, seq, proc); err != nil {
		return err
	}
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
