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
	"math"
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const fillSpillMagic = uint64(0x46494c4c5350494c)
const fillSpillWriteBufferSize = 64 << 10

type fillSpillFile struct {
	file      *os.File
	writer    *spillutil.AccountedWriter
	fdToken   *process.ExecutionSpillFDReservation
	diskToken *process.ExecutionSpillDiskReservation
}

func newFillSpillFile(
	proc *process.Process,
	ctr *container,
	suffix string,
) (*fillSpillFile, error) {
	if proc == nil || ctr == nil || (ctr.allocationAccount != nil && ctr.budget == nil) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	var (
		fdToken   *process.ExecutionSpillFDReservation
		diskToken *process.ExecutionSpillDiskReservation
		err       error
	)
	if ctr.budget != nil {
		fdToken, err = ctr.budget.ReserveSpillFD(1)
		if err != nil {
			return nil, err
		}
		diskToken, err = ctr.budget.ReserveSpillDisk(0)
		if err != nil {
			fdToken.Release()
			return nil, err
		}
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	fd, err := fs.CreateAndRemoveFile(
		proc.Ctx,
		fmt.Sprintf("fill_%s_%s", uuid.NewString(), suffix),
	)
	if err != nil {
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	writer, err := spillutil.NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		ctr.allocationAccount,
		mpool.AllocationOwnerFill,
		fillAllocationSiteSpillWriteBuffer,
		spillutil.NewDiskReservationWriter(fd, diskToken),
		fillSpillWriteBufferSize,
	)
	if err != nil {
		_ = fd.Close()
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	return &fillSpillFile{
		file:      fd,
		writer:    writer,
		fdToken:   fdToken,
		diskToken: diskToken,
	}, nil
}

func (f *fillSpillFile) finishWriting() error {
	if f == nil || f.writer == nil {
		return nil
	}
	writer := f.writer
	f.writer = nil
	err := writer.Flush()
	writer.Free()
	return err
}

func (f *fillSpillFile) flush() error {
	if f == nil || f.writer == nil {
		return nil
	}
	return f.writer.Flush()
}

func (f *fillSpillFile) close() error {
	if f == nil {
		return nil
	}
	if f.writer != nil {
		f.writer.Free()
		f.writer = nil
	}
	var err error
	if f.file != nil {
		err = f.file.Close()
		f.file = nil
	}
	if f.diskToken != nil {
		f.diskToken.Release()
		f.diskToken = nil
	}
	if f.fdToken != nil {
		f.fdToken.Release()
		f.fdToken = nil
	}
	return err
}

type fillSpill struct {
	input  *fillSpillFile
	output *fillSpillFile
	next   *fillSpillFile

	allocation *spillutil.SpillAllocationAccount

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
	linearLeftSteps  []uint64
	forwardPart      spillPartitionSnapshot
	scanPart         spillPartitionSnapshot
}

type spillPartitionSnapshot struct {
	keys    [][]byte
	buffers []*mpool.AccountedBuffer
	nulls   []bool
	set     bool
	mp      *mpool.MPool
	account *mpool.AllocationAccount
	site    mpool.AllocationSite
}

func (s *spillPartitionSnapshot) configure(
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	site mpool.AllocationSite,
) {
	if s == nil || account == nil {
		return
	}
	s.mp = mp
	s.account = account
	s.site = site
}

func (s *spillPartitionSnapshot) hasCapacity() bool {
	if s == nil {
		return false
	}
	for i := range s.keys {
		if cap(s.keys[i]) != 0 {
			return true
		}
	}
	return false
}

func (s *spillPartitionSnapshot) free() {
	if s == nil {
		return
	}
	for _, buffer := range s.buffers {
		if buffer != nil {
			buffer.Free()
		}
	}
	*s = spillPartitionSnapshot{}
}

func (s *spillPartitionSnapshot) ensureShape(length int) {
	if cap(s.keys) < length {
		keys := make([][]byte, length)
		nulls := make([]bool, length)
		copy(keys, s.keys)
		copy(nulls, s.nulls)
		s.keys = keys
		s.nulls = nulls
		if s.account != nil {
			buffers := make([]*mpool.AccountedBuffer, length)
			copy(buffers, s.buffers)
			s.buffers = buffers
		}
	} else {
		s.keys = s.keys[:length]
		s.nulls = s.nulls[:length]
		if s.account != nil {
			if length < len(s.buffers) {
				for _, buffer := range s.buffers[length:] {
					if buffer != nil {
						buffer.Free()
					}
				}
			}
			if cap(s.buffers) < length {
				next := make([]*mpool.AccountedBuffer, length)
				copy(next, s.buffers)
				s.buffers = next
			} else {
				s.buffers = s.buffers[:length]
			}
		}
	}
}

func (s *spillPartitionSnapshot) setKey(index int, value []byte) error {
	if s.account == nil {
		s.keys[index] = append(s.keys[index][:0], value...)
		return nil
	}
	if s.buffers[index] == nil {
		buffer, err := mpool.NewAccountedBuffer(
			s.mp,
			s.account,
			mpool.AllocationOwnerFill,
			s.site,
		)
		if err != nil {
			return err
		}
		s.buffers[index] = buffer
	}
	s.buffers[index].Reset()
	if _, err := s.buffers[index].Write(value); err != nil {
		return err
	}
	s.keys[index] = s.buffers[index].Bytes()
	return nil
}

func (s *spillPartitionSnapshot) cloneFrom(src *spillPartitionSnapshot) error {
	if s == nil || src == nil || s == src {
		return mpool.ErrAllocationAccountInvalid
	}
	s.ensureShape(len(src.keys))
	for i := range src.keys {
		s.nulls[i] = src.nulls[i]
		if err := s.setKey(i, src.keys[i]); err != nil {
			return err
		}
	}
	s.set = src.set
	return nil
}

func addOriginalNullMarkers(
	bat *batch.Batch,
	colLen int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) error {
	rows := bat.RowCount()
	vecCount := len(bat.Vecs)
	attrCount := len(bat.Attrs)
	rollback := func() {
		for _, marker := range bat.Vecs[vecCount:] {
			if marker != nil {
				marker.Free(mp)
			}
		}
		bat.Vecs = bat.Vecs[:vecCount]
		bat.Attrs = bat.Attrs[:attrCount]
	}
	for c := 0; c < colLen; c++ {
		marker := vector.NewVec(types.T_bool.ToType())
		if selection != nil {
			marker = vector.NewOffHeapVecWithType(types.T_bool.ToType())
			if err := marker.SetAllocationAccount(selection); err != nil {
				marker.Free(mp)
				rollback()
				return err
			}
		}
		bat.Vecs = append(bat.Vecs, nil)
		bat.Attrs = append(bat.Attrs, "")
		bat.SetVector(int32(len(bat.Vecs)-1), marker)
		if err := marker.PreExtend(rows, mp); err != nil {
			rollback()
			return err
		}
		marker.SetLength(rows)
		values := vector.MustFixedColWithTypeCheck[bool](marker)
		for r := 0; r < rows; r++ {
			values[r] = bat.Vecs[c].IsNull(uint64(r))
		}
	}
	return nil
}

// addLinearDistanceMarkers inserts one uint64 marker per fill column before
// the original-NULL markers. The reverse spill pass stores how many missing
// positions separate a row from its right endpoint; originalNullAt can keep
// addressing the final bool marker block unchanged.
func addLinearDistanceMarkers(
	bat *batch.Batch,
	colLen int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) error {
	if colLen == 0 {
		return nil
	}
	markers, err := makeLinearDistanceMarkers(bat.RowCount(), colLen, mp, selection)
	if err != nil {
		return err
	}
	originalStart := len(bat.Vecs) - colLen
	bat.Vecs = append(bat.Vecs, make([]*vector.Vector, colLen)...)
	copy(bat.Vecs[originalStart+colLen:], bat.Vecs[originalStart:originalStart+colLen])
	copy(bat.Vecs[originalStart:], markers)
	bat.Attrs = append(bat.Attrs, make([]string, colLen)...)
	return nil
}

func makeLinearDistanceMarkers(
	rows, colLen int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) ([]*vector.Vector, error) {
	markers := make([]*vector.Vector, colLen)
	for col := range markers {
		marker := vector.NewVec(types.T_uint64.ToType())
		if selection != nil {
			marker = vector.NewOffHeapVecWithType(types.T_uint64.ToType())
			if err := marker.SetAllocationAccount(selection); err != nil {
				marker.Free(mp)
				for _, allocated := range markers[:col] {
					allocated.Free(mp)
				}
				return nil, err
			}
		}
		if err := vector.AppendMultiFixed(marker, uint64(0), false, rows, mp); err != nil {
			marker.Free(mp)
			for _, allocated := range markers[:col] {
				allocated.Free(mp)
			}
			return nil, err
		}
		markers[col] = marker
	}
	return markers, nil
}

func makeBorrowedSpillBatch(
	source *batch.Batch,
	colLen int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if source == nil || colLen < 0 || colLen > len(source.Vecs) {
		return nil, moerr.NewInvalidInputNoCtx("invalid fill spill input batch")
	}
	bat := batch.NewOffHeapWithSize(len(source.Vecs))
	copy(bat.Vecs, source.Vecs)
	bat.Recursive = source.Recursive
	bat.ShuffleIDX = source.ShuffleIDX
	bat.SetRowCount(source.RowCount())
	if err := addOriginalNullMarkers(bat, colLen, mp, selection); err != nil {
		clear(bat.Vecs)
		bat.Vecs = nil
		bat.Attrs = nil
		bat.SetRowCount(0)
		return nil, err
	}
	return bat, nil
}

func releaseBorrowedSpillBatch(
	bat *batch.Batch,
	colLen int,
	linear bool,
	mp *mpool.MPool,
) {
	if bat == nil {
		return
	}
	stripOriginalNullMarkers(bat, colLen, mp)
	if linear {
		stripLinearDistanceMarkers(bat, colLen, mp)
	}
	clear(bat.Vecs)
	bat.Vecs = nil
	bat.Attrs = nil
	bat.SetRowCount(0)
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

func stripLinearDistanceMarkers(bat *batch.Batch, colLen int, mp *mpool.MPool) {
	stripOriginalNullMarkers(bat, colLen, mp)
}

func originalNullAt(bat *batch.Batch, colLen, col, row int) bool {
	marker := bat.Vecs[len(bat.Vecs)-colLen+col]
	return vector.GetFixedAtNoTypeCheck[bool](marker, row)
}

func linearRightDistanceAt(bat *batch.Batch, colLen, col, row int) uint64 {
	marker := bat.Vecs[len(bat.Vecs)-2*colLen+col]
	return vector.GetFixedAtNoTypeCheck[uint64](marker, row)
}

func setLinearRightDistance(bat *batch.Batch, colLen, col, row int, distance uint64) error {
	marker := bat.Vecs[len(bat.Vecs)-2*colLen+col]
	return vector.SetFixedAtNoTypeCheck(marker, row, distance)
}

func newFillSpill(ctr *container, proc *process.Process) (*fillSpill, error) {
	input, err := newFillSpillFile(proc, ctr, "in")
	if err != nil {
		return nil, err
	}
	return &fillSpill{
		input:      input,
		allocation: ctr.spillAllocation,
	}, nil
}

func (s *fillSpill) ensureOutput(ctr *container, proc *process.Process) error {
	if s.output != nil {
		return nil
	}
	var err error
	s.output, err = newFillSpillFile(proc, ctr, "out")
	return err
}

func (s *fillSpill) ensureNext(ctr *container, proc *process.Process) error {
	if s.next != nil {
		return nil
	}
	var err error
	s.next, err = newFillSpillFile(proc, ctr, "next")
	return err
}

func (s *fillSpill) writeRecord(
	proc *process.Process,
	file *fillSpillFile,
	bat *batch.Batch,
) (int64, error) {
	if file == nil || file.writer == nil || bat == nil {
		return 0, io.ErrClosedPipe
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return 0, err
	}
	var wire batch.Batch
	wire.Vecs = bat.Vecs
	wire.Recursive = bat.Recursive
	wire.ShuffleIDX = bat.ShuffleIDX
	wire.SetRowCount(bat.RowCount())
	payloadSize, err := wire.MarshalBinaryWithPrepareParamKindsSize()
	if err != nil {
		return 0, err
	}
	if payloadSize < 0 || payloadSize > math.MaxInt-24 {
		return 0, moerr.NewInvalidInputNoCtx("fill spill payload exceeds format")
	}
	size := int64(payloadSize)
	if err = writeFillBytes(file.writer, types.EncodeInt64(&size)); err != nil {
		return 0, err
	}
	if err = wire.MarshalBinaryWithPrepareParamKindsTo(file.writer); err != nil {
		return 0, err
	}
	if err = writeFillBytes(file.writer, types.EncodeInt64(&size)); err != nil {
		return 0, err
	}
	magic := fillSpillMagic
	if err = writeFillBytes(file.writer, types.EncodeUint64(&magic)); err != nil {
		return 0, err
	}
	return size + 24, nil
}

func writeFillBytes(writer io.Writer, value []byte) error {
	n, err := writer.Write(value)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *fillSpill) readRecordReverse(
	file *fillSpillFile,
	pos *int64,
	mp *mpool.MPool,
	reuse *batch.Batch,
) (*batch.Batch, error) {
	if file == nil || file.file == nil {
		return nil, io.ErrClosedPipe
	}
	fd := file.file
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
	if size > *pos-24 {
		return nil, moerr.NewInternalErrorNoCtx("invalid fill spill record size")
	}
	start := *pos - size - 24
	var head [8]byte
	if _, err := fd.ReadAt(head[:], start); err != nil {
		return nil, err
	}
	if types.DecodeInt64(head[:]) != size {
		return nil, moerr.NewInternalErrorNoCtx("fill spill record length mismatch")
	}
	allocated := reuse == nil
	if reuse == nil {
		reuse = batch.NewOffHeapWithSize(0)
		if s.allocation != nil {
			if err := s.allocation.ConfigureDecodedBatch(reuse); err != nil {
				reuse.Clean(mp)
				return nil, err
			}
		}
	} else {
		reuse.CleanOnlyData()
	}
	section := io.NewSectionReader(fd, start+8, size)
	if err := reuse.UnmarshalFromReaderWithPrepareParamKindsForSpill(section, size, mp); err != nil {
		if allocated {
			reuse.Clean(mp)
		}
		return nil, err
	}
	*pos = start
	return reuse, nil
}

func (s *fillSpill) close(proc *process.Process) {
	if s == nil {
		return
	}
	if s.input != nil {
		_ = s.input.close()
		s.input = nil
	}
	if s.output != nil {
		_ = s.output.close()
		s.output = nil
	}
	if s.next != nil {
		_ = s.next.close()
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
	s.linearLeftSteps = nil
	s.forwardPart.free()
	s.scanPart.free()
}

func (ctr *container) cleanupSpill(proc *process.Process) {
	if ctr.spill != nil {
		ctr.spill.close(proc)
		ctr.spill = nil
	}
}

func (s *spillPartitionSnapshot) sameAndSet(
	partIdx []int32,
	bat *batch.Batch,
	row int,
) (bool, error) {
	same := s.set
	s.ensureShape(len(partIdx))
	for i, col := range partIdx {
		value, isNull := partKeyAt(bat.Vecs[col], row)
		if s.set && (isNull != s.nulls[i] || (!isNull && !bytes.Equal(value, s.keys[i]))) {
			same = false
		}
		s.nulls[i] = isNull
		if err := s.setKey(i, value); err != nil {
			return false, err
		}
	}
	s.set = true
	return same, nil
}

func makeEndpoint(
	vec *vector.Vector,
	row int,
	proc *process.Process,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	result := vector.NewVec(*vec.GetType())
	if selection != nil {
		result = vector.NewOffHeapVecWithType(*vec.GetType())
		if err := result.SetAllocationAccount(selection); err != nil {
			result.Free(proc.Mp())
			return nil, err
		}
	}
	if err := appendValue(result, vec, row, proc); err != nil {
		result.Free(proc.Mp())
		return nil, err
	}
	return result, nil
}

func setEndpoint(
	dst **vector.Vector,
	src *vector.Vector,
	row int,
	proc *process.Process,
	selection *vector.AllocationAccountSelection,
) error {
	if *dst == nil {
		var err error
		*dst, err = makeEndpoint(src, row, proc, selection)
		return err
	}
	return setValue(*dst, src, 0, row, proc)
}

func clearEndpoints(valid []bool) {
	for i := range valid {
		valid[i] = false
	}
}

func cloneBatchWindow(
	bat *batch.Batch,
	start, end int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	result := batch.NewWithSize(len(bat.Vecs))
	if selection != nil {
		result = batch.NewOffHeapWithSize(len(bat.Vecs))
		if err := result.SetAllocationAccount(selection); err != nil {
			result.Clean(mp)
			return nil, err
		}
	}
	result.Attrs = append(result.Attrs, bat.Attrs...)
	for i, vec := range bat.Vecs {
		cloned, err := vec.CloneWindowWithAllocation(start, end, mp, selection)
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
	ctr := &ap.ctr
	if err := s.input.finishWriting(); err != nil {
		return err
	}
	if err := s.ensureOutput(ctr, proc); err != nil {
		return err
	}
	pos := int64(-1)
	var reuse *batch.Batch
	next := make([]*vector.Vector, ap.ColLen)
	valid := make([]bool, ap.ColLen)
	rightSteps := make([]uint64, ap.ColLen)
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
	part.configure(
		proc.Mp(),
		ctr.allocationAccount,
		fillAllocationSitePartitionSnapshot,
	)
	defer part.free()
	for {
		bat, err := s.readRecordReverse(s.input, &pos, proc.Mp(), reuse)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		reuse = bat
		for row := bat.RowCount() - 1; row >= 0; row-- {
			if row&255 == 0 {
				if cancelErr, canceled := vm.CancelCheck(proc); canceled {
					return cancelErr
				}
			}
			if len(ap.PartitionColIdx) > 0 {
				same, partErr := part.sameAndSet(ap.PartitionColIdx, bat, row)
				if partErr != nil {
					return partErr
				}
				if !same {
					clearEndpoints(valid)
					if ap.FillType == plan.Node_LINEAR {
						clear(rightSteps)
					}
				}
			}
			for col := 0; col < ap.ColLen; col++ {
				if originalNullAt(bat, ap.ColLen, col, row) {
					if valid[col] {
						if ap.FillType == plan.Node_LINEAR {
							rightSteps[col]++
							if err = setLinearRightDistance(bat, ap.ColLen, col, row, rightSteps[col]); err != nil {
								return err
							}
						}
						if err = setValue(bat.Vecs[col], next[col], row, 0, proc); err != nil {
							return err
						}
					} else {
						if ap.FillType == plan.Node_LINEAR {
							if err = setLinearRightDistance(bat, ap.ColLen, col, row, 0); err != nil {
								return err
							}
						}
						bat.Vecs[col].GetNulls().Add(uint64(row))
					}
					continue
				}
				if ap.FillType == plan.Node_LINEAR {
					rightSteps[col] = 0
				}
				if err = setEndpoint(
					&next[col], bat.Vecs[col], row, proc, ctr.retainedAllocation,
				); err != nil {
					return err
				}
				valid[col] = true
			}
		}
		if _, err = s.writeRecord(proc, s.output, bat); err != nil {
			return err
		}
		s.outputRecords++
	}
	if err := s.output.finishWriting(); err != nil {
		return err
	}
	if _, err := s.output.file.Seek(0, io.SeekEnd); err != nil {
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
		if len(s.linearLeftSteps) < ap.ColLen {
			s.linearLeftSteps = make([]uint64, ap.ColLen)
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
func (s *fillSpill) scanSegment(
	ap *Fill,
	bat *batch.Batch,
	proc *process.Process,
) error {
	for row := 0; row < bat.RowCount(); row++ {
		if row&255 == 0 {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return err
			}
		}
		logicalRow := s.segmentRows
		newSegment := false
		if len(ap.PartitionColIdx) > 0 {
			wasSet := s.scanPart.set
			same, err := s.scanPart.sameAndSet(ap.PartitionColIdx, bat, row)
			if err != nil {
				return err
			}
			newSegment = wasSet && !same
		}
		if newSegment {
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
	return nil
}

func (s *fillSpill) finalizeSegment(ctr *container, ap *Fill, proc *process.Process) error {
	s.safeRows = s.safeWatermark - s.segmentStart
	if err := s.transformReverse(ap, proc); err != nil {
		return err
	}
	if s.input != nil {
		_ = s.input.close()
		s.input = nil
	}
	return nil
}

func (ctr *container) beginSpill(
	ap *Fill,
	proc *process.Process,
	analyzer process.Analyzer,
	finalizeReadyPrefix bool,
) error {
	spill, err := newFillSpill(ctr, proc)
	if err != nil {
		return err
	}
	spill.segmentPending = make([]int64, ap.ColLen)
	spill.segmentLeftValid = make([]bool, ap.ColLen)
	for col := 0; col < ap.ColLen; col++ {
		spill.segmentPending[col] = -1
		if ap.FillType == plan.Node_LINEAR && col < len(ctr.linEntryValid) {
			spill.segmentLeftValid[col] = ctr.linEntryValid[col]
		}
	}
	spill.scanPart.configure(
		proc.Mp(),
		ctr.allocationAccount,
		fillAllocationSitePartitionSnapshot,
	)
	spill.forwardPart.configure(
		proc.Mp(),
		ctr.allocationAccount,
		fillAllocationSitePartitionSnapshot,
	)
	if ap.FillType == plan.Node_LINEAR && len(ctr.linEntry) > 0 {
		if err = spill.scanPart.cloneFrom(&ctr.linEntryPart); err != nil {
			spill.close(proc)
			return err
		}
		if err = spill.forwardPart.cloneFrom(&ctr.linEntryPart); err != nil {
			spill.close(proc)
			return err
		}
		spill.linearLeft = ctr.linEntry
		spill.linearLeftValid = ctr.linEntryValid
		ctr.linEntry = make([]*vector.Vector, ap.ColLen)
		ctr.linEntryValid = make([]bool, ap.ColLen)
		ctr.linEntryPart.free()
		ctr.linEntryPart.configure(
			proc.Mp(),
			ctr.allocationAccount,
			fillAllocationSitePartitionSnapshot,
		)
	}
	for i, bat := range ctr.bats {
		if ap.FillType == plan.Node_LINEAR {
			if err = addLinearDistanceMarkers(
				bat, ap.ColLen, proc.Mp(), ctr.retainedAllocation,
			); err != nil {
				spill.close(proc)
				return err
			}
		}
		if err = spill.scanSegment(ap, bat, proc); err != nil {
			spill.close(proc)
			return err
		}
		written, writeErr := spill.writeRecord(proc, spill.input, bat)
		if writeErr != nil {
			spill.close(proc)
			return writeErr
		}
		spill.inputRecords++
		if analyzer != nil {
			analyzer.Spill(written)
			analyzer.SpillRows(int64(bat.RowCount()))
		}
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
	if ctr.allocationAccount != nil {
		ctr.freeCoordRuns(proc.Mp())
	}
	ctr.spill = spill
	if finalizeReadyPrefix && spill.safeWatermark > spill.segmentStart {
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
		if err = ctr.appendSpillSource(ap, proc, analyzer, result.Batch); err != nil {
			return err
		}
		if ctr.spill.safeWatermark > ctr.spill.segmentStart {
			return ctr.spill.finalizeSegment(ctr, ap, proc)
		}
	}
}

func (ctr *container) appendSpillSource(
	ap *Fill,
	proc *process.Process,
	analyzer process.Analyzer,
	source *batch.Batch,
) error {
	if ctr.spill == nil || ctr.spill.input == nil {
		return moerr.NewInternalErrorNoCtx("fill spill input is closed")
	}
	borrowed, err := makeBorrowedSpillBatch(
		source,
		ap.ColLen,
		proc.Mp(),
		ctr.retainedAllocation,
	)
	if err != nil {
		return err
	}
	linearMarkers := false
	defer func() {
		releaseBorrowedSpillBatch(borrowed, ap.ColLen, linearMarkers, proc.Mp())
	}()
	if ap.FillType == plan.Node_LINEAR {
		// Distance markers precede the original-NULL marker block so the
		// latter remains the final ColLen vectors in every spilled record.
		if err = addLinearDistanceMarkers(
			borrowed, ap.ColLen, proc.Mp(), ctr.retainedAllocation,
		); err != nil {
			return err
		}
		linearMarkers = true
	}
	if err = ctr.spill.scanSegment(ap, borrowed, proc); err != nil {
		return err
	}
	written, err := ctr.spill.writeRecord(proc, ctr.spill.input, borrowed)
	if err != nil {
		return err
	}
	ctr.spill.inputRecords++
	if borrowed.RowCount() > 0 && len(ap.PartitionColIdx) > 0 {
		if err = ctr.snapshotPartKey(
			ap.PartitionColIdx, borrowed, borrowed.RowCount()-1, proc,
		); err != nil {
			return err
		}
	}
	if analyzer != nil {
		analyzer.Spill(written)
		analyzer.SpillRows(int64(borrowed.RowCount()))
	}
	return nil
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
		bat, err := s.readRecordReverse(s.output, &s.outputReversePos, proc.Mp(), nil)
		if err != nil {
			return nil, err
		}
		rows := int64(bat.RowCount())
		if s.safeRows <= 0 {
			if err = s.writeSuffix(ctr, proc, bat); err != nil {
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
			if ap.FillType == plan.Node_LINEAR {
				stripLinearDistanceMarkers(bat, ap.ColLen, proc.Mp())
			}
			return bat, nil
		}

		end := int(s.safeRows)
		prefix, err := cloneBatchWindow(
			bat, 0, end, proc.Mp(), ctr.outputAllocation,
		)
		if err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		suffix, err := cloneBatchWindow(
			bat, end, bat.RowCount(), proc.Mp(), ctr.retainedAllocation,
		)
		if err != nil {
			prefix.Clean(proc.Mp())
			bat.Clean(proc.Mp())
			return nil, err
		}
		bat.Clean(proc.Mp())
		if err = s.writeSuffix(ctr, proc, suffix); err != nil {
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
		if ap.FillType == plan.Node_LINEAR {
			stripLinearDistanceMarkers(prefix, ap.ColLen, proc.Mp())
		}
		return prefix, nil
	}
}

func (s *fillSpill) writeSuffix(
	ctr *container,
	proc *process.Process,
	bat *batch.Batch,
) error {
	if err := s.ensureNext(ctr, proc); err != nil {
		return err
	}
	if _, err := s.writeRecord(proc, s.next, bat); err != nil {
		return err
	}
	s.hasSuffix = true
	return nil
}

func (s *fillSpill) rotateSuffix() error {
	if s.output != nil {
		_ = s.output.close()
		s.output = nil
	}
	if err := s.next.flush(); err != nil {
		return err
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
	return nil
}

func (ctr *container) finishSpillReplay(
	ap *Fill,
	proc *process.Process,
) error {
	spill := ctr.spill
	if spill.hasSuffix {
		return spill.rotateSuffix()
	}
	if ap.FillType == plan.Node_LINEAR {
		seed := make([]*vector.Vector, ap.ColLen)
		seedValid := make([]bool, ap.ColLen)
		var entryPart spillPartitionSnapshot
		entryPart.configure(
			proc.Mp(),
			ctr.allocationAccount,
			fillAllocationSitePartitionSnapshot,
		)
		for col := 0; col < ap.ColLen; col++ {
			if !spill.linearLeftValid[col] {
				continue
			}
			var err error
			seed[col], err = makeEndpoint(
				spill.linearLeft[col], 0, proc, ctr.retainedAllocation,
			)
			if err != nil {
				for _, vec := range seed {
					if vec != nil {
						vec.Free(proc.Mp())
					}
				}
				entryPart.free()
				return err
			}
			seedValid[col] = true
		}
		if err := entryPart.cloneFrom(&spill.forwardPart); err != nil {
			for _, vec := range seed {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			entryPart.free()
			return err
		}
		ctr.clearLinearSeeds(proc.Mp())
		ctr.clearLinearEntries(proc.Mp())
		ctr.linSeed = seed
		ctr.linSeedValid = seedValid
		ctr.linEntry = spill.linearLeft
		ctr.linEntryValid = spill.linearLeftValid
		ctr.linEntryPart = entryPart
		spill.linearLeft = nil
		spill.linearLeftValid = nil
	}
	ctr.cleanupSpill(proc)
	return nil
}

func (s *fillSpill) finishLinearBatch(ctr *container, ap *Fill, proc *process.Process, bat *batch.Batch) error {
	for row := 0; row < bat.RowCount(); row++ {
		if row&255 == 0 {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return err
			}
		}
		if len(ap.PartitionColIdx) > 0 {
			wasSet := s.forwardPart.set
			same, err := s.forwardPart.sameAndSet(ap.PartitionColIdx, bat, row)
			if err != nil {
				return err
			}
			if !same && wasSet {
				clearEndpoints(s.linearLeftValid)
				clear(s.linearLeftSteps)
			}
		}
		for col := 0; col < ap.ColLen; col++ {
			if !originalNullAt(bat, ap.ColLen, col, row) {
				if err := setEndpoint(
					&s.linearLeft[col], bat.Vecs[col], row, proc, ctr.retainedAllocation,
				); err != nil {
					return err
				}
				s.linearLeftValid[col] = true
				s.linearLeftSteps[col] = 0
				continue
			}
			rightSteps := linearRightDistanceAt(bat, ap.ColLen, col, row)
			if bat.Vecs[col].IsNull(uint64(row)) || !s.linearLeftValid[col] || rightSteps == 0 {
				bat.Vecs[col].GetNulls().Add(uint64(row))
				continue
			}
			s.linearLeftSteps[col]++
			total := s.linearLeftSteps[col] + rightSteps
			if total == 2 {
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
				continue
			}
			if err := setLinearInterpolatedValue(
				bat.Vecs[col], row,
				s.linearLeft[col], 0, bat.Vecs[col], row,
				s.linearLeftSteps[col], total,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
