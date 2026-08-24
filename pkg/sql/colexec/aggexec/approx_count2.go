// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"slices"

	metro "github.com/dgryski/go-metro"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	hllPrecision   = uint8(14)
	hllRegisterCnt = 1 << hllPrecision
	hllHeaderSize  = 8
	hllEncodedSize = hllHeaderSize + hllRegisterCnt
	hllVersion     = byte(2)
)

var canonicalEmptyHLL = func() [hllEncodedSize]byte {
	var encoded [hllEncodedSize]byte
	encoded[0] = hllVersion
	encoded[1] = hllPrecision
	binary.BigEndian.PutUint32(encoded[4:hllHeaderSize], hllRegisterCnt)
	return encoded
}()

// hllSketch is the dense p=14 representation historically produced by
// hyperloglog.NewNoSparse. Keeping the register array in MPool makes the
// fixed 16 KiB per-group allocation physically accountable; the encoding and
// estimator remain wire- and result-compatible with that implementation.
type hllSketch struct {
	mp   *mpool.MPool
	regs []byte
}

func makeHllSketch(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (MarshalerUnmarshaler, error) {
	if mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	regs, err := allocation.allocArgumentArena(mp, hllRegisterCnt)
	if err != nil {
		return nil, err
	}
	return &hllSketch{mp: mp, regs: regs}, nil
}

func (s *hllSketch) ensureRegisters() error {
	if len(s.regs) == hllRegisterCnt {
		return nil
	}
	return moerr.NewInvalidInputNoCtx("invalid HLL register count")
}

func (s *hllSketch) Insert(value []byte) {
	if s == nil || len(s.regs) != hllRegisterCnt {
		panic(mpool.ErrAllocationAccountInvariant)
	}
	hash := metro.Hash64(value, 1337)
	index := hash >> (64 - hllPrecision)
	word := hash<<hllPrecision | 1<<(hllPrecision-1)
	rank := byte(bits.LeadingZeros64(word) + 1)
	if rank > s.regs[index] {
		s.regs[index] = rank
	}
}

func (s *hllSketch) Merge(other *hllSketch) error {
	if s == nil || other == nil || len(s.regs) != hllRegisterCnt ||
		len(other.regs) != hllRegisterCnt {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch")
	}
	for i, value := range other.regs {
		if value > s.regs[i] {
			s.regs[i] = value
		}
	}
	return nil
}

func (s *hllSketch) Estimate() uint64 {
	if s == nil || len(s.regs) != hllRegisterCnt {
		return 0
	}
	var sum, zeros float64
	for _, value := range s.regs {
		if value == 0 {
			zeros++
		}
		sum += 1.0 / math.Pow(2.0, float64(value))
	}
	m := float64(hllRegisterCnt)
	alpha := 0.7213 / (1 + 1.079/m)
	estimate := alpha * m * (m - zeros) / (sum + hllBeta14(zeros))
	return uint64(estimate + 0.5)
}

func hllBeta14(zeros float64) float64 {
	logZeros := math.Log(zeros + 1)
	return -0.371009760230692*zeros +
		0.00978811941207509*logZeros +
		0.185796293324165*math.Pow(logZeros, 2) +
		0.203015527328432*math.Pow(logZeros, 3) -
		0.116710521803686*math.Pow(logZeros, 4) +
		0.0431106699492820*math.Pow(logZeros, 5) -
		0.00599583540511831*math.Pow(logZeros, 6) +
		0.000449704299509437*math.Pow(logZeros, 7)
}

func (s *hllSketch) MarshaledSize() int {
	return hllEncodedSize
}

func (s *hllSketch) MarshalTo(writer io.Writer) error {
	if s == nil || len(s.regs) != hllRegisterCnt || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch")
	}
	var header [hllHeaderSize]byte
	header[0] = hllVersion
	header[1] = hllPrecision
	binary.BigEndian.PutUint32(header[4:], hllRegisterCnt)
	written, err := writer.Write(header[:])
	if err != nil {
		return err
	}
	if written != len(header) {
		return io.ErrShortWrite
	}
	written, err = writer.Write(s.regs)
	if err == nil && written != len(s.regs) {
		return io.ErrShortWrite
	}
	return err
}

func (s *hllSketch) MarshalBinary() ([]byte, error) {
	if s == nil || len(s.regs) != hllRegisterCnt {
		return nil, moerr.NewInvalidInputNoCtx("invalid HLL sketch")
	}
	encoded := make([]byte, hllEncodedSize)
	encoded[0] = hllVersion
	encoded[1] = hllPrecision
	binary.BigEndian.PutUint32(encoded[4:8], hllRegisterCnt)
	copy(encoded[hllHeaderSize:], s.regs)
	return encoded, nil
}

func (s *hllSketch) UnmarshalBinary(data []byte) error {
	return s.unmarshalDense(data)
}

func (s *hllSketch) UnmarshalFromReader(reader io.Reader) error {
	if s == nil || reader == nil {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch")
	}
	var header [hllHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if err := validateDenseHLLHeader(header[:]); err != nil {
		return err
	}
	if err := s.ensureRegisters(); err != nil {
		return err
	}
	_, err := io.ReadFull(reader, s.regs)
	return err
}

func validateDenseHLLHeader(header []byte) error {
	if len(header) != hllHeaderSize || header[0] != hllVersion ||
		header[1] != hllPrecision || header[2] != 0 || header[3] != 0 ||
		binary.BigEndian.Uint32(header[4:]) != hllRegisterCnt {
		return moerr.NewInvalidInputNoCtx("invalid dense HLL sketch")
	}
	return nil
}

func (s *hllSketch) unmarshalDense(data []byte) error {
	if len(data) != hllEncodedSize {
		return moerr.NewInvalidInputNoCtx("invalid dense HLL sketch size")
	}
	if err := validateDenseHLLHeader(data[:hllHeaderSize]); err != nil {
		return err
	}
	if err := s.ensureRegisters(); err != nil {
		return err
	}
	copy(s.regs, data[hllHeaderSize:])
	return nil
}

func (s *hllSketch) mergeBytes(data []byte) error {
	if len(data) < hllHeaderSize {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch size")
	}
	if data[0] != hllVersion || data[1] != hllPrecision || data[2] != 0 {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch header")
	}
	if data[3] == 1 {
		return s.mergeSparseBytes(data)
	}
	if len(data) != hllEncodedSize {
		return moerr.NewInvalidInputNoCtx("invalid dense HLL sketch size")
	}
	if err := validateDenseHLLHeader(data[:hllHeaderSize]); err != nil {
		return moerr.NewInvalidInputNoCtxf("invalid HLL sketch: %v", err)
	}
	for i, value := range data[hllHeaderSize:] {
		if value > s.regs[i] {
			s.regs[i] = value
		}
	}
	return nil
}

func (s *hllSketch) mergeSparseBytes(data []byte) error {
	if len(data) < 8 {
		return moerr.NewInvalidInputNoCtx("invalid sparse HLL sketch")
	}
	temporaryCount := binary.BigEndian.Uint32(data[4:8])
	if uint64(temporaryCount) > uint64(len(data)-8)/4 {
		return moerr.NewInvalidInputNoCtx("invalid sparse HLL temporary set")
	}
	offset := 8
	temporaryOffset := offset
	offset += int(temporaryCount) * 4
	if len(data)-offset < 12 {
		return moerr.NewInvalidInputNoCtx("invalid sparse HLL list")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	last := binary.BigEndian.Uint32(data[offset+4 : offset+8])
	listSize := binary.BigEndian.Uint32(data[offset+8 : offset+12])
	offset += 12
	if uint64(listSize) != uint64(len(data)-offset) {
		return moerr.NewInvalidInputNoCtx("invalid sparse HLL list size")
	}
	position := 0
	value := uint32(0)
	decoded := uint32(0)
	for position < int(listSize) {
		delta, next, err := decodeHLLVarUint(data[offset:], position)
		if err != nil || value > math.MaxUint32-delta {
			return moerr.NewInvalidInputNoCtx("invalid sparse HLL list value")
		}
		value += delta
		position = next
		decoded++
	}
	if decoded != count || (decoded != 0 && value != last) {
		return moerr.NewInvalidInputNoCtx("invalid sparse HLL list metadata")
	}
	// Parsing and metadata validation are deliberately complete before the
	// destination is changed. HLL_MERGE input is user data; a malformed suffix
	// must not publish a valid prefix into aggregate state.
	for pos := temporaryOffset; pos < temporaryOffset+int(temporaryCount)*4; pos += 4 {
		s.mergeSparseHash(binary.BigEndian.Uint32(data[pos : pos+4]))
	}
	position = 0
	value = 0
	for position < int(listSize) {
		delta, next, _ := decodeHLLVarUint(data[offset:], position)
		value += delta
		position = next
		s.mergeSparseHash(value)
	}
	return nil
}

func decodeHLLVarUint(data []byte, position int) (uint32, int, error) {
	var value uint32
	for shift := uint(0); ; shift += 7 {
		if position >= len(data) || shift >= 35 {
			return 0, position, io.ErrUnexpectedEOF
		}
		part := data[position]
		position++
		if shift == 28 && part > 0x0f {
			return 0, position, moerr.NewInvalidInputNoCtx("sparse HLL varint overflow")
		}
		value |= uint32(part&0x7f) << shift
		if part&0x80 == 0 {
			return value, position, nil
		}
	}
}

func (s *hllSketch) mergeSparseHash(encoded uint32) {
	const sparsePrecision = uint8(25)
	var index uint32
	var rank uint8
	if encoded&1 == 1 {
		index = encoded >> (32 - hllPrecision)
		rank = uint8((encoded>>1)&0x3f) + sparsePrecision - hllPrecision
	} else {
		index = (encoded >> (sparsePrecision - hllPrecision + 1)) &
			(hllRegisterCnt - 1)
		rank = uint8(bits.LeadingZeros64(
			uint64(encoded<<(32-sparsePrecision+hllPrecision-1))) - 31)
	}
	if rank > s.regs[index] {
		s.regs[index] = rank
	}
}

func (s *hllSketch) Free() {
	if s == nil {
		return
	}
	if len(s.regs) != 0 && s.mp != nil {
		s.mp.Free(s.regs)
	}
	s.regs = nil
	s.mp = nil
}

type hllStateExec struct {
	aggExec
	family hllStateFamily
}

type hllStateFamily uint8

const (
	hllStateFamilyApproxCount hllStateFamily = iota + 1
	hllStateFamilyAdd
	hllStateFamilyMerge
)

func (exec *hllStateExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 1 || vectors[0] == nil ||
		len(groups) > hashmap.UnitLimit || offset < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if !vectors[0].CoversLogicalRows(offset, len(groups)) {
		return mpool.ErrAllocationAccountInvalid
	}
	var active [hashmap.UnitLimit]bool
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		active[i] = !vectors[0].IsNull(uint64(row))
	}
	return exec.preallocateMappedGroups(groups, &active)
}

func (exec *hllStateExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, family, ok := hllAggregateBase(next)
	if !ok || other == nil || family != exec.family ||
		len(groups) > hashmap.UnitLimit || offset < 0 ||
		offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	var active [hashmap.UnitLimit]bool
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		_, _, destination, err := exec.validatePreflightTarget(group)
		if err != nil || destination == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		sx, sy := other.getXY(uint64(offset + i))
		if sx >= len(other.state) || int(sy) >= len(other.state[sx].mobs) {
			return mpool.ErrAllocationAccountInvariant
		}
		active[i] = other.state[sx].mobs[sy] != nil
	}
	return exec.preallocateMappedGroups(groups, &active)
}

func (exec *hllStateExec) preallocateMappedGroups(
	groups []uint64,
	active *[hashmap.UnitLimit]bool,
) error {
	type allocatedSketch struct {
		state *aggState
		row   uint16
	}
	var allocated [hashmap.UnitLimit]allocatedSketch
	allocatedCount := 0
	committed := false
	defer func() {
		if committed {
			return
		}
		for i := allocatedCount - 1; i >= 0; i-- {
			entry := allocated[i]
			if entry.state != nil && entry.state.mobs[entry.row] != nil {
				entry.state.mobs[entry.row].(*hllSketch).Free()
				entry.state.mobs[entry.row] = nil
			}
		}
	}()
	for index, group := range groups {
		if group == GroupNotMatched || active == nil || !active[index] {
			continue
		}
		_, y, state, err := exec.validatePreflightTarget(group)
		if err != nil || state == nil || int(y) >= len(state.mobs) {
			return mpool.ErrAllocationAccountInvariant
		}
		if state.mobs[y] != nil {
			continue
		}
		mob, err := makeHllSketch(exec.mp, exec.allocation)
		if err != nil {
			return err
		}
		state.mobs[y] = mob
		allocated[allocatedCount] = allocatedSketch{state: state, row: y}
		allocatedCount++
	}
	committed = true
	return nil
}

func (exec *hllStateExec) sketchForPublication(
	x int, y uint16,
) (*hllSketch, error) {
	if x < 0 || x >= len(exec.state) || int(y) >= len(exec.state[x].mobs) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	if sketch, ok := exec.state[x].mobs[y].(*hllSketch); ok && sketch != nil {
		return sketch, nil
	}
	// Accounted Group must have created this state in preflight. Direct legacy
	// callers do not run that protocol, so retain lazy allocation for them.
	if exec.allocation != nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	mob, err := makeHllSketch(exec.mp, nil)
	if err != nil {
		return nil, err
	}
	sketch := mob.(*hllSketch)
	exec.state[x].mobs[y] = sketch
	return sketch, nil
}

func hllAggregateBase(exec AggFuncExec) (*aggExec, hllStateFamily, bool) {
	switch value := exec.(type) {
	case *approxCountExec:
		return &value.aggExec, hllStateFamilyApproxCount, true
	case *hllAddExec:
		return &value.aggExec, hllStateFamilyAdd, true
	case *hllMergeExec:
		return &value.aggExec, hllStateFamilyMerge, true
	default:
		return nil, 0, false
	}
}

func makeHLLStateInfo(id int64, arg, ret types.Type) aggInfo {
	return aggInfo{
		aggId:                    id,
		argTypes:                 []types.Type{arg},
		retType:                  ret,
		makeMarshalerUnmarshaler: makeHllSketch,
		boundedOpaqueState:       true,
		stableEmptyOpaqueState: func(writer io.Writer) error {
			if err := types.WriteInt32(writer, hllEncodedSize); err != nil {
				return err
			}
			written, err := writer.Write(canonicalEmptyHLL[:])
			if err == nil && written != len(canonicalEmptyHLL) {
				return io.ErrShortWrite
			}
			return err
		},
	}
}

type approxCountExec struct {
	hllStateExec
}

func makeApproxCount(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	return &approxCountExec{hllStateExec: hllStateExec{family: hllStateFamilyApproxCount, aggExec: aggExec{
		mp:      mp,
		aggInfo: makeHLLStateInfo(id, arg, types.T_uint64.ToType()),
	}}}
}

func (exec *approxCountExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *approxCountExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *approxCountExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y := exec.getXY(group - 1)
		sketch, err := exec.sketchForPublication(x, y)
		if err != nil {
			return err
		}
		sketch.Insert(vectors[0].GetRawBytesAt(row))
	}
	return nil
}

func (exec *approxCountExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *approxCountExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxCountExec)
	return mergeHLLStates(&exec.aggExec, &other.aggExec, offset, groups)
}

func (exec *approxCountExec) SetExtraInformation(any, int) error { return nil }

func (exec *approxCountExec) Flush() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer freeAggregateResultsOnError(exec.mp, vecs, &retErr)
	for chunk, state := range exec.state {
		vecs[chunk], retErr = exec.allocation.newVector(types.T_uint64.ToType())
		if retErr != nil {
			return nil, retErr
		}
		if retErr = vecs[chunk].PreExtend(int(state.length), exec.mp); retErr != nil {
			return nil, retErr
		}
		vecs[chunk].SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[uint64](vecs[chunk])
		for row := range int(state.length) {
			if state.mobs[row] != nil {
				values[row] = state.mobs[row].(*hllSketch).Estimate()
			}
		}
	}
	return vecs, nil
}

func (exec *approxCountExec) Size() int64 { return hllStateSize(exec.state) }

type hllAddExec struct {
	hllStateExec
}

func makeHllAdd(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	return &hllAddExec{hllStateExec: hllStateExec{family: hllStateFamilyAdd, aggExec: aggExec{
		mp:      mp,
		aggInfo: makeHLLStateInfo(id, arg, types.T_varbinary.ToType()),
	}}}
}

func (exec *hllAddExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *hllAddExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *hllAddExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y := exec.getXY(group - 1)
		sketch, err := exec.sketchForPublication(x, y)
		if err != nil {
			return err
		}
		sketch.Insert(vectors[0].GetRawBytesAt(row))
	}
	return nil
}

func (exec *hllAddExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *hllAddExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*hllAddExec)
	return mergeHLLStates(&exec.aggExec, &other.aggExec, offset, groups)
}

func (exec *hllAddExec) SetExtraInformation(any, int) error { return nil }

func (exec *hllAddExec) Flush() ([]*vector.Vector, error) {
	return flushHLLSketches(&exec.aggExec)
}

func (exec *hllAddExec) Size() int64 { return hllStateSize(exec.state) }

type hllMergeExec struct {
	hllStateExec
}

func makeHllMerge(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	return &hllMergeExec{hllStateExec: hllStateExec{family: hllStateFamilyMerge, aggExec: aggExec{
		mp:      mp,
		aggInfo: makeHLLStateInfo(id, arg, types.T_varbinary.ToType()),
	}}}
}

func (exec *hllMergeExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *hllMergeExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *hllMergeExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y := exec.getXY(group - 1)
		sketch, err := exec.sketchForPublication(x, y)
		if err != nil {
			return err
		}
		if err := sketch.mergeBytes(vectors[0].GetBytesAt(row)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *hllMergeExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *hllMergeExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*hllMergeExec)
	return mergeHLLStates(&exec.aggExec, &other.aggExec, offset, groups)
}

func (exec *hllMergeExec) SetExtraInformation(any, int) error { return nil }

func (exec *hllMergeExec) Flush() ([]*vector.Vector, error) {
	return flushHLLSketches(&exec.aggExec)
}

func (exec *hllMergeExec) Size() int64 { return hllStateSize(exec.state) }

func mergeHLLStates(destination, source *aggExec, offset int, groups []uint64) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x1, y1 := destination.getXY(group - 1)
		x2, y2 := source.getXY(uint64(offset + i))
		if source.state[x2].mobs[y2] == nil {
			continue
		}
		destinationSketch, ok := destination.state[x1].mobs[y1].(*hllSketch)
		if !ok || destinationSketch == nil {
			if destination.allocation != nil {
				return mpool.ErrAllocationAccountInvariant
			}
			mob, err := makeHllSketch(destination.mp, nil)
			if err != nil {
				return err
			}
			destinationSketch = mob.(*hllSketch)
			destination.state[x1].mobs[y1] = destinationSketch
		}
		if err := destinationSketch.Merge(
			source.state[x2].mobs[y2].(*hllSketch)); err != nil {
			return err
		}
	}
	return nil
}

func flushHLLSketches(exec *aggExec) (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer freeAggregateResultsOnError(exec.mp, vecs, &retErr)
	for chunk, state := range exec.state {
		vecs[chunk], retErr = exec.allocation.newVector(types.T_varbinary.ToType())
		if retErr != nil {
			return nil, retErr
		}
		areaBytes := int(state.length) * hllEncodedSize
		if retErr = vecs[chunk].PreExtendWithArea(int(state.length), areaBytes, exec.mp); retErr != nil {
			return nil, retErr
		}
		for row := range int(state.length) {
			if retErr = vector.AppendBytes(vecs[chunk], canonicalEmptyHLL[:], false, exec.mp); retErr != nil {
				return nil, retErr
			}
			if state.mobs[row] != nil {
				sketch := state.mobs[row].(*hllSketch)
				stored := vecs[chunk].GetBytesAt(row)
				copy(stored[hllHeaderSize:], sketch.regs)
			}
		}
	}
	return vecs, nil
}

func hllStateSize(states []aggState) int64 {
	var size int64
	for _, state := range states {
		size += int64(cap(state.mobs)) * 8
		for _, mob := range state.mobs {
			if mob != nil {
				size += hllRegisterCnt
			}
		}
	}
	return size
}
