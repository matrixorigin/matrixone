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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	hllPrecision        = uint8(14)
	hllRegisterCnt      = 1 << hllPrecision
	hllHeaderSize       = 8
	hllEncodedSize      = hllHeaderSize + hllRegisterCnt
	hllLegacyVersion    = byte(2)
	hllFloatZeroVersion = byte(3)
	hllVersion          = byte(4)
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
// fixed 16 KiB per-group allocation physically accountable. Version 2 keeps
// the legacy raw-value hash semantics, version 3 canonicalizes only scalar
// floating-point signed zero, and version 4 uses the complete typed SQL
// equivalence key. Sketches with different hash versions cannot be merged
// losslessly.
type hllSketch struct {
	mp          *mpool.MPool
	regs        []byte
	wireVersion byte
	hasValue    bool
}

func makeHllSketch(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (MarshalerUnmarshaler, error) {
	return makeHllSketchWithVersion(mp, allocation, hllVersion)
}

func makeHllSketchWithVersion(
	mp *mpool.MPool,
	allocation *AllocationAccount,
	version byte,
) (MarshalerUnmarshaler, error) {
	if mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	regs, err := allocation.allocArgumentArena(mp, hllRegisterCnt)
	if err != nil {
		return nil, err
	}
	return &hllSketch{mp: mp, regs: regs, wireVersion: version}, nil
}

func makeLegacyHllSketch(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (MarshalerUnmarshaler, error) {
	return makeHllSketchWithVersion(mp, allocation, hllLegacyVersion)
}

func makeFloatZeroHllSketch(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (MarshalerUnmarshaler, error) {
	return makeHllSketchWithVersion(mp, allocation, hllFloatZeroVersion)
}

func (s *hllSketch) effectiveWireVersion() byte {
	if s.wireVersion == 0 {
		return hllVersion
	}
	return s.wireVersion
}

func (s *hllSketch) hasRegisters() bool {
	if s.hasValue {
		return true
	}
	for _, value := range s.regs {
		if value != 0 {
			s.hasValue = true
			return true
		}
	}
	return false
}

func hasHLLRegisters(regs []byte) bool {
	for _, value := range regs {
		if value != 0 {
			return true
		}
	}
	return false
}

func (s *hllSketch) useWireVersion(version byte) error {
	if version != hllLegacyVersion && version != hllFloatZeroVersion &&
		version != hllVersion {
		return moerr.NewInvalidInputNoCtx("invalid HLL hash version")
	}
	current := s.effectiveWireVersion()
	if current == version || !s.hasRegisters() {
		s.wireVersion = version
		return nil
	}
	return moerr.NewInvalidInputNoCtxf(
		"incompatible HLL hash versions: destination %d, input %d",
		current, version)
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
	s.hasValue = true
}

func insertHLLValue(sketch *hllSketch, typ types.Type, value []byte) {
	insertHLLValueWithScratch(sketch, typ, value, nil)
}

func insertHLLValueWithScratch(
	sketch *hllSketch,
	typ types.Type,
	value []byte,
	scratch []byte,
) []byte {
	switch sketch.effectiveWireVersion() {
	case hllLegacyVersion:
		sketch.Insert(value)
		return scratch
	case hllFloatZeroVersion:
		switch typ.Oid {
		case types.T_float32:
			if len(value) == types.T_float32.TypeLen() &&
				types.DecodeFixed[float32](value) == 0 {
				var zero float32
				sketch.Insert(types.EncodeFixed(zero))
				return scratch
			}
		case types.T_float64:
			if len(value) == types.T_float64.TypeLen() &&
				types.DecodeFixed[float64](value) == 0 {
				var zero float64
				sketch.Insert(types.EncodeFixed(zero))
				return scratch
			}
		}
		sketch.Insert(value)
		return scratch
	case hllVersion:
		switch typ.Oid {
		case types.T_char:
			sketch.Insert(keycodec.CanonicalCharValue(value))
			return scratch
		case types.T_float32:
			if len(value) == types.T_float32.TypeLen() {
				encoded := keycodec.NewFloat32Codec(typ.Scale).CanonicalBytes(
					types.DecodeFixed[float32](value))
				sketch.Insert(encoded[:])
				return scratch
			}
		case types.T_float64:
			if len(value) == types.T_float64.TypeLen() {
				encoded := keycodec.CanonicalFloat64Bytes(
					types.DecodeFixed[float64](value))
				sketch.Insert(encoded[:])
				return scratch
			}
		}
		// These types already have the exact bytes used by the v4 equality
		// domain. Passing nil scratch to AppendCanonicalValue would allocate a
		// fresh copy for every row outside the aggregate allocation account.
		if !canonicalValueNeedsScratch(typ) {
			sketch.Insert(value)
			return scratch
		}
		scratch = keycodec.AppendCanonicalValue(scratch[:0], typ, value)
		sketch.Insert(scratch)
		return scratch
	default:
		// The sketch constructors and wire validators reject unknown versions;
		// retain a defensive raw fallback for an in-memory malformed object.
		sketch.Insert(value)
		return scratch
	}
}

func (s *hllSketch) Merge(other *hllSketch) error {
	if s == nil || other == nil || len(s.regs) != hllRegisterCnt ||
		len(other.regs) != hllRegisterCnt {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch")
	}
	if other.hasRegisters() {
		if err := s.useWireVersion(other.effectiveWireVersion()); err != nil {
			return err
		}
	}
	for i, value := range other.regs {
		if value > s.regs[i] {
			s.regs[i] = value
		}
	}
	if other.hasValue {
		s.hasValue = true
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
	header[0] = s.effectiveWireVersion()
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
	encoded[0] = s.effectiveWireVersion()
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
	if _, err := io.ReadFull(reader, s.regs); err != nil {
		return err
	}
	s.wireVersion = header[0]
	s.hasValue = false
	s.hasValue = s.hasRegisters()
	return nil
}

func validateDenseHLLHeader(header []byte) error {
	if len(header) != hllHeaderSize ||
		(header[0] != hllLegacyVersion && header[0] != hllFloatZeroVersion &&
			header[0] != hllVersion) ||
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
	s.wireVersion = data[0]
	s.hasValue = false
	s.hasValue = s.hasRegisters()
	return nil
}

func (s *hllSketch) mergeBytes(data []byte) error {
	if len(data) < hllHeaderSize {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch size")
	}
	if (data[0] != hllLegacyVersion && data[0] != hllFloatZeroVersion &&
		data[0] != hllVersion) ||
		data[1] != hllPrecision || data[2] != 0 {
		return moerr.NewInvalidInputNoCtx("invalid HLL sketch header")
	}
	if data[3] == 1 {
		view, err := parseSparseHLL(data)
		if err != nil {
			return err
		}
		if view.empty() {
			return nil
		}
		if data[0] != hllLegacyVersion {
			return moerr.NewInvalidInputNoCtx("invalid HLL sparse hash version")
		}
		if err := s.useWireVersion(data[0]); err != nil {
			return err
		}
		s.mergeSparseView(data, view)
		s.hasValue = s.hasRegisters()
		return nil
	}
	if len(data) != hllEncodedSize {
		return moerr.NewInvalidInputNoCtx("invalid dense HLL sketch size")
	}
	if err := validateDenseHLLHeader(data[:hllHeaderSize]); err != nil {
		return moerr.NewInvalidInputNoCtxf("invalid HLL sketch: %v", err)
	}
	if !hasHLLRegisters(data[hllHeaderSize:]) {
		return nil
	}
	if err := s.useWireVersion(data[0]); err != nil {
		return err
	}
	for i, value := range data[hllHeaderSize:] {
		if value > s.regs[i] {
			s.regs[i] = value
		}
	}
	s.hasValue = s.hasRegisters()
	return nil
}

func (s *hllSketch) mergeSparseBytes(data []byte) error {
	view, err := parseSparseHLL(data)
	if err != nil {
		return err
	}
	s.mergeSparseView(data, view)
	return nil
}

type sparseHLLView struct {
	temporaryOffset int
	temporaryCount  int
	listOffset      int
	listSize        int
}

func (view sparseHLLView) empty() bool {
	return view.temporaryCount == 0 && view.listSize == 0
}

func parseSparseHLL(data []byte) (sparseHLLView, error) {
	var view sparseHLLView
	if len(data) < 8 {
		return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL sketch")
	}
	temporaryCount := binary.BigEndian.Uint32(data[4:8])
	if uint64(temporaryCount) > uint64(len(data)-8)/4 {
		return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL temporary set")
	}
	offset := 8
	temporaryOffset := offset
	offset += int(temporaryCount) * 4
	if len(data)-offset < 12 {
		return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL list")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	last := binary.BigEndian.Uint32(data[offset+4 : offset+8])
	listSize := binary.BigEndian.Uint32(data[offset+8 : offset+12])
	offset += 12
	if uint64(listSize) != uint64(len(data)-offset) {
		return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL list size")
	}
	position := 0
	value := uint32(0)
	decoded := uint32(0)
	for position < int(listSize) {
		delta, next, err := decodeHLLVarUint(data[offset:], position)
		if err != nil || value > math.MaxUint32-delta {
			return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL list value")
		}
		value += delta
		position = next
		decoded++
	}
	if decoded != count || (decoded != 0 && value != last) {
		return view, moerr.NewInvalidInputNoCtx("invalid sparse HLL list metadata")
	}
	return sparseHLLView{
		temporaryOffset: temporaryOffset,
		temporaryCount:  int(temporaryCount),
		listOffset:      offset,
		listSize:        int(listSize),
	}, nil
}

func (s *hllSketch) mergeSparseView(data []byte, view sparseHLLView) {
	// Parsing and metadata validation are deliberately complete before the
	// destination is changed. HLL_MERGE input is user data; a malformed suffix
	// must not publish a valid prefix into aggregate state.
	for pos := view.temporaryOffset; pos < view.temporaryOffset+view.temporaryCount*4; pos += 4 {
		s.mergeSparseHash(binary.BigEndian.Uint32(data[pos : pos+4]))
	}
	position := 0
	value := uint32(0)
	for position < view.listSize {
		delta, next, _ := decodeHLLVarUint(data[view.listOffset:], position)
		value += delta
		position = next
		s.mergeSparseHash(value)
	}
	if !view.empty() {
		s.hasValue = true
	}
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
	s.hasValue = false
}

type hllStateExec struct {
	aggExec
	family             hllStateFamily
	legacyWireState    bool
	floatZeroWireState bool
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
	var scratchSizes [hashmap.UnitLimit]int
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		active[i] = !vectors[0].IsNull(uint64(row))
		if active[i] {
			scratchSizes[i] = exec.canonicalScratchSize(
				vectors[0], row)
		}
	}
	return exec.preallocateMappedGroups(groups, &active, &scratchSizes)
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
	return exec.preallocateMappedGroups(groups, &active, nil)
}

func (exec *hllStateExec) preallocateMappedGroups(
	groups []uint64,
	active *[hashmap.UnitLimit]bool,
	scratchSizes *[hashmap.UnitLimit]int,
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
		if scratchSizes != nil && scratchSizes[index] > 0 {
			if _, err := state.resizeArgScratch(
				exec.mp, scratchSizes[index]); err != nil {
				return err
			}
		}
		if state.mobs[y] != nil {
			continue
		}
		mob, err := exec.makeHLLSketch(exec.mp, exec.allocation)
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
	mob, err := exec.makeHLLSketch(exec.mp, nil)
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
	return makeHLLStateInfoWithVersion(id, arg, ret, hllVersion)
}

func makeLegacyHLLStateInfo(id int64, arg, ret types.Type) aggInfo {
	return makeHLLStateInfoWithVersion(id, arg, ret, hllLegacyVersion)
}

func makeHLLStateInfoWithVersion(id int64, arg, ret types.Type, version byte) aggInfo {
	makeSketch := makeHllSketch
	if version == hllLegacyVersion {
		makeSketch = makeLegacyHllSketch
	}
	return aggInfo{
		aggId:                    id,
		argTypes:                 []types.Type{arg},
		retType:                  ret,
		makeMarshalerUnmarshaler: makeSketch,
		boundedOpaqueState:       true,
		stableEmptyOpaqueState:   stableEmptyHLLState(version),
	}
}

func stableEmptyHLLState(version byte) func(io.Writer) error {
	return func(writer io.Writer) error {
		empty := canonicalEmptyHLL
		empty[0] = version
		if err := types.WriteInt32(writer, hllEncodedSize); err != nil {
			return err
		}
		written, err := writer.Write(empty[:])
		if err == nil && written != len(canonicalEmptyHLL) {
			return io.ErrShortWrite
		}
		return err
	}
}

// ConfigureHLLLegacyState makes a newly constructed remote executor emit the
// version-2 hash semantics understood by pre-v73 peers. It is applied before
// GroupGrow so lazy and preflight allocations use the same version.
func ConfigureHLLLegacyState(aggregate AggFuncExec) {
	if configurable, ok := aggregate.(interface{ setLegacyHLLState() }); ok {
		configurable.setLegacyHLLState()
	}
}

// ConfigureHLLFloatZeroState makes a newly constructed remote executor emit
// the version-3 hash semantics used by protocol v73: only scalar floating
// point signed zero is canonicalized. Protocol v74 introduces the complete
// typed SQL-equivalence key and must not be sent this compatibility state.
func ConfigureHLLFloatZeroState(aggregate AggFuncExec) {
	if configurable, ok := aggregate.(interface{ setFloatZeroHLLState() }); ok {
		configurable.setFloatZeroHLLState()
	}
}

type approxCountExec struct {
	hllStateExec
}

func (exec *hllStateExec) setLegacyHLLState() {
	exec.legacyWireState = true
	exec.floatZeroWireState = false
	exec.aggInfo.makeMarshalerUnmarshaler = makeLegacyHllSketch
	exec.aggInfo.stableEmptyOpaqueState = stableEmptyHLLState(hllLegacyVersion)
}

func (exec *hllStateExec) setFloatZeroHLLState() {
	exec.legacyWireState = true
	exec.floatZeroWireState = true
	exec.aggInfo.makeMarshalerUnmarshaler = makeFloatZeroHllSketch
	exec.aggInfo.stableEmptyOpaqueState = stableEmptyHLLState(hllFloatZeroVersion)
}

func (exec *hllStateExec) makeHLLSketch(
	mp *mpool.MPool,
	allocation *AllocationAccount,
) (MarshalerUnmarshaler, error) {
	if exec.legacyWireState {
		if exec.floatZeroWireState {
			return makeFloatZeroHllSketch(mp, allocation)
		}
		return makeLegacyHllSketch(mp, allocation)
	}
	return makeHllSketch(mp, allocation)
}

func canonicalValueNeedsScratch(typ types.Type) bool {
	switch typ.Oid {
	case types.T_json, types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16:
		return true
	default:
		return false
	}
}

func (exec *hllStateExec) canonicalScratchSize(
	vec *vector.Vector,
	row int,
) int {
	if exec == nil || exec.legacyWireState || len(exec.argTypes) == 0 ||
		vec == nil || !canonicalValueNeedsScratch(exec.argTypes[0]) {
		return 0
	}
	return keycodec.CanonicalValueSize(exec.argTypes[0], vec.GetRawBytesAt(row))
}

func (exec *hllStateExec) prepareCanonicalScratch(
	x int,
	value []byte,
) ([]byte, error) {
	if exec == nil || exec.legacyWireState || len(exec.argTypes) == 0 ||
		!canonicalValueNeedsScratch(exec.argTypes[0]) {
		return nil, nil
	}
	if x < 0 || x >= len(exec.state) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	size := keycodec.CanonicalValueSize(exec.argTypes[0], value)
	return exec.state[x].resizeArgScratch(exec.mp, size)
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
		scratch, err := exec.prepareCanonicalScratch(
			x, vectors[0].GetRawBytesAt(row))
		if err != nil {
			return err
		}
		insertHLLValueWithScratch(
			sketch, exec.argTypes[0], vectors[0].GetRawBytesAt(row), scratch)
	}
	return nil
}

func (exec *approxCountExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *approxCountExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxCountExec)
	return mergeHLLStates(&exec.hllStateExec, &other.hllStateExec, offset, groups)
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
	// HLL_ADD_AGG output is persisted and consumed by HLL_MERGE_AGG. Keep its
	// legacy wire/hash semantics so an upgrade can append to existing states.
	return &hllAddExec{hllStateExec: hllStateExec{family: hllStateFamilyAdd, legacyWireState: true, aggExec: aggExec{
		mp:      mp,
		aggInfo: makeLegacyHLLStateInfo(id, arg, types.T_varbinary.ToType()),
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
		scratch, err := exec.prepareCanonicalScratch(
			x, vectors[0].GetRawBytesAt(row))
		if err != nil {
			return err
		}
		insertHLLValueWithScratch(
			sketch, exec.argTypes[0], vectors[0].GetRawBytesAt(row), scratch)
	}
	return nil
}

func (exec *hllAddExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *hllAddExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*hllAddExec)
	return mergeHLLStates(&exec.hllStateExec, &other.hllStateExec, offset, groups)
}

func (exec *hllAddExec) SetExtraInformation(any, int) error { return nil }

func (exec *hllAddExec) Flush() ([]*vector.Vector, error) {
	return flushHLLSketches(&exec.hllStateExec)
}

func (exec *hllAddExec) Size() int64 { return hllStateSize(exec.state) }

type hllMergeExec struct {
	hllStateExec
}

func makeHllMerge(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	// HLL_MERGE_AGG must accept and emit the same v2 state as HLL_ADD_AGG.
	return &hllMergeExec{hllStateExec: hllStateExec{family: hllStateFamilyMerge, legacyWireState: true, aggExec: aggExec{
		mp:      mp,
		aggInfo: makeLegacyHLLStateInfo(id, arg, types.T_varbinary.ToType()),
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
	return mergeHLLStates(&exec.hllStateExec, &other.hllStateExec, offset, groups)
}

func (exec *hllMergeExec) SetExtraInformation(any, int) error { return nil }

func (exec *hllMergeExec) Flush() ([]*vector.Vector, error) {
	return flushHLLSketches(&exec.hllStateExec)
}

func (exec *hllMergeExec) Size() int64 { return hllStateSize(exec.state) }

func mergeHLLStates(destination, source *hllStateExec, offset int, groups []uint64) error {
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
			mob, err := destination.makeHLLSketch(destination.mp, nil)
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

func flushHLLSketches(exec *hllStateExec) (_ []*vector.Vector, retErr error) {
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
		empty := canonicalEmptyHLL
		if exec.legacyWireState {
			empty[0] = hllLegacyVersion
			if exec.floatZeroWireState {
				empty[0] = hllFloatZeroVersion
			}
		}
		for row := range int(state.length) {
			if retErr = vector.AppendBytes(vecs[chunk], empty[:], false, exec.mp); retErr != nil {
				return nil, retErr
			}
			if state.mobs[row] != nil {
				sketch := state.mobs[row].(*hllSketch)
				stored := vecs[chunk].GetBytesAt(row)
				stored[0] = sketch.effectiveWireVersion()
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
		size += int64(cap(state.argScratch))
	}
	return size
}
