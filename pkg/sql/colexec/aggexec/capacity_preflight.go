// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type aggregateBaseCarrier interface {
	aggregateBase() *aggExec
}

func (ae *aggExec) aggregateBase() *aggExec { return ae }

// The default preflight covers fixed-vector aggregates and the generic saved
// argument arena used by ordinary DISTINCT aggregates. Concrete varlen-state
// aggregates override it because only they know which candidates actually
// publish a new value.
func (ae *aggExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if ae == nil || ae.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if ae.saveArg {
		return ae.preflightBatchFillArgs(offset, groups, vectors, ae.isDistinct)
	}
	for _, typ := range ae.stateTypes {
		if typ.IsVarlen() {
			return mpool.ErrAllocationAccountInvariant
		}
	}
	return nil
}

func (ae *aggExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	if ae == nil || ae.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	otherCarrier, ok := next.(aggregateBaseCarrier)
	if !ok || otherCarrier.aggregateBase() == nil {
		return moerr.NewInternalErrorNoCtx("aggregate merge source has no base state")
	}
	if ae.saveArg {
		return ae.preflightBatchMergeArgs(
			otherCarrier.aggregateBase(), offset, groups)
	}
	for _, typ := range ae.stateTypes {
		if typ.IsVarlen() {
			return mpool.ErrAllocationAccountInvariant
		}
	}
	return nil
}

type argumentChunkCapacity struct {
	chunk         int
	arenaConsumed uint64
	arenaRequired uint64
	scratch       int
}

func addArgumentChunkCapacity(
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	count *int,
	chunk int,
	key []byte,
) error {
	return addArgumentChunkCapacityWithValue(
		needs, count, chunk, key, 0)
}

func addArgumentChunkCapacityWithValue(
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	count *int,
	chunk int,
	key []byte,
	valueSize int,
) error {
	if len(key) > math.MaxUint32 || valueSize < 0 || valueSize > math.MaxUint32 {
		return mpool.ErrAllocationAccountInvalid
	}
	plan := arenaskl.MakeAddPlan(key)
	consumed, _, ok := plan.ArenaFootprint(
		uint32(len(key)), uint32(valueSize))
	if !ok {
		return mpool.ErrAllocationAllocatorLimit
	}
	for i := 0; i < *count; i++ {
		if needs[i].chunk != chunk {
			continue
		}
		if needs[i].arenaConsumed > math.MaxUint64-consumed {
			return mpool.ErrAllocationAllocatorLimit
		}
		needs[i].arenaConsumed += consumed
		trailing := arenaskl.MaxNodeTrailingSize()
		if needs[i].arenaConsumed > math.MaxUint64-trailing {
			return mpool.ErrAllocationAllocatorLimit
		}
		needs[i].arenaRequired = max(
			needs[i].arenaRequired,
			needs[i].arenaConsumed+trailing,
		)
		needs[i].scratch = max(needs[i].scratch, len(key))
		return nil
	}
	if *count >= len(needs) {
		return mpool.ErrAllocationAccountInvalid
	}
	needs[*count] = argumentChunkCapacity{
		chunk:         chunk,
		arenaConsumed: consumed,
		arenaRequired: consumed + arenaskl.MaxNodeTrailingSize(),
		scratch:       len(key),
	}
	*count = *count + 1
	return nil
}

func (ae *aggExec) applyArgumentChunkCapacity(
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	count int,
) error {
	for i := 0; i < count; i++ {
		need := needs[i]
		state := ae.preflightStateAt(need.chunk)
		if state == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := state.preflightArgumentCapacity(
			ae.mp, need.arenaRequired, need.scratch); err != nil {
			return err
		}
	}
	return nil
}

func (ag *aggState) preflightArgumentCapacity(
	mp *mpool.MPool,
	additionalArenaRequired uint64,
	scratch int,
) error {
	if ag == nil || ag.allocation == nil || ag.argSkl == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if scratch > 0 {
		if _, err := ag.resizeArgScratch(mp, scratch); err != nil {
			return err
		}
	}
	arena := ag.argSkl.Arena()
	used := uint64(arena.Size())
	if used > math.MaxUint32 || additionalArenaRequired > math.MaxUint32-used {
		return mpool.ErrAllocationAllocatorLimit
	}
	required := used + additionalArenaRequired
	if required <= uint64(arena.Capacity()) {
		return nil
	}

	capacity, err := nextArgumentArenaCapacity(
		uint64(len(ag.argbuf)), required)
	if err != nil {
		return err
	}
	fallbackCapacity, err := nextLinearArgumentArenaCapacity(
		uint64(len(ag.argbuf)), required)
	if err != nil {
		return err
	}
	if ag.boundedArgumentGrowth {
		capacity, err = nextBoundedArgumentArenaCapacity(
			uint64(len(ag.argbuf)), required)
		if err != nil {
			return err
		}
		fallbackCapacity = capacity
	}
	// Geometric growth is speculative spare capacity. Clamp both candidates to
	// the physical single-allocation limit before admission so crossing that
	// boundary neither logs a failed oversized allocation nor rejects a smaller
	// arena that still satisfies required.
	maxAllocation := mpool.MaxAllocationSize()
	if maxAllocation <= 0 || required > uint64(maxAllocation) {
		return mpool.ErrAllocationAllocatorLimit
	}
	capacity = min(capacity, uint64(maxAllocation))
	fallbackCapacity = min(fallbackCapacity, uint64(maxAllocation))
	next, err := ag.allocation.allocArgumentArena(mp, int(capacity))
	if err != nil && capacity != fallbackCapacity &&
		mpool.IsRetryableAllocationCapacity(err) {
		// Geometric growth keeps repeated relocation linear in the final state
		// size, but the old arena remains charged until relocation publishes.
		// Under memory pressure, retain the former chunked-growth availability
		// envelope instead of failing a query solely because of speculative
		// spare capacity.
		next, err = ag.allocation.allocArgumentArena(mp, int(fallbackCapacity))
	}
	if err != nil {
		return err
	}
	if err = ag.argSkl.GrowArena(next); err != nil {
		mp.Free(next)
		return err
	}
	old := ag.argbuf
	ag.argbuf = next
	mp.Free(old)
	return nil
}

func nextBoundedArgumentArenaCapacity(current, required uint64) (uint64, error) {
	if required <= current {
		return current, nil
	}
	if current > math.MaxUint32 || required > math.MaxUint32 {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	const quantum = uint64(64 * 1024)
	missing := required - current
	steps := (missing + quantum - 1) / quantum
	if steps > (math.MaxUint32-current)/quantum {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	capacity := current + steps*quantum
	if capacity < required || capacity > uint64(math.MaxInt) {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	return capacity, nil
}

func nextArgumentArenaCapacity(current, required uint64) (uint64, error) {
	if required <= current {
		return current, nil
	}
	if current > math.MaxUint32 || required > math.MaxUint32 {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	capacity := current
	for capacity < required {
		grow := max(capacity, uint64(kAggArgArenaSize))
		if grow > math.MaxUint32-capacity {
			capacity = math.MaxUint32
		} else {
			capacity += grow
		}
		if capacity == math.MaxUint32 && capacity < required {
			return 0, mpool.ErrAllocationAllocatorLimit
		}
	}
	if capacity > uint64(math.MaxInt) {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	return capacity, nil
}

func nextLinearArgumentArenaCapacity(current, required uint64) (uint64, error) {
	if required <= current {
		return current, nil
	}
	if current > math.MaxUint32 || required > math.MaxUint32 {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	missing := required - current
	chunk := uint64(kAggArgArenaSize)
	steps := (missing + chunk - 1) / chunk
	grow := steps * chunk
	capacity := uint64(math.MaxUint32)
	if grow <= math.MaxUint32-current {
		capacity = current + grow
	}
	if capacity < required || capacity > uint64(math.MaxInt) {
		return 0, mpool.ErrAllocationAllocatorLimit
	}
	return capacity, nil
}

func validatePreflightVectors(
	vectors []*vector.Vector,
	offset int,
	rows int,
) error {
	if offset < 0 || rows < 0 || rows > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	for _, vec := range vectors {
		if !vec.CoversLogicalRows(offset, rows) {
			return mpool.ErrAllocationAccountInvalid
		}
	}
	return nil
}

func preflightPhysicalRow(vec *vector.Vector, logicalRow int) (int, error) {
	if vec == nil || logicalRow < 0 ||
		!vec.CoversLogicalRows(logicalRow, 1) {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	if vec.IsConst() {
		return 0, nil
	}
	return logicalRow, nil
}

func (ae *aggExec) preflightStateAt(chunk int) *aggState {
	if ae == nil || chunk < 0 {
		return nil
	}
	if chunk < len(ae.state) {
		return &ae.state[chunk]
	}
	standby := chunk - len(ae.state)
	if standby < 0 || standby >= len(ae.standby) {
		return nil
	}
	return &ae.standby[standby]
}

func (ae *aggExec) validatePreflightTarget(
	group uint64,
) (int, uint16, *aggState, error) {
	if group == GroupNotMatched {
		return 0, 0, nil, nil
	}
	index := group - 1
	x, y := ae.getXY(index)
	state := ae.preflightStateAt(x)
	if state == nil || int(y) >= int(state.capacity) {
		return 0, 0, nil, mpool.ErrAllocationAccountInvariant
	}
	return x, y, state, nil
}

func preflightArgumentRowsEqual(
	vectors []*vector.Vector,
	left int,
	right int,
) (bool, error) {
	for _, vec := range vectors {
		leftRow, err := preflightPhysicalRow(vec, left)
		if err != nil {
			return false, err
		}
		rightRow, err := preflightPhysicalRow(vec, right)
		if err != nil {
			return false, err
		}
		leftNull := vec.IsNull(uint64(leftRow))
		rightNull := vec.IsNull(uint64(rightRow))
		if leftNull || rightNull {
			return leftNull && rightNull, nil
		}
		if !distinctArgumentRowsEqual(vec, leftRow, rightRow) {
			return false, nil
		}
	}
	return true, nil
}

const distinctArgumentBatchSlots = hashmap.UnitLimit * 2
const distinctArgumentLinearLimit = 8

// distinctArgumentLinearBatch keeps the low-cardinality path hash-free. Most
// duplicate-heavy batches find a representative after one or two exact
// comparisons, avoiding both hashing large payloads and clearing the larger
// open-addressing table below.
type distinctArgumentLinearBatch struct {
	rows  [distinctArgumentLinearLimit]uint16
	count uint16
}

func (batch *distinctArgumentLinearBatch) seen(
	groups []uint64,
	vectors []*vector.Vector,
	offset int,
	row int,
) (bool, error) {
	// Cardinality one is the common duplicate-heavy case. Keep its control
	// flow as short as the former scan over earlier rows: one group check and
	// one exact comparison, without entering the representative loop.
	if batch.count > 0 {
		earlier := int(batch.rows[0])
		if groups[earlier] == groups[row] {
			equal, err := preflightArgumentRowsEqual(
				vectors, offset+earlier, offset+row)
			if err != nil {
				return false, err
			}
			if equal {
				return true, nil
			}
		}
	}
	for i := uint16(1); i < batch.count; i++ {
		earlier := int(batch.rows[i])
		if groups[earlier] != groups[row] {
			continue
		}
		equal, err := preflightArgumentRowsEqual(
			vectors, offset+earlier, offset+row)
		if err != nil {
			return false, err
		}
		if equal {
			return true, nil
		}
	}
	return false, nil
}

func (batch *distinctArgumentLinearBatch) insertUnique(row int) bool {
	if batch.count == distinctArgumentLinearLimit {
		return false
	}
	batch.rows[batch.count] = uint16(row)
	batch.count++
	return true
}

// distinctArgumentBatch suppresses duplicate candidates inside one immutable
// preflight work unit. The table is deliberately fixed-size: UnitLimit bounds
// the input at 256 rows, so twice as many open-addressing slots keep the load
// factor at or below 50% without row-frequency allocation.
type distinctArgumentBatch struct {
	rows   [distinctArgumentBatchSlots]uint16
	hashes [distinctArgumentBatchSlots]uint64
}

func distinctArgumentRowHash(
	group uint64,
	vectors []*vector.Vector,
	logicalRow int,
) (uint64, error) {
	// Start from the target group because DISTINCT is scoped to one aggregate
	// group. Hash combination preserves column boundaries for multi-argument
	// DISTINCT; full row comparison below remains the collision oracle.
	hash := group * 0x9e3779b97f4a7c15
	var zero [8]byte
	for _, vec := range vectors {
		row, err := preflightPhysicalRow(vec, logicalRow)
		if err != nil {
			return 0, err
		}
		value := vec.GetRawBytesAt(row)
		if size, ok := canonicalDistinctArgumentSize(vec, row); ok {
			value = zero[:size]
		}
		hash = keycodec.HashCombine(hash, xxhash.Sum64(value))
	}
	return hash, nil
}

func (batch *distinctArgumentBatch) seenOrInsert(
	groups []uint64,
	vectors []*vector.Vector,
	offset int,
	row int,
) (bool, error) {
	if batch == nil || row < 0 || row >= len(groups) || len(groups) > hashmap.UnitLimit {
		return false, mpool.ErrAllocationAccountInvalid
	}
	hash, err := distinctArgumentRowHash(groups[row], vectors, offset+row)
	if err != nil {
		return false, err
	}
	const mask = distinctArgumentBatchSlots - 1
	for slot := int(hash & uint64(mask)); ; slot = (slot + 1) & mask {
		encodedRow := batch.rows[slot]
		if encodedRow == 0 {
			batch.rows[slot] = uint16(row + 1)
			batch.hashes[slot] = hash
			return false, nil
		}
		earlier := int(encodedRow - 1)
		if batch.hashes[slot] != hash || groups[earlier] != groups[row] {
			continue
		}
		equal, err := preflightArgumentRowsEqual(
			vectors, offset+earlier, offset+row)
		if err != nil {
			return false, err
		}
		if equal {
			return true, nil
		}
	}
}

func (ag *aggState) preparePreflightArgumentKey(
	mp *mpool.MPool,
	y uint16,
	vectors []*vector.Vector,
	logicalRow int,
	payload int,
	distinct bool,
	ordinal uint32,
) ([]byte, error) {
	header := kAggArgPrefixSz
	if !distinct {
		header += kAggArgOrdinalSz
	}
	if ag == nil || ag.argSkl == nil || payload < 0 ||
		payload > math.MaxInt-header {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	key, err := ag.resizeArgScratch(mp, header+payload)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], y)
	if !distinct {
		binary.BigEndian.PutUint32(key[kAggArgPrefixSz:header], ordinal)
	}
	off := header
	if len(vectors) == 1 {
		row, err := preflightPhysicalRow(vectors[0], logicalRow)
		if err != nil {
			return nil, err
		}
		raw := vectors[0].GetRawBytesAt(row)
		if distinct {
			copyCanonicalDistinctArgument(key[off:], vectors[0], row)
		} else {
			copy(key[off:], raw)
		}
	} else {
		for _, vec := range vectors {
			row, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return nil, err
			}
			raw := vec.GetRawBytesAt(row)
			binary.BigEndian.PutUint32(key[off:], uint32(len(raw)))
			off += 4
			if distinct {
				copyCanonicalDistinctArgument(key[off:], vec, row)
			} else {
				copy(key[off:], raw)
			}
			off += len(raw)
		}
	}
	return key, nil
}

type argumentTargetProgress struct {
	chunk int
	row   uint16
	added uint32
}

func nextArgumentOrdinal(
	progress *[hashmap.UnitLimit]argumentTargetProgress,
	count *int,
	chunk int,
	row uint16,
	base uint32,
) (uint32, error) {
	for i := 0; i < *count; i++ {
		if progress[i].chunk != chunk || progress[i].row != row {
			continue
		}
		if progress[i].added == math.MaxUint32-base {
			return 0, mpool.ErrAllocationAllocatorLimit
		}
		ordinal := base + progress[i].added
		progress[i].added++
		return ordinal, nil
	}
	if *count >= len(progress) {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	progress[*count] = argumentTargetProgress{
		chunk: chunk,
		row:   row,
		added: 1,
	}
	*count++
	return base, nil
}

func (ae *aggExec) preflightBatchFillArgs(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
	distinct bool,
) error {
	if len(vectors) == 0 || len(vectors) != len(ae.argTypes) {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	if distinct {
		return ae.preflightDistinctBatchFillArgs(offset, groups, vectors)
	}
	if len(vectors) == 1 {
		return ae.preflightNonDistinctSingleBatchFillArg(
			offset, groups, vectors)
	}
	return ae.preflightNonDistinctBatchFillArgs(offset, groups, vectors)
}

func (ae *aggExec) preflightNonDistinctSingleBatchFillArg(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	vec := vectors[0]
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	const header = kAggArgPrefixSz + kAggArgOrdinalSz
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := ae.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		logicalRow := offset + i
		row, err := preflightPhysicalRow(vec, logicalRow)
		if err != nil {
			return err
		}
		if vec.IsNull(uint64(row)) {
			continue
		}
		payload := len(vec.GetRawBytesAt(row))
		if payload > math.MaxInt-header {
			return mpool.ErrAllocationAllocatorLimit
		}
		ordinal, err := nextArgumentOrdinal(
			&progress, &progressCount, x, y, state.argCnt[y])
		if err != nil {
			return err
		}
		key, err := state.preparePreflightArgumentKey(
			ae.mp, y, vectors, logicalRow, payload, false, ordinal)
		if err != nil {
			return err
		}
		if err = addArgumentChunkCapacity(
			&needs, &needCount, x, key); err != nil {
			return err
		}
	}
	return ae.applyArgumentChunkCapacity(&needs, needCount)
}

func (ae *aggExec) preflightNonDistinctBatchFillArgs(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	const header = kAggArgPrefixSz + kAggArgOrdinalSz
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := ae.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		logicalRow := offset + i
		payload := 0
		for _, vec := range vectors {
			row, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			if vec.IsNull(uint64(row)) {
				payload = -1
				break
			}
			raw := vec.GetRawBytesAt(row)
			if len(raw) > math.MaxInt-payload-4 {
				return mpool.ErrAllocationAllocatorLimit
			}
			payload += 4 + len(raw)
		}
		if payload < 0 {
			continue
		}
		if payload > math.MaxInt-header {
			return mpool.ErrAllocationAllocatorLimit
		}
		ordinal, err := nextArgumentOrdinal(
			&progress, &progressCount, x, y, state.argCnt[y])
		if err != nil {
			return err
		}
		key, err := state.preparePreflightArgumentKey(
			ae.mp, y, vectors, logicalRow, payload, false, ordinal)
		if err != nil {
			return err
		}
		if err := addArgumentChunkCapacity(
			&needs, &needCount, x, key); err != nil {
			return err
		}
	}
	return ae.applyArgumentChunkCapacity(&needs, needCount)
}

func (ae *aggExec) addDistinctArgumentCapacity(
	x int,
	y uint16,
	state *aggState,
	logicalRow int,
	payload int,
	vectors []*vector.Vector,
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	needCount *int,
) error {
	key, err := state.preparePreflightArgumentKey(
		ae.mp, y, vectors, logicalRow, payload, true, 0)
	if err != nil {
		return err
	}
	if state.argSkl.Contains(key) {
		return nil
	}
	return addArgumentChunkCapacity(needs, needCount, x, key)
}

func (ae *aggExec) preflightDistinctBatchFillArgs(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var linear distinctArgumentLinearBatch
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := ae.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		logicalRow := offset + i
		payload := 0
		physicalRow := 0
		if len(vectors) == 1 {
			physicalRow, err = preflightPhysicalRow(vectors[0], logicalRow)
			if err != nil {
				return err
			}
			if vectors[0].IsNull(uint64(physicalRow)) {
				continue
			}
		} else {
			for _, vec := range vectors {
				row, err := preflightPhysicalRow(vec, logicalRow)
				if err != nil {
					return err
				}
				if vec.IsNull(uint64(row)) {
					payload = -1
					break
				}
				raw := vec.GetRawBytesAt(row)
				if len(raw) > math.MaxInt-payload-4 {
					return mpool.ErrAllocationAllocatorLimit
				}
				payload += 4 + len(raw)
			}
			if payload < 0 {
				continue
			}
		}
		duplicate, err := linear.seen(groups, vectors, offset, i)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if !linear.insertUnique(i) {
			return ae.preflightDistinctBatchFillArgsHashed(
				offset, groups, vectors, i, &linear, &needs, &needCount)
		}
		if len(vectors) == 1 {
			// Duplicate rows need neither a retained key nor its payload size.
			// Delay the raw-value access until exact admission accepts this row.
			payload = len(vectors[0].GetRawBytesAt(physicalRow))
		}
		if payload > math.MaxInt-kAggArgPrefixSz {
			return mpool.ErrAllocationAllocatorLimit
		}
		if err = ae.addDistinctArgumentCapacity(
			x, y, state, logicalRow, payload, vectors, &needs, &needCount); err != nil {
			return err
		}
	}
	return ae.applyArgumentChunkCapacity(&needs, needCount)
}

// preflightDistinctBatchFillArgsHashed owns the large table in a separate
// frame and is called only after the exact representative set overflows. This
// mirrors radix aggregation admission: hash work is paid only for a batch with
// enough distinct tuples to amortize it.
func (ae *aggExec) preflightDistinctBatchFillArgsHashed(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
	start int,
	linear *distinctArgumentLinearBatch,
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	needCount *int,
) error {
	if start < 0 || start >= len(groups) || linear == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	var hashed distinctArgumentBatch
	for i := uint16(0); i < linear.count; i++ {
		duplicate, err := hashed.seenOrInsert(
			groups, vectors, offset, int(linear.rows[i]))
		if err != nil {
			return err
		}
		if duplicate {
			return mpool.ErrAllocationAccountInvariant
		}
	}
	for i := start; i < len(groups); i++ {
		group := groups[i]
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := ae.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		logicalRow := offset + i
		payload := 0
		physicalRow := 0
		if len(vectors) == 1 {
			physicalRow, err = preflightPhysicalRow(vectors[0], logicalRow)
			if err != nil {
				return err
			}
			if vectors[0].IsNull(uint64(physicalRow)) {
				continue
			}
		} else {
			for _, vec := range vectors {
				row, err := preflightPhysicalRow(vec, logicalRow)
				if err != nil {
					return err
				}
				if vec.IsNull(uint64(row)) {
					payload = -1
					break
				}
				raw := vec.GetRawBytesAt(row)
				if len(raw) > math.MaxInt-payload-4 {
					return mpool.ErrAllocationAllocatorLimit
				}
				payload += 4 + len(raw)
			}
			if payload < 0 {
				continue
			}
		}
		duplicate, err := hashed.seenOrInsert(groups, vectors, offset, i)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if len(vectors) == 1 {
			payload = len(vectors[0].GetRawBytesAt(physicalRow))
		}
		if payload > math.MaxInt-kAggArgPrefixSz {
			return mpool.ErrAllocationAllocatorLimit
		}
		if err = ae.addDistinctArgumentCapacity(
			x, y, state, logicalRow, payload, vectors, needs, needCount); err != nil {
			return err
		}
	}
	return ae.applyArgumentChunkCapacity(needs, *needCount)
}

func (ae *aggExec) preflightBatchMergeArgs(
	other *aggExec,
	offset int,
	groups []uint64,
) error {
	if other == nil || offset < 0 || offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := ae.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		otherX, otherY := other.getXY(uint64(offset + i))
		if otherX < 0 || otherX >= len(other.state) ||
			int(otherY) >= int(other.state[otherX].length) {
			return mpool.ErrAllocationAccountInvariant
		}
		var upper [kAggArgPrefixSz]byte
		binary.BigEndian.PutUint16(upper[:], y+1)
		var targetIter *arenaskl.Iterator
		var targetKey []byte
		var targetOK bool
		err = other.state[otherX].iter(otherY, func(key []byte) error {
			if len(key) < kAggArgPrefixSz {
				return mpool.ErrAllocationAccountInvariant
			}
			candidate, err := state.resizeArgScratch(ae.mp, len(key))
			if err != nil {
				return err
			}
			copy(candidate, key)
			binary.BigEndian.PutUint16(candidate[:kAggArgPrefixSz], y)
			if ae.isDistinct {
				if targetIter == nil {
					targetIter = state.argSkl.NewIter(nil, upper[:])
					targetOK, targetKey, _ = targetIter.SeekGE(candidate)
				}
				for targetOK && bytes.Compare(targetKey, candidate) < 0 {
					targetOK, targetKey, _ = targetIter.Next()
				}
				if targetOK && bytes.Equal(targetKey, candidate) {
					return nil
				}
				// A merge work unit can map several source groups into the same
				// target group. The target skiplist is immutable until admission,
				// so also suppress a value already supplied by an earlier source
				// group in this unit. Rewriting only the two-byte group prefix lets
				// each source skiplist answer that lookup without a temporary set.
				for earlier := 0; earlier < i; earlier++ {
					if groups[earlier] != group {
						continue
					}
					earlierX, earlierY := other.getXY(uint64(offset + earlier))
					if earlierX < 0 || earlierX >= len(other.state) ||
						int(earlierY) >= int(other.state[earlierX].length) {
						return mpool.ErrAllocationAccountInvariant
					}
					binary.BigEndian.PutUint16(
						candidate[:kAggArgPrefixSz], earlierY)
					if other.state[earlierX].argSkl.Contains(candidate) {
						return nil
					}
				}
				binary.BigEndian.PutUint16(candidate[:kAggArgPrefixSz], y)
			} else {
				if len(candidate) < kAggArgPrefixSz+kAggArgOrdinalSz {
					return mpool.ErrAllocationAccountInvariant
				}
				ordinal, err := nextArgumentOrdinal(
					&progress, &progressCount, x, y, state.argCnt[y])
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint32(
					candidate[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz],
					ordinal,
				)
			}
			valueSize := 0
			if ae.preserveDistinctInputOrder {
				valueSize = kAggArgOrdinalSz
			}
			return addArgumentChunkCapacityWithValue(
				&needs, &needCount, x, candidate, valueSize)
		})
		if targetIter != nil {
			targetIter.Close()
		}
		if err != nil {
			return err
		}
	}
	return ae.applyArgumentChunkCapacity(&needs, needCount)
}

type vectorAreaChunkCapacity struct {
	chunk int
	bytes [3]int
}

type prepareParamKindEvent struct {
	chunk  int
	column int
	row    int
	kind   vector.PrepareParamKind
}

type stringSourceEvent struct {
	chunk  int
	column int
	row    int
	source types.StringSource
}

func addPrepareParamKindEvent(
	events *[hashmap.UnitLimit]prepareParamKindEvent,
	count *int,
	chunk int,
	column int,
	row int,
	kind vector.PrepareParamKind,
) error {
	if chunk < 0 || column < 0 || row < 0 || *count >= len(events) {
		return mpool.ErrAllocationAccountInvalid
	}
	events[*count] = prepareParamKindEvent{
		chunk: chunk, column: column, row: row, kind: kind,
	}
	*count++
	return nil
}

func addStringSourceEvent(
	events *[hashmap.UnitLimit]stringSourceEvent,
	count *int,
	chunk int,
	column int,
	row int,
	source types.StringSource,
) error {
	if chunk < 0 || column < 0 || row < 0 || !source.Valid() || *count >= len(events) {
		return mpool.ErrAllocationAccountInvalid
	}
	events[*count] = stringSourceEvent{
		chunk: chunk, column: column, row: row, source: source,
	}
	*count++
	return nil
}

func (ae *aggExec) applyStringSourceEvents(
	events *[hashmap.UnitLimit]stringSourceEvent,
	count int,
) error {
	for i := 0; i < count; i++ {
		event := events[i]
		state := ae.preflightStateAt(event.chunk)
		if state == nil || event.column >= len(state.vecs) {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := state.vecs[event.column].PreflightSetStringSourceAtLength(
			event.row, max(int(state.length), event.row+1), event.source, ae.mp); err != nil {
			return err
		}
		// Runtime may publish several same/mixed updates to this vector. Keep
		// its admitted sidecar alive until the enclosing batch completes.
		state.vecs[event.column].RetainStringSourcePreflight()
	}
	return nil
}

func (ae *aggExec) applyPrepareParamKindEvents(
	events *[hashmap.UnitLimit]prepareParamKindEvent,
	count int,
) error {
	var processed [hashmap.UnitLimit]bool
	var rows [hashmap.UnitLimit]int
	var kinds [hashmap.UnitLimit]vector.PrepareParamKind
	for i := 0; i < count; i++ {
		if processed[i] {
			continue
		}
		event := events[i]
		state := ae.preflightStateAt(event.chunk)
		if state == nil || event.column >= len(state.vecs) {
			return mpool.ErrAllocationAccountInvariant
		}
		n := 0
		finalLength := int(state.length)
		for j := i; j < count; j++ {
			candidate := events[j]
			if candidate.chunk != event.chunk || candidate.column != event.column {
				continue
			}
			processed[j] = true
			rows[n] = candidate.row
			kinds[n] = candidate.kind
			finalLength = max(finalLength, candidate.row+1)
			n++
		}
		if err := state.vecs[event.column].PreflightSetPrepareParamKindsAtLength(
			rows[:n], kinds[:n], finalLength, ae.mp); err != nil {
			return err
		}
	}
	return nil
}

func addVectorAreaCapacity(
	needs *[hashmap.UnitLimit]vectorAreaChunkCapacity,
	count *int,
	chunk int,
	column int,
	value []byte,
) error {
	if column < 0 || column >= len(needs[0].bytes) ||
		len(value) <= types.VarlenaInlineSize {
		return nil
	}
	for i := 0; i < *count; i++ {
		if needs[i].chunk != chunk {
			continue
		}
		if len(value) > math.MaxInt-needs[i].bytes[column] {
			return mpool.ErrAllocationAllocatorLimit
		}
		needs[i].bytes[column] += len(value)
		return nil
	}
	if *count >= len(needs) {
		return mpool.ErrAllocationAccountInvalid
	}
	needs[*count].chunk = chunk
	needs[*count].bytes[column] = len(value)
	*count = *count + 1
	return nil
}

func (ae *aggExec) applyVectorAreaCapacity(
	needs *[hashmap.UnitLimit]vectorAreaChunkCapacity,
	count int,
) error {
	for i := 0; i < count; i++ {
		need := needs[i]
		state := ae.preflightStateAt(need.chunk)
		if state == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		for column, area := range need.bytes {
			if area == 0 {
				continue
			}
			if column >= len(state.vecs) {
				return mpool.ErrAllocationAccountInvariant
			}
			if err := state.vecs[column].PreExtendWithArea(
				0, area, ae.mp); err != nil {
				return err
			}
		}
	}
	return nil
}

type preflightGroupWinner struct {
	group     uint64
	winner    int
	kind      vector.PrepareParamKind
	source    types.StringSource
	sourceSet bool
}

func winnerForGroup(
	winners *[hashmap.UnitLimit]preflightGroupWinner,
	count *int,
	group uint64,
) (*preflightGroupWinner, error) {
	for i := 0; i < *count; i++ {
		if winners[i].group == group {
			return &winners[i], nil
		}
	}
	if *count >= len(winners) {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	winners[*count] = preflightGroupWinner{group: group, winner: -1}
	entry := &winners[*count]
	*count = *count + 1
	return entry, nil
}

func (exec *anyExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit || len(vectors) != 1 {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	var kindEvents [hashmap.UnitLimit]prepareParamKindEvent
	kindEventCount := 0
	var sourceEvents [hashmap.UnitLimit]stringSourceEvent
	sourceEventCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		row, err := preflightPhysicalRow(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(row)) ||
			!state.vecs[0].IsNull(uint64(y)) {
			continue
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil || winner.winner >= 0 {
			if err != nil {
				return err
			}
			continue
		}
		winner.winner = row
		kind := vectors[0].GetPrepareParamKindAt(row)
		if err = addStringSourceEvent(
			&sourceEvents, &sourceEventCount, x, 0, int(y),
			vectors[0].GetStringSourceAt(row)); err != nil {
			return err
		}
		if err = addPrepareParamKindEvent(
			&kindEvents, &kindEventCount, x, 0, int(y), kind); err != nil {
			return err
		}
		if err = addVectorAreaCapacity(
			&needs, &needCount, x, 0, vectors[0].GetRawBytesAt(row)); err != nil {
			return err
		}
	}
	if err := exec.applyStringSourceEvents(&sourceEvents, sourceEventCount); err != nil {
		return err
	}
	if err := exec.applyPrepareParamKindEvents(&kindEvents, kindEventCount); err != nil {
		return err
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

func (exec *anyExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*anyExec)
	if !ok || other == nil {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible any_value states")
	}
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit || offset < 0 ||
		offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	var kindEvents [hashmap.UnitLimit]prepareParamKindEvent
	kindEventCount := 0
	var sourceEvents [hashmap.UnitLimit]stringSourceEvent
	sourceEventCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		otherX, otherY := other.getXY(uint64(offset + i))
		if otherX < 0 || otherX >= len(other.state) ||
			int(otherY) >= int(other.state[otherX].length) {
			return mpool.ErrAllocationAccountInvariant
		}
		if other.state[otherX].vecs[0].IsNull(uint64(otherY)) ||
			!state.vecs[0].IsNull(uint64(y)) {
			continue
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil || winner.winner >= 0 {
			if err != nil {
				return err
			}
			continue
		}
		winner.winner = offset + i
		source := other.state[otherX].vecs[0]
		kind := source.GetPrepareParamKindAt(int(otherY))
		if err = addStringSourceEvent(
			&sourceEvents, &sourceEventCount, x, 0, int(y),
			source.GetStringSourceAt(int(otherY))); err != nil {
			return err
		}
		if err = addPrepareParamKindEvent(
			&kindEvents, &kindEventCount, x, 0, int(y), kind); err != nil {
			return err
		}
		if err = addVectorAreaCapacity(
			&needs, &needCount, x, 0, source.GetRawBytesAt(int(otherY))); err != nil {
			return err
		}
	}
	if err := exec.applyStringSourceEvents(&sourceEvents, sourceEventCount); err != nil {
		return err
	}
	if err := exec.applyPrepareParamKindEvents(&kindEvents, kindEventCount); err != nil {
		return err
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

func (exec *bitOpExecBytes) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit || len(vectors) != 1 {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row, err := preflightPhysicalRow(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		if !state.vecs[0].IsNull(uint64(y)) {
			continue
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil {
			return err
		}
		if winner.winner >= 0 {
			continue
		}
		winner.winner = row
		if err = addVectorAreaCapacity(
			&needs, &needCount, x, 0, vectors[0].GetRawBytesAt(row)); err != nil {
			return err
		}
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

func (exec *bitOpExecBytes) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*bitOpExecBytes)
	if !ok || other == nil || exec == nil || exec.op != other.op ||
		exec.width != other.width {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible binary bit states")
	}
	if exec.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit || offset < 0 ||
		offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		if !state.vecs[0].IsNull(uint64(y)) {
			continue
		}
		otherX, otherY := other.getXY(uint64(offset + i))
		if otherX < 0 || otherX >= len(other.state) ||
			int(otherY) >= int(other.state[otherX].length) {
			return mpool.ErrAllocationAccountInvariant
		}
		source := other.state[otherX].vecs[0]
		if source.IsNull(uint64(otherY)) {
			continue
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil {
			return err
		}
		if winner.winner >= 0 {
			continue
		}
		winner.winner = offset + i
		if err = addVectorAreaCapacity(
			&needs, &needCount, x, 0,
			source.GetRawBytesAt(int(otherY))); err != nil {
			return err
		}
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

type minMaxCandidate func(row int) (
	value []byte,
	kind vector.PrepareParamKind,
	present bool,
	err error,
)

type fixedMinMaxCandidate[T types.FixedSizeT] func(row int) (
	value T,
	kind vector.PrepareParamKind,
	present bool,
	err error,
)

type fixedMinMaxWinner[T types.FixedSizeT] struct {
	group   uint64
	value   T
	kind    vector.PrepareParamKind
	present bool
	changed bool
}

func fixedMinMaxWinnerForGroup[T types.FixedSizeT](
	winners *[hashmap.UnitLimit]fixedMinMaxWinner[T],
	count *int,
	group uint64,
) (*fixedMinMaxWinner[T], error) {
	for i := 0; i < *count; i++ {
		if winners[i].group == group {
			return &winners[i], nil
		}
	}
	if *count >= len(winners) {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	winners[*count].group = group
	winner := &winners[*count]
	*count++
	return winner, nil
}

func (exec *minMaxExecFixed[T]) preflightFixedCandidates(
	groups []uint64,
	candidate fixedMinMaxCandidate[T],
	recordEveryMutation bool,
) error {
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	var winners [hashmap.UnitLimit]fixedMinMaxWinner[T]
	winnerCount := 0
	var events [hashmap.UnitLimit]prepareParamKindEvent
	eventCount := 0
	for row, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		value, kind, present, err := candidate(row)
		if err != nil || !present {
			if err != nil {
				return err
			}
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		winner, err := fixedMinMaxWinnerForGroup(&winners, &winnerCount, group)
		if err != nil {
			return err
		}
		if !winner.present && !state.vecs[0].IsNull(uint64(y)) {
			winner.value = vector.GetFixedAtNoTypeCheck[T](state.vecs[0], int(y))
			winner.kind = state.vecs[0].GetPrepareParamKindAt(int(y))
			winner.present = true
		}
		mutation := false
		switch {
		case !winner.present:
			winner.value = value
			winner.kind = kind
			winner.present = true
			winner.changed = true
			mutation = true
		default:
			cmp := exec.comp(value, winner.value)
			switch {
			case cmp < 0:
				winner.value = value
				winner.kind = kind
				winner.changed = true
				mutation = true
			case cmp == 0:
				winner.kind = vector.MergePrepareParamKinds(winner.kind, kind)
				winner.changed = true
				mutation = true
			}
		}
		if recordEveryMutation && mutation {
			if err = addPrepareParamKindEvent(
				&events, &eventCount, x, 0, int(y), winner.kind); err != nil {
				return err
			}
		}
	}
	if !recordEveryMutation {
		for i := 0; i < winnerCount; i++ {
			winner := &winners[i]
			if !winner.changed {
				continue
			}
			x, y, _, err := exec.validatePreflightTarget(winner.group)
			if err != nil {
				return err
			}
			if err = addPrepareParamKindEvent(
				&events, &eventCount, x, 0, int(y), winner.kind); err != nil {
				return err
			}
		}
	}
	return exec.applyPrepareParamKindEvents(&events, eventCount)
}

func (exec *minMaxExecFixed[T]) preflightFixedStringSources(
	groups []uint64,
	candidate fixedMinMaxCandidate[T],
	candidateSource func(row int) (types.StringSource, error),
) error {
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	// Runtime admits every non-NULL candidate source before comparing values.
	// Mirror that admission order exactly: losers and transient winners can
	// require a mixed sidecar even when the final representatives stay uniform.
	var events [hashmap.UnitLimit]stringSourceEvent
	eventCount := 0
	for row, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		_, _, present, err := candidate(row)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		source, err := candidateSource(row)
		if err != nil {
			return err
		}
		x, y, _, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		if err = addStringSourceEvent(
			&events, &eventCount, x, 0, int(y), source); err != nil {
			return err
		}
	}
	return exec.applyStringSourceEvents(&events, eventCount)
}

func (exec *minMaxExecFixed[T]) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 1 {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	values := vector.MustFixedColNoTypeCheck[T](vectors[0])
	candidate := func(row int) (T, vector.PrepareParamKind, bool, error) {
		physicalRow, err := preflightPhysicalRow(vectors[0], offset+row)
		if err != nil {
			var zero T
			return zero, vector.PrepareParamNone, false, err
		}
		if vectors[0].IsNull(uint64(physicalRow)) {
			var zero T
			return zero, vector.PrepareParamNone, false, nil
		}
		return values[physicalRow],
			vectors[0].GetPrepareParamKindAt(physicalRow), true, nil
	}
	if err := exec.preflightFixedStringSources(groups, candidate,
		func(row int) (types.StringSource, error) {
			physicalRow, err := preflightPhysicalRow(vectors[0], offset+row)
			if err != nil {
				return types.StringSourceExpression, err
			}
			return vectors[0].GetStringSourceAt(physicalRow), nil
		}); err != nil {
		return err
	}
	return exec.preflightFixedCandidates(groups, candidate, false)
}

func (exec *minMaxExecFixed[T]) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*minMaxExecFixed[T])
	if !ok || other == nil {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible min/max states")
	}
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if offset < 0 || offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	candidate := func(row int) (T, vector.PrepareParamKind, bool, error) {
		x, y := other.getXY(uint64(offset + row))
		if x < 0 || x >= len(other.state) ||
			int(y) >= int(other.state[x].length) {
			var zero T
			return zero, vector.PrepareParamNone, false,
				mpool.ErrAllocationAccountInvariant
		}
		source := other.state[x].vecs[0]
		if source.IsNull(uint64(y)) {
			var zero T
			return zero, vector.PrepareParamNone, false, nil
		}
		return vector.GetFixedAtNoTypeCheck[T](source, int(y)),
			source.GetPrepareParamKindAt(int(y)), true, nil
	}
	if err := exec.preflightFixedStringSources(groups, candidate,
		func(row int) (types.StringSource, error) {
			x, y := other.getXY(uint64(offset + row))
			if x < 0 || x >= len(other.state) ||
				int(y) >= int(other.state[x].length) {
				return types.StringSourceExpression, mpool.ErrAllocationAccountInvariant
			}
			return other.state[x].vecs[0].GetStringSourceAt(int(y)), nil
		}); err != nil {
		return err
	}
	return exec.preflightFixedCandidates(groups, candidate, true)
}

func (exec *minMaxExecBytes) preflightCandidates(
	groups []uint64,
	candidate minMaxCandidate,
	candidateSource func(row int) (types.StringSource, error),
) error {
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	var kindEvents [hashmap.UnitLimit]prepareParamKindEvent
	kindEventCount := 0
	var sourceEvents [hashmap.UnitLimit]stringSourceEvent
	sourceEventCount := 0
	for row, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		value, kind, present, err := candidate(row)
		if err != nil || !present {
			if err != nil {
				return err
			}
			continue
		}
		source, err := candidateSource(row)
		if err != nil {
			return err
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil {
			return err
		}
		var current []byte
		var currentKind vector.PrepareParamKind
		var currentSource types.StringSource
		hasCurrent := false
		if winner.winner >= 0 {
			var present bool
			current, _, present, err = candidate(winner.winner)
			if err != nil {
				return err
			}
			if !present {
				return mpool.ErrAllocationAccountInvariant
			}
			currentKind = winner.kind
			currentSource = winner.source
			hasCurrent = true
		} else if !state.vecs[0].IsNull(uint64(y)) {
			current = state.vecs[0].GetBytesAt(int(y))
			currentKind = state.vecs[0].GetPrepareParamKindAt(int(y))
			currentSource = state.vecs[0].GetStringSourceAt(int(y))
			hasCurrent = true
		}
		cmp := -1
		if hasCurrent {
			cmp = exec.comp(value, current)
		}
		switch {
		case cmp < 0:
			winner.winner = row
			winner.kind = kind
			winner.source = source
			if err = addStringSourceEvent(
				&sourceEvents, &sourceEventCount, x, 0, int(y), source); err != nil {
				return err
			}
			if err = addPrepareParamKindEvent(
				&kindEvents, &kindEventCount, x, 0, int(y), kind); err != nil {
				return err
			}
			if err = addVectorAreaCapacity(
				&needs, &needCount, x, 0, value); err != nil {
				return err
			}
		case cmp == 0:
			winner.kind = vector.MergePrepareParamKinds(currentKind, kind)
			winner.source, err = types.MergeStringSources(currentSource, source)
			if err != nil {
				return err
			}
			if err = addStringSourceEvent(
				&sourceEvents, &sourceEventCount, x, 0, int(y), winner.source); err != nil {
				return err
			}
			if err = addPrepareParamKindEvent(
				&kindEvents, &kindEventCount, x, 0, int(y), winner.kind); err != nil {
				return err
			}
		}
	}
	if err := exec.applyStringSourceEvents(&sourceEvents, sourceEventCount); err != nil {
		return err
	}
	if err := exec.applyPrepareParamKindEvents(&kindEvents, kindEventCount); err != nil {
		return err
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

func (exec *minMaxExecBytes) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 1 {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	return exec.preflightCandidates(groups,
		func(row int) ([]byte, vector.PrepareParamKind, bool, error) {
			index, err := preflightPhysicalRow(vectors[0], offset+row)
			if err != nil {
				return nil, vector.PrepareParamNone, false, err
			}
			if vectors[0].IsNull(uint64(index)) {
				return nil, vector.PrepareParamNone, false, nil
			}
			return vectors[0].GetBytesAt(index),
				vectors[0].GetPrepareParamKindAt(index), true, nil
		}, func(row int) (types.StringSource, error) {
			index, err := preflightPhysicalRow(vectors[0], offset+row)
			if err != nil {
				return types.StringSourceExpression, err
			}
			return vectors[0].GetStringSourceAt(index), nil
		})
}

func (exec *minMaxExecBytes) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*minMaxExecBytes)
	if !ok || other == nil {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible min/max states")
	}
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if offset < 0 || offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	return exec.preflightCandidates(groups,
		func(row int) ([]byte, vector.PrepareParamKind, bool, error) {
			x, y := other.getXY(uint64(offset + row))
			if x < 0 || x >= len(other.state) ||
				int(y) >= int(other.state[x].length) {
				return nil, vector.PrepareParamNone, false, mpool.ErrAllocationAccountInvariant
			}
			source := other.state[x].vecs[0]
			if source.IsNull(uint64(y)) {
				return nil, vector.PrepareParamNone, false, nil
			}
			return source.GetBytesAt(int(y)),
				source.GetPrepareParamKindAt(int(y)), true, nil
		}, func(row int) (types.StringSource, error) {
			x, y := other.getXY(uint64(offset + row))
			if x < 0 || x >= len(other.state) ||
				int(y) >= int(other.state[x].length) {
				return types.StringSourceExpression, mpool.ErrAllocationAccountInvariant
			}
			return other.state[x].vecs[0].GetStringSourceAt(int(y)), nil
		})
}

type maxByCandidate func(row int) (
	[]*vector.Vector,
	[3]int,
	error,
)

func (exec *maxByExec) preflightCandidates(
	groups []uint64,
	candidate maxByCandidate,
) error {
	if len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	var winners [hashmap.UnitLimit]preflightGroupWinner
	winnerCount := 0
	var needs [hashmap.UnitLimit]vectorAreaChunkCapacity
	needCount := 0
	var kindEvents [hashmap.UnitLimit]prepareParamKindEvent
	kindEventCount := 0
	var sourceEvents [hashmap.UnitLimit]stringSourceEvent
	sourceEventCount := 0
	for row, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		vectors, rows, err := candidate(row)
		if err != nil {
			return err
		}
		if vectors[1].IsNull(uint64(rows[1])) ||
			exec.nonNullValue && vectors[0].IsNull(uint64(rows[0])) {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		winner, err := winnerForGroup(&winners, &winnerCount, group)
		if err != nil {
			return err
		}
		current := state.vecs
		currentRows := [3]int{int(y), int(y), int(y)}
		if winner.winner >= 0 {
			current, currentRows, err = candidate(winner.winner)
			if err != nil {
				return err
			}
			vectors, rows, err = candidate(row)
			if err != nil {
				return err
			}
		}
		currentPresent := !current[1].IsNull(uint64(currentRows[1]))
		wins := !currentPresent || maxByCandidateWins(
			vectors, rows, current, currentRows, exec.argTypes)
		equal := currentPresent && maxByCandidateEquals(
			vectors, rows, current, currentRows, exec.argTypes)
		if !wins && !equal {
			continue
		}
		if equal {
			currentSource := current[0].GetStringSourceAt(currentRows[0])
			if winner.sourceSet {
				currentSource = winner.source
			}
			winner.source, err = types.MergeStringSources(
				currentSource, vectors[0].GetStringSourceAt(rows[0]))
			if err != nil {
				return err
			}
			winner.sourceSet = true
			if err = addStringSourceEvent(
				&sourceEvents, &sourceEventCount, x, 0, int(y), winner.source); err != nil {
				return err
			}
			continue
		}
		winner.winner = row
		winner.source = vectors[0].GetStringSourceAt(rows[0])
		winner.sourceSet = true
		if err = addStringSourceEvent(
			&sourceEvents, &sourceEventCount, x, 0, int(y), winner.source); err != nil {
			return err
		}
		if !vectors[0].IsNull(uint64(rows[0])) {
			kind := vectors[0].GetPrepareParamKindAt(rows[0])
			if err = addPrepareParamKindEvent(
				&kindEvents, &kindEventCount, x, 0, int(y), kind); err != nil {
				return err
			}
		}
		for column, source := range vectors {
			if !source.GetType().IsVarlen() || source.IsNull(uint64(rows[column])) {
				continue
			}
			if err = addVectorAreaCapacity(
				&needs, &needCount, x, column,
				source.GetRawBytesAt(rows[column])); err != nil {
				return err
			}
		}
	}
	if err := exec.applyStringSourceEvents(&sourceEvents, sourceEventCount); err != nil {
		return err
	}
	if err := exec.applyPrepareParamKindEvents(&kindEvents, kindEventCount); err != nil {
		return err
	}
	return exec.applyVectorAreaCapacity(&needs, needCount)
}

func maxByCandidateWins(
	candidate []*vector.Vector,
	candidateRows [3]int,
	current []*vector.Vector,
	currentRows [3]int,
	typs []types.Type,
) bool {
	if cmp := compareVectorValue(
		candidate[1], candidateRows[1],
		current[1], currentRows[1], typs[1]); cmp != 0 {
		return cmp > 0
	}
	if cmp := compareNullableVectorValue(
		candidate[2], candidateRows[2],
		current[2], currentRows[2], typs[2]); cmp != 0 {
		return cmp > 0
	}
	return compareNullableRaw(
		candidate[0], candidateRows[0],
		current[0], currentRows[0]) > 0
}

func maxByCandidateEquals(
	candidate []*vector.Vector,
	candidateRows [3]int,
	current []*vector.Vector,
	currentRows [3]int,
	typs []types.Type,
) bool {
	return compareVectorValue(
		candidate[1], candidateRows[1],
		current[1], currentRows[1], typs[1]) == 0 &&
		compareNullableVectorValue(
			candidate[2], candidateRows[2],
			current[2], currentRows[2], typs[2]) == 0 &&
		compareNullableRaw(
			candidate[0], candidateRows[0],
			current[0], currentRows[0]) == 0
}

func (exec *maxByExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 3 {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	return exec.preflightCandidates(groups,
		func(row int) ([]*vector.Vector, [3]int, error) {
			rows := [3]int{offset + row, offset + row, offset + row}
			for column := range rows {
				if vectors[column].IsConst() {
					rows[column] = 0
				}
			}
			return vectors, rows, nil
		})
}

func (exec *maxByExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*maxByExec)
	if !ok || other == nil || exec == nil ||
		other.nonNullValue != exec.nonNullValue ||
		!typesEqual(other.argTypes, exec.argTypes) {
		return moerr.NewInternalErrorNoCtx("cannot merge incompatible max_by states")
	}
	if exec.allocation == nil {
		return nil
	}
	if offset < 0 || offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	return exec.preflightCandidates(groups,
		func(row int) ([]*vector.Vector, [3]int, error) {
			x, y := other.getXY(uint64(offset + row))
			if x < 0 || x >= len(other.state) ||
				int(y) >= int(other.state[x].length) {
				return nil, [3]int{}, mpool.ErrAllocationAccountInvariant
			}
			index := int(y)
			return other.state[x].vecs, [3]int{index, index, index}, nil
		})
}

func typesEqual(left, right []types.Type) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (exec *groupConcatExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(groups) > hashmap.UnitLimit || len(vectors) != len(exec.argTypes) {
		return mpool.ErrAllocationAccountInvariant
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y, state, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		logicalRow := offset + i
		payload := 0
		for column, vec := range vectors[:exec.concatArgCnt] {
			physicalRow, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			if vec.IsNull(uint64(physicalRow)) {
				payload = -1
				break
			}
			fieldSize := len(groupConcatFieldBytes(
				vec, physicalRow, exec.argTypes[column]))
			if fieldSize > math.MaxInt-payload-5 {
				return mpool.ErrAllocationAllocatorLimit
			}
			payload += 5 + fieldSize
		}
		if payload < 0 {
			continue
		}
		concatPayloadSize := payload
		if exec.orderArgCnt != 0 {
			if payload > math.MaxInt-4 {
				return mpool.ErrAllocationAllocatorLimit
			}
			payload += 4
			for _, index := range exec.orderArgIndexes {
				vec := vectors[index]
				physicalRow, err := preflightPhysicalRow(vec, logicalRow)
				if err != nil {
					return err
				}
				if vec.IsNull(uint64(physicalRow)) {
					if payload == math.MaxInt {
						return mpool.ErrAllocationAllocatorLimit
					}
					payload++
					continue
				}
				fieldSize := len(groupConcatFieldBytes(
					vec, physicalRow, exec.argTypes[index]))
				if fieldSize > math.MaxInt-payload-5 {
					return mpool.ErrAllocationAllocatorLimit
				}
				payload += 5 + fieldSize
			}
		}
		headerSize := kAggArgPrefixSz
		if !exec.aggInfo.isDistinct {
			headerSize += kAggArgOrdinalSz
		}
		keySize := headerSize + payload
		key, err := state.resizeArgScratch(exec.mp, keySize)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], y)
		if !exec.aggInfo.isDistinct {
			ordinal, err := nextArgumentOrdinal(
				&progress, &progressCount, x, y, state.argCnt[y])
			if err != nil {
				return err
			}
			binary.BigEndian.PutUint32(
				key[kAggArgPrefixSz:headerSize], ordinal)
		}
		keyOffset := headerSize
		if exec.orderArgCnt != 0 {
			binary.BigEndian.PutUint32(key[keyOffset:], uint32(concatPayloadSize))
			keyOffset += 4
		}
		for column, vec := range vectors[:exec.concatArgCnt] {
			physicalRow, err := preflightPhysicalRow(vec, logicalRow)
			if err != nil {
				return err
			}
			field := groupConcatFieldBytes(
				vec, physicalRow, exec.argTypes[column])
			key[keyOffset] = 1
			keyOffset++
			binary.NativeEndian.PutUint32(key[keyOffset:], uint32(len(field)))
			keyOffset += 4
			copy(key[keyOffset:], field)
			keyOffset += len(field)
		}
		if exec.orderArgCnt != 0 {
			for _, index := range exec.orderArgIndexes {
				vec := vectors[index]
				physicalRow, err := preflightPhysicalRow(vec, logicalRow)
				if err != nil {
					return err
				}
				if vec.IsNull(uint64(physicalRow)) {
					key[keyOffset] = 0
					keyOffset++
					continue
				}
				field := groupConcatFieldBytes(
					vec, physicalRow, exec.argTypes[index])
				key[keyOffset] = 1
				keyOffset++
				binary.NativeEndian.PutUint32(key[keyOffset:], uint32(len(field)))
				keyOffset += 4
				copy(key[keyOffset:], field)
				keyOffset += len(field)
			}
		}
		if exec.aggInfo.isDistinct {
			if state.argSkl.Contains(key) {
				continue
			}
			duplicate := false
			for earlier := 0; earlier < i; earlier++ {
				if groups[earlier] != group {
					continue
				}
				candidateRow := offset + earlier
				equal := true
				for column, vec := range vectors[:exec.concatArgCnt] {
					left, err := preflightPhysicalRow(vec, candidateRow)
					if err != nil {
						return err
					}
					right, err := preflightPhysicalRow(vec, logicalRow)
					if err != nil {
						return err
					}
					if vec.IsNull(uint64(left)) ||
						!bytes.Equal(groupConcatFieldBytes(
							vec, left, exec.argTypes[column]),
							groupConcatFieldBytes(vec, right, exec.argTypes[column])) {
						equal = false
						break
					}
				}
				if equal {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
		}
		valueSize := 0
		if exec.aggInfo.preserveDistinctInputOrder {
			valueSize = kAggArgOrdinalSz
		}
		if err := addArgumentChunkCapacityWithValue(
			&needs, &needCount, x, key, valueSize); err != nil {
			return err
		}
	}
	return exec.applyArgumentChunkCapacity(&needs, needCount)
}

func (exec *groupConcatExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*groupConcatExec)
	if !ok || other == nil || exec.distinct != other.distinct ||
		exec.orderArgCnt != other.orderArgCnt ||
		exec.concatArgCnt != other.concatArgCnt ||
		!slices.Equal(exec.orderArgIndexes, other.orderArgIndexes) ||
		!slices.Equal(exec.orderDesc, other.orderDesc) ||
		!slices.Equal(exec.orderNullsLast, other.orderNullsLast) ||
		!typesEqual(exec.argTypes, other.argTypes) {
		return moerr.NewInternalErrorNoCtx(
			"cannot preflight incompatible group_concat states")
	}
	return exec.aggExec.PreflightBatchMerge(next, offset, groups)
}

type bitmapPreflightTarget struct {
	chunk    int
	row      uint16
	required int
}

func (e *bmpExecCommon) bitmapTarget(
	group uint64,
) (int, uint16, *aggState, *bmp, error) {
	x, y, state, err := e.validatePreflightTarget(group)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	if state.mobs[y] == nil {
		return x, y, state, nil, nil
	}
	value, ok := state.mobs[y].(*bmp)
	if !ok || value == nil {
		return 0, 0, nil, nil, mpool.ErrAllocationAccountInvariant
	}
	return x, y, state, value, nil
}

func addBitmapPreflightTarget(
	targets *[hashmap.UnitLimit]bitmapPreflightTarget,
	count *int,
	chunk int,
	row uint16,
	additional int,
) error {
	if additional < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	for i := 0; i < *count; i++ {
		if targets[i].chunk == chunk && targets[i].row == row {
			if targets[i].required > math.MaxInt-additional {
				return mpool.ErrAllocationAllocatorLimit
			}
			targets[i].required += additional
			return nil
		}
	}
	if *count == len(targets) {
		return mpool.ErrAllocationAccountInvalid
	}
	targets[*count] = bitmapPreflightTarget{
		chunk: chunk, row: row, required: additional,
	}
	*count++
	return nil
}

func (e *bmpExecCommon) applyBitmapPreflight(
	targets *[hashmap.UnitLimit]bitmapPreflightTarget,
	count int,
) error {
	for i := 0; i < count; i++ {
		target := targets[i]
		state := e.preflightStateAt(target.chunk)
		if state == nil || int(target.row) >= int(state.capacity) {
			return mpool.ErrAllocationAccountInvariant
		}
		var value *bmp
		if state.mobs[target.row] == nil {
			var err error
			value, err = makeBmp(e.mp, e.allocation)
			if err != nil {
				return err
			}
			state.mobs[target.row] = value
		} else {
			var ok bool
			value, ok = state.mobs[target.row].(*bmp)
			if !ok || value.allocation == nil {
				return mpool.ErrAllocationAccountInvariant
			}
		}
		if target.required > math.MaxInt-len(value.values) {
			return mpool.ErrAllocationAllocatorLimit
		}
		if err := value.ensureCapacity(len(value.values) + target.required); err != nil {
			return err
		}
	}
	return nil
}

func (e *bmpConstructExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if e == nil || e.allocation == nil {
		return nil
	}
	if len(vectors) != 1 || len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var targets [hashmap.UnitLimit]bitmapPreflightTarget
	targetCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row, err := preflightPhysicalRow(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y, _, current, err := e.bitmapTarget(group)
		if err != nil {
			return err
		}
		value := uint32(vector.GetFixedAtNoTypeCheck[uint64](vectors[0], row))
		if current != nil {
			if _, found := slices.BinarySearch(current.values, value); found {
				continue
			}
		}
		duplicate := false
		for earlier := 0; earlier < i; earlier++ {
			if groups[earlier] != group {
				continue
			}
			earlierRow, err := preflightPhysicalRow(vectors[0], offset+earlier)
			if err != nil {
				return err
			}
			if !vectors[0].IsNull(uint64(earlierRow)) &&
				uint32(vector.GetFixedAtNoTypeCheck[uint64](vectors[0], earlierRow)) == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			if err := addBitmapPreflightTarget(
				&targets, &targetCount, x, y, 1); err != nil {
				return err
			}
		}
	}
	return e.applyBitmapPreflight(&targets, targetCount)
}

func (e *bmpConstructExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*bmpConstructExec)
	if !ok || other == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	return e.preflightBitmapMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpOrExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if e == nil || e.allocation == nil {
		return nil
	}
	if len(vectors) != 1 || len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var targets [hashmap.UnitLimit]bitmapPreflightTarget
	targetCount := 0
	// Publication decodes directly into the pre-reserved target and then
	// normalizes it in place. Reserve the input cardinality: overlap may make
	// this conservative, but publication performs no second allocation and the
	// work stays linear in the input wire size.
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row, err := preflightPhysicalRow(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		x, y, _, _, err := e.bitmapTarget(group)
		if err != nil {
			return err
		}
		cardinality, err := scanAccountedBitmapWire(
			vectors[0].GetBytesAt(row), nil)
		if err != nil {
			return err
		}
		if err := addBitmapPreflightTarget(
			&targets, &targetCount, x, y, cardinality); err != nil {
			return err
		}
	}
	return e.applyBitmapPreflight(&targets, targetCount)
}

func (e *bmpOrExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*bmpOrExec)
	if !ok || other == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	return e.preflightBitmapMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpExecCommon) preflightBitmapMerge(
	other *bmpExecCommon,
	offset int,
	groups []uint64,
) error {
	if e == nil || e.allocation == nil || other == nil ||
		offset < 0 || len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	var targets [hashmap.UnitLimit]bitmapPreflightTarget
	targetCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		sourceIndex := offset + i
		x2, y2 := other.getXY(uint64(sourceIndex))
		if x2 < 0 || x2 >= len(other.state) ||
			int(y2) >= int(other.state[x2].length) {
			return mpool.ErrAllocationAccountInvalid
		}
		if other.state[x2].mobs[y2] == nil {
			continue
		}
		source, ok := other.state[x2].mobs[y2].(*bmp)
		if !ok {
			return mpool.ErrAllocationAccountInvariant
		}
		x, y, _, current, err := e.bitmapTarget(group)
		if err != nil {
			return err
		}
		additional := 0
		if source.allocation != nil {
			for _, value := range source.values {
				if current == nil {
					additional++
				} else if _, found := slices.BinarySearch(current.values, value); !found {
					additional++
				}
			}
		} else {
			for iterator := source.legacy.Iterator(); iterator.HasNext(); {
				value := iterator.Next()
				if current == nil {
					additional++
				} else if _, found := slices.BinarySearch(current.values, value); !found {
					additional++
				}
			}
		}
		// Merge batches can map several immutable source groups to one target.
		// Counting source overlap conservatively reserves extra reusable capacity;
		// it never mutates values and cannot under-admit the committed merge.
		if err := addBitmapPreflightTarget(
			&targets, &targetCount, x, y, additional); err != nil {
			return err
		}
	}
	return e.applyBitmapPreflight(&targets, targetCount)
}

func accountedJSONValueSize(
	vec *vector.Vector,
	logicalRow int,
) (int, error) {
	row, err := preflightPhysicalRow(vec, logicalRow)
	if err != nil {
		return 0, err
	}
	return jsonAggregateValueSize(vec, uint64(row))
}

func addJSONArgumentCapacity(
	base *aggExec,
	needs *[hashmap.UnitLimit]argumentChunkCapacity,
	needCount *int,
	progress *[hashmap.UnitLimit]argumentTargetProgress,
	progressCount *int,
	group uint64,
	payloadSize int,
	build func([]byte) ([]byte, error),
) error {
	x, y, state, err := base.validatePreflightTarget(group)
	if err != nil {
		return err
	}
	ordinal, err := nextArgumentOrdinal(
		progress, progressCount, x, y, state.argCnt[y])
	if err != nil {
		return err
	}
	header := kAggArgPrefixSz + kAggArgOrdinalSz
	if payloadSize < 0 || payloadSize > math.MaxInt-header {
		return mpool.ErrAllocationAllocatorLimit
	}
	key, err := state.resizeArgScratch(base.mp, header+payloadSize)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], y)
	binary.BigEndian.PutUint32(key[kAggArgPrefixSz:header], ordinal)
	payload, err := build(key[header:header])
	if err != nil {
		return err
	}
	if len(payload) != payloadSize {
		return mpool.ErrAllocationAccountInvariant
	}
	return addArgumentChunkCapacity(
		needs, needCount, x, key[:header+payloadSize])
}

func (exec *jsonArrayAggExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 1 || len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		valueSize, err := accountedJSONValueSize(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if valueSize > math.MaxInt-5 {
			return mpool.ErrAllocationAllocatorLimit
		}
		err = addJSONArgumentCapacity(
			&exec.aggExec, &needs, &needCount, &progress, &progressCount,
			group, 5+valueSize, func(dst []byte) ([]byte, error) {
				dst = dst[:5]
				dst[0] = 1
				binary.NativeEndian.PutUint32(dst[1:], uint32(valueSize))
				row, err := preflightPhysicalRow(vectors[0], offset+i)
				if err != nil {
					return nil, err
				}
				return appendJSONAggregateValue(dst, vectors[0], uint64(row))
			})
		if err != nil {
			return err
		}
	}
	return exec.applyArgumentChunkCapacity(&needs, needCount)
}

func (exec *jsonArrayAggExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*jsonArrayAggExec)
	if !ok || other == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	return exec.aggExec.PreflightBatchMerge(other, offset, groups)
}

func (exec *jsonObjectAggExec) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec == nil || exec.allocation == nil {
		return nil
	}
	if len(vectors) != 2 || len(groups) > hashmap.UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		keyRow, err := preflightPhysicalRow(vectors[0], offset+i)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(keyRow)) {
			return moerr.NewInvalidInputNoCtx("json_objectagg key cannot be NULL")
		}
		key, err := getStringKey(vectors[0], uint64(keyRow))
		if err != nil {
			return err
		}
		valueSize, err := accountedJSONValueSize(vectors[1], offset+i)
		if err != nil {
			return err
		}
		if len(key) > math.MaxInt-valueSize-10 {
			return mpool.ErrAllocationAllocatorLimit
		}
		err = addJSONArgumentCapacity(
			&exec.aggExec, &needs, &needCount, &progress, &progressCount,
			group, 10+len(key)+valueSize, func(dst []byte) ([]byte, error) {
				dst = appendJSONPayloadField(dst, []byte(key))
				valueHeader := len(dst)
				dst = dst[:valueHeader+5]
				dst[valueHeader] = 1
				binary.NativeEndian.PutUint32(dst[valueHeader+1:], uint32(valueSize))
				row, err := preflightPhysicalRow(vectors[1], offset+i)
				if err != nil {
					return nil, err
				}
				return appendJSONAggregateValue(dst, vectors[1], uint64(row))
			})
		if err != nil {
			return err
		}
	}
	return exec.applyArgumentChunkCapacity(&needs, needCount)
}

func (exec *jsonObjectAggExec) PreflightBatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other, ok := next.(*jsonObjectAggExec)
	if !ok || other == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	return exec.aggExec.PreflightBatchMerge(other, offset, groups)
}
