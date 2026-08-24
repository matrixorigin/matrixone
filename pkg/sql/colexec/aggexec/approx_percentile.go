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
	"bytes"
	"io"
	"math"
	"math/big"
	"slices"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	approxPercentileSketchCapacity = 200
	approxPercentileMaxLevels      = 64
	approxPercentileSketchVersion  = byte(1)
	approxPercentileDecimalWidth   = int32(38)
)

func ApproxPercentileReturnType(args []types.Type) types.Type {
	if !args[0].IsDecimal() {
		return types.T_float64.ToType()
	}
	scale := args[0].Scale
	if args[0].Width < approxPercentileDecimalWidth {
		scale++
	}
	return types.New(types.T_decimal128, approxPercentileDecimalWidth, scale)
}

type quantileValue interface {
	numeric | types.Decimal64 | types.Decimal128
}

// quantileSketch is a bounded, mergeable KLL-style sketch. Each level stores
// values with weight 2^level and is compacted once it reaches 2*k entries.
// The fixed level limit bounds every group's retained state independently of
// its input row count. Level headers are Go-managed, while every value buffer
// is allocated from mp so group spill accounting sees the retained samples.
type quantileSketch[T quantileValue] struct {
	levels     [][]T
	parity     []bool
	levelCnt   uint8
	count      uint64
	min        T
	max        T
	hasValue   bool
	compare    func(T, T) int
	mp         *mpool.MPool
	allocation *AllocationAccount
}

func newQuantileSketch[T quantileValue](
	mp *mpool.MPool,
	compare func(T, T) int,
	allocation *AllocationAccount,
) *quantileSketch[T] {
	return &quantileSketch[T]{compare: compare, mp: mp, allocation: allocation}
}

func (s *quantileSketch[T]) Add(value T) error {
	if s.count == math.MaxUint64 {
		return moerr.NewInvalidInputNoCtx("approx_percentile: row count overflow")
	}
	if err := s.appendValue(0, value); err != nil {
		return err
	}
	if !s.hasValue {
		s.min, s.max, s.hasValue = value, value, true
	} else {
		if s.compare(value, s.min) < 0 {
			s.min = value
		}
		if s.compare(value, s.max) > 0 {
			s.max = value
		}
	}
	s.count++
	return s.compactFrom(0)
}

func (s *quantileSketch[T]) Merge(other *quantileSketch[T]) error {
	if other == nil || other.count == 0 {
		return nil
	}
	if math.MaxUint64-s.count < other.count {
		return moerr.NewInvalidInputNoCtx("approx_percentile: row count overflow")
	}
	if !s.hasValue {
		s.min, s.max, s.hasValue = other.min, other.max, other.hasValue
	} else if other.hasValue {
		if s.compare(other.min, s.min) < 0 {
			s.min = other.min
		}
		if s.compare(other.max, s.max) > 0 {
			s.max = other.max
		}
	}
	for level := 0; level < int(other.levelCnt); level++ {
		values := other.levels[level]
		s.ensureLevel(level)
		if err := s.appendValues(level, values); err != nil {
			return err
		}
		if other.parity[level] {
			s.parity[level] = !s.parity[level]
		}
	}
	if err := s.compactFrom(0); err != nil {
		return err
	}
	s.count += other.count
	return nil
}

func (s *quantileSketch[T]) ensureLevel(level int) {
	if level >= approxPercentileMaxLevels {
		panic(mpool.ErrAllocationAccountInvariant)
	}
	if int(s.levelCnt) <= level {
		for len(s.levels) <= level {
			s.levels = append(s.levels, nil)
			s.parity = append(s.parity, false)
		}
		s.levelCnt = uint8(level + 1)
	}
}

func (s *quantileSketch[T]) appendValue(level int, value T) error {
	s.ensureLevel(level)
	values := s.levels[level]
	if len(values) < cap(values) {
		values = values[:len(values)+1]
		values[len(values)-1] = value
		s.levels[level] = values
		return nil
	}
	return s.replaceLevel(level, len(values)+1, func(dst []T) {
		copy(dst, values)
		dst[len(values)] = value
	})
}

func (s *quantileSketch[T]) appendValues(level int, added []T) error {
	if len(added) == 0 {
		return nil
	}
	s.ensureLevel(level)
	values := s.levels[level]
	needed := len(values) + len(added)
	if needed <= cap(values) {
		values = values[:needed]
		copy(values[needed-len(added):], added)
		s.levels[level] = values
		return nil
	}
	return s.replaceLevel(level, needed, func(dst []T) {
		copy(dst, values)
		copy(dst[len(values):], added)
	})
}

func (s *quantileSketch[T]) replaceLevel(level, length int, fill func([]T)) error {
	old := s.levels[level]
	capacity := max(1, cap(old)*2)
	for capacity < length {
		capacity *= 2
	}
	values, err := makeAccountedScratch[T](s.allocation, s.mp, capacity)
	if err != nil {
		return err
	}
	values = values[:length]
	fill(values)
	s.freeLevel(old)
	s.levels[level] = values
	return nil
}

// reserveLevel grows physical capacity without changing any logical sketch
// value. A later work-unit capacity failure may leave this reusable capacity
// behind, but never a partially applied percentile sample.
func (s *quantileSketch[T]) reserveLevel(level, capacity int) error {
	s.ensureLevel(level)
	old := s.levels[level]
	if capacity <= cap(old) {
		return nil
	}
	values, err := makeAccountedScratch[T](s.allocation, s.mp, capacity)
	if err != nil {
		return err
	}
	values = values[:len(old)]
	copy(values, old)
	s.freeLevel(old)
	s.levels[level] = values
	return nil
}

func (s *quantileSketch[T]) preflightAdd(count int) error {
	if count < 0 || uint64(count) > math.MaxUint64-s.count {
		return moerr.NewInvalidInputNoCtx("approx_percentile: row count overflow")
	}
	var lengths, capacities [approxPercentileMaxLevels]int
	levelCount := int(s.levelCnt)
	for level := 0; level < levelCount; level++ {
		lengths[level] = len(s.levels[level])
		capacities[level] = cap(s.levels[level])
	}
	for range count {
		lengths[0]++
		if capacities[0] < lengths[0] {
			capacities[0] = grownQuantileCapacity(capacities[0], lengths[0])
		}
		if levelCount == 0 {
			levelCount = 1
		}
		for level := 0; level < levelCount && lengths[level] >= 2*approxPercentileSketchCapacity; level++ {
			if level+1 >= approxPercentileMaxLevels {
				return moerr.NewInvalidInputNoCtx("approx_percentile: sketch level overflow")
			}
			promoted := lengths[level] / 2
			lengths[level] &= 1
			if level+1 == levelCount {
				levelCount++
			}
			lengths[level+1] += promoted
			if capacities[level+1] < lengths[level+1] {
				capacities[level+1] = grownQuantileCapacity(
					capacities[level+1], lengths[level+1])
			}
		}
	}
	for level := 0; level < levelCount; level++ {
		if err := s.reserveLevel(level, capacities[level]); err != nil {
			return err
		}
	}
	return nil
}

type quantileCapacityPlan[T quantileValue] struct {
	lengths    [approxPercentileMaxLevels]int
	capacities [approxPercentileMaxLevels]int
	levelCount int
	count      uint64
}

func newQuantileCapacityPlan[T quantileValue](
	sketch *quantileSketch[T],
) quantileCapacityPlan[T] {
	plan := quantileCapacityPlan[T]{
		levelCount: int(sketch.levelCnt),
		count:      sketch.count,
	}
	for level := 0; level < plan.levelCount; level++ {
		plan.lengths[level] = len(sketch.levels[level])
		plan.capacities[level] = cap(sketch.levels[level])
	}
	return plan
}

func (plan *quantileCapacityPlan[T]) merge(other *quantileSketch[T]) error {
	if other == nil || other.count == 0 {
		return nil
	}
	if math.MaxUint64-plan.count < other.count {
		return moerr.NewInvalidInputNoCtx("approx_percentile: row count overflow")
	}
	plan.count += other.count
	plan.levelCount = max(plan.levelCount, int(other.levelCnt))
	for level := 0; level < plan.levelCount; level++ {
		if level < len(other.levels) {
			plan.lengths[level] += len(other.levels[level])
		}
		if plan.capacities[level] < plan.lengths[level] {
			plan.capacities[level] = grownQuantileCapacity(
				plan.capacities[level], plan.lengths[level])
		}
	}
	for level := 0; level < plan.levelCount; level++ {
		for plan.lengths[level] >= 2*approxPercentileSketchCapacity {
			if level+1 >= approxPercentileMaxLevels {
				return moerr.NewInvalidInputNoCtx("approx_percentile: sketch level overflow")
			}
			promoted := plan.lengths[level] / 2
			plan.lengths[level] &= 1
			if level+1 == plan.levelCount {
				plan.levelCount++
			}
			plan.lengths[level+1] += promoted
			if plan.capacities[level+1] < plan.lengths[level+1] {
				plan.capacities[level+1] = grownQuantileCapacity(
					plan.capacities[level+1], plan.lengths[level+1])
			}
		}
	}
	return nil
}

func (plan *quantileCapacityPlan[T]) reserve(sketch *quantileSketch[T]) error {
	for level := 0; level < plan.levelCount; level++ {
		if err := sketch.reserveLevel(level, plan.capacities[level]); err != nil {
			return err
		}
	}
	return nil
}

func grownQuantileCapacity(current, required int) int {
	capacity := max(1, current*2)
	for capacity < required {
		capacity *= 2
	}
	return capacity
}

func (s *quantileSketch[T]) freeLevel(values []T) {
	if cap(values) > 0 {
		mpool.FreeSlice(s.mp, values[:1])
	}
}

func (s *quantileSketch[T]) compactFrom(start int) error {
	for level := start; level < int(s.levelCnt); level++ {
		for len(s.levels[level]) >= 2*approxPercentileSketchCapacity {
			if level+1 >= approxPercentileMaxLevels {
				return moerr.NewInvalidInputNoCtx("approx_percentile: sketch level overflow")
			}
			values := s.levels[level]
			slices.SortFunc(values, s.compare)

			pickSecond := s.parity[level]
			startAt, endAt := 0, len(values)
			retained := 0
			var retainedValue T
			if len(values)&1 == 1 {
				if pickSecond {
					retainedValue = values[0]
					startAt = 1
				} else {
					endAt--
					retainedValue = values[endAt]
				}
				retained = 1
			}
			pick := 0
			if pickSecond {
				pick = 1
			}
			promoted := (endAt - startAt) / 2
			s.ensureLevel(level + 1)
			next := s.levels[level+1]
			needed := len(next) + promoted
			if needed <= cap(next) {
				next = next[:needed]
				for i, dst := startAt, needed-promoted; i < endAt; i, dst = i+2, dst+1 {
					next[dst] = values[i+pick]
				}
				s.levels[level+1] = next
			} else if err := s.replaceLevel(level+1, needed, func(dst []T) {
				copy(dst, next)
				for i, out := startAt, len(next); i < endAt; i, out = i+2, out+1 {
					dst[out] = values[i+pick]
				}
			}); err != nil {
				return err
			}
			if retained == 1 {
				values[0] = retainedValue
			}
			s.levels[level] = values[:retained]
			s.parity[level] = !s.parity[level]
		}
	}
	return nil
}

type weightedQuantileValue[T quantileValue] struct {
	value  T
	weight uint64
}

func (s *quantileSketch[T]) valueAtRank(rank uint64, sorted []weightedQuantileValue[T]) T {
	var cumulative uint64
	for _, item := range sorted {
		if rank < cumulative+item.weight {
			return item.value
		}
		cumulative += item.weight
	}
	return sorted[len(sorted)-1].value
}

func (s *quantileSketch[T]) QuantileAtRanks(loRank, hiRank uint64) (
	lo, hi T, err error,
) {
	if s.count == 0 {
		return lo, hi, moerr.NewInternalErrorNoCtx("approx_percentile: empty sketch")
	}
	if loRank > hiRank || hiRank >= s.count {
		return lo, hi, moerr.NewInternalErrorNoCtx(
			"approx_percentile: invalid quantile rank")
	}
	if hiRank == 0 {
		return s.min, s.min, nil
	}
	if loRank == s.count-1 {
		return s.max, s.max, nil
	}
	weighted, err := makeAccountedScratch[weightedQuantileValue[T]](
		s.allocation, s.mp, s.retained())
	if err != nil {
		return lo, hi, err
	}
	defer mpool.FreeSlice(s.mp, weighted)
	weighted = weighted[:0]
	for level := 0; level < int(s.levelCnt); level++ {
		values := s.levels[level]
		weight := uint64(1) << level
		for _, value := range values {
			weighted = append(weighted, weightedQuantileValue[T]{value: value, weight: weight})
		}
	}
	slices.SortFunc(weighted, func(left, right weightedQuantileValue[T]) int {
		return s.compare(left.value, right.value)
	})
	return s.valueAtRank(loRank, weighted), s.valueAtRank(hiRank, weighted), nil
}

func (s *quantileSketch[T]) retained() int {
	n := 0
	for level := 0; level < int(s.levelCnt); level++ {
		values := s.levels[level]
		n += len(values)
	}
	return n
}

func (s *quantileSketch[T]) Size() int64 {
	var zero T
	valueSize := len(types.EncodeFixed(zero))
	capacity := 0
	for level := 0; level < int(s.levelCnt); level++ {
		values := s.levels[level]
		capacity += cap(values)
	}
	return int64((capacity + 2) * valueSize)
}

func (s *quantileSketch[T]) Free() {
	for level := 0; level < int(s.levelCnt); level++ {
		values := s.levels[level]
		s.freeLevel(values)
		s.levels[level] = nil
	}
	s.levelCnt = 0
	s.levels = nil
	s.parity = nil
	s.count = 0
	s.hasValue = false
}

func (s *quantileSketch[T]) MarshaledSize() int {
	var zero T
	valueSize := len(types.EncodeFixed(zero))
	size := 1 + 8 + 1 + 2
	if s.hasValue {
		size += 2 * valueSize
	}
	for level := 0; level < int(s.levelCnt); level++ {
		size += 1 + 2 + len(s.levels[level])*valueSize
	}
	return size
}

func (s *quantileSketch[T]) MarshalTo(writer io.Writer) error {
	if s == nil || writer == nil || int(s.levelCnt) > approxPercentileMaxLevels {
		return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch")
	}
	write := func(data []byte) error {
		n, err := writer.Write(data)
		if err == nil && n != len(data) {
			return io.ErrShortWrite
		}
		return err
	}
	if err := write([]byte{approxPercentileSketchVersion}); err != nil {
		return err
	}
	if err := types.WriteUint64(writer, s.count); err != nil {
		return err
	}
	if s.hasValue {
		if err := write([]byte{1}); err != nil {
			return err
		}
		if err := write(types.EncodeFixed(s.min)); err != nil {
			return err
		}
		if err := write(types.EncodeFixed(s.max)); err != nil {
			return err
		}
	} else if err := write([]byte{0}); err != nil {
		return err
	}
	if err := types.WriteUint16(writer, uint16(s.levelCnt)); err != nil {
		return err
	}
	for level := 0; level < int(s.levelCnt); level++ {
		values := s.levels[level]
		if len(values) >= 2*approxPercentileSketchCapacity {
			return moerr.NewInternalErrorNoCtx("approx_percentile: uncompacted sketch")
		}
		parity := byte(0)
		if s.parity[level] {
			parity = 1
		}
		if err := write([]byte{parity}); err != nil {
			return err
		}
		if err := types.WriteUint16(writer, uint16(len(values))); err != nil {
			return err
		}
		for _, value := range values {
			if err := write(types.EncodeFixed(value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *quantileSketch[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(s.MarshaledSize())
	if err := s.MarshalTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *quantileSketch[T]) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	restored, err := s.decode(reader)
	if err != nil {
		return err
	}
	if reader.Len() != 0 {
		restored.Free()
		return moerr.NewInvalidInputNoCtx("approx_percentile: inconsistent sketch state")
	}
	s.restore(restored)
	return nil
}

func (s *quantileSketch[T]) decode(reader io.Reader) (_ *quantileSketch[T], retErr error) {
	restored := newQuantileSketch(s.mp, s.compare, s.allocation)
	defer func() {
		if retErr != nil {
			restored.Free()
		}
	}()
	readByte := func() (byte, error) {
		var encoded [1]byte
		_, err := io.ReadFull(reader, encoded[:])
		return encoded[0], err
	}
	version, err := readByte()
	if err != nil {
		return nil, err
	}
	if version != approxPercentileSketchVersion {
		return nil, moerr.NewInvalidInputNoCtxf("approx_percentile: unsupported sketch version %d", version)
	}
	count, err := types.ReadUint64(reader)
	if err != nil {
		return nil, err
	}
	var zero T
	valueSize := len(types.EncodeFixed(zero))
	hasValue, err := readByte()
	if err != nil || hasValue > 1 {
		return nil, moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch extrema flag")
	}
	var encodedStorage [types.Decimal128Size]byte
	if valueSize > len(encodedStorage) {
		return nil, moerr.NewInternalErrorNoCtx(
			"approx_percentile: unsupported encoded value size")
	}
	encoded := encodedStorage[:valueSize]
	if hasValue == 1 {
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return nil, err
		}
		restored.min = types.DecodeFixed[T](encoded)
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return nil, err
		}
		restored.max = types.DecodeFixed[T](encoded)
		restored.hasValue = true
	}
	levelCount, err := types.ReadUint16(reader)
	if err != nil {
		return nil, err
	}
	if levelCount > approxPercentileMaxLevels {
		return nil, moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch level count")
	}
	if levelCount > 0 {
		restored.ensureLevel(int(levelCount) - 1)
	}
	var represented uint64
	for level := range int(levelCount) {
		parity, err := readByte()
		if err != nil || parity > 1 {
			return nil, moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch parity")
		}
		restored.parity[level] = parity == 1
		length, err := types.ReadUint16(reader)
		if err != nil {
			return nil, err
		}
		if length >= 2*approxPercentileSketchCapacity {
			return nil, moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch level size")
		}
		if length > 0 {
			values, err := makeAccountedScratch[T](restored.allocation, restored.mp, int(length))
			if err != nil {
				return nil, err
			}
			restored.levels[level] = values
			for i := range values {
				if _, err := io.ReadFull(reader, encoded); err != nil {
					return nil, err
				}
				values[i] = types.DecodeFixed[T](encoded)
			}
		}
		weight := uint64(1) << level
		if uint64(length) > math.MaxUint64/weight || math.MaxUint64-represented < uint64(length)*weight {
			return nil, moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch weight")
		}
		represented += uint64(length) * weight
	}
	if represented != count || restored.hasValue != (count > 0) ||
		restored.hasValue && restored.compare(restored.min, restored.max) > 0 {
		return nil, moerr.NewInvalidInputNoCtx("approx_percentile: inconsistent sketch state")
	}
	restored.count = count
	return restored, nil
}

func (s *quantileSketch[T]) UnmarshalFromReader(reader io.Reader) error {
	restored, err := s.decode(reader)
	if err != nil {
		return err
	}
	s.restore(restored)
	return nil
}

func (s *quantileSketch[T]) restore(restored *quantileSketch[T]) {
	s.Free()
	*s = *restored
	restored.levelCnt = 0
	restored.levels = nil
	restored.parity = nil
}

type percentileFraction struct {
	numerator   *big.Int
	denominator *big.Int
}

func (fraction percentileFraction) sign() int {
	if fraction.numerator == nil {
		return 0
	}
	return fraction.numerator.Sign()
}

// percentileArithmeticScratch owns the fixed number of big-number work
// buffers needed by percentile rank and interpolation arithmetic. One scratch
// is reused for a complete executor Flush, so group cardinality cannot create
// a proportional Go-heap allocation stream. The buffers grow only to the
// immutable percentile precision plus the fixed 128-bit value/count domain.
type percentileArithmeticScratch struct {
	one       big.Int
	ten       big.Int
	pow2_127  big.Int
	pow2_128  big.Int
	rank      big.Int
	quotient  big.Int
	remainder big.Int

	left       big.Int
	right      big.Int
	difference big.Int
	numerator  big.Int
	product    big.Int
	result     big.Int
	resultRem  big.Int
	comparison big.Int
	conversion big.Int
	floatRem   big.Int
}

func (scratch *percentileArithmeticScratch) constants() {
	scratch.one.SetUint64(1)
	scratch.ten.SetUint64(10)
	if scratch.pow2_128.Sign() == 0 {
		scratch.pow2_127.Lsh(&scratch.one, 127)
		scratch.pow2_128.Lsh(&scratch.one, 128)
	}
}

func (scratch *percentileArithmeticScratch) ranks(
	count uint64,
	p *big.Rat,
) (lo, hi uint64, fraction percentileFraction) {
	fraction = percentileFraction{
		numerator: &scratch.remainder, denominator: p.Denom(),
	}
	if count <= 1 {
		scratch.remainder.SetUint64(0)
		return 0, 0, fraction
	}
	scratch.rank.SetUint64(count - 1)
	scratch.rank.Mul(&scratch.rank, p.Num())
	scratch.quotient.QuoRem(&scratch.rank, p.Denom(), &scratch.remainder)
	lo = scratch.quotient.Uint64()
	hi = lo
	if lo < count-1 {
		hi++
	}
	return lo, hi, fraction
}

func (scratch *percentileArithmeticScratch) discreteRank(
	count uint64,
	p *big.Rat,
) uint64 {
	if count == 0 {
		return 0
	}
	scratch.constants()
	scratch.rank.SetUint64(count)
	scratch.rank.Mul(&scratch.rank, p.Num())
	scratch.quotient.QuoRem(&scratch.rank, p.Denom(), &scratch.remainder)
	if scratch.remainder.Sign() != 0 {
		scratch.quotient.Add(&scratch.quotient, &scratch.one)
	}
	if scratch.quotient.Sign() == 0 {
		return 0
	}
	scratch.quotient.Sub(&scratch.quotient, &scratch.one)
	return scratch.quotient.Uint64()
}

func (scratch *percentileArithmeticScratch) fractionFloat64(
	fraction percentileFraction,
) float64 {
	if fraction.sign() == 0 {
		return 0
	}
	return rationalToFloat64(
		fraction.numerator, fraction.denominator,
		&scratch.quotient, &scratch.comparison, &scratch.floatRem)
}

func numericIntegerToScratch[T numeric](dst *big.Int, value T) bool {
	switch value := any(value).(type) {
	case int8:
		dst.SetInt64(int64(value))
	case int16:
		dst.SetInt64(int64(value))
	case int32:
		dst.SetInt64(int64(value))
	case int64:
		dst.SetInt64(value)
	case uint8:
		dst.SetUint64(uint64(value))
	case uint16:
		dst.SetUint64(uint64(value))
	case uint32:
		dst.SetUint64(uint64(value))
	case uint64:
		dst.SetUint64(value)
	default:
		return false
	}
	return true
}

func interpolateNumericWithScratch[T numeric](
	scratch *percentileArithmeticScratch,
	lo, hi T,
	fraction percentileFraction,
) float64 {
	if !numericIntegerToScratch(&scratch.left, lo) {
		return interpolateFloat64(
			float64(lo), float64(hi), scratch.fractionFloat64(fraction))
	}
	if fraction.sign() == 0 {
		return float64(lo)
	}
	numericIntegerToScratch(&scratch.right, hi)
	scratch.numerator.Mul(&scratch.left, fraction.denominator)
	scratch.difference.Sub(&scratch.right, &scratch.left)
	scratch.product.Mul(&scratch.difference, fraction.numerator)
	scratch.numerator.Add(&scratch.numerator, &scratch.product)
	return rationalToFloat64(
		&scratch.numerator, fraction.denominator,
		&scratch.quotient, &scratch.comparison, &scratch.floatRem)
}

func rationalToFloat64(
	numerator, denominator *big.Int,
	quotient, scaledDenominator, remainder *big.Int,
) float64 {
	if numerator.Sign() == 0 {
		return 0
	}
	sign := 1.0
	if numerator.Sign() < 0 {
		sign = -1
	}
	numeratorBits := numerator.BitLen()
	denominatorBits := denominator.BitLen()
	exponent := numeratorBits - denominatorBits
	if exponent >= 0 {
		scaledDenominator.Lsh(denominator, uint(exponent))
		if numerator.CmpAbs(scaledDenominator) < 0 {
			exponent--
		}
	} else {
		quotient.Lsh(numerator, uint(-exponent))
		if quotient.CmpAbs(denominator) < 0 {
			exponent--
		}
	}
	// Percentile arithmetic is bounded to roughly 256 bits and produces
	// ordinary SQL numeric results, so this scale remains small. Round directly
	// to a 53-bit binary64 significand. Keeping one extra bit here would make
	// the subsequent uint64-to-float64 conversion round a second time and can
	// move exact halfway cases by one ULP.
	shift := 52 - exponent
	if shift >= 0 {
		quotient.Lsh(numerator, uint(shift))
		scaledDenominator.Set(denominator)
	} else {
		quotient.Set(numerator)
		scaledDenominator.Lsh(denominator, uint(-shift))
	}
	quotient.Abs(quotient)
	quotient.QuoRem(quotient, scaledDenominator, remainder)
	doubledRemainder := remainder.Lsh(remainder, 1)
	comparison := doubledRemainder.Cmp(scaledDenominator)
	if comparison > 0 || comparison == 0 && quotient.Bit(0) != 0 {
		remainder.SetUint64(1)
		quotient.Add(quotient, remainder)
	}
	if quotient.BitLen() > 53 {
		quotient.Rsh(quotient, 1)
		exponent++
	}
	return sign * math.Ldexp(float64(quotient.Uint64()), exponent-52)
}

func (scratch *percentileArithmeticScratch) decimal128ToInt(
	dst *big.Int,
	value types.Decimal128,
) {
	dst.SetUint64(value.B64_127)
	dst.Lsh(dst, 64)
	scratch.conversion.SetUint64(value.B0_63)
	dst.Or(dst, &scratch.conversion)
	if value.Sign() {
		dst.Sub(dst, &scratch.pow2_128)
	}
}

func (scratch *percentileArithmeticScratch) decimal128FromInt(
	value *big.Int,
) (types.Decimal128, error) {
	scratch.comparison.Neg(&scratch.pow2_127)
	if value.Cmp(&scratch.comparison) < 0 {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtx(
			"approx_percentile: decimal interpolation overflow")
	}
	scratch.comparison.Sub(&scratch.pow2_127, &scratch.one)
	if value.Cmp(&scratch.comparison) > 0 {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtx(
			"approx_percentile: decimal interpolation overflow")
	}
	scratch.conversion.Set(value)
	if scratch.conversion.Sign() < 0 {
		scratch.conversion.Add(&scratch.conversion, &scratch.pow2_128)
	}
	low := scratch.conversion.Uint64()
	scratch.conversion.Rsh(&scratch.conversion, 64)
	return types.Decimal128{
		B0_63: low, B64_127: scratch.conversion.Uint64(),
	}, nil
}

// interpolateDecimal converts from the input scale to the declared result
// scale. All arithmetic stays integral/rational, so values above 2^53 do not
// pass through float64.
func (scratch *percentileArithmeticScratch) interpolateDecimal(
	lo, hi types.Decimal128,
	fraction percentileFraction,
	scaleDelta int32,
) (types.Decimal128, error) {
	if scaleDelta < 0 || scaleDelta > 1 {
		return types.Decimal128{}, moerr.NewInternalErrorNoCtx(
			"approx_percentile: invalid decimal result scale")
	}
	scratch.constants()
	scratch.decimal128ToInt(&scratch.left, lo)
	scratch.decimal128ToInt(&scratch.right, hi)
	scratch.difference.Sub(&scratch.right, &scratch.left)
	scratch.numerator.Mul(&scratch.left, fraction.denominator)
	scratch.product.Mul(&scratch.difference, fraction.numerator)
	scratch.numerator.Add(&scratch.numerator, &scratch.product)
	if scaleDelta == 1 {
		scratch.numerator.Mul(&scratch.numerator, &scratch.ten)
	}
	scratch.result.QuoRem(
		&scratch.numerator, fraction.denominator, &scratch.resultRem)
	scratch.comparison.Abs(&scratch.resultRem)
	scratch.comparison.Lsh(&scratch.comparison, 1)
	if scratch.comparison.Cmp(fraction.denominator) >= 0 {
		if scratch.numerator.Sign() < 0 {
			scratch.result.Sub(&scratch.result, &scratch.one)
		} else {
			scratch.result.Add(&scratch.result, &scratch.one)
		}
	}
	return scratch.decimal128FromInt(&scratch.result)
}

func percentileRanks(count uint64, p *big.Rat) (lo, hi uint64, frac *big.Rat) {
	var scratch percentileArithmeticScratch
	lo, hi, fraction := scratch.ranks(count, p)
	return lo, hi, new(big.Rat).SetFrac(
		new(big.Int).Set(fraction.numerator),
		new(big.Int).Set(fraction.denominator))
}

func parsePercentileConfig(partialResult any) (*big.Rat, float64, error) {
	b, ok := partialResult.([]byte)
	if !ok {
		return nil, 0, moerr.NewInternalErrorNoCtx("approx_percentile: expected []byte config")
	}
	text := string(b)
	p, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, 0, err
	}
	if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
		return nil, 0, moerr.NewInvalidInputNoCtxf(
			"approx_percentile: percentile must be in [0,1] and finite, got %v", p)
	}
	rat, ok := new(big.Rat).SetString(text)
	if !ok || rat.Sign() < 0 || rat.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, 0, moerr.NewInvalidInputNoCtxf("approx_percentile: invalid percentile %q", text)
	}
	return rat, p, nil
}

func orderedCompare[T numeric](a, b T) int {
	af, bf := float64(a), float64(b)
	if math.IsNaN(af) {
		if math.IsNaN(bf) {
			return 0
		}
		return -1
	}
	if math.IsNaN(bf) {
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type approxPercentileExecBase[T quantileValue] struct {
	aggExec
	percentile      *big.Rat
	percentileFloat float64
	compare         func(T, T) int
	arithmetic      percentileArithmeticScratch
}

func (exec *approxPercentileExecBase[T]) PreflightBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if exec.allocation == nil {
		return nil
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var targetGroups [hashmap.UnitLimit]uint64
	var counts [hashmap.UnitLimit]int
	targetCount := 0
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
		_, _, _, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		found := -1
		for index := 0; index < targetCount; index++ {
			if targetGroups[index] == group {
				found = index
				break
			}
		}
		if found < 0 {
			found = targetCount
			targetGroups[targetCount] = group
			targetCount++
		}
		counts[found]++
	}
	for index := 0; index < targetCount; index++ {
		sketch, err := exec.preflightSketch(targetGroups[index] - 1)
		if err != nil {
			return err
		}
		if err = sketch.preflightAdd(counts[index]); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxPercentileExecBase[T]) preflightBatchMerge(
	other *approxPercentileExecBase[T],
	offset int,
	groups []uint64,
) error {
	if exec.allocation == nil {
		return nil
	}
	if other == nil || !exec.mergeCompatible(other) ||
		len(groups) > hashmap.UnitLimit || offset < 0 ||
		offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	if exec.percentile != nil && other.percentile != nil &&
		exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx(
			"approx_percentile: cannot merge different percentile configurations")
	}
	type mergeNeed struct {
		target *quantileSketch[T]
		plan   quantileCapacityPlan[T]
	}
	var needs [hashmap.UnitLimit]mergeNeed
	needCount := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		sx, sy := other.getXY(uint64(offset + i))
		if sx >= len(other.state) || int(sy) >= len(other.state[sx].mobs) {
			return mpool.ErrAllocationAccountInvariant
		}
		source, _ := other.state[sx].mobs[sy].(*quantileSketch[T])
		if source == nil || source.count == 0 {
			continue
		}
		_, _, _, err := exec.validatePreflightTarget(group)
		if err != nil {
			return err
		}
		found := -1
		for index := 0; index < needCount; index++ {
			x, y := exec.getXY(group - 1)
			if exec.preflightStateAt(x) != nil &&
				exec.preflightStateAt(x).mobs[y] == needs[index].target {
				found = index
				break
			}
		}
		if found < 0 {
			target, sketchErr := exec.preflightSketch(group - 1)
			if sketchErr != nil {
				return sketchErr
			}
			found = needCount
			needs[needCount].target = target
			needs[needCount].plan = newQuantileCapacityPlan(target)
			needCount++
		}
		if err = needs[found].plan.merge(source); err != nil {
			return err
		}
	}
	for index := 0; index < needCount; index++ {
		if err := needs[index].plan.reserve(needs[index].target); err != nil {
			return err
		}
	}
	return nil
}

func newApproxPercentileExecBase[T quantileValue](mp *mpool.MPool, info singleAggInfo, compare func(T, T) int) approxPercentileExecBase[T] {
	exec := approxPercentileExecBase[T]{compare: compare}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      info.aggID,
		isDistinct: info.distinct,
		argTypes:   []types.Type{info.argType},
		retType:    info.retType,
		emptyNull:  true,
		saveArg:    false,
		makeMarshalerUnmarshaler: func(mp *mpool.MPool, allocation *AllocationAccount) (MarshalerUnmarshaler, error) {
			return newQuantileSketch(mp, compare, allocation), nil
		},
		boundedOpaqueState: true,
		stableEmptyOpaqueState: func(writer io.Writer) error {
			empty := newQuantileSketch[T](nil, compare, nil)
			if err := types.WriteInt32(writer, int32(empty.MarshaledSize())); err != nil {
				return err
			}
			return empty.MarshalTo(writer)
		},
	}
	return exec
}

func (exec *approxPercentileExecBase[T]) ensureSketch(group uint64) (*quantileSketch[T], error) {
	x, y := exec.getXY(group)
	if exec.state[x].mobs[y] == nil {
		mob, err := exec.makeMarshalerUnmarshaler(exec.mp, exec.allocation)
		if err != nil {
			return nil, err
		}
		exec.state[x].mobs[y] = mob
	}
	return exec.state[x].mobs[y].(*quantileSketch[T]), nil
}

func (exec *approxPercentileExecBase[T]) preflightSketch(
	group uint64,
) (*quantileSketch[T], error) {
	x, y := exec.getXY(group)
	state := exec.preflightStateAt(x)
	if state == nil || int(y) >= len(state.mobs) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	if state.mobs[y] == nil {
		mob, err := exec.makeMarshalerUnmarshaler(exec.mp, exec.allocation)
		if err != nil {
			return nil, err
		}
		state.mobs[y] = mob
	}
	sketch, ok := state.mobs[y].(*quantileSketch[T])
	if !ok {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	return sketch, nil
}

func (exec *approxPercentileExecBase[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	sketch, err := exec.ensureSketch(uint64(groupIndex))
	if err != nil {
		return err
	}
	return sketch.Add(vector.MustFixedColWithTypeCheck[T](vectors[0])[row])
}

func (exec *approxPercentileExecBase[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	sketch, err := exec.ensureSketch(uint64(groupIndex))
	if err != nil {
		return err
	}
	values := vector.MustFixedColWithTypeCheck[T](vectors[0])
	if vectors[0].IsConst() {
		for range vectors[0].Length() {
			if err := sketch.Add(values[0]); err != nil {
				return err
			}
		}
		return nil
	}
	for row, value := range values {
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		if err := sketch.Add(value); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxPercentileExecBase[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	values := vector.MustFixedColWithTypeCheck[T](vectors[0])
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
		sketch, err := exec.ensureSketch(group - 1)
		if err != nil {
			return err
		}
		if err := sketch.Add(values[row]); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxPercentileExecBase[T]) merge(other *approxPercentileExecBase[T], groupIdx1, groupIdx2 int) error {
	if !exec.mergeCompatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	if exec.percentile != nil && other.percentile != nil && exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx("approx_percentile: cannot merge different percentile configurations")
	}
	x2, y2 := other.getXY(uint64(groupIdx2))
	if other.state[x2].mobs[y2] == nil {
		return nil
	}
	target, err := exec.ensureSketch(uint64(groupIdx1))
	if err != nil {
		return err
	}
	return target.Merge(other.state[x2].mobs[y2].(*quantileSketch[T]))
}

func (exec *approxPercentileExecBase[T]) mergeCompatible(
	other *approxPercentileExecBase[T],
) bool {
	return exec != nil && other != nil &&
		exec.aggId == other.aggId &&
		exec.isDistinct == other.isDistinct &&
		len(exec.argTypes) == 1 && len(other.argTypes) == 1 &&
		exec.argTypes[0].Eq(other.argTypes[0]) &&
		exec.retType.Eq(other.retType)
}

func (exec *approxPercentileExecBase[T]) batchMerge(other *approxPercentileExecBase[T], offset int, groups []uint64) error {
	if !exec.mergeCompatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.merge(other, int(group-1), offset+i); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxPercentileExecBase[T]) SetExtraInformation(partialResult any, groupIndex int) error {
	percentile, percentileFloat, err := parsePercentileConfig(partialResult)
	if err != nil {
		return err
	}
	exec.percentile = percentile
	exec.percentileFloat = percentileFloat
	return nil
}

func (exec *approxPercentileExecBase[T]) Size() int64 {
	var size int64
	for _, state := range exec.state {
		size += int64(cap(state.mobs)) * 8
		for _, mob := range state.mobs {
			if mob != nil {
				size += mob.(*quantileSketch[T]).Size()
			}
		}
	}
	return size
}

func (exec *approxPercentileExecBase[T]) Free() {
	exec.aggExec.Free()
	exec.state = nil
}

type approxPercentileNumericExec[T numeric] struct {
	approxPercentileExecBase[T]
}

func (exec *approxPercentileNumericExec[T]) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*approxPercentileNumericExec[T])
	if !ok {
		return mpool.ErrAllocationAccountInvalid
	}
	return exec.preflightBatchMerge(
		&other.approxPercentileExecBase, offset, groups)
}

func (exec *approxPercentileNumericExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.merge(&next.(*approxPercentileNumericExec[T]).approxPercentileExecBase, groupIdx1, groupIdx2)
}

func (exec *approxPercentileNumericExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	return exec.batchMerge(&next.(*approxPercentileNumericExec[T]).approxPercentileExecBase, offset, groups)
}

func (exec *approxPercentileNumericExec[T]) Flush() (_ []*vector.Vector, retErr error) {
	if exec.percentile == nil {
		return nil, moerr.NewInternalErrorNoCtx("approx_percentile: percentile configuration is not set")
	}
	results := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, result := range results {
				if result != nil {
					result.Free(exec.mp)
				}
			}
		}
	}()
	for x, state := range exec.state {
		result, err := exec.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[x] = result
		if err := result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		if err := result.PreExtendNulls(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[float64](result)
		for y := 0; y < int(state.length); y++ {
			if state.mobs[y] == nil || state.mobs[y].(*quantileSketch[T]).count == 0 {
				result.SetNull(uint64(y))
				continue
			}
			loRank, hiRank, fraction := exec.arithmetic.ranks(
				state.mobs[y].(*quantileSketch[T]).count, exec.percentile)
			lo, hi, err := state.mobs[y].(*quantileSketch[T]).QuantileAtRanks(
				loRank, hiRank)
			if err != nil {
				return nil, err
			}
			values[y] = interpolateNumericWithScratch(
				&exec.arithmetic, lo, hi, fraction)
		}
	}
	return results, nil
}

type approxPercentileDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	approxPercentileExecBase[T]
}

func (exec *approxPercentileDecimalExec[T]) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*approxPercentileDecimalExec[T])
	if !ok {
		return mpool.ErrAllocationAccountInvalid
	}
	return exec.preflightBatchMerge(
		&other.approxPercentileExecBase, offset, groups)
}

func (exec *approxPercentileDecimalExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.merge(&next.(*approxPercentileDecimalExec[T]).approxPercentileExecBase, groupIdx1, groupIdx2)
}

func (exec *approxPercentileDecimalExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	return exec.batchMerge(&next.(*approxPercentileDecimalExec[T]).approxPercentileExecBase, offset, groups)
}

func (exec *approxPercentileDecimalExec[T]) Flush() (_ []*vector.Vector, retErr error) {
	if exec.percentile == nil {
		return nil, moerr.NewInternalErrorNoCtx("approx_percentile: percentile configuration is not set")
	}
	results := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, result := range results {
				if result != nil {
					result.Free(exec.mp)
				}
			}
		}
	}()
	for x, state := range exec.state {
		result, err := exec.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[x] = result
		if err := result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		if err := result.PreExtendNulls(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[types.Decimal128](result)
		for y := 0; y < int(state.length); y++ {
			if state.mobs[y] == nil || state.mobs[y].(*quantileSketch[T]).count == 0 {
				result.SetNull(uint64(y))
				continue
			}
			loRank, hiRank, fraction := exec.arithmetic.ranks(
				state.mobs[y].(*quantileSketch[T]).count, exec.percentile)
			lo, hi, err := state.mobs[y].(*quantileSketch[T]).QuantileAtRanks(
				loRank, hiRank)
			if err != nil {
				return nil, err
			}
			values[y], err = exec.arithmetic.interpolateDecimal(
				toDecimal128(lo), toDecimal128(hi), fraction,
				exec.retType.Scale-exec.argTypes[0].Scale,
			)
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func toDecimal128[T types.Decimal64 | types.Decimal128](value T) types.Decimal128 {
	switch value := any(value).(type) {
	case types.Decimal64:
		return FromD64ToD128(value)
	case types.Decimal128:
		return value
	default:
		panic("unreachable")
	}
}

func interpolateDecimal(lo, hi types.Decimal128, frac *big.Rat, scaleDelta int32) (types.Decimal128, error) {
	var scratch percentileArithmeticScratch
	return scratch.interpolateDecimal(
		lo, hi,
		percentileFraction{
			numerator: frac.Num(), denominator: frac.Denom(),
		},
		scaleDelta)
}

func newApproxPercentileExec(mp *mpool.MPool, info singleAggInfo) (AggFuncExec, error) {
	if info.distinct {
		return nil, moerr.NewNotSupportedNoCtx("approx_percentile in distinct mode")
	}
	switch info.argType.Oid {
	case types.T_bit:
		return &approxPercentileNumericExec[uint64]{newApproxPercentileExecBase[uint64](mp, info, orderedCompare[uint64])}, nil
	case types.T_int8:
		return &approxPercentileNumericExec[int8]{newApproxPercentileExecBase[int8](mp, info, orderedCompare[int8])}, nil
	case types.T_int16:
		return &approxPercentileNumericExec[int16]{newApproxPercentileExecBase[int16](mp, info, orderedCompare[int16])}, nil
	case types.T_int32:
		return &approxPercentileNumericExec[int32]{newApproxPercentileExecBase[int32](mp, info, orderedCompare[int32])}, nil
	case types.T_int64:
		return &approxPercentileNumericExec[int64]{newApproxPercentileExecBase[int64](mp, info, orderedCompare[int64])}, nil
	case types.T_uint8:
		return &approxPercentileNumericExec[uint8]{newApproxPercentileExecBase[uint8](mp, info, orderedCompare[uint8])}, nil
	case types.T_uint16:
		return &approxPercentileNumericExec[uint16]{newApproxPercentileExecBase[uint16](mp, info, orderedCompare[uint16])}, nil
	case types.T_uint32:
		return &approxPercentileNumericExec[uint32]{newApproxPercentileExecBase[uint32](mp, info, orderedCompare[uint32])}, nil
	case types.T_uint64:
		return &approxPercentileNumericExec[uint64]{newApproxPercentileExecBase[uint64](mp, info, orderedCompare[uint64])}, nil
	case types.T_float32:
		return &approxPercentileNumericExec[float32]{newApproxPercentileExecBase[float32](mp, info, orderedCompare[float32])}, nil
	case types.T_float64:
		return &approxPercentileNumericExec[float64]{newApproxPercentileExecBase[float64](mp, info, orderedCompare[float64])}, nil
	case types.T_decimal64:
		compare := func(a, b types.Decimal64) int { return a.Compare(b) }
		return &approxPercentileDecimalExec[types.Decimal64]{newApproxPercentileExecBase[types.Decimal64](mp, info, compare)}, nil
	case types.T_decimal128:
		compare := func(a, b types.Decimal128) int { return a.Compare(b) }
		return &approxPercentileDecimalExec[types.Decimal128]{newApproxPercentileExecBase[types.Decimal128](mp, info, compare)}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for approx_percentile()")
	}
}

// Exact helpers retained for direct callers and small-data regression tests.
func PercentileNumeric[T numeric](vs *Vectors[T], p float64) (float64, error) {
	return percentileNumericVals(collectMedianValues(vs), p), nil
}

func interpolateFloat64(lo, hi, fraction float64) float64 {
	if fraction == 0 {
		return lo
	}
	if fraction == 1 {
		return hi
	}
	if lo == hi {
		return lo
	}
	// Preserve IEEE-754 propagation when distinct non-finite values actually
	// need interpolation.
	if math.IsNaN(lo) || math.IsNaN(hi) || math.IsInf(lo, 0) || math.IsInf(hi, 0) {
		return lo + (hi-lo)*fraction
	}
	// For opposite signs, hi-lo can overflow even though the interpolated value
	// is finite. Both weighted terms are bounded by their finite endpoints, and
	// their opposite signs keep the final addition from overflowing.
	if math.Signbit(lo) != math.Signbit(hi) {
		return math.FMA(lo, 1-fraction, hi*fraction)
	}
	return lo + (hi-lo)*fraction
}

func interpolateNumeric[T numeric](lo, hi T, fraction *big.Rat) float64 {
	var scratch percentileArithmeticScratch
	return interpolateNumericWithScratch(&scratch, lo, hi, percentileFraction{
		numerator: fraction.Num(), denominator: fraction.Denom(),
	})
}

func percentileNumericVals[T numeric](values []T, p float64) float64 {
	if len(values) == 0 || p < 0 || p > 1 {
		return math.NaN()
	}
	rat, _, err := parsePercentileConfig([]byte(strconv.FormatFloat(p, 'g', -1, 64)))
	if err != nil {
		return math.NaN()
	}
	loRank, hiRank, frac := percentileRanks(uint64(len(values)), rat)
	lo := selectKthNumeric(values, int(loRank))
	hi := selectKthNumeric(values, int(hiRank))
	return interpolateNumeric(lo, hi, frac)
}

func PercentileDecimal64(vs *Vectors[types.Decimal64], p float64, argScale int32) (types.Decimal128, error) {
	return percentileDecimal64Vals(collectMedianValues(vs), p, argScale)
}

func percentileDecimal64Vals(values []types.Decimal64, p float64, argScale int32) (types.Decimal128, error) {
	if len(values) == 0 || p < 0 || p > 1 {
		return types.Decimal128{}, nil
	}
	rat, _, err := parsePercentileConfig([]byte(strconv.FormatFloat(p, 'g', -1, 64)))
	if err != nil {
		return types.Decimal128{}, err
	}
	loRank, hiRank, frac := percentileRanks(uint64(len(values)), rat)
	compare := func(a, b types.Decimal64) int { return a.Compare(b) }
	lo := FromD64ToD128(selectKthFunc(values, int(loRank), compare))
	hi := FromD64ToD128(selectKthFunc(values, int(hiRank), compare))
	return interpolateDecimal(lo, hi, frac, 1)
}

func PercentileDecimal128(vs *Vectors[types.Decimal128], p float64, argScale int32) (types.Decimal128, error) {
	argWidth := approxPercentileDecimalWidth
	if len(vs.vecs) > 0 {
		argWidth = vs.vecs[0].GetType().Width
	}
	return percentileDecimal128Vals(collectMedianValues(vs), p, argWidth, argScale)
}

func percentileDecimal128Vals(values []types.Decimal128, p float64, argWidth, argScale int32) (types.Decimal128, error) {
	if len(values) == 0 || p < 0 || p > 1 {
		return types.Decimal128{}, nil
	}
	rat, _, err := parsePercentileConfig([]byte(strconv.FormatFloat(p, 'g', -1, 64)))
	if err != nil {
		return types.Decimal128{}, err
	}
	loRank, hiRank, frac := percentileRanks(uint64(len(values)), rat)
	compare := func(a, b types.Decimal128) int { return a.Compare(b) }
	lo := selectKthFunc(values, int(loRank), compare)
	hi := selectKthFunc(values, int(hiRank), compare)
	argType := types.New(types.T_decimal128, argWidth, argScale)
	resultType := ApproxPercentileReturnType([]types.Type{argType})
	return interpolateDecimal(lo, hi, frac, resultType.Scale-argScale)
}
