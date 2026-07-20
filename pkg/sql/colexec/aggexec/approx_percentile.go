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
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	approxPercentileSketchCapacity = 200
	approxPercentileMaxLevels      = 64
	approxPercentileSketchVersion  = byte(1)
)

type quantileValue interface {
	numeric | types.Decimal64 | types.Decimal128
}

// quantileSketch is a bounded, mergeable KLL-style sketch. Each level stores
// values with weight 2^level and is compacted once it reaches 2*k entries.
// The fixed level limit bounds every group's retained state independently of
// its input row count.
type quantileSketch[T quantileValue] struct {
	levels   [][]T
	parity   []bool
	count    uint64
	min      T
	max      T
	hasValue bool
	compare  func(T, T) int
}

func newQuantileSketch[T quantileValue](compare func(T, T) int) *quantileSketch[T] {
	return &quantileSketch[T]{compare: compare}
}

func (s *quantileSketch[T]) Add(value T) error {
	if s.count == math.MaxUint64 {
		return moerr.NewInvalidInputNoCtx("approx_percentile: row count overflow")
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
	s.ensureLevel(0)
	s.levels[0] = append(s.levels[0], value)
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
	s.count += other.count
	for level, values := range other.levels {
		s.ensureLevel(level)
		s.levels[level] = append(s.levels[level], values...)
		if level < len(other.parity) && other.parity[level] {
			s.parity[level] = !s.parity[level]
		}
	}
	return s.compactFrom(0)
}

func (s *quantileSketch[T]) ensureLevel(level int) {
	for len(s.levels) <= level {
		s.levels = append(s.levels, nil)
		s.parity = append(s.parity, false)
	}
}

func (s *quantileSketch[T]) compactFrom(start int) error {
	for level := start; level < len(s.levels); level++ {
		for len(s.levels[level]) >= 2*approxPercentileSketchCapacity {
			if level+1 >= approxPercentileMaxLevels {
				return moerr.NewInvalidInputNoCtx("approx_percentile: sketch level overflow")
			}
			values := s.levels[level]
			sort.Slice(values, func(i, j int) bool {
				return s.compare(values[i], values[j]) < 0
			})

			pickSecond := s.parity[level]
			s.parity[level] = !s.parity[level]
			startAt, endAt := 0, len(values)
			var retained []T
			if len(values)&1 == 1 {
				if pickSecond {
					retained = append(retained, values[0])
					startAt = 1
				} else {
					endAt--
					retained = append(retained, values[endAt])
				}
			}
			pick := 0
			if pickSecond {
				pick = 1
			}
			promoted := make([]T, 0, (endAt-startAt)/2)
			for i := startAt; i < endAt; i += 2 {
				promoted = append(promoted, values[i+pick])
			}
			s.levels[level] = retained
			s.ensureLevel(level + 1)
			s.levels[level+1] = append(s.levels[level+1], promoted...)
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

func (s *quantileSketch[T]) Quantile(p *big.Rat) (lo, hi T, frac *big.Rat, err error) {
	if s.count == 0 {
		return lo, hi, nil, moerr.NewInternalErrorNoCtx("approx_percentile: empty sketch")
	}
	if p.Sign() == 0 {
		return s.min, s.min, new(big.Rat), nil
	}
	if p.Cmp(big.NewRat(1, 1)) == 0 {
		return s.max, s.max, new(big.Rat), nil
	}
	weighted := make([]weightedQuantileValue[T], 0, s.retained())
	for level, values := range s.levels {
		weight := uint64(1) << level
		for _, value := range values {
			weighted = append(weighted, weightedQuantileValue[T]{value: value, weight: weight})
		}
	}
	sort.Slice(weighted, func(i, j int) bool {
		return s.compare(weighted[i].value, weighted[j].value) < 0
	})
	loRank, hiRank, frac := percentileRanks(s.count, p)
	return s.valueAtRank(loRank, weighted), s.valueAtRank(hiRank, weighted), frac, nil
}

func (s *quantileSketch[T]) retained() int {
	n := 0
	for _, values := range s.levels {
		n += len(values)
	}
	return n
}

func (s *quantileSketch[T]) Size() int64 {
	var zero T
	valueSize := len(types.EncodeFixed(zero))
	capacity := 0
	for _, values := range s.levels {
		capacity += cap(values)
	}
	return int64((capacity+2)*valueSize + cap(s.levels)*24 + cap(s.parity))
}

func (s *quantileSketch[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(approxPercentileSketchVersion)
	if err := types.WriteUint64(&buf, s.count); err != nil {
		return nil, err
	}
	if s.hasValue {
		buf.WriteByte(1)
		if _, err := buf.Write(types.EncodeFixed(s.min)); err != nil {
			return nil, err
		}
		if _, err := buf.Write(types.EncodeFixed(s.max)); err != nil {
			return nil, err
		}
	} else {
		buf.WriteByte(0)
	}
	if len(s.levels) > approxPercentileMaxLevels {
		return nil, moerr.NewInternalErrorNoCtx("approx_percentile: too many sketch levels")
	}
	if err := types.WriteUint16(&buf, uint16(len(s.levels))); err != nil {
		return nil, err
	}
	for level, values := range s.levels {
		if s.parity[level] {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		if len(values) >= 2*approxPercentileSketchCapacity {
			return nil, moerr.NewInternalErrorNoCtx("approx_percentile: uncompacted sketch")
		}
		if err := types.WriteUint16(&buf, uint16(len(values))); err != nil {
			return nil, err
		}
		for _, value := range values {
			if _, err := buf.Write(types.EncodeFixed(value)); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func (s *quantileSketch[T]) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	version, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if version != approxPercentileSketchVersion {
		return moerr.NewInvalidInputNoCtxf("approx_percentile: unsupported sketch version %d", version)
	}
	count, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	var zero T
	valueSize := len(types.EncodeFixed(zero))
	hasValue, err := reader.ReadByte()
	if err != nil || hasValue > 1 {
		return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch extrema flag")
	}
	if hasValue == 1 {
		encoded := make([]byte, valueSize)
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return err
		}
		s.min = types.DecodeFixed[T](encoded)
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return err
		}
		s.max = types.DecodeFixed[T](encoded)
		s.hasValue = true
	}
	levelCount, err := types.ReadUint16(reader)
	if err != nil {
		return err
	}
	if levelCount > approxPercentileMaxLevels {
		return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch level count")
	}
	s.levels = make([][]T, int(levelCount))
	s.parity = make([]bool, int(levelCount))
	var represented uint64
	for level := range int(levelCount) {
		parity, err := reader.ReadByte()
		if err != nil || parity > 1 {
			return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch parity")
		}
		s.parity[level] = parity == 1
		length, err := types.ReadUint16(reader)
		if err != nil {
			return err
		}
		if length >= 2*approxPercentileSketchCapacity {
			return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch level size")
		}
		values := make([]T, int(length))
		for i := range values {
			encoded := make([]byte, valueSize)
			if _, err := io.ReadFull(reader, encoded); err != nil {
				return err
			}
			values[i] = types.DecodeFixed[T](encoded)
		}
		s.levels[level] = values
		weight := uint64(1) << level
		if uint64(length) > math.MaxUint64/weight || math.MaxUint64-represented < uint64(length)*weight {
			return moerr.NewInvalidInputNoCtx("approx_percentile: invalid sketch weight")
		}
		represented += uint64(length) * weight
	}
	if reader.Len() != 0 || represented != count || s.hasValue != (count > 0) || s.hasValue && s.compare(s.min, s.max) > 0 {
		return moerr.NewInvalidInputNoCtx("approx_percentile: inconsistent sketch state")
	}
	s.count = count
	return nil
}

func (s *quantileSketch[T]) UnmarshalFromReader(reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return s.UnmarshalBinary(data)
}

func percentileRanks(count uint64, p *big.Rat) (lo, hi uint64, frac *big.Rat) {
	if count <= 1 {
		return 0, 0, new(big.Rat)
	}
	rank := new(big.Rat).Mul(p, new(big.Rat).SetInt(new(big.Int).SetUint64(count-1)))
	loInt := new(big.Int).Quo(rank.Num(), rank.Denom())
	lo = loInt.Uint64()
	hi = lo
	if lo < count-1 {
		hi++
	}
	rem := new(big.Int).Mod(new(big.Int).Set(rank.Num()), rank.Denom())
	return lo, hi, new(big.Rat).SetFrac(rem, new(big.Int).Set(rank.Denom()))
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
		makeMarshalerUnmarshaler: func(*mpool.MPool) (MarshalerUnmarshaler, error) {
			return newQuantileSketch(compare), nil
		},
	}
	return exec
}

func (exec *approxPercentileExecBase[T]) GroupGrow(more int) error {
	start := exec.GetNumGroups()
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	for group := start; group < start+more; group++ {
		if _, err := exec.ensureSketch(uint64(group)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxPercentileExecBase[T]) ensureSketch(group uint64) (*quantileSketch[T], error) {
	x, y := exec.getXY(group)
	if exec.state[x].mobs[y] == nil {
		mob, err := exec.makeMarshalerUnmarshaler(exec.mp)
		if err != nil {
			return nil, err
		}
		exec.state[x].mobs[y] = mob
	}
	return exec.state[x].mobs[y].(*quantileSketch[T]), nil
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

func (exec *approxPercentileExecBase[T]) batchMerge(other *approxPercentileExecBase[T], offset int, groups []uint64) error {
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
		result := vector.NewOffHeapVecWithType(exec.retType)
		results[x] = result
		if err := result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[float64](result)
		for y := 0; y < int(state.length); y++ {
			if state.mobs[y] == nil || state.mobs[y].(*quantileSketch[T]).count == 0 {
				result.SetNull(uint64(y))
				continue
			}
			lo, hi, frac, err := state.mobs[y].(*quantileSketch[T]).Quantile(exec.percentile)
			if err != nil {
				return nil, err
			}
			fraction, _ := frac.Float64()
			values[y] = float64(lo) + (float64(hi)-float64(lo))*fraction
		}
	}
	return results, nil
}

type approxPercentileDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	approxPercentileExecBase[T]
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
		result := vector.NewOffHeapVecWithType(exec.retType)
		results[x] = result
		if err := result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[types.Decimal128](result)
		for y := 0; y < int(state.length); y++ {
			if state.mobs[y] == nil || state.mobs[y].(*quantileSketch[T]).count == 0 {
				result.SetNull(uint64(y))
				continue
			}
			lo, hi, frac, err := state.mobs[y].(*quantileSketch[T]).Quantile(exec.percentile)
			if err != nil {
				return nil, err
			}
			values[y], err = interpolateDecimal(toDecimal128(lo), toDecimal128(hi), frac)
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

func decimal128ToBigInt(value types.Decimal128) *big.Int {
	result := new(big.Int).SetUint64(value.B64_127)
	result.Lsh(result, 64)
	result.Or(result, new(big.Int).SetUint64(value.B0_63))
	if value.Sign() {
		result.Sub(result, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return result
}

func decimal128FromBigInt(value *big.Int) (types.Decimal128, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 127)
	if value.Cmp(new(big.Int).Neg(new(big.Int).Set(limit))) < 0 || value.Cmp(new(big.Int).Sub(new(big.Int).Set(limit), big.NewInt(1))) > 0 {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtx("approx_percentile: decimal interpolation overflow")
	}
	unsigned := new(big.Int).Set(value)
	if unsigned.Sign() < 0 {
		unsigned.Add(unsigned, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	lowMask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))
	low := new(big.Int).And(new(big.Int).Set(unsigned), lowMask).Uint64()
	high := new(big.Int).Rsh(unsigned, 64).Uint64()
	return types.Decimal128{B0_63: low, B64_127: high}, nil
}

// interpolateDecimal returns a Decimal128 whose scale is one greater than the
// input scale. All arithmetic stays integral/rational, so values above 2^53 do
// not pass through float64.
func interpolateDecimal(lo, hi types.Decimal128, frac *big.Rat) (types.Decimal128, error) {
	den := new(big.Int).Set(frac.Denom())
	rem := new(big.Int).Set(frac.Num())
	loInt := decimal128ToBigInt(lo)
	diff := new(big.Int).Sub(decimal128ToBigInt(hi), loInt)
	numerator := new(big.Int).Mul(loInt, den)
	numerator.Add(numerator, new(big.Int).Mul(diff, rem))
	numerator.Mul(numerator, big.NewInt(10))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, den, remainder)
	if new(big.Int).Lsh(new(big.Int).Abs(remainder), 1).Cmp(den) >= 0 {
		if numerator.Sign() < 0 {
			quotient.Sub(quotient, big.NewInt(1))
		} else {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	return decimal128FromBigInt(quotient)
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
	fraction, _ := frac.Float64()
	return float64(lo) + (float64(hi)-float64(lo))*fraction
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
	return interpolateDecimal(lo, hi, frac)
}

func PercentileDecimal128(vs *Vectors[types.Decimal128], p float64, argScale int32) (types.Decimal128, error) {
	return percentileDecimal128Vals(collectMedianValues(vs), p, argScale)
}

func percentileDecimal128Vals(values []types.Decimal128, p float64, argScale int32) (types.Decimal128, error) {
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
	return interpolateDecimal(lo, hi, frac)
}
