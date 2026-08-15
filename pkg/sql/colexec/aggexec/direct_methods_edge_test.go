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
	"errors"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type aggregateBytesThenErrorReader struct {
	data []byte
	err  error
}

func (r *aggregateBytesThenErrorReader) Read(value []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(value, r.data)
	r.data = r.data[n:]
	return n, nil
}

type aggregateShortWriter struct {
	shortCall int
	calls     int
}

type unboundedAggregateState struct{}

func (*unboundedAggregateState) MarshalBinary() ([]byte, error) { return nil, nil }
func (*unboundedAggregateState) UnmarshalBinary([]byte) error   { return nil }
func (*unboundedAggregateState) UnmarshalFromReader(io.Reader) error {
	return nil
}

type boundedAggregateState struct {
	size int
	err  error
}

func (*boundedAggregateState) MarshalBinary() ([]byte, error) { return nil, nil }
func (*boundedAggregateState) UnmarshalBinary([]byte) error   { return nil }
func (*boundedAggregateState) UnmarshalFromReader(io.Reader) error {
	return nil
}
func (s *boundedAggregateState) MarshaledSize() int { return s.size }
func (s *boundedAggregateState) MarshalTo(writer io.Writer) error {
	if s.err != nil {
		return s.err
	}
	_, err := writer.Write(make([]byte, s.size))
	return err
}

func (w *aggregateShortWriter) Write(value []byte) (int, error) {
	w.calls++
	if w.calls == w.shortCall && len(value) != 0 {
		return len(value) - 1, nil
	}
	return len(value), nil
}

func freeDirectAggregate(t *testing.T, mp *mpool.MPool, exec AggFuncExec) []*vector.Vector {
	t.Helper()
	result, err := exec.Flush()
	require.NoError(t, err)
	exec.Free()
	t.Cleanup(func() {
		for _, vec := range result {
			vec.Free(mp)
		}
	})
	return result
}

func assertJSONFixedValue[T types.FixedSizeTExceptStrType](
	t *testing.T, mp *mpool.MPool, typ types.Type, value T,
) {
	t.Helper()
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	encoded, err := appendJSONAggregateValue(nil, vec, 0)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)
	vec.Free(mp)
}

func TestAggregateSingleRowAndMergeEntryPoints(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("any", func(t *testing.T) {
		input := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytesList(input, [][]byte{[]byte("left"), []byte("right")}, nil, mp))
		defer input.Free(mp)

		left := makeAnyValueExec(mp, AggIdOfAny, types.T_varchar.ToType())
		right := makeAnyValueExec(mp, AggIdOfAny, types.T_varchar.ToType())
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, right.Fill(0, 1, []*vector.Vector{input}))
		require.NoError(t, left.Merge(right, 0, 0))
		require.NoError(t, left.SetExtraInformation(nil, 0))
		right.Free()
		result := freeDirectAggregate(t, mp, left)
		require.Equal(t, "right", string(result[0].GetBytesAt(0)))
	})

	t.Run("fixed-bit-operation", func(t *testing.T) {
		input := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixedList(input, []uint64{3, 5}, nil, mp))
		defer input.Free(mp)

		left := makeBitOrExec(mp, AggIdOfBitOr, false, types.T_uint64.ToType())
		right := makeBitOrExec(mp, AggIdOfBitOr, false, types.T_uint64.ToType())
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.Fill(0, 0, []*vector.Vector{input}))
		require.NoError(t, right.Fill(0, 1, []*vector.Vector{input}))
		require.NoError(t, left.Merge(right, 0, 0))
		require.NoError(t, left.SetExtraInformation(nil, 0))
		right.Free()
		result := freeDirectAggregate(t, mp, left)
		require.Equal(t, uint64(7), vector.MustFixedColNoTypeCheck[uint64](result[0])[0])
	})

	t.Run("binary-bit-operation", func(t *testing.T) {
		typ := types.New(types.T_binary, 2, 0)
		input := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytesList(input, [][]byte{{0x0f, 0xf0}, {0xf0, 0x0f}}, nil, mp))
		defer input.Free(mp)

		left := makeBitXorExec(mp, AggIdOfBitXor, false, typ)
		right := makeBitXorExec(mp, AggIdOfBitXor, false, typ)
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.Fill(0, 0, []*vector.Vector{input}))
		require.NoError(t, right.Fill(0, 1, []*vector.Vector{input}))
		require.NoError(t, left.Merge(right, 0, 0))
		require.NoError(t, left.SetExtraInformation(nil, 0))
		right.Free()
		result := freeDirectAggregate(t, mp, left)
		require.Equal(t, []byte{0xff, 0xff}, result[0].GetBytesAt(0))
	})

	t.Run("binary-bit-neutral-values", func(t *testing.T) {
		typ := types.New(types.T_binary, 2, 0)
		for _, tc := range []struct {
			id   int64
			want []byte
		}{
			{id: AggIdOfBitAnd, want: []byte{0xff, 0xff}},
			{id: AggIdOfBitOr, want: []byte{0, 0}},
			{id: AggIdOfBitXor, want: []byte{0, 0}},
		} {
			exec := makeBitAggExec(mp, tc.id, typ)
			require.NoError(t, exec.GroupGrow(1))
			result := freeDirectAggregate(t, mp, exec)
			require.Equal(t, tc.want, result[0].GetBytesAt(0))
		}
	})

	t.Run("variance", func(t *testing.T) {
		input := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input, []int64{1, 2, 3}, nil, mp))
		defer input.Free(mp)

		left := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_int64.ToType())
		right := makeVarPopExec(mp, AggIdOfVarPop, false, types.T_int64.ToType())
		require.NoError(t, left.GroupGrow(1))
		require.NoError(t, right.GroupGrow(1))
		require.NoError(t, left.Fill(0, 0, []*vector.Vector{input}))
		require.NoError(t, right.BulkFill(0, []*vector.Vector{input}))
		require.NoError(t, left.Merge(right, 0, 0))
		require.NoError(t, left.SetExtraInformation(nil, 0))
		right.Free()
		result := freeDirectAggregate(t, mp, left)
		require.False(t, result[0].IsNull(0))
	})

	t.Run("median-bulk", func(t *testing.T) {
		input := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input, []int64{4, 1, 3, 2}, nil, mp))
		defer input.Free(mp)
		exec, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		result := freeDirectAggregate(t, mp, exec)
		require.Equal(t, 2.5, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	})

	t.Run("group-concat-bulk", func(t *testing.T) {
		input := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytesList(input, [][]byte{[]byte("a"), []byte("b")}, nil, mp))
		defer input.Free(mp)
		exec, err := MakeAgg(mp, AggIdOfGroupConcat, false, types.T_varchar.ToType())
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
		result := freeDirectAggregate(t, mp, exec)
		require.Equal(t, "a,b", string(result[0].GetBytesAt(0)))
	})
}

func TestHLLSingleRowAndBulkEntryPoints(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int64{1, 2, 3}, nil, mp))
	defer input.Free(mp)

	add := makeHllAdd(mp, AggIdOfHllAdd, types.T_int64.ToType())
	require.NoError(t, add.GroupGrow(1))
	require.NoError(t, add.Fill(0, 0, []*vector.Vector{input}))
	require.NoError(t, add.BulkFill(0, []*vector.Vector{input}))
	require.NoError(t, add.SetExtraInformation(nil, 0))
	states := freeDirectAggregate(t, mp, add)
	require.NotEmpty(t, states[0].GetBytesAt(0))

	merge := makeHllMerge(mp, AggIdOfHllMerge, types.T_varbinary.ToType())
	require.NoError(t, merge.GroupGrow(1))
	require.NoError(t, merge.Fill(0, 0, states))
	require.NoError(t, merge.BulkFill(0, states))
	require.NoError(t, merge.SetExtraInformation(nil, 0))
	merged := freeDirectAggregate(t, mp, merge)
	require.NotEmpty(t, merged[0].GetBytesAt(0))
}

func TestAccountedWideBinaryBitNeutralFlush(t *testing.T) {
	for _, tc := range []struct {
		id      int64
		neutral byte
	}{
		{id: AggIdOfBitAnd, neutral: 0xff},
		{id: AggIdOfBitOr, neutral: 0x00},
	} {
		mp := mpool.MustNewZero()
		registry, account, allocation := newTestAggregateAllocation(t)
		typ := types.New(types.T_binary, types.VarlenaInlineSize+8, 0)
		exec, err := MakeAgg(mp, tc.id, false, typ)
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(2))

		input := vector.NewVec(typ)
		value := bytes.Repeat([]byte{0x5a}, int(typ.Width))
		require.NoError(t, vector.AppendBytes(input, value, false, mp))
		require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, []uint64{1}, []*vector.Vector{input}))
		require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
		input.Free(mp)

		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, value, result[0].GetBytesAt(0))
		require.Equal(t, bytes.Repeat([]byte{tc.neutral}, int(typ.Width)),
			result[0].GetBytesAt(1))
		result[0].Free(mp)
		exec.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	}
}

func TestAggregateVectorWireFailureRollback(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	source := NewVectors[int64](typ)
	require.NoError(t, AppendMultiFixed(source, 7, false, 3, mp))
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	assertRejected := func(payload io.Reader) {
		t.Helper()
		target := NewVectors[int64](typ)
		baseline := len(target.vecs)
		require.Error(t, target.UnmarshalFromReader(payload, typ, mp))
		require.Len(t, target.vecs, baseline)
		target.Free(mp)
	}
	assertRejected(bytes.NewReader(encoded[:7]))
	negative := int64(-1)
	assertRejected(bytes.NewReader(types.EncodeInt64(&negative)))
	one := int64(1)
	assertRejected(bytes.NewReader(types.EncodeInt64(&one)))

	vectorPayload := encoded[12:]
	declared := uint32(len(vectorPayload) + 1)
	truncated := append(bytes.Clone(types.EncodeInt64(&one)), types.EncodeUint32(&declared)...)
	truncated = append(truncated, vectorPayload...)
	assertRejected(bytes.NewReader(truncated))
	assertRejected(&aggregateBytesThenErrorReader{
		data: truncated,
		err:  errors.New("aggregate vector reader failed"),
	})

	_, err = WriteBytes([]byte("payload"), &aggregateShortWriter{shortCall: 1})
	require.ErrorIs(t, err, io.ErrShortWrite)
	_, err = WriteBytes([]byte("payload"), &aggregateShortWriter{shortCall: 2})
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Zero(t, mp.CurrNB())
}

func TestAggregateAllocationBindingBoundaryMatrix(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 64)
	require.NoError(t, err)
	firstAccount, err := registry.Open(1 << 20)
	require.NoError(t, err)
	secondAccount, err := registry.Open(1 << 20)
	require.NoError(t, err)
	sites := AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4,
		ArgumentCount: 5, ArgumentArena: 6,
	}
	first, err := NewAllocationAccount(firstAccount, mpool.AllocationOwnerGroup, sites)
	require.NoError(t, err)
	second, err := NewAllocationAccount(secondAccount, mpool.AllocationOwnerGroup, sites)
	require.NoError(t, err)

	var nilExec *aggExec
	require.ErrorIs(t, nilExec.SetAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilExec.ClearAllocationAccount(first))
	exec := &aggExec{}
	require.ErrorIs(t, exec.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, exec.SetAllocationAccount(first))
	require.NoError(t, exec.SetAllocationAccount(first))
	require.ErrorIs(t, exec.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, exec.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	exec.state = []aggState{{}}
	require.ErrorIs(t, exec.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	exec.state = nil
	require.NoError(t, exec.ClearAllocationAccount(first))

	live := &aggExec{state: []aggState{{}}}
	require.ErrorIs(t, live.SetAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	opaque := &aggExec{aggInfo: aggInfo{
		aggId: 99,
		makeMarshalerUnmarshaler: func(*mpool.MPool, *AllocationAccount) (MarshalerUnmarshaler, error) {
			return nil, nil
		},
	}}
	require.Error(t, opaque.SetAllocationAccount(first))

	firstAccount.Seal()
	secondAccount.Seal()
	_, err = registry.Finalize(firstAccount)
	require.NoError(t, err)
	_, err = registry.Finalize(secondAccount)
	require.NoError(t, err)
}

func TestAccountedSumAndAverageFlushFamilies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		id    int64
		typ   types.Type
		build func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "sum-int64", id: AggIdOfSum, typ: types.T_int64.ToType(),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3})
			},
		},
		{
			name: "avg-int64", id: AggIdOfAvg, typ: types.T_int64.ToType(),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3})
			},
		},
		{
			name: "sum-decimal64", id: AggIdOfSum, typ: types.New(types.T_decimal64, 10, 2),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				typ := types.New(types.T_decimal64, 10, 2)
				return buildFixedVec(t, mp, typ, mustDecimal64s(t, "1.00", "2.00", "3.00"))
			},
		},
		{
			name: "avg-decimal64", id: AggIdOfAvg, typ: types.New(types.T_decimal64, 10, 2),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				typ := types.New(types.T_decimal64, 10, 2)
				return buildFixedVec(t, mp, typ, mustDecimal64s(t, "1.00", "2.00", "3.00"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(mp, tc.id, false, tc.typ)
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			require.NoError(t, exec.GroupGrow(2))
			input := tc.build(t, mp)
			groups := []uint64{1, 1, 1}
			require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
				0, groups, []*vector.Vector{input}))
			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
			input.Free(mp)
			result, err := exec.Flush()
			require.NoError(t, err)
			require.False(t, result[0].IsNull(0))
			require.True(t, result[0].IsNull(1))
			result[0].Free(mp)
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedOrderedPercentileResultModes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		aggID      int64
		percentile string
		want       float64
	}{
		{name: "continuous", aggID: AggIdOfPercentileCont, percentile: "0.25", want: 17.5},
		{name: "discrete-descending", aggID: AggIdOfPercentileDisc, percentile: "0.5", want: 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(mp, tc.aggID, false, types.T_int64.ToType())
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			require.NoError(t, exec.SetExtraInformation(
				EncodeOrderedPercentileConfig([]byte(tc.percentile), tc.aggID == AggIdOfPercentileDisc), 0))
			require.NoError(t, exec.GroupGrow(2))

			input := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(input, []int64{10, 20, 30, 40}, nil, mp))
			groups := []uint64{1, 1, 1, 1}
			require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
				0, groups, []*vector.Vector{input}))
			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
			input.Free(mp)

			result, err := exec.Flush()
			require.NoError(t, err)
			if tc.aggID == AggIdOfPercentileDisc {
				require.Equal(t, int64(tc.want), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			} else {
				require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
			}
			require.True(t, result[0].IsNull(1))
			result[0].Free(mp)
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedDecimalMedianFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	typ := types.New(types.T_decimal64, 10, 2)
	exec, err := MakeAgg(mp, AggIdOfMedian, false, typ)
	require.NoError(t, err)
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(2))

	input := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(
		input, mustDecimal64s(t, "1.00", "2.00", "4.00", "8.00"), nil, mp))
	groups := []uint64{1, 1, 1, 1}
	require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
		0, groups, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
	input.Free(mp)

	result, err := exec.Flush()
	require.NoError(t, err)
	require.False(t, result[0].IsNull(0))
	require.True(t, result[0].IsNull(1))
	result[0].Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestWirePrimitiveBoundaries(t *testing.T) {
	value, next, err := decodeHLLVarUint([]byte{0xac, 0x02}, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(300), value)
	require.Equal(t, 2, next)
	_, _, err = decodeHLLVarUint(nil, 0)
	require.Error(t, err)
	_, _, err = decodeHLLVarUint([]byte{0x80, 0x80, 0x80, 0x80, 0x80}, 0)
	require.Error(t, err)

	require.Equal(t, int64(-42), int64(binary.LittleEndian.Uint64(appendJSONInt64(nil, -42))))
	require.Equal(t, uint64(42), binary.LittleEndian.Uint64(appendJSONUint64(nil, 42)))
	require.True(t, math.IsNaN(math.Float64frombits(
		binary.LittleEndian.Uint64(appendJSONFloat64(nil, math.NaN())))))
	require.Equal(t, 1.5, math.Float64frombits(
		binary.LittleEndian.Uint64(appendJSONFloat64(nil, 1.5))))
}

func TestSparseHLLMergeValidationAndPublication(t *testing.T) {
	sketch := &hllSketch{regs: make([]byte, hllRegisterCnt)}
	require.Error(t, sketch.mergeSparseBytes(nil))

	frame := func(temporary []uint32, count, last uint32, list []byte) []byte {
		data := make([]byte, 8+len(temporary)*4+12+len(list))
		data[0], data[1], data[3] = hllVersion, hllPrecision, 1
		binary.BigEndian.PutUint32(data[4:8], uint32(len(temporary)))
		offset := 8
		for _, value := range temporary {
			binary.BigEndian.PutUint32(data[offset:offset+4], value)
			offset += 4
		}
		binary.BigEndian.PutUint32(data[offset:offset+4], count)
		binary.BigEndian.PutUint32(data[offset+4:offset+8], last)
		binary.BigEndian.PutUint32(data[offset+8:offset+12], uint32(len(list)))
		copy(data[offset+12:], list)
		return data
	}
	badTemporary := make([]byte, 8)
	binary.BigEndian.PutUint32(badTemporary[4:], 1)
	require.Error(t, sketch.mergeSparseBytes(badTemporary))
	require.Error(t, sketch.mergeSparseBytes(make([]byte, 12)))

	badListSize := frame(nil, 0, 0, nil)
	binary.BigEndian.PutUint32(badListSize[16:20], 1)
	require.Error(t, sketch.mergeSparseBytes(badListSize))
	require.Error(t, sketch.mergeSparseBytes(frame(nil, 1, 1, []byte{0x80})))
	require.Error(t, sketch.mergeSparseBytes(frame(nil, 1, 2, []byte{1})))
	require.Error(t, sketch.mergeSparseBytes(frame(nil, 2, 1, []byte{1})))
	require.NoError(t, sketch.mergeSparseBytes(frame([]uint32{3}, 2, 3, []byte{1, 2})))
	require.NotZero(t, sketch.Estimate())
}

func TestJSONAggregateValueEncodingTypeMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	assertJSONFixedValue(t, mp, types.T_bool.ToType(), true)
	assertJSONFixedValue(t, mp, types.T_int8.ToType(), int8(-1))
	assertJSONFixedValue(t, mp, types.T_int16.ToType(), int16(-2))
	assertJSONFixedValue(t, mp, types.T_int32.ToType(), int32(-3))
	assertJSONFixedValue(t, mp, types.T_int64.ToType(), int64(-4))
	assertJSONFixedValue(t, mp, types.T_uint8.ToType(), uint8(1))
	assertJSONFixedValue(t, mp, types.T_uint16.ToType(), uint16(2))
	assertJSONFixedValue(t, mp, types.T_uint32.ToType(), uint32(3))
	assertJSONFixedValue(t, mp, types.T_uint64.ToType(), uint64(4))
	assertJSONFixedValue(t, mp, types.T_float32.ToType(), float32(1.25))
	assertJSONFixedValue(t, mp, types.T_float64.ToType(), 2.5)
	assertJSONFixedValue(t, mp, types.New(types.T_decimal64, 10, 2), mustDecimal64s(t, "1.25")[0])
	decimal128, err := types.ParseDecimal128("2.50", 20, 2)
	require.NoError(t, err)
	assertJSONFixedValue(t, mp, types.New(types.T_decimal128, 20, 2), decimal128)
	assertJSONFixedValue(t, mp, types.T_date.ToType(), types.Date(1))
	assertJSONFixedValue(t, mp, types.T_time.ToType(), types.Time(1))
	assertJSONFixedValue(t, mp, types.T_datetime.ToType(), types.Datetime(1))
	assertJSONFixedValue(t, mp, types.T_timestamp.ToType(), types.Timestamp(1))
	assertJSONFixedValue(t, mp, types.T_uuid.ToType(), types.Uuid{})

	for _, typ := range []types.Type{
		types.T_char.ToType(), types.T_varchar.ToType(), types.T_text.ToType(),
	} {
		vec := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(vec, []byte("text"), false, mp))
		encoded, err := appendJSONAggregateValue(nil, vec, 0)
		require.NoError(t, err)
		require.NotEmpty(t, encoded)
		vec.Free(mp)
	}
	for _, typ := range []types.Type{
		types.T_binary.ToType(), types.T_varbinary.ToType(), types.T_blob.ToType(),
	} {
		vec := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(vec, []byte{1}, false, mp))
		_, err := appendJSONAggregateValue(nil, vec, 0)
		require.Error(t, err)
		vec.Free(mp)
	}
	jsonVec := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(jsonVec, nil, false, mp))
	_, err = appendJSONAggregateValue(nil, jsonVec, 0)
	require.Error(t, err)
	jsonVec.Free(mp)

	arrays := []*vector.Vector{
		vector.NewVec(types.T_array_float32.ToType()),
		vector.NewVec(types.T_array_float64.ToType()),
		vector.NewVec(types.T_array_bf16.ToType()),
		vector.NewVec(types.T_array_float16.ToType()),
		vector.NewVec(types.T_array_int8.ToType()),
		vector.NewVec(types.T_array_uint8.ToType()),
	}
	require.NoError(t, vector.AppendArrayList(arrays[0], [][]float32{{1, 2}}, nil, mp))
	require.NoError(t, vector.AppendArrayList(arrays[1], [][]float64{{1, 2}}, nil, mp))
	require.NoError(t, vector.AppendArrayList(arrays[2], [][]types.BF16{
		{types.BF16FromFloat32(1), types.BF16FromFloat32(2)}}, nil, mp))
	require.NoError(t, vector.AppendArrayList(arrays[3], [][]types.Float16{
		{types.Float16FromFloat32(1), types.Float16FromFloat32(2)}}, nil, mp))
	require.NoError(t, vector.AppendArrayList(arrays[4], [][]int8{{1, 2}}, nil, mp))
	require.NoError(t, vector.AppendArrayList(arrays[5], [][]uint8{{1, 2}}, nil, mp))
	for _, vec := range arrays {
		size, err := jsonAggregateValueSize(vec, 0)
		require.NoError(t, err)
		encoded, err := appendJSONAggregateValue(make([]byte, 0, size), vec, 0)
		require.NoError(t, err, vec.GetType().String())
		require.NotEmpty(t, encoded)
		vec.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func TestNtileIntermediateRoundTripAndTruncation(t *testing.T) {
	mp := mpool.MustNewZero()
	makeExec := func() *ntileWindowExec {
		exec, err := makeNtileExec(
			mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		return exec.(*ntileWindowExec)
	}
	input := vector.NewVec(types.T_int64.ToType())
	buckets := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int64{10, 20}, nil, mp))
	require.NoError(t, vector.AppendFixedList(buckets, []int64{2, 2}, nil, mp))
	defer input.Free(mp)
	defer buckets.Free(mp)

	source := makeExec()
	require.NoError(t, source.GroupGrow(2))
	require.NoError(t, source.Fill(0, 0, []*vector.Vector{input, buckets}))
	require.NoError(t, source.Fill(1, 1, []*vector.Vector{input, buckets}))
	// The NTILE intermediate group codec stores its bucket count as the final
	// int64 in each group payload.
	for group := range source.groups {
		source.groups[group] = append(source.groups[group], source.bucketCounts[group])
	}
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))
	for cut := 0; cut < encoded.Len(); cut++ {
		target := makeExec()
		require.Error(t, target.UnmarshalFromReader(
			bytes.NewReader(encoded.Bytes()[:cut]), mp), "cut=%d", cut)
		target.Free()
	}
	target := makeExec()
	require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
	require.Equal(t, []int64{2, 2}, target.bucketCounts)
	require.Equal(t, []i64Slice{{10}, {20}}, target.groups)
	target.Free()
	source.Free()
	require.Zero(t, mp.CurrNB())
}

func TestAggregateStatePrimitiveBoundaryMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	require.Error(t, writeBoundedOpaqueState(
		&unboundedAggregateState{}, io.Discard, 7))
	require.Error(t, writeBoundedOpaqueState(
		&boundedAggregateState{size: -1}, io.Discard, 7))
	require.Error(t, writeBoundedOpaqueState(
		&boundedAggregateState{size: math.MaxInt32 + 1}, io.Discard, 7))
	require.Error(t, writeBoundedOpaqueState(
		&boundedAggregateState{size: 4}, &aggregateShortWriter{shortCall: 1}, 7))
	sentinel := errors.New("opaque marshal failure")
	require.ErrorIs(t, writeBoundedOpaqueState(
		&boundedAggregateState{size: 4, err: sentinel}, io.Discard, 7), sentinel)
	var encoded bytes.Buffer
	require.NoError(t, writeBoundedOpaqueState(
		&boundedAggregateState{size: 4}, &encoded, 7))
	require.Len(t, encoded.Bytes(), 8)

	info := &aggInfo{
		aggId:      7,
		isDistinct: true,
		argTypes:   []types.Type{types.T_int64.ToType()},
		retType:    types.T_uint64.ToType(),
		emptyNull:  true,
	}
	require.Equal(t, int64(7), info.AggID())
	require.True(t, info.IsDistinct())
	args, result := info.TypesInfo()
	require.Equal(t, info.argTypes, args)
	require.Equal(t, info.retType, result)
	require.False(t, info.usesOpaqueArgEncoding())
	info.argTypes = []types.Type{types.T_varchar.ToType()}
	require.True(t, info.usesOpaqueArgEncoding())
	info.argTypes = []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}
	require.True(t, info.usesOpaqueArgEncoding())
	require.Contains(t, info.String(), "aggId: 7")

	var nilExec *aggExec
	require.Zero(t, nilExec.PrepareParamKindChunkCount())
	exec := &aggExec{chunkSize: AggBatchSize, state: []aggState{{length: 2}, {length: 3}}}
	require.Equal(t, 2, exec.PrepareParamKindChunkCount())
	require.Equal(t, AggBatchSize, exec.getChunkSize())
	exec.modifyChunkSize(1)
	require.Equal(t, 1, exec.getChunkSize())
	exec.modifyChunkSize(AggBatchSize)
	require.Panics(t, func() { exec.modifyChunkSize(2) })
	require.Same(t, exec, exec.GetOptResult())
	x, y := exec.getXY(AggBatchSize + 3)
	require.Equal(t, 1, x)
	require.Equal(t, uint16(3), y)
	require.Equal(t, 2, exec.GetNumChunks())
	require.Equal(t, 5, exec.GetNumGroups())
	require.Zero(t, exec.AdditionalMemorySize())
	require.Panics(t, func() { exec.Size() })
	require.Nil(t, exec.PrepareParamKindVectorForChunk(-1))
	require.Nil(t, exec.PrepareParamKindVectorForChunk(2))

	scalar := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(scalar, []int64{1, 2}, nil, mp))
	exact := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(exact, []int64{1, 2}, nil, mp))
	require.NoError(t, exact.SetPrepareParamKindsWithMP([]vector.PrepareParamKind{
		vector.PrepareParamInteger, vector.PrepareParamDecimal,
	}, mp))
	exec.state = []aggState{{vecs: []*vector.Vector{scalar}}, {vecs: []*vector.Vector{exact}}}
	exec.SetPrepareParamKind(vector.PrepareParamBoolean)
	require.Equal(t, vector.PrepareParamBoolean, scalar.GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamInteger, exact.GetPrepareParamKindAt(0))
	require.Same(t, scalar, exec.PrepareParamKindVectorForChunk(0))
	scalar.Free(mp)
	exact.Free(mp)
	exec.state = nil

	var invalid aggState
	require.Error(t, invalid.initWithAllocation(mp, 0, 0, info, false, nil))
	require.Error(t, invalid.initWithAllocation(mp, 1, 2, info, false, nil))

	growing := aggState{length: 2, capacity: 4}
	added, remaining, err := growing.grow(mp, 1, false)
	require.NoError(t, err)
	require.Equal(t, int32(1), added)
	require.Zero(t, remaining)
	require.Equal(t, int32(2), growing.length)
	added, remaining, err = growing.grow(mp, 4, true)
	require.NoError(t, err)
	require.Equal(t, int32(2), added)
	require.Equal(t, int32(2), remaining)
	require.Equal(t, int32(4), growing.length)

	saved := &aggInfo{saveArg: true, isDistinct: true,
		argTypes: []types.Type{types.T_int64.ToType()}}
	var state aggState
	require.NoError(t, state.initWithAllocation(mp, 1, 1, saved, false, nil))
	state.argCnt[0] = 1
	require.ErrorContains(t, state.writeStateArg(mp, 0, io.Discard, saved),
		"mismatch count")
	state.free(mp)
}
