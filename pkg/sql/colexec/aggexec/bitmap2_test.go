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
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func newBitmapTestAllocation(
	t *testing.T,
	limit uint64,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount, *AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData:     1,
		VectorArea:     2,
		VectorNulls:    3,
		VectorGrouping: 4,
		ArgumentCount:  5,
		ArgumentArena:  6,
	})
	require.NoError(t, err)
	return registry, account, allocation
}

type errMarshalerUnmarshaler struct {
	err error
}

func (e errMarshalerUnmarshaler) MarshalBinary() ([]byte, error) {
	return nil, e.err
}

func (e errMarshalerUnmarshaler) UnmarshalBinary([]byte) error {
	return e.err
}

func (e errMarshalerUnmarshaler) UnmarshalFromReader(io.Reader) error {
	return e.err
}

func buildTestBitmapVecs(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector) {
	nulls := []bool{false, false, false, false, true, false, false, false, false, true}
	uint64s := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	vec1 := testutil.NewUInt64Vector(10, types.T_uint64.ToType(), mp, false, nil, uint64s[:10])
	vec2 := testutil.NewUInt64Vector(10, types.T_uint64.ToType(), mp, false, nulls, uint64s[2:])
	return vec1, vec2
}

func checkBitmap(t *testing.T, vec *vector.Vector, idx int, expected []uint32) {
	bitmap := roaring.NewBitmap()
	bs := vec.GetBytesAt(idx)
	require.NoError(t, bitmap.UnmarshalBinary(bs))
	require.Equal(t, expected, bitmap.ToArray())
}

func TestAccountedBitmapPortableWireAndLifecycle(t *testing.T) {
	for _, aggregateID := range []int64{
		AggIdOfBitmapConstruct,
		AggIdOfBitmapOr,
	} {
		t.Run(map[int64]string{
			AggIdOfBitmapConstruct: "construct",
			AggIdOfBitmapOr:        "or",
		}[aggregateID], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			param := types.T_uint64.ToType()
			if aggregateID == AggIdOfBitmapOr {
				param = types.T_varbinary.ToType()
			}
			exec, err := MakeAgg(mp, aggregateID, false, param)
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			require.NoError(t, exec.GroupGrow(1))

			if aggregateID == AggIdOfBitmapConstruct {
				input := testutil.NewUInt64Vector(
					5000, types.T_uint64.ToType(), mp, false, nil, nil)
				values := vector.MustFixedColNoTypeCheck[uint64](input)
				for i := range values {
					values[i] = uint64(i)
				}
				groups := slices.Repeat([]uint64{1}, input.Length())
				require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
				input.Free(mp)
			} else {
				first := roaring.BitmapOf(1, 2, 65537)
				second := roaring.New()
				second.AddRange(4000, 9000)
				firstBytes, err := first.ToBytes()
				require.NoError(t, err)
				secondBytes, err := second.ToBytes()
				require.NoError(t, err)
				input := vector.NewVec(types.T_varbinary.ToType())
				require.NoError(t, vector.AppendBytesList(
					input, [][]byte{firstBytes, secondBytes}, nil, mp))
				require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
					0, []uint64{1, 1}, []*vector.Vector{input}))
				require.NoError(t, exec.BatchFill(
					0, []uint64{1, 1}, []*vector.Vector{input}))
				input.Free(mp)
			}

			var spill bytes.Buffer
			codec := exec.(SpillStateCodec)
			require.NoError(t, codec.SaveSpillIntermediateRows(
				0, []int32{0}, &spill))
			restored, err := MakeAgg(mp, aggregateID, false, param)
			require.NoError(t, err)
			restoredOwner := restored.(AllocationAccountOwner)
			require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
			require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
				bytes.NewReader(spill.Bytes()), mp))
			result, err := restored.Flush()
			require.NoError(t, err)
			portable := roaring.New()
			require.NoError(t, portable.UnmarshalBinary(result[0].GetBytesAt(0)))
			if aggregateID == AggIdOfBitmapConstruct {
				require.Equal(t, uint64(5000), portable.GetCardinality())
				require.True(t, portable.Contains(0))
				require.True(t, portable.Contains(4999))
			} else {
				require.Equal(t, uint64(5003), portable.GetCardinality())
				require.True(t, portable.Contains(65537))
			}

			result[0].Free(mp)
			exec.Free()
			restored.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedBitmapPreflightAndCrossAccountMerge(t *testing.T) {
	makeConstruct := func(
		t *testing.T,
		mp *mpool.MPool,
		allocation *AllocationAccount,
		groups int,
	) *bmpConstructExec {
		t.Helper()
		exec := makeBmpConstructExec(
			mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(groups))
		return exec
	}

	t.Run("preflight-reserves-and-deduplicates-work-unit", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newBitmapTestAllocation(t, 128<<20)
		exec := makeConstruct(t, mp, allocation, 1)
		input := testutil.NewUInt64Vector(
			6, types.T_uint64.ToType(), mp, false, nil,
			[]uint64{9, 1, 9, 7, 1, 3})
		groups := slices.Repeat([]uint64{1}, input.Length())
		preflight := AggFuncExec(exec).(BatchCapacityPreflight)
		require.NoError(t, preflight.PreflightBatchFill(
			0, groups, []*vector.Vector{input}))
		mob := exec.state[0].mobs[0].(*bmp)
		capacity := cap(mob.values)
		peak := account.Snapshot().Peak
		require.GreaterOrEqual(t, capacity, 4)
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
		require.Equal(t, []uint32{1, 3, 7, 9}, mob.values)
		require.Equal(t, peak, account.Snapshot().Used,
			"committed mutation must not allocate after preflight")

		input.Free(mp)
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("bitmap-or-publication-is-allocation-free", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newBitmapTestAllocation(t, 128<<20)
		exec := makeBmpOrExec(
			mp, AggIdOfBitmapOr, types.T_varbinary.ToType())
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		first := roaring.BitmapOf(1, 2, 65537)
		second := roaring.New()
		second.AddRange(2, 5002)
		firstBytes, err := first.ToBytes()
		require.NoError(t, err)
		secondBytes, err := second.ToBytes()
		require.NoError(t, err)
		input := vector.NewVec(types.T_varbinary.ToType())
		require.NoError(t, vector.AppendBytesList(
			input, [][]byte{firstBytes, secondBytes}, nil, mp))
		groups := []uint64{1, 1}
		require.NoError(t, exec.PreflightBatchFill(
			0, groups, []*vector.Vector{input}))
		used := account.Snapshot().Used
		var publicationErr error
		allocations := testing.AllocsPerRun(100, func() {
			mob := exec.state[0].mobs[0].(*bmp)
			mob.values = mob.values[:0]
			publicationErr = mergeAccountedBitmapWire(firstBytes, mob)
			if publicationErr == nil {
				publicationErr = mergeAccountedBitmapWire(secondBytes, mob)
			}
		})
		require.NoError(t, publicationErr)
		require.Zero(t, allocations,
			"accounted bitmap publication must not allocate on the Go heap")
		require.Equal(t, used, account.Snapshot().Used)
		require.Equal(t, 5002, len(exec.state[0].mobs[0].(*bmp).values))

		input.Free(mp)
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("bitmap-or-preflight-validates-full-wire", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newBitmapTestAllocation(t, 128<<20)
		exec := makeBmpOrExec(
			mp, AggIdOfBitmapOr, types.T_varbinary.ToType())
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		valid, err := roaring.BitmapOf(1, 2, 3).ToBytes()
		require.NoError(t, err)
		invalidWires := [][]byte{
			append([]byte(nil), valid[:len(valid)-1]...),
			append(append([]byte(nil), valid...), 0xff),
		}
		for _, invalid := range invalidWires {
			input := vector.NewVec(types.T_varbinary.ToType())
			require.NoError(t, vector.AppendBytes(input, invalid, false, mp))
			used := account.Snapshot().Used
			err = exec.PreflightBatchFill(
				0, []uint64{1}, []*vector.Vector{input})
			require.Error(t, err)
			require.Nil(t, exec.state[0].mobs[0])
			require.Equal(t, used, account.Snapshot().Used)
			input.Free(mp)
		}
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("one-byte-short-rejects-without-value-publication", func(t *testing.T) {
		measure := func(limit uint64) (uint64, int, error) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newBitmapTestAllocation(t, limit)
			exec := makeConstruct(t, mp, allocation, 1)
			input := testutil.NewUInt64Vector(
				4, types.T_uint64.ToType(), mp, false, nil,
				[]uint64{4, 3, 2, 1})
			groups := slices.Repeat([]uint64{1}, input.Length())
			err := AggFuncExec(exec).(BatchCapacityPreflight).PreflightBatchFill(
				0, groups, []*vector.Vector{input})
			published := 0
			if exec.state[0].mobs[0] != nil {
				published = len(exec.state[0].mobs[0].(*bmp).values)
			}
			peak := account.Snapshot().Peak
			input.Free(mp)
			exec.Free()
			require.NoError(t, exec.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
			return peak, published, err
		}
		peak, published, err := measure(128 << 20)
		require.NoError(t, err)
		require.Zero(t, published)
		_, published, err = measure(peak - 1)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Zero(t, published)
	})

	t.Run("spill-source-account-may-differ", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry1, account1, allocation1 := newBitmapTestAllocation(t, 128<<20)
		registry2, account2, allocation2 := newBitmapTestAllocation(t, 128<<20)
		target := makeConstruct(t, mp, allocation1, 1)
		source := makeConstruct(t, mp, allocation2, 1)
		left := testutil.NewUInt64Vector(
			2, types.T_uint64.ToType(), mp, false, nil, []uint64{1, 3})
		right := testutil.NewUInt64Vector(
			2, types.T_uint64.ToType(), mp, false, nil, []uint64{2, 3})
		require.NoError(t, target.BatchFill(
			0, []uint64{1, 1}, []*vector.Vector{left}))
		require.NoError(t, source.BatchFill(
			0, []uint64{1, 1}, []*vector.Vector{right}))
		require.NoError(t, target.PreflightBatchMerge(source, 0, []uint64{1}))
		peak := account1.Snapshot().Peak
		require.NoError(t, target.BatchMerge(source, 0, []uint64{1}))
		require.Equal(t, []uint32{1, 2, 3}, target.state[0].mobs[0].(*bmp).values)
		require.Equal(t, peak, account1.Snapshot().Peak)

		left.Free(mp)
		right.Free(mp)
		target.Free()
		source.Free()
		require.NoError(t, target.ClearAllocationAccount(allocation1))
		require.NoError(t, source.ClearAllocationAccount(allocation2))
		finishTestAggregateAllocation(t, registry1, account1)
		finishTestAggregateAllocation(t, registry2, account2)
		require.Zero(t, mp.CurrNB())
	})
}

func TestBitmapConstructExec(t *testing.T) {
	mp := mpool.MustNewZero()
	vec1, vec2 := buildTestBitmapVecs(t, mp)

	t.Run("BulkFill", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		exec.GetOptResult().modifyChunkSize(1)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec1}))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec2}))
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("BatchFill", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec1}))
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec2}))
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("MarshalNilState", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			save func(*bmpConstructExec, *bytes.Buffer) error
		}{
			{
				name: "chunk",
				save: func(exec *bmpConstructExec, buf *bytes.Buffer) error {
					return exec.SaveIntermediateResultOfChunk(0, buf)
				},
			},
			{
				name: "flags",
				save: func(exec *bmpConstructExec, buf *bytes.Buffer) error {
					return exec.SaveIntermediateResult(2, [][]uint8{{1, 1}}, buf)
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				curNB := mp.CurrNB()
				vec := testutil.NewUInt64Vector(2, types.T_uint64.ToType(), mp, false, []bool{false, true}, []uint64{42, 99})
				exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
				restored := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())

				require.NoError(t, exec.GroupGrow(2))
				require.NoError(t, exec.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec}))

				buf := bytes.NewBuffer(make([]byte, 0, common.MiB))
				require.NoError(t, tc.save(exec, buf))
				require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

				results, err := restored.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				checkBitmap(t, results[0], 0, []uint32{42})
				require.True(t, results[0].IsNull(1))

				vec.Free(mp)
				exec.Free()
				restored.Free()
				for _, result := range results {
					result.Free(mp)
				}
				require.Equal(t, curNB, mp.CurrNB())
			})
		}
	})

	t.Run("Merge", func(t *testing.T) {
		curNB := mp.CurrNB()
		execa1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa1.GetOptResult().modifyChunkSize(1)
		execa2.GetOptResult().modifyChunkSize(1)
		require.NoError(t, execa1.GroupGrow(1))
		require.NoError(t, execa2.GroupGrow(1))

		execb1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb1.GetOptResult().modifyChunkSize(1)
		execb1.GroupGrow(1)
		execb2.GetOptResult().modifyChunkSize(1)
		execb2.GroupGrow(1)

		require.NoError(t, execa1.BulkFill(0, []*vector.Vector{vec1}))
		require.NoError(t, execa2.BulkFill(0, []*vector.Vector{vec2}))

		buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
		buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

		err := execa1.SaveIntermediateResultOfChunk(0, buf1)
		require.NoError(t, err)
		err = execa2.SaveIntermediateResultOfChunk(0, buf2)
		require.NoError(t, err)

		r1 := bytes.NewReader(buf1.Bytes())
		r2 := bytes.NewReader(buf2.Bytes())

		err = execb1.UnmarshalFromReader(r1, mp)
		require.NoError(t, err)
		err = execb2.UnmarshalFromReader(r2, mp)
		require.NoError(t, err)

		execb1.Merge(execb2, 0, 0)
		results, err := execb1.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		execa1.Free()
		execa2.Free()
		execb1.Free()
		execb2.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("SaveIntermediateWithNilMobs", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, exec.GroupGrow(4))
		// Only fill groups 1 and 3, leaving groups 2 and 4 with nil mobs entries.
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec1}))
		require.NoError(t, exec.BatchFill(0, []uint64{3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, []*vector.Vector{vec2}))

		buf := bytes.NewBuffer(make([]byte, 0, common.MiB))
		require.NoError(t, exec.SaveIntermediateResultOfChunk(0, buf))

		execb := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		r := bytes.NewReader(buf.Bytes())
		require.NoError(t, execb.UnmarshalFromReader(r, mp))

		results, err := execb.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
		require.True(t, results[0].IsNull(1))
		checkBitmap(t, results[0], 2, []uint32{3, 4, 5, 6, 8, 9, 10, 11})
		require.True(t, results[0].IsNull(3))

		exec.Free()
		execb.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("BatchMerge", func(t *testing.T) {
		curNB := mp.CurrNB()
		execa1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execa2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		require.NoError(t, execa1.GroupGrow(1))
		require.NoError(t, execa2.GroupGrow(1))
		require.NoError(t, execa1.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec1}))
		require.NoError(t, execa2.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec2}))

		buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
		buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

		err := execa1.SaveIntermediateResult(1, [][]uint8{{1}}, buf1)
		require.NoError(t, err)
		err = execa2.SaveIntermediateResult(1, [][]uint8{{1}}, buf2)
		require.NoError(t, err)

		execb1 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
		execb2 := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())

		r1 := bytes.NewReader(buf1.Bytes())
		r2 := bytes.NewReader(buf2.Bytes())

		err = execb1.UnmarshalFromReader(r1, mp)
		require.NoError(t, err)
		err = execb2.UnmarshalFromReader(r2, mp)
		require.NoError(t, err)

		execb1.BatchMerge(execb2, 0, []uint64{1})
		results, err := execb1.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		checkBitmap(t, results[0], 0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})

		execa1.Free()
		execa2.Free()
		execb1.Free()
		execb2.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})
}

func TestBitmapConstructSaveIntermediateResultOfChunkMinimal(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.NewUInt64Vector(
		1,
		types.T_uint64.ToType(),
		mp,
		false,
		nil,
		[]uint64{42},
	)

	exec := makeBmpConstructExec(mp, AggIdOfBitmapConstruct, types.T_uint64.ToType())
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResultOfChunk(0, &buf))

	vec.Free(mp)
	exec.Free()
}

func TestAggStateMarshalerUnmarshalerErrorPaths(t *testing.T) {
	mp := mpool.MustNewZero()
	expectedErr := errors.New("expected marshaler error")
	info := aggInfo{
		makeMarshalerUnmarshaler: makeBmpMarshalerUnmarshaler,
	}

	t.Run("write flagged state", func(t *testing.T) {
		ag := aggState{
			length:   1,
			capacity: 1,
			mobs:     []MarshalerUnmarshaler{errMarshalerUnmarshaler{err: expectedErr}},
		}

		var buf bytes.Buffer
		err := ag.writeStateToBuf(mp, &info, []uint8{1}, &buf)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("write whole chunk", func(t *testing.T) {
		ag := aggState{
			length:   1,
			capacity: 1,
			mobs:     []MarshalerUnmarshaler{errMarshalerUnmarshaler{err: expectedErr}},
		}

		var buf bytes.Buffer
		err := ag.writeAllStatesToBuf(mp, &buf, &info)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("make marshaler while reading", func(t *testing.T) {
		info := aggInfo{
			makeMarshalerUnmarshaler: func(*mpool.MPool, *AllocationAccount) (MarshalerUnmarshaler, error) {
				return nil, expectedErr
			},
		}

		var buf bytes.Buffer
		require.NoError(t, types.WriteInt32(&buf, 1))
		require.NoError(t, types.WriteInt32(&buf, 1))
		require.NoError(t, buf.WriteByte(0))

		var ag aggState
		_, err := ag.readState(mp, &buf, &info)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("unmarshal marshaler while reading", func(t *testing.T) {
		info := aggInfo{
			makeMarshalerUnmarshaler: func(*mpool.MPool, *AllocationAccount) (MarshalerUnmarshaler, error) {
				return errMarshalerUnmarshaler{err: expectedErr}, nil
			},
		}

		var buf bytes.Buffer
		require.NoError(t, types.WriteInt32(&buf, 1))
		require.NoError(t, types.WriteInt32(&buf, 1))
		require.NoError(t, buf.WriteByte(0))

		var ag aggState
		_, err := ag.readState(mp, &buf, &info)
		require.ErrorIs(t, err, expectedErr)
	})
}
