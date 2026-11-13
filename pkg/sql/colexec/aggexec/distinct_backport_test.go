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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func backportCount(t *testing.T, mp *mpool.MPool, typ types.T, groups int, fixed bool) *countColumnExec {
	t.Helper()
	exec := newCountColumnExec(mp, 0, true, []types.Type{typ.ToType()}).(*countColumnExec)
	exec.fixedDistinct = fixed
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(groups))
	return exec
}

func TestFixedDistinctBackportChunkAndMerge(t *testing.T) {
	for _, targetFixed := range []bool{false, true} {
		for _, sourceFixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("target=%t/source=%t", targetFixed, sourceFixed), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				dst := backportCount(t, mp, types.T_int64, AggBatchSize+1, targetFixed)
				defer dst.Free()
				src := backportCount(t, mp, types.T_int64, AggBatchSize+1, sourceFixed)
				defer src.Free()
				vec := vector.NewVec(types.T_int64.ToType())
				defer vec.Free(mp)
				require.NoError(t, vector.AppendFixedList(vec, []int64{4, 4, 5, 8, 9}, nil, mp))
				// Two full group ids share local id zero in distinct state chunks.
				require.NoError(t, src.BatchFill(0, []uint64{1, 1, 1, AggBatchSize + 1, GroupNotMatched}, []*vector.Vector{vec}))
				require.NoError(t, dst.BatchFill(0, []uint64{1}, []*vector.Vector{vec}))
				require.NoError(t, dst.Merge(src, 0, 0))
				require.NoError(t, dst.Merge(src, AggBatchSize, AggBatchSize))
				require.NoError(t, dst.Merge(src, 0, 0))
				require.Equal(t, uint32(2), dst.state[0].argCnt[0])
				require.Equal(t, uint32(1), dst.state[1].argCnt[0])
				results, err := dst.Flush()
				require.NoError(t, err)
				require.Equal(t, int64(2), vector.MustFixedColNoTypeCheck[int64](results[0])[0])
				require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](results[1])[0])
				for _, v := range results {
					v.Free(mp)
				}
			})
		}
	}
}

func TestFixedDistinctBackportLegacyWire(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, fixed := range []bool{false, true} {
		src := backportCount(t, mp, types.T_int64, distinctFixedIndexMinGroups, fixed)
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []int64{9, 9, 10}, nil, mp))
		require.NoError(t, src.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{vec}))
		vec.Free(mp)
		var buf bytes.Buffer
		require.NoError(t, src.SaveIntermediateResultOfChunk(0, &buf))
		data := append([]byte(nil), buf.Bytes()...)
		for _, readerFixed := range []bool{false, true} {
			dst := backportCount(t, mp, types.T_int64, 0, readerFixed)
			require.NoError(t, dst.UnmarshalFromReader(bytes.NewReader(data), mp))
			require.Equal(t, uint32(2), dst.state[0].argCnt[0])
			require.Zero(t, dst.state[0].argCnt[1])
			if readerFixed {
				require.Equal(t, 8, dst.state[0].distinctKeyWidth)
			} else {
				require.Zero(t, dst.state[0].distinctKeyWidth)
			}
			dst.Free()
			dst.Free()
		}
		for _, cut := range []int{0, 1, 8, len(data) - 1, len(data) - 8} {
			dst := backportCount(t, mp, types.T_int64, 0, true)
			require.Error(t, dst.UnmarshalFromReader(bytes.NewReader(data[:cut]), mp))
			dst.Free()
		}
		src.Free()
	}
}

func TestDistinctBackportSignedZeroAndNaNPayload(t *testing.T) {
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		for _, fixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fixed=%t", typ, fixed), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				exec := backportCount(t, mp, typ, 1, fixed)
				defer exec.Free()
				vec := vector.NewVec(typ.ToType())
				defer vec.Free(mp)
				if typ == types.T_float32 {
					require.NoError(t, vector.AppendFixedList(vec, []float32{0, math.Float32frombits(0x80000000), math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002), math.Float32frombits(0x7fc00001)}, nil, mp))
				} else {
					require.NoError(t, vector.AppendFixedList(vec, []float64{0, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002), math.Float64frombits(0x7ff8000000000001)}, nil, mp))
				}
				require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1}, []*vector.Vector{vec}))
				require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1}, []*vector.Vector{vec}))
				require.Equal(t, uint32(3), exec.state[0].argCnt[0])
				// Input -0 has not been rewritten in place.
				if typ == types.T_float64 {
					require.True(t, math.Signbit(vector.MustFixedColNoTypeCheck[float64](vec)[1]))
				}
			})
		}
	}
}

func TestDistinctBackportReadsOldSignedZeroKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := backportCount(t, mp, types.T_float64, 0, true)
	defer exec.Free()
	var wire bytes.Buffer
	for _, v := range []any{uint64(magicNumber), int32(1), int32(1), uint32(2), uint64(0), uint64(0x8000000000000000), uint64(magicNumber)} {
		require.NoError(t, binary.Write(&wire, binary.LittleEndian, v))
	}
	require.NoError(t, exec.UnmarshalFromReader(&wire, mp))
	require.Equal(t, uint32(1), exec.state[0].argCnt[0])
}

func TestFixedDistinctIndexCollisionsAndFailureCleanup(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	index := distinctFixedIndex{groupLimit: 4}
	defer index.free(mp)
	var keys []uint64
	for k := uint64(0); len(keys) < 4; k++ {
		if distinctFixedHash(0, k)&15 == 0 {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		dup, err := index.prepare(mp, 0, k)
		require.NoError(t, err)
		require.False(t, dup)
		require.NoError(t, index.insert(0, k))
	}
	for _, k := range keys {
		require.True(t, index.lookup(0, k))
		require.False(t, index.lookup(1, k))
	}
	require.ErrorIs(t, index.ensureCapacityFor(mp, math.MaxUint32), mpool.ErrAllocationAllocatorLimit)
	// A corrupt full table must return rather than spin indefinitely.
	for i := range index.slotGroups {
		index.slotGroups[i] = 0
		index.slotKeys[i] = uint64(i)
	}
	require.ErrorIs(t, index.insert(1, 9999), mpool.ErrAllocationAccountInvariant)
}

func TestFixedDistinctIndexAllocationFailure(t *testing.T) {
	for _, limit := range []int64{100, 2100, 3000, 7000, 9000} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			mp, err := mpool.NewMPool("distinct-fail", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			filler, err := mp.Alloc((1<<20)-int(limit), true)
			require.NoError(t, err)
			index := distinctFixedIndex{groupLimit: 1024}
			err = index.ensureCapacityFor(mp, 1)
			require.Error(t, err)
			index.free(mp)
			index.free(mp)
			mp.Free(filler)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestDistinctBackportArenaCapacityFallback(t *testing.T) {
	mp, err := mpool.NewMPool("arena-fallback", (11 << 18), mpool.NoFixed)
	require.NoError(t, err)
	var state aggState
	defer func() { state.free(mp); require.Zero(t, mp.CurrNB()) }()
	buf, err := mp.Alloc(1<<20, true)
	require.NoError(t, err)
	state.argbuf = buf
	state.argSkl = arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare)
	key := bytes.Repeat([]byte("k"), 1<<20)
	require.NoError(t, state.insertArg(mp, key))
	require.Less(t, len(state.argbuf), 2<<20, "geometric request must fall back under the live-old-arena pool cap")
	it := state.argSkl.NewIter(nil, nil)
	defer it.Close()
	ok, got, _ := it.First()
	require.True(t, ok)
	require.Equal(t, key, got)
}

func TestFixedDistinctBackportPartialMergeFailure(t *testing.T) {
	mp, err := mpool.NewMPool("merge-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	dst := backportCount(t, mp, types.T_int64, 1, true)
	defer dst.Free()
	src := backportCount(t, mp, types.T_int64, 1, true)
	defer src.Free()
	vec := vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 178)
	for i := range values {
		values[i] = int64(i)
	}
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
	groups := make([]uint64, len(values))
	for i := range groups {
		groups[i] = 1
	}
	require.NoError(t, dst.BatchFill(0, groups, []*vector.Vector{vec}))
	vec.Free(mp)
	vec = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []int64{178, 179, 180}, nil, mp))
	require.NoError(t, src.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{vec}))
	vec.Free(mp)
	filler, err := mp.Alloc(int((1<<20)-mp.CurrNB()-1024), true)
	require.NoError(t, err)
	require.Error(t, dst.Merge(src, 0, 0))
	require.Equal(t, uint32(179), dst.state[0].argCnt[0])
	require.Equal(t, dst.state[0].argCnt[0], dst.state[0].distinctIndex.count)
	var seen int
	require.NoError(t, dst.state[0].iter(0, func([]byte) error { seen++; return nil }))
	require.Equal(t, 179, seen)
	mp.Free(filler)
	dst.Free()
	src.Free()
	require.Zero(t, mp.CurrNB())
}

func TestDistinctBackportSelfMergeAndRepack(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		mp := mpool.MustNewZero()
		exec := backportCount(t, mp, types.T_int64, AggBatchSize+1, fixed)
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []int64{1, 2, 3, 4}, nil, mp))
		require.NoError(t, exec.BatchFill(1, []uint64{1, 1, AggBatchSize + 1}, []*vector.Vector{vec}))
		vec.Free(mp)
		require.NoError(t, exec.Merge(exec, 1, 0))
		require.Equal(t, uint32(2), exec.state[0].argCnt[1])
		require.NoError(t, exec.Merge(exec, 1, 1))
		require.Equal(t, uint32(2), exec.state[0].argCnt[1])
		var wire bytes.Buffer
		require.NoError(t, exec.SaveIntermediateResult(3, [][]uint8{{1, 1}, {1}}, &wire))
		dst := backportCount(t, mp, types.T_int64, 0, !fixed)
		require.NoError(t, dst.UnmarshalFromReader(&wire, mp))
		require.Len(t, dst.state, 1)
		require.Equal(t, []uint32{2, 2, 1}, dst.state[0].argCnt[:3])
		exec.Free()
		dst.Free()
		require.Zero(t, mp.CurrNB())
	}
}

func TestDistinctBackportCompositeZeroAndNull(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := newCountColumnExec(mp, 0, true, []types.Type{types.T_float64.ToType(), types.T_int64.ToType()}).(*countColumnExec)
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(2))
	a := vector.NewVec(types.T_float64.ToType())
	defer a.Free(mp)
	b := vector.NewVec(types.T_int64.ToType())
	defer b.Free(mp)
	require.NoError(t, vector.AppendFixedList(a, []float64{0, math.Copysign(0, -1), 0, 1}, nil, mp))
	require.NoError(t, vector.AppendFixedList(b, []int64{1, 1, 2, 4}, nil, mp))
	a.SetNull(3)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{a, b}))
	require.Equal(t, []uint32{1, 1}, exec.state[0].argCnt[:2])
	require.Zero(t, exec.state[0].distinctKeyWidth)
}

func TestDistinctBackportReusesFreedExecutor(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	exec := backportCount(t, mp, types.T_int64, 1, true)
	defer exec.Free()
	for generation := 0; generation < 2; generation++ {
		if generation > 0 {
			require.NoError(t, exec.GroupGrow(1))
		}
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []int64{1, 1, 2}, nil, mp))
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{vec}))
		vec.Free(mp)
		result, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, int64(2), vector.MustFixedColNoTypeCheck[int64](result[0])[0])
		for _, v := range result {
			v.Free(mp)
		}
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}
}
