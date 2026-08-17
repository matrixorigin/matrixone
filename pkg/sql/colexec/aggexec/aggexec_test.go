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

package aggexec

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMakeAggSpecialAgg(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	param := types.T_int64.ToType()

	exec, err := MakeAgg(mp, AggIdOfAvgTwCache, false, param)
	require.NoError(t, err)

	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, exec.PreAllocateGroups(4))
	require.NoError(t, exec.GroupGrow(2))
	exec.Free()
}

func TestAggExecPreAllocateAcrossChunkBoundaryKeepsPackedGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()

	exec := &aggExec{
		mp:        mp,
		chunkSize: AggBatchSize,
		aggInfo: aggInfo{
			stateTypes: []types.Type{types.T_int64.ToType()},
		},
	}
	defer exec.Free()

	require.NoError(t, exec.GroupGrow(AggBatchSize-1))
	require.NoError(t, exec.PreAllocateGroups(2))
	require.Len(t, exec.state, 1)
	require.Len(t, exec.standby, 1)
	require.Equal(t, int32(AggBatchSize-1), exec.state[0].length)

	require.NoError(t, exec.GroupGrow(2))
	require.Len(t, exec.state, 2)
	require.Empty(t, exec.standby)
	require.Equal(t, int32(AggBatchSize), exec.state[0].length)
	require.Equal(t, int32(1), exec.state[1].length)
	require.Equal(t, AggBatchSize+1, exec.GetNumGroups())
}

func TestAggExecPreAllocateFirstChunkReusesStandbySlice(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()

	exec := &aggExec{
		mp:        mp,
		chunkSize: AggBatchSize,
		aggInfo: aggInfo{
			stateTypes: []types.Type{types.T_int64.ToType()},
		},
	}
	defer exec.Free()

	require.NoError(t, exec.PreAllocateGroups(AggBatchSize))
	require.Empty(t, exec.state)
	require.Len(t, exec.standby, 1)
	standby := &exec.standby[0]

	require.NoError(t, exec.GroupGrow(1))
	require.Len(t, exec.state, 1)
	require.Empty(t, exec.standby)
	require.Same(t, standby, &exec.state[0])
	require.Equal(t, 1, exec.GetNumGroups())
}

func TestAggExecSaveIntermediateRejectsInvalidChunkBeforeWriting(t *testing.T) {
	exec := &aggExec{state: make([]aggState, 1)}
	for _, chunk := range []int{-1, 1} {
		var output bytes.Buffer
		require.Error(t, exec.SaveIntermediateResultOfChunk(chunk, &output))
		require.Zero(t, output.Len())
	}
}

func TestAggArgumentArenaGrowthFailureIsTransactional(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(128 << 10)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, 1, AllocationAccountSites{
		VectorData:     1,
		VectorArea:     2,
		VectorNulls:    3,
		VectorGrouping: 4,
		ArgumentCount:  5,
		ArgumentArena:  6,
	})
	require.NoError(t, err)

	exec := &aggExec{
		mp:         mp,
		chunkSize:  1,
		allocation: allocation,
		aggInfo: aggInfo{
			argTypes: []types.Type{types.T_varchar.ToType()},
			saveArg:  true,
		},
	}
	require.NoError(t, exec.GroupGrow(1))
	state := &exec.state[0]
	require.NoError(t, state.fillArg(mp, 0, []byte("kept"), false))
	require.Equal(t, uint32(1), state.argCnt[0])
	originalArena := &state.argbuf[0]

	err = state.fillArg(mp, 0, bytes.Repeat([]byte("x"), 32<<10), false)
	require.Error(t, err)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity), err)
	require.Equal(t, uint32(1), state.argCnt[0])
	require.Same(t, originalArena, &state.argbuf[0])
	seen := 0
	require.NoError(t, state.iter(0, func(key []byte) error {
		seen++
		require.Equal(t, []byte("kept"), aggPayloadFromKey(&exec.aggInfo, key))
		return nil
	}))
	require.Equal(t, 1, seen)

	exec.Free()
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

// TestVectorsUnmarshalFromReader exercises Vectors.UnmarshalFromReader via a
// median exec roundtrip.
func TestVectorsUnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()

	param := types.T_float64.ToType()
	exec, err := makeMedian(mp, 0, false, param)
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(2))

	v := vector.NewVec(param)
	require.NoError(t, vector.AppendFixed(v, float64(1), false, mp))
	require.NoError(t, vector.AppendFixed(v, float64(3), false, mp))
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v}))
	require.NoError(t, exec.Fill(1, 1, []*vector.Vector{v}))
	v.Free(mp)

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &buf))

	exec2, err := makeMedian(mp, 0, false, param)
	require.NoError(t, err)
	r := bytes.NewReader(buf.Bytes())
	require.NoError(t, exec2.UnmarshalFromReader(r, mp))
	require.Zero(t, r.Len())

	exec.Free()
	exec2.Free()
}

func TestAggExecUnmarshalReplacesExistingState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()
	newExec := func() *countColumnExec {
		return newCountColumnExec(
			mp,
			AggIdOfCountColumn,
			false,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
	}

	t.Run("empty", func(t *testing.T) {
		target := newExec()
		defer target.Free()
		require.NoError(t, target.GroupGrow(1))

		empty := newExec()
		var buf bytes.Buffer
		require.NoError(t, empty.SaveIntermediateResult(0, nil, &buf))
		empty.Free()
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
		require.Zero(t, target.GetNumGroups())
	})

	t.Run("multiple_chunks", func(t *testing.T) {
		target := newExec()
		defer target.Free()
		require.NoError(t, target.GroupGrow(2))

		source := newExec()
		defer source.Free()
		require.NoError(t, source.GroupGrow(AggBatchSize+1))
		flags := [][]uint8{make([]uint8, AggBatchSize), {1}}
		for i := range flags[0] {
			flags[0][i] = 1
		}
		var buf bytes.Buffer
		require.NoError(t, source.SaveIntermediateResult(AggBatchSize+1, flags, &buf))
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
		require.Equal(t, AggBatchSize+1, target.GetNumGroups())

		results, err := target.Flush()
		require.NoError(t, err)
		require.Len(t, results, 2)
		for _, result := range results {
			result.Free(mp)
		}
	})
}

func TestAggExecSpillDecodeUsesExactSourceCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()
	newExec := func() *countColumnExec {
		return newCountColumnExec(
			mp,
			AggIdOfCountColumn,
			false,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
	}

	source := newExec()
	require.NoError(t, source.GroupGrow(2))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveSpillIntermediateRows(
		0, []int32{0, 1}, &encoded))
	source.Free()

	target := newExec()
	require.NoError(t, target.UnmarshalSpillFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	require.Len(t, target.state, 1)
	require.Len(t, target.state[0].vecs, 1)
	require.Equal(t, 2, target.state[0].vecs[0].Capacity())

	// Exact-capacity spill state is still a valid immutable BatchMerge source;
	// source access must not require the destination's 8K array backing.
	destination := newExec()
	require.NoError(t, destination.GroupGrow(2))
	require.NoError(t, destination.BatchMerge(target, 0, []uint64{1, 2}))
	results, err := destination.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0}, vector.MustFixedColNoTypeCheck[int64](results[0]))
	results[0].Free(mp)
	destination.Free()
	target.Free()
}

func TestAggExecSpillDecodeReusesLargeSmallLargeCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	newExec := func() *countColumnExec {
		return newCountColumnExec(
			mp,
			AggIdOfCountColumn,
			false,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
	}
	encode := func(rows int) []byte {
		source := newExec()
		require.NoError(t, source.GroupGrow(rows))
		selected := make([]int32, rows)
		for i := range selected {
			selected[i] = int32(i)
		}
		var encoded bytes.Buffer
		require.NoError(t, source.SaveSpillIntermediateRows(
			0, selected, &encoded))
		source.Free()
		return encoded.Bytes()
	}

	large := encode(256)
	small := encode(1)
	target := newExec()
	require.NoError(t, target.UnmarshalSpillFromReader(bytes.NewReader(large), mp))
	require.Equal(t, int32(256), target.state[0].capacity)
	allocated := mp.CurrNB()
	require.Positive(t, allocated)

	require.NoError(t, target.UnmarshalSpillFromReader(bytes.NewReader(small), mp))
	require.Equal(t, int32(1), target.state[0].length)
	require.Equal(t, int32(256), target.state[0].capacity)
	require.Equal(t, allocated, mp.CurrNB())

	require.NoError(t, target.UnmarshalSpillFromReader(bytes.NewReader(large), mp))
	require.Equal(t, int32(256), target.state[0].length)
	require.Equal(t, int32(256), target.state[0].capacity)
	require.Equal(t, allocated, mp.CurrNB())
	target.Free()
}

func TestAggExecSpillRejectsSelectionBeyondStateRows(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		false,
		[]types.Type{types.T_int64.ToType()},
	).(*countColumnExec)
	require.NoError(t, exec.GroupGrow(1))

	var encoded bytes.Buffer
	err := exec.SaveSpillIntermediateRows(
		0, []int32{1}, &encoded)
	require.ErrorContains(t, err, "spill row 1 exceeds state row count 1")

	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestAggregateArgumentNodeSizeRejectsArenaOverflow(t *testing.T) {
	_, err := aggregateArgumentNodeSize(math.MaxUint32, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)

	size, err := aggregateArgumentNodeSize(1, 0)
	require.NoError(t, err)
	require.Positive(t, size)
}

func TestAggExecUnmarshalRejectsCorruptFramingWithoutPanic(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	newExec := func() *countColumnExec {
		return newCountColumnExec(
			mp,
			AggIdOfCountColumn,
			false,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
	}
	source := newExec()
	require.NoError(t, source.GroupGrow(1))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(
		1, [][]uint8{{1}}, &encoded))
	source.Free()

	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "header",
			mutate: func(data []byte) []byte {
				data[0] ^= 0xff
				return data
			},
			want: "magic number",
		},
		{
			name: "trailer",
			mutate: func(data []byte) []byte {
				data[len(data)-1] ^= 0xff
				return data
			},
			want: "magic number",
		},
		{
			name: "truncated",
			mutate: func(data []byte) []byte {
				return data[:len(data)-1]
			},
			want: "EOF",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.mutate(append([]byte(nil), encoded.Bytes()...))
			target := newExec()
			require.NoError(t, target.GroupGrow(1))
			err := target.UnmarshalFromReader(bytes.NewReader(payload), mp)
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, target.GetNumGroups())
			target.Free()
		})
	}
}

func TestIntermediateResultCompactsAndReusesSingleChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	info := aggInfo{stateTypes: []types.Type{types.T_int64.ToType()}}
	source := &aggExec{mp: mp, aggInfo: info, chunkSize: AggBatchSize}
	require.NoError(t, source.GroupGrow(2*AggBatchSize+2))

	flags := make([][]uint8, 3)
	flags[2] = []uint8{1, 1}
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(2, flags, &encoded))

	header := bytes.NewReader(encoded.Bytes())
	magic, err := types.ReadUint64(header)
	require.NoError(t, err)
	require.Equal(t, magicNumber, magic)
	chunks, err := types.ReadInt32(header)
	require.NoError(t, err)
	require.Equal(t, int32(1), chunks)

	target := &aggExec{mp: mp, aggInfo: info, chunkSize: AggBatchSize}
	require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
	require.Equal(t, 2, target.GetNumGroups())
	retained := mp.CurrNB()
	for range 100 {
		require.NoError(t, target.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
		require.Equal(t, 2, target.GetNumGroups())
		require.Equal(t, retained, mp.CurrNB())
	}

	source.Free()
	target.Free()
}

func TestAggStateReadRejectsNegativeCountBeforeReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	info := aggInfo{stateTypes: []types.Type{types.T_int64.ToType()}}
	state := aggState{}
	require.NoError(t, state.init(mp, 0, AggBatchSize, &info, false))

	var encoded bytes.Buffer
	types.WriteInt32(&encoded, -1)
	_, err := state.readState(mp, &encoded, &info)
	require.ErrorContains(t, err, "invalid count: -1")

	state.free(mp)
	require.Zero(t, mp.CurrNB())
}

type nonConsumingAggregateState struct{}

func (*nonConsumingAggregateState) MarshalBinary() ([]byte, error) { return nil, nil }
func (*nonConsumingAggregateState) UnmarshalBinary([]byte) error   { return nil }
func (*nonConsumingAggregateState) UnmarshalFromReader(io.Reader) error {
	return nil
}

func TestAggStateReadRejectsInconsistentAndTruncatedState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	t.Run("vector row count", func(t *testing.T) {
		info := aggInfo{stateTypes: []types.Type{types.T_int64.ToType()}}
		source := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(source, []int64{1, 2}, nil, mp))
		var encoded bytes.Buffer
		require.NoError(t, types.WriteInt32(&encoded, 1))
		require.NoError(t, source.MarshalBinaryTo(&encoded))
		source.Free(mp)

		var state aggState
		_, err := state.readState(mp, &encoded, &info)
		require.ErrorContains(t, err, "row count 2 does not match 1")
		state.free(mp)
	})

	newOpaqueInfo := func() aggInfo {
		return aggInfo{
			makeMarshalerUnmarshaler: func(*mpool.MPool, *AllocationAccount) (MarshalerUnmarshaler, error) {
				return &nonConsumingAggregateState{}, nil
			},
		}
	}
	for _, tc := range []struct {
		name    string
		size    int32
		payload []byte
		want    string
	}{
		{name: "negative opaque size", size: -1, want: "invalid aggregate opaque state size"},
		{name: "truncated opaque payload", size: 2, payload: []byte{1}, want: "unexpected EOF"},
		{name: "unconsumed opaque payload", size: 1, payload: []byte{1}, want: "did not consume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var encoded bytes.Buffer
			require.NoError(t, types.WriteInt32(&encoded, 1))
			require.NoError(t, types.WriteInt32(&encoded, tc.size))
			_, err := encoded.Write(tc.payload)
			require.NoError(t, err)
			var state aggState
			info := newOpaqueInfo()
			_, err = state.readState(mp, &encoded, &info)
			require.ErrorContains(t, err, tc.want)
			state.free(mp)
		})
	}
}

func BenchmarkIntermediateResultUnmarshal(b *testing.B) {
	mp := mpool.MustNewZero()
	info := aggInfo{stateTypes: []types.Type{types.T_int64.ToType()}}
	source := &aggExec{mp: mp, aggInfo: info, chunkSize: AggBatchSize}
	require.NoError(b, source.GroupGrow(256))
	flags := make([]uint8, 256)
	for i := range flags {
		flags[i] = 1
	}

	var compact bytes.Buffer
	require.NoError(b, source.SaveIntermediateResult(256, [][]uint8{flags}, &compact))

	// Q18 profiles showed roughly 977 state chunks with one selected chunk per
	// spill record. Reproduce the legacy wire shape exactly: 976 zero-length
	// chunks followed by the selected state.
	var legacy bytes.Buffer
	require.NoError(b, types.WriteUint64(&legacy, magicNumber))
	types.WriteInt32(&legacy, 977)
	for range 976 {
		types.WriteInt32(&legacy, 0)
	}
	require.NoError(b, source.state[0].writeStateToBuf(mp, &source.aggInfo, flags, &legacy))
	require.NoError(b, types.WriteUint64(&legacy, magicNumber))

	benchmarks := []struct {
		name    string
		encoded []byte
	}{
		{name: "legacy-977-chunks", encoded: legacy.Bytes()},
		{name: "compact-one-chunk", encoded: compact.Bytes()},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			target := &aggExec{mp: mp, aggInfo: info, chunkSize: AggBatchSize}
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalFromReader(bytes.NewReader(benchmark.encoded), mp); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(benchmark.encoded)), "bytes/record")
			target.Free()
		})
	}

	source.Free()
	require.Zero(b, mp.CurrNB())
}

func TestAggStateInitSaveArgCleanup(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	t.Run("normal_init_and_free", func(t *testing.T) {
		ag := &aggState{}
		info := &aggInfo{saveArg: true}
		err := ag.init(mp, 0, 100, info, false)
		require.NoError(t, err)
		require.NotNil(t, ag.argCnt)
		require.NotNil(t, ag.argbuf)
		require.NotNil(t, ag.argSkl)
		ag.free(mp)
		require.Nil(t, ag.argCnt)
		require.Nil(t, ag.argSkl)
	})

	t.Run("error_path_argCnt_cleanup", func(t *testing.T) {
		// Create a limited mpool and pre-fill it so MakeSlice succeeds
		// but the subsequent Alloc(16KB) fails — exercising the real fix path.
		limitedMp, err := mpool.NewMPool("limited", 1024*1024, mpool.NoFixed)
		require.NoError(t, err)

		// Pre-fill to leave only ~4KB free (16KB Alloc will fail)
		filler, err := limitedMp.Alloc(1024*1024-4*1024, true)
		require.NoError(t, err)

		ag := &aggState{}
		info := &aggInfo{saveArg: true}
		err = ag.init(limitedMp, 0, 100, info, false)
		require.Error(t, err, "Alloc should fail due to mpool capacity")
		require.Nil(t, ag.argCnt, "argCnt must be freed on Alloc failure")

		limitedMp.Free(filler)
		mpool.DeleteMPool(limitedMp)
	})

	t.Run("non_savearg_path", func(t *testing.T) {
		ag := &aggState{}
		info := &aggInfo{
			saveArg:    false,
			stateTypes: []types.Type{types.T_int64.ToType()},
			emptyNull:  true,
		}
		err := ag.init(mp, 0, 100, info, true)
		require.NoError(t, err)
		require.Nil(t, ag.argCnt)
		require.NotNil(t, ag.vecs)
		ag.free(mp)
	})
}

func TestGroupGrowReturnsPrepareParamSidecarOOM(t *testing.T) {
	const poolCapacity = int64(1024 * 1024)
	mp, err := mpool.NewMPool("group-grow-prepare-param", poolCapacity, mpool.NoFixed)
	require.NoError(t, err)

	input := vector.NewVec(types.T_int64.ToType())
	exec := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
	var filler []byte
	defer func() {
		if filler != nil {
			mp.Free(filler)
		}
		input.Free(mp)
		exec.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, vector.AppendFixedList(input, []int64{1, 2}, nil, mp))
	require.NoError(t, input.SetPrepareParamKindsWithMP([]vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamFloat,
	}, mp))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 2}, []*vector.Vector{input}))

	remaining := poolCapacity - mp.CurrNB()
	require.Greater(t, remaining, int64(1))
	filler, err = mp.Alloc(int(remaining-1), true)
	require.NoError(t, err)

	var growErr error
	require.NotPanics(t, func() {
		growErr = exec.GroupGrow(1)
	})
	require.Error(t, growErr)
	require.Equal(t, 2, exec.(*minMaxExecFixed[int64]).GetNumGroups())

	// Capacity-only work retained by the failed attempt is reusable; removing
	// the pressure lets the same logical grow complete exactly once.
	mp.Free(filler)
	filler = nil
	require.NoError(t, exec.GroupGrow(1))
	require.Equal(t, 3, exec.(*minMaxExecFixed[int64]).GetNumGroups())
}
