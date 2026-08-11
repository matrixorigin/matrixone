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
