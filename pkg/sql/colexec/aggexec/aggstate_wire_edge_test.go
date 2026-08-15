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
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAccountedAggregateStableAndSpillWireRejectTruncation(t *testing.T) {
	long := "accounted-aggregate-varlen-payload-that-does-not-fit-inline"
	tests := []struct {
		name     string
		id       int64
		distinct bool
		param    types.Type
		build    func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "distinct-fixed-argument", id: AggIdOfSum, distinct: true,
			param: types.T_int64.ToType(),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 5})
			},
		},
		{
			name: "saved-varlen-argument", id: AggIdOfAny,
			param: types.T_varchar.ToType(),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"})
			},
		},
		{
			name: "opaque-hll-state", id: AggIdOfHllAdd,
			param: types.T_int64.ToType(),
			build: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				return buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 8})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			makeExec := func() GroupAggFuncExec {
				exec, err := MakeGroupAgg(
					mp, tc.id, tc.distinct, allocation, nil, tc.param)
				require.NoError(t, err)
				SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
				return exec
			}
			source := makeExec()
			require.NoError(t, source.GroupGrow(2))
			input := tc.build(t, mp)
			groups := []uint64{1, 1, 2, 2}
			require.NoError(t, source.PreflightBatchFill(0, groups, []*vector.Vector{input}))
			require.NoError(t, source.BatchFill(0, groups, []*vector.Vector{input}))

			var stable bytes.Buffer
			require.NoError(t, source.SaveIntermediateResult(
				2, [][]uint8{{1, 1}}, &stable))
			var spill bytes.Buffer
			require.NoError(t, source.SaveSpillIntermediateResult(
				2, 0, []uint8{1, 1}, &spill))
			baseline := account.Snapshot().Used

			assertTruncations := func(
				name string,
				payload []byte,
				decode func(GroupAggFuncExec, []byte) error,
			) {
				t.Helper()
				// Every byte of the compact encodings is checked. Large opaque
				// payloads need only structural boundaries plus the final byte;
				// all bytes in their body share one io.ReadFull failure edge.
				cuts := make([]int, 0, min(len(payload), 512)+1)
				for cut := 0; cut < min(len(payload), 512); cut++ {
					cuts = append(cuts, cut)
				}
				if len(payload) > 512 {
					cuts = append(cuts, len(payload)-1)
				}
				for _, cut := range cuts {
					target := makeExec()
					require.Error(t, decode(target, payload[:cut]),
						"%s cut=%d", name, cut)
					target.Free()
					require.NoError(t, target.ClearAllocationAccount(allocation))
					require.Equal(t, baseline, account.Snapshot().Used,
						"%s cut=%d", name, cut)
				}
			}
			assertTruncations("stable", stable.Bytes(), func(target GroupAggFuncExec, payload []byte) error {
				return target.UnmarshalFromReader(bytes.NewReader(payload), mp)
			})
			assertTruncations("spill", spill.Bytes(), func(target GroupAggFuncExec, payload []byte) error {
				return target.UnmarshalSpillFromReader(bytes.NewReader(payload), mp)
			})

			input.Free(mp)
			source.Free()
			require.NoError(t, source.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedAggregateWireFamiliesRejectTruncation(t *testing.T) {
	long := "aggregate-family-wire-payload-beyond-inline-storage"
	jsonValue := func(t *testing.T, value any) []byte {
		t.Helper()
		bj, err := bytejson.CreateByteJSONWithCheck(value)
		require.NoError(t, err)
		encoded, err := bj.Marshal()
		require.NoError(t, err)
		return encoded
	}
	tests := []struct {
		name   string
		id     int64
		extra  any
		params []types.Type
		build  func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name: "min-varlen", id: AggIdOfMin, params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-d", long + "-a", long + "-c", long + "-b"})}
			},
		},
		{
			name: "max-by", id: AggIdOfMaxBy,
			params: []types.Type{types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{
					buildVarlenVec(t, mp, types.T_varchar.ToType(),
						[]string{long + "-a", long + "-b", long + "-c", long + "-d"}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 1, 1}),
				}
			},
		},
		{
			name: "group-concat", id: AggIdOfGroupConcat,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{"a", "b", "c", "d"})}
			},
		},
		{
			name: "bitmap", id: AggIdOfBitmapConstruct,
			params: []types.Type{types.T_uint64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_uint64.ToType(), []uint64{1, 2, 3, 4})}
			},
		},
		{
			name: "json-array", id: AggIdOfJsonArrayAgg,
			params: []types.Type{types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := vector.NewVec(types.T_json.ToType())
				for _, value := range []any{long, int64(2), true, nil} {
					require.NoError(t, vector.AppendBytes(vec, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{vec}
			},
		},
		{
			name: "median-decimal", id: AggIdOfMedian,
			params: []types.Type{types.New(types.T_decimal64, 10, 2)},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.New(types.T_decimal64, 10, 2),
					mustDecimal64s(t, "1.00", "2.00", "3.00", "4.00"))}
			},
		},
		{
			name: "approx-percentile", id: AggIdOfApproxPercentile, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{1, 2, 3, 4})}
			},
		},
		{
			name: "approx-count", id: AggIdOfApproxCount,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{1, 2, 3, 4})}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			makeExec := func() GroupAggFuncExec {
				exec, err := MakeGroupAgg(mp, tc.id, false, allocation, tc.extra, tc.params...)
				require.NoError(t, err)
				SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
				return exec
			}
			source := makeExec()
			require.NoError(t, source.GroupGrow(2))
			vectors := tc.build(t, mp)
			groups := []uint64{1, 1, 2, 2}
			require.NoError(t, source.PreflightBatchFill(0, groups, vectors))
			require.NoError(t, source.BatchFill(0, groups, vectors))
			var encoded bytes.Buffer
			require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))
			baseline := account.Snapshot().Used
			payload := encoded.Bytes()
			for cut := 0; cut < len(payload); cut++ {
				target := makeExec()
				require.Error(t, target.UnmarshalFromReader(bytes.NewReader(payload[:cut]), mp),
					"cut=%d", cut)
				target.Free()
				require.NoError(t, target.ClearAllocationAccount(allocation))
				require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
			}
			// Corrupt each compact header window as a hostile length/count value.
			// Decoders may reject the frame or accept an unrelated but valid value;
			// either outcome must leave the shared account unchanged after cleanup.
			for offset := 0; offset < min(len(payload), 256); offset++ {
				corrupt := bytes.Clone(payload)
				for i := offset; i < min(offset+8, len(corrupt)); i++ {
					corrupt[i] = 0xff
				}
				target := makeExec()
				_ = target.UnmarshalFromReader(bytes.NewReader(corrupt), mp)
				target.Free()
				require.NoError(t, target.ClearAllocationAccount(allocation))
				require.Equal(t, baseline, account.Snapshot().Used,
					"corrupt offset=%d", offset)
			}
			for _, vec := range vectors {
				vec.Free(mp)
			}
			source.Free()
			require.NoError(t, source.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedAggregateWireWriterFailuresAndPolicyEdges(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeGroupAgg(
		mp, AggIdOfSum, true, allocation, nil, types.T_int64.ToType())
	require.NoError(t, err)
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	require.NoError(t, exec.GroupGrow(2))
	input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 5})
	groups := []uint64{1, 1, 2, 2}
	require.NoError(t, exec.PreflightBatchFill(0, groups, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))

	var stable bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &stable))
	var chunk bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResultOfChunk(0, &chunk))
	var spill bytes.Buffer
	require.NoError(t, exec.SaveSpillIntermediateResult(2, 0, []uint8{1, 1}, &spill))
	baseline := account.Snapshot().Used

	writes := []struct {
		name    string
		payload []byte
		write   func(io.Writer) error
	}{
		{name: "stable", payload: stable.Bytes(), write: func(w io.Writer) error {
			return exec.SaveIntermediateResult(2, [][]uint8{{1, 1}}, w)
		}},
		{name: "chunk", payload: chunk.Bytes(), write: func(w io.Writer) error {
			return exec.SaveIntermediateResultOfChunk(0, w)
		}},
		{name: "spill", payload: spill.Bytes(), write: func(w io.Writer) error {
			return exec.SaveSpillIntermediateResult(2, 0, []uint8{1, 1}, w)
		}},
	}
	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			for cut := 0; cut < len(tc.payload); cut++ {
				require.Error(t, tc.write(&medianFailAfterWriter{remaining: cut}),
					"cut=%d", cut)
				require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
			}
		})
	}

	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1, 1}}, io.Discard))
	require.Error(t, exec.SaveIntermediateResult(1, [][]uint8{{1}, {1}}, io.Discard))
	require.Error(t, exec.SaveIntermediateResultOfChunk(-1, io.Discard))
	require.Error(t, exec.SaveIntermediateResultOfChunk(2, io.Discard))
	require.Error(t, exec.SaveSpillIntermediateResult(1, -1, []uint8{1}, io.Discard))
	require.Error(t, exec.SaveSpillIntermediateResult(1, 0, []uint8{1, 1, 1}, io.Discard))
	require.Error(t, exec.UnmarshalFromReader(nil, mp))
	require.Error(t, exec.UnmarshalSpillFromReader(nil, mp))
	require.Equal(t, 2, exec.GetNumGroups())
	require.Nil(t, exec.PrepareParamKindVectorForChunk(-1))
	require.Nil(t, exec.PrepareParamKindVectorForChunk(2))

	input.Free(mp)
	exec.Free()
	require.NoError(t, exec.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
