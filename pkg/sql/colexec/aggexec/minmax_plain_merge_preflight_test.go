// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func (f *fixedMetadataPreflightFixture) mergeSource(t testing.TB, values []int64, nulls []bool) *fixedMetadataPreflightFixture {
	t.Helper()
	source := newFixedMetadataPreflightFixture(t, f.exec.AggID(), types.T_int64.ToType())
	require.NoError(t, source.exec.GroupGrow(len(values)))
	groups := make([]uint64, len(values))
	for row := range groups {
		groups[row] = uint64(row + 1)
	}
	require.NoError(t, source.exec.BatchFill(0, groups, []*vector.Vector{source.intInput(t, values, nulls)}))
	return source
}

func TestFixedMinMaxPlainMergeMetadataPreflight(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		for _, mode := range []string{"active", "standby", "spill-source"} {
			t.Run(fmt.Sprint(id)+"/"+mode, func(t *testing.T) {
				f := newFixedMetadataPreflightFixture(t, id, types.T_int64.ToType())
				if mode == "standby" {
					require.NoError(t, f.exec.PreAllocateGroups(3))
				} else {
					require.NoError(t, f.exec.GroupGrow(3))
					require.NoError(t, f.exec.BatchFill(0, []uint64{1, 2, 3}, []*vector.Vector{
						f.intInput(t, []int64{8, -2, 0}, []bool{false, false, true}),
					}))
				}
				source := f.mergeSource(t, []int64{99, -5, -5, 7, 7, 0, 100},
					[]bool{false, false, false, false, false, true, false})
				if mode == "spill-source" {
					var encoded bytes.Buffer
					require.NoError(t, source.exec.(SpillStateCodec).SaveSpillIntermediateRows(
						0, []int32{0, 1, 2, 3, 4, 5, 6}, &encoded))
					source = newFixedMetadataPreflightFixture(t, id, types.T_int64.ToType())
					require.NoError(t, source.exec.(SpillStateCodec).UnmarshalSpillFromReader(&encoded, source.mp))
					require.Equal(t, 7, source.exec.GetNumGroups())
					require.Equal(t, 7, source.base.state[0].vecs[0].Length())
					// Accounted allocation may round physical capacity; the source stays compact.
					require.GreaterOrEqual(t, source.base.state[0].vecs[0].Capacity(), 7)
					require.Less(t, source.base.state[0].vecs[0].Capacity(), AggBatchSize)
				}
				groups := []uint64{1, 1, 2, 2, 3, GroupNotMatched}
				before, sourceBefore := snapshotFixedMetadataPreflight(f), snapshotFixedMetadataPreflight(source)
				account, calls := f.account.Snapshot(), f.controller.calls
				sourceAccount, sourceCalls := source.account.Snapshot(), source.controller.calls
				f.controller.failAt = calls + 1
				for range 2 {
					require.NoError(t, f.exec.PreflightBatchMerge(source.exec, 1, groups))
					require.Equal(t, before, snapshotFixedMetadataPreflight(f))
					require.Equal(t, sourceBefore, snapshotFixedMetadataPreflight(source))
				}
				if mode == "standby" {
					require.NoError(t, f.exec.GroupGrow(3))
				}
				require.NoError(t, f.exec.BatchMerge(source.exec, 1, groups))
				require.Equal(t, account, f.account.Snapshot())
				require.Equal(t, calls, f.controller.calls)
				require.False(t, f.controller.rejected)
				require.Equal(t, sourceBefore, snapshotFixedMetadataPreflight(source), "merge borrows its source")
				require.Equal(t, sourceAccount, source.account.Snapshot())
				require.Equal(t, sourceCalls, source.controller.calls)
				f.controller.failAt = 0
				result, err := f.exec.Flush()
				require.NoError(t, err)
				for _, v := range result {
					t.Cleanup(func() { v.Free(f.mp) })
				}
				require.Len(t, result, 1)
				want := []int64{-5, -2}
				if mode == "standby" {
					want = []int64{-5, 7}
				} else if id == AggIdOfMax {
					want = []int64{8, 7}
				}
				require.Equal(t, want, vector.MustFixedColNoTypeCheck[int64](result[0])[:2])
				for row := range 3 {
					require.Equal(t, row == 2, result[0].IsNull(uint64(row)))
				}
			})
		}
	}
}

func TestFixedMinMaxPlainMergePreflightBounds(t *testing.T) {
	f := newFixedMetadataPreflightFixture(t, AggIdOfMax, types.T_int64.ToType())
	require.NoError(t, f.exec.GroupGrow(1))
	source := f.mergeSource(t, []int64{3, 0}, []bool{false, true})
	require.NoError(t, source.exec.GroupGrow(hashmap.UnitLimit-1))
	groups := make([]uint64, hashmap.UnitLimit+1)
	for row := range groups {
		groups[row] = 1
	}
	before, sourceBefore := snapshotFixedMetadataPreflight(f), snapshotFixedMetadataPreflight(source)
	account, calls := f.account.Snapshot(), f.controller.calls
	f.controller.failAt = calls + 1
	for _, tc := range []struct {
		name   string
		offset int
		groups []uint64
		want   error
	}{
		{name: "empty"},
		{name: "256 rows", groups: groups[:hashmap.UnitLimit]},
		{name: "257 rows", groups: groups, want: mpool.ErrAllocationAccountInvalid},
		{name: "negative offset", offset: -1, groups: groups[:1], want: mpool.ErrAllocationAccountInvalid},
		{name: "source range", offset: hashmap.UnitLimit + 1, groups: groups[:1], want: mpool.ErrAllocationAccountInvalid},
		{name: "NULL ignores target", offset: 1, groups: []uint64{AggBatchSize + 1}},
		{name: "unmatched", groups: []uint64{GroupNotMatched}},
		{name: "present validates target", groups: []uint64{AggBatchSize + 1}, want: mpool.ErrAllocationAccountInvariant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := f.exec.PreflightBatchMerge(source.exec, tc.offset, tc.groups)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.Equal(t, before, snapshotFixedMetadataPreflight(f))
			require.Equal(t, sourceBefore, snapshotFixedMetadataPreflight(source))
		})
	}
	func() {
		original := source.base.state
		defer func() { source.base.state = original }()
		// Keep the declared total while making row 1 absent from its chunk.
		// Source bounds must be checked before its NULL bit or the target.
		source.base.state = []aggState{original[0], {length: hashmap.UnitLimit}}
		source.base.state[0].length = 1
		require.NoError(t, f.exec.PreflightBatchMerge(source.exec, 1, []uint64{GroupNotMatched}))
		require.ErrorIs(t, f.exec.PreflightBatchMerge(source.exec, 1, []uint64{AggBatchSize + 1}), mpool.ErrAllocationAccountInvariant)
	}()
	require.True(t, moerr.IsMoErrCode(f.exec.PreflightBatchMerge(nil, -1, groups), moerr.ErrInternal), "compatibility precedes offset and batch bounds")
	require.Equal(t, before, snapshotFixedMetadataPreflight(f))
	require.Equal(t, sourceBefore, snapshotFixedMetadataPreflight(source))
	require.Equal(t, account, f.account.Snapshot())
	require.Equal(t, calls, f.controller.calls)
	require.False(t, f.controller.rejected)
}

func TestFixedMinMaxMergePreflightTargetShape(t *testing.T) {
	for _, standby := range []bool{false, true} {
		t.Run(fmt.Sprint("standby=", standby), func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, AggIdOfMin, types.T_int64.ToType())
			if standby {
				require.NoError(t, f.exec.PreAllocateGroups(1))
			} else {
				require.NoError(t, f.exec.GroupGrow(1))
			}
			source := f.mergeSource(t, []int64{7, 0}, []bool{false, true})
			before := snapshotFixedMetadataPreflight(f)
			account, calls := f.account.Snapshot(), f.controller.calls
			func() {
				state := f.base.preflightStateAt(0)
				original := state.vecs
				defer func() { state.vecs = original }()
				state.vecs = []*vector.Vector{f.intInput(t, []int64{3, 4}, nil)}
				require.NoError(t, f.exec.PreflightBatchMerge(source.exec, 1, []uint64{1}), "NULL source does not inspect target shape")
				require.ErrorIs(t, f.exec.PreflightBatchMerge(source.exec, 0, []uint64{1}), mpool.ErrAllocationAccountInvalid)
			}()
			require.Equal(t, before, snapshotFixedMetadataPreflight(f))
			require.Equal(t, account, f.account.Snapshot())
			require.Equal(t, calls, f.controller.calls)
		})
	}
}

func BenchmarkFixedMinMaxPlainMergeMetadataPreflight(b *testing.B) {
	for _, tc := range []struct {
		name    string
		groups  int
		standby bool
	}{
		{name: "one-group", groups: 1},
		{name: "alternating-32", groups: 32},
		{name: "distinct-256", groups: hashmap.UnitLimit},
		{name: "standby", groups: 32, standby: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			f := newFixedMetadataPreflightFixture(b, AggIdOfMax, types.T_int64.ToType())
			values := make([]int64, hashmap.UnitLimit)
			groups := make([]uint64, len(values))
			for row := range values {
				values[row], groups[row] = int64(row), uint64(row%tc.groups+1)
			}
			source := f.mergeSource(b, values, nil)
			if tc.standby {
				require.NoError(b, f.exec.PreAllocateGroups(tc.groups))
			} else {
				require.NoError(b, f.exec.GroupGrow(tc.groups))
				require.NoError(b, f.exec.BatchMerge(source.exec, 0, groups))
			}
			account, calls := f.account.Snapshot(), f.controller.calls
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := f.exec.PreflightBatchMerge(source.exec, 0, groups); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			require.Equal(b, account, f.account.Snapshot())
			require.Equal(b, calls, f.controller.calls)
			if tc.standby {
				require.Zero(b, f.exec.GetNumGroups())
				require.NoError(b, f.exec.GroupGrow(tc.groups))
				require.NoError(b, f.exec.BatchMerge(source.exec, 0, groups))
			}
			v := f.base.state[0].vecs[0]
			for row := range tc.groups {
				want := int64(row + (hashmap.UnitLimit-1-row)/tc.groups*tc.groups)
				require.False(b, v.IsNull(uint64(row)))
				require.Equal(b, want, vector.GetFixedAtNoTypeCheck[int64](v, row))
			}
		})
	}
}
