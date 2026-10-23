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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type fixedMetadataPreflightFixture struct {
	mp         *mpool.MPool
	account    *mpool.AllocationAccount
	controller *rejectNthAggregateAllocation
	exec       GroupAggFuncExec
	base       *aggExec
}

func newFixedMetadataPreflightFixture(t testing.TB, id int64, typ types.Type) *fixedMetadataPreflightFixture {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	controller := &rejectNthAggregateAllocation{}
	account, err := registry.OpenWithController(128<<20, controller)
	require.NoError(t, err)
	t.Cleanup(func() {
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, controller.used)
	})
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3,
		VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
	})
	require.NoError(t, err)
	exec, err := MakeGroupAgg(mp, id, false, allocation, nil, typ)
	require.NoError(t, err)
	t.Cleanup(func() {
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
	})
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	return &fixedMetadataPreflightFixture{
		mp: mp, account: account, controller: controller, exec: exec,
		base: exec.(aggregateBaseCarrier).aggregateBase(),
	}
}

func (f *fixedMetadataPreflightFixture) intInput(t testing.TB, values []int64, nulls []bool) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { v.Free(f.mp) })
	require.NoError(t, vector.AppendFixedList(v, values, nulls, f.mp))
	return v
}

type fixedMetadataPreflightSnapshot struct {
	groups int
	vecs   []fixedMetadataPreflightVectorSnapshot
}

type fixedMetadataPreflightVectorSnapshot struct {
	length, capacity, kindCapacity, sourceCapacity int
	values                                         []int64
	nulls                                          []bool
	rowKinds                                       []vector.PrepareParamKind
	rowSources                                     []types.StringSource
	kind                                           vector.PrepareParamKind
	seen, kindSidecar, sourceSidecar               bool
	kinds                                          []vector.PrepareParamKind
	source                                         types.StringSource
	sources                                        []types.StringSource
}

func snapshotFixedMetadataPreflight(f *fixedMetadataPreflightFixture) fixedMetadataPreflightSnapshot {
	result := fixedMetadataPreflightSnapshot{groups: f.exec.GetNumGroups()}
	for _, states := range [][]aggState{f.base.state, f.base.standby} {
		for i := range states {
			v := states[i].vecs[0]
			s := fixedMetadataPreflightVectorSnapshot{
				length: v.Length(), capacity: v.Capacity(),
				kindCapacity: cap(v.GetPrepareParamKinds()), sourceCapacity: cap(v.GetStringSources()),
				values: append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](v)...),
				kind:   v.GetPrepareParamKind(), seen: v.HasPrepareParamKind(),
				kindSidecar: v.GetPrepareParamKinds() != nil, sourceSidecar: v.GetStringSources() != nil,
				kinds:  append([]vector.PrepareParamKind(nil), v.GetPrepareParamKinds()...),
				source: v.GetStringSource(), sources: append([]types.StringSource(nil), v.GetStringSources()...),
			}
			for row := 0; row < v.Length(); row++ {
				s.nulls = append(s.nulls, v.IsNull(uint64(row)))
				s.rowKinds = append(s.rowKinds, v.GetPrepareParamKindAt(row))
				s.rowSources = append(s.rowSources, v.GetStringSourceAt(row))
			}
			result.vecs = append(result.vecs, s)
		}
	}
	return result
}

func requireFixedPreflightRowsUnchanged(t testing.TB, before, after fixedMetadataPreflightSnapshot) {
	t.Helper()
	require.Equal(t, before.groups, after.groups)
	require.Len(t, after.vecs, len(before.vecs))
	for i, v := range before.vecs {
		require.Equal(t, v.length, after.vecs[i].length)
		require.Equal(t, v.values, after.vecs[i].values)
		require.Equal(t, v.nulls, after.vecs[i].nulls)
		require.Equal(t, v.rowKinds, after.vecs[i].rowKinds)
		require.Equal(t, v.rowSources, after.vecs[i].rowSources)
	}
}

func TestFixedMinMaxPlainMetadataPreflight(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, id, types.T_int64.ToType())
			require.NoError(t, f.exec.GroupGrow(3))
			seed := f.intInput(t, []int64{8, -2, 0}, []bool{false, false, true})
			require.NoError(t, f.exec.BatchFill(0, []uint64{1, 2, 3}, []*vector.Vector{seed}))
			target := f.base.state[0].vecs[0]
			require.True(t, target.HasPrepareParamKind(), "ordinary fill observes None")
			require.Equal(t, vector.PrepareParamNone, target.GetPrepareParamKind())
			require.Nil(t, target.GetPrepareParamKinds())
			require.False(t, target.HasStringSourceMetadata())
			input := f.intInput(t, []int64{99, -5, 7, 7, 0, 100}, []bool{false, false, false, false, true, false})
			input.SetPrepareParamKind(vector.PrepareParamNone)
			groups := []uint64{1, 2, 2, AggBatchSize + 1, GroupNotMatched}
			before := snapshotFixedMetadataPreflight(f)
			used, calls := f.account.Snapshot().Used, f.controller.calls
			f.controller.failAt = calls + 1
			for range 2 {
				require.NoError(t, f.exec.PreflightBatchFill(1, groups, []*vector.Vector{input}))
				require.Equal(t, before, snapshotFixedMetadataPreflight(f))
			}
			require.NoError(t, f.exec.BatchFill(1, groups, []*vector.Vector{input}))
			require.Equal(t, calls, f.controller.calls)
			require.False(t, f.controller.rejected)
			require.Equal(t, used, f.account.Snapshot().Used)
			f.controller.failAt = 0
			result, err := f.exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			t.Cleanup(func() { result[0].Free(f.mp) })
			want := []int64{-5, -2}
			if id == AggIdOfMax {
				want = []int64{8, 7}
			}
			require.Equal(t, want, vector.MustFixedColNoTypeCheck[int64](result[0])[:2])
			require.True(t, result[0].IsNull(2))
			for row := range 3 {
				require.Equal(t, vector.PrepareParamNone, result[0].GetPrepareParamKindAt(row))
				require.Equal(t, types.StringSourceExpression, result[0].GetStringSourceAt(row))
			}
		})
	}
}

func TestFixedMinMaxPlainMetadataPreflightStandby(t *testing.T) {
	f := newFixedMetadataPreflightFixture(t, AggIdOfMax, types.T_int64.ToType())
	require.NoError(t, f.exec.PreAllocateGroups(2))
	input := f.intInput(t, []int64{3, 0, 9}, []bool{false, true, false})
	groups := []uint64{1, AggBatchSize + 1, 2}
	before := snapshotFixedMetadataPreflight(f)
	used, calls := f.account.Snapshot().Used, f.controller.calls
	f.controller.failAt = calls + 1
	require.NoError(t, f.exec.PreflightBatchFill(0, groups, []*vector.Vector{input}))
	require.Equal(t, before, snapshotFixedMetadataPreflight(f))
	require.ErrorIs(t, f.exec.PreflightBatchFill(0, []uint64{1, 2, AggBatchSize + 1}, []*vector.Vector{input}),
		mpool.ErrAllocationAccountInvariant)
	require.Equal(t, before, snapshotFixedMetadataPreflight(f))
	require.ErrorIs(t, f.exec.PreflightBatchFill(-1, groups, []*vector.Vector{input}), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, f.exec.PreflightBatchFill(1, groups, []*vector.Vector{input}), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, f.exec.PreflightBatchFill(0, nil, nil), mpool.ErrAllocationAccountInvalid)
	require.Equal(t, calls, f.controller.calls)
	require.Equal(t, used, f.account.Snapshot().Used)
	f.controller.failAt = 0
	require.NoError(t, f.exec.GroupGrow(2))
	used, calls = f.account.Snapshot().Used, f.controller.calls
	f.controller.failAt = calls + 1
	require.NoError(t, f.exec.BatchFill(0, groups, []*vector.Vector{input}))
	require.Equal(t, calls, f.controller.calls)
	require.Equal(t, used, f.account.Snapshot().Used)
	f.controller.failAt = 0
	result, err := f.exec.Flush()
	require.NoError(t, err)
	require.Len(t, result, 1)
	t.Cleanup(func() { result[0].Free(f.mp) })
	require.Equal(t, []int64{3, 9}, vector.MustFixedColNoTypeCheck[int64](result[0]))
	for row := range 2 {
		require.False(t, result[0].IsNull(uint64(row)))
		require.Equal(t, vector.PrepareParamNone, result[0].GetPrepareParamKindAt(row))
	}
}

func TestFixedMinMaxTargetShapePreflight(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, id, types.T_int64.ToType())
			require.NoError(t, f.exec.GroupGrow(1))
			input := f.intInput(t, []int64{7}, nil)
			nullInput := f.intInput(t, []int64{0}, []bool{true})
			ahead := f.intInput(t, []int64{3, 4}, nil)
			for _, test := range []struct {
				name string
				vecs []*vector.Vector
				want error
			}{
				{"nil vector list", nil, mpool.ErrAllocationAccountInvariant},
				{"empty vector list", []*vector.Vector{}, mpool.ErrAllocationAccountInvariant},
				{"nil first vector", []*vector.Vector{nil}, mpool.ErrAllocationAccountInvalid},
				{"vector length ahead of state", []*vector.Vector{ahead}, mpool.ErrAllocationAccountInvalid},
			} {
				t.Run(test.name, func(t *testing.T) {
					before := snapshotFixedMetadataPreflight(f)
					used, calls := f.account.Snapshot().Used, f.controller.calls
					func() {
						state := &f.base.state[0]
						original := state.vecs
						state.vecs = test.vecs
						// Restore before assertions can reach fixture cleanup or a
						// snapshot, including FailNow/panic from a preflight call.
						defer func() { state.vecs = original }()
						require.NoError(t, f.exec.PreflightBatchFill(0, []uint64{1}, []*vector.Vector{nullInput}), "NULL candidates do not inspect target shape")
						require.NoError(t, f.exec.PreflightBatchFill(0, []uint64{GroupNotMatched}, []*vector.Vector{input}), "unmatched groups do not inspect target shape")
						require.ErrorIs(t, f.exec.PreflightBatchFill(0, []uint64{1}, []*vector.Vector{input}), test.want)
					}()
					require.Equal(t, before, snapshotFixedMetadataPreflight(f))
					require.Equal(t, calls, f.controller.calls)
					require.Equal(t, used, f.account.Snapshot().Used)
				})
			}
		})
	}
}

func TestFixedMinMaxTargetMetadataPreflight(t *testing.T) {
	for _, kind := range []string{"kind", "kind-second-chunk", "empty-kind-sidecar", "source"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, AggIdOfMin, types.T_int64.ToType())
			input := f.intInput(t, []int64{4, 4}, nil)
			groups := []uint64{1}
			future := 0
			if kind == "empty-kind-sidecar" {
				future = 128
				require.NoError(t, f.exec.PreAllocateGroups(future))
				v := f.base.standby[0].vecs[0]
				require.NoError(t, v.PreflightSetPrepareParamKindsAtLength([]int{0, 1},
					[]vector.PrepareParamKind{vector.PrepareParamInteger, vector.PrepareParamFloat}, 2, f.mp))
				require.NotNil(t, v.GetPrepareParamKinds())
				require.Empty(t, v.GetPrepareParamKinds())
				groups = []uint64{1, uint64(future)}
			} else {
				count := 2
				first := uint64(1)
				if kind == "kind-second-chunk" {
					count = AggBatchSize + 2
					first = AggBatchSize + 1
				}
				require.NoError(t, f.exec.GroupGrow(count))
				if first > 1 {
					plain := f.intInput(t, []int64{8}, nil)
					require.NoError(t, f.exec.BatchFill(0, []uint64{1}, []*vector.Vector{plain}))
					groups = []uint64{1, first}
				}
				seed := f.intInput(t, []int64{5, 5}, nil)
				if kind == "source" {
					require.NoError(t, seed.SetStringSource(types.StringSourceLiteral))
				} else {
					seed.SetPrepareParamKind(vector.PrepareParamFloat)
				}
				require.NoError(t, f.exec.BatchFill(0, []uint64{first, first + 1}, []*vector.Vector{seed}))
			}
			before := snapshotFixedMetadataPreflight(f)
			used := f.account.Snapshot().Used
			f.controller.failAt = f.controller.calls + 1
			require.ErrorIs(t, f.exec.PreflightBatchFill(0, groups, []*vector.Vector{input}), mpool.ErrAllocationAccountCapacity)
			require.True(t, f.controller.rejected)
			require.Equal(t, before, snapshotFixedMetadataPreflight(f), "failed admission cannot publish")
			require.Equal(t, used, f.account.Snapshot().Used)
			f.controller.failAt, f.controller.rejected = 0, false
			require.NoError(t, f.exec.PreflightBatchFill(0, groups, []*vector.Vector{input}))
			requireFixedPreflightRowsUnchanged(t, before, snapshotFixedMetadataPreflight(f))
			if future > 0 {
				require.NoError(t, f.exec.GroupGrow(future))
			}
			admitted, calls := f.account.Snapshot().Used, f.controller.calls
			f.controller.failAt = calls + 1
			require.NoError(t, f.exec.BatchFill(0, groups, []*vector.Vector{input}))
			require.Equal(t, calls, f.controller.calls)
			require.False(t, f.controller.rejected)
			require.LessOrEqual(t, f.account.Snapshot().Used, admitted)
			for _, group := range groups {
				x, y := f.base.getXY(group - 1)
				v := f.base.state[x].vecs[0]
				require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](v, int(y)))
				require.False(t, v.IsNull(uint64(y)))
				require.Equal(t, vector.PrepareParamNone, v.GetPrepareParamKindAt(int(y)))
				require.Equal(t, types.StringSourceExpression, v.GetStringSourceAt(int(y)))
			}
			if future == 0 {
				last := f.base.state[len(f.base.state)-1].vecs[0]
				require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](last, 1))
				if kind == "source" {
					require.Equal(t, types.StringSourceLiteral, last.GetStringSourceAt(1))
				} else {
					require.Equal(t, vector.PrepareParamFloat, last.GetPrepareParamKindAt(1))
				}
			}
			for x := range f.base.state {
				v := f.base.state[x].vecs[0]
				for row := 0; row < v.Length(); row++ {
					group := uint64(x*AggBatchSize + row + 1)
					wantNull := true
					for _, changed := range groups {
						wantNull = wantNull && group != changed
					}
					if future == 0 && row == 1 && x == len(f.base.state)-1 {
						wantNull = false // the untouched seeded representative
					}
					require.Equal(t, wantNull, v.IsNull(uint64(row)), "group %d", group)
				}
			}
		})
	}
}

func TestFixedMinMaxPlainConstPreflight(t *testing.T) {
	f := newFixedMetadataPreflightFixture(t, AggIdOfMax, types.T_int64.ToType())
	require.NoError(t, f.exec.GroupGrow(2))
	input, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, f.mp)
	require.NoError(t, err)
	t.Cleanup(func() { input.Free(f.mp) })
	groups := make([]uint64, hashmap.UnitLimit)
	for row := range groups {
		groups[row] = uint64(row%2 + 1)
	}
	used := f.account.Snapshot().Used
	require.NoError(t, f.exec.PreflightBatchFill(3, groups, []*vector.Vector{input}), "ordinary const broadcasts")
	require.NoError(t, f.exec.BatchFill(3, groups, []*vector.Vector{input}))
	require.Equal(t, used, f.account.Snapshot().Used)
	result, err := f.exec.Flush()
	require.NoError(t, err)
	require.Len(t, result, 1)
	t.Cleanup(func() { result[0].Free(f.mp) })
	require.Equal(t, []int64{7, 7}, vector.MustFixedColNoTypeCheck[int64](result[0]))
}

func BenchmarkFixedMinMaxPlainMetadataPreflight(b *testing.B) {
	for _, tc := range []struct {
		name                           string
		groups                         int
		constant, nulls, standby, kind bool
	}{
		{name: "one-group", groups: 1},
		{name: "alternating-32", groups: 32},
		{name: "distinct-256", groups: hashmap.UnitLimit},
		{name: "nulls", groups: 32, nulls: true},
		{name: "constant", groups: 32, constant: true},
		{name: "standby", groups: 32, standby: true},
		{name: "metadata-fallback", groups: 32, kind: true},
	} {
		for _, fill := range []bool{false, true} {
			if tc.standby && fill {
				continue // repeated new-group publication is not a steady state
			}
			path := "preflight"
			if fill {
				path = "preflight-fill"
			}
			b.Run(tc.name+"/"+path, func(b *testing.B) {
				b.StopTimer()
				f := newFixedMetadataPreflightFixture(b, AggIdOfMax, types.T_float64.ToType())
				input := vector.NewVec(types.T_float64.ToType())
				if tc.constant {
					input.Free(f.mp)
					var err error
					input, err = vector.NewConstFixed(types.T_float64.ToType(), float64(7), 1, f.mp)
					require.NoError(b, err)
				}
				b.Cleanup(func() { input.Free(f.mp) })
				groups := make([]uint64, hashmap.UnitLimit)
				want := make([]float64, tc.groups)
				present := make([]bool, tc.groups)
				for row := range groups {
					group := row % tc.groups
					groups[row] = uint64(group + 1)
					value := float64(row)
					if tc.constant {
						value = 7
					}
					isNull := tc.nulls && row%32 == 31
					if !tc.constant {
						require.NoError(b, vector.AppendFixed(input, value, isNull, f.mp))
					}
					if !isNull && (!present[group] || value > want[group]) {
						want[group], present[group] = value, true
					}
				}
				kind := vector.PrepareParamNone
				if tc.kind {
					kind = vector.PrepareParamFloat
				}
				input.SetPrepareParamKind(kind)
				require.True(b, input.HasPrepareParamKind())
				require.Equal(b, kind, input.GetPrepareParamKind())
				require.Nil(b, input.GetPrepareParamKinds())
				require.False(b, input.HasStringSourceMetadata())
				vectors := []*vector.Vector{input}
				if tc.standby {
					require.NoError(b, f.exec.PreAllocateGroups(tc.groups))
				} else {
					require.NoError(b, f.exec.GroupGrow(tc.groups))
					require.NoError(b, f.exec.PreflightBatchFill(0, groups, vectors))
					require.NoError(b, f.exec.BatchFill(0, groups, vectors))
					target := f.base.state[0].vecs[0]
					require.True(b, target.HasPrepareParamKind(), "benchmark the observed steady state")
					if tc.kind {
						require.Equal(b, vector.PrepareParamNone, target.GetPrepareParamKind())
						require.NotNil(b, target.GetPrepareParamKinds())
						require.Len(b, target.GetPrepareParamKinds(), tc.groups)
						for row, expected := range want {
							require.False(b, target.IsNull(uint64(row)))
							require.Equal(b, kind, target.GetPrepareParamKindAt(row))
							require.Equal(b, expected, vector.GetFixedAtNoTypeCheck[float64](target, row))
						}
					} else {
						require.Equal(b, kind, target.GetPrepareParamKind())
						require.Nil(b, target.GetPrepareParamKinds())
					}
					require.False(b, target.HasStringSourceMetadata())
				}
				used, calls := f.account.Snapshot().Used, f.controller.calls
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					if err := f.exec.PreflightBatchFill(0, groups, vectors); err != nil {
						b.Fatal(err)
					}
					if fill {
						if err := f.exec.BatchFill(0, groups, vectors); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				require.Equal(b, used, f.account.Snapshot().Used)
				require.Equal(b, calls, f.controller.calls)
				if tc.standby {
					require.Zero(b, f.exec.GetNumGroups())
					require.NoError(b, f.exec.GroupGrow(tc.groups))
					require.NoError(b, f.exec.BatchFill(0, groups, vectors))
				}
				result, err := f.exec.Flush()
				require.NoError(b, err)
				require.Len(b, result, 1)
				b.Cleanup(func() { result[0].Free(f.mp) })
				for row := range want {
					require.Equal(b, !present[row], result[0].IsNull(uint64(row)))
					wantKind := vector.PrepareParamNone
					if present[row] {
						require.Equal(b, want[row], vector.GetFixedAtNoTypeCheck[float64](result[0], row))
						wantKind = kind
					}
					require.Equal(b, wantKind, result[0].GetPrepareParamKindAt(row))
					require.Equal(b, types.StringSourceExpression, result[0].GetStringSourceAt(row))
				}
			})
		}
	}
}
