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

package group

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestDynamicGroupingAggregateMarker(t *testing.T) {
	for _, tc := range []struct {
		name         string
		constant     bool
		values       []bool
		nulls        []uint64
		offset       int
		empty        bool
		shortScratch bool
		masked       bool
		want         []uint64
		wantErr      string
	}{
		{name: "constant-real", constant: true, values: []bool{false}, offset: 1, want: []uint64{11, 12}},
		{name: "constant-synthetic", constant: true, values: []bool{true}, offset: 1, masked: true, want: []uint64{0, 0}},
		{name: "constant-null", constant: true, offset: 1, wantErr: "cannot be NULL"},
		{name: "empty-constant-null", constant: true, offset: 4, empty: true, want: []uint64{}},
		{name: "flat-offset", values: []bool{true, false, true, false}, offset: 1, masked: true, want: []uint64{11, 0}},
		{name: "flat-null-outside-window", values: []bool{false, false, false, false}, nulls: []uint64{0, 3}, offset: 1, want: []uint64{11, 12}},
		{name: "flat-null-after-synthetic", values: []bool{false, true, false, false}, nulls: []uint64{2}, offset: 1, wantErr: "cannot be NULL"},
		{name: "short-scratch", constant: true, values: []bool{false}, offset: 1, shortScratch: true, wantErr: "missing its aggregate marker"},
		{name: "empty-outside-window", constant: true, offset: 5, empty: true, wantErr: "invalid dynamic grouping aggregate marker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			var marker *vector.Vector
			t.Cleanup(func() {
				if marker != nil {
					marker.Free(mp)
				}
			})
			if tc.constant {
				if len(tc.values) == 0 {
					marker = vector.NewConstNull(types.T_bool.ToType(), 4, mp)
				} else {
					var err error
					marker, err = vector.NewConstFixed(types.T_bool.ToType(), tc.values[0], 4, mp)
					require.NoError(t, err)
				}
			} else {
				marker = testutil.MakeBoolVector(tc.values, tc.nulls, mp)
			}
			input := &batch.Batch{Vecs: []*vector.Vector{marker, nil}}
			groups := []uint64{11, 12}
			scratch := []uint64{91, 92, 93}
			selected := groups
			if tc.empty {
				selected = groups[:0]
			}
			work := scratch
			if tc.shortScratch {
				work = scratch[:1]
			}
			got, err := dynamicGroupingAggregateGroups(input, tc.offset, selected, work)
			require.Equal(t, []uint64{11, 12}, groups, "hash group IDs must remain unchanged")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
				if len(got) > 0 {
					if tc.masked {
						require.Same(t, &scratch[0], &got[0])
					} else {
						require.Same(t, &groups[0], &got[0])
					}
				}
			}
			if !tc.masked {
				require.Equal(t, []uint64{91, 92, 93}, scratch)
			}
			require.Equal(t, uint64(93), scratch[2], "only the selected scratch window may change")
		})
	}
}

// One hash mini-batch isolates marker selection from hash and aggregate costs.
func BenchmarkDynamicGroupingAggregateMarker(b *testing.B) {
	const rows = hashmap.UnitLimit
	for _, name := range []string{"constant-real", "constant-synthetic", "flat-real", "flat-mixed"} {
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { require.Zero(b, mp.CurrNB()) })
			values := make([]bool, rows+1)
			var marker *vector.Vector
			b.Cleanup(func() {
				if marker != nil {
					marker.Free(mp)
				}
			})
			if name == "constant-real" || name == "constant-synthetic" {
				var err error
				marker, err = vector.NewConstFixed(types.T_bool.ToType(), name == "constant-synthetic", rows+1, mp)
				require.NoError(b, err)
			} else {
				if name == "flat-mixed" {
					values[rows/2] = true
				}
				marker = testutil.MakeBoolVector(values, nil, mp)
			}
			input := &batch.Batch{Vecs: []*vector.Vector{marker, nil}}
			groups, scratch := make([]uint64, rows), make([]uint64, rows)
			for i := range groups {
				groups[i] = uint64(i + 1)
			}
			b.ReportAllocs()
			var got []uint64
			var err error
			for b.Loop() {
				got, err = dynamicGroupingAggregateGroups(input, 1, groups, scratch)
				if err != nil {
					b.Fatal(err)
				}
			}
			want := groups[rows/2-1]
			if name == "constant-synthetic" || name == "flat-mixed" {
				want = aggexec.GroupNotMatched
			}
			require.Equal(b, want, got[rows/2-1])
		})
	}
}

func TestGroupMiniBatchScratchResults(t *testing.T) {
	const rows = hashmap.UnitLimit + 3
	type key struct {
		text string
		null bool
	}
	for _, tc := range []struct {
		name      string
		dynamic   bool
		synthetic bool
	}{
		{name: "ordinary"},
		{name: "dynamic-real", dynamic: true},
		{name: "dynamic-synthetic-tail", dynamic: true, synthetic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
			})
			texts := make([]string, rows)
			values := make([]int32, rows)
			markers := make([]bool, rows)
			setIDs := make([]int64, rows)
			want := make(map[key][2]int64)
			for row := 0; row < rows; row++ {
				texts[row] = []string{"", "a\x00b", "c"}[row%3]
				values[row] = int32(row + 1)
				markers[row] = tc.synthetic && row == rows-1
				if !markers[row] {
					k := key{text: texts[row], null: row == 5}
					if k.null {
						k.text = ""
					}
					v := want[k]
					v[0]++
					v[1] += int64(values[row])
					want[k] = v
				}
			}
			input := batch.NewWithSize(4)
			input.Vecs[0] = testutil.MakeVarcharVector(texts, []uint64{5}, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
			input.Vecs[2] = testutil.MakeBoolVector(markers, nil, proc.Mp())
			input.Vecs[3] = testutil.MakeInt64Vector(setIDs, nil, proc.Mp())
			input.SetRowCount(rows)
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
			t.Cleanup(func() { child.Free(proc, false, nil) })
			g := newGroupOp(proc, []*plan.Expr{
				colExpr(0, types.T_varchar), colExpr(3, types.T_int64),
			}, []aggexec.AggFuncExecExpression{countStarLiteralAgg(), sumAgg(1)})
			g.DynamicGrouping = tc.dynamic
			g.AppendChild(child)
			t.Cleanup(func() { g.Free(proc, false, nil) })
			require.NoError(t, g.Prepare(proc))
			got := make(map[key][2]int64)
			for _, output := range collectBatches(t, g, proc) {
				counts := vector.MustFixedColNoTypeCheck[int64](output.Vecs[2])
				sums := vector.MustFixedColNoTypeCheck[int64](output.Vecs[3])
				for row := 0; row < output.RowCount(); row++ {
					k := key{null: output.Vecs[0].GetNulls().Contains(uint64(row))}
					if !k.null {
						k.text = string(output.Vecs[0].GetBytesAt(row))
					}
					require.Zero(t, vector.MustFixedColNoTypeCheck[int64](output.Vecs[1])[row])
					require.False(t, output.Vecs[2].GetNulls().Contains(uint64(row)))
					require.False(t, output.Vecs[3].GetNulls().Contains(uint64(row)))
					_, duplicate := got[k]
					require.False(t, duplicate, "duplicate output group %#v", k)
					got[k] = [2]int64{counts[row], sums[row]}
				}
			}
			require.Equal(t, want, got)
		})
	}
}

// Fixed-width hash keys isolate the Group scratch allocation from raw-string
// encoding. Both modes use the same input, key columns and aggregate result.
func BenchmarkGroupMiniBatchScratch(b *testing.B) {
	const rows, groups = 8192, 4
	keys := make([]int32, rows)
	markers := make([]bool, rows)
	setIDs := make([]int64, rows)
	for row := range keys {
		keys[row] = int32(row % groups)
	}
	for _, tc := range []struct {
		name    string
		dynamic bool
	}{
		{name: "ordinary"},
		{name: "dynamic-control", dynamic: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			proc := testutil.NewProcess(b)
			b.ReportAllocs()
			b.SetBytes(rows * (4 + 1 + 8))
			for b.Loop() {
				b.StopTimer()
				func() {
					input := batch.NewWithSize(3)
					input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
					input.Vecs[1] = testutil.MakeBoolVector(markers, nil, proc.Mp())
					input.Vecs[2] = testutil.MakeInt64Vector(setIDs, nil, proc.Mp())
					input.SetRowCount(rows)
					child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
					defer child.Free(proc, false, nil)
					g := newGroupOp(proc, []*plan.Expr{
						colExpr(0, types.T_int32), colExpr(2, types.T_int64),
					}, []aggexec.AggFuncExecExpression{countStarLiteralAgg()})
					defer g.Free(proc, false, nil)
					g.DynamicGrouping = tc.dynamic
					g.AppendChild(child)
					b.StartTimer()
					if err := g.Prepare(proc); err != nil {
						b.Fatal(err)
					}
					outputRows := 0
					var seen [groups]bool
					for {
						result, err := vm.Exec(g, proc)
						if err != nil {
							b.Fatal(err)
						}
						if result.Status == vm.ExecStop || result.Batch == nil {
							break
						}
						outputRows += result.Batch.RowCount()
						for row, key := range vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]) {
							if key < 0 || key >= groups || seen[key] {
								b.Fatalf("invalid or duplicate key %d", key)
							}
							seen[key] = true
							setID := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])[row]
							count := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2])[row]
							if setID != 0 || count != rows/groups {
								b.Fatalf("key %d: set/count = %d/%d, want 0/%d", key, setID, count, rows/groups)
							}
						}
					}
					if outputRows != groups {
						b.Fatalf("groups = %d, want %d", outputRows, groups)
					}
					b.StopTimer()
				}()
				if allocated := proc.Mp().CurrNB(); allocated != 0 {
					b.Fatalf("group leaked %d bytes", allocated)
				}
				b.StartTimer()
			}
		})
	}
}
