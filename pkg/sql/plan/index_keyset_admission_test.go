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

package plan

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexKeySetProbeCost(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stats        *planpb.Stats
		count        int
		forced, want bool
	}{
		{"small scan three", &planpb.Stats{TableCnt: 1000, Cost: 1000}, 3, false, false},
		{"small scan 100", &planpb.Stats{TableCnt: 1000, Cost: 1000}, 100, false, false},
		{"small scan 101", &planpb.Stats{TableCnt: 1000, Cost: 1000}, 101, false, false},
		{"below startup boundary", &planpb.Stats{TableCnt: 1595, Cost: 1000}, 3, false, false},
		{"at startup boundary", &planpb.Stats{TableCnt: 1596, Cost: 1000}, 3, false, true},
		{"above startup boundary", &planpb.Stats{TableCnt: 1597, Cost: 1000}, 3, false, true},
		{"filtered original shape", &planpb.Stats{TableCnt: 5000, Cost: 303.03}, 3, false, true},
		{"single point unchanged", &planpb.Stats{TableCnt: 1, Cost: 1}, 1, false, true},
		{"existing cost boundary", &planpb.Stats{TableCnt: 60000, Cost: 1000}, 100, false, true},
		{"above existing cost boundary", &planpb.Stats{TableCnt: 60000, Cost: 1000}, 101, false, false},
		{"large scan", &planpb.Stats{TableCnt: 60000, Cost: 60000}, 128, false, true},
		{"cheap block scan", &planpb.Stats{TableCnt: 60000, Cost: 20}, 3, false, false},
		{"default three", DefaultStats(), 3, false, true},
		{"default 128", DefaultStats(), 128, false, false},
		{"nil stats", nil, 128, false, false},
		{"unknown zero cost", &planpb.Stats{TableCnt: 60000}, 128, false, false},
		{"unknown zero cardinality", &planpb.Stats{TableCnt: 0, Cost: 60000}, 128, false, false},
		{"unknown nan cardinality", &planpb.Stats{TableCnt: math.NaN(), Cost: 60000}, 128, false, false},
		{"unknown nan cost", &planpb.Stats{TableCnt: 60000, Cost: math.NaN()}, 128, false, false},
		{"unknown infinite cost", &planpb.Stats{TableCnt: 60000, Cost: math.Inf(1)}, 128, false, false},
		{"force bypasses cost", &planpb.Stats{TableCnt: 1000, Cost: 1000}, 128, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, indexKeySetWorthwhile(tc.stats, tc.count, tc.forced))
		})
	}
}

func replaceIntKeySetForTest(t *testing.T, f *keySetRankingFixture, count int) {
	t.Helper()
	v := vector.NewVec(types.T_int32.ToType())
	defer v.Free(f.ctx.GetProcess().Mp())
	for i := 0; i < count; i++ {
		require.NoError(t, vector.AppendFixed(v, int32(i), false, f.ctx.GetProcess().Mp()))
	}
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	literal := f.inB.GetF().Args[1].GetVec()
	literal.Data, literal.Len = data, int32(count)
}

func TestIndexKeySetAdmissionKeepsPlansAndHints(t *testing.T) {
	for _, tc := range []struct {
		name             string
		count            int
		stats            *planpb.Stats
		force, wantIndex bool
	}{
		{"small three", 3, &planpb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, false},
		{"small 100", 100, &planpb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, false},
		{"below startup boundary", 3, &planpb.Stats{TableCnt: 1595, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, false},
		{"at startup boundary", 3, &planpb.Stats{TableCnt: 1596, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, true},
		{"original shape", 3, &planpb.Stats{TableCnt: 5000, Cost: 303.03, Outcnt: 500, Selectivity: .5}, false, true},
		{"small above boundary", 101, &planpb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, false},
		{"small 128", 128, &planpb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 500, Selectivity: .5}, false, false},
		{"large 128", 128, &planpb.Stats{TableCnt: 60000, Cost: 60000, Outcnt: 30000, Selectivity: .5}, false, true},
		{"default three", 3, DefaultStats(), false, true},
		{"default 128", 128, DefaultStats(), false, false},
		{"forced small 128", 128, &planpb.Stats{TableCnt: 1000, Cost: 1000, Outcnt: 500, Selectivity: .5}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeKeySetRankingFixture(t, "duplicates")
			replaceIntKeySetForTest(t, f, tc.count)
			f.node.FilterList = f.node.FilterList[:2]
			f.node.TableDef.Indexes = []*IndexDef{f.indexes[0]}
			f.node.Stats = tc.stats
			before := *tc.stats
			scanID := f.node.NodeId
			if tc.force {
				f.builder.indexHintsByScan = map[int32]*indexHintSet{scanID: {scan: indexHintScopeSet{forceSpecified: true, force: map[string]struct{}{"ab": {}}}}}
			}
			tag := f.node.BindingTags[0]
			cPos := f.node.TableDef.Name2ColIndex["c"]
			result := f.builder.applyIndicesForFiltersRegularIndex(scanID, f.node, map[[2]int32]int{{tag, cPos}: 1}, map[[2]int32]*planpb.Expr{})
			require.Equal(t, before, *f.node.Stats)
			if !tc.wantIndex {
				require.Equal(t, scanID, result)
				require.Len(t, f.builder.qry.Nodes, 1)
				require.Same(t, f.limit, f.node.Limit)
				require.Same(t, f.offset, f.node.Offset)
				return
			}
			require.NotEqual(t, scanID, result)
			join := f.builder.qry.Nodes[result]
			require.Equal(t, planpb.Node_INDEX, join.JoinType)
			require.Equal(t, "ab", f.builder.qry.Nodes[join.Children[1]].IndexScanInfo.IndexName)
		})
	}
}

func TestIndexKeySetRestoreUsesUnknownCost(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		count                  int
		restore, force, useSet bool
	}{
		{"restore small key set", 3, true, false, true},
		{"restore large key set", 128, true, false, false},
		{"force restore key set", 128, true, true, true},
		{"real large scan", 128, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeKeySetRankingFixture(t, "duplicates")
			replaceIntKeySetForTest(t, f, tc.count)
			f.node.FilterList = f.node.FilterList[:2]
			f.node.Stats = DefaultHugeStats()
			f.builder.isRestore = tc.restore
			if tc.force {
				f.builder.indexHintsByScan = map[int32]*indexHintSet{f.node.NodeId: {scan: indexHintScopeSet{forceSpecified: true, force: map[string]struct{}{"ab": {}}}}}
			}
			_, _, count := f.builder.selectPointIndexWithBudget([]*IndexDef{f.indexes[0]}, f.node, true, 1, true, newIndexKeySetWorkBudget(), nil)
			if tc.useSet {
				require.Equal(t, tc.count, count)
			} else {
				require.Zero(t, count)
			}
		})
	}
}

func TestIndexKeySetCostRejectionDoesNotMaskOtherCandidate(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	replaceIntKeySetForTest(t, f, 128)
	f.node.TableDef.Indexes = []*IndexDef{f.indexes[0], f.indexes[2]}
	f.node.Stats = &planpb.Stats{TableCnt: 5000, Cost: 5000, Outcnt: 500, Selectivity: .5}
	f.inB.Selectivity = 1e-8 // The rejected candidate would otherwise rank first.
	tag := f.node.BindingTags[0]
	result := f.builder.applyIndicesForFiltersRegularIndex(f.node.NodeId, f.node,
		map[[2]int32]int{{tag, f.node.TableDef.Name2ColIndex["b"]}: 1, {tag, f.node.TableDef.Name2ColIndex["c"]}: 1}, map[[2]int32]*planpb.Expr{})
	require.NotEqual(t, f.node.NodeId, result)
	join := f.builder.qry.Nodes[result]
	indexScan := f.builder.qry.Nodes[join.Children[1]]
	require.Equal(t, "ac", indexScan.IndexScanInfo.IndexName)
	keyCount, ok := exactUniqueKeySetSize(indexScan.FilterList[0])
	require.True(t, ok, "the second candidate must remain a key set")
	require.Equal(t, 2, keyCount)
}

func TestIndexKeySetWorkBoundAndDynamicValues(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	budget := newIndexKeySetWorkBudget()
	positions := []int32{0, 1}
	work, ok := budget.estimate(f.node.FilterList, positions)
	require.True(t, ok)
	require.Positive(t, work)
	budget.remaining = work
	_, ok = budget.estimate(f.node.FilterList, positions)
	require.True(t, ok, "exact budget is accepted")
	budget.remaining = work - 1
	_, ok = budget.estimate(f.node.FilterList, positions)
	require.False(t, ok, "one byte below the reservation is rejected")

	// The executor constructs a string vector from Sval even when Typ says int.
	value := f.node.FilterList[0].GetF().Args[1]
	value.GetLit().Value = &planpb.Literal_Sval{Sval: strings.Repeat("x", 200000)}
	bound, ok := indexKeySetLiteralBound(value)
	require.True(t, ok)
	require.EqualValues(t, 400003, bound)
	_, ok = newIndexKeySetWorkBudget().estimate(f.node.FilterList, positions)
	require.False(t, ok, "wide equality is charged for every tuple")
	value.GetLit().Value = &planpb.Literal_I32Val{I32Val: 1}
	value.GetLit().Src = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	_, ok = newIndexKeySetWorkBudget().estimate(f.node.FilterList, positions)
	require.False(t, ok, "a sourced literal cannot be fixed into a prepared plan")
	value.GetLit().Src = nil
	value.GetLit().Isnull = true
	_, ok = newIndexKeySetWorkBudget().estimate(f.node.FilterList, positions)
	require.False(t, ok)
}

func TestIndexKeySetRevalidatesChangedInputAfterResolve(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	setStringData := func(values []string) []byte {
		v := vector.NewVec(types.T_varchar.ToType())
		defer v.Free(f.ctx.GetProcess().Mp())
		for _, value := range values {
			require.NoError(t, vector.AppendBytes(v, []byte(value), false, f.ctx.GetProcess().Mp()))
		}
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		return data
	}
	vtyp := planpb.Type{Id: int32(types.T_varchar), Width: 1000000}
	f.inB.GetF().Args[0].Typ = vtyp
	f.inB.GetF().Args[1].Typ = vtyp
	f.inB.GetF().Args[1].GetVec().Data = setStringData([]string{"a", "b", "c"})
	wide := strings.Repeat("w", 150000)
	wideData := setStringData([]string{wide, wide, wide})
	budget := newIndexKeySetWorkBudget()
	positions := []int32{0, 1}
	work, ok := budget.estimate(f.node.FilterList, positions)
	require.True(t, ok)
	require.True(t, budget.charge(work))
	f.ctx.beforeResolve = func(string) {
		require.Len(t, f.builder.qry.Nodes, 1)
		f.inB.GetF().Args[1].GetVec().Data = wideData
	}
	prepared, err := f.builder.prepareIndexJoinWithBudget(f.indexes[0], f.node, KeySetIndexCondition, positions, &Snapshot{}, &indexKeySetReservation{budget: budget, work: work})
	require.NoError(t, err)
	require.Nil(t, prepared, "changed wide input must be declined before encoding")
	require.Len(t, f.builder.qry.Nodes, 1)
	require.Equal(t, maxIndexKeySetPlanningWork-work, budget.remaining)
	require.Same(t, f.limit, f.node.Limit)
	require.Same(t, f.offset, f.node.Offset)
}

func TestIndexKeySetRevalidatesScanCostAfterResolve(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	budget := newIndexKeySetWorkBudget()
	positions := []int32{0, 1}
	work, ok := budget.estimate(f.node.FilterList, positions)
	require.True(t, ok)
	require.True(t, budget.charge(work))
	stats := &planpb.Stats{TableCnt: 5000, Cost: 1000}
	require.True(t, budget.revalidate(f.node.FilterList, positions, work, stats, false))
	stats.TableCnt = 1000
	require.False(t, budget.revalidate(f.node.FilterList, positions, work, stats, false))
	require.True(t, budget.revalidate(f.node.FilterList, positions, work, stats, true))
}

type canceledKeySetContext struct{ *keySetRankingContext }

func (c *canceledKeySetContext) ResolveIndexTableByRef(*ObjectRef, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	return nil, nil, context.Canceled
}

func TestIndexKeySetResolveFailureTriesOtherCandidate(t *testing.T) {
	f := makeKeySetRankingFixture(t, "resolve_failure")
	f.node.TableDef.Indexes = []*IndexDef{f.indexes[0], f.indexes[2]}
	f.node.Stats = &planpb.Stats{TableCnt: 5000, Cost: 5000, Outcnt: 500, Selectivity: .5}
	f.inB.Selectivity = 1e-8
	var resolved []string
	f.ctx.beforeResolve = func(name string) {
		require.Len(t, f.builder.qry.Nodes, 1)
		resolved = append(resolved, name)
	}
	tag := f.node.BindingTags[0]
	result := f.builder.applyIndicesForFiltersRegularIndex(f.node.NodeId, f.node,
		map[[2]int32]int{{tag, f.node.TableDef.Name2ColIndex["b"]}: 1, {tag, f.node.TableDef.Name2ColIndex["c"]}: 1}, map[[2]int32]*planpb.Expr{})
	require.NotEqual(t, f.node.NodeId, result)
	join := f.builder.qry.Nodes[result]
	require.Equal(t, "ac", f.builder.qry.Nodes[join.Children[1]].IndexScanInfo.IndexName)
	require.Contains(t, resolved, "keyset_ab")
	require.Contains(t, resolved, "keyset_ac")
}

func TestIndexKeySetCancellationIsNotADecline(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	f.node.TableDef.Indexes = []*IndexDef{f.indexes[0]}
	f.node.Stats = &planpb.Stats{TableCnt: 5000, Cost: 5000, Outcnt: 500, Selectivity: .5}
	f.builder.compCtx = &canceledKeySetContext{f.ctx}
	tag := f.node.BindingTags[0]
	require.PanicsWithValue(t, context.Canceled, func() {
		f.builder.applyIndicesForFiltersRegularIndex(f.node.NodeId, f.node,
			map[[2]int32]int{{tag, f.node.TableDef.Name2ColIndex["c"]}: 1}, map[[2]int32]*planpb.Expr{})
	})
	require.Len(t, f.builder.qry.Nodes, 1)
}

func TestIndexKeySetBudgetCountsConstantAndEscapedValues(t *testing.T) {
	f := makeKeySetRankingFixture(t, "duplicates")
	value := []byte{'a', 0, 'b'}
	v, err := vector.NewConstBytes(types.T_varchar.ToType(), value, 8, f.ctx.GetProcess().Mp())
	require.NoError(t, err)
	defer v.Free(f.ctx.GetProcess().Mp())
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	vtyp := planpb.Type{Id: int32(types.T_varchar)}
	f.inB.GetF().Args[0].Typ = vtyp
	f.inB.GetF().Args[1].Typ = vtyp
	f.inB.GetF().Args[1].GetVec().Data = data
	f.inB.GetF().Args[1].GetVec().Len = 8
	bound := newIndexKeySetWorkBudget().inputBound(f.inB)
	require.True(t, bound.ok)
	require.Equal(t, 8, bound.count)
	require.EqualValues(t, 8*(len(value)+1+3), bound.bytes, "count every logical row and the escaped zero byte")
	f.inB.GetF().Args[1].GetVec().Data = []byte{1, 2, 3}
	require.False(t, newIndexKeySetWorkBudget().inputBound(f.inB).ok, "invalid wire input must decline")
}

func TestIndexKeySetBudgetIsSharedAcrossAttempts(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		roomForSecond, mismatch bool
		wantIndex               bool
	}{
		{"failed candidate spends its reservation", false, false, false},
		{"affordable second candidate remains available", true, false, true},
		{"rank mismatch cannot reset the budget", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := makeKeySetRankingFixture(t, "duplicates")
			indexes := []*IndexDef{f.indexes[0], f.indexes[2]}
			budget := newIndexKeySetWorkBudget()
			workA, ok := budget.estimate(f.node.FilterList, []int32{0, 1})
			require.True(t, ok)
			workC, ok := budget.estimate(f.node.FilterList, []int32{0, 2})
			require.True(t, ok)
			budget.remaining = budget.inputBound(f.inB).bytes + budget.inputBound(f.inC).bytes + workA
			if tc.roomForSecond {
				budget.remaining += workC
			}
			calls := 0
			if tc.mismatch {
				v := vector.NewVec(types.T_int32.ToType())
				defer v.Free(f.ctx.GetProcess().Mp())
				appendKeySetFixedForTest(t, v, f.ctx.GetProcess().Mp(), int32(1), int32(2), int32(3))
				data, err := v.MarshalBinary()
				require.NoError(t, err)
				f.ctx.beforeResolve = func(name string) {
					calls++
					if name == "keyset_ab" {
						f.inB.GetF().Args[1].GetVec().Data = data
					}
				}
			} else {
				f.ctx.fail["keyset_ab"] = true
				f.ctx.beforeResolve = func(string) { calls++ }
			}
			result, scan := f.builder.applyBoundedUniqueKeySet(indexes, f.node, &Snapshot{}, 1, budget, nil)
			if !tc.wantIndex {
				require.EqualValues(t, -1, result)
				require.EqualValues(t, -1, scan)
				require.Len(t, f.builder.qry.Nodes, 1)
				require.Equal(t, 1, calls)
				return
			}
			require.GreaterOrEqual(t, scan, int32(0))
			require.Equal(t, "ac", f.builder.qry.Nodes[scan].IndexScanInfo.IndexName)
			require.Equal(t, 2, calls)
		})
	}
}

// Independent planners can prepare key sets at the same time. Synchronize
// inside index resolution so the test exercises overlapping attempts, then
// verify each published key vector owns its data after the input is changed.
func TestIndexKeySetIndependentConcurrentBuilders(t *testing.T) {
	const workers = 4
	fixtures := make([]*keySetRankingFixture, workers)
	results := make([]struct{ result, scan int32 }, workers)
	ready := make(chan struct{}, workers)
	release := make(chan struct{})
	var done sync.WaitGroup
	done.Add(workers)
	for i := range fixtures {
		f := makeKeySetRankingFixture(t, "duplicates")
		fixtures[i] = f
		var once sync.Once
		f.ctx.beforeResolve = func(string) {
			once.Do(func() {
				ready <- struct{}{}
				<-release
			})
		}
		go func(i int) {
			defer done.Done()
			results[i].result, results[i].scan = f.builder.applyBoundedUniqueKeySet(f.indexes, f.node, &Snapshot{}, 1, newIndexKeySetWorkBudget(), nil)
		}(i)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for range fixtures {
		select {
		case <-ready:
		case <-timer.C:
			close(release)
			done.Wait()
			t.Fatal("independent builders did not all reach index resolution")
		}
	}
	close(release)
	done.Wait()
	for i, f := range fixtures {
		require.GreaterOrEqual(t, results[i].result, int32(0))
		require.GreaterOrEqual(t, results[i].scan, int32(0))
		scan := f.builder.qry.Nodes[results[i].scan]
		require.Equal(t, "ab", scan.IndexScanInfo.IndexName)
		count, ok := exactUniqueKeySetSize(scan.FilterList[0])
		require.True(t, ok)
		require.Equal(t, 1, count)
		encoded := scan.FilterList[0].GetF().Args[1].GetVec().Data
		saved := append([]byte(nil), encoded...)
		f.inB.GetF().Args[1].GetVec().Data[0] ^= 0xff
		require.Equal(t, saved, encoded)
	}
}
