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

package group

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	orderop "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BenchmarkRollupAlgorithms measures the physical work that is specific to a
// ROLLUP implementation. The unordered sort case includes the ORDER operator;
// the ordered sort case reuses the pre-sorted input. The shared hash case
// models the protocol 49+ grouping-set expansion, while the serial and
// parallel hash cases retain the genuinely unshared fallback.
//
// The serial hash case is a CPU baseline. The parallel hash case approximates
// UNION ALL's concurrent topology and is useful when deciding whether a sort
// win survives branch parallelism. Fixture setup is outside the timer, while
// every iteration still duplicates the source batch and prepares a new
// operator so the measured path includes source-to-aggregate data movement.
func BenchmarkRollupAlgorithms(b *testing.B) {
	cases := []struct {
		name         string
		rows         int
		ndv          int
		ndvByKey     []int
		keyCount     int
		ordered      bool
		derivedOrder bool
		aggregate    string
	}{
		// These are the shapes in which one scan plus one ordered aggregate can
		// amortize the fixed cost of the legacy grouping-set branches.
		{name: "tiny_low_ndv", rows: 256, ndv: 4, keyCount: 3},
		{name: "small_low_ndv", rows: 4096, ndv: 8, keyCount: 3},
		{name: "medium_low_ndv", rows: 32768, ndv: 16, keyCount: 3},
		{name: "wide_low_ndv", rows: 4096, ndv: 8, keyCount: 6},
		{name: "tiny_many_levels", rows: 256, ndv: 4, keyCount: 12},
		{name: "small_many_levels", rows: 4096, ndv: 8, keyCount: 12},
		{name: "small_very_many_levels", rows: 4096, ndv: 8, keyCount: 20},
		// This is the counterexample: a large detail cardinality makes the
		// global sort and repeated per-level updates more expensive than hash.
		{name: "medium_high_ndv", rows: 32768, ndv: 32768, keyCount: 3},
		// At production-sized input, an existing global order removes the sort
		// operator entirely. Keep the ordered and unordered shapes paired so the
		// benefit of order reuse is visible independently of hash parallelism.
		{name: "large_ordered_low_ndv", rows: 100000, ndv: 4, keyCount: 3, ordered: true},
		{name: "large_unordered_low_ndv", rows: 100000, ndv: 4, keyCount: 3},
		{name: "large_ordered_one_key", rows: 100000, ndv: 8, keyCount: 1, ordered: true},
		{name: "large_ordered_single_group", rows: 100000, ndv: 1, keyCount: 12, ordered: true},
		{name: "large_ordered_many_levels", rows: 100000, ndv: 2, keyCount: 12, ordered: true},
		{name: "large_ordered_wider_ndv", rows: 100000, ndv: 4, keyCount: 12, ordered: true},
		{name: "large_ordered_high_ndv", rows: 100000, ndv: 64, keyCount: 12, ordered: true},
		// Unlike the correlated NDV=64 fixture above, every complete key in
		// this three-column input is distinct. It makes the ordered path pay
		// for the completed-group flush/free/recreate lifecycle.
		{name: "large_ordered_unique_keys", rows: 100000, ndv: 100000, keyCount: 3, ordered: true},
		{name: "large_derived_order_unique_keys", rows: 100000, ndv: 100000, keyCount: 3, derivedOrder: true},
		// The first two prefixes are singletons while the complete key has
		// 50,000 values occurring twice. This catches a cost model that only
		// calibrates the all-unique endpoint and misses deep-prefix finalization.
		{name: "large_ordered_asymmetric_high_ndv", rows: 100000, ndv: 50000, ndvByKey: []int{1, 1, 50000}, keyCount: 3, ordered: true},
		{name: "large_derived_order_asymmetric_high_ndv", rows: 100000, ndv: 50000, ndvByKey: []int{1, 1, 50000}, keyCount: 3, derivedOrder: true},
		// Calibrate the admitted boundary and both neighboring sides, not only
		// the high-cardinality endpoint. AVG challenges the aggregate weight.
		{name: "large_derived_order_asymmetric_low_ndv", rows: 100000, ndv: 4, ndvByKey: []int{1, 1, 4}, keyCount: 3, derivedOrder: true},
		{name: "large_derived_order_asymmetric_edge_ndv", rows: 100000, ndv: 5000, ndvByKey: []int{1, 1, 5000}, keyCount: 3, derivedOrder: true},
		{name: "large_derived_order_asymmetric_intermediate_ndv", rows: 100000, ndv: 10000, ndvByKey: []int{1, 1, 10000}, keyCount: 3, derivedOrder: true},
		{name: "large_derived_order_asymmetric_middle_ndv", rows: 100000, ndv: 25000, ndvByKey: []int{1, 1, 25000}, keyCount: 3, derivedOrder: true},
		{name: "large_derived_order_asymmetric_avg_edge_ndv", rows: 100000, ndv: 5000, ndvByKey: []int{1, 1, 5000}, keyCount: 3, derivedOrder: true, aggregate: "avg"},
		{name: "large_ordered_avg_many_levels", rows: 100000, ndv: 2, keyCount: 12, ordered: true, aggregate: "avg"},
		{name: "large_ordered_very_many_levels", rows: 100000, ndv: 2, keyCount: 20, ordered: true},
		{name: "large_ordered_extreme_levels", rows: 100000, ndv: 2, keyCount: 32, ordered: true},
		{name: "million_ordered_low_ndv", rows: 1000000, ndv: 4, keyCount: 3, ordered: true},
		// This models:
		//   SELECT ... FROM (SELECT ... ORDER BY k1, k2, k3) d
		//   GROUP BY k1, k2, k3 WITH ROLLUP
		// The sort path pays for the derived ORDER BY once. Shared HASH pays it
		// once before expansion; the unshared fallback sorts every branch.
		{name: "million_derived_order_low_ndv", rows: 1000000, ndv: 4, keyCount: 3, derivedOrder: true},
		{name: "million_derived_order_avg_low_ndv", rows: 1000000, ndv: 4, keyCount: 2, derivedOrder: true, aggregate: "avg"},
	}

	for _, tc := range cases {
		for _, algorithm := range []string{"sort", "hash-shared", "hash-serial", "hash-parallel"} {
			name := fmt.Sprintf("%s/%s", tc.name, algorithm)
			b.Run(name, func(b *testing.B) {
				runner, err := newRollupBenchmarkRunner(b, tc.rows, tc.ndv, tc.ndvByKey, tc.keyCount,
					tc.ordered, tc.derivedOrder, tc.aggregate)
				if err != nil {
					b.Fatal(err)
				}
				defer runner.free()

				b.ReportAllocs()
				var peak int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var current int64
					switch algorithm {
					case "sort":
						current, err = runner.runSort()
					case "hash-shared":
						current, err = runner.runHashShared()
					case "hash-serial":
						current, err = runner.runHash(false)
					case "hash-parallel":
						current, err = runner.runHash(true)
					}
					if err != nil {
						b.Fatal(err)
					}
					if current > peak {
						peak = current
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(peak), "peak-mpool-B")
			})
		}
	}
}

// Each operation is a cohort of four independent queries. Both alternatives
// include the same derived ORDER BY and drain all finalized groups. Validate
// their results before timing; no wall-clock threshold belongs in a unit test.
func BenchmarkRollupConcurrentAlgorithms(b *testing.B) {
	for _, tc := range []struct {
		name string
		ndv  int
	}{
		{name: "low_ndv", ndv: 4},
		{name: "admitted_edge", ndv: 2500},
		{name: "concurrent_regression", ndv: 5800},
	} {
		for _, algorithm := range []struct {
			name string
			run  func(*rollupBenchmarkRunner) (int64, error)
		}{
			{name: "hash-shared", run: (*rollupBenchmarkRunner).runHashShared},
			{name: "sort", run: (*rollupBenchmarkRunner).runSort},
		} {
			b.Run(tc.name+"/"+algorithm.name, func(b *testing.B) {
				runners, err := newRollupBenchmarkCohort(b, 100000, tc.ndv)
				if err != nil {
					b.Fatal(err)
				}
				if err := checkRollupBenchmarkCohort(runners, algorithm.run); err != nil {
					b.Fatal(err)
				}
				// Exclude discarded oracle scratch from measured GC pressure.
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := runRollupBenchmarkCohort(runners, algorithm.run); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				runtime.ReadMemStats(&after)
				b.ReportMetric(4, "queries/cohort")
				b.ReportMetric(float64(after.NumGC-before.NumGC)/float64(b.N), "GCs/cohort")
			})
		}
	}
}

func newRollupBenchmarkCohort(t testing.TB, rows, ndv int) ([]*rollupBenchmarkRunner, error) {
	runners := make([]*rollupBenchmarkRunner, 0, 4)
	for i := 0; i < 4; i++ {
		runner, err := newRollupBenchmarkRunner(t, rows, ndv, []int{1, 1, ndv}, 3, false, true, "count")
		if err != nil {
			return nil, err
		}
		// Registered after the process owners, so bases are freed before their
		// pools on success, partial construction failure, or Fatal.
		t.Cleanup(runner.free)
		// This cohort uses only the one-scan alternatives. Drop the unused
		// legacy branch batches so they do not inflate the live GC heap.
		for _, input := range runner.inputs[1:] {
			input.base.Clean(input.proc.Mp())
			input.base = nil
		}
		runners = append(runners, runner)
	}
	return runners, nil
}

func runRollupBenchmarkCohort(
	runners []*rollupBenchmarkRunner,
	run func(*rollupBenchmarkRunner) (int64, error),
) error {
	start := make(chan struct{})
	errs := make([]error, len(runners))
	var ready, done sync.WaitGroup
	ready.Add(len(runners))
	done.Add(len(runners))
	for i, runner := range runners {
		go func(i int, runner *rollupBenchmarkRunner) {
			defer done.Done()
			ready.Done()
			<-start
			_, errs[i] = run(runner)
		}(i, runner)
	}
	ready.Wait()
	close(start)
	done.Wait() // Join all owners before reporting an error or reclaiming bases.
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

type rollupBenchmarkCountKey struct {
	prefix int
	keys   [3]int32
}

// Build expectations directly from source rows and SQL prefix semantics,
// independently of either aggregate implementation and its grouping codec.
func rollupBenchmarkCountOracle(base *batch.Batch) (func(*batch.Batch) error, func() error) {
	expected := make(map[rollupBenchmarkCountKey]int64)
	var values [3][]int32
	for i := range values {
		values[i] = vector.MustFixedColWithTypeCheck[int32](base.Vecs[i])
	}
	for row := 0; row < base.RowCount(); row++ {
		for prefix := 0; prefix <= 3; prefix++ {
			key := rollupBenchmarkCountKey{prefix: prefix}
			for i := 0; i < prefix; i++ {
				key.keys[i] = values[i][row]
			}
			expected[key]++
		}
	}
	seen := make(map[rollupBenchmarkCountKey]bool, len(expected))
	observe := func(out *batch.Batch) error {
		if len(out.Vecs) != 4 && len(out.Vecs) != 5 {
			return fmt.Errorf("unexpected ROLLUP output width: %d", len(out.Vecs))
		}
		counts := vector.MustFixedColWithTypeCheck[int64](out.Vecs[len(out.Vecs)-1])
		var keys [3][]int32
		for i := range keys {
			keys[i] = vector.MustFixedColWithTypeCheck[int32](out.Vecs[i])
		}
		for row := 0; row < out.RowCount(); row++ {
			key := rollupBenchmarkCountKey{prefix: 3}
			for i := range keys {
				rolled := out.Vecs[i].GetGrouping().Contains(uint64(row))
				if rolled != out.Vecs[i].GetNulls().Contains(uint64(row)) {
					return fmt.Errorf("wrong grouping/NULL provenance at key %d", i)
				}
				if rolled {
					if key.prefix == 3 {
						key.prefix = i
					}
				} else {
					if key.prefix != 3 {
						return fmt.Errorf("non-prefix ROLLUP grouping flags")
					}
					key.keys[i] = keys[i][row]
				}
			}
			want, ok := expected[key]
			if !ok || seen[key] || counts[row] != want {
				return fmt.Errorf("wrong or duplicate ROLLUP row %v: count=%d want=%d", key, counts[row], want)
			}
			seen[key] = true
		}
		return nil
	}
	return observe, func() error {
		if len(seen) != len(expected) {
			return fmt.Errorf("ROLLUP returned %d groups, want %d", len(seen), len(expected))
		}
		return nil
	}
}

func checkRollupBenchmarkCohort(
	runners []*rollupBenchmarkRunner,
	run func(*rollupBenchmarkRunner) (int64, error),
) error {
	defer func() {
		for _, runner := range runners {
			runner.observe = nil
		}
	}()
	complete := make([]func() error, len(runners))
	for i, runner := range runners {
		runner.observe, complete[i] = rollupBenchmarkCountOracle(runner.inputs[0].base)
	}
	if err := runRollupBenchmarkCohort(runners, run); err != nil {
		return err
	}
	for _, finish := range complete {
		if err := finish(); err != nil {
			return err
		}
	}
	return nil
}

func TestRollupConcurrentBenchmarkCohort(t *testing.T) {
	for _, algorithm := range []struct {
		name string
		run  func(*rollupBenchmarkRunner) (int64, error)
	}{
		{name: "sort", run: (*rollupBenchmarkRunner).runSort},
		{name: "hash-shared", run: (*rollupBenchmarkRunner).runHashShared},
	} {
		t.Run(algorithm.name, func(t *testing.T) {
			runners, err := newRollupBenchmarkCohort(t, 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			baseline := make([]int64, len(runners))
			for i, runner := range runners {
				baseline[i] = runner.inputs[0].proc.Mp().CurrNB()
			}
			checkMemory := func() {
				t.Helper()
				for i, runner := range runners {
					if got := runner.inputs[0].proc.Mp().CurrNB(); got != baseline[i] {
						t.Fatalf("worker %d retained %d bytes; baseline %d", i, got, baseline[i])
					}
				}
			}
			if err := checkRollupBenchmarkCohort(runners, algorithm.run); err != nil {
				t.Fatal(err)
			}
			checkMemory()
			injected := errors.New("injected result observer failure")
			for _, runner := range runners {
				runner.observe = func(*batch.Batch) error { return injected }
			}
			if err := runRollupBenchmarkCohort(runners, algorithm.run); !errors.Is(err, injected) {
				t.Fatalf("observer error lost: %v", err)
			}
			checkMemory()
			if err := checkRollupBenchmarkCohort(runners, algorithm.run); err != nil {
				t.Fatal(err)
			}
			checkMemory()
		})
	}
}

type rollupBenchmarkRunner struct {
	inputs       []*rollupBenchmarkInput
	groupBy      [][]*plan.Expr
	specs        []*plan.OrderBySpec
	ordered      bool
	derivedOrder bool
	aggregate    string
	aggs         []aggexec.AggFuncExecExpression
	observe      func(*batch.Batch) error
}

type rollupBenchmarkInput struct {
	proc *process.Process
	base *batch.Batch
}

func newRollupBenchmarkRunner(
	t testing.TB,
	rows, ndv int, ndvByKey []int, keyCount int, ordered, derivedOrder bool, aggregate string,
) (*rollupBenchmarkRunner, error) {
	if rows <= 0 || ndv <= 0 || keyCount <= 0 {
		return nil, fmt.Errorf("invalid rollup benchmark shape: rows=%d ndv=%d keys=%d", rows, ndv, keyCount)
	}
	if len(ndvByKey) == 0 {
		ndvByKey = make([]int, keyCount)
		for key := range ndvByKey {
			ndvByKey[key] = ndv
		}
	} else {
		if len(ndvByKey) != keyCount {
			return nil, fmt.Errorf("invalid per-key NDV shape: got %d keys, want %d", len(ndvByKey), keyCount)
		}
		for key, value := range ndvByKey {
			if value <= 0 {
				return nil, fmt.Errorf("invalid per-key NDV at key %d: %d", key, value)
			}
		}
	}

	runner := &rollupBenchmarkRunner{
		inputs:       make([]*rollupBenchmarkInput, 0, keyCount+1),
		groupBy:      make([][]*plan.Expr, keyCount+1),
		specs:        make([]*plan.OrderBySpec, keyCount),
		ordered:      ordered,
		derivedOrder: derivedOrder,
		aggregate:    aggregate,
	}
	switch aggregate {
	case "", "count":
		runner.aggs = []aggexec.AggFuncExecExpression{countStarAgg()}
	case "avg":
		runner.aggs = []aggexec.AggFuncExecExpression{avgAgg(int32(keyCount))}
	default:
		return nil, fmt.Errorf("invalid rollup benchmark aggregate: %s", aggregate)
	}
	for prefix := 0; prefix <= keyCount; prefix++ {
		runner.groupBy[prefix] = makeRollupBenchmarkGroupBy(prefix)
	}
	for key := 0; key < keyCount; key++ {
		runner.specs[key] = &plan.OrderBySpec{
			Expr: colExpr(int32(key), types.T_int32),
			Flag: plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST,
		}
	}

	// Keep one independent process/mpool per legacy branch. It models the
	// branch-local aggregate state and lets the parallel case run without
	// sharing mutable operator state.
	for i := 0; i <= keyCount; i++ {
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
		base, err := makeRollupBenchmarkBatch(proc, rows, ndvByKey, keyCount, ordered, aggregate == "avg")
		if err != nil {
			if base != nil {
				base.Clean(proc.Mp())
			}
			// Release batches created for earlier branches as well. The owned
			// process cleanup handles their pools at sub-benchmark teardown, but
			// it must not be asked to reclaim still-referenced input vectors.
			runner.free()
			return nil, err
		}
		runner.inputs = append(runner.inputs, &rollupBenchmarkInput{
			proc: proc,
			base: base,
		})
	}
	return runner, nil
}

func (runner *rollupBenchmarkRunner) free() {
	for _, input := range runner.inputs {
		if input == nil {
			continue
		}
		if input.base != nil {
			input.base.Clean(input.proc.Mp())
			input.base = nil
		}
	}
	runner.inputs = nil
}

func makeRollupBenchmarkGroupBy(prefix int) []*plan.Expr {
	if prefix == 0 {
		return nil
	}
	groupBy := make([]*plan.Expr, prefix)
	for key := range groupBy {
		groupBy[key] = colExpr(int32(key), types.T_int32)
	}
	return groupBy
}

func makeRollupBenchmarkBatch(
	proc *process.Process,
	rows int, ndvByKey []int, keyCount int, ordered bool, withMeasure bool,
) (*batch.Batch, error) {
	if len(ndvByKey) != keyCount {
		return nil, fmt.Errorf("invalid per-key NDV shape: got %d keys, want %d", len(ndvByKey), keyCount)
	}
	columnCount := keyCount
	if withMeasure {
		columnCount++
	}
	bat := batch.NewWithSize(columnCount)
	allValues := make([][]int32, keyCount)
	for key := 0; key < keyCount; key++ {
		values := make([]int32, rows)
		for row := range values {
			// A deterministic permutation keeps the benchmark reproducible across
			// runs and machines. The ordered fixture is sorted below, outside the
			// timed operator path, to model an input property supplied by a scan or
			// an earlier ORDER BY.
			values[row] = int32((row*7919 + key*104729) % ndvByKey[key])
		}
		allValues[key] = values
	}
	if withMeasure {
		values := make([]int32, rows)
		for row := range values {
			values[row] = int32((row*31 + 7) % 1000003)
		}
		allValues = append(allValues, values)
	}
	if ordered {
		order := make([]int, rows)
		for row := range order {
			order[row] = row
		}
		sort.SliceStable(order, func(i, j int) bool {
			left, right := order[i], order[j]
			for key := 0; key < keyCount; key++ {
				if allValues[key][left] != allValues[key][right] {
					return allValues[key][left] < allValues[key][right]
				}
			}
			return left < right
		})
		for key := 0; key < keyCount; key++ {
			sortedValues := make([]int32, rows)
			for row, sourceRow := range order {
				sortedValues[row] = allValues[key][sourceRow]
			}
			allValues[key] = sortedValues
		}
	}
	for key := 0; key < len(allValues); key++ {
		vec := vector.NewVec(types.T_int32.ToType())
		if err := vector.AppendFixedList(vec, allValues[key], nil, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		bat.Vecs[key] = vec
	}
	bat.SetRowCount(rows)
	return bat, nil
}

func (runner *rollupBenchmarkRunner) runSort() (int64, error) {
	source := runner.inputs[0]
	bat, err := source.base.Dup(source.proc.Mp())
	if err != nil {
		return 0, err
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	var inputOp vm.Operator = child
	var order *orderop.Order
	if !runner.ordered {
		order = orderop.NewArgument()
		order.OrderBySpec = runner.specs
		order.AppendChild(child)
		inputOp = order
	}
	group := newGroupOp(source.proc, runner.groupBy[len(runner.groupBy)-1],
		runner.aggs)
	group.SortRollup = true
	group.SpillMem = 1 << 30
	group.AppendChild(inputOp)

	if order != nil {
		if err = order.Prepare(source.proc); err != nil {
			order.Free(source.proc, true, err)
			child.Free(source.proc, true, err)
			return 0, err
		}
	}
	if err = group.Prepare(source.proc); err != nil {
		group.Free(source.proc, true, err)
		if order != nil {
			order.Free(source.proc, true, err)
		}
		child.Free(source.proc, true, err)
		return 0, err
	}
	peak, err := drainRollupBenchmarkOpWithObserver(
		group, source.proc, source.proc.Mp().CurrNB(), runner.observe, group.ctr.mp)
	group.Free(source.proc, err != nil, err)
	if order != nil {
		order.Free(source.proc, err != nil, err)
	}
	child.Free(source.proc, err != nil, err)
	return peak, err
}

// runHashShared models the protocol 49+ grouping-set plan: the input is
// scanned once, expanded into all grouping sets, and consumed by one
// grouping-aware hash aggregate. It intentionally uses the same projection
// metadata as the production grouping-set rewrite so the benchmark measures
// the shared topology rather than a hand-written approximation.
func (runner *rollupBenchmarkRunner) runHashShared() (int64, error) {
	source := runner.inputs[0]
	bat, err := source.base.Dup(source.proc.Mp())
	if err != nil {
		return 0, err
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	var inputOp vm.Operator = child
	var order *orderop.Order
	if runner.derivedOrder {
		order = orderop.NewArgument()
		order.OrderBySpec = runner.specs
		order.AppendChild(child)
		inputOp = order
	}

	groupCount := len(runner.groupBy) - 1
	projectList := make([]*plan.Expr, 0, groupCount+3)
	for key := 0; key < groupCount; key++ {
		projectList = append(projectList, colExpr(int32(key), types.T_int32))
	}
	if runner.aggregate == "avg" {
		projectList = append(projectList, colExpr(int32(groupCount), types.T_int32))
	}
	projectList = append(projectList,
		&plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}},
		},
		&plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}},
		})
	setIDPos := len(projectList) - 1
	expand := projection.NewArgument()
	expand.ProjectList = projectList
	expand.GroupingSetCount = groupCount + 1
	expand.GroupingFlags = make([]bool, 0, (groupCount+1)*groupCount)
	for set := 0; set <= groupCount; set++ {
		for key := 0; key < groupCount; key++ {
			expand.GroupingFlags = append(expand.GroupingFlags, key < groupCount-set)
		}
	}
	expand.AppendChild(inputOp)

	groupBy := make([]*plan.Expr, 0, groupCount+1)
	for key := 0; key < groupCount; key++ {
		groupBy = append(groupBy, colExpr(int32(key), types.T_int32))
	}
	groupBy = append(groupBy, colExpr(int32(setIDPos), types.T_int64))
	group := newGroupOp(source.proc, groupBy, runner.aggs)
	group.DynamicGrouping = true
	group.SpillMem = 1 << 30
	group.AppendChild(expand)

	if order != nil {
		if err = order.Prepare(source.proc); err != nil {
			order.Free(source.proc, true, err)
			child.Free(source.proc, true, err)
			return 0, err
		}
	}
	if err = expand.Prepare(source.proc); err != nil {
		expand.Free(source.proc, true, err)
		if order != nil {
			order.Free(source.proc, true, err)
		}
		child.Free(source.proc, true, err)
		return 0, err
	}
	if err = group.Prepare(source.proc); err != nil {
		group.Free(source.proc, true, err)
		expand.Free(source.proc, true, err)
		if order != nil {
			order.Free(source.proc, true, err)
		}
		child.Free(source.proc, true, err)
		return 0, err
	}
	peak, err := drainRollupBenchmarkOpWithObserver(
		group, source.proc, source.proc.Mp().CurrNB(), runner.observe, group.ctr.mp)
	group.Free(source.proc, err != nil, err)
	expand.Free(source.proc, err != nil, err)
	if order != nil {
		order.Free(source.proc, err != nil, err)
	}
	child.Free(source.proc, err != nil, err)
	return peak, err
}

func (runner *rollupBenchmarkRunner) runHash(parallel bool) (int64, error) {
	if !parallel {
		var peak int64
		for prefix := range runner.inputs {
			current, err := runner.runHashBranch(prefix)
			if err != nil {
				return peak, err
			}
			if current > peak {
				peak = current
			}
		}
		return peak, nil
	}

	peaks := make([]int64, len(runner.inputs))
	errs := make(chan error, len(runner.inputs))
	var wg sync.WaitGroup
	for prefix := range runner.inputs {
		wg.Add(1)
		go func(prefix int) {
			defer wg.Done()
			var err error
			peaks[prefix], err = runner.runHashBranch(prefix)
			if err != nil {
				errs <- err
			}
		}(prefix)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return 0, err
	}
	var peak int64
	for _, branchPeak := range peaks {
		peak += branchPeak
	}
	return peak, nil
}

func (runner *rollupBenchmarkRunner) runHashBranch(prefix int) (int64, error) {
	input := runner.inputs[prefix]
	bat, err := input.base.Dup(input.proc.Mp())
	if err != nil {
		return 0, err
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	inputOp := vm.Operator(child)
	var order *orderop.Order
	if runner.derivedOrder {
		order = orderop.NewArgument()
		order.OrderBySpec = runner.specs
		order.AppendChild(child)
		inputOp = order
	}
	group := newGroupOp(input.proc, runner.groupBy[prefix],
		runner.aggs)
	group.SpillMem = 1 << 30
	group.AppendChild(inputOp)
	if order != nil {
		if err = order.Prepare(input.proc); err != nil {
			order.Free(input.proc, true, err)
			child.Free(input.proc, true, err)
			return 0, err
		}
	}
	if err = group.Prepare(input.proc); err != nil {
		group.Free(input.proc, true, err)
		if order != nil {
			order.Free(input.proc, true, err)
		}
		child.Free(input.proc, true, err)
		return 0, err
	}
	peak, err := drainRollupBenchmarkOp(
		group, input.proc, input.proc.Mp().CurrNB(), group.ctr.mp)
	group.Free(input.proc, err != nil, err)
	if order != nil {
		order.Free(input.proc, err != nil, err)
	}
	child.Free(input.proc, err != nil, err)
	return peak, err
}

func drainRollupBenchmarkOp(
	op vm.Operator,
	proc *process.Process,
	peak int64,
	additional ...*mpool.MPool,
) (int64, error) {
	return drainRollupBenchmarkOpWithObserver(op, proc, peak, nil, additional...)
}

func drainRollupBenchmarkOpWithObserver(
	op vm.Operator,
	proc *process.Process,
	peak int64,
	observe func(*batch.Batch) error,
	additional ...*mpool.MPool,
) (int64, error) {
	for {
		result, err := vm.Exec(op, proc)
		if err != nil {
			return peak, err
		}
		if observe != nil && result.Batch != nil {
			if err := observe(result.Batch); err != nil {
				return peak, err
			}
		}
		current := proc.Mp().CurrNB()
		for _, mp := range additional {
			if mp != nil {
				current += mp.CurrNB()
			}
		}
		if current > peak {
			peak = current
		}
		if result.Status == vm.ExecStop || result.Batch == nil {
			return peak, nil
		}
	}
}
