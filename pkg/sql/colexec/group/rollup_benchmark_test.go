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
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BenchmarkRollupAlgorithms measures the physical work that is specific to a
// ROLLUP implementation. The unordered sort case includes the ORDER operator;
// the ordered sort case reuses the pre-sorted input. The hash cases execute
// every grouping-set branch over the same materialized input.
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
		{name: "large_ordered_avg_many_levels", rows: 100000, ndv: 2, keyCount: 12, ordered: true, aggregate: "avg"},
		{name: "large_ordered_very_many_levels", rows: 100000, ndv: 2, keyCount: 20, ordered: true},
		{name: "large_ordered_extreme_levels", rows: 100000, ndv: 2, keyCount: 32, ordered: true},
		{name: "million_ordered_low_ndv", rows: 1000000, ndv: 4, keyCount: 3, ordered: true},
		// This models:
		//   SELECT ... FROM (SELECT ... ORDER BY k1, k2, k3) d
		//   GROUP BY k1, k2, k3 WITH ROLLUP
		// The sort path pays for the derived ORDER BY once. The hash path sorts
		// every grouping-set branch, matching the current expanded plan shape.
		{name: "million_derived_order_low_ndv", rows: 1000000, ndv: 4, keyCount: 3, derivedOrder: true},
		{name: "million_derived_order_avg_low_ndv", rows: 1000000, ndv: 4, keyCount: 2, derivedOrder: true, aggregate: "avg"},
	}

	for _, tc := range cases {
		for _, algorithm := range []string{"sort", "hash-serial", "hash-parallel"} {
			name := fmt.Sprintf("%s/%s", tc.name, algorithm)
			b.Run(name, func(b *testing.B) {
				runner, err := newRollupBenchmarkRunner(b, tc.rows, tc.ndv, tc.keyCount,
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

type rollupBenchmarkRunner struct {
	inputs       []*rollupBenchmarkInput
	groupBy      [][]*plan.Expr
	specs        []*plan.OrderBySpec
	ordered      bool
	derivedOrder bool
	aggs         []aggexec.AggFuncExecExpression
}

type rollupBenchmarkInput struct {
	proc *process.Process
	base *batch.Batch
}

func newRollupBenchmarkRunner(
	t testing.TB,
	rows, ndv, keyCount int, ordered, derivedOrder bool, aggregate string,
) (*rollupBenchmarkRunner, error) {
	if rows <= 0 || ndv <= 0 || keyCount <= 0 {
		return nil, fmt.Errorf("invalid rollup benchmark shape: rows=%d ndv=%d keys=%d", rows, ndv, keyCount)
	}

	runner := &rollupBenchmarkRunner{
		inputs:       make([]*rollupBenchmarkInput, 0, keyCount+1),
		groupBy:      make([][]*plan.Expr, keyCount+1),
		specs:        make([]*plan.OrderBySpec, keyCount),
		ordered:      ordered,
		derivedOrder: derivedOrder,
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
		proc := testutil.NewProcessWithMPool(t, "", mp)
		base, err := makeRollupBenchmarkBatch(proc, rows, ndv, keyCount, ordered, aggregate == "avg")
		if err != nil {
			proc.Free()
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
		input.proc.Free()
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
	rows, ndv, keyCount int, ordered bool, withMeasure bool,
) (*batch.Batch, error) {
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
			values[row] = int32((row*7919 + key*104729) % ndv)
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
	peak, err := drainRollupBenchmarkOp(
		group, source.proc, source.proc.Mp().CurrNB(), group.ctr.mp)
	group.Free(source.proc, err != nil, err)
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
	for {
		result, err := vm.Exec(op, proc)
		if err != nil {
			return peak, err
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
