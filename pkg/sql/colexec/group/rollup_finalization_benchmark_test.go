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
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Reuse the physical runner, comparing only the current one-scan algorithms.
// Each timed shape first checks every completed group against source-row SQL
// prefix semantics. The multi shape uses seven independent aggregate states.
func BenchmarkRollupFinalization(b *testing.B) {
	for _, tc := range []struct {
		name      string
		rows, ndv int
		keys      int
		ndvByKey  []int
		ordered   bool
		aggregate string
	}{
		{"ordered_unique_count", 100000, 100000, 3, nil, true, "count"},
		{"ordered_unique_multi", 100000, 100000, 3, nil, true, "multi"},
		{"derived_unique_count", 100000, 100000, 3, nil, false, "count"},
		{"derived_unique_multi", 100000, 100000, 3, nil, false, "multi"},
		{"ordered_asymmetric_count", 100000, 50000, 3, []int{1, 1, 50000}, true, "count"},
		{"ordered_asymmetric_multi", 100000, 50000, 3, []int{1, 1, 50000}, true, "multi"},
		{"derived_asymmetric_count", 100000, 50000, 3, []int{1, 1, 50000}, false, "count"},
		{"derived_asymmetric_multi", 100000, 50000, 3, []int{1, 1, 50000}, false, "multi"},
		{"million_derived_low_count", 1000000, 4, 3, nil, false, "count"},
		{"million_derived_low_avg", 1000000, 4, 2, nil, false, "avg"},
		{"ordered_many_levels_avg", 100000, 2, 12, nil, true, "avg"},
	} {
		for _, algorithm := range []struct {
			name string
			run  func(*rollupBenchmarkRunner) (int64, error)
		}{
			{"sort", (*rollupBenchmarkRunner).runSort},
			{"hash-shared", (*rollupBenchmarkRunner).runHashShared},
		} {
			b.Run(tc.name+"/"+algorithm.name, func(b *testing.B) {
				runner, err := newRollupBenchmarkRunner(b, tc.rows, tc.ndv, tc.ndvByKey,
					tc.keys, tc.ordered, !tc.ordered, tc.aggregate)
				if err != nil {
					b.Fatal(err)
				}
				defer runner.free()
				if err := checkRollupFinalization(runner, algorithm.run); err != nil {
					b.Fatal(err)
				}
				runtime.GC() // Discard oracle scratch before timed allocation/GC work.
				b.ReportAllocs()
				var peak int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					current, err := algorithm.run(runner)
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

// The oracle deliberately does not use the grouping codec or either aggregate
// implementation. Inputs contain no source NULLs, so rolled NULLs must match
// GROUPING metadata exactly. Large cases check every output, not a checksum.
func checkRollupFinalization(runner *rollupBenchmarkRunner, run func(*rollupBenchmarkRunner) (int64, error)) error {
	type key struct {
		prefix int
		values [12]int32
	}
	type totals struct {
		count int64
		sums  [12]int64
	}
	base := runner.inputs[0].base
	keys := len(runner.groupBy) - 1
	if keys > 12 {
		return fmt.Errorf("oracle supports at most 12 keys")
	}
	columns := make([][]int32, len(base.Vecs))
	for i, vec := range base.Vecs {
		columns[i] = vector.MustFixedColWithTypeCheck[int32](vec)
	}
	expected := make(map[key]totals)
	for row := 0; row < base.RowCount(); row++ {
		for prefix := 0; prefix <= keys; prefix++ {
			k := key{prefix: prefix}
			for i := 0; i < prefix; i++ {
				k.values[i] = columns[i][row]
			}
			total := expected[k]
			total.count++
			if runner.aggregate == "avg" {
				total.sums[0] += int64(columns[keys][row])
			} else if runner.aggregate == "multi" {
				for i := 0; i < keys; i++ {
					total.sums[i] += int64(columns[keys+i][row])
				}
			}
			expected[k] = total
		}
	}
	seen := make(map[key]bool, len(expected))
	runner.observe = func(out *batch.Batch) error {
		start := len(out.Vecs) - len(runner.aggs)
		for row := 0; row < out.RowCount(); row++ {
			k := key{prefix: keys}
			for i := 0; i < keys; i++ {
				rolled := out.Vecs[i].GetGrouping().Contains(uint64(row))
				if rolled != out.Vecs[i].GetNulls().Contains(uint64(row)) {
					return fmt.Errorf("invalid grouping provenance")
				}
				if rolled {
					if k.prefix == keys {
						k.prefix = i
					}
				} else {
					if k.prefix != keys {
						return fmt.Errorf("non-prefix grouping")
					}
					k.values[i] = vector.GetFixedAtNoTypeCheck[int32](out.Vecs[i], row)
				}
			}
			total, ok := expected[k]
			if !ok || seen[k] {
				return fmt.Errorf("unexpected or duplicate group %v", k)
			}
			for i := range runner.aggs {
				vec := out.Vecs[start+i]
				if vec.GetNulls().Contains(uint64(row)) {
					return fmt.Errorf("unexpected NULL aggregate for %v", k)
				}
				if runner.aggregate == "avg" || runner.aggregate == "multi" && i > 0 && i%2 == 0 {
					sumIndex := 0
					if runner.aggregate == "multi" {
						sumIndex = (i - 1) / 2
					}
					// Integer AVG exposes DECIMAL128 with four fractional digits.
					// All fixture values are nonnegative; round the source-row
					// rational independently using integer arithmetic.
					if vec.GetType().Oid != types.T_decimal128 || vec.GetType().Scale != 4 {
						return fmt.Errorf("unexpected integer AVG type: %v", vec.GetType())
					}
					want := types.Decimal128FromInt64((total.sums[sumIndex]*10000 + total.count/2) / total.count)
					got := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row)
					if got != want {
						return fmt.Errorf("wrong AVG for %v: got %v want %v", k, got.Format(4), want.Format(4))
					}
				} else {
					want := total.count
					if i > 0 {
						want = total.sums[(i-1)/2]
					}
					if got := vector.GetFixedAtNoTypeCheck[int64](vec, row); got != want {
						return fmt.Errorf("wrong aggregate %d for %v: got %v want %v", i, k, got, want)
					}
				}
			}
			seen[k] = true
		}
		return nil
	}
	defer func() { runner.observe = nil }()
	if _, err := run(runner); err != nil {
		return err
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("returned %d groups, want %d", len(seen), len(expected))
	}
	return nil
}
