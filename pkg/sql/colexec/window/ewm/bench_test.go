// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ewm

import "testing"

// The measured cost is the post-implementation responsibility the design
// pins (§9): focused benchmarks on one ordinary singleton path and one
// tied-batch path over a large synthetic partition, each in row-decay and
// event-time form; -benchmem supplies the allocation measurements while
// TestRetainedAccountBytesConstant owns the O(1) state-size assert. The
// per-row expectation is a constant number of small constant-size merge
// steps (no per-row allocations beyond the O(1) retained account; event
// mode adds one exp/expm1 per instant), replacing the naive O(r²)
// per-row re-scan the design tore out.

// benchmarkRowsCount is the large synthetic partition the two paths run
// over.
const benchmarkRowsCount = 1 << 14

func benchDistinctRows(n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		x := float64(i) + 1
		rows[i] = Row{Keys: []KeyVal{IntKey(int64(i))}, X: &x}
	}
	return rows
}

func benchTiedRows(n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		x := float64(i) + 1
		// one giant peer group: every row ties on the ORDER BY key
		rows[i] = Row{Keys: []KeyVal{IntKey(7)}, X: &x}
	}
	return rows
}

// runEwmBenchmark drives one evaluation of the partition per benchmark
// iteration: build the evaluator over the prebuilt synthetic rows,
// pre-scan, then publish in slices of the requested publication shape.
func runEwmBenchmark(
	b *testing.B,
	rows []Row,
	cfg Config,
	slice int,
) {
	b.Helper()
	b.ReportAllocs()
	poll := NOP()
	for i := 0; i < b.N; i++ {
		ev, err := NewEvaluator(rows, groupRows, cfg)
		if err != nil {
			b.Fatalf("new evaluator: %v", err)
		}
		if err := ev.ValidateSamples(); err != nil {
			b.Fatalf("pre-scan: %v", err)
		}
		for start := 0; start < len(rows); start += slice {
			end := start + slice
			if end > len(rows) {
				end = len(rows)
			}
			if err := ev.FillSlice(start, end, poll, func(int, Out) error {
				return nil
			}); err != nil {
				b.Fatalf("fill: %v", err)
			}
		}
	}
}

// BenchmarkEwmSingletonRowDecay is the ordinary singleton path: every
// row its own peer group, published one row per FillSlice.
func BenchmarkEwmSingletonRowDecay(b *testing.B) {
	runEwmBenchmark(b, benchDistinctRows(benchmarkRowsCount),
		Config{Mode: ModeRow, Alpha: 0.5, Adjust: false}, 1)
}

// BenchmarkEwmSingletonEventTime is the event-time singleton path: the
// int64 keys are the event instants, published one row per FillSlice.
func BenchmarkEwmSingletonEventTime(b *testing.B) {
	runEwmBenchmark(b, benchDistinctRows(benchmarkRowsCount),
		Config{Mode: ModeTime, HalfLife: 10, Adjust: false}, 1)
}

// BenchmarkEwmTiedBatchRowDecay is the tied-batch path: one giant row-
// decay peer group published in a single FillSlice.
func BenchmarkEwmTiedBatchRowDecay(b *testing.B) {
	runEwmBenchmark(b, benchTiedRows(benchmarkRowsCount),
		Config{Mode: ModeRow, Alpha: 0.5, Adjust: false}, benchmarkRowsCount)
}

// BenchmarkEwmTiedBatchEventTime is the tied-batch event-time path: one
// giant peer group at a single instant.
func BenchmarkEwmTiedBatchEventTime(b *testing.B) {
	runEwmBenchmark(b, benchTiedRows(benchmarkRowsCount),
		Config{Mode: ModeTime, HalfLife: 10, Adjust: false}, benchmarkRowsCount)
}
