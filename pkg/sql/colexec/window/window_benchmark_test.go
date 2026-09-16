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

package window

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	orderop "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	execpartition "github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// BenchmarkWindowHashPartitionAcceptance measures the real Window consumer,
// not only the blocking Partition prerequisite. The matrix is intentionally
// kept in the benchmark source so every reported result has an exact, rerunnable
// shape: 1K/64K/1M rows, 1/1%/100% NDV, one/three fixed or variable keys, and
// ordered/unordered windows. Each input is split into two upstream batches to
// exercise blocking/finalization across batches. The single MockOperator does
// not model a compiled multi-scope Merge and this benchmark is not evidence for
// the default-enable gate.
func BenchmarkWindowHashPartitionAcceptance(b *testing.B) {
	for _, rows := range []int{1 << 10, 1 << 16, 1 << 20} {
		for _, ndv := range []int{1, max(1, rows/100), rows} {
			for _, keyCount := range []int{1, 3} {
				for _, varlen := range []bool{false, true} {
					for _, ordered := range []bool{false, true} {
						for _, algorithm := range []string{"sort", "hash"} {
							name := fmt.Sprintf("rows=%d/ndv=%d/keys=%d/%s/%s/%s",
								rows, ndv, keyCount,
								map[bool]string{false: "fixed", true: "varlen"}[varlen],
								map[bool]string{false: "unordered", true: "ordered"}[ordered],
								algorithm)
							b.Run(name, func(b *testing.B) {
								b.ReportAllocs()
								var peak uint64
								for i := 0; i < b.N; i++ {
									b.StopTimer()
									pipeline := newWindowHashAcceptancePipeline(
										b, rows, ndv, keyCount, varlen, ordered,
										algorithm == "hash", 1<<30,
									)
									b.StartTimer()
									token := pipeline.mp.StartResourcePeakEpoch()
									if token == nil {
										b.Fatal("failed to start resource peak epoch")
									}
									rowsSeen := 0
									for {
										result, err := vm.Exec(pipeline.window, pipeline.proc)
										if err != nil {
											b.Fatal(err)
										}
										if result.Batch == nil {
											break
										}
										rowsSeen += result.Batch.RowCount()
									}
									measuredPeak, ok := pipeline.mp.EndResourcePeakEpoch(token)
									if !ok {
										b.Fatal("failed to end resource peak epoch")
									}
									b.StopTimer()
									if rowsSeen != rows {
										b.Fatalf("Window emitted %d rows, want %d", rowsSeen, rows)
									}
									peak = max(peak, measuredPeak)
									pipeline.free()
								}
								b.ReportMetric(float64(peak), "peak-mpool-B")
							})
						}
					}
				}
			}
		}
	}
}

type windowHashAcceptancePipeline struct {
	proc      *process.Process
	mp        *mpool.MPool
	window    *Window
	partition *execpartition.Partition
	order     *orderop.Order
	child     *colexec.MockOperator
	valuePos  int
}

func (p *windowHashAcceptancePipeline) free() {
	p.window.Free(p.proc, false, nil)
	p.partition.Free(p.proc, false, nil)
	if p.order != nil {
		p.order.Free(p.proc, false, nil)
	}
	p.child.Free(p.proc, false, nil)
	p.proc.Free()
}

func newWindowHashAcceptancePipeline(
	t testing.TB,
	rows, ndv, keyCount int,
	varlen, ordered, useHash bool,
	spillMem int64,
) *windowHashAcceptancePipeline {
	t.Helper()
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	inputs, partitionSpecs, partitionExprs, valuePos := makeWindowHashAcceptanceInput(
		t, proc, rows, ndv, keyCount, varlen,
	)
	child := colexec.NewMockOperator().WithBatchs(inputs)
	partition := &execpartition.Partition{
		OrderBySpecs: partitionSpecs,
		SpillMem:     spillMem,
	}
	var order *orderop.Order
	if useHash {
		partition.Algorithm = plan.Node_PARTITION_ALGORITHM_HASH
		partition.AppendChild(child)
	} else {
		order = orderop.NewArgument()
		order.OrderBySpec = partitionSpecs
		order.AppendChild(child)
		if err := order.Prepare(proc); err != nil {
			t.Fatal(err)
		}
		partition.AppendChild(order)
	}
	if err := partition.Prepare(proc); err != nil {
		t.Fatal(err)
	}

	spec := makeWindowSpec()
	w := spec.GetW()
	// Use a linear frame so the acceptance matrix measures the partition
	// consumer rather than the quadratic work of an unbounded aggregate frame.
	w.Frame = makeCurrentRowFrame()
	w.PartitionBy = partitionExprs
	if ordered {
		w.OrderBy = []*plan.OrderBySpec{{
			Expr: newColExprWithType(int32(valuePos), types.T_int32.ToType()),
		}}
	}
	window := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(int32(valuePos))},
	}
	window.AppendChild(partition)
	if err := window.Prepare(proc); err != nil {
		t.Fatal(err)
	}
	return &windowHashAcceptancePipeline{
		proc:      proc,
		mp:        mp,
		window:    window,
		partition: partition,
		order:     order,
		child:     child,
		valuePos:  valuePos,
	}
}

func TestWindowHashPartitionAcceptanceConsumer(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(map[bool]string{false: "unordered", true: "ordered"}[ordered], func(t *testing.T) {
			var checksums [2]int64
			for algorithm, useHash := range []bool{false, true} {
				func() {
					pipeline := newWindowHashAcceptancePipeline(
						t, 4096, 64, 1, false, ordered, useHash, 1<<30,
					)
					defer pipeline.free()
					rowsSeen := 0
					for {
						result, err := vm.Exec(pipeline.window, pipeline.proc)
						require.NoError(t, err)
						if result.Batch == nil {
							break
						}
						rowsSeen += result.Batch.RowCount()
						values := vector.MustFixedColWithTypeCheck[int64](
							result.Batch.Vecs[pipeline.valuePos+1],
						)
						for _, value := range values {
							checksums[algorithm] += value
						}
					}
					require.Equal(t, 4096, rowsSeen)
				}()
			}
			require.Equal(t, checksums[0], checksums[1])
			require.NotZero(t, checksums[0])
		})
	}
}

func makeWindowHashAcceptanceInput(
	t testing.TB,
	proc *process.Process,
	rows, ndv, keyCount int,
	varlen bool,
) ([]*batch.Batch, []*plan.OrderBySpec, []*plan.Expr, int) {
	t.Helper()
	if ndv < 1 {
		ndv = 1
	}
	split := rows / 2
	if split == 0 {
		split = rows
	}
	parts := []int{split, rows - split}
	if parts[1] == 0 {
		parts = parts[:1]
	}
	partitionSpecs := make([]*plan.OrderBySpec, keyCount)
	partitionExprs := make([]*plan.Expr, keyCount)
	for key := 0; key < keyCount; key++ {
		if varlen {
			partitionExprs[key] = newColExprWithType(int32(key), types.T_varchar.ToType())
		} else {
			partitionExprs[key] = newColExprWithType(int32(key), types.T_int32.ToType())
		}
		partitionSpecs[key] = &plan.OrderBySpec{Expr: partitionExprs[key]}
	}
	valuePos := keyCount
	inputs := make([]*batch.Batch, 0, len(parts))
	start := 0
	for _, partRows := range parts {
		bat := batch.NewWithSize(keyCount + 1)
		for key := 0; key < keyCount; key++ {
			if varlen {
				vec := vector.NewVec(types.T_varchar.ToType())
				values := make([][]byte, partRows)
				for row := range values {
					values[row] = strconv.AppendInt(nil, int64((start+row)*7919+key*104729)%int64(ndv), 10)
				}
				if err := vector.AppendBytesList(vec, values, nil, proc.Mp()); err != nil {
					t.Fatal(err)
				}
				bat.Vecs[key] = vec
			} else {
				vec := vector.NewVec(types.T_int32.ToType())
				values := make([]int32, partRows)
				for row := range values {
					values[row] = int32((start+row)*7919+key*104729) % int32(ndv)
				}
				if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
					t.Fatal(err)
				}
				bat.Vecs[key] = vec
			}
		}
		values := make([]int32, partRows)
		for row := range values {
			values[row] = int32(start + row + 1)
		}
		bat.Vecs[valuePos] = vector.NewVec(types.T_int32.ToType())
		if err := vector.AppendFixedList(bat.Vecs[valuePos], values, nil, proc.Mp()); err != nil {
			t.Fatal(err)
		}
		bat.SetRowCount(partRows)
		inputs = append(inputs, bat)
		start += partRows
	}
	return inputs, partitionSpecs, partitionExprs, valuePos
}

// BenchmarkWindowFirstBatch models the #23107 LIMIT consumer: it asks Window
// for only the first output batch of a large cumulative frame and then resets
// the pipeline.
func BenchmarkWindowFirstBatch(b *testing.B) {
	const rows = colexec.DefaultBatchSize * 8

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
		values := make([]int32, rows)
		for row := range values {
			values[row] = int32(row + 1)
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(rows)

		spec := makeWindowSpec()
		spec.Expr.(*plan.Expr_W).W.Frame = makeFiniteCumulativeFrame(2147483647)
		arg := &Window{
			WinSpecList: []*plan.Expr{spec},
			Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{Idx: 0},
			},
		}
		op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg.AppendChild(op)

		if err := arg.Prepare(proc); err != nil {
			b.Fatal(err)
		}
		result, err := vm.Exec(arg, proc)
		if err != nil {
			b.Fatal(err)
		}
		if result.Batch == nil || result.Batch.RowCount() == 0 {
			b.Fatal("window returned no rows")
		}

		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		op.Free(proc, false, nil)
		proc.Free()
		if got := proc.Mp().CurrNB(); got != 0 {
			b.Fatalf("mpool leak: %d bytes", got)
		}
	}
}

// BenchmarkWindowBoundedRowsSum covers the finite sliding shape from #27352.
// Runtime should remain approximately flat as the frame width grows because
// each output row performs at most one add and one remove.
func BenchmarkWindowBoundedRowsSum(b *testing.B) {
	const rows = 80_000
	widths := []struct {
		name  string
		bound uint64
	}{
		{name: "preceding_0", bound: 0},
		{name: "preceding_31", bound: 31},
		{name: "preceding_128", bound: 128},
		{name: "preceding_512", bound: 512},
		{name: "preceding_1024", bound: 1024},
	}

	for _, width := range widths {
		b.Run(width.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
				values := make([]int32, rows)
				for row := range values {
					values[row] = 1
				}
				input := batch.NewWithSize(1)
				input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				input.SetRowCount(rows)

				spec := makeWindowSpec()
				spec.Expr.(*plan.Expr_W).W.Frame = makeFiniteCumulativeFrame(width.bound)
				arg := &Window{
					WinSpecList: []*plan.Expr{spec},
					Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{Idx: 0},
					},
				}
				op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				arg.AppendChild(op)

				if err := arg.Prepare(proc); err != nil {
					b.Fatal(err)
				}
				var last int64
				for {
					result, err := vm.Exec(arg, proc)
					if err != nil {
						b.Fatal(err)
					}
					if result.Batch == nil {
						break
					}
					values := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
					last = values[len(values)-1]
				}
				if want := int64(width.bound + 1); last != want {
					b.Fatalf("last sliding sum: got %d, want %d", last, want)
				}

				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				op.Free(proc, false, nil)
				proc.Free()
				if got := proc.Mp().CurrNB(); got != 0 {
					b.Fatalf("mpool leak: %d bytes", got)
				}
			}
		})
	}
}

// BenchmarkWindowBoundedRangeAvg covers #13008's finite RANGE AVG evaluator.
// Repeated order keys make the ordinary implementation increasingly expensive
// because it refills the complete frame for every peer row. The sliding path
// instead searches and updates once per peer while still emitting every row.
func BenchmarkWindowBoundedRangeAvg(b *testing.B) {
	const rows = 80_000
	peers := []struct {
		name string
		size int
	}{
		{name: "peer_1", size: 1},
		{name: "peer_16", size: 16},
		{name: "peer_64", size: 64},
	}

	for _, peer := range peers {
		b.Run(peer.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
				values := make([]int32, rows)
				boundaries := make([]int64, 0, rows/peer.size)
				for row := range values {
					values[row] = int32(row / peer.size)
					if row%peer.size == 0 {
						boundaries = append(boundaries, int64(row))
					}
				}
				input := makeInt32Batch(proc.Mp(), values)
				spec := makeWindowSpec()
				spec.GetW().Frame = makeBoundedRangeFrame(2, 2)
				arg := &Window{
					WinSpecList: []*plan.Expr{spec},
					Aggs: []aggexec.AggFuncExecExpression{
						newTypedAvgAggExpr(b, 0, types.T_int32.ToType()),
					},
				}
				ctr := &container{
					bat:       input,
					os:        boundaries,
					orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{input.Vecs[0]}}},
					aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{input.Vecs[0]}}},
				}
				b.StartTimer()

				var last float64
				for start := 0; start < rows; start += colexec.DefaultBatchSize {
					end := min(start+colexec.DefaultBatchSize, rows)
					result, err := ctr.processAggregateFuncRange(0, arg, proc, start, end)
					if err != nil {
						b.Fatal(err)
					}
					resultValues := vector.MustFixedColWithTypeCheck[float64](result)
					last = resultValues[len(resultValues)-1]
					result.Free(proc.Mp())
				}
				b.StopTimer()

				if want := float64(rows/peer.size - 2); last != want {
					b.Fatalf("last sliding avg: got %v, want %v", last, want)
				}
				input.Clean(proc.Mp())
				proc.Free()
				if got := proc.Mp().CurrNB(); got != 0 {
					b.Fatalf("mpool leak: %d bytes", got)
				}
			}
		})
	}
}

// BenchmarkCumulativeMaxPartitionShapes keeps the allocation cost of the
// cumulative running path visible for the three materially different partition
// shapes: high-cardinality singleton partitions, mixed small/large partitions,
// and large partitions whose saved prefix work dominates reset overhead.
func BenchmarkCumulativeMaxPartitionShapes(b *testing.B) {
	const rows = 2048
	tests := []struct {
		name       string
		partitions func() []int64
	}{
		{
			name: "singleton",
			partitions: func() []int64 {
				starts := make([]int64, rows)
				for i := range starts {
					starts[i] = int64(i)
				}
				return starts
			},
		},
		{
			name: "mixed",
			partitions: func() []int64 {
				starts := make([]int64, 0, rows/128)
				for start := 0; start < rows; {
					starts = append(starts, int64(start))
					start++
					if start < rows {
						starts = append(starts, int64(start))
						start += min(255, rows-start)
					}
				}
				return starts
			},
		},
		{
			name: "large",
			partitions: func() []int64 {
				starts := make([]int64, 0, rows/256)
				for start := 0; start < rows; start += 256 {
					starts = append(starts, int64(start))
				}
				return starts
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			var mpoolAllocBytes, mpoolAllocs int64
			for i := 0; i < b.N; i++ {
				proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
				values := make([]int32, rows)
				for row := range values {
					values[row] = int32(rows - row)
				}
				input := makeInt32Batch(proc.Mp(), values)
				spec := makeWindowSpec()
				spec.GetW().Frame = makeCumulativeFrame()
				arg := &Window{
					WinSpecList: []*plan.Expr{spec},
					Aggs: []aggexec.AggFuncExecExpression{
						newTypedMaxAggExpr(b, 0, *input.Vecs[0].GetType()),
					},
				}
				ctr := &container{
					bat: input,
					ps:  test.partitions(),
					aggVecs: []colexec.ExprEvalVector{{
						Vec: []*vector.Vector{input.Vecs[0]},
					}},
				}

				for start := 0; start < rows; start += colexec.DefaultBatchSize {
					end := min(start+colexec.DefaultBatchSize, rows)
					result, err := ctr.processAggregateFuncRange(0, arg, proc, start, end)
					if err != nil {
						b.Fatal(err)
					}
					result.Free(proc.Mp())
				}

				input.Clean(proc.Mp())
				proc.Free()
				mpoolAllocBytes += proc.Mp().Stats().NumAllocBytes.Load()
				mpoolAllocs += proc.Mp().Stats().NumAlloc.Load()
				if got := proc.Mp().CurrNB(); got != 0 {
					b.Fatalf("mpool leak: %d bytes", got)
				}
			}
			b.ReportMetric(float64(mpoolAllocBytes)/float64(b.N), "mpool-bytes/op")
			b.ReportMetric(float64(mpoolAllocs)/float64(b.N), "mpool-allocs/op")
		})
	}
}
