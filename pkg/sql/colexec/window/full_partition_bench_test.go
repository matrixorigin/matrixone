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

package window

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// BenchmarkWindowFullPartitionSum exercises a complete decimal partition,
// including output materialization, without relying on wall-clock assertions.
func BenchmarkWindowFullPartitionSum(b *testing.B) {
	for _, singleton := range []bool{false, true} {
		name := "one_partition"
		if singleton {
			name = "singleton_partitions"
		}
		for _, reference := range []bool{false, true} {
			mode := "/reused"
			if reference {
				mode = "/general_frame"
			}
			b.Run(name+mode, func(b *testing.B) {
				benchmarkWindowFullPartitionSum(b, singleton, reference)
			})
		}
	}
}

func benchmarkWindowFullPartitionSum(b *testing.B, singleton, reference bool) {
	proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(b, proc.Mp().CurrNB()) }()
	typ := types.New(types.T_decimal128, 23, 2)
	const rows = 2048
	values := make([]types.Decimal128, rows)
	for i := range values {
		values[i] = types.Decimal128{B0_63: uint64(i + 1)}
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.NewDecimal128Vector(rows, typ, proc.Mp(), false, nil, values)
	bat.SetRowCount(rows)
	defer bat.Clean(proc.Mp())
	e, err := function.GetFunctionByName(context.Background(), "sum", []types.Type{typ})
	require.NoError(b, err)
	arg := &Window{
		WinSpecList: []*plan.Expr{makeAggWindowSpec("sum")},
		Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
			e.GetEncodedOverloadID(), false, []*plan.Expr{newColExprWithType(0, typ)}, nil)},
	}
	arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "window")
	ctr := &container{bat: bat, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	if singleton {
		ctr.ps = make([]int64, rows)
		for i := range ctr.ps {
			ctr.ps[i] = int64(i)
		}
	}
	if reference {
		// A finite bound covering the entire input is semantically identical,
		// but uses the general evaluator rather than full-partition reuse.
		arg.WinSpecList[0].GetW().Frame.Start = makeFiniteCumulativeFrame(rows).Start
	}
	defer ctr.freeFullPartitionResults(proc.Mp())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, rows)
		if err != nil {
			b.Fatal(err)
		}
		result.Free(proc.Mp())
	}
	b.StopTimer()
}
