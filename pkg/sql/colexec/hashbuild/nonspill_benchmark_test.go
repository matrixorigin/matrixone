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

package hashbuild

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	nonSpillBenchmarkBatchCount = 16
	nonSpillBenchmarkBatchRows  = 8192
	nonSpillBenchmarkJoinMapTag = int32(101)
	nonSpillBenchmarkFilterTag  = int32(102)
)

// BenchmarkHashBuildNonSpillE2E measures the complete resident HashBuild Call
// path: child pulls, budgeted batch copies, hashmap construction, the legal
// shuffle runtime-filter publication, and JoinMap publication. Input creation,
// Prepare, dependency consumption, and ownership cleanup are outside the timed
// region so base/head comparisons isolate the operator body.
//
// Run a statistically useful A/B sample with:
//
//	taskset -c 4 .agents/skills/mo-dev/scripts/mo-cgo-test \
//	  -run '^$' -bench '^BenchmarkHashBuildNonSpillE2E$' \
//	  -benchtime=1s -count=10 -cpu=1 -benchmem \
//	  ./pkg/sql/colexec/hashbuild
func BenchmarkHashBuildNonSpillE2E(b *testing.B) {
	benchmarks := []struct {
		name                string
		typ                 types.Type
		varcharPayloadBytes int
		computed            bool
	}{
		{name: "INT64", typ: types.T_int64.ToType()},
		{name: "INT64_PLUS_ZERO", typ: types.T_int64.ToType(), computed: true},
		{name: "VARCHAR_INLINE", typ: types.T_varchar.ToType()},
		{
			name:                "VARCHAR_AREA_128",
			typ:                 types.T_varchar.ToType(),
			varcharPayloadBytes: 128,
		},
	}
	for _, benchmark := range benchmarks {
		b.Run("Shuffle/"+benchmark.name, func(b *testing.B) {
			benchmarkHashBuildNonSpillE2E(
				b, benchmark.typ, benchmark.varcharPayloadBytes, benchmark.computed)
		})
	}
}

func benchmarkHashBuildNonSpillE2E(
	b *testing.B,
	keyType types.Type,
	varcharPayloadBytes int,
	computed bool,
) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Base.Lim.Size = 4 << 30
	proc.Base.Lim.BatchRows = nonSpillBenchmarkBatchRows
	proc.Base.Lim.BatchSize = 64 << 20

	inputs := makeNonSpillBenchmarkBatches(
		b, proc, keyType, varcharPayloadBytes)
	var inputBytes int64
	for _, input := range inputs {
		inputBytes += int64(input.Size())
	}
	budget, err := proc.GetHashBuildBudget()
	require.NoError(b, err)
	require.NotNil(b, budget)
	keyExpr := nonSpillBenchmarkColumnExpr(0, keyType)
	if computed {
		keyExpr, err = plan2.BindFuncExprImplByPlanExpr(
			proc.Ctx,
			"+",
			[]*plan.Expr{keyExpr, plan2.MakePlan2Int64ConstExprWithType(0)},
		)
		require.NoError(b, err)
	}

	b.SetBytes(inputBytes)
	b.ReportAllocs()
	b.ReportMetric(
		float64(nonSpillBenchmarkBatchCount*nonSpillBenchmarkBatchRows),
		"rows/op",
	)
	b.ResetTimer()
	b.StopTimer()

	for i := 0; i < b.N; i++ {
		child := colexec.NewMockOperator().WithBatchs(inputs)
		arg := newNonSpillBenchmarkHashBuild(keyExpr, child)
		require.NoError(b, child.Prepare(proc))
		require.NoError(b, arg.Prepare(proc))
		before := budget.Snapshot()

		b.StartTimer()
		result, execErr := vm.Exec(arg, proc)
		b.StopTimer()

		require.NoError(b, execErr)
		require.Equal(b, vm.ExecStop, result.Status)
		joinMap := receiveNonSpillBenchmarkJoinMap(b, proc)
		receiveNonSpillBenchmarkRuntimeFilter(b, proc)
		require.False(b, joinMap.IsSpilled())
		require.Equal(
			b,
			int64(nonSpillBenchmarkBatchCount*nonSpillBenchmarkBatchRows),
			joinMap.GetRowCount(),
		)
		require.Equal(
			b,
			uint64(nonSpillBenchmarkBatchCount*nonSpillBenchmarkBatchRows),
			joinMap.GetGroupCount(),
			"benchmark keys must remain unique",
		)
		afterBuild := budget.Snapshot()
		require.Greater(b, afterBuild.ReserveCount, before.ReserveCount)
		require.Equal(b, before.RejectCount, afterBuild.RejectCount)
		require.Greater(b, afterBuild.Used, uint64(0))
		require.Zero(
			b,
			arg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"],
		)

		joinMap.Free()
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		child.Release()
		require.Zero(b, budget.Used())
		proc.GetMessageBoard().Reset()
		proc.SetMessageBoard(message.NewMessageBoard())
	}

	proc.GetMessageBoard().Reset()
	proc.SetStmtProfile(nil)
	for _, input := range inputs {
		input.Clean(proc.Mp())
	}
	if fs := proc.GetFileService(); fs != nil {
		fs.Close(proc.Ctx)
	}
	proc.Free()
	require.Zero(b, proc.Mp().CurrNB())
}

func newNonSpillBenchmarkHashBuild(
	keyExpr *plan.Expr,
	child vm.Operator,
) *HashBuild {
	arg := &HashBuild{
		NeedHashMap:    true,
		NeedBatches:    true,
		IsShuffle:      true,
		Conditions:     []*plan.Expr{keyExpr},
		JoinMapTag:     nonSpillBenchmarkJoinMapTag,
		JoinMapRefCnt:  1,
		ShuffleIdx:     0,
		SpillThreshold: math.MaxInt64,
		DelColIdx:      -1,
		RuntimeFilterSpec: &plan.RuntimeFilterSpec{
			Tag:        nonSpillBenchmarkFilterTag,
			UpperLimit: math.MaxInt32,
			Expr:       keyExpr,
		},
		DedupDeleteMarkerColIdx: -1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	arg.SetChildren([]vm.Operator{child})
	return arg
}

func nonSpillBenchmarkColumnExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func makeNonSpillBenchmarkBatches(
	b testing.TB,
	proc *process.Process,
	typ types.Type,
	varcharPayloadBytes int,
) []*batch.Batch {
	inputs := make([]*batch.Batch, nonSpillBenchmarkBatchCount)
	for batchIndex := range inputs {
		input := batch.NewWithSize(1)
		vec := vector.NewVec(typ)
		rowBase := batchIndex * nonSpillBenchmarkBatchRows
		for row := 0; row < nonSpillBenchmarkBatchRows; row++ {
			value := rowBase + row
			switch typ.Oid {
			case types.T_int64:
				require.NoError(
					b,
					vector.AppendFixed(vec, int64(value), false, proc.Mp()),
				)
			case types.T_varchar:
				key := "key-" + strconv.Itoa(value)
				if varcharPayloadBytes > len(key) {
					key += strings.Repeat("x", varcharPayloadBytes-len(key))
				}
				require.NoError(
					b,
					vector.AppendBytes(
						vec,
						[]byte(key),
						false,
						proc.Mp(),
					),
				)
			default:
				b.Fatalf("unsupported non-spill benchmark type %s", typ.Oid)
			}
		}
		input.SetVector(0, vec)
		input.SetRowCount(nonSpillBenchmarkBatchRows)
		inputs[batchIndex] = input
	}
	return inputs
}

func receiveNonSpillBenchmarkJoinMap(
	b testing.TB,
	proc *process.Process,
) *message.JoinMap {
	result, err := message.ReceiveJoinMapResult(
		nonSpillBenchmarkJoinMapTag,
		true,
		0,
		proc.GetMessageBoard(),
		proc.Ctx,
	)
	require.NoError(b, err)
	require.True(b, result.IsSuccess())
	joinMap := result.JoinMap()
	require.NotNil(b, joinMap)
	return joinMap
}

func receiveNonSpillBenchmarkRuntimeFilter(
	b testing.TB,
	proc *process.Process,
) {
	receiver := message.NewMessageReceiver(
		[]int32{nonSpillBenchmarkFilterTag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	messages, done, err := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(b, err)
	require.False(b, done)
	require.Len(b, messages, 1)
	runtimeFilter, ok := messages[0].(message.RuntimeFilterMessage)
	require.True(b, ok)
	require.Equal(b, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	runtimeFilter.Destroy()
}
