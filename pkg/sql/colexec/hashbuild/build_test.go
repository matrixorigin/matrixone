// Copyright 2021 Matrix Origin
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
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

func runtimeFilterPlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func rawRuntimeFilterSpec(tag, upperLimit int32, typ types.Type) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperLimit,
		BuildExpr:   newExpr(0, typ),
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		ProbeType:   runtimeFilterPlanType(typ),
	}
}

// add unit tests for cases
type buildTestCase struct {
	arg    *HashBuild
	marg   *merge.Merge
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

func makeTestCases(t *testing.T) []buildTestCase {
	return []buildTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int8.ToType()},
			[]*plan.Expr{
				newExpr(0, types.T_int8.ToType()),
			}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int8.ToType()},
			[]*plan.Expr{
				newExpr(0, types.T_int8.ToType()),
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestBuild(t *testing.T) {
	for _, tc := range makeTestCases(t)[:1] {
		err := tc.marg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.SetChildren([]vm.Operator{tc.marg})
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(newBatch(tc.types, tc.proc, Rows), nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
		ok, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, ok.Status == vm.ExecStop)

		tc.arg.Reset(tc.proc, false, nil)
		tc.marg.Reset(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()

		err = tc.marg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(newBatch(tc.types, tc.proc, Rows), nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

		ok, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, ok.Status == vm.ExecStop)

		tc.arg.Free(tc.proc, false, nil)
		tc.marg.Reset(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
	}
}

func TestBroadcastBudgetFailureUnblocksAllConsumers(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	// testutil's process limit is 1 MiB. The build input itself belongs to the
	// upstream operator; adopting a copy larger than that must fail before the
	// HashBuild starts an unbounded retained allocation.
	bat := newBatch(tc.types, tc.proc, 300_000)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	_, buildErr := vm.Exec(tc.arg, tc.proc)
	require.Error(t, buildErr)
	require.True(t, errors.Is(buildErr, process.ErrHashBuildBudgetAdmission))

	const consumers = 4
	results := make([]message.JoinMapResult, consumers)
	receiveErrs := make([]error, consumers)
	var wg sync.WaitGroup
	wg.Add(consumers)
	for i := range consumers {
		go func(i int) {
			defer wg.Done()
			results[i], receiveErrs[i] = message.ReceiveJoinMapResult(
				tc.arg.JoinMapTag, false, 0, tc.proc.GetMessageBoard(), tc.proc.Ctx)
		}(i)
	}
	wg.Wait()

	for i := range consumers {
		require.NoError(t, receiveErrs[i])
		require.True(t, results[i].IsBuildError())
		require.Equal(t, results[0].BuildError().ErrorCode(), results[i].BuildError().ErrorCode())
		require.Equal(t, results[0].BuildError().Error(), results[i].BuildError().Error())
	}
	tc.arg.Reset(tc.proc, true, buildErr)

	bat.Clean(tc.proc.Mp())
	tc.marg.Reset(tc.proc, true, buildErr)
	tc.arg.Free(tc.proc, true, buildErr)
}

func TestHashBuildWithoutMapStillBudgetsRetainedBatches(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, nil)
	tc.arg.NeedHashMap = false
	tc.arg.NeedBatches = true
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	bat := newBatch(tc.types, tc.proc, 300_000)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, tc.proc.Mp())
	_, err := vm.Exec(tc.arg, tc.proc)
	require.Error(t, err)
	require.True(t, errors.Is(err, process.ErrHashBuildBudgetAdmission))
	tc.arg.Free(tc.proc, true, err)
	bat.Clean(tc.proc.Mp())
	budget, budgetErr := tc.proc.GetHashBuildBudget()
	require.NoError(t, budgetErr)
	require.Zero(t, budget.Used())
}

func TestShuffleWithoutMapRejectsMissingRuntimeFilter(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, nil)
	tc.arg.IsShuffle = true
	tc.arg.NeedHashMap = false
	require.Error(t, tc.arg.Prepare(tc.proc))
	tc.arg.Free(tc.proc, true, nil)
}

func TestHashBuildFreeWithoutResetReleasesOwnedMemory(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	require.NoError(t, tc.arg.Prepare(tc.proc))
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)
	input := newBatch(tc.types, tc.proc, 100)
	require.NoError(t, tc.arg.ctr.hashmapBuilder.copyBuildBatch(input, tc.proc))
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = input.RowCount()
	input.Clean(tc.proc.Mp())
	require.NoError(t, tc.arg.ctr.hashmapBuilder.BuildHashmap(false, false, false, tc.proc))
	require.Greater(t, budget.Used(), uint64(0))

	buildErr := errors.New("injected build failure")
	tc.arg.Free(tc.proc, true, buildErr)
	require.Zero(t, budget.Used())
}

func BenchmarkBuild(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []buildTestCase{
			newTestCase(b, []bool{false}, []types.Type{types.T_int8.ToType()},
				[]*plan.Expr{
					newExpr(0, types.T_int8.ToType()),
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(newBatch(tc.types, tc.proc, Rows), nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
			for {
				ok, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				require.Equal(t, true, ok)
				//mp := ok.Batch.AuxData.(*hashmap.JoinMap)
				tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
				//mp.Free()
				ok.Batch.Clean(tc.proc.Mp())
				break
			}
		}
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(t testing.TB, flgs []bool, ts []types.Type, cs []*plan.Expr) buildTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	_, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 10),
	}
	return buildTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &HashBuild{
			JoinMapTag:    1,
			JoinMapRefCnt: 1,
			Conditions:    cs,
			NeedHashMap:   true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		marg: &merge.Merge{},
	}
}

func TestHashBuildPrepareDropsPriorGenerationSpillFileService(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	prior, err := proc.GetSpillFileService()
	require.NoError(t, err)

	arg := &HashBuild{NeedHashMap: false}
	arg.ctr.spillFS = prior
	require.NoError(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.spillFS, "a reused operator must not retain the prior Process service")

	current, err := arg.ctr.getSpillFS(proc)
	require.NoError(t, err)
	require.NotSame(t, prior, current, "the current generation must resolve its own borrowed wrapper")
	arg.Free(proc, false, nil)
	require.Nil(t, arg.ctr.spillFS)
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func TestCalculateBloomFilterProbability(t *testing.T) {
	tests := []struct {
		name     string
		rowCount int
		wantProb float64
	}{
		{
			name:     "very small row count",
			rowCount: 1000,
			wantProb: 0.00001,
		},
		{
			name:     "small row count boundary",
			rowCount: 10_0000,
			wantProb: 0.00001,
		},
		{
			name:     "medium row count lower bound",
			rowCount: 10_0001,
			wantProb: 0.000003,
		},
		{
			name:     "medium row count upper bound",
			rowCount: 100_0000,
			wantProb: 0.000003,
		},
		{
			name:     "large row count lower bound",
			rowCount: 100_0001,
			wantProb: 0.000001,
		},
		{
			name:     "large row count upper bound",
			rowCount: 1000_0000,
			wantProb: 0.000001,
		},
		{
			name:     "very large row count lower bound",
			rowCount: 1000_0001,
			wantProb: 0.0000005,
		},
		{
			name:     "very large row count upper bound",
			rowCount: 1_0000_0000,
			wantProb: 0.0000005,
		},
		{
			name:     "huge row count lower bound",
			rowCount: 1_0000_0001,
			wantProb: 0.0000002,
		},
		{
			name:     "huge row count upper bound",
			rowCount: 10_0000_0000,
			wantProb: 0.0000002,
		},
		{
			name:     "extremely large row count",
			rowCount: 10_0000_0001,
			wantProb: 0.0000001,
		},
		{
			name:     "maximum row count",
			rowCount: 100_0000_0000,
			wantProb: 0.0000001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateBloomFilterProbability(tt.rowCount)
			require.Equal(t, tt.wantProb, got, "calculateBloomFilterProbability(%d) = %v, want %v", tt.rowCount, got, tt.wantProb)
		})
	}
}

func TestHashBuildTypeName(t *testing.T) {
	arg := NewArgument()
	require.Equal(t, "hash_build", arg.TypeName())
	arg.Release()
}

func TestHashBuildOpType(t *testing.T) {
	arg := NewArgument()
	require.Equal(t, vm.HashBuild, arg.OpType())
	arg.Release()
}

func TestHashBuildReleaseAndReuse(t *testing.T) {
	arg := NewArgument()
	arg.JoinMapTag = 100
	arg.Release()

	arg2 := NewArgument()
	require.Equal(t, int32(0), arg2.JoinMapTag)
	arg2.Release()
}

func TestHashBuildWithRuntimeFilter(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 10),
	}

	arg := &HashBuild{
		JoinMapTag:    1,
		JoinMapRefCnt: 1,
		Conditions: []*plan.Expr{
			newExpr(0, types.T_int32.ToType()),
		},
		NeedHashMap: true,
		RuntimeFilterSpec: &plan.RuntimeFilterSpec{
			Tag: 1,
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	err := arg.Prepare(proc)
	require.NoError(t, err)

	bat := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, false, 10, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, proc.Mp())

	marg := &merge.Merge{}
	err = marg.Prepare(proc)
	require.NoError(t, err)
	arg.SetChildren([]vm.Operator{marg})

	ok, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)

	arg.Free(proc, false, nil)
	proc.Free()
}

func TestHashBuildFloatRuntimeFilterFallsBackToPass(t *testing.T) {
	buildType := types.T_float32.ToType()
	buildType.Width = 5
	buildType.Scale = 2

	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{buildType},
		[]*plan.Expr{newExpr(0, buildType)},
	)
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()
	spec := &plan.RuntimeFilterSpec{
		Tag:         101,
		UpperLimit:  100,
		BuildExpr:   newExpr(0, buildType),
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		ProbeType:   runtimeFilterPlanType(buildType),
	}
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	build := batch.NewWithSize(1)
	build.Vecs[0] = vector.NewVec(buildType)
	require.NoError(t, vector.AppendFixed(build.Vecs[0], float32(1.234), false, tc.proc.Mp()))
	require.NoError(t, vector.AppendFixed(build.Vecs[0], float32(1.23), false, tc.proc.Mp()))
	build.SetRowCount(2)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)
	require.False(t, tc.arg.ctr.runtimeFilterIn)

	joinResult, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag,
		false,
		0,
		tc.proc.GetMessageBoard(),
		tc.proc.Ctx,
	)
	require.NoError(t, err)
	require.True(t, joinResult.IsSuccess())
	joinMap := joinResult.JoinMap()
	require.NotNil(t, joinMap)
	require.Equal(t, uint64(1), joinMap.GetGroupCount(),
		"SQL-equal build values must still form one resident hash key")
	require.False(t, joinMap.PushedRuntimeFilterIn())
	joinMap.Free()
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
}

func TestHashBuildOptionalRuntimeFilterCollectionFallsBackToJoinMap(
	t *testing.T,
) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{typ},
		[]*plan.Expr{newExpr(0, typ)},
	)
	tc.arg.RuntimeFilterSpec = rawRuntimeFilterSpec(
		tc.arg.JoinMapTag+500, 100, typ)
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(64 << 20)
	aggregate := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	providerCalls := 0
	forcedCollectionReject := false
	aggregate.SetAggregateCapProvider(func() (uint64, error) {
		providerCalls++
		// For one retained non-shuffle batch, admission #1 owns the copied
		// build batch and #2 is build auxiliary memory including the optional
		// UniqueJoinKeys payload. Reject only #2; the retry without that
		// payload must still build and publish the required JoinMap.
		if providerCalls == 2 {
			forcedCollectionReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	input := newBatch([]types.Type{typ}, tc.proc, Rows)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(input, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.True(t, forcedCollectionReject)

	extra := tc.arg.OpAnalyzer.GetOpStats().ExtraStats
	require.Equal(t, int64(1),
		extra["HashBuildRuntimeFilterCollectionFallbacks"])
	require.True(t, tc.arg.ctr.runtimeFilterDone)
	require.False(t, tc.arg.ctr.runtimeFilterIn)

	receiver := message.NewMessageReceiver(
		[]int32{tc.arg.RuntimeFilterSpec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	messages, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	runtimeFilter, ok := messages[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)

	joinResult, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag,
		false,
		0,
		tc.proc.GetMessageBoard(),
		tc.proc.Ctx,
	)
	require.NoError(t, err)
	require.True(t, joinResult.IsSuccess())
	joinMap := joinResult.JoinMap()
	require.NotNil(t, joinMap)
	require.False(t, joinMap.IsSpilled())
	require.Equal(t, int64(Rows), joinMap.GetRowCount())
	require.Greater(t, joinMap.GetGroupCount(), uint64(0))
	require.False(t, joinMap.PushedRuntimeFilterIn())
	joinMap.Free()
	require.Zero(t, generation.Used())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestHashmapBuilderUniqueGrowthFailureAbandonsOptionalKeysInPlace(
	t *testing.T,
) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{typ},
		[]*plan.Expr{newExpr(0, typ)},
	)
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(64 << 20)
	aggregate := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	const uniqueGrowthRows = hashmap.UnitLimit * 2
	input := newBatch(
		[]types.Type{typ}, tc.proc, uniqueGrowthRows)
	require.NoError(t,
		tc.arg.ctr.hashmapBuilder.copyBuildBatch(input, tc.proc))
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = uniqueGrowthRows
	input.Clean(tc.proc.Mp())

	providerCalls := 0
	rejectCall := 0
	forcedUniqueGrowthReject := false
	aggregate.SetAggregateCapProvider(func() (uint64, error) {
		providerCalls++
		if rejectCall > 0 && providerCalls == rejectCall {
			forcedUniqueGrowthReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	// HashOnPK preallocates the complete mandatory map before row insertion.
	// Calibrate those deterministic admissions without optional keys, then
	// reject the next admission in the equivalent build: growth overlap for
	// the second UnitLimit-sized UniqueJoinKeys append.
	require.NoError(t, tc.arg.ctr.hashmapBuilder.BuildHashmap(
		true, false, false, tc.proc))
	mandatoryCalls := providerCalls
	require.Greater(t, mandatoryCalls, 0)
	tc.arg.ctr.hashmapBuilder.FreeHashMapOnly(tc.proc)
	providerCalls = 0
	rejectCall = mandatoryCalls + 1

	require.NoError(t, tc.arg.ctr.hashmapBuilder.BuildHashmap(
		true, false, true, tc.proc))
	require.True(t, forcedUniqueGrowthReject)
	fallback, rebuildSafe :=
		tc.arg.ctr.hashmapBuilder.runtimeFilterFallbackState()
	require.True(t, fallback)
	require.True(t, rebuildSafe)
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
	require.Greater(t,
		tc.arg.ctr.hashmapBuilder.GetGroupCount(), uint64(0))

	tc.arg.Free(tc.proc, false, nil)
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestDedupBatchRewriteRecollectsOptionalKeysWithoutUnsafeReplay(
	t *testing.T,
) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{typ},
		[]*plan.Expr{newExpr(0, typ)},
	)
	tc.arg.IsDedup = true
	tc.arg.DedupBuildKeepLast = true
	tc.arg.OnDuplicateAction = plan.Node_FAIL
	tc.arg.DelColIdx = -1
	tc.arg.DedupDeleteMarkerColIdx = -1
	require.NoError(t, tc.arg.Prepare(tc.proc))

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2}, nil, tc.proc.Mp())
	input.SetRowCount(3)
	require.NoError(t,
		tc.arg.ctr.hashmapBuilder.copyBuildBatch(input, tc.proc))
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 3
	input.Clean(tc.proc.Mp())

	require.NoError(t, tc.arg.ctr.hashmapBuilder.BuildHashmap(
		false, false, true, tc.proc))
	fallback, rebuildSafe :=
		tc.arg.ctr.hashmapBuilder.runtimeFilterFallbackState()
	require.False(t, fallback,
		"canonical rebuild should preserve the runtime-filter optimization")
	require.False(t, rebuildSafe,
		"a failed build cannot replay across an in-place Dedup rewrite")
	require.Len(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys, 1)
	require.Equal(t, 2,
		tc.arg.ctr.hashmapBuilder.UniqueJoinKeys[0].Length())
	require.Equal(t, 2,
		tc.arg.ctr.hashmapBuilder.InputBatchRowCount)
	require.Equal(t, uint64(2),
		tc.arg.ctr.hashmapBuilder.GetGroupCount())

	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestDedupDeleteOnlyRowsPreserveAuxBudgetThroughRuntimeFilter(
	t *testing.T,
) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false, false, true},
		[]types.Type{typ, typ, typ},
		[]*plan.Expr{newExpr(0, typ)},
	)
	tc.arg.IsDedup = true
	tc.arg.DedupBuildKeepLast = true
	tc.arg.OnDuplicateAction = plan.Node_FAIL
	tc.arg.DelColIdx = -1
	tc.arg.DedupDeleteMarkerColIdx = 2
	tc.arg.DedupDeleteKeepColIdxList = []int32{2}
	tc.arg.DedupColName = "id"
	tc.arg.DedupColTypes = []plan.Type{newExpr(0, typ).Typ}
	tc.arg.RuntimeFilterSpec = rawRuntimeFilterSpec(
		tc.arg.JoinMapTag+650, 100, typ)
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)

	input := makeIntKeyValueBatchWithMarker(
		tc.proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2},
	)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(input, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	filterReceiver := message.NewMessageReceiver(
		[]int32{tc.arg.RuntimeFilterSpec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	messages, done, err := filterReceiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	runtimeFilter := messages[0].(message.RuntimeFilterMessage)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)

	joinResult, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag,
		false,
		0,
		tc.proc.GetMessageBoard(),
		tc.proc.Ctx,
	)
	require.NoError(t, err)
	require.True(t, joinResult.IsSuccess())
	joinMap := joinResult.JoinMap()
	require.NotNil(t, joinMap)
	require.Equal(t, int64(3), joinMap.GetRowCount())
	require.Equal(t, uint64(2), joinMap.GetGroupCount())
	require.True(t, joinMap.IsDeleted(2))

	runtimeFilter.Destroy()
	joinMap.Free()
	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, budget.Used())
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestShuffleDedupAdmissionAfterRewriteDoesNotSpillPartialInput(
	t *testing.T,
) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false, false, true},
		[]types.Type{typ, typ, typ},
		[]*plan.Expr{newExpr(0, typ)},
	)
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1 << 30
	tc.arg.IsDedup = true
	tc.arg.DedupBuildKeepLast = true
	tc.arg.OnDuplicateAction = plan.Node_FAIL
	tc.arg.DelColIdx = -1
	tc.arg.DedupDeleteMarkerColIdx = 2
	tc.arg.DedupDeleteKeepColIdxList = []int32{2}
	tc.arg.DedupColName = "id"
	tc.arg.DedupColTypes = []plan.Type{newExpr(0, typ).Typ}
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{
		Tag: tc.arg.JoinMapTag + 700,
	}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(64 << 20)
	aggregate := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)
	// Ingress happens before BuildHashmap initializes this phase. Mark the
	// retained source safe so the provider rejects only after Dedup crosses
	// its explicit in-place rewrite boundary.
	tc.arg.ctr.hashmapBuilder.retainedBatchRecoverySafe = true
	forcedUnsafeReject := false
	aggregate.SetAggregateCapProvider(func() (uint64, error) {
		if !tc.arg.ctr.hashmapBuilder.retainedBatchRecoverySafe {
			forcedUnsafeReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	input := makeIntKeyValueBatchWithMarker(
		tc.proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2},
	)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(input, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	_, buildErr := vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, buildErr, process.ErrHashBuildBudgetAdmission)
	require.True(t, forcedUnsafeReject)
	require.False(t,
		tc.arg.ctr.hashmapBuilder.retainedBatchRecoverySafe)
	require.Empty(t, tc.arg.ctr.spilledFds,
		"partially rewritten Dedup input must never become a spill payload")

	joinResult, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag,
		true,
		tc.arg.ShuffleIdx,
		tc.proc.GetMessageBoard(),
		tc.proc.Ctx,
	)
	require.NoError(t, err)
	require.True(t, joinResult.IsBuildError())
	receiver := message.NewMessageReceiver(
		[]int32{tc.arg.RuntimeFilterSpec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	messages, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	require.Equal(t, int32(message.RuntimeFilter_PASS),
		messages[0].(message.RuntimeFilterMessage).Typ)

	tc.arg.Reset(tc.proc, true, buildErr)
	tc.arg.Free(tc.proc, true, buildErr)
	tc.marg.Reset(tc.proc, true, buildErr)
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestHashBuildFloatRuntimeFilterClosesSignedZero(t *testing.T) {
	tests := []struct {
		name         string
		typ          types.Type
		negativeZero bool
	}{
		{name: "FLOAT32/positive-representative", typ: types.T_float32.ToType()},
		{name: "FLOAT32/negative-representative", typ: types.T_float32.ToType(), negativeZero: true},
		{name: "FLOAT64/positive-representative", typ: types.T_float64.ToType()},
		{name: "FLOAT64/negative-representative", typ: types.T_float64.ToType(), negativeZero: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tc := newTestCase(
				t,
				[]bool{false},
				[]types.Type{test.typ},
				[]*plan.Expr{newExpr(0, test.typ)},
			)
			defer func() {
				tc.arg.Free(tc.proc, false, nil)
				tc.proc.GetMessageBoard().Reset()
				tc.proc.Free()
				require.Zero(t, tc.proc.Mp().CurrNB())
			}()
			spec := &plan.RuntimeFilterSpec{
				Tag:         104,
				UpperLimit:  100,
				BuildExpr:   newExpr(0, test.typ),
				KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
				ProbeType:   runtimeFilterPlanType(test.typ),
			}
			tc.arg.RuntimeFilterSpec = spec
			tc.arg.SetChildren([]vm.Operator{tc.marg})
			require.NoError(t, tc.marg.Prepare(tc.proc))
			require.NoError(t, tc.arg.Prepare(tc.proc))

			build := batch.NewWithSize(1)
			build.Vecs[0] = vector.NewVec(test.typ)
			switch test.typ.Oid {
			case types.T_float32:
				zero := float32(0)
				if test.negativeZero {
					zero = math.Float32frombits(uint32(1) << 31)
				}
				require.NoError(t, vector.AppendFixed(build.Vecs[0], zero, false, tc.proc.Mp()))
				require.NoError(t, vector.AppendFixed(build.Vecs[0], float32(1.25), false, tc.proc.Mp()))
			case types.T_float64:
				zero := float64(0)
				if test.negativeZero {
					zero = math.Float64frombits(uint64(1) << 63)
				}
				require.NoError(t, vector.AppendFixed(build.Vecs[0], zero, false, tc.proc.Mp()))
				require.NoError(t, vector.AppendFixed(build.Vecs[0], float64(1.25), false, tc.proc.Mp()))
			}
			build.SetRowCount(2)
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)
			require.True(t, tc.arg.ctr.runtimeFilterIn)

			receiver := message.NewMessageReceiver(
				[]int32{spec.Tag},
				message.AddrBroadCastOnCurrentCN(),
				tc.proc.GetMessageBoard(),
			)
			msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
			require.NoError(t, err)
			require.False(t, done)
			require.Len(t, msgs, 1)
			runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
			require.True(t, ok)
			require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
			require.Equal(t, int32(3), runtimeFilter.Card)

			payload := vector.NewVec(types.T_any.ToType())
			require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
			require.Equal(t, test.typ.Oid, payload.GetType().Oid)
			require.Equal(t, 3, payload.Length())
			var positiveZero, negativeZero bool
			switch test.typ.Oid {
			case types.T_float32:
				for _, value := range vector.MustFixedColNoTypeCheck[float32](payload) {
					if math.Float32bits(value) == 0 {
						positiveZero = true
					}
					if math.Float32bits(value) == uint32(1)<<31 {
						negativeZero = true
					}
				}
			case types.T_float64:
				for _, value := range vector.MustFixedColNoTypeCheck[float64](payload) {
					if math.Float64bits(value) == 0 {
						positiveZero = true
					}
					if math.Float64bits(value) == uint64(1)<<63 {
						negativeZero = true
					}
				}
			}
			require.True(t, positiveZero)
			require.True(t, negativeZero)
			payload.Free(tc.proc.Mp())
			runtimeFilter.Destroy()

			joinResult, err := message.ReceiveJoinMapResult(
				tc.arg.JoinMapTag,
				false,
				0,
				tc.proc.GetMessageBoard(),
				tc.proc.Ctx,
			)
			require.NoError(t, err)
			require.True(t, joinResult.IsSuccess())
			require.Equal(t, uint64(2), joinResult.JoinMap().GetGroupCount())
			joinResult.JoinMap().Free()
			require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
		})
	}
}

func TestExactRuntimeFilterEncodingContract(t *testing.T) {
	decimal2 := types.New(types.T_decimal64, 18, 2)
	decimal3 := types.New(types.T_decimal64, 18, 3)
	float32Scaled := types.New(types.T_float32, 5, 2)
	float64Type := types.T_float64.ToType()
	tests := []struct {
		name      string
		probeType *plan.Type
		buildType types.Type
		payload   types.Type
		encoding  plan.RuntimeFilterKeyEncoding
		want      keycodec.ExactRuntimeFilterEncoding
	}{
		{
			name:      "legacy default has no pair contract",
			buildType: decimal3,
			payload:   decimal3,
		},
		{
			name:      "stale probe scale differs from matching spec and payload",
			probeType: runtimeFilterPlanType(decimal2),
			buildType: decimal3,
			payload:   decimal3,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		},
		{
			name:      "materialized payload drifts from declared build",
			probeType: runtimeFilterPlanType(decimal2),
			buildType: decimal2,
			payload:   decimal3,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		},
		{
			name:      "valid explicit decimal raw",
			probeType: runtimeFilterPlanType(decimal3),
			buildType: decimal3,
			payload:   decimal3,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			want:      keycodec.ExactRuntimeFilterRaw,
		},
		{
			name:      "decimal cannot overclaim float closure",
			probeType: runtimeFilterPlanType(decimal3),
			buildType: decimal3,
			payload:   decimal3,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		},
		{
			name:      "float cannot underclaim raw",
			probeType: runtimeFilterPlanType(float64Type),
			buildType: float64Type,
			payload:   float64Type,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		},
		{
			name:      "scaled float32 remains unsupported",
			probeType: runtimeFilterPlanType(float32Scaled),
			buildType: float32Scaled,
			payload:   float32Scaled,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
		},
		{
			name:      "valid explicit float closure",
			probeType: runtimeFilterPlanType(float64Type),
			buildType: float64Type,
			payload:   float64Type,
			encoding:  plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
			want:      keycodec.ExactRuntimeFilterFloatZeroClosed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := &plan.RuntimeFilterSpec{
				BuildExpr:   newExpr(0, test.buildType),
				KeyEncoding: test.encoding,
				ProbeType:   test.probeType,
			}
			require.Equal(t, test.want,
				runtimefilter.ExactKeyEncoding(spec, test.payload))
		})
	}
}

func TestRuntimeFilterPayloadStateContract(t *testing.T) {
	tests := []struct {
		name         string
		membership   bool
		legacy       bool
		negativeSlot bool
		inputRows    int
		keyState     string
		want         int32
	}{
		{name: "exact/empty-input", keyState: "value", want: message.RuntimeFilter_DROP},
		{name: "exact/legacy-empty-input", legacy: true, keyState: "value", want: message.RuntimeFilter_PASS},
		{name: "exact/missing-slice", inputRows: 1, want: message.RuntimeFilter_PASS},
		{name: "exact/nil-key", inputRows: 1, keyState: "nil", want: message.RuntimeFilter_PASS},
		{name: "exact/negative-slot", negativeSlot: true, inputRows: 1, keyState: "value", want: message.RuntimeFilter_PASS},
		{name: "exact/legacy-empty-key", legacy: true, inputRows: 1, keyState: "empty", want: message.RuntimeFilter_PASS},
		{name: "exact/empty-key", inputRows: 1, keyState: "empty", want: message.RuntimeFilter_DROP},
		{name: "membership/empty-input", membership: true, keyState: "value", want: message.RuntimeFilter_DROP},
		{name: "membership/missing-slice", membership: true, inputRows: 1, want: message.RuntimeFilter_PASS},
		{name: "membership/nil-key", membership: true, inputRows: 1, keyState: "nil", want: message.RuntimeFilter_PASS},
		{name: "membership/empty-key", membership: true, inputRows: 1, keyState: "empty", want: message.RuntimeFilter_DROP},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()},
				[]*plan.Expr{newExpr(0, types.T_int32.ToType())})
			spec := &plan.RuntimeFilterSpec{
				Tag:                 102,
				Expr:                newExpr(0, types.T_int32.ToType()),
				UseMembershipFilter: test.membership,
			}
			if !test.membership && !test.legacy {
				spec.BuildExpr = spec.Expr
				spec.Expr = nil
				spec.KeyEncoding = plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1
				spec.ProbeType = runtimeFilterPlanType(types.T_int32.ToType())
				if test.negativeSlot {
					spec.BuildExpr.GetCol().ColPos = -1
				}
			}
			tc.arg.RuntimeFilterSpec = spec
			tc.arg.ctr.hashmapBuilder.InputBatchRowCount = test.inputRows
			switch test.keyState {
			case "nil":
				tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{nil}
			case "empty":
				tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
					vector.NewVec(types.T_int32.ToType()),
				}
			case "value":
				tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
					testutil.MakeInt32Vector([]int32{1}, nil, tc.proc.Mp()),
				}
			}

			require.NotPanics(t, func() {
				require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
			})
			require.True(t, tc.arg.ctr.runtimeFilterDone)
			require.False(t, tc.arg.ctr.runtimeFilterIn)
			require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)

			receiver := message.NewMessageReceiver(
				[]int32{spec.Tag},
				message.AddrBroadCastOnCurrentCN(),
				tc.proc.GetMessageBoard(),
			)
			msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
			require.NoError(t, err)
			require.False(t, done)
			require.Len(t, msgs, 1)
			runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
			require.True(t, ok)
			require.Equal(t, test.want, runtimeFilter.Typ)
			require.Zero(t, runtimeFilter.Card)
			require.Empty(t, runtimeFilter.Data)

			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

func TestRuntimeFilterStaleProbeContractFailsOpen(t *testing.T) {
	payloadType := types.New(types.T_decimal64, 18, 3)
	probeType := types.New(types.T_decimal64, 18, 2)
	tc := newTestCase(t, []bool{false}, []types.Type{payloadType},
		[]*plan.Expr{newExpr(0, payloadType)})
	spec := &plan.RuntimeFilterSpec{
		Tag:         103,
		UpperLimit:  100,
		BuildExpr:   newExpr(0, payloadType),
		ProbeType:   runtimeFilterPlanType(probeType),
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
	}
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1

	oracleBatch := batch.NewWithSize(2)
	oracleBatch.Vecs[0] = vector.NewVec(probeType)
	oracleBatch.Vecs[1] = vector.NewVec(payloadType)
	require.NoError(t, vector.AppendFixed(
		oracleBatch.Vecs[0], types.Decimal64(100), false, tc.proc.Mp()))
	require.NoError(t, vector.AppendFixed(
		oracleBatch.Vecs[1], types.Decimal64(1000), false, tc.proc.Mp()))
	oracleBatch.SetRowCount(1)
	equalExpr, err := plan2.BindFuncExprImplByPlanExpr(
		tc.proc.Ctx,
		"=",
		[]*plan.Expr{
			newExpr(0, probeType),
			newExpr(1, payloadType),
		},
	)
	require.NoError(t, err)
	equalResult, freeEqual, err := colexec.GetReadonlyResultFromExpression(
		tc.proc, equalExpr, []*batch.Batch{oracleBatch})
	require.NoError(t, err)
	require.True(t, vector.MustFixedColNoTypeCheck[bool](equalResult)[0],
		"DECIMAL scale2 1.00 and scale3 1.000 are SQL-equal")
	freeEqual()

	rawPayload, err := oracleBatch.Vecs[1].MarshalBinary()
	require.NoError(t, err)
	inExpr := plan2.MakeInExpr(tc.proc.Ctx, newExpr(0, probeType), 1, rawPayload, false)
	inResult, freeIn, err := colexec.GetReadonlyResultFromExpression(
		tc.proc, inExpr, []*batch.Batch{oracleBatch})
	require.NoError(t, err)
	require.False(t, vector.MustFixedColNoTypeCheck[bool](inResult)[0],
		"legacy raw IN compares 100 with 1000 and would lose the join row")
	freeIn()
	oracleBatch.Clean(tc.proc.Mp())

	// Model the real stale-plan shape: SQL compares probe 1.00 with build
	// 1.000, while both the build spec and materialized payload use scale 3.
	// A build-only self-pair check would accept raw value 1000 and could reject
	// the SQL-equal probe raw value 100.
	payload := vector.NewVec(payloadType)
	require.NoError(t, vector.AppendFixed(payload, types.Decimal64(1000), false, tc.proc.Mp()))
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{payload}

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	require.True(t, tc.arg.ctr.runtimeFilterDone)
	require.False(t, tc.arg.ctr.runtimeFilterIn)
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)

	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestRuntimeFilterExplicitDecimalContractProducesIn(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 18, 3)
	tc := newTestCase(t, []bool{false}, []types.Type{decimalType},
		[]*plan.Expr{newExpr(0, decimalType)})
	spec := rawRuntimeFilterSpec(105, 100, decimalType)
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
	payload := vector.NewVec(decimalType)
	require.NoError(t, vector.AppendFixed(
		payload, types.Decimal64(1000), false, tc.proc.Mp()))
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{payload}
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	require.True(t, tc.arg.ctr.runtimeFilterDone)
	require.True(t, tc.arg.ctr.runtimeFilterIn)
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
	require.Equal(t, int32(1), runtimeFilter.Card)
	require.NotEmpty(t, runtimeFilter.Data)
	runtimeFilter.Destroy()
	require.Zero(t, generation.Used())
	generation.Close()

	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestDirectRuntimeFilterUsesDeclaredHashSlot(t *testing.T) {
	typ := types.T_int32.ToType()
	tc := newTestCase(
		t,
		[]bool{false, false},
		[]types.Type{typ, typ},
		[]*plan.Expr{newExpr(0, typ), newExpr(1, typ)},
	)
	spec := rawRuntimeFilterSpec(106, 100, typ)
	spec.BuildExpr = newExpr(1, typ)
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 2
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{901, 902}, nil, tc.proc.Mp()),
		testutil.MakeInt32Vector([]int32{11, 12}, nil, tc.proc.Mp()),
	}
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)

	payload := vector.NewVec(types.T_any.ToType())
	require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
	require.Equal(t, []int32{11, 12},
		vector.MustFixedColNoTypeCheck[int32](payload))
	payload.Free(tc.proc.Mp())
	runtimeFilter.Destroy()
	require.Zero(t, generation.Used())
	generation.Close()

	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func makeSerializedRuntimeFilterSpec(
	t *testing.T,
	proc *process.Process,
	tag, upperLimit int32,
	componentTypes []types.Type,
	full bool,
) *plan.RuntimeFilterSpec {
	t.Helper()
	args := make([]*plan.Expr, len(componentTypes))
	probeTypes := make([]plan.Type, len(componentTypes))
	for i, typ := range componentTypes {
		args[i] = newExpr(int32(i), typ)
		probeTypes[i] = *runtimeFilterPlanType(typ)
	}
	functionName := "serial"
	encoding :=
		plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1
	if full {
		functionName = "serial_full"
		encoding =
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx, functionName, args)
	require.NoError(t, err)
	return &plan.RuntimeFilterSpec{
		Tag:                    tag,
		UpperLimit:             upperLimit,
		BuildExpr:              expr,
		ProbeType:              runtimeFilterPlanType(types.T_varchar.ToType()),
		KeyEncoding:            encoding,
		KeyComponentProbeTypes: probeTypes,
		MatchPrefix:            full,
	}
}

func TestSerializedRuntimeFilterUsesTightBudgetAndProducesIn(t *testing.T) {
	const rowCount = 1000
	componentType := types.T_int32.ToType()
	conditions := []*plan.Expr{
		newExpr(0, componentType),
		newExpr(1, componentType),
	}
	tc := newTestCase(
		t,
		[]bool{false, false},
		[]types.Type{componentType, componentType},
		conditions,
	)
	spec := makeSerializedRuntimeFilterSpec(
		t, tc.proc, 106, rowCount+1,
		[]types.Type{componentType, componentType}, true)
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = rowCount

	first := make([]int32, rowCount)
	second := make([]int32, rowCount)
	for i := range first {
		first[i] = int32(i)
		second[i] = int32(i * 2)
	}
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
		testutil.MakeInt32Vector(first, nil, tc.proc.Mp()),
		testutil.MakeInt32Vector(second, nil, tc.proc.Mp()),
	}

	service := tc.proc.GetService()
	rt := moruntime.ServiceRuntime(service)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	// The generic VARCHAR(max) estimator would request roughly 64 MiB for
	// these tiny tuples. The tuple-specific bound must fit comfortably here.
	budget := process.MustNewHashBuildBudget(512<<10, 512<<10)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	require.True(t, tc.arg.ctr.runtimeFilterDone)
	require.True(t, tc.arg.ctr.runtimeFilterIn)
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
	require.Less(t, generation.Peak(), uint64(512<<10))

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
	require.Equal(t, int32(rowCount), runtimeFilter.Card)

	payload := vector.NewVec(types.T_any.ToType())
	require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
	require.Equal(t, types.T_varchar, payload.GetType().Oid)
	require.Equal(t, rowCount, payload.Length())

	expected := make(map[string]struct{}, 2)
	packer := types.NewPacker()
	for _, row := range []int{0, rowCount - 1} {
		packer.Reset()
		packer.EncodeInt32(first[row])
		packer.EncodeInt32(second[row])
		expected[string(packer.GetBuf())] = struct{}{}
	}
	packer.Close()
	for i := 0; i < payload.Length(); i++ {
		delete(expected, string(payload.GetBytesAt(i)))
	}
	require.Empty(t, expected)

	payload.Free(tc.proc.Mp())
	runtimeFilter.Destroy()
	require.Zero(t, generation.Used())
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestSerializedRuntimeFilterBudgetAccountsPackerSizeClass(t *testing.T) {
	componentType := types.T_varchar.ToType()
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{componentType},
		[]*plan.Expr{newExpr(0, componentType)},
	)
	spec := makeSerializedRuntimeFilterSpec(
		t, tc.proc, 107, 2, []types.Type{componentType}, false)
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
	value := strings.Repeat("x", 128<<10)
	keys := []*vector.Vector{
		testutil.MakeVarcharVector([]string{value}, nil, tc.proc.Mp()),
	}
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = keys

	service := tc.proc.GetService()
	rt := moruntime.ServiceRuntime(service)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	areaBytes, maxRowBytes, err :=
		serializedRuntimeFilterBounds(
			tc.proc, keys, []int{0}, 1, false)
	require.NoError(t, err)
	require.Greater(t, maxRowBytes, uint64(128<<10))
	packerBytes, ok := types.PackerAllocationSize(maxRowBytes)
	require.True(t, ok)
	require.Equal(t, uint64(256<<10), packerBytes)
	peak, err := serializedRuntimeFilterAllocationPeak(
		1, areaBytes, maxRowBytes)
	require.NoError(t, err)

	// One byte below the true peak must fail open before constructing the
	// packer. Counting only its requested slice would incorrectly admit it.
	budget := process.MustNewHashBuildBudget(peak-1, peak-1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, _, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, int32(message.RuntimeFilter_PASS),
		msgs[0].(message.RuntimeFilterMessage).Typ)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.Peak())

	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestSerializedRuntimeFilterBoundsObserveCancellation(t *testing.T) {
	tc := newTestCase(t, nil, nil, nil)
	vec := testutil.MakeInt32Vector([]int32{1}, nil, tc.proc.Mp())
	ctx, cancel := context.WithCancel(tc.proc.Ctx)
	tc.proc.Ctx = ctx
	cancel()

	_, _, err := serializedRuntimeFilterBounds(
		tc.proc, []*vector.Vector{vec}, []int{0}, 1, false)
	require.ErrorIs(t, err, context.Canceled)

	vec.Free(tc.proc.Mp())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestSerializedRuntimeFilterActualComponentMismatchFailsOpen(t *testing.T) {
	declaredType := types.New(types.T_decimal64, 18, 2)
	actualType := types.New(types.T_decimal64, 18, 3)
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{declaredType},
		[]*plan.Expr{newExpr(0, declaredType)},
	)
	spec := makeSerializedRuntimeFilterSpec(
		t, tc.proc, 107, 100, []types.Type{declaredType}, false)
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
	actual := vector.NewVec(actualType)
	require.NoError(t, vector.AppendFixed(
		actual, types.Decimal64(1000), false, tc.proc.Mp()))
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{actual}

	service := tc.proc.GetService()
	rt := moruntime.ServiceRuntime(service)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		tc.proc.GetMessageBoard(),
	)
	msgs, _, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, int32(message.RuntimeFilter_PASS),
		msgs[0].(message.RuntimeFilterMessage).Typ)

	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestSerializedRuntimeFilterMetadataMismatchFailsOpen(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	for _, test := range []struct {
		name   string
		full   bool
		mutate func(*plan.RuntimeFilterSpec)
	}{
		{
			name: "function identity",
			mutate: func(spec *plan.RuntimeFilterSpec) {
				spec.BuildExpr.GetF().Func.Obj++
			},
		},
		{
			name: "serial cannot drive prefix consumer",
			mutate: func(spec *plan.RuntimeFilterSpec) {
				spec.MatchPrefix = true
			},
		},
		{
			name: "serial full requires prefix consumer",
			full: true,
			mutate: func(spec *plan.RuntimeFilterSpec) {
				spec.MatchPrefix = false
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			componentType := types.T_int32.ToType()
			tc := newTestCase(
				t,
				[]bool{false},
				[]types.Type{componentType},
				[]*plan.Expr{newExpr(0, componentType)},
			)
			spec := makeSerializedRuntimeFilterSpec(
				t, tc.proc, 108, 100,
				[]types.Type{componentType}, test.full)
			test.mutate(spec)
			tc.arg.RuntimeFilterSpec = spec
			tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
			tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
				testutil.MakeInt32Vector(
					[]int32{1}, nil, tc.proc.Mp()),
			}

			require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
			receiver := message.NewMessageReceiver(
				[]int32{spec.Tag},
				message.AddrBroadCastOnCurrentCN(),
				tc.proc.GetMessageBoard(),
			)
			msgs, _, err := receiver.ReceiveMessage(
				false, tc.proc.Ctx)
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			require.Equal(t, int32(message.RuntimeFilter_PASS),
				msgs[0].(message.RuntimeFilterMessage).Typ)

			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

func TestRuntimeFilterMarshalBudgetAdmissionFallsBackToPass(t *testing.T) {
	tests := []struct {
		name       string
		membership bool
	}{
		{name: "in"},
		{name: "unique join keys", membership: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()},
				[]*plan.Expr{newExpr(0, types.T_int32.ToType())})
			spec := &plan.RuntimeFilterSpec{
				Tag:                 101,
				UpperLimit:          100,
				BuildExpr:           newExpr(0, types.T_int32.ToType()),
				UseMembershipFilter: test.membership,
				KeyEncoding:         plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
				ProbeType:           runtimeFilterPlanType(types.T_int32.ToType()),
			}
			if test.membership {
				spec.BuildExpr = nil
			}
			tc.arg.RuntimeFilterSpec = spec
			tc.arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "hash build")
			tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
			tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
				testutil.MakeInt32Vector([]int32{1}, nil, tc.proc.Mp()),
			}

			budget := process.MustNewHashBuildBudget(1, 1)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			tc.arg.ctr.hashmapBuilder.setBudget(generation)

			require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
			require.True(t, tc.arg.ctr.runtimeFilterDone)
			require.False(t, tc.arg.ctr.runtimeFilterIn)
			require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
			require.Zero(t, generation.Used())
			require.Equal(t, uint64(1), generation.RejectCount())

			receiver := message.NewMessageReceiver(
				[]int32{spec.Tag}, message.AddrBroadCastOnCurrentCN(), tc.proc.GetMessageBoard())
			msgs, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
			require.NoError(t, err)
			require.False(t, done)
			require.Len(t, msgs, 1)
			runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
			require.True(t, ok)
			require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
			require.Zero(t, runtimeFilter.Card)
			require.Empty(t, runtimeFilter.Data)

			extra := tc.arg.OpAnalyzer.GetOpStats().ExtraStats
			require.Equal(t, int64(1), extra["HashBuildRuntimeFilterBudgetFallbacks"])
			require.Greater(t, extra["HashBuildRuntimeFilterBudgetFallbackRequestedBytes"], int64(1))
			require.Zero(t, extra["HashBuildRuntimeFilterBudgetFallbackUsedBytes"])
			require.Equal(t, int64(1), extra["HashBuildRuntimeFilterBudgetFallbackCapBytes"])

			generation.Close()
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

func TestRuntimeFilterMarshalUsesSinglePayloadBudget(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()},
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())})
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, tc.proc.Mp())
	wireBytes := uint64(1+len(types.EncodeType(vec.GetType()))+4*4+1) +
		uint64(len(vec.GetData())+len(vec.GetArea()))
	projected := wireBytes + 64<<10

	budget := process.MustNewHashBuildBudget(projected, projected)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	data, release, err := tc.arg.ctr.hashmapBuilder.marshalRuntimeFilterVector(vec)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, projected, generation.Peak())
	require.LessOrEqual(t, generation.Used(), projected)
	require.NotNil(t, release)
	release()
	require.Zero(t, generation.Used())

	vec.Free(tc.proc.Mp())
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestRuntimeFilterMarshalSinglePayloadCoversVarlenaPeak(t *testing.T) {
	tc := newTestCase(t, []bool{true}, []types.Type{types.T_varchar.ToType()},
		[]*plan.Expr{newExpr(0, types.T_varchar.ToType())})
	values := make([]string, 128)
	for i := range values {
		values[i] = strings.Repeat("x", 1024+i)
	}
	vec := testutil.MakeVarcharVector(values, nil, tc.proc.Mp())
	wireBytes := uint64(1+len(types.EncodeType(vec.GetType()))+4*4+1) +
		uint64(len(vec.GetData())+len(vec.GetArea()))
	projected := wireBytes + 64<<10

	budget := process.MustNewHashBuildBudget(projected, projected)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	data, release, err := tc.arg.ctr.hashmapBuilder.marshalRuntimeFilterVector(vec)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, projected, generation.Peak())
	require.LessOrEqual(t, generation.Used(), projected)
	release()
	require.Zero(t, generation.Used())

	vec.Free(tc.proc.Mp())
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestRuntimeFilterMarshalClosedBudgetRemainsFatal(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()},
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())})
	spec := rawRuntimeFilterSpec(102, 100, types.T_int32.ToType())
	tc.arg.RuntimeFilterSpec = spec
	tc.arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "hash build")
	tc.arg.ctr.hashmapBuilder.InputBatchRowCount = 1
	tc.arg.ctr.hashmapBuilder.UniqueJoinKeys = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{1}, nil, tc.proc.Mp()),
	}

	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	generation.Close()
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	err = tc.arg.handleRuntimeFilter(tc.proc)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
	require.Nil(t, tc.arg.ctr.hashmapBuilder.UniqueJoinKeys)
	require.False(t, tc.arg.ctr.runtimeFilterDone)
	require.Zero(t, tc.arg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildRuntimeFilterBudgetFallbacks"])

	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag}, message.AddrBroadCastOnCurrentCN(), tc.proc.GetMessageBoard())
	msgs, done, receiveErr := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, receiveErr)
	require.False(t, done)
	require.Empty(t, msgs)

	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestHashBuildMultipleTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  types.Type
	}{
		{"int16", types.T_int16.ToType()},
		{"int32", types.T_int32.ToType()},
		{"int64", types.T_int64.ToType()},
		{"uint8", types.T_uint8.ToType()},
		{"varchar", types.T_varchar.ToType()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCase(t, []bool{false}, []types.Type{tt.typ}, []*plan.Expr{newExpr(0, tt.typ)})
			err := tc.marg.Prepare(tc.proc)
			require.NoError(t, err)
			err = tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			tc.arg.SetChildren([]vm.Operator{tc.marg})
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(newBatch(tc.types, tc.proc, Rows), nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
			tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
			ok, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, ok.Status)
			tc.arg.Free(tc.proc, false, nil)
			tc.proc.Free()
		})
	}
}

func TestHashBuildNullable(t *testing.T) {
	tc := newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	err := tc.marg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	bat := testutil.NewBatch(tc.types, true, int(Rows), tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	ok, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
}

func TestHashBuildEmptyBatch(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	err := tc.marg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	ok, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
}

func TestHashBuildHashOnPK(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.HashOnPK = true
	err := tc.marg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(newBatch(tc.types, tc.proc, Rows), nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	ok, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
}

// TestHashBuildRuntimeFilterWithNulls verifies that NULLs in the build side
// don't corrupt the runtime filter. Before the fix, InplaceSort reordered
// data but NOT the null bitmap, corrupting the serialized filter.
func TestHashBuildRuntimeFilterWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 10),
	}

	arg := &HashBuild{
		JoinMapTag:    1,
		JoinMapRefCnt: 1,
		Conditions: []*plan.Expr{
			newExpr(0, types.T_int32.ToType()),
		},
		NeedHashMap:       true,
		RuntimeFilterSpec: rawRuntimeFilterSpec(1, 10000, types.T_int32.ToType()),
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	err := arg.Prepare(proc)
	require.NoError(t, err)

	// Create a batch with NULLs at every even index.
	bat := testutil.NewBatchWithNulls(
		[]types.Type{types.T_int32.ToType()}, false, 10, proc.Mp(),
	)
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, proc.Mp())

	marg := &merge.Merge{}
	err = marg.Prepare(proc)
	require.NoError(t, err)
	arg.SetChildren([]vm.Operator{marg})

	ok, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)

	arg.Free(proc, false, nil)
	proc.Free()
}

// TestHashBuildRuntimeFilterWithNullsHashOnPK tests the hashOnPK path
// where UniqueJoinKeys include NULLs from UnionBatch.
func TestHashBuildRuntimeFilterWithNullsHashOnPK(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 10),
	}

	arg := &HashBuild{
		JoinMapTag:    1,
		JoinMapRefCnt: 1,
		HashOnPK:      true,
		Conditions: []*plan.Expr{
			newExpr(0, types.T_int32.ToType()),
		},
		NeedHashMap:       true,
		RuntimeFilterSpec: rawRuntimeFilterSpec(1, 10000, types.T_int32.ToType()),
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	err := arg.Prepare(proc)
	require.NoError(t, err)

	bat := testutil.NewBatchWithNulls(
		[]types.Type{types.T_int32.ToType()}, false, 10, proc.Mp(),
	)
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, proc.Mp())
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, proc.Mp())

	marg := &merge.Merge{}
	err = marg.Prepare(proc)
	require.NoError(t, err)
	arg.SetChildren([]vm.Operator{marg})

	ok, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, ok.Status)

	arg.Free(proc, false, nil)
	proc.Free()
}

func TestHashBuildIsShuffle(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	budget, budgetErr := tc.proc.GetHashBuildBudget()
	require.NoError(t, budgetErr)
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1
	tc.arg.TrackNullKeys = true
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 2}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	for cycle := 0; cycle < 2; cycle++ {
		if cycle > 0 {
			tc.marg.Reset(tc.proc, false, nil)
			tc.proc.GetMessageBoard().Reset()
		}
		require.NoError(t, tc.marg.Prepare(tc.proc))
		require.NoError(t, tc.arg.Prepare(tc.proc))
		build := batch.NewWithSize(1)
		var buildNulls []uint64
		if cycle == 0 {
			buildNulls = []uint64{1}
		}
		build.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0, 2}, buildNulls, tc.proc.Mp())
		build.SetRowCount(3)
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(batch.EmptyBatch, nil, tc.proc.Mp())
		tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
		_, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		result, receiveErr := message.ReceiveJoinMapResult(tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
		require.NoError(t, receiveErr)
		require.True(t, result.IsSuccess(), "cycle %d must publish a spilled JoinMap", cycle)
		jm := result.JoinMap()
		require.NotNil(t, jm)
		require.True(t, jm.IsSpilled())
		spillPayload, err := jm.TakeSpillBuildPayload()
		require.NoError(t, err)
		require.Len(t, spillPayload.Files, spillNumBuckets)
		require.Same(t, budget, spillPayload.BudgetRef)
		require.NoError(t, spillPayload.Close())
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		tc.arg.Reset(tc.proc, false, nil)
	}
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
}

func TestBroadcastHashBuildParallelConsumersStayResident(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.IsShuffle = false
	tc.arg.JoinMapRefCnt = 2
	tc.arg.SpillThreshold = 1
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	build := batch.NewWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, tc.proc.Mp())
	build.SetRowCount(3)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	first, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag, false, 0, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	second, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag, false, 0, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, first.IsSuccess())
	require.True(t, second.IsSuccess())
	require.Same(t, first.JoinMap(), second.JoinMap())
	require.False(t, first.JoinMap().IsSpilled())
	require.Equal(t, int64(2), first.JoinMap().GetRefCount())
	require.Zero(t, tc.arg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"])

	first.JoinMap().Free()
	second.JoinMap().Free()
	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	tc.proc.Free()
}

func TestHashBuildRejectsSharedSpillPayload(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.JoinMapRefCnt = 2
	tc.arg.SpillThreshold = 1
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 2}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	build := batch.NewWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, tc.proc.Mp())
	build.SetRowCount(3)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	_, buildErr := vm.Exec(tc.arg, tc.proc)
	require.ErrorContains(t, buildErr, "hash build spill requires exactly one consumer, got 2")

	first, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	second, err := message.ReceiveJoinMapResult(
		tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, first.IsBuildError())
	require.True(t, second.IsBuildError())
	require.Same(t, first.BuildError(), second.BuildError())

	tc.arg.Reset(tc.proc, true, buildErr)
	tc.marg.Reset(tc.proc, true, buildErr)
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	tc.arg.Free(tc.proc, true, buildErr)
	tc.proc.Free()
}

func TestShuffleHashBuildFallsBackToDirectSpillWhenRetainedProofRejects(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_varchar.ToType()}, []*plan.Expr{newExpr(0, types.T_varchar.ToType())})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1 << 30
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 3500}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(8 << 20)
	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = 'x'
	}
	build := batch.NewWithSize(1)
	build.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), payload, 1, tc.proc.Mp())
	require.NoError(t, err)
	build.SetRowCount(1)

	directNeed, err := spillBudgetBytes(build)
	require.NoError(t, err)
	require.Less(t, directNeed, capBytes)
	retainedNeed, err := spillRetainedBudgetBytes(build)
	require.NoError(t, err)
	require.Greater(t, retainedNeed, capBytes)

	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	_, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)

	result, err := message.ReceiveJoinMapResult(tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, result.IsSuccess())
	jm := result.JoinMap()
	require.NotNil(t, jm)
	require.True(t, jm.IsSpilled(), "failed future-retained proof must choose direct spill")
	require.Equal(t, int64(1), jm.GetRowCount())
	spillPayload, err := jm.TakeSpillBuildPayload()
	require.NoError(t, err)
	require.NoError(t, spillPayload.Close())
	require.Empty(t, tc.arg.ctr.hashmapBuilder.Batches.Buf)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	generation.Close()
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestShuffleHashBuildSpillsExpressionKey(t *testing.T) {
	bindProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	col := newExpr(0, types.T_int32.ToType())
	modulo, err := plan2.BindFuncExprImplByPlanExpr(
		bindProc.Ctx,
		"%",
		[]*plan.Expr{col, plan2.MakePlan2Int32ConstExprWithType(2)},
	)
	require.NoError(t, err)

	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{modulo})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 4000}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	build := batch.NewWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, tc.proc.Mp())
	build.SetRowCount(4)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(build, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())

	_, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	result, err := message.ReceiveJoinMapResult(tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, result.IsSuccess())
	jm := result.JoinMap()
	require.NotNil(t, jm)
	require.True(t, jm.IsSpilled())
	require.Equal(t, int64(4), jm.GetRowCount())
	spillPayload, err := jm.TakeSpillBuildPayload()
	require.NoError(t, err)
	require.NoError(t, spillPayload.Close())
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
	bindProc.Free()
}

func TestShuffleHashBuildResizeRejectReleasesPartialMapAndSpills(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1 << 30
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 3000}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(64 << 20)
	aggregate := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	// With one full ingress batch, the first four admissions are emergency
	// scratch, retained copy, build auxiliary memory, and the initial map.
	// Reject exactly the fifth admission: the first resize after insertion.
	providerCalls := 0
	forcedResizeReject := false
	aggregate.SetAggregateCapProvider(func() (uint64, error) {
		providerCalls++
		if providerCalls == 5 {
			forcedResizeReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	bat := newBatch(tc.types, tc.proc, 8192)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	_, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.True(t, forcedResizeReject)
	require.Zero(t, generation.Used(), "partial map, retained batches, and emergency scratch must be released before publication")

	result, err := message.ReceiveJoinMapResult(tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, result.IsSuccess())
	jm := result.JoinMap()
	require.NotNil(t, jm)
	require.True(t, jm.IsSpilled())
	require.Equal(t, int64(8192), jm.GetRowCount())
	spillPayload, err := jm.TakeSpillBuildPayload()
	require.NoError(t, err)
	require.NoError(t, spillPayload.Close())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestObserveHashBuildBudgetUsesGenerationSnapshot(t *testing.T) {
	budget := process.MustNewHashBuildBudget(1024, 1024)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	reservation, err := generation.Reserve(128)
	require.NoError(t, err)
	_, err = generation.Reserve(1024)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	reservation.Release()

	analyzer := process.NewAnalyzer(0, false, false, "hash build")
	observeHashBuildBudget(analyzer, generation)
	extra := analyzer.GetOpStats().ExtraStats
	require.Equal(t, int64(1024), extra["QueryHashBudgetCapBytes"])
	require.Equal(t, int64(128), extra["QueryHashBudgetPeakBytes"])
	require.Equal(t, int64(1), extra["QueryHashBudgetRejects"])
	require.Equal(t, int64(1), extra["QueryHashBudgetReserves"])

	// Sampling the same cumulative generation again must not double count.
	observeHashBuildBudget(analyzer, generation)
	require.Equal(t, int64(1), extra["QueryHashBudgetRejects"])
	generation.Close()
}

func TestShuffleHashBuildSpillFailureReleasesEmergencyResources(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 4000}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	aggregate := process.MustNewHashBuildBudget(64<<20, 64<<20)
	generation, err := aggregate.OpenGenerationWithSpillCaps(1, 64<<20, 1, 32)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)

	bat := newBatch(tc.types, tc.proc, 8192)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(bat, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	_, buildErr := vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, buildErr, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, tc.arg.ctr.spillScratchReservation)
	require.Zero(t, cap(tc.arg.ctr.spillHashValues))
	require.Zero(t, cap(tc.arg.ctr.spillSelection))
	require.Zero(t, cap(tc.arg.ctr.spillKeyVecs))
	require.Zero(t, tc.arg.ctr.spillWriteBuf.Cap())

	tc.arg.Reset(tc.proc, true, buildErr)
	tc.arg.Reset(tc.proc, true, buildErr)
	tc.arg.Free(tc.proc, true, buildErr)
	tc.arg.Free(tc.proc, true, buildErr)
	tc.marg.Reset(tc.proc, true, buildErr)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestShuffleHashBuildDrainsRetainedBatchBeforeGrowingScratch(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []*plan.Expr{newExpr(0, types.T_int32.ToType())})
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1 << 30
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 5000}
	tc.arg.SetChildren([]vm.Operator{tc.marg})
	require.NoError(t, tc.marg.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	const capBytes = uint64(64 << 20)
	aggregate := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	tc.arg.ctr.hashmapBuilder.setBudget(generation)
	providerCalls := 0
	forcedGrowReject := false
	aggregate.SetAggregateCapProvider(func() (uint64, error) {
		providerCalls++
		// First ingress reserves direct scratch, grows it for future retained
		// drain, then reserves its copy. Reject the fourth admission: growing
		// direct scratch for the larger second ingress while that copy is live.
		if providerCalls == 4 {
			forcedGrowReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	first := newBatch(tc.types, tc.proc, 8192)
	second := newBatch(tc.types, tc.proc, 65536)
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(first, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(second, nil, tc.proc.Mp())
	tc.proc.Reg.MergeReceivers[0].Ch2 <- process.NewPipelineSignalToDirectly(nil, nil, tc.proc.Mp())
	_, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.True(t, forcedGrowReject)
	require.Zero(t, generation.Used())

	result, err := message.ReceiveJoinMapResult(tc.arg.JoinMapTag, true, tc.arg.ShuffleIdx, tc.proc.GetMessageBoard(), tc.proc.Ctx)
	require.NoError(t, err)
	require.True(t, result.IsSuccess())
	jm := result.JoinMap()
	require.True(t, jm.IsSpilled())
	require.Equal(t, int64(73728), jm.GetRowCount())
	spillPayload, err := jm.TakeSpillBuildPayload()
	require.NoError(t, err)
	require.NoError(t, spillPayload.Close())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}
