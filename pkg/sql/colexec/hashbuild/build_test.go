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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
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
		NeedHashMap: true,
		RuntimeFilterSpec: &plan.RuntimeFilterSpec{
			Tag:        1,
			UpperLimit: 10000,
			Expr:       newExpr(0, types.T_int32.ToType()),
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
		NeedHashMap: true,
		RuntimeFilterSpec: &plan.RuntimeFilterSpec{
			Tag:        1,
			UpperLimit: 10000,
			Expr:       newExpr(0, types.T_int32.ToType()),
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
		files := jm.TakeSpillBuildFiles()
		require.Len(t, files, spillNumBuckets)
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		tc.arg.Reset(tc.proc, false, nil)
	}
	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
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
	for _, file := range jm.TakeSpillBuildFiles() {
		if file != nil {
			require.NoError(t, file.Close())
		}
	}
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
		if providerCalls == 3 {
			forcedGrowReject = true
			return generation.Used(), nil
		}
		return capBytes, nil
	})

	first := newBatch(tc.types, tc.proc, 8192)
	second := newBatch(tc.types, tc.proc, 16384)
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
	require.Equal(t, int64(24576), jm.GetRowCount())
	for _, file := range jm.TakeSpillBuildFiles() {
		if file != nil {
			require.NoError(t, file.Close())
		}
	}
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())

	tc.arg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.marg.Reset(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}
