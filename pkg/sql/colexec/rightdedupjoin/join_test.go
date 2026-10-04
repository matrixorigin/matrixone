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

package rightdedupjoin

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
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
type joinTestCase struct {
	arg    *RightDedupJoin
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.HashBuild
}

func newRightDedupTestProcess(t *testing.T, pessimistic bool) (*process.Process, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	meta := txn.TxnMeta{}
	if pessimistic {
		meta.Mode = txn.TxnMode_Pessimistic
	}
	txnOp.EXPECT().Txn().Return(meta).AnyTimes()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Base.TxnOperator = txnOp
	return proc, ctrl
}

func runRightDedupCase(t *testing.T, buildVals, probeVals []int32, pessimistic, wantDuplicate bool) {
	proc, ctrl := newRightDedupTestProcess(t, pessimistic)
	defer ctrl.Finish()
	typ := types.T_int32.ToType()
	tag++
	curTag := tag
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}

	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(buildVals, nil, proc.Mp())
	buildBat.SetRowCount(len(buildVals))
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		Conditions:    conditions[1],
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))

	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(probeVals, nil, proc.Mp())
	probeBat.SetRowCount(len(probeVals))
	arg := &RightDedupJoin{
		LeftTypes:         []types.Type{typ},
		RightTypes:        []types.Type{typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{{Rel: 0, Pos: 0}},
		OnDuplicateAction: plan.Node_FAIL,
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        curTag,
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
	installTestAllocation(t, arg, buildArg)

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	res, err := vm.Exec(arg, proc)
	if wantDuplicate {
		require.Error(t, err)
		require.Contains(t, err.Error(), "Duplicate entry")
	} else {
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, res.Status)
	}

	arg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
}

func TestRightDedupDuplicateTracking(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("success_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupCase(t, []int32{1}, []int32{2}, pessimistic, false)
		})
		t.Run(fmt.Sprintf("one_group_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupCase(t, []int32{1}, []int32{1, 1}, pessimistic, true)
		})
		t.Run(fmt.Sprintf("bitmap_boundary_pessimistic_%t", pessimistic), func(t *testing.T) {
			buildVals := make([]int32, 64)
			for i := range buildVals {
				buildVals[i] = int32(i + 1)
			}
			runRightDedupCase(t, buildVals, []int32{64, 64}, pessimistic, true)
		})
		t.Run(fmt.Sprintf("watermark_pessimistic_%t", pessimistic), func(t *testing.T) {
			buildVals := make([]int32, 100)
			for i := range buildVals {
				buildVals[i] = int32(i + 1)
			}
			runRightDedupCase(t, buildVals, []int32{101, 5, 101}, pessimistic, true)
		})
	}
}

func TestRightDedupInputKeysUniqueLookupOnly(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("unique_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupInputUniqueCase(t, pessimistic, false)
		})
		t.Run(fmt.Sprintf("target_conflict_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupInputUniqueCase(t, pessimistic, true)
		})
	}
}

func runRightDedupInputUniqueCase(t *testing.T, pessimistic, targetConflict bool) {
	proc, ctrl := newRightDedupTestProcess(t, pessimistic)
	defer ctrl.Finish()
	typ := types.T_int32.ToType()
	tag++
	curTag := tag
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}

	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	buildBat.SetRowCount(1)
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		Conditions:    conditions[1],
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))

	probeValues := []int32{2, 3}
	if targetConflict {
		probeValues = []int32{1}
	}
	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector(probeValues, nil, proc.Mp())
	probeBat.SetRowCount(len(probeValues))
	arg := &RightDedupJoin{
		LeftTypes:         []types.Type{typ},
		RightTypes:        []types.Type{typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{{Rel: 0, Pos: 0}},
		OnDuplicateAction: plan.Node_FAIL,
		InputKeysUnique:   true,
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        curTag,
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
	installTestAllocation(t, arg, buildArg)
	defer func() {
		arg.Free(proc, false, nil)
		buildArg.Free(proc, false, nil)
		buildBat.Clean(proc.Mp())
		probeBat.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	res, err := vm.Exec(arg, proc)
	if targetConflict {
		require.Error(t, err)
		require.Contains(t, err.Error(), "Duplicate entry")
	} else {
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, uint64(1), arg.ctr.mp.GetGroupCount(), "lookup-only probes must not grow the target map")
		extra := arg.OpAnalyzer.GetOpStats().ExtraStats
		require.Equal(t, int64(len(probeValues)), extra["RightDedupInputUniqueRows"])
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, res.Status)
	}

}

func runRightDedupSpilledEmptyBuild(t *testing.T, pessimistic, duplicateAcrossBatches, inputKeysUnique bool) {
	proc, ctrl := newRightDedupTestProcess(t, pessimistic)
	defer ctrl.Finish()
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)
	typ := types.T_int32.ToType()
	tag++
	curTag := tag
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	arg := &RightDedupJoin{
		LeftTypes:         []types.Type{typ},
		RightTypes:        []types.Type{typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}},
		IsShuffle:         true,
		ShuffleIdx:        0,
		OnDuplicateAction: plan.Node_FAIL,
		InputKeysUnique:   inputKeysUnique,
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        curTag,
		SpillThreshold:    1,
	}
	require.NoError(t, arg.SetAllocationAccount(account))

	probeValues := [][]int32{{1}, {2}}
	if duplicateAcrossBatches {
		probeValues = [][]int32{{1}, {1}}
	}
	probeBatches := make([]*batch.Batch, 0, len(probeValues))
	for _, values := range probeValues {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		bat.SetRowCount(len(values))
		probeBatches = append(probeBatches, bat)
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(probeBatches))

	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files:     make([]*message.SpillFile, spillutil.SpillNumBuckets),
		BudgetRef: generation,
	}))
	message.SendMessage(message.JoinMapMsg{
		Result:     message.NewJoinMapResult(jm),
		IsShuffle:  true,
		ShuffleIdx: 0,
		Tag:        curTag,
	}, proc.GetMessageBoard())

	require.NoError(t, arg.Prepare(proc))
	res, err := vm.Exec(arg, proc)
	if duplicateAcrossBatches {
		require.Error(t, err)
		require.Contains(t, err.Error(), "Duplicate entry")
	} else {
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.True(t, res.Batch.Vecs[1].IsConstNull())
		got := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
		require.Equal(t, []int32{1}, got)
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)
		require.Equal(t, []int32{2}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0]))
		require.True(t, res.Batch.Vecs[1].IsConstNull())
		res, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, res.Status)
	}

	arg.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestRightDedupSpilledEmptyBuild(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("unique_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledEmptyBuild(t, pessimistic, false, false)
		})
		t.Run(fmt.Sprintf("duplicate_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledEmptyBuild(t, pessimistic, true, false)
		})
	}
}

func TestRightDedupSpilledInputKeysUnique(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_target_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledEmptyBuild(t, pessimistic, false, true)
		})
		t.Run(fmt.Sprintf("nonempty_target_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledInputKeysUniqueWithBuild(t, pessimistic)
		})
	}
}

func runRightDedupSpilledInputKeysUniqueWithBuild(t *testing.T, pessimistic bool) {
	proc, ctrl := newRightDedupTestProcess(t, pessimistic)
	defer ctrl.Finish()
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20

	typ := types.T_int32.ToType()
	tag++
	curTag := tag
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	buildBat.SetRowCount(2)
	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	probeBat.SetRowCount(1)

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:       true,
		Conditions:        conditions[1],
		IsShuffle:         true,
		ShuffleIdx:        0,
		SpillThreshold:    2,
		JoinMapTag:        curTag,
		JoinMapRefCnt:     1,
		RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: curTag + 9000},
	}
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
	rightDedupArg := &RightDedupJoin{
		LeftTypes:         []types.Type{typ},
		RightTypes:        []types.Type{typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{{Rel: 0, Pos: 0}},
		IsShuffle:         true,
		ShuffleIdx:        0,
		OnDuplicateAction: plan.Node_FAIL,
		InputKeysUnique:   true,
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        curTag,
		SpillThreshold:    2,
	}
	rightDedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
	installTestAllocation(t, rightDedupArg, buildArg)

	var execErr error
	defer func() {
		failed := execErr != nil
		rightDedupArg.Free(proc, failed, execErr)
		buildArg.Free(proc, failed, execErr)
		buildBat.Clean(proc.Mp())
		probeBat.Clean(proc.Mp())
		budget, budgetErr := proc.GetExecutionResourceBudget()
		var used, diskUsed, fdUsed uint64
		if budgetErr == nil {
			used = budget.Used()
			diskUsed = budget.SpillDiskUsed()
			fdUsed = budget.SpillFDUsed()
		}
		proc.Free()
		require.NoError(t, budgetErr)
		require.Zero(t, used)
		require.Zero(t, diskUsed)
		require.Zero(t, fdUsed)
		require.Zero(t, proc.Mp().CurrNB())
	}()

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, rightDedupArg.Prepare(proc))
	_, execErr = vm.Exec(buildArg, proc)
	require.NoError(t, execErr)
	require.Positive(t, buildArg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"],
		"the regression must exercise a nonempty spilled target payload")
	_, execErr = vm.Exec(rightDedupArg, proc)
	require.Error(t, execErr)
	require.Contains(t, execErr.Error(), "Duplicate entry")
}

func TestRightDedupResetAndPrepareRetry(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	typ := types.T_int32.ToType()
	valid := newExpr(0, typ)
	invalid := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}}
	arg := &RightDedupJoin{
		Conditions:        [][]*plan.Expr{{valid}, {valid}},
		UpdateColExprList: []*plan.Expr{valid, invalid},
	}
	installTestAllocation(t, arg)

	require.Error(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.vecs)
	require.Nil(t, arg.ctr.evecs)
	require.Nil(t, arg.ctr.exprExecs)
	arg.UpdateColExprList[1] = valid
	require.NoError(t, arg.Prepare(proc))

	arg.ctr.groupCount = 10
	arg.ctr.buildGroupCount = 10
	arg.Reset(proc, false, nil)
	require.Zero(t, arg.ctr.groupCount)
	require.Zero(t, arg.ctr.buildGroupCount)
	arg.Free(proc, false, nil)
	proc.Free()
}

func TestRightDedupEmptyMapUsesEvaluatedKeyType(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	varcharTyp := types.T_varchar.ToType()
	arg := &RightDedupJoin{
		LeftTypes:  []types.Type{types.T_int32.ToType()},
		Conditions: [][]*plan.Expr{{newExpr(0, varcharTyp)}, {newExpr(0, varcharTyp)}},
	}
	installTestAllocation(t, arg)
	jm, err := arg.newEmptyJoinMap(proc)
	require.NoError(t, err)
	require.NoError(t, jm.PreAlloc(2))
	keys := testutil.MakeVarcharVector([]string{"12345678a", "12345678b"}, nil, proc.Mp())
	vals, _, err := jm.NewIterator().Insert(0, 2, []*vector.Vector{keys})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, vals[:2])
	jm.Free()
	keys.Free(proc.Mp())
	proc.Free()
}

func TestRightDedupEmptyBuildProbeMapHonorsExecutionResourceBudget(t *testing.T) {
	proc, ctrl := newRightDedupTestProcess(t, false)
	defer ctrl.Finish()

	initialBytes := hashtable.Int64HashMapInitialAllocationBytes()
	proc.Base.Lim.Size = int64(initialBytes)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)

	const rows = 2_048
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	probe := batch.NewWithSize(1)
	probe.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	probe.SetRowCount(rows)
	probeSource := colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe})

	typ := types.T_int32.ToType()
	tag++
	arg := &RightDedupJoin{
		LeftTypes:         []types.Type{typ},
		RightTypes:        []types.Type{typ},
		Conditions:        [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}},
		Result:            []colexec.ResultPos{{Rel: 0, Pos: 0}},
		OnDuplicateAction: plan.Node_FAIL,
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        tag,
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(initialBytes, budget)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	arg.AppendChild(probeSource)
	var callErr error
	t.Cleanup(func() {
		arg.Free(proc, true, callErr)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		probeSource.Free(proc, true, callErr)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	message.SendJoinMapResult(
		message.NewJoinMapResult(nil),
		arg.JoinMapTag,
		false,
		0,
		proc.GetMessageBoard(),
	)

	require.NoError(t, arg.Prepare(proc))
	_, callErr = arg.Call(proc)
	require.Error(t, callErr)
	require.True(t, moerr.IsMoErrCode(callErr, moerr.ErrOOM), callErr)
	require.NotErrorIs(t, callErr, process.ErrExecutionResourceAdmission)
	require.NotContains(t, callErr.Error(), "convert go error")
	require.NotContains(t, callErr.Error(), process.ErrExecutionResourceAdmission.Error())
	require.Contains(t, callErr.Error(), "hash build memory budget exceeded")
	require.Zero(t, budget.Used(),
		"failed probe-map construction must roll back its physical allocation")
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []joinTestCase {
	return []joinTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []int32{0},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []int32{1},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestRightDedupJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = false
		tc.barg.DelColIdx = -1
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		tc.proc.GetMessageBoard().Reset()
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = false
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		tc.proc.GetMessageBoard().Reset()
		tc.arg.OnDuplicateAction = plan.Node_IGNORE
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = false
		tc.barg.OnDuplicateAction = plan.Node_IGNORE
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []int32{0},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []int32{0},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
			}
			t := new(testing.T)
			for _, tc := range tcs {
				bats := hashBuild(t, tc)
				err := tc.arg.Prepare(tc.proc)
				require.NoError(t, err)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[0])
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[1])
				for {
					ok, err := tc.arg.Call(tc.proc)
					if ok.Status == vm.ExecStop || err != nil {
						break
					}
				}
			}
		}
	}
*/
func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    int32(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []int32, cs [][]*plan.Expr) joinTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	_, cancel := context.WithCancel(context.Background())
	//args := make([]*plan.Expr, 0, 2)
	//args = append(args, &plan.Expr{
	//	Typ: plan.Type{
	//		Id: int32(ts[0].Oid),
	//	},
	//	Expr: &plan.Expr_Col{
	//		Col: &plan.ColRef{
	//			RelPos: 0,
	//			ColPos: 0,
	//		},
	//	},
	//})
	//args = append(args, &plan.Expr{
	//	Typ: plan.Type{
	//		Id: int32(ts[0].Oid),
	//	},
	//	Expr: &plan.Expr_Col{
	//		Col: &plan.ColRef{
	//			RelPos: 1,
	//			ColPos: 0,
	//		},
	//	},
	//})
	tag++
	tc := joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &RightDedupJoin{
			RightTypes: ts,
			Conditions: cs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedHashMap: true,
			Conditions:  cs[1],
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			NeedAllocateSels: false,
			JoinMapTag:       tag,
			JoinMapRefCnt:    1,
		},
	}
	installTestAllocation(t, tc.arg, tc.barg)
	return tc
}

func resetChildren(arg *RightDedupJoin, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetHashBuildChildren(arg *hashbuild.HashBuild, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
