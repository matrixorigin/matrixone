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
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

func runRightDedupDuplicateCase(t *testing.T, buildVals, probeVals []int32, pessimistic bool) {
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

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	_, err = vm.Exec(arg, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate entry")

	arg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
}

func TestRightDedupDuplicateTracking(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("one_group_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupDuplicateCase(t, []int32{1}, []int32{1, 1}, pessimistic)
		})
		t.Run(fmt.Sprintf("bitmap_boundary_pessimistic_%t", pessimistic), func(t *testing.T) {
			buildVals := make([]int32, 64)
			for i := range buildVals {
				buildVals[i] = int32(i + 1)
			}
			runRightDedupDuplicateCase(t, buildVals, []int32{64, 64}, pessimistic)
		})
		t.Run(fmt.Sprintf("watermark_pessimistic_%t", pessimistic), func(t *testing.T) {
			buildVals := make([]int32, 100)
			for i := range buildVals {
				buildVals[i] = int32(i + 1)
			}
			runRightDedupDuplicateCase(t, buildVals, []int32{101, 5, 101}, pessimistic)
		})
	}
}

func runRightDedupSpilledEmptyBuild(t *testing.T, pessimistic, duplicateAcrossBatches bool) {
	proc, ctrl := newRightDedupTestProcess(t, pessimistic)
	defer ctrl.Finish()
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
		DedupColName:      "pk",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_int32)}},
		JoinMapTag:        curTag,
		SpillThreshold:    1,
	}

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
	jm.Spilled = true
	jm.SpillBuildFds = make([]*os.File, spillutil.SpillNumBuckets)
	jm.IncRef(1)
	message.SendMessage(message.JoinMapMsg{
		JoinMapPtr: jm,
		IsShuffle:  true,
		ShuffleIdx: 0,
		Tag:        curTag,
		Spilled:    true,
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
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestRightDedupSpilledEmptyBuild(t *testing.T) {
	for _, pessimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("unique_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledEmptyBuild(t, pessimistic, false)
		})
		t.Run(fmt.Sprintf("duplicate_pessimistic_%t", pessimistic), func(t *testing.T) {
			runRightDedupSpilledEmptyBuild(t, pessimistic, true)
		})
	}
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
	return joinTestCase{
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

// TestRightDedupJoinCall exercises the Call/Build/Probe/Finalize cycle
// to cover the non-spill branches in the operator.
func TestRightDedupJoinCall(t *testing.T) {
	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	arg := &RightDedupJoin{
		LeftTypes:  []types.Type{int32Typ},
		RightTypes: []types.Type{int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			{Rel: 0, Pos: 0},
		},
		OnDuplicateAction: plan.Node_FAIL,
		JoinMapTag:        curTag,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Base.TxnOperator = txnOp

	// Build: single row key=10.
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{10}, nil, proc.Mp())
	buildBat.SetRowCount(1)
	buildOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat})
	buildArg.AppendChild(buildOp)

	// Probe: rows key=[10, 20, 30] — all different, no duplicate errors.
	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	probeBat.SetRowCount(3)
	probeOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat})
	arg.AppendChild(probeOp)

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))

	// Build hashmap.
	res, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	// First Call: Build → Probe → processes probe batch.
	res, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)

	// Second Call: probe exhausted → Finalize → End.
	res, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	arg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
}
