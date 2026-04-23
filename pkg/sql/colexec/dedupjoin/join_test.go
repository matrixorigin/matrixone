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

package dedupjoin

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
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
	arg    *DedupJoin
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.HashBuild
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

func TestDedupJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.barg.IsDedup = true
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
		tc.barg.IsDedup = true
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
		tc.barg.IsDedup = true
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
		arg: &DedupJoin{
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
			NeedAllocateSels: true,
			JoinMapTag:       tag,
			JoinMapRefCnt:    1,
		},
	}
}

func resetChildren(arg *DedupJoin, m *mpool.MPool) {
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

// newCaptureTestProc creates a process with a mock TxnOperator, which is
// required by probe() since it calls proc.GetTxnOperator().Txn().IsPessimistic().
func newCaptureTestProc(t *testing.T) (*process.Process, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Base.TxnOperator = txnOp
	return proc, ctrl
}

// makeInt32Batch creates a batch with the given int32 columns.
// Each element of cols is a column: cols[0] is column 0, cols[1] is column 1, etc.
// nsp[colIdx] lists the null positions for that column (nil = no nulls).
func makeInt32Batch(mp *mpool.MPool, cols [][]int32, nsp [][]uint64) *batch.Batch {
	names := make([]string, len(cols))
	for i := range names {
		names[i] = "c" + string(rune('0'+i))
	}
	bat := batch.New(names)
	for i, data := range cols {
		var ns []uint64
		if nsp != nil && i < len(nsp) {
			ns = nsp[i]
		}
		bat.Vecs[i] = testutil.MakeInt32Vector(data, ns, mp)
	}
	bat.SetRowCount(len(cols[0]))
	return bat
}

// TestDedupJoinCapture tests the REPLACE INTO merged-scan capture path end to
// end. Build side has 2 rows; probe side has 2 rows with matching keys. All
// probe rows hit build buckets and their values are captured. In finalize the
// captured values replace the NULL placeholder column in the output.
func TestDedupJoinCapture(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	// Build batch: key=[10,20], placeholder=[0,0] (values don't matter, will be overwritten)
	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20}, {0, 0}}, [][]uint64{nil, {0, 1}})
	// Probe batch: key=[10,20], old_values=[100,200]
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20}, {100, 200}}, nil)

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)}, // probe conditions
		{newExpr(0, int32Typ)}, // build conditions
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0), // build key
			colexec.NewResultPos(1, 1), // build placeholder (capture target)
		},
		OnDuplicateAction:               plan.Node_FAIL,
		OldColCapturePlaceholderIdxList: []int32{1},
		OldColCaptureProbeIdxList:       []int32{1},
		JoinMapTag:                      curTag,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
		IsDedup:       true,
		DelColIdx:     -1,
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}

	// Set up children
	buildOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat})
	buildArg.Children = nil
	buildArg.AppendChild(buildOp)

	probeOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat})
	dedupArg.Children = nil
	dedupArg.AppendChild(probeOp)

	// Prepare
	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))

	// Build phase: run hashbuild to completion
	res, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	// Probe phase: first call processes the probe batch (capture happens)
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	// FAIL + capture returns 0-row batch from probe (no rowCntInc)
	require.NotNil(t, res.Batch)
	require.Equal(t, 0, res.Batch.RowCount())

	// Finalize: second call transitions to finalize and emits captured results
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, res.Batch.RowCount())

	// Check column 0: build keys [10, 20]
	col0 := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
	require.Equal(t, int32(10), col0[0])
	require.Equal(t, int32(20), col0[1])

	// Check column 1: captured values [100, 200] (replaced NULL placeholders)
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(1))
	col1 := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
	require.Equal(t, int32(100), col1[0])
	require.Equal(t, int32(200), col1[1])

	// End
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestDedupJoinCapturePartialMatch tests capture when only some probe rows
// match build rows. Unmatched build positions keep their original NULL.
func TestDedupJoinCapturePartialMatch(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	// Build: 3 rows with keys [10, 20, 30], placeholder all NULL
	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20, 30}, {0, 0, 0}}, [][]uint64{nil, {0, 1, 2}})
	// Probe: 2 rows with keys [10, 30] (20 has no match), old values [100, 300]
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 30}, {100, 300}}, nil)

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0),
			colexec.NewResultPos(1, 1),
		},
		OnDuplicateAction:               plan.Node_FAIL,
		OldColCapturePlaceholderIdxList: []int32{1},
		OldColCaptureProbeIdxList:       []int32{1},
		JoinMapTag:                      curTag,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
		IsDedup:       true,
		DelColIdx:     -1,
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}

	buildOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat})
	buildArg.Children = nil
	buildArg.AppendChild(buildOp)
	probeOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat})
	dedupArg.Children = nil
	dedupArg.AppendChild(probeOp)

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))

	// Build
	res, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	// Probe
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.Equal(t, 0, res.Batch.RowCount())

	// Finalize: emits all 3 build rows
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 3, res.Batch.RowCount())

	// Column 0: build keys [10, 20, 30]
	col0 := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
	require.Equal(t, int32(10), col0[0])
	require.Equal(t, int32(20), col0[1])
	require.Equal(t, int32(30), col0[2])

	// Column 1: captured [100, NULL, 300]
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0), "row 0 should have captured value 100")
	require.True(t, res.Batch.Vecs[1].GetNulls().Contains(1), "row 1 should remain NULL (no probe match)")
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(2), "row 2 should have captured value 300")
	col1 := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
	require.Equal(t, int32(100), col1[0])
	require.Equal(t, int32(300), col1[2])

	// End
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestDedupJoinCaptureReset verifies that Reset properly cleans up capture
// buffers and the operator can be reused.
func TestDedupJoinCaptureReset(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0),
			colexec.NewResultPos(1, 1),
		},
		OnDuplicateAction:               plan.Node_FAIL,
		OldColCapturePlaceholderIdxList: []int32{1},
		OldColCaptureProbeIdxList:       []int32{1},
		JoinMapTag:                      curTag,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
		IsDedup:       true,
		DelColIdx:     -1,
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
	}

	// --- First run ---
	buildBat1 := makeInt32Batch(proc.Mp(), [][]int32{{10, 20}, {0, 0}}, [][]uint64{nil, {0, 1}})
	probeBat1 := makeInt32Batch(proc.Mp(), [][]int32{{10, 20}, {100, 200}}, nil)

	buildArg.Children = nil
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat1}))
	dedupArg.Children = nil
	dedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat1}))

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))

	// Build
	res, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	// Run to completion
	for {
		res, err = vm.Exec(dedupArg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
	}

	// Reset and rerun
	dedupArg.Reset(proc, false, nil)
	buildArg.Reset(proc, false, nil)
	proc.GetMessageBoard().Reset()

	// Verify capture buffers are cleaned
	require.Nil(t, dedupArg.ctr.capturedVecs)
	require.Nil(t, dedupArg.ctr.captured)
	require.Nil(t, dedupArg.ctr.captureResultIdx)

	// --- Second run with different data ---
	buildBat2 := makeInt32Batch(proc.Mp(), [][]int32{{30}, {0}}, [][]uint64{nil, {0}})
	probeBat2 := makeInt32Batch(proc.Mp(), [][]int32{{30}, {999}}, nil)

	buildArg.Children = nil
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat2}))
	dedupArg.Children = nil
	dedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat2}))

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))

	res, err = vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil)

	// Probe
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)

	// Finalize
	res, err = vm.Exec(dedupArg, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 1, res.Batch.RowCount())
	col1 := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
	require.Equal(t, int32(999), col1[0])

	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// makeCaptureFixture constructs a merger container and a ready-to-send
// WorkerJoinMsg sharing the same bucket layout. Caller owns cleanup of both
// sides via Free of the returned vectors (merger's via its container, msg's
// via freeCapturedVecs or merger ownership transfer).
func makeCaptureFixture(t *testing.T, proc *process.Process, bucketCnt int) (*container, *WorkerJoinMsg) {
	int32Typ := types.T_int32.ToType()
	mkVec := func() *vector.Vector {
		v := vector.NewOffHeapVecWithType(int32Typ)
		require.NoError(t, vector.AppendMultiFixed(v, int32(0), true, bucketCnt, proc.Mp()))
		return v
	}
	ctr := &container{
		capturedVecs: []*vector.Vector{mkVec()},
		captured:     &bitmap.Bitmap{},
		matched:      &bitmap.Bitmap{},
	}
	ctr.captured.InitWithSize(int64(bucketCnt))
	ctr.matched.InitWithSize(int64(bucketCnt))

	msg := &WorkerJoinMsg{
		matched:      &bitmap.Bitmap{},
		captured:     &bitmap.Bitmap{},
		capturedVecs: []*vector.Vector{mkVec()},
	}
	msg.matched.InitWithSize(int64(bucketCnt))
	msg.captured.InitWithSize(int64(bucketCnt))
	return ctr, msg
}

// writeBucketValue sets capturedVecs[0][bucket] = val and records the bucket
// in the accompanying captured bitmap.
func writeBucketValue(t *testing.T, vecs []*vector.Vector, captured *bitmap.Bitmap, bucket uint64, val int32, proc *process.Process) {
	src := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	defer src.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(src, val, false, proc.Mp()))
	require.NoError(t, vecs[0].Copy(src, int64(bucket), 0, proc.Mp()))
	captured.Add(bucket)
}

// TestMergeCaptured_DisjointBuckets covers the common parallel case where
// merger and non-merger captured different buckets. After merge, the merger
// owns the union of both sides.
func TestMergeCaptured_DisjointBuckets(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	ap := &DedupJoin{OldColCapturePlaceholderIdxList: []int32{1}, OldColCaptureProbeIdxList: []int32{1}}
	ctr, msg := makeCaptureFixture(t, proc, 4)

	writeBucketValue(t, ctr.capturedVecs, ctr.captured, 0, 10, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 2, 20, proc)

	require.NoError(t, ctr.mergeCaptured(ap, msg, proc))

	require.True(t, ctr.captured.Contains(0))
	require.True(t, ctr.captured.Contains(2))
	require.False(t, ctr.captured.Contains(1))
	vals := vector.MustFixedColNoTypeCheck[int32](ctr.capturedVecs[0])
	require.Equal(t, int32(10), vals[0])
	require.Equal(t, int32(20), vals[2])

	freeCapturedVecs(msg.capturedVecs, proc)
	for _, v := range ctr.capturedVecs {
		v.Free(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestMergeCaptured_FirstWinsOnConflict verifies that when merger and
// non-merger both captured the same bucket, the merger's value is retained.
func TestMergeCaptured_FirstWinsOnConflict(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	ap := &DedupJoin{OldColCapturePlaceholderIdxList: []int32{1}, OldColCaptureProbeIdxList: []int32{1}}
	ctr, msg := makeCaptureFixture(t, proc, 2)

	writeBucketValue(t, ctr.capturedVecs, ctr.captured, 0, 111, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 0, 222, proc)

	require.NoError(t, ctr.mergeCaptured(ap, msg, proc))

	require.True(t, ctr.captured.Contains(0))
	vals := vector.MustFixedColNoTypeCheck[int32](ctr.capturedVecs[0])
	require.Equal(t, int32(111), vals[0], "merger's existing capture must win")

	freeCapturedVecs(msg.capturedVecs, proc)
	for _, v := range ctr.capturedVecs {
		v.Free(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestMergeCaptured_EmptyWorkerMsg verifies a non-merger worker that captured
// nothing does not corrupt the merger state.
func TestMergeCaptured_EmptyWorkerMsg(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	ap := &DedupJoin{OldColCapturePlaceholderIdxList: []int32{1}, OldColCaptureProbeIdxList: []int32{1}}
	ctr, msg := makeCaptureFixture(t, proc, 2)

	writeBucketValue(t, ctr.capturedVecs, ctr.captured, 1, 77, proc)

	require.NoError(t, ctr.mergeCaptured(ap, msg, proc))

	require.True(t, ctr.captured.Contains(1))
	require.False(t, ctr.captured.Contains(0))
	vals := vector.MustFixedColNoTypeCheck[int32](ctr.capturedVecs[0])
	require.Equal(t, int32(77), vals[1])

	freeCapturedVecs(msg.capturedVecs, proc)
	for _, v := range ctr.capturedVecs {
		v.Free(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestWorkerJoinMsg_ChannelRoundTrip verifies the channel transport:
// non-merger sends a WorkerJoinMsg that transfers capture ownership; receiver
// reads it back and folds it in via mergeCaptured with no leaks.
func TestWorkerJoinMsg_ChannelRoundTrip(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	ap := &DedupJoin{OldColCapturePlaceholderIdxList: []int32{1}, OldColCaptureProbeIdxList: []int32{1}}
	ctr, msg := makeCaptureFixture(t, proc, 3)

	writeBucketValue(t, ctr.capturedVecs, ctr.captured, 0, 1, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 1, 2, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 2, 3, proc)

	ch := make(chan *WorkerJoinMsg, 1)
	ch <- msg
	close(ch)

	received := receiveWorkerMsg(context.Background(), ch)
	require.NotNil(t, received)
	require.Same(t, msg, received)

	require.NoError(t, ctr.mergeCaptured(ap, received, proc))
	freeCapturedVecs(received.capturedVecs, proc)

	require.True(t, ctr.captured.Contains(0))
	require.True(t, ctr.captured.Contains(1))
	require.True(t, ctr.captured.Contains(2))
	vals := vector.MustFixedColNoTypeCheck[int32](ctr.capturedVecs[0])
	require.Equal(t, int32(1), vals[0])
	require.Equal(t, int32(2), vals[1])
	require.Equal(t, int32(3), vals[2])

	for _, v := range ctr.capturedVecs {
		v.Free(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestReceiveWorkerMsg_ContextCancel verifies the receive helper respects
// context cancellation and returns nil (used to unblock the merger when a
// worker dies abnormally).
func TestReceiveWorkerMsg_ContextCancel(t *testing.T) {
	ch := make(chan *WorkerJoinMsg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := receiveWorkerMsg(ctx, ch)
	require.Nil(t, msg)
}

// TestReceiveWorkerMsg_ChannelClose verifies that a closed channel returns nil.
func TestReceiveWorkerMsg_ChannelClose(t *testing.T) {
	ch := make(chan *WorkerJoinMsg)
	close(ch)

	msg := receiveWorkerMsg(context.Background(), ch)
	require.Nil(t, msg)
}
