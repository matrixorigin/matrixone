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
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	arg    *DedupJoin
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.HashBuild
}

func newDedupTestSpillEngine(
	t *testing.T,
	cfg spillutil.SpillEngineConfig,
) *spillutil.SpillEngine {
	t.Helper()
	if cfg.Budget == nil {
		budget := process.MustNewExecutionResourceBudget(1<<60, 1<<60)
		var err error
		cfg.Budget, err = budget.OpenGeneration(1)
		require.NoError(t, err)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, cfg.Budget)
	require.NoError(t, err)
	engine, err := spillutil.NewSpillEngine(
		cfg,
		account,
		hashbuild.HashBuildAllocationOwner,
	)
	require.NoError(t, err)
	return engine
}

func TestDedupFinalizeCleansConsumedBuffer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()

	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, proc.Mp()))
	bat.SetRowCount(1)

	arg := &DedupJoin{}
	arg.ctr.state = Finalize
	arg.ctr.buf = []*batch.Batch{bat}
	arg.ctr.lastPos = 1
	arg.ctr.spillEngine = newDedupTestSpillEngine(
		t,
		spillutil.SpillEngineConfig{},
	)

	res, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, res.Status)
	require.Nil(t, arg.ctr.buf)
	require.Equal(t, baseline, proc.Mp().CurrNB())
	arg.Free(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().CurrNB())
	proc.Free()
}

func TestWithRestoredJoinBat1VectorsRestoresOwnerOnError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	original := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	executorOwned := testutil.MakeInt32Vector([]int32{2}, nil, proc.Mp())
	joinBat := batch.NewWithSize(1)
	joinBat.Vecs[0] = original
	joinBat.SetRowCount(1)
	ctr := container{joinBat1: joinBat}
	wantErr := errors.New("injected update expression failure")

	err := ctr.withRestoredJoinBat1Vectors([]int32{0}, func() error {
		ctr.joinBat1.Vecs[0] = executorOwned
		return wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Same(t, original, ctr.joinBat1.Vecs[0])
	joinBat.Clean(proc.Mp())
	executorOwned.Free(proc.Mp())
	require.Equal(t, baseline, proc.Mp().CurrNB())
	proc.Free()
}

func writeDedupSpillBatch(t *testing.T, proc *process.Process, name string, value int32) *os.File {
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fd, err := spillfs.CreateAndRemoveFile(proc.Ctx, name)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{value}, nil, proc.Mp())
	bat.SetRowCount(1)
	var payload bytes.Buffer
	require.NoError(t, bat.MarshalBinaryWithGroupingTo(&payload))
	rows, size, magic := int64(1), int64(payload.Len()), uint64(spillutil.SpillMagic)
	for _, part := range [][]byte{
		types.EncodeInt64(&rows),
		types.EncodeInt64(&size),
		payload.Bytes(),
		types.EncodeUint64(&magic),
	} {
		_, err = fd.Write(part)
		require.NoError(t, err)
	}
	_, err = fd.Seek(0, io.SeekStart)
	require.NoError(t, err)
	bat.Clean(proc.Mp())
	return fd
}

func newDedupSpillFile(t *testing.T, fd *os.File, rows int64) *message.SpillFile {
	t.Helper()
	info, err := fd.Stat()
	require.NoError(t, err)
	return message.NewSpillFile(fd, rows, uint64(info.Size()), nil)
}

func TestDedupSpillAdvancesAfterOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	typ := types.T_int32.ToType()
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	engine := newDedupTestSpillEngine(t, spillutil.SpillEngineConfig{
		BuildKeyExprs:           conditions[1],
		NeedBatches:             true,
		NeedsBuildForEmptyProbe: true,
		IsDedup:                 true,
	})
	engine.InitFromSpilledFiles([]*message.SpillFile{
		newDedupSpillFile(
			t,
			writeDedupSpillBatch(t, proc, "dedup_bucket_1", 1),
			1,
		),
		newDedupSpillFile(
			t,
			writeDedupSpillBatch(t, proc, "dedup_bucket_2", 2),
			1,
		),
	})

	arg := &DedupJoin{
		RightTypes:        []types.Type{typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{{Rel: 1, Pos: 0}},
		OnDuplicateAction: plan.Node_FAIL,
	}
	installTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.state = Finalize
	arg.ctr.spillEngine = engine

	for _, want := range []int32{1, 2} {
		res, err := arg.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecHasMore, res.Status)
		require.Equal(t, []int32{want}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0]))
	}
	res, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, res.Status)
	require.Nil(t, arg.ctr.buf)

	arg.Free(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().CurrNB())
	proc.Free()
}

func TestDedupResetClearsBucketState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &DedupJoin{}
	arg.ctr.batches = []*batch.Batch{batch.EmptyBatch}
	arg.ctr.batchRowCount = 1
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.batches)
	require.Zero(t, arg.ctr.batchRowCount)
	require.Nil(t, arg.ctr.matched)
	proc.Free()
}

func TestDedupShuffleWorkersFinalizeTheirOwnPartitions(t *testing.T) {
	tests := []struct {
		name        string
		isShuffle   bool
		wantOutput  bool
		wantMessage bool
	}{
		{
			name:        "broadcast worker defers to merger",
			wantMessage: true,
		},
		{
			name:       "shuffle worker emits local partition",
			isShuffle:  true,
			wantOutput: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			baseline := proc.Mp().CurrNB()
			typ := types.T_int32.ToType()
			bat := batch.NewOffHeapWithSize(1)
			bat.Vecs[0] = vector.NewOffHeapVecWithType(typ)
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(42), false, proc.Mp()))
			bat.SetRowCount(1)

			jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, []*batch.Batch{bat}, proc.Mp())
			jm.SetRowCount(1)
			jm.IncRef(1)
			matched := &bitmap.Bitmap{}
			matched.InitWithSize(1)
			mailbox := NewWorkerJoinMailbox(2)
			arg := &DedupJoin{
				RightTypes:        []types.Type{typ},
				Result:            []colexec.ResultPos{{Rel: 1, Pos: 0}},
				OnDuplicateAction: plan.Node_FAIL,
				NumCPU:            2,
				IsMerger:          false,
				IsShuffle:         test.isShuffle,
				Mailbox:           mailbox,
			}
			arg.ctr.mp = jm
			arg.ctr.batches = jm.GetBatches()
			arg.ctr.batchRowCount = jm.GetRowCount()
			arg.ctr.matched = matched

			if test.wantMessage {
				errC := make(chan error, 1)
				go func() {
					errC <- arg.ctr.finalize(arg, proc)
				}()
				msg, err := receiveWorkerMsg(proc.Ctx, mailbox)
				require.NoError(t, err)
				require.Same(t, matched, msg.matched)
				mailbox.completeRound()
				require.NoError(t, <-errC)
			} else {
				require.NoError(t, arg.ctr.finalize(arg, proc))
			}
			if test.wantOutput {
				require.Len(t, arg.ctr.buf, 1)
				require.Equal(t, []int32{42}, vector.MustFixedColNoTypeCheck[int32](arg.ctr.buf[0].Vecs[0]))
			} else {
				require.Nil(t, arg.ctr.buf)
			}
			require.Empty(t, mailbox.ch)

			arg.Free(proc, false, nil)
			require.Equal(t, baseline, proc.Mp().CurrNB())
			proc.Free()
		})
	}
}

func TestDedupResetNotifiesOnlySharedBuildMerger(t *testing.T) {
	for _, test := range []struct {
		name        string
		isShuffle   bool
		wantMessage bool
	}{
		{name: "broadcast worker notifies merger", wantMessage: true},
		{name: "shuffle worker owns its partition", isShuffle: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			mailbox := NewWorkerJoinMailbox(2)
			arg := &DedupJoin{
				NumCPU:    2,
				IsMerger:  false,
				IsShuffle: test.isShuffle,
				Mailbox:   mailbox,
			}

			arg.Reset(proc, false, nil)

			require.Equal(t, test.wantMessage, len(mailbox.ch) == 1)
			if test.wantMessage {
				msg := <-mailbox.ch
				require.True(t, msg.aborted)
				require.NoError(t, msg.err)
			}
			proc.Free()
		})
	}
}

func TestDedupResetReportsWorkerFailure(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	mailbox := NewWorkerJoinMailbox(2)
	arg := &DedupJoin{
		NumCPU:   2,
		IsMerger: false,
		Mailbox:  mailbox,
	}
	workerErr := moerr.NewInternalErrorNoCtx("worker failed")

	arg.Reset(proc, true, workerErr)

	msg := <-mailbox.ch
	require.True(t, msg.aborted)
	require.ErrorIs(t, msg.err, workerErr)
}

func TestDedupPrepareFailureCanRetry(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	typ := types.T_int32.ToType()
	valid := newExpr(0, typ)
	invalid := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}}
	arg := &DedupJoin{
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
	require.Len(t, arg.ctr.evecs, 1)
	require.Len(t, arg.ctr.exprExecs, 2)
	arg.Free(proc, false, nil)
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
	tc := joinTestCase{
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
	installTestAllocation(t, tc.arg, tc.barg)
	return tc
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
	installTestAllocation(t, dedupArg, buildArg)

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
	installTestAllocation(t, dedupArg, buildArg)

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
	installTestAllocation(t, dedupArg, buildArg)

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

// TestWorkerJoinMsg_MailboxRoundTrip verifies the mailbox transport:
// non-merger sends a WorkerJoinMsg that transfers capture ownership; receiver
// reads it back and folds it in via mergeCaptured with no leaks.
func TestWorkerJoinMsg_MailboxRoundTrip(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	ap := &DedupJoin{OldColCapturePlaceholderIdxList: []int32{1}, OldColCaptureProbeIdxList: []int32{1}}
	ctr, msg := makeCaptureFixture(t, proc, 3)

	writeBucketValue(t, ctr.capturedVecs, ctr.captured, 0, 1, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 1, 2, proc)
	writeBucketValue(t, msg.capturedVecs, msg.captured, 2, 3, proc)

	mailbox := NewWorkerJoinMailbox(2)
	sent, stopped, _ := mailbox.trySend(msg)
	require.True(t, sent)
	require.False(t, stopped)

	received, err := receiveWorkerMsg(context.Background(), mailbox)
	require.NoError(t, err)
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

// TestReceiveWorkerMsg_ContextCancel verifies the receive helper preserves
// the cancellation cause used to unblock the merger.
func TestReceiveWorkerMsg_ContextCancel(t *testing.T) {
	mailbox := NewWorkerJoinMailbox(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg, err := receiveWorkerMsg(ctx, mailbox)
	require.Nil(t, msg)
	require.ErrorIs(t, err, context.Canceled)
}

// TestReceiveWorkerMsg_ChannelClose verifies that premature channel closure is
// not mistaken for a successful end-of-stream.
func TestReceiveWorkerMsg_ChannelClose(t *testing.T) {
	mailbox := NewWorkerJoinMailbox(2)
	close(mailbox.ch)

	msg, err := receiveWorkerMsg(context.Background(), mailbox)
	require.Nil(t, msg)
	require.Error(t, err)
}

func TestReceiveWorkerMsg_RejectsMissingMailboxAndNilStatus(t *testing.T) {
	msg, err := receiveWorkerMsg(context.Background(), nil)
	require.Nil(t, msg)
	require.ErrorContains(t, err, "mailbox is not initialized")

	mailbox := NewWorkerJoinMailbox(2)
	mailbox.ch <- nil
	msg, err = receiveWorkerMsg(context.Background(), mailbox)
	require.Nil(t, msg)
	require.ErrorContains(t, err, "empty finalize status")
}

func TestDedupFinalizeWorkerPublicationBoundaries(t *testing.T) {
	t.Run("missing mailbox", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		t.Cleanup(proc.Free)
		worker := &DedupJoin{
			NumCPU:   2,
			IsMerger: false,
		}

		err := worker.ctr.finalize(worker, proc)
		require.ErrorContains(t, err, "mailbox is not initialized")
	})

	t.Run("canceled before publication", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		t.Cleanup(proc.Free)
		ctx, cancel := context.WithCancel(proc.Ctx)
		cancel()
		proc.Ctx = ctx
		worker := &DedupJoin{
			NumCPU:   2,
			IsMerger: false,
			Mailbox:  NewWorkerJoinMailbox(2),
		}

		err := worker.ctr.finalize(worker, proc)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, worker.Mailbox.ch)
	})

	t.Run("merger already stopped", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		t.Cleanup(proc.Free)
		mailbox := NewWorkerJoinMailbox(2)
		mailbox.stopAndDrain(proc)
		worker := &DedupJoin{
			NumCPU:   2,
			IsMerger: false,
			Mailbox:  mailbox,
		}

		require.NoError(t, worker.ctr.finalize(worker, proc))
		require.False(t, worker.ctr.roundStatusPublished)
		require.Equal(t, End, worker.ctr.state)
	})

	t.Run("full mailbox", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		t.Cleanup(proc.Free)
		mailbox := NewWorkerJoinMailbox(1)
		sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{})
		require.True(t, sent)
		require.False(t, stopped)
		t.Cleanup(func() {
			mailbox.stopAndDrain(proc)
		})
		worker := &DedupJoin{
			NumCPU:   2,
			IsMerger: false,
			Mailbox:  mailbox,
		}

		err := worker.ctr.finalize(worker, proc)
		require.ErrorContains(t, err, "mailbox is unexpectedly full")
	})
}

func TestWorkerJoinMailboxStopAndSendHaveSingleCaptureOwner(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	t.Cleanup(proc.Free)

	for range 32 {
		mailbox := NewWorkerJoinMailbox(2)
		captured := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(captured, int32(1), false, proc.Mp()))

		start := make(chan struct{})
		sendResult := make(chan [2]bool, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			sent, stopped, _ := mailbox.trySend(
				&WorkerJoinMsg{capturedVecs: []*vector.Vector{captured}},
			)
			sendResult <- [2]bool{sent, stopped}
		}()
		go func() {
			defer wg.Done()
			<-start
			mailbox.stopAndDrain(proc)
		}()
		close(start)
		wg.Wait()

		result := <-sendResult
		if !result[0] {
			require.True(t, result[1])
			captured.Free(proc.Mp())
		}
		require.Equal(t, baseline, proc.Mp().CurrNB())
	}
}

func TestWorkerJoinMailboxReopensAfterCompleteResetGeneration(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	mailbox := NewWorkerJoinMailbox(2)

	mailbox.stopAndDrain(proc)
	sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{})
	require.False(t, sent)
	require.True(t, stopped)

	mailbox.resetParticipant(proc)
	sent, stopped, _ = mailbox.trySend(&WorkerJoinMsg{})
	require.False(t, sent)
	require.True(t, stopped)

	mailbox.resetParticipant(proc)
	sent, stopped, _ = mailbox.trySend(&WorkerJoinMsg{})
	require.True(t, sent)
	require.False(t, stopped)
	mailbox.drain(proc)
}

func TestDedupFinalizeMissingWorkerHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &DedupJoin{
		NumCPU:            2,
		IsMerger:          true,
		Mailbox:           NewWorkerJoinMailbox(2),
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()

	t.Cleanup(func() {
		arg.Free(proc, true, context.Canceled)
		proc.Free()
	})

	result, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
}

func TestDedupFinalizeConcurrentCancellationReturns(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &DedupJoin{
		NumCPU:            2,
		IsMerger:          true,
		Mailbox:           NewWorkerJoinMailbox(2),
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx

	resultC := make(chan error, 1)
	resultReceived := false
	t.Cleanup(func() {
		cancel()
		if !resultReceived {
			cleanupGuard := time.NewTimer(2 * time.Second)
			defer cleanupGuard.Stop()
			select {
			case <-resultC:
			case <-cleanupGuard.C:
				return
			}
		}
		arg.Free(proc, true, context.Canceled)
		proc.Free()
	})
	go func() {
		_, err := arg.Call(proc)
		resultC <- err
	}()
	cancel()

	guard := time.NewTimer(2 * time.Second)
	defer guard.Stop()
	select {
	case err := <-resultC:
		resultReceived = true
		require.ErrorIs(t, err, context.Canceled)
	case <-guard.C:
		t.Fatal("dedup finalize did not return after cancellation")
	}
}

func TestDedupFinalizeWorkerFailureCleansTransferredMessages(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	captured := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(captured, int32(1), false, proc.Mp()))

	workerErr := moerr.NewInternalErrorNoCtx("worker failed")
	mailbox := NewWorkerJoinMailbox(3)
	arg := &DedupJoin{
		NumCPU:            3,
		IsMerger:          true,
		Mailbox:           mailbox,
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)
	require.True(t, func() bool {
		sent, _, _ := mailbox.trySend(&WorkerJoinMsg{aborted: true, err: workerErr})
		return sent
	}())
	require.True(t, func() bool {
		sent, _, _ := mailbox.trySend(&WorkerJoinMsg{capturedVecs: []*vector.Vector{captured}})
		return sent
	}())
	t.Cleanup(func() {
		arg.Free(proc, true, workerErr)
		proc.Free()
	})

	result, err := arg.Call(proc)
	require.ErrorIs(t, err, workerErr)
	require.Nil(t, result.Batch)
	require.Empty(t, mailbox.ch)
	require.Equal(t, baseline, proc.Mp().CurrNB())
}

func TestDedupFinalizeWorkerFailureDoesNotWaitForMissingWorker(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	workerErr := moerr.NewInternalErrorNoCtx("worker failed")
	mailbox := NewWorkerJoinMailbox(3)
	arg := &DedupJoin{
		NumCPU:            3,
		IsMerger:          true,
		Mailbox:           mailbox,
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	proc.Ctx = ctx
	resultC := make(chan error, 1)
	resultReceived := false
	t.Cleanup(func() {
		if !resultReceived {
			proc.Cancel(workerErr)
			<-resultC
		}
		arg.Free(proc, true, workerErr)
		proc.Free()
	})
	sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{aborted: true, err: workerErr})
	require.True(t, sent)
	require.False(t, stopped)
	cancel(context.Canceled)
	go func() {
		_, err := arg.Call(proc)
		resultC <- err
	}()

	guard := time.NewTimer(2 * time.Second)
	defer guard.Stop()

	var err error
	select {
	case err = <-resultC:
		resultReceived = true
	case <-guard.C:
		t.Fatal("merger waited for a missing worker after cancellation")
	}
	require.ErrorIs(t, err, workerErr)
}

func TestDedupFinalizeNormalAbortDoesNotHideCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	mailbox := NewWorkerJoinMailbox(2)
	sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{aborted: true})
	require.True(t, sent)
	require.False(t, stopped)
	arg := &DedupJoin{
		NumCPU:            2,
		IsMerger:          true,
		Mailbox:           mailbox,
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	t.Cleanup(func() {
		arg.Free(proc, true, context.Canceled)
		proc.Free()
	})

	result, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
}

func TestDedupFinalizeMailboxSupportsMultipleSpillBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	mailbox := NewWorkerJoinMailbox(3)
	workers := []*DedupJoin{
		{
			NumCPU:   3,
			IsMerger: false,
			Mailbox:  mailbox,
		},
		{
			NumCPU:   3,
			IsMerger: false,
			Mailbox:  mailbox,
		},
	}

	for bucket := range 2 {
		for i, worker := range workers {
			worker.ctr.matched = &bitmap.Bitmap{}
			worker.ctr.matched.InitWithSize(4)
			worker.ctr.matched.Add(uint64(bucket*2 + i))
		}

		fastErrC := make(chan error, 1)
		go func() {
			fastErrC <- workers[0].ctr.finalize(workers[0], proc)
		}()
		fastMsg, err := receiveWorkerMsg(proc.Ctx, mailbox)
		require.NoError(t, err)
		require.True(t, fastMsg.matched.Contains(uint64(bucket*2)))
		require.Empty(t, mailbox.ch)
		select {
		case err := <-fastErrC:
			require.NoError(t, err)
			t.Fatal("fast worker advanced before the slow worker published its spill bucket")
		default:
		}

		slowErrC := make(chan error, 1)
		go func() {
			slowErrC <- workers[1].ctr.finalize(workers[1], proc)
		}()
		slowMsg, err := receiveWorkerMsg(proc.Ctx, mailbox)
		require.NoError(t, err)
		require.True(t, slowMsg.matched.Contains(uint64(bucket*2+1)))
		select {
		case err := <-slowErrC:
			require.NoError(t, err)
			t.Fatal("slow worker advanced before the merger completed the spill bucket")
		default:
		}

		mailbox.completeRound()
		require.NoError(t, <-fastErrC)
		require.NoError(t, <-slowErrC)
		for _, worker := range workers {
			require.False(t, worker.ctr.roundStatusPublished)
		}
	}
}

func TestDedupFinalizeResetPublishesAbortForNextSpillBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)

	mailbox := NewWorkerJoinMailbox(2)
	worker := &DedupJoin{
		NumCPU:   2,
		IsMerger: false,
		Mailbox:  mailbox,
	}
	worker.ctr.matched = &bitmap.Bitmap{}
	worker.ctr.matched.InitWithSize(1)

	finalizeErrC := make(chan error, 1)
	go func() {
		finalizeErrC <- worker.ctr.finalize(worker, proc)
	}()

	first, err := receiveWorkerMsg(proc.Ctx, mailbox)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.False(t, first.aborted)
	mailbox.completeRound()
	require.NoError(t, <-finalizeErrC)
	require.False(t, worker.ctr.roundStatusPublished)

	// A normal upper-operator early stop can Reset this worker before it
	// reaches the next spill bucket. The merger may already be waiting in that
	// next round, so Reset must publish a terminal status for it.
	worker.Reset(proc, false, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := receiveWorkerMsg(ctx, mailbox)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.True(t, second.aborted)
	require.NoError(t, second.err)
	freeWorkerJoinMsg(second, proc)

	merger := &DedupJoin{
		NumCPU:   2,
		IsMerger: true,
		Mailbox:  mailbox,
	}
	merger.Reset(proc, false, nil)
	require.Empty(t, mailbox.ch)
}

func TestDedupFinalizeCancellationAfterPublishDoesNotDuplicateStatus(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	t.Cleanup(proc.Free)

	mailbox := NewWorkerJoinMailbox(2)
	worker := &DedupJoin{
		NumCPU:   2,
		IsMerger: false,
		Mailbox:  mailbox,
	}
	worker.ctr.matched = &bitmap.Bitmap{}
	worker.ctr.matched.InitWithSize(1)

	errC := make(chan error, 1)
	go func() {
		errC <- worker.ctr.finalize(worker, proc)
	}()
	msg, err := receiveWorkerMsg(proc.Ctx, mailbox)
	require.NoError(t, err)
	require.NotNil(t, msg)

	cancel()
	require.ErrorIs(t, <-errC, context.Canceled)
	worker.Reset(proc, true, context.Canceled)
	require.Empty(t, mailbox.ch, "Reset must not publish a second status for the same worker and bucket")
	mailbox.stopAndDrain(proc)
}

func TestDedupFinalizeNormalWorkerAbortStopsWithoutPartialOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	typ := types.T_int32.ToType()
	build := batch.NewOffHeapWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{42}, nil, proc.Mp())
	build.SetRowCount(1)
	joinMap := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil,
		[]*batch.Batch{build}, proc.Mp(),
	)
	joinMap.SetRowCount(1)
	joinMap.IncRef(1)

	mailbox := NewWorkerJoinMailbox(2)
	sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{aborted: true})
	require.True(t, sent)
	require.False(t, stopped)
	arg := &DedupJoin{
		RightTypes:        []types.Type{typ},
		Result:            []colexec.ResultPos{{Rel: 1, Pos: 0}},
		NumCPU:            2,
		IsMerger:          true,
		Mailbox:           mailbox,
		OnDuplicateAction: plan.Node_FAIL,
	}
	arg.ctr.state = Finalize
	arg.ctr.mp = joinMap
	arg.ctr.batches = joinMap.GetBatches()
	arg.ctr.batchRowCount = joinMap.GetRowCount()
	arg.ctr.matched = &bitmap.Bitmap{}
	arg.ctr.matched.InitWithSize(1)
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		require.Equal(t, baseline, proc.Mp().CurrNB())
		proc.Free()
	})

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)
}

func TestDedupFinalizeParallelMergePreservesDataAcrossReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseline := proc.Mp().CurrNB()
	mailbox := NewWorkerJoinMailbox(2)
	arg := &DedupJoin{
		RightTypes:        []types.Type{types.T_int32.ToType()},
		Result:            []colexec.ResultPos{{Rel: 1, Pos: 0}},
		OnDuplicateAction: plan.Node_IGNORE,
		NumCPU:            2,
		IsMerger:          true,
		Mailbox:           mailbox,
	}
	workerArg := &DedupJoin{
		NumCPU:   2,
		IsMerger: false,
		Mailbox:  mailbox,
	}
	installTestAllocation(t, arg, workerArg)
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			arg.Free(proc, false, nil)
			proc.Free()
		}
	})

	run := func(buildValues []int32, mergerMatches, workerMatches []uint64, want []int32) {
		build := batch.NewOffHeapWithSize(1)
		build.Vecs[0] = testutil.MakeInt32Vector(buildValues, nil, proc.Mp())
		build.SetRowCount(len(buildValues))
		joinMap := message.NewJoinMap(
			message.GroupSels{}, nil, nil, nil,
			[]*batch.Batch{build}, proc.Mp(),
		)
		joinMap.SetRowCount(int64(len(buildValues)))
		joinMap.IncRef(1)

		arg.ctr.state = Finalize
		arg.ctr.mp = joinMap
		arg.ctr.batches = joinMap.GetBatches()
		arg.ctr.batchRowCount = joinMap.GetRowCount()
		arg.ctr.matched = &bitmap.Bitmap{}
		arg.ctr.matched.InitWithSize(int64(len(buildValues)))
		for _, row := range mergerMatches {
			arg.ctr.matched.Add(row)
		}
		worker := &bitmap.Bitmap{}
		worker.InitWithSize(int64(len(buildValues)))
		for _, row := range workerMatches {
			worker.Add(row)
		}
		sent, stopped, _ := mailbox.trySend(&WorkerJoinMsg{matched: worker})
		require.True(t, sent)
		require.False(t, stopped)

		result, err := arg.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecHasMore, result.Status)
		require.Equal(t, want, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]))
		result, err = arg.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		require.Empty(t, mailbox.ch)
	}

	run([]int32{10, 20, 30, 40}, []uint64{0}, []uint64{2}, []int32{20, 40})
	workerArg.Reset(proc, false, nil)
	arg.Reset(proc, false, nil)
	run([]int32{50, 60}, nil, []uint64{0}, []int32{60})

	workerArg.Reset(proc, false, nil)
	arg.Reset(proc, false, nil)
	arg.Free(proc, false, nil)
	workerArg.Free(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().CurrNB())
	proc.Free()
	cleaned = true
}
