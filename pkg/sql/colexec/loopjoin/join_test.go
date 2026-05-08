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

package loopjoin

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg         *LoopJoin
	flgs        []bool // flgs[i] == true: nullable
	types       []types.Type
	proc        *process.Process
	cancel      context.CancelFunc
	barg        *hashbuild.HashBuild
	resultBatch *batch.Batch
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []joinTestCase {
	return []joinTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
			require.Equal(t, bytes.Compare(vec1.GetArea(), vec2.GetArea()), 0)
			require.Equal(t, bytes.Compare(vec1.UnsafeGetRawData(), vec2.UnsafeGetRawData()), 0)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		tc.proc.GetMessageBoard().Reset()
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
			require.Equal(t, bytes.Compare(vec1.GetArea(), vec2.GetArea()), 0)
			require.Equal(t, bytes.Compare(vec1.UnsafeGetRawData(), vec2.UnsafeGetRawData()), 0)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestMarkJoinEmitsOneRowPerProbeRowAcrossBuildBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	int32Type := types.T_int32.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	fid := fr.GetEncodedOverloadID()
	cond := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
					}},
				},
			},
			Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
		}},
	}

	tag++
	join := &LoopJoin{
		NonEqCond:  cond,
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_MARK,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 1,
			},
		},
		JoinMapTag: tag,
	}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1, 4}),
	}))

	build := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}
	build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1}),
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1}),
	}))

	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err := vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, res.Batch.RowCount())
	require.Equal(t, []int32{1, 4}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[:2])
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1])[:2])
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(1))

	join.Free(proc, false, nil)
	build.Free(proc, false, nil)
	proc.Free()
}

func TestMarkJoinResumesAfterDefaultBatchSize(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	int32Type := types.T_int32.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	fid := fr.GetEncodedOverloadID()
	cond := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
					}},
				},
			},
			Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
		}},
	}

	tag++
	join := &LoopJoin{
		NonEqCond:  cond,
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_MARK,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 1,
			},
		},
		JoinMapTag: tag,
	}
	leftVals := make([]int32, colexec.DefaultBatchSize+1)
	for i := range leftVals {
		leftVals[i] = int32(i)
	}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), leftVals),
	}))

	build := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}
	build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{-1}),
	}))

	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err := vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, colexec.DefaultBatchSize, res.Batch.RowCount())
	require.Equal(t, int32(0), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[0])
	require.Equal(t, int32(colexec.DefaultBatchSize-1), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[colexec.DefaultBatchSize-1])

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 1, res.Batch.RowCount())
	require.Equal(t, int32(colexec.DefaultBatchSize), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[0])
	require.Equal(t, []bool{false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1])[:1])
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))

	join.Free(proc, false, nil)
	build.Free(proc, false, nil)
	proc.Free()
}

func makeInt32LoopJoinBatch(mp *mpool.MPool, vals []int32) *batch.Batch {
	bat := batch.New([]string{"id"})
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, mp)
	bat.SetRowCount(len(vals))
	return bat
}

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
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

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []colexec.ResultPos) joinTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	ctx, cancel := context.WithCancel(context.Background())
	fr, _ := function.GetFunctionByName(ctx, "=", ts)
	fid := fr.GetEncodedOverloadID()
	args := make([]*plan.Expr, 0, 2)
	args = append(args, &plan.Expr{
		Typ: plan.Type{
			Id: int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
	})
	args = append(args, &plan.Expr{
		Typ: plan.Type{
			Id: int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})
	cond := &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: args,
				Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
			},
		},
	}
	resultBatch := batch.NewWithSize(len(rp))
	resultBatch.SetRowCount(2)
	for i := range rp {
		bat := colexec.MakeMockBatchs(proc.Mp())
		resultBatch.Vecs[i] = bat.Vecs[rp[i].Pos]
	}
	tag++
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &LoopJoin{
			NonEqCond:  cond,
			ResultCols: rp,
			RightTypes: []types.Type{types.T_int32.ToType()},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     1,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedBatches: true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag:    tag,
			JoinMapRefCnt: 1,
		},
		resultBatch: resultBatch,
	}
}

func resetChildren(arg *LoopJoin, m *mpool.MPool) {
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
