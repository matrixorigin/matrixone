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

package left

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	arg         *LeftJoin
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
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
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

func TestJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {

		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
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

		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
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

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
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

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []colexec.ResultPos, cs [][]*plan.Expr) joinTestCase {
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
		bat := colexec.MakeMockBatchs()
		resultBatch.Vecs[i] = bat.Vecs[rp[i].Pos]
	}
	tag++
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &LeftJoin{
			Typs:       ts,
			Result:     rp,
			Conditions: cs,
			Cond:       cond,
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
			NeedBatches:      true,
			JoinMapTag:       tag,
			JoinMapRefCnt:    1,
		},
		resultBatch: resultBatch,
	}
}

func resetChildren(arg *LeftJoin) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetHashBuildChildren(arg *hashbuild.HashBuild) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestJoinSplitNoCond(t *testing.T) {
	cs := [][]*plan.Expr{
		{newExpr(0, types.T_int32.ToType())},
		{newExpr(0, types.T_int32.ToType())},
	}

	types := []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}
	tag := int32(42)
	join := &LeftJoin{
		Typs: types,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		Conditions: cs,
		Cond:       nil,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     1,
				IsFirst: false,
				IsLast:  false,
			},
		},
		JoinMapTag: tag,
	}
	hashbuild := &hashbuild.HashBuild{
		NeedHashMap: true,
		Conditions:  cs[1],
		NeedBatches: true,
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
	}
	bat := batch.New([]string{"a", "b"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 4, 5, 6, 7}, nil)
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 7}, nil)
	bat.SetRowCount(bat.Vecs[0].Length())
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))

	bat2 := batch.New([]string{"a", "b"})
	vals := []int32{0, 2, 3}
	vals = append(vals, slices.Repeat([]int32{1}, 5000)...)
	vals = append(vals, slices.Repeat([]int32{4}, 5000)...)
	vals = append(vals, 5, 5, 7, 7)
	bat2.Vecs[0] = testutil.MakeInt32Vector(vals, nil)
	bat2.Vecs[1] = testutil.MakeInt32Vector(vals, nil)
	bat2.SetRowCount(bat2.Vecs[0].Length())
	hashbuild.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat2}))

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	err := join.Prepare(proc)
	require.NoError(t, err)
	err = hashbuild.Prepare(proc)
	require.NoError(t, err)
	res, err := vm.Exec(hashbuild, proc)
	require.NoError(t, err)
	require.Equal(t, true, res.Batch == nil)

	batCnt := 0
	rowCount := 0
	for end := false; !end; {
		result, er := vm.Exec(join, proc)
		if er != nil {
			t.Fatal(er)
		}
		end = result.Status == vm.ExecStop || result.Batch == nil
		if result.Batch != nil {
			rowCount += result.Batch.RowCount()
			batCnt++
		}
	}

	require.Equal(t, 2, batCnt)
	//                       1   2  4   5 6 7
	require.Equal(t, 5000+1+5000+2+1+2, rowCount)

	join.Free(proc, false, nil)
	hashbuild.Free(proc, false, nil)
	proc.Free()
}

func TestJoinEvalCondFalse(t *testing.T) {
	cs := [][]*plan.Expr{
		{newExpr(0, types.T_int32.ToType())},
		{newExpr(0, types.T_int32.ToType())},
	}
	tag := int32(42)
	args := make([]*plan.Expr, 0, 2)
	typs := []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}
	args = append(args, &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_int32),
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
			Id: int32(types.T_int32),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 1,
			},
		},
	})

	fr, _ := function.GetFunctionByName(
		context.Background(),
		">",
		typs,
	)
	fid := fr.GetEncodedOverloadID()
	cond := &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: args,
				Func: &plan.ObjectRef{Obj: fid, ObjName: ">"},
			},
		},
	}
	join := &LeftJoin{
		Typs: typs,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		Conditions: cs,
		Cond:       cond,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     1,
				IsFirst: false,
				IsLast:  false,
			},
		},
		JoinMapTag: tag,
	}
	hashbuild := &hashbuild.HashBuild{
		NeedHashMap: true,
		Conditions:  cs[1],
		NeedBatches: true,
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
	}
	// trigger sels for one row
	bat := batch.New([]string{"a", "b"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 4, 5, 6, 7}, nil)
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 7}, nil)
	bat.SetRowCount(bat.Vecs[0].Length())
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))

	bat2 := batch.New([]string{"a", "b"})
	vals := []int32{0, 2, 3}                                // !(2 > 2), 2 will be filtered out
	vals = append(vals, slices.Repeat([]int32{1}, 5000)...) // !(1 > 2), 1 will be filtered out
	vals = append(vals, slices.Repeat([]int32{4}, 5050)...) // 4 > 2, 4 will be selected
	vals = append(vals, 5, 5, 7, 7)                         // 5(7) > 2, 5(7) will be selected
	col2 := slices.Repeat([]int32{2}, len(vals))
	bat2.Vecs[0] = testutil.MakeInt32Vector(vals, nil)
	bat2.Vecs[1] = testutil.MakeInt32Vector(col2, nil)
	bat2.SetRowCount(bat2.Vecs[0].Length())
	hashbuild.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat2}))

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	err := join.Prepare(proc)
	require.NoError(t, err)
	err = hashbuild.Prepare(proc)
	require.NoError(t, err)
	res, err := vm.Exec(hashbuild, proc)
	require.NoError(t, err)
	require.Equal(t, true, res.Batch == nil)

	batCnt := 0
	rowCount := 0
	for end := false; !end; {
		result, er := vm.Exec(join, proc)
		if er != nil {
			t.Fatal(er)
		}
		end = result.Status == vm.ExecStop || result.Batch == nil
		if result.Batch != nil {
			rowCount += result.Batch.RowCount()
			batCnt++
		}
	}

	require.Equal(t, 1, batCnt)
	//                       1  2  4  5 6 7
	require.Equal(t, 1+1+5050+2+1+2, rowCount)

	{
		// trigger hashOnUnique
		bat := batch.New([]string{"a", "b"})
		lvals := make([]int32, 0, 9000)
		for i := 0; i < 9000; i++ {
			lvals = append(lvals, int32(i))
		}
		bat.Vecs[0] = testutil.MakeInt32Vector(lvals, nil)
		bat.Vecs[1] = testutil.MakeInt32Vector(lvals, nil)
		bat.SetRowCount(bat.Vecs[0].Length())
		join.Reset(proc, false, nil)
		join.Children = nil
		join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))

		bat2 := batch.New([]string{"a", "b"})
		vals := []int32{0, 2, 3, 4, 6, 7, 8}         // no dup values
		col2 := slices.Repeat([]int32{5}, len(vals)) // make 6, 7, 8 in left table not null
		bat2.Vecs[0] = testutil.MakeInt32Vector(vals, nil)
		bat2.Vecs[1] = testutil.MakeInt32Vector(col2, nil)
		bat2.SetRowCount(bat2.Vecs[0].Length())
		hashbuild.Reset(proc, false, nil)
		hashbuild.Children = nil
		hashbuild.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat2}))

		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		proc.SetMessageBoard(message.NewMessageBoard())

		err := join.Prepare(proc)
		require.NoError(t, err)
		err = hashbuild.Prepare(proc)
		require.NoError(t, err)
		res, err := vm.Exec(hashbuild, proc)
		require.NoError(t, err)
		require.Equal(t, true, res.Batch == nil)

		batCnt := 0
		rowCount := 0
		nullCount := 0
		for end := false; !end; {
			result, er := vm.Exec(join, proc)
			if er != nil {
				t.Fatal(er)
			}
			end = result.Status == vm.ExecStop || result.Batch == nil
			if result.Batch != nil {
				rowCount += result.Batch.RowCount()
				batCnt++
				nullCount += result.Batch.Vecs[1].GetNulls().GetCardinality()
			}
		}

		require.Equal(t, 2, batCnt)
		require.Equal(t, 9000, rowCount)
		require.Equal(t, 9000-3, nullCount)

	}

	join.Free(proc, false, nil)
	hashbuild.Free(proc, false, nil)
	proc.Free()

}
