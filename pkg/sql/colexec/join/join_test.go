// Copyright 2021 - 2022 Matrix Origin
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

package join

import (
	"bytes"
	"context"
	"testing"

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
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg         *InnerJoin
	flgs        []bool // flgs[i] == true: nullable
	types       []types.Type
	proc        *process.Process
	cancel      context.CancelFunc
	barg        *hashbuild.HashBuild
	resultBatch *batch.Batch
}

var (
	tcs []joinTestCase
	tag int32
)

func init() {
	tag = 0
	tcs = []joinTestCase{
		newTestCase(2, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
		newTestCase(2, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
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
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range tcs {

		resetChildren(tc.arg)
		resetHashBuildChildren(tc.barg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := tc.barg.Call(tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = tc.arg.Call(tc.proc)
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
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = tc.barg.Call(tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = tc.arg.Call(tc.proc)
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
func TestLowCardinalityJoin(t *testing.T) {
	tc := newTestCase([]bool{false}, []types.Type{types.T_varchar.ToType()}, []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		[][]*plan.Expr{
			{
				newExpr(0, types.T_varchar.ToType()),
			},
			{
				newExpr(0, types.T_varchar.ToType()),
			},
		})
	tc.arg.Cond = nil // only numeric type can be compared

	values0 := []string{"a", "b", "a", "c", "b", "c", "a", "a"}
	v0 := testutil.NewVector(len(values0), types.T_varchar.ToType(), tc.proc.Mp(), false, values0)
	constructIndex(t, v0, tc.proc.Mp())

	// hashbuild
	bat := hashBuildWithBatch(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v0}, nil))

	values1 := []string{"c", "d", "c", "c", "b", "a", "b", "d", "a", "b"}
	v1 := testutil.NewVector(len(values1), types.T_varchar.ToType(), tc.proc.Mp(), false, values1)

	// probe
	// only the join column of right table is indexed
	rbat := probeWithBatches(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v1}, nil), bat)

	result := rbat.Vecs[0]
	require.NotNil(t, result.Index())
	resultIdx := result.Index().(*index.LowCardinalityIndex)
	require.Equal(
		t,
		[]uint16{3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 2, 2},
		vector.MustFixedCol[uint16](resultIdx.GetPoses()),
	)
}

func TestLowCardinalityIndexesJoin(t *testing.T) {
	tc := newTestCase([]bool{false}, []types.Type{types.T_varchar.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{
				newExpr(0, types.T_varchar.ToType()),
			},
			{
				newExpr(0, types.T_varchar.ToType()),
			},
		})
	tc.arg.Cond = nil // only numeric type can be compared

	values0 := []string{"a", "b", "a", "c", "b", "c", "a", "a"}
	v0 := testutil.NewVector(len(values0), types.T_varchar.ToType(), tc.proc.Mp(), false, values0)
	constructIndex(t, v0, tc.proc.Mp())

	// hashbuild
	bat := hashBuildWithBatch(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v0}, nil))

	values1 := []string{"c", "d", "c", "c", "b", "a", "b", "d", "a", "b"}
	v1 := testutil.NewVector(len(values1), types.T_varchar.ToType(), tc.proc.Mp(), false, values1)
	constructIndex(t, v1, tc.proc.Mp())

	// probe
	// the join columns of both left table and right table are indexed
	rbat := probeWithBatches(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v1}, nil), bat)

	result := rbat.Vecs[0]
	require.NotNil(t, result.Index())
	resultIdx := result.Index().(*index.LowCardinalityIndex)
	require.Equal(
		t,
		[]uint16{1, 1, 1, 1, 1, 1, 3, 3, 4, 4, 4, 4, 3, 3, 4, 4, 4, 4, 3, 3},
		vector.MustFixedCol[uint16](resultIdx.GetPoses()),
	)
}
*/
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
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
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

func newTestCase(rows int, flgs []bool, ts []types.Type, rp []colexec.ResultPos, cs [][]*plan.Expr) joinTestCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
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
	resultBatch.SetRowCount(rows)
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
		arg: &InnerJoin{
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
			Typs:            ts,
			NeedHashMap:     true,
			Conditions:      cs[1],
			NeedMergedBatch: true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			NeedAllocateSels: true,
			JoinMapTag:       tag,
		},
		resultBatch: resultBatch,
	}
}

/*
func hashBuild(t *testing.T, tc joinTestCase) []*batch.Batch {
	return hashBuildWithBatch(t, tc, newBatch(tc.types, tc.proc, Rows))
}

func hashBuildWithBatch(t *testing.T, tc joinTestCase, bat *batch.Batch) []*batch.Batch {
	err := tc.marg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.barg.Prepare(tc.proc)
	require.NoError(t, err)
	tc.barg.SetChildren([]vm.Operator{tc.marg})
	tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(bat)
	for _, r := range tc.proc.Reg.MergeReceivers {
		r.Ch <- nil
	}
	ok1, err := tc.barg.Call(tc.proc)
	require.NoError(t, err)
	require.Equal(t, false, ok1.Status == vm.ExecStop)
	ok2, err := tc.barg.Call(tc.proc)
	require.NoError(t, err)
	require.Equal(t, false, ok2.Status == vm.ExecStop)
	return []*batch.Batch{ok1.Batch, ok2.Batch}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
*/
/*
func constructIndex(t *testing.T, v *vector.Vector, m *mpool.MPool) {
	idx, err := index.New(*v.GetType(), m)
	require.NoError(t, err)

	err = idx.InsertBatch(v)
	require.NoError(t, err)

	v.SetIndex(idx)
}
*/

func resetChildren(arg *InnerJoin) {
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
