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

package join

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.Argument
}

var (
	tcs []joinTestCase
)

func init() {
	tcs = []joinTestCase{
		newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
		newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range tcs {
		bat := hashBuild(t, tc)
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- bat
		for {
			if ok, err := Call(0, tc.proc, tc.arg); ok || err != nil {
				break
			}
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
		}
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp()))
	}
}

func TestLowCardinalityIndexJoin(t *testing.T) {
	tc := newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{types.T_varchar.ToType()}, []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		[][]*plan.Expr{
			{
				newExpr(0, types.T_varchar.ToType()),
			},
			{
				newExpr(0, types.T_varchar.ToType()),
			},
		})
	tc.arg.Cond = nil // only numeric type can be compared

	err := hashbuild.Prepare(tc.proc, tc.barg)
	require.NoError(t, err)

	values0 := []string{"a", "b", "a", "c", "b", "c", "a", "a"}
	v0 := testutil.NewVector(8, types.T_varchar.ToType(), tc.proc.Mp(), false, values0)
	constructIndex(t, v0, v0.Typ, tc.proc.Mp())

	tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewBatchWithVectors([]*vector.Vector{v0}, nil)
	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	ok, err := hashbuild.Call(0, tc.proc, tc.barg)
	require.NoError(t, err)
	require.Equal(t, true, ok)

	bat := tc.proc.Reg.InputBatch
	err = Prepare(tc.proc, tc.arg)
	require.NoError(t, err)

	values1 := []string{"c", "d", "c", "c", "b", "a", "b", "d", "a", "b"}
	v1 := testutil.NewVector(10, types.T_varchar.ToType(), tc.proc.Mp(), false, values1)

	// only the join column of right table is indexed
	tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewBatchWithVectors([]*vector.Vector{v1}, nil)
	tc.proc.Reg.MergeReceivers[1].Ch <- bat
	ok, err = Call(0, tc.proc, tc.arg)
	require.NoError(t, err)
	require.Equal(t, false, ok)

	result := tc.proc.Reg.InputBatch.Vecs[0]
	t.Log(vector.GetStrVectorValues(result))

	require.NotNil(t, result.Index())
	idx := result.Index().(*index.LowCardinalityIndex)
	require.Equal(
		t,
		[]uint16{3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 2, 2},
		idx.GetPoses().Col.([]uint16),
	)
	t.Log(idx.GetPoses().Col.([]uint16))
}

func TestLowCardinalityIndexesJoin(t *testing.T) {
	tc := newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{types.T_varchar.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{
				newExpr(0, types.T_varchar.ToType()),
			},
			{
				newExpr(0, types.T_varchar.ToType()),
			},
		})
	tc.arg.Cond = nil // only numeric type can be compared

	err := hashbuild.Prepare(tc.proc, tc.barg)
	require.NoError(t, err)

	values0 := []string{"a", "b", "a", "c", "b", "c", "a", "a"}
	v0 := testutil.NewVector(8, types.T_varchar.ToType(), tc.proc.Mp(), false, values0)
	constructIndex(t, v0, v0.Typ, tc.proc.Mp())

	tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewBatchWithVectors([]*vector.Vector{v0}, nil)
	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	ok, err := hashbuild.Call(0, tc.proc, tc.barg)
	require.NoError(t, err)
	require.Equal(t, true, ok)

	bat := tc.proc.Reg.InputBatch
	err = Prepare(tc.proc, tc.arg)
	require.NoError(t, err)

	values1 := []string{"c", "d", "c", "c", "b", "a", "b", "d", "a", "b"}
	v1 := testutil.NewVector(10, types.T_varchar.ToType(), tc.proc.Mp(), false, values1)
	constructIndex(t, v1, v1.Typ, tc.proc.Mp())

	// the join columns of both left table and right table are indexed
	tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewBatchWithVectors([]*vector.Vector{v1}, nil)
	tc.proc.Reg.MergeReceivers[1].Ch <- bat
	ok, err = Call(0, tc.proc, tc.arg)
	require.NoError(t, err)
	require.Equal(t, false, ok)

	result := tc.proc.Reg.InputBatch.Vecs[0]
	t.Log(vector.GetStrVectorValues(result))

	require.NotNil(t, result.Index())
	idx := result.Index().(*index.LowCardinalityIndex)
	require.Equal(
		t,
		[]uint16{1, 1, 1, 1, 1, 1, 3, 3, 4, 4, 4, 4, 3, 3, 4, 4, 4, 4, 3, 3},
		idx.GetPoses().Col.([]uint16),
	)
	t.Log(idx.GetPoses().Col.([]uint16))
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []joinTestCase{
			newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
				[][]*plan.Expr{
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
				}),
			newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
				[][]*plan.Expr{
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			bat := hashBuild(t, tc)
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- bat
			for {
				if ok, err := Call(0, tc.proc, tc.arg); ok || err != nil {
					break
				}
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
			}
		}
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: &plan.Type{
			Size:  typ.Size,
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

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []colexec.ResultPos, cs [][]*plan.Expr) joinTestCase {
	proc := testutil.NewProcessWithMheap(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	fid := function.EncodeOverloadID(function.EQUAL, 4)
	args := make([]*plan.Expr, 0, 2)
	args = append(args, &plan.Expr{
		Typ: &plan.Type{
			Size: ts[0].Size,
			Id:   int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
	})
	args = append(args, &plan.Expr{
		Typ: &plan.Type{
			Size: ts[0].Size,
			Id:   int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})
	cond := &plan.Expr{
		Typ: &plan.Type{
			Size: 1,
			Id:   int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: args,
				Func: &plan.ObjectRef{Obj: fid},
			},
		},
	}
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Typs:       ts,
			Result:     rp,
			Conditions: cs,
			Cond:       cond,
		},
		barg: &hashbuild.Argument{
			Typs:        ts,
			NeedHashMap: true,
			Conditions:  cs[1],
		},
	}
}

func hashBuild(t *testing.T, tc joinTestCase) *batch.Batch {
	err := hashbuild.Prepare(tc.proc, tc.barg)
	require.NoError(t, err)
	tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	ok, err := hashbuild.Call(0, tc.proc, tc.barg)
	require.NoError(t, err)
	require.Equal(t, true, ok)
	return tc.proc.Reg.InputBatch
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func constructIndex(t *testing.T, v *vector.Vector, typ types.Type, m *mheap.Mheap) {
	idx, err := index.NewLowCardinalityIndex(typ, m)
	require.NoError(t, err)

	dict := idx.GetDict()
	ips, err := dict.InsertBatch(v)
	require.NoError(t, err)

	us := make([]uint16, len(ips))
	for i, ip := range ips {
		us[i] = uint16(ip)
	}
	poses := idx.GetPoses()
	err = vector.AppendFixed(poses, us, m)
	require.NoError(t, err)

	v.SetIndex(idx)
}
