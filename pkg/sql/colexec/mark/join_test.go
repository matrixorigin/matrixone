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

package mark

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type markTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
	barg   *hashbuild.Argument
}

var (
	tcs []markTestCase
)

func init() {
	tcs = []markTestCase{
		newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []int32{0},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
		newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []int32{0},
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

func TestThreeValueLogic(t *testing.T) {
	a := condTrue
	b := condFalse
	c := condUnkown

	// three values or
	require.Equal(t, condTrue, threeValueOr(a, a))
	require.Equal(t, condTrue, threeValueOr(a, b))
	require.Equal(t, condTrue, threeValueOr(a, c))

	require.Equal(t, condTrue, threeValueOr(b, a))
	require.Equal(t, condFalse, threeValueOr(b, b))
	require.Equal(t, condUnkown, threeValueOr(b, c))

	require.Equal(t, condTrue, threeValueOr(c, a))
	require.Equal(t, condUnkown, threeValueOr(c, b))
	require.Equal(t, condUnkown, threeValueOr(c, c))

	// three values and
	require.Equal(t, condTrue, threeValueAnd(a, a))
	require.Equal(t, condFalse, threeValueAnd(a, b))
	require.Equal(t, condUnkown, threeValueAnd(a, c))
	require.Equal(t, condFalse, threeValueAnd(b, a))
	require.Equal(t, condFalse, threeValueAnd(b, b))
	require.Equal(t, condFalse, threeValueAnd(b, c))
	require.Equal(t, condUnkown, threeValueAnd(c, a))
	require.Equal(t, condFalse, threeValueAnd(c, b))
	require.Equal(t, condUnkown, threeValueAnd(c, c))
}

func TestEqualJoin(t *testing.T) {
	for _, tc := range tcs {
		tc.arg.ctr.vecs[0].Nsp = nulls.NewWithSize(Rows)
		tc.arg.ctr.vecs[0].Nsp.Np.Add(0)
		tc.arg.ctr.buildEqVec[0].Nsp = nulls.NewWithSize(Rows)
		tc.arg.ctr.buildEqVec[0].Nsp.Np.Add(1)
		for i := 0; i < Rows; i++ {
			tc.arg.ctr.eqJoin(i, i)
		}
	}
}

func TestNonEqJoin(t *testing.T) {
	for _, tc := range tcs {
		pbat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		pbat.Vecs[0].Nsp.Np.Add(0)
		rbat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.arg.ctr.bat = newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		for i := 0; i < Rows; i++ {
			tc.arg.ctr.nonEqJoin(pbat, rbat, i, i, tc.arg, tc.proc)
		}
		pbat.Clean(tc.proc.GetMheap())
		rbat.Clean(tc.proc.GetMheap())
		tc.arg.ctr.bat.Clean(tc.proc.GetMheap())
	}
}

func TestMark(t *testing.T) {
	for _, tc := range tcs {
		freeTestVecs(tc.arg.ctr, tc.proc)
		bat := hashBuild(t, tc)
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		batWithNull := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		batWithNull.Vecs[0].Nsp.Np.Add(0)
		tc.proc.Reg.MergeReceivers[0].Ch <- batWithNull
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
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(0, tc.proc, tc.arg); ok || err != nil {
				break
			}
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func TestHandleResultType(t *testing.T) {
	ctr := new(container)
	ctr.joinFlags = make([]bool, 3)
	ctr.Nsp = nulls.NewWithSize(3)
	ctr.handleResultType(0, condTrue)
	ctr.handleResultType(1, condFalse)
	ctr.handleResultType(2, condUnkown)
}

func BenchmarkMark(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []markTestCase{
			newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []int32{0},
				[][]*plan.Expr{
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
				}),
			newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []int32{0},
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
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
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

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []int32, cs [][]*plan.Expr) markTestCase {
	proc := process.New(m)
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

	ctr := new(container)
	ctr.vecs = make([]*vector.Vector, 2)
	ctr.vecs[0] = testutil.NewVector(Rows, types.T_varchar.ToType(), m, false, nil)
	ctr.vecs[1] = testutil.NewVector(Rows, types.T_int32.ToType(), m, false, nil)

	ctr.evecs = make([]evalVector, 2)
	ctr.evecs[0].vec = testutil.NewVector(Rows, types.T_varchar.ToType(), m, false, nil)
	ctr.evecs[1].vec = testutil.NewVector(Rows, types.T_int32.ToType(), m, false, nil)

	ctr.evecs[0].needFree = true
	ctr.evecs[1].needFree = true

	ctr.buildEqVec = make([]*vector.Vector, 2)
	ctr.buildEqVec[0] = testutil.NewVector(Rows, types.T_varchar.ToType(), m, true, nil)
	ctr.buildEqVec[1] = testutil.NewVector(Rows, types.T_int32.ToType(), m, false, nil)

	ctr.buildEqEvecs = make([]evalVector, 2)
	ctr.buildEqEvecs[0].vec = testutil.NewVector(Rows, types.T_varchar.ToType(), m, true, nil)
	ctr.buildEqEvecs[1].vec = testutil.NewVector(Rows, types.T_int32.ToType(), m, false, nil)

	ctr.buildEqEvecs[0].needFree = true
	ctr.buildEqEvecs[1].needFree = true

	c := markTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Typs:       ts,
			Result:     rp,
			Conditions: cs,
			Cond:       cond,
			ctr:        ctr,
		},
		barg: &hashbuild.Argument{
			Typs:        ts,
			NeedHashMap: true,
			Conditions:  cs[1],
		},
	}
	return c
}

func hashBuild(t *testing.T, tc markTestCase) *batch.Batch {
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
	return testutil.NewBatch(ts, false, int(rows), proc.Mp)
}

func freeTestVecs(ctr *container, proc *process.Process) {
	ctr.vecs[0].Free(proc.GetMheap())
	ctr.vecs[1].Free(proc.GetMheap())
	ctr.freeProbeEqVec(proc)
	ctr.freeBuildEqVec(proc)
	ctr.buildEqVec[0].Free(proc.GetMheap())
	ctr.buildEqVec[1].Free(proc.GetMheap())
}
