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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestMark(t *testing.T) {
	/* XXX There are some problems with the mark join code for handling null. Modify later
	   for _, tc := range tcs {
	   	bat := hashBuild(t, tc)
	   	err := tc.arg.Prepare(tc.proc)
	   	require.NoError(t, err)
	   	batWithNull := newBatch( tc.types, tc.proc, Rows)
	   	batWithNull.Vecs[0].GetNulls().Np.Add(0)
	   	tc.proc.Reg.MergeReceivers[0].Ch <- batWithNull
	   	tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
	   	tc.proc.Reg.MergeReceivers[0].Ch <- newBatch( tc.types, tc.proc, Rows)
	   	tc.proc.Reg.MergeReceivers[0].Ch <- newBatch( tc.types, tc.proc, Rows)
	   	tc.proc.Reg.MergeReceivers[0].Ch <- newBatch( tc.types, tc.proc, Rows)
	   	tc.proc.Reg.MergeReceivers[0].Ch <- nil
	   	tc.proc.Reg.MergeReceivers[1].Ch <- bat
	   	for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				cleanResult(&ok, tc.proc)
				break
			}
			cleanResult(&ok, tc.proc)
	   	}
	   	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	   }
	*/
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestHandleResultType(t *testing.T) {
	ctr := new(container)
	ctr.markVals = make([]bool, 3)
	ctr.markNulls = nulls.NewWithSize(3)
	ctr.handleResultType(0, condTrue)
	ctr.handleResultType(1, condFalse)
	ctr.handleResultType(2, condUnkown)
}

func BenchmarkMark(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []markTestCase{
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
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- bats[0]
			tc.proc.Reg.MergeReceivers[1].Ch <- bats[1]
			for {
				ok, err := tc.arg.Call(tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}
		}
	}
}

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

func newTestCase(flgs []bool, ts []types.Type, rp []int32, cs [][]*plan.Expr) markTestCase {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
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
			OnList:     []*plan.Expr{cond},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		barg: &hashbuild.Argument{
			Typs:        ts,
			NeedHashMap: true,
			Conditions:  cs[1],
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
	return c
}

func hashBuild(t *testing.T, tc markTestCase) []*batch.Batch {
	err := tc.barg.Prepare(tc.proc)
	require.NoError(t, err)
	tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
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
	return testutil.NewBatch(ts, true, int(rows), proc.Mp())
}
