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

package mergeorder

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
type orderTestCase struct {
	arg    *Argument
	marg   *merge.Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

func TestString(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1, types.T_int64), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}, {Expr: newExpression(1, types.T_int64), Flag: 0}}),
	}
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1, types.T_int64), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}, {Expr: newExpression(1, types.T_int64), Flag: 0}}),
	}
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1, types.T_int64), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}, {Expr: newExpression(1, types.T_int64), Flag: 0}}),
	}

	for tci, tc := range tcs {
		err := tc.marg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.SetChildren([]vm.Operator{tc.marg})
		tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs))
		tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs))
		tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := tc.arg.Call(tc.proc); ok.Status == vm.ExecStop || err != nil {
				require.NoError(t, err)
				// do the result check
				if len(tc.arg.OrderBySpecs) > 0 {
					desc := tc.arg.OrderBySpecs[0].Flag&plan.OrderBySpec_DESC != 0
					index := tc.arg.OrderBySpecs[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos
					bat := ok.Batch
					vec := bat.Vecs[index]
					if vec.GetType().Oid == types.T_int8 {
						i8c := vector.MustFixedCol[int8](vec)
						if desc {
							for j := range i8c {
								if j > 0 {
									require.True(t, i8c[j] <= i8c[j-1], fmt.Sprintf("tc %d require desc, but get %v", tci, i8c))
								}
							}
						} else {
							for j := range i8c {
								if j > 0 {
									require.True(t, i8c[j] >= i8c[j-1])
								}
							}
						}
					} else if vec.GetType().Oid == types.T_int64 {
						i64c := vector.MustFixedCol[int64](vec)
						if desc {
							for j := range i64c {
								if j > 0 {
									require.True(t, i64c[j] <= i64c[j-1])
								}
							}
						} else {
							for j := range i64c {
								if j > 0 {
									require.True(t, i64c[j] >= i64c[j-1])
								}
							}
						}
					}
				}

				break
			}
		}
		for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
			for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
				msg := <-tc.proc.Reg.MergeReceivers[i].Ch
				if msg.Batch != nil {
					msg.Batch.Clean(tc.proc.Mp())
				}
			}
		}
		tc.proc.FreeVectors()
		tc.arg.Free(tc.proc, false, nil)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []orderTestCase{
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newRandomBatch(tc.types, tc.proc, BenchmarkRows))
			tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(newRandomBatch(tc.types, tc.proc, BenchmarkRows))
			tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				ok, err := tc.arg.Call(tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}
			for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
				for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
					msg := <-tc.proc.Reg.MergeReceivers[i].Ch
					if msg.Batch != nil {
						msg.Batch.Clean(tc.proc.Mp())
					}
				}
			}
		}
	}
}

func newTestCase(ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *process.RegisterMessage, 3),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *process.RegisterMessage, 3),
	}
	return orderTestCase{
		types: ts,
		proc:  proc,
		arg: &Argument{
			OrderBySpecs: fs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		cancel: cancel,
		marg:   &merge.Argument{},
	}
}

func newExpression(pos int32, typeID types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(typeID),
		},
	}
}

func newIntBatch(ts []types.Type, proc *process.Process, rows int64, fs []*plan.OrderBySpec) *batch.Batch {
	vs := make([]*vector.Vector, len(ts))
	for i, t := range ts {
		generateDescData := false
		for _, f := range fs {
			if f.Flag == plan.OrderBySpec_DESC {
				index := f.Expr.Expr.(*plan.Expr_Col).Col.ColPos
				if int(index) == i {
					generateDescData = true
				}
			}
		}

		if t.Oid == types.T_int8 {
			values := make([]int8, rows)
			if generateDescData {
				for j := range values {
					values[j] = int8(-j)
				}
			} else {
				for j := range values {
					values[j] = int8(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		} else if t.Oid == types.T_int64 {
			values := make([]int64, rows)
			if generateDescData {
				for j := range values {
					values[j] = int64(-j)
				}
			} else {
				for j := range values {
					values[j] = int64(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		}
	}
	zs := make([]int64, rows)
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors(vs, zs)
}

func newRandomBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
