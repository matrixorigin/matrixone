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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

func TestString(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	tcs := []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}

	for tci, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newIntBatch(tc.types, tc.proc, Rows, tc.arg.Fs)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newIntBatch(tc.types, tc.proc, Rows, tc.arg.Fs)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
				require.NoError(t, err)
				// do the result check
				if len(tc.arg.Fs) > 0 {
					desc := tc.arg.Fs[0].Flag&plan.OrderBySpec_DESC != 0
					index := tc.arg.Fs[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos
					bat := tc.proc.Reg.InputBatch
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

				if tc.proc.Reg.InputBatch != nil {
					tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
				}
				break
			}
		}
		for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
			for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
				bat := <-tc.proc.Reg.MergeReceivers[i].Ch
				if bat != nil {
					bat.Clean(tc.proc.Mp())
				}
			}
		}
		tc.proc.FreeVectors()
		require.Equal(t, tc.proc.Mp().CurrNB(), int64(0))
	}
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []orderTestCase{
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newRandomBatch(tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newRandomBatch(tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				if ok, err := Call(0, tc.proc, tc.arg, false, false); ok || err != nil {
					if tc.proc.Reg.InputBatch != nil {
						tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
					}
					break
				}
			}
			for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
				for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
					bat := <-tc.proc.Reg.MergeReceivers[i].Ch
					if bat != nil {
						bat.Clean(tc.proc.Mp())
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
		Ch:  make(chan *batch.Batch, 3),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return orderTestCase{
		types: ts,
		proc:  proc,
		arg: &Argument{
			Fs: fs,
		},
		cancel: cancel,
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: &plan.Type{
			Id: int32(types.T_int64),
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
