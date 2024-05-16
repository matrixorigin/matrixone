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

package hashbuild

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
type buildTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []buildTestCase
)

func init() {
	tcs = []buildTestCase{
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()},
			[]*plan.Expr{
				newExpr(0, types.T_int8.ToType()),
			}),
		newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()},
			[]*plan.Expr{
				newExpr(0, types.T_int8.ToType()),
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestBuild(t *testing.T) {
	for _, tc := range tcs[:1] {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			require.NoError(t, err)
			require.Equal(t, false, ok.Status == vm.ExecStop)
			mp := ok.Batch.AuxData.(*hashmap.JoinMap)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			mp.Free()
			ok.Batch.Clean(tc.proc.Mp())
			break
		}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil

		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			require.NoError(t, err)
			require.Equal(t, false, ok.Status == vm.ExecStop)
			mp := ok.Batch.AuxData.(*hashmap.JoinMap)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			mp.Free()
			ok.Batch.Clean(tc.proc.Mp())
			break
		}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkBuild(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []buildTestCase{
			newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()},
				[]*plan.Expr{
					newExpr(0, types.T_int8.ToType()),
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			for {
				ok, err := tc.arg.Call(tc.proc)
				require.NoError(t, err)
				require.Equal(t, true, ok)
				mp := ok.Batch.AuxData.(*hashmap.JoinMap)
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
				mp.Free()
				ok.Batch.Clean(tc.proc.Mp())
				break
			}
		}
	}
}

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(flgs []bool, ts []types.Type, cs []*plan.Expr) buildTestCase {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	return buildTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Typs:        ts,
			Conditions:  cs,
			NeedHashMap: true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
