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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []joinTestCase
)

func init() {
	tcs = []joinTestCase{
		newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
		newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_int8}),
				},
			}),
		newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_decimal64}}, []ResultPos{{0, 0}},
			[][]*plan.Expr{
				{
					newExpr(0, types.Type{Oid: types.T_decimal64}),
				},
				{
					newExpr(0, types.Type{Oid: types.T_decimal64, Scale: 1}),
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
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
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

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []joinTestCase{
			newTestCase(testutil.NewMheap(), []bool{false}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
				[][]*plan.Expr{
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
					{
						newExpr(0, types.Type{Oid: types.T_int8}),
					},
				}),
			newTestCase(testutil.NewMheap(), []bool{true}, []types.Type{{Oid: types.T_int8}}, []ResultPos{{0, 0}, {1, 0}},
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
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
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

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []ResultPos, cs [][]*plan.Expr) joinTestCase {
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
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Typs:       ts,
			Result:     rp,
			Conditions: cs,
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp)
}
