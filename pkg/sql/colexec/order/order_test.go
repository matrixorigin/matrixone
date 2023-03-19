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

package order

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []orderTestCase
)

func init() {
	tcs = []orderTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}, {Expr: newExpression(1), Flag: 0}}),
		newTestCase([]types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 2}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows)
		_, _ = Call(0, tc.proc, tc.arg, false, false)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
		}
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows)
		_, _ = Call(0, tc.proc, tc.arg, false, false)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
		}
		tc.proc.Reg.InputBatch = &batch.Batch{}
		_, _ = Call(0, tc.proc, tc.arg, false, false)
		tc.proc.Reg.InputBatch = nil
		_, _ = Call(0, tc.proc, tc.arg, false, false)
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []orderTestCase{
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase([]types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, BenchmarkRows)
			_, _ = Call(0, tc.proc, tc.arg, false, false)
			if tc.proc.Reg.InputBatch != nil {
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
			}
			tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, BenchmarkRows)
			_, _ = Call(0, tc.proc, tc.arg, false, false)
			if tc.proc.Reg.InputBatch != nil {
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
			}
			tc.proc.Reg.InputBatch = &batch.Batch{}
			_, _ = Call(0, tc.proc, tc.arg, false, false)
			tc.proc.Reg.InputBatch = nil
			_, _ = Call(0, tc.proc, tc.arg, false, false)
		}
	}
}

func newTestCase(ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	return orderTestCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool(mpool.MustNewZero()),
		arg: &Argument{
			Fs: fs,
		},
	}
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
