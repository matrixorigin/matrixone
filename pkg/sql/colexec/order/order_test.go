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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
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
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []orderTestCase{
		newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}}, []Field{{E: newExpression(0), Type: 0}}),
		newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}}, []Field{{E: newExpression(0), Type: 2}}),
		newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []Field{{E: newExpression(0), Type: 0}, {E: newExpression(1), Type: 0}}),
		newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []Field{{E: newExpression(0), Type: 2}, {E: newExpression(1), Type: 2}}),
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
		_, _ = Call(0, tc.proc, tc.arg)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows)
		_, _ = Call(0, tc.proc, tc.arg)
		if tc.proc.Reg.InputBatch != nil {
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		tc.proc.Reg.InputBatch = &batch.Batch{}
		_, _ = Call(0, tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		_, _ = Call(0, tc.proc, tc.arg)
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		tcs = []orderTestCase{
			newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}}, []Field{{E: newExpression(0), Type: 0}}),
			newTestCase(mheap.New(gm), []types.Type{{Oid: types.T_int8}}, []Field{{E: newExpression(0), Type: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, BenchmarkRows)
			_, _ = Call(0, tc.proc, tc.arg)
			if tc.proc.Reg.InputBatch != nil {
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
			}
			tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, BenchmarkRows)
			_, _ = Call(0, tc.proc, tc.arg)
			if tc.proc.Reg.InputBatch != nil {
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
			}
			tc.proc.Reg.InputBatch = &batch.Batch{}
			_, _ = Call(0, tc.proc, tc.arg)
			tc.proc.Reg.InputBatch = nil
			_, _ = Call(0, tc.proc, tc.arg)
		}
	}
}

func newTestCase(m *mheap.Mheap, ts []types.Type, fs []Field) orderTestCase {
	return orderTestCase{
		types: ts,
		proc:  process.New(m),
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
	return testutil.NewBatch(ts, false, int(rows), proc.Mp)
}
