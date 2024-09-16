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

package mergetop

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type testCase struct {
	ds     []bool // Directions, ds[i] == true: the attrs[i] are in descending order
	arg    *MergeTop
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

func genTestCases() []testCase {
	return []testCase{
		newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]bool{false, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
		newTestCase([]bool{true, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		newTestCase([]bool{true, false}, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}, {Expr: newExpression(1), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range genTestCases() {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range genTestCases() {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestTop(t *testing.T) {
	for _, tc := range genTestCases() {
		tc.proc.Mp().EnableDetailRecording()

		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
			newBatch(tc.types, tc.proc, Rows),
		}
		resetChildren(tc.arg, bats)

		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}

		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
			newBatch(tc.types, tc.proc, Rows),
		}
		resetChildren(tc.arg, bats)

		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}

		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)

		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkTop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []testCase{
			newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 0}}),
			newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, 3, []*plan.OrderBySpec{{Expr: newExpression(0), Flag: 2}}),
		}

		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(b, err)

			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, Rows),
				batch.EmptyBatch,
				newBatch(tc.types, tc.proc, Rows),
			}
			resetChildren(tc.arg, bats)

			for {
				ok, err := tc.arg.Call(tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}

			tc.arg.Free(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)

			tc.proc.Free()
		}
	}
}

func newTestCase(ds []bool, ts []types.Type, limit int64, fs []*plan.OrderBySpec) testCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	_, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 3),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ch2: make(chan process.PipelineSignal, 3),
	}
	return testCase{
		ds:    ds,
		types: ts,
		proc:  proc,
		arg: &MergeTop{
			Fs:    fs,
			Limit: plan2.MakePlan2Uint64ConstExprWithType(uint64(limit)),
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
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
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	}
}

// create a new block based on the type information, ds[i] == true: in descending order
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *MergeTop, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
