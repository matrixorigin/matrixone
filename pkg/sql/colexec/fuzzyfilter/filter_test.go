// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fuzzyTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	rowCnts []float64

	referM []float64

	tcs []fuzzyTestCase
)

func init() {
	rowCnts = []float64{1000000, 10000000}

	// https://hur.st/bloomfilter/?n=100000&p=0.00001&m=&k=3
	referM = []float64{
		68871111,
		137742221,
	}

	tcs = []fuzzyTestCase{
		{
			arg:  newArgument(types.T_int32.ToType()),
			proc: newProcess(),
			types: []types.Type{
				types.T_int32.ToType(),
			},
		},
		{
			arg:  newArgument(types.T_date.ToType()),
			proc: newProcess(),
			types: []types.Type{
				types.T_date.ToType(),
			},
		},
		{
			arg:  newArgument(types.T_float32.ToType()),
			proc: newProcess(),
			types: []types.Type{
				types.T_float32.ToType(),
			},
		},
		{
			arg:  newArgument(types.T_varchar.ToType()),
			proc: newProcess(),
			types: []types.Type{
				types.T_varchar.ToType(),
			},
		},
		{
			arg:  newArgument(types.T_binary.ToType()),
			proc: newProcess(),
			types: []types.Type{
				types.T_binary.ToType(),
			},
		},
	}
}

func newArgument(typ types.Type) *Argument {
	arg := new(Argument)
	arg.PkTyp = plan.MakePlan2Type(&typ)
	return arg
}

func newProcess() *process.Process {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: proc.Ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: proc.Ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return proc
}

func TestString(t *testing.T) {
	for _, tc := range tcs {
		buf := new(bytes.Buffer)
		tc.arg.String(buf)
		require.Equal(t, "fuzzy_filter: fuzzy check duplicate constraint", buf.String())
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestFuzzyFilter(t *testing.T) {
	for _, tc := range tcs {
		for _, r := range rowCnts {
			tc.arg.N = r
			tc.arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)

			bat := newBatch(tc.types, tc.proc, int64(r))
			tc.proc.Reg.MergeReceivers[0].Ch <- bat
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- nil

			resetChildren(tc.arg, []*batch.Batch{bat})
			for {
				result, err := tc.arg.Call(tc.proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop {
					tc.arg.Reset(tc.proc, false, err)
					tc.arg.GetChildren(0).Free(tc.proc, false, err)
					break
				}
			}

			err = tc.arg.Prepare(tc.proc)
			require.NoError(t, err)

			bat = newBatch(tc.types, tc.proc, int64(r))
			tc.proc.Reg.MergeReceivers[0].Ch <- bat
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- nil

			resetChildren(tc.arg, []*batch.Batch{bat})
			for {
				result, err := tc.arg.Call(tc.proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop {
					tc.arg.Free(tc.proc, false, err)
					tc.arg.GetChildren(0).Free(tc.proc, false, err)
					require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
					break
				}
			}
		}
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	// not random
	bat := testutil.NewBatch(ts, false, int(rows), proc.Mp())
	pkAttr := make([]string, 1)
	pkAttr[0] = "pkCol"
	bat.SetAttributes(pkAttr)
	return bat
}

func resetChildren(arg *Argument, bats []*batch.Batch) {
	arg.SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: bats,
			},
		})
}
