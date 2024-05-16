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

package output

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type outputTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []outputTestCase
)

func sqlOutput(_ *batch.Batch) error {
	return nil
}

func init() {
	tcs = []outputTestCase{
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Argument{
				Data: nil,
				Func: sqlOutput,
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOutput(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Argument, bats []*batch.Batch) {
	arg.SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: bats,
			},
		})
}
