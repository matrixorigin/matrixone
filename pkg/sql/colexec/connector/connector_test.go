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

package connector

import (
	"bytes"
	"context"
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
type connectorTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []connectorTestCase
)

func init() {
	tcs = []connectorTestCase{
		newTestCase(),
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

func TestConnector(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		/*{
			for _, vec := range bat.Vecs {
				if vec.IsOriginal() {
					vec.FreeOriginal(tc.proc.Mp())
				}
			}
		}*/
		_, _ = tc.arg.Call(tc.proc)
		for len(tc.arg.Reg.Ch) > 0 {
			bat := <-tc.arg.Reg.Ch
			if bat == nil {
				break
			}
			if bat.IsEmpty() {
				continue
			}
			bat.Clean(tc.proc.Mp())
		}
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)

		tc.arg.Reset(tc.proc, false, nil)

		tc.arg.Reg = &process.WaitRegister{
			Ctx: tc.arg.Reg.Ctx,
			Ch:  make(chan *batch.Batch, 3),
		}
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		/*{
			for _, vec := range bat.Vecs {
				if vec.IsOriginal() {
					vec.FreeOriginal(tc.proc.Mp())
				}
			}
		}*/
		_, _ = tc.arg.Call(tc.proc)
		for len(tc.arg.Reg.Ch) > 0 {
			bat := <-tc.arg.Reg.Ch
			if bat == nil {
				break
			}
			if bat.IsEmpty() {
				continue
			}
			bat.Clean(tc.proc.Mp())
		}
		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func newTestCase() connectorTestCase {
	proc := testutil.NewProcessWithMPool(mpool.MustNewZero())
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	return connectorTestCase{
		proc:  proc,
		types: []types.Type{types.T_int8.ToType()},
		arg: &Argument{
			Reg: &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 3),
			},
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

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func resetChildren(arg *Argument, bats []*batch.Batch) {
	arg.GetOperatorBase().SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: bats,
			},
		},
	)
}
