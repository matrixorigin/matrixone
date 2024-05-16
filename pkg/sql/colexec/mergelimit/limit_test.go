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

package mergelimit

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10      // default rows
	BenchmarkRows = 1000000 // default rows for benchmark
)

// add unit tests for cases
type limitTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []limitTestCase
)

func init() {
	tcs = []limitTestCase{
		newTestCase(8),
		newTestCase(10),
		newTestCase(12),
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
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestLimit(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- batch.EmptyBatch
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			ok, err := tc.arg.Call(tc.proc)
			if ok.Status == vm.ExecStop || err != nil {
				break
			}
		}
		// for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
		// 	for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
		// 		bat := <-tc.proc.Reg.MergeReceivers[i].Ch
		// 		if bat != nil {
		// 			bat.Clean(tc.proc.Mp())
		// 		}
		// 	}
		// }
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkLimit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []limitTestCase{
			newTestCase(8),
			newTestCase(10),
			newTestCase(12),
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[0].Ch <- batch.EmptyBatch
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[1].Ch <- batch.EmptyBatch
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				ok, err := tc.arg.Call(tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
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
			tc.arg.Free(tc.proc, false, nil)
			tc.proc.FreeVectors()
		}
	}
}

func newTestCase(limit uint64) limitTestCase {
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
	return limitTestCase{
		proc: proc,
		types: []types.Type{
			types.T_int8.ToType(),
		},
		arg: &Argument{
			Limit: plan.MakePlan2Int64ConstExprWithType(int64(limit)),
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
