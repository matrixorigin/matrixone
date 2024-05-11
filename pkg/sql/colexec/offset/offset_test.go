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

package offset

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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
type offsetTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []offsetTestCase
)

func init() {
	tcs = []offsetTestCase{
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Argument{
				Seen:       0,
				OffsetExpr: plan2.MakePlan2Int64ConstExprWithType(8),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Argument{
				Seen:       0,
				OffsetExpr: plan2.MakePlan2Int64ConstExprWithType(10),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Argument{
				Seen:       0,
				OffsetExpr: plan2.MakePlan2Int64ConstExprWithType(12),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     1,
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
		tc.arg.Free(tc.proc, false, nil)
	}
}

func TestOffset(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)

		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, Rows),
			newBatch(tc.types, tc.proc, Rows),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = tc.arg.Call(tc.proc)

		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.FreeVectors()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkOffset(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []offsetTestCase{
			{
				proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
				types: []types.Type{
					types.T_int8.ToType(),
				},
				arg: &Argument{
					Seen:       0,
					OffsetExpr: plan2.MakePlan2Int64ConstExprWithType(8),
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				},
			},
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			bats := []*batch.Batch{
				newBatch(tc.types, tc.proc, BenchmarkRows),
				batch.EmptyBatch,
			}
			resetChildren(tc.arg, bats)
			_, _ = tc.arg.Call(tc.proc)
			tc.arg.Free(tc.proc, false, nil)
			tc.proc.FreeVectors()
		}
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
