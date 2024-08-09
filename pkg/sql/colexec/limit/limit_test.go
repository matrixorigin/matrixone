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

package limit

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10      // default rows
	BenchmarkRows = 1000000 // default rows for benchmark
)

// add unit tests for cases
type limitTestCase struct {
	arg         *Limit
	proc        *process.Process
	getRowCount int
}

var (
	tcs []limitTestCase
)

func init() {
	tcs = []limitTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Limit{
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(0),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 0,
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Limit{
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(1),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 1,
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Limit{
				ctr: container{
					seen: 0,
				},
				LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(5),
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
			getRowCount: 2, //if colexec.MakeMockBatchs return more rows, you need to change it
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

func TestLimit(t *testing.T) {
	for _, tc := range tcs {
		resetChildren(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ := tc.arg.Call(tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}
		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		res, _ = tc.arg.Call(tc.proc)
		if tc.getRowCount > 0 {
			require.Equal(t, res.Batch.RowCount(), tc.getRowCount)
		} else {
			require.Equal(t, res.Batch == nil, true)
		}

		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func BenchmarkLimit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs = []limitTestCase{
			{
				proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
				arg: &Limit{
					LimitExpr: plan2.MakePlan2Uint64ConstExprWithType(8),
				},
			},
		}

		t := new(testing.T)
		for _, tc := range tcs {
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			resetChildren(tc.arg)
			_, _ = tc.arg.Call(tc.proc)
			tc.arg.Free(tc.proc, false, nil)
		}
	}
}

func resetChildren(arg *Limit) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
