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

package fill

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
type fillTestCase struct {
	arg  *Fill
	proc *process.Process
}

var (
	tcs []fillTestCase
)

func init() {
	tcs = []fillTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_VALUE,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_PREV,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_NONE,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
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

func TestFill(t *testing.T) {
	for _, tc := range tcs {
		resetChildren(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *Fill) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
