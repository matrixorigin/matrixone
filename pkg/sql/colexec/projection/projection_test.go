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

package projection

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type projectionTestCase struct {
	arg   *Projection
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []projectionTestCase {
	return []projectionTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Projection{
				ProjectList: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
						Typ: plan.Type{
							Id: int32(types.T_int8),
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
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestProjection(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		nb0 := tc.proc.Mp().CurrNB()
		op := resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)
		op.Free(tc.proc, false, nil)

		op = resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		op.Free(tc.proc, false, nil)
		tc.proc.Free()
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
	}
}

func TestProjectionMaterializesRawBinaryLiteralMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &Projection{ProjectList: []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_varbinary), Width: 1},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			IsBin: true,
			Value: &plan.Literal_Sval{Sval: "1"},
		}},
	}}}
	child := resetChildren(arg, proc.Mp())
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Len(t, result.Batch.Vecs, 1)
	require.False(t, result.Batch.Vecs[0].GetIsBin())
	require.False(t, result.Batch.Vecs[0].GetIsBinaryString())
	require.Equal(t, types.T_varbinary, result.Batch.Vecs[0].GetType().Oid)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
}

func TestMaterializeBinaryStringVectorUsesStaticBinaryType(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec, err := vector.NewConstBytes(types.T_text.ToType(), []byte("你好"), 1, proc.Mp())
	require.NoError(t, err)
	defer vec.Free(proc.Mp())
	vec.SetIsBinaryString(true)

	materializeBinaryStringVector(vec)

	require.Equal(t, types.T_varbinary, vec.GetType().Oid)
	require.Equal(t, int32(len("你好")), vec.GetType().Width)
	require.False(t, vec.GetIsBin())
	require.False(t, vec.GetIsBinaryString())
}

func resetChildren(arg *Projection, m *mpool.MPool) *colexec.MockOperator {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
	return op
}
