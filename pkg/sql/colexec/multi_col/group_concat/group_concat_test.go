// Copyright 2022 Matrix Origin
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
package group_concat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type GroupConcatTestCase struct {
	proc   *process.Process
	ityp   []types.Type
	arg    *Argument
	vecs   []*vector.Vector
	expect string
}

func NewTestCases(proc *process.Process) []GroupConcatTestCase {
	return []GroupConcatTestCase{
		{
			// int16 varchar int64
			//   1      a     2
			//   3      b     4		--> 1a2,3b4,1a2
			//   1      a     2
			// dist is false
			proc: proc,
			ityp: []types.Type{types.T_int16.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()},
			arg:  &Argument{Dist: false, Separator: ",", GroupExpr: []*plan.Expr{{}, {}, {}}, OrderByExpr: nil},
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int16.ToType(), proc.Mp(), false, []int16{1, 3, 1}),
				testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"a", "b", "a"}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{2, 4, 2}),
			},
			expect: "1a2,3b4,1a2",
		},
		{
			// int16 varchar int64
			//   1      a     2
			//   3      b     4		--> 1a2,3b4
			//   1      a     2
			// dist is true
			proc: proc,
			ityp: []types.Type{types.T_int16.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()},
			arg:  &Argument{Dist: true, Separator: ",", GroupExpr: []*plan.Expr{{}, {}, {}}, OrderByExpr: nil},
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int16.ToType(), proc.Mp(), false, []int16{1, 3, 1}),
				testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"a", "b", "a"}),
				testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{2, 4, 2}),
			},
			expect: "1a2,3b4",
		},
		{
			// int16 varchar varchar
			//   1      ab    c
			//   3      b     d		--> 1abc,3bd,1abc
			//   1      a     bc
			// dist is true
			proc: proc,
			ityp: []types.Type{types.T_int16.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
			arg:  &Argument{Dist: true, Separator: ",", GroupExpr: []*plan.Expr{{}, {}, {}}, OrderByExpr: nil},
			vecs: []*vector.Vector{
				testutil.NewVector(3, types.T_int16.ToType(), proc.Mp(), false, []int16{1, 3, 1}),
				testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"ab", "b", "a"}),
				testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"c", "d", "bc"}),
			},
			expect: "1abc,3bd,1abc",
		},
	}
}

func TestGroupConcat(t *testing.T) {
	proc := testutil.NewProcess()
	cases := NewTestCases(proc)
	RunTestCases(cases, t, proc)
}

func RunTestCases(cases []GroupConcatTestCase, t *testing.T, proc *process.Process) {
	for _, case_ := range cases {
		gc := NewGroupConcat(case_.arg, case_.ityp)
		gc.Grows(1, case_.proc.Mp())
		gc.Fill(0, 0, case_.vecs)
		gc.Fill(0, 1, case_.vecs)
		gc.Fill(0, 2, case_.vecs)
		res, _ := gc.Eval(case_.proc.Mp())
		require.Equal(t, case_.expect, res.GetStringAt(0))
		gc.Free(case_.proc.Mp())
		for i := 0; i < len(case_.vecs); i++ {
			case_.vecs[i].Free(case_.proc.Mp())
		}
		res.Free(case_.proc.Mp())
	}
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
