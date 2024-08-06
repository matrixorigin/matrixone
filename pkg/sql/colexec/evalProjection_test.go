// Copyright 2024 Matrix Origin
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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type projectionTestCase struct {
	Projection
	proc *process.Process
}

var (
	tcs []projectionTestCase
)

func init() {
	tcs = []projectionTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			Projection: Projection{
				ProjectList: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
						Typ: plan.Type{
							Id: int32(types.T_int32),
						},
					},
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
						Typ: plan.Type{
							Id: int32(types.T_uuid),
						},
					},
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}},
						Typ: plan.Type{
							Id: int32(types.T_varchar),
						},
					},
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 3}},
						Typ: plan.Type{
							Id: int32(types.T_json),
						},
					},
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 4}},
						Typ: plan.Type{
							Id: int32(types.T_datetime),
						},
					},
				},
			},
		},
	}
}

func TestProjection(t *testing.T) {
	for _, tc := range tcs {
		bat := MakeMockBatchs()
		err := tc.PrepareProjection(tc.proc)
		require.NoError(t, err)
		_, err = tc.EvalProjection(bat, tc.proc)
		require.NoError(t, err)
		tc.ResetProjection(tc.proc)

		bat = MakeMockBatchs()
		err = tc.PrepareProjection(tc.proc)
		require.NoError(t, err)
		_, err = tc.EvalProjection(bat, tc.proc)
		require.NoError(t, err)
		tc.ResetProjection(tc.proc)

		tc.FreeProjection(tc.proc)
		tc.proc.Free()

		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}
