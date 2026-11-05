// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestNormalizeInternalPreparePlan(t *testing.T) {
	for _, ddl := range []bool{false, true} {
		name := "query preserves gaps"
		positions := []int32{2, 4}
		if ddl {
			name = "ddl retains existing handling"
			positions = []int32{1, 2}
		}
		t.Run(name, func(t *testing.T) {
			ctx := plan2.NewEmptyCompilerContext()
			ctx.SetContext(context.Background())
			projection := make([]*plan.Expr, len(positions))
			for i, pos := range positions {
				projection[i] = &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
				}
			}
			query := &plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{{ProjectList: projection}}}
			pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
			if ddl {
				pn = &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
					DdlType: plan.DataDefinition_CREATE_TABLE,
					Query:   query,
				}}}
			}
			require.NoError(t, normalizeInternalPreparePlan(ctx, pn))
			for i, expr := range projection {
				require.Equal(t, positions[i]-1, expr.GetP().Pos)
			}
		})
	}
	t.Run("unsupported dcl still rejected", func(t *testing.T) {
		ctx := plan2.NewEmptyCompilerContext()
		ctx.SetContext(context.Background())
		pn := &plan.Plan{Plan: &plan.Plan_Dcl{Dcl: &plan.DataControl{DclType: plan.DataControl_PREPARE}}}
		require.ErrorContains(t, normalizeInternalPreparePlan(ctx, pn), "cannot prepare TCL and DCL")
	})
}
