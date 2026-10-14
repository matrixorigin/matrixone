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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalIntegerDivSchemaBinding(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		oid         types.T
	}{
		{"folded_narrow", "select cast(5.678 as decimal(10,3)) div cast(2 as signed) as q", types.T_int32},
		{"wide", "select cast(5.678 as decimal(18,3)) div cast(2 as signed) as q", types.T_int64},
		{"unsigned", "select cast(5.678 as decimal(10,3)) div cast(2 as unsigned) as q", types.T_uint32},
		{"derived", "select q from (select cast(n_nationkey as decimal(10,3)) div 2 as q from nation) s", types.T_int32},
		{"unrelated_bigint", "select cast(2 as signed) as q", types.T_int64},
		{"semantic_wrapper", "select cast(n_nationkey as decimal(10,3)) div 2 + 1 as q from nation", types.T_int64},
	} {
		for _, kind := range []string{"table", "view"} {
			t.Run(kind+"_"+tc.name, func(t *testing.T) {
				p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "create "+kind+" decimal_div_test as "+tc.query)
				require.NoError(t, err)
				var cols []*ColDef
				if kind == "table" {
					cols = p.GetDdl().GetCreateTable().TableDef.Cols
				} else {
					cols = p.GetDdl().GetCreateView().TableDef.Cols
				}
				require.NotEmpty(t, cols)
				require.Equal(t, int32(tc.oid), cols[0].Typ.Id)
			})
		}
	}
}
func TestDecimalIntegerDivPersistedFloor(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "prepare decimal_div_floor from 'select cast(? as decimal(10,3)) div cast(2 as unsigned)'")
	require.NoError(t, err)
	expr := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "div")
	require.NotNil(t, expr)
	floor, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion121, floor)
}
