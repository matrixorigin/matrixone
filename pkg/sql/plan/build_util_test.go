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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func Test_replaceFuncId(t *testing.T) {
	case1 := &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{
					ObjName: "current_timestamp",
					Obj:     function.CURRENT_TIMESTAMP,
				},
				Args: []*Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 10,
								Name:   "a",
							},
						},
					},
				},
			},
		},
	}

	err := replaceFuncId(context.Background(), case1)
	assert.NoError(t, err)

	case1ColDef := &plan.ColDef{
		Default: &plan.Default{
			Expr: case1,
		},
	}
	case1Expr, err := getDefaultExpr(context.Background(), case1ColDef)
	assert.NoError(t, err)
	assert.NotNil(t, case1Expr)
}

// TestRewriteCountNotNullColToStarcount ensures plan-level rewrite sets both ObjName and Obj
// so runtime uses countStarExec; regression test for count(not_null_col) performance fix.
func TestRewriteCountNotNullColToStarcount(t *testing.T) {
	wantObj := function.EncodeOverloadID(int32(function.STARCOUNT), 0)

	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "count"},
						Args: []*plan.Expr{
							{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
						},
					},
				},
			},
		},
	}
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{NotNullable: true}},
		},
	}

	RewriteCountNotNullColToStarcount(node, tableDef)

	agg := node.AggList[0].GetF()
	require.NotNil(t, agg)
	require.NotNil(t, agg.Func)
	assert.Equal(t, "starcount", agg.Func.ObjName, "ObjName must be starcount so compile treats as single starcount")
	assert.Equal(t, wantObj, agg.Func.Obj, "Obj must be CountStar overload so runtime uses countStarExec")
}
