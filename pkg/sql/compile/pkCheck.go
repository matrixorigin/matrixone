// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getPKIdx(tableDef *plan.TableDef) (int32, bool) {
	if tableDef.CompositePkey != nil {
		return int32(tableDef.CompositePkey.ColId), true
	} else {
		for i := range tableDef.Cols {
			if tableDef.Cols[i].Primary {
				return int32(i), true
			}
		}
	}
	return 0, false
}

func groupArgument(pkIdx int32, colDef *plan.ColDef) *group.Argument {
	arg := &group.Argument{
		NeedEval: true,
		Exprs: []*plan.Expr{{
			Typ: colDef.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: pkIdx,
					Name:   colDef.Name,
				},
			},
		}},
		Types: []types.Type{vector.ProtoTypeToType(colDef.Typ)},
		Aggs: []agg.Aggregate{{
			Op:   agg.AggregateCount,
			Dist: false,
			E: &plan.Expr{
				Typ: colDef.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: pkIdx,
						Name:   colDef.Name,
					},
				},
			},
		},
		},
	}
	return arg
}

func buildCountCheckExpr(pkCol *plan.ColDef) (*plan.Expr, error) {
	tVarLen := types.T_varchar.ToType()
	pkExpr := &plan.Expr{
		Typ: pkCol.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 0,
				Name:   pkCol.Name,
			},
		},
	}
	pkStrExpr, err := plan2.AppendCastBeforeExpr(context.TODO(), pkExpr, plan2.MakePlan2Type(&tVarLen))
	if err != nil {
		return nil, err
	}
	tInt64 := types.T_int64.ToType()
	countExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&tInt64),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 1,
			},
		},
	}
	colNameExpr := plan2.MakePlan2StringConstExprWithType(pkCol.Name)
	funcExpr, err := plan2.BindFuncExprImplByPlanExpr(context.TODO(), "internal_assert_count_eq_1", []*plan.Expr{countExpr, pkStrExpr, colNameExpr})
	if err != nil {
		return nil, err
	}
	return funcExpr, nil
}

func buildGroupDedupScope(c *Compile, pkIdx int32, pkCol *plan.ColDef) (*Scope, error) {
	groupDedupScope := &Scope{
		Magic:        Merge,
		Instructions: make([]vm.Instruction, 0, 3),
		Proc:         process.NewWithAnalyze(c.proc, c.ctx, 1, c.anal.Nodes()),
	}
	groupDedupScope.appendInstruction(vm.Instruction{
		Op:  vm.Merge,
		Idx: c.anal.curr,
		Arg: &merge.Argument{},
	})

	// select count(pk) from table group by table.pk;
	groupDedupScope.appendInstruction(vm.Instruction{
		Op:  vm.Group,
		Idx: c.anal.curr,
		Arg: groupArgument(pkIdx, pkCol),
	})

	funcExpr, err := buildCountCheckExpr(pkCol)
	if err != nil {
		return nil, err
	}

	// select assert(count(pk) = 1) from table;
	groupDedupScope.appendInstruction(vm.Instruction{
		Op:  vm.Restrict,
		Idx: c.anal.curr,
		Arg: &restrict.Argument{
			E: funcExpr,
		},
	})
	return groupDedupScope, nil
}
