// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func getPreparePlan(ctx CompilerContext, stmt tree.Statement) (*Plan, error) {
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			return BuildPlan(ctx, stmt, true)
		}
	}

	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable:
		opt := NewPrepareOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, true)
		if err != nil {
			return nil, err
		}
		return &Plan{
			Plan: &Plan_Query{
				Query: optimized,
			},
		}, nil
	default:
		return BuildPlan(ctx, stmt, true)
	}
}

func buildPrepare(stmt tree.Prepare, ctx CompilerContext) (*Plan, error) {
	var preparePlan *Plan
	var err error
	var stmtName string

	switch pstmt := stmt.(type) {
	case *tree.PrepareStmt:
		stmtName = string(pstmt.Name)
		preparePlan, err = getPreparePlan(ctx, pstmt.Stmt)
		if err != nil {
			return nil, err
		}
		preparePlan.IsPrepare = true

	case *tree.PrepareString:
		var v interface{}
		v, err = ctx.ResolveVariable("lower_case_table_names", true, true)
		if err != nil {
			v = int64(1)
		}
		stmts, err := mysql.Parse(ctx.GetContext(), pstmt.Sql, v.(int64))
		if err != nil {
			return nil, err
		}
		if len(stmts) > 1 {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare multi statements")
		}
		stmtName = string(pstmt.Name)
		preparePlan, err = getPreparePlan(ctx, stmts[0])
		if err != nil {
			return nil, err
		}
		preparePlan.IsPrepare = true
	}

	// dcl tcl is not support
	var schemas []*plan.ObjectRef
	var paramTypes []int32

	switch pp := preparePlan.Plan.(type) {
	case *plan.Plan_Tcl, *plan.Plan_Dcl:
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare TCL and DCL statement")

	case *plan.Plan_Ddl:
		if pp.Ddl.Query != nil {
			getParamRule := NewGetParamRule()
			VisitQuery := NewVisitPlan(preparePlan, []VisitPlanRule{getParamRule})
			err = VisitQuery.Visit(ctx.GetContext())
			if err != nil {
				return nil, err
			}
			// TODO : need confirm
			if len(getParamRule.params) > 0 {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot plan DDL statement")
			}
		}

	case *plan.Plan_Query:
		// collect args
		getParamRule := NewGetParamRule()
		VisitQuery := NewVisitPlan(preparePlan, []VisitPlanRule{getParamRule})
		err = VisitQuery.Visit(ctx.GetContext())
		if err != nil {
			return nil, err
		}

		// sort arg
		getParamRule.SetParamOrder()
		args := getParamRule.params
		schemas = getParamRule.schemas
		paramTypes = getParamRule.paramTypes

		// reset arg order
		resetParamRule := NewResetParamOrderRule(args)
		VisitQuery = NewVisitPlan(preparePlan, []VisitPlanRule{resetParamRule})
		err = VisitQuery.Visit(ctx.GetContext())
		if err != nil {
			return nil, err
		}
	}

	prepare := &plan.Prepare{
		Name:       stmtName,
		Schemas:    schemas,
		Plan:       preparePlan,
		ParamTypes: paramTypes,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_PREPARE,
				Control: &plan.DataControl_Prepare{
					Prepare: prepare,
				},
			},
		},
	}, nil
}

func buildExecute(stmt *tree.Execute, ctx CompilerContext) (*Plan, error) {
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false)
	binder := NewWhereBinder(builder, &BindContext{})

	args := make([]*Expr, len(stmt.Variables))
	for idx, variable := range stmt.Variables {
		arg, err := binder.baseBindExpr(variable, 0, true)
		if err != nil {
			return nil, err
		}
		args[idx] = arg
	}

	execute := &plan.Execute{
		Name: string(stmt.Name),
		Args: args,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_EXECUTE,
				Control: &plan.DataControl_Execute{
					Execute: execute,
				},
			},
		},
	}, nil
}

func buildDeallocate(stmt *tree.Deallocate, _ CompilerContext) (*Plan, error) {
	deallocate := &plan.Deallocate{
		Name: string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_DEALLOCATE,
				Control: &plan.DataControl_Deallocate{
					Deallocate: deallocate,
				},
			},
		},
	}, nil
}

func buildSetVariables(stmt *tree.SetVar, ctx CompilerContext) (*Plan, error) {
	var err error
	items := make([]*plan.SetVariablesItem, len(stmt.Assignments))

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false)
	binder := NewWhereBinder(builder, &BindContext{})

	for idx, assignment := range stmt.Assignments {
		item := &plan.SetVariablesItem{
			System: assignment.System,
			Global: assignment.Global,
			Name:   assignment.Name,
		}
		if assignment.Value == nil {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Set statement has no value")
		}
		item.Value, err = binder.baseBindExpr(assignment.Value, 0, true)
		if err != nil {
			return nil, err
		}
		if assignment.Reserved != nil {
			item.Reserved, err = binder.baseBindExpr(assignment.Reserved, 0, true)
			if err != nil {
				return nil, err
			}
		}
		items[idx] = item
	}

	setVariables := &plan.SetVariables{
		Items: items,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_SET_VARIABLES,
				Control: &plan.DataControl_SetVariables{
					SetVariables: setVariables,
				},
			},
		},
	}, nil
}
