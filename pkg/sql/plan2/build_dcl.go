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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildSetVariables(stmt *tree.SetVar, ctx CompilerContext) (*Plan, error) {
	var err error
	items := make([]*plan.SetVariablesItem, len(stmt.Assignments))

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	binder := NewWhereBinder(builder, &BindContext{})

	for idx, assignment := range stmt.Assignments {
		item := &plan.SetVariablesItem{
			System: assignment.System,
			Global: assignment.Global,
			Name:   assignment.Name,
		}
		if assignment.Value == nil {
			return nil, errors.New("", "value is required in SET statement")
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
