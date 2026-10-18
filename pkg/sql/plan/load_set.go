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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// validateLoadSetAssignments checks the entire SET list before InitNullMap
// consumes expressions. Otherwise a later error can leave a partially consumed
// AST whose first nil expression makes InitNullMap skip validation on retry.
// Keep this LOAD-only: external tables also use InitNullMap.
func validateLoadSetAssignments(param *tree.ExternParam, ctx CompilerContext) error {
	if param == nil || param.Tail == nil {
		return nil
	}
	for _, assignment := range param.Tail.Assignments {
		if assignment != nil && assignment.Expr == nil {
			// A nil expression marks an already normalized assignment.
			continue
		}
		unsupported := func() error {
			return moerr.NewNotSupported(ctx.GetContext(),
				"LOAD DATA SET only supports NULLIF(column, literal) or NULLIF(@variable, literal)")
		}
		if assignment == nil {
			return unsupported()
		}
		expr, collated := loadNullIfFunction(assignment.Expr)
		if expr == nil {
			return unsupported()
		}
		switch source := expr.Exprs[0].(type) {
		case *tree.UnresolvedName:
			if collated {
				return moerr.NewNotSupported(ctx.GetContext(), "COLLATE on column NULLIF in LOAD/external SET requires an expression projection")
			}
			if _, ok := expr.Exprs[1].(*tree.NumVal); !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func second param is not NumVal form")
			}
			for _, target := range assignment.Names {
				if target == nil || target.ColName() != source.ColName() {
					return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param must equal to colName")
				}
			}
		case *tree.VarExpr:
			_, literal := expr.Exprs[1].(*tree.NumVal)
			if source.System || !literal {
				return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
			}
			if len(assignment.Names) != 1 || assignment.Names[0] == nil {
				return moerr.NewNotSupported(ctx.GetContext(), "LOAD DATA SET with row variables requires one target column")
			}
		default:
			return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
		}
	}
	return nil
}
