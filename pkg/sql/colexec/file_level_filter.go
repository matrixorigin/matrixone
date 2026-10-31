// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// FileLevelFilterClassifier preserves the planner and executor eligibility rules.
type FileLevelFilterClassifier bool

const (
	FileLevelPlanner   FileLevelFilterClassifier = false
	FileLevelExecution FileLevelFilterClassifier = true
)

func (c FileLevelFilterClassifier) IsFileLevelColumn(node *plan.Node, col *plan.ColRef) bool {
	if node == nil || node.TableDef == nil || node.ExternScan == nil || col == nil {
		return false
	}

	colPos := int(col.ColPos)
	if colPos < 0 || colPos >= len(node.TableDef.Cols) || colPos != len(node.TableDef.Cols)-1 {
		return false
	}
	// Two hidden trailing columns exist: __mo_filepath on ordinary external
	// tables and __mo_query on ESQL/SQL foreign tables. Either one is the
	// "file-level" column of its scan (the query text plays the file-name
	// role for foreign tables).
	switch node.TableDef.Cols[colPos].Name {
	case catalog.ExternalFilePath:
		return node.TableDef.Cols[colPos].ColId == catalog.ExternalFilePathColId
	case catalog.ExternalQuery:
		return c == FileLevelExecution && node.TableDef.Cols[colPos].ColId == catalog.ExternalQueryColId
	}
	return false
}

func (c FileLevelFilterClassifier) IsSafeFileLevelFunction(ref *plan.ObjectRef) bool {
	if ref == nil {
		return false
	}
	overload, exists := function.GetFunctionByIdWithoutError(ref.Obj)
	if !exists {
		return false
	}
	// last_kafka_message_id is realTimeRelated (never constant-folded into a
	// cached plan) but is deterministic WITHIN one compile on the session CN —
	// it reads session state. Allowing it here is what makes server-side
	// gap-free chaining work:
	//   where __mo_read_start_id = last_kafka_message_id()
	functionID, _ := function.DecodeOverloadID(ref.Obj)
	if c == FileLevelExecution && functionID == function.LAST_KAFKA_MESSAGE_ID {
		return true
	}
	if overload.IsRealTimeRelated() {
		return false
	}
	if !overload.CannotFold() {
		return true
	}

	// mo_log_date is marked volatile to prevent ordinary constant folding, but
	// it is a deterministic transform of __mo_filepath and is the established
	// file-pruning primitive. Other volatile functions (for example rand) are
	// row-dependent and must remain at row level.
	return functionID == function.MO_LOG_DATE
}

func (c FileLevelFilterClassifier) ClassifyFileLevelColumns(node *plan.Node, expr *plan.Expr) (hasFileLevelColumn, hasUnsupportedColumn bool) {
	if expr == nil {
		return false, false
	}

	switch typedExpr := expr.Expr.(type) {
	case *plan.Expr_Col:
		if typedExpr.Col == nil {
			return false, true
		}
		if c.IsFileLevelColumn(node, typedExpr.Col) {
			return true, false
		}
		return false, true
	case *plan.Expr_F:
		if typedExpr.F == nil || typedExpr.F.Func == nil {
			return false, true
		}
		if !c.IsSafeFileLevelFunction(typedExpr.F.Func) {
			return false, true
		}
		for _, arg := range typedExpr.F.Args {
			hasFileLevel, hasUnsupported := c.ClassifyFileLevelColumns(node, arg)
			hasFileLevelColumn = hasFileLevelColumn || hasFileLevel
			hasUnsupportedColumn = hasUnsupportedColumn || hasUnsupported
		}
		return hasFileLevelColumn, hasUnsupportedColumn
	case *plan.Expr_List:
		if typedExpr.List == nil {
			return false, false
		}
		for _, item := range typedExpr.List.List {
			hasFileLevel, hasUnsupported := c.ClassifyFileLevelColumns(node, item)
			hasFileLevelColumn = hasFileLevelColumn || hasFileLevel
			hasUnsupportedColumn = hasUnsupportedColumn || hasUnsupported
		}
		return hasFileLevelColumn, hasUnsupportedColumn
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_T,
		*plan.Expr_Max, *plan.Expr_Vec, *plan.Expr_Fold:
		return false, false
	default:
		return false, true
	}
}

func (c FileLevelFilterClassifier) IsFileLevelFilter(node *plan.Node, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}

	functionExpr, ok := expr.Expr.(*plan.Expr_F)
	if !ok || functionExpr.F == nil || functionExpr.F.Func == nil {
		return false
	}
	if !c.IsSafeFileLevelFunction(functionExpr.F.Func) {
		return false
	}
	if functionExpr.F.Func.ObjName == "or" {
		if len(functionExpr.F.Args) == 0 {
			return false
		}
		for _, arg := range functionExpr.F.Args {
			if !c.IsFileLevelFilter(node, arg) {
				return false
			}
		}
		return true
	}

	hasFileLevelColumn, hasUnsupportedColumn := c.ClassifyFileLevelColumns(node, expr)
	return hasFileLevelColumn && !hasUnsupportedColumn
}
