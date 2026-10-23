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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func knownInformationSchemaColumns(schema, name, definition string) bool {
	if !strings.EqualFold(schema, "information_schema") || !strings.EqualFold(name, "columns") {
		return false
	}
	if sysview.IsCurrentInformationSchemaColumnsDDL(definition) {
		return true
	}
	_, known := sysview.ViewMetadataColumnsDDL(definition)
	return known
}

// Catalog attnum is a physical identity, not a dense public ordinal when a
// hidden backing column precedes an appended column. Repair only a trusted
// system template's parsed projection, so existing persisted views work without
// rewriting catalog identities or versioned view definitions.
func rewriteInformationSchemaColumnOrdinals(statement tree.SelectStatement) bool {
	switch node := statement.(type) {
	case *tree.Select:
		return rewriteInformationSchemaColumnOrdinals(node.Select)
	case *tree.ParenSelect:
		return rewriteInformationSchemaColumnOrdinals(node.Select)
	case *tree.UnionClause:
		return rewriteInformationSchemaColumnOrdinals(node.Left) && rewriteInformationSchemaColumnOrdinals(node.Right)
	case *tree.SelectClause:
		if len(node.Exprs) < 5 || node.Exprs[4].As == nil ||
			!strings.EqualFold(node.Exprs[4].As.Compare(), "ordinal_position") {
			return false
		}
		schema := ordinalPartitionColumn(node.Exprs[1].Expr)
		table := ordinalPartitionColumn(node.Exprs[2].Expr)
		ordinal, ok := node.Exprs[4].Expr.(*tree.UnresolvedName)
		if schema == nil || table == nil || schema.TblName() != table.TblName() || !ok ||
			ordinal.NumParts != 2 || ordinal.ColName() != "attnum" {
			return false
		}
		if schema.TblName() != ordinal.TblName() {
			// Derived View functions already return dense public ordinals.
			// A window here would block target-View filters above APPLY and
			// could describe unrelated definitions before eliminating them.
			return schema.TblName() == "mt" && schema.ColName() == "reldatabase" &&
				table.ColName() == "relname" && ordinal.TblName() == "mc"
		}
		account := tree.NewUnresolvedName(tree.NewCStr(schema.TblName(), 1), tree.NewCStr("account_id", 1))
		window := &tree.FuncExpr{
			Func:     tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("row_number")),
			FuncName: tree.NewCStr("row_number", 1),
			Type:     tree.FUNC_TYPE_DEFAULT,
			WindowSpec: &tree.WindowSpec{
				PartitionBy: tree.Exprs{account, cloneTreeExpr(schema), cloneTreeExpr(table)},
				OrderBy: tree.OrderBy{
					tree.NewOrder(cloneTreeExpr(ordinal), tree.Ascending, tree.DefaultNullsPosition, false),
				},
				Frame: &tree.FrameClause{
					Type:  tree.Range,
					Start: &tree.FrameBound{Type: tree.Preceding, UnBounded: true},
					End:   &tree.FrameBound{Type: tree.CurrentRow},
				},
			},
		}
		// Keep the public INT type. The original WHERE removes hidden columns
		// before numbering; outer column-name predicates must stay above it.
		node.Exprs[4].Expr = tree.NewCastExpr(window, tree.TYPE_LONG)
		return true
	default:
		return false
	}
}

func ordinalPartitionColumn(expression tree.Expr) *tree.UnresolvedName {
	for {
		if cast, ok := expression.(*tree.CastExpr); ok {
			expression = cast.Expr
			continue
		}
		name, ok := expression.(*tree.UnresolvedName)
		if !ok || name.NumParts != 2 {
			return nil
		}
		return name
	}
}
