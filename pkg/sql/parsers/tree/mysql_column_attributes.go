// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import "iter"

// MySQLColumnDefaultExpression separates a trailing column COLLATE from its
// default. Parenthesized collations remain part of the expression.
func MySQLColumnDefaultExpression(expr Expr) (Expr, string) {
	if collated, ok := expr.(*CollateExpr); ok {
		return collated.Expr, collated.Collation
	}
	if unary, ok := expr.(*UnaryExpr); ok {
		if collated, ok := unary.Expr.(*CollateExpr); ok {
			copy := *unary
			copy.Expr = collated.Expr
			return &copy, collated.Collation
		}
	}
	return expr, ""
}

// MySQLEffectiveColumnAttributes mirrors PT_field_def_base's last-per-class
// selection before itemization. It borrows the original attributes and preserves
// their indices and relative order; ordinary MO binding does not use this policy.
func MySQLEffectiveColumnAttributes(attrs []ColumnAttribute) iter.Seq2[int, ColumnAttribute] {
	return func(yield func(int, ColumnAttribute) bool) {
		var last [16]int
		for i, attr := range attrs {
			last[mysqlColumnAttributeClass(attr)] = i + 1
		}
		for i, attr := range attrs {
			class := mysqlColumnAttributeClass(attr)
			if class == 0 || last[class] == i+1 {
				if !yield(i, attr) {
					return
				}
			}
		}
	}
}

func mysqlColumnAttributeClass(attr ColumnAttribute) int {
	switch a := attr.(type) {
	case *AttributeDefault:
		expr, _ := MySQLColumnDefaultExpression(a.Expr)
		switch expr.(type) {
		case *ParenExpr, *Subquery:
			// A grouped SELECT owns its parentheses in ParenSelect rather
			// than ParenExpr. The parser separately checks DEFAULT syntax.
			return 2
		default:
			return 1
		}
	case *AttributeNull:
		if a.Is {
			return 3
		}
		return 4
	case *AttributePrimaryKey, *AttributeKey:
		return 5
	case *AttributeUnique, *AttributeUniqueKey:
		return 6
	case *AttributeAutoIncrement:
		return 7
	case *AttributeComment:
		return 8
	case *AttributeOnUpdate:
		return 9
	case *AttributeColumnFormat:
		return 10
	case *AttributeStorage:
		return 11
	case *AttributeSRID:
		return 12
	case *AttributeVisable:
		return 13
	default:
		// COLLATE and additive constraints are deliberately not deduplicated.
		return 0
	}
}
