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

package mysql

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// 原生特殊调用在 grammar 阶段构造，参数语法即使位于未引用 CTE 也必须成立。
// 仅由原生 keyword production 调用；ID/quoted/IGNORE_SPACE 身份仍归 scanner 所有。
// 边界来自 MySQL 8.4.11 sql_yacc.yy 的 function_call_keyword/nonkeyword/conflict。
func digestNativeFunctionArgs(yylex yyLexer, name string, args tree.Exprs) bool {
	if yylex.(*Lexer).digest == nil {
		return true
	}
	count := len(args)
	valid := true
	switch strings.ToLower(name) {
	case "curdate", "session_user", "system_user", "database", "row_count", "user":
		valid = count == 0
	case "ascii", "charset", "collation", "date", "day", "hour", "microsecond", "minute", "month", "quarter", "reverse", "second", "time", "year":
		valid = count == 1
	case "left", "point", "repeat", "right", "truncate", "adddate", "subdate":
		valid = count == 2
	case "if", "replace":
		valid = count == 3
	case "insert":
		valid = count == 4
	case "format", "substr", "substring", "mid":
		valid = count == 2 || count == 3
	case "timestamp", "week", "log":
		valid = count == 1 || count == 2
	case "coalesce", "linestring", "multilinestring", "multipoint", "multipolygon", "polygon":
		valid = count > 0
	case "weight_string":
		valid = count == 1 || count == 4
		if count == 4 {
			for _, arg := range args[1:] {
				num, ok := arg.(*tree.NumVal)
				if !ok {
					valid = false
					break
				}
				switch num.ValType {
				case tree.P_int64, tree.P_uint64, tree.P_float64, tree.P_decimal, tree.P_hexnum:
				default:
					valid = false
				}
			}
		}
	case "now":
		valid = count == 0 || (count == 1 && digestDatetimePrecision(args[0]))
	}
	if !valid {
		yylex.Error("Invalid native function arguments")
	}
	return valid
}

// 部分 native SYM 在 MO 中是 ID；quoted ID 不属于这些原生 grammar production。
func digestNativeIdentifierArgs(yylex yyLexer, name string, args tree.Exprs, quoted bool) bool {
	if yylex.(*Lexer).digest == nil || quoted {
		return true
	}
	switch digestKeywordUpper(name) {
	case "LOG", "LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "POLYGON", "WEIGHT_STRING":
		return digestNativeFunctionArgs(yylex, name, args)
	default:
		return true
	}
}

// 聚合量词属于 native grammar，不能因所在 CTE 未引用而跳过。
// 仅由原生聚合产生式调用；generic/quoted/空白角色不按函数拼写套规则。
func digestAggregateArgs(yylex yyLexer, name string, quantifier tree.FuncType, count int, window bool) bool {
	if yylex.(*Lexer).digest == nil {
		return true
	}
	valid := true
	switch strings.ToUpper(name) {
	case "COUNT":
		valid = quantifier == tree.FUNC_TYPE_DISTINCT || count == 1
	case "GROUP_CONCAT":
		valid = quantifier != tree.FUNC_TYPE_ALL
	case "BIT_AND", "BIT_OR", "BIT_XOR", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP", "JSON_ARRAYAGG", "JSON_OBJECTAGG":
		valid = quantifier != tree.FUNC_TYPE_DISTINCT
	case "GROUPING":
		valid = quantifier == tree.FUNC_TYPE_DEFAULT && !window
	}
	if !valid {
		yylex.Error("Invalid aggregate arguments or quantifier")
	}
	return valid
}

// 稳定整数中的裸ident是原生SP局部变量，不是当前行的列。
// 无SP上下文的digest仅在该调用实际itemize时拒绝它，unused CTE保留。
func digestWindowCall(yylex yyLexer, name string, args tree.Exprs, window *tree.WindowSpec, stableIndex int) *tree.FuncExpr {
	node := &tree.FuncExpr{
		Func:     tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
		FuncName: tree.NewCStr(name, 1), Exprs: args, WindowSpec: window,
	}
	if stableIndex >= 0 {
		if _, local := args[stableIndex].(*tree.UnresolvedName); local {
			yylex.(*Lexer).digest.rejectExpression(node)
		}
	}
	return node
}

// MySQL func_datetime_precision 要求 NUM（有符号32位非负整数字面量），
// 不是任意 expression，也不是运行时 FSP 0..6 校验；例如 NOW(7) 可以生成 digest。
func digestDatetimePrecision(expr tree.Expr) bool {
	num, ok := expr.(*tree.NumVal)
	if !ok || num.ValType != tree.P_int64 {
		return false
	}
	value, ok := num.Int64()
	return ok && value >= 0 && value <= 1<<31-1
}

// 公共 native factory 的 arity/parity 在 itemization 才检查。
// 只登记当前 parse 的坏节点，由现有引用式 CTE/derived/view walk 检查；
// 合法调用不分配候选表，也不增加遍历。未知/UDF/qualified 调用不套用 MO 签名。
func digestRegisterFunction(yylex yyLexer, expr tree.Expr) {
	if syntax := yylex.(*Lexer).digest; syntax != nil {
		node := expr.(*tree.FuncExpr)
		if !digestBuiltinArgumentCount(digestKeywordUpper(node.FuncName.Origin()), len(node.Exprs)) {
			syntax.rejectExpression(node)
		}
	}
}

func (syntax *statementDigestSyntax) rejectExpression(node *tree.FuncExpr) {
	if syntax.invalidExpressions == nil {
		syntax.invalidExpressions = make(map[*tree.FuncExpr]struct{})
	}
	syntax.invalidExpressions[node] = struct{}{}
}

func (syntax *statementDigestSyntax) checkExpression(expr tree.Expr) string {
	if node, ok := expr.(*tree.FuncExpr); ok {
		if _, invalid := syntax.invalidExpressions[node]; invalid {
			return "Invalid digest expression"
		}
	}
	return ""
}
