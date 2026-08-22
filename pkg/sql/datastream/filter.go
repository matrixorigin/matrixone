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

package datastream

import (
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// DeparseFilters renders scan-node filter conjuncts back into MySQL-dialect
// SQL text for pushdown to the datastream server.  It is deliberately
// conservative: a conjunct is only pushed when every part of it deparses to
// unambiguous MySQL (column refs, plain literals, comparisons, AND/OR/NOT,
// IN, BETWEEN, IS [NOT] NULL, LIKE).  Anything else — functions, casts,
// parameters, subqueries — keeps that conjunct local.
//
// cols is the scan's TableDef.Cols; column references are resolved by
// ColPos, never by parsing the display name (which is "table.column"
// qualified at the scan node, and MO permits column names that themselves
// contain dots).  loc is the session time zone used to render timestamp
// literals; when nil, conjuncts containing timestamp literals are not
// pushed.
//
// The returned pushed slice is index-aligned with exprs and reports which
// conjuncts made it into the returned filter text (joined with AND).
func DeparseFilters(exprs []*plan.Expr, cols []*plan.ColDef, loc *time.Location) (filter string, pushed []bool) {
	pushed = make([]bool, len(exprs))
	var parts []string
	for i, expr := range exprs {
		if text, ok := deparseExpr(expr, cols, loc); ok {
			parts = append(parts, text)
			pushed[i] = true
		}
	}
	return strings.Join(parts, " AND "), pushed
}

func deparseExpr(expr *plan.Expr, cols []*plan.ColDef, loc *time.Location) (string, bool) {
	if expr == nil {
		return "", false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return deparseColRef(impl.Col, cols)
	case *plan.Expr_Lit:
		return deparseLiteral(expr, impl.Lit, loc)
	case *plan.Expr_F:
		return deparseFunc(expr.GetF(), cols, loc)
	default:
		return "", false
	}
}

func deparseColRef(col *plan.ColRef, cols []*plan.ColDef) (string, bool) {
	// ColPos indexes the scan's TableDef.Cols (the same convention the
	// external scan's file-level filter uses).  The display Name is not
	// trustworthy for identifier surgery: at the scan node it is
	// "table.column" qualified, while a column may also literally be named
	// "a.b" — resolving by position sidesteps the ambiguity entirely.
	pos := int(col.ColPos)
	if pos < 0 || pos >= len(cols) || cols[pos] == nil || cols[pos].Hidden {
		return "", false
	}
	name := cols[pos].GetOriginCaseName()
	if name == "" {
		return "", false
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`", true
}

func deparseLiteral(expr *plan.Expr, lit *plan.Literal, loc *time.Location) (string, bool) {
	if lit.Isnull {
		return "NULL", true
	}
	switch val := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return strconv.FormatInt(int64(val.I8Val), 10), true
	case *plan.Literal_I16Val:
		return strconv.FormatInt(int64(val.I16Val), 10), true
	case *plan.Literal_I32Val:
		return strconv.FormatInt(int64(val.I32Val), 10), true
	case *plan.Literal_I64Val:
		return strconv.FormatInt(val.I64Val, 10), true
	case *plan.Literal_U8Val:
		return strconv.FormatUint(uint64(val.U8Val), 10), true
	case *plan.Literal_U16Val:
		return strconv.FormatUint(uint64(val.U16Val), 10), true
	case *plan.Literal_U32Val:
		return strconv.FormatUint(uint64(val.U32Val), 10), true
	case *plan.Literal_U64Val:
		return strconv.FormatUint(val.U64Val, 10), true
	case *plan.Literal_Fval:
		return strconv.FormatFloat(float64(val.Fval), 'f', -1, 32), true
	case *plan.Literal_Dval:
		return strconv.FormatFloat(val.Dval, 'f', -1, 64), true
	case *plan.Literal_Bval:
		if val.Bval {
			return "TRUE", true
		}
		return "FALSE", true
	case *plan.Literal_Sval:
		// A backslash has no sql_mode-independent representation in a
		// single-quoted literal (default mode reads '\b' as backspace; a
		// NO_BACKSLASH_ESCAPES source reads it literally), so a string
		// containing one cannot be pushed to a source of unknown mode. Refuse
		// it rather than emit text that parses differently — or breaks out —
		// on the far side.
		if lit.IsBin || !isPrintableText(val.Sval) ||
			strings.ContainsRune(val.Sval, '\\') {
			return "", false
		}
		return quoteStringLiteral(val.Sval), true
	case *plan.Literal_Dateval:
		return "'" + types.Date(val.Dateval).String() + "'", true
	case *plan.Literal_Datetimeval:
		return "'" + types.Datetime(val.Datetimeval).String2(expr.Typ.Scale) + "'", true
	case *plan.Literal_Timeval:
		return "'" + types.Time(val.Timeval).String2(expr.Typ.Scale) + "'", true
	case *plan.Literal_Timestampval:
		if loc == nil {
			return "", false
		}
		return "'" + types.Timestamp(val.Timestampval).String2(loc, expr.Typ.Scale) + "'", true
	case *plan.Literal_Decimal64Val:
		return types.Decimal64(val.Decimal64Val.A).Format(expr.Typ.GetScale()), true
	case *plan.Literal_Decimal128Val:
		return types.Decimal128{
			B0_63:   uint64(val.Decimal128Val.A),
			B64_127: uint64(val.Decimal128Val.B),
		}.Format(expr.Typ.GetScale()), true
	default:
		return "", false
	}
}

func deparseFunc(fn *plan.Function, cols []*plan.ColDef, loc *time.Location) (string, bool) {
	if fn == nil || fn.Func == nil {
		return "", false
	}
	name := fn.Func.GetObjName()
	switch name {
	case "and", "or":
		if len(fn.Args) < 2 {
			return "", false
		}
		op := " AND "
		if name == "or" {
			op = " OR "
		}
		parts := make([]string, 0, len(fn.Args))
		for _, arg := range fn.Args {
			text, ok := deparseExpr(arg, cols, loc)
			if !ok {
				return "", false
			}
			parts = append(parts, text)
		}
		return "(" + strings.Join(parts, op) + ")", true
	case "not":
		if len(fn.Args) != 1 {
			return "", false
		}
		text, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		return "(NOT " + text + ")", true
	case "=", ">", ">=", "<", "<=", "<>", "!=", "like":
		if len(fn.Args) != 2 {
			return "", false
		}
		left, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		right, ok := deparseExpr(fn.Args[1], cols, loc)
		if !ok {
			return "", false
		}
		op := strings.ToUpper(name)
		if op == "!=" {
			op = "<>"
		}
		return "(" + left + " " + op + " " + right + ")", true
	case "in", "not_in":
		if len(fn.Args) != 2 {
			return "", false
		}
		left, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		list, ok := fn.Args[1].Expr.(*plan.Expr_List)
		if !ok || list.List == nil || len(list.List.List) == 0 {
			return "", false
		}
		items := make([]string, 0, len(list.List.List))
		for _, item := range list.List.List {
			text, itemOK := deparseExpr(item, cols, loc)
			if !itemOK {
				return "", false
			}
			items = append(items, text)
		}
		op := " IN ("
		if name == "not_in" {
			op = " NOT IN ("
		}
		return "(" + left + op + strings.Join(items, ", ") + "))", true
	case "between":
		if len(fn.Args) != 3 {
			return "", false
		}
		target, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		low, ok := deparseExpr(fn.Args[1], cols, loc)
		if !ok {
			return "", false
		}
		high, ok := deparseExpr(fn.Args[2], cols, loc)
		if !ok {
			return "", false
		}
		return "(" + target + " BETWEEN " + low + " AND " + high + ")", true
	case "isnull", "is_null":
		if len(fn.Args) != 1 {
			return "", false
		}
		text, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		return "(" + text + " IS NULL)", true
	case "isnotnull", "is_not_null":
		if len(fn.Args) != 1 {
			return "", false
		}
		text, ok := deparseExpr(fn.Args[0], cols, loc)
		if !ok {
			return "", false
		}
		return "(" + text + " IS NOT NULL)", true
	default:
		return "", false
	}
}

func isPrintableText(value string) bool {
	return utf8.ValidString(value) &&
		strings.IndexFunc(value, func(r rune) bool { return !unicode.IsPrint(r) && r != ' ' }) == -1
}

// quoteStringLiteral renders a MySQL string literal using ”-doubling only.
// Doubling is sql_mode-independent: a single-quoted literal with no backslash
// (callers refuse those, see deparseLiteral) and apostrophes doubled parses
// identically whether or not the receiving source runs NO_BACKSLASH_ESCAPES,
// so a crafted predicate value cannot terminate the literal early and inject
// SQL. Backslash escaping would NOT be safe here — a NO_BACKSLASH_ESCAPES
// source reads '\” as a literal backslash followed by a string terminator.
func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
