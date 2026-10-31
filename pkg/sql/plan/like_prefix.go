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
	"context"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	analyzestats "github.com/matrixorigin/matrixone/pkg/statistics/analyze"
)

// likeLiteralPrefix follows two-argument LIKE's default escape semantics.
// The complete LIKE remains the truth oracle, including a terminal escape.
func likeLiteralPrefix(pattern string) (prefix string, wildcard, escaped, pure bool) {
	var out strings.Builder
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '\\' {
			escaped = true
			if i+1 < len(pattern) {
				i++
				c = pattern[i]
			}
		} else if c == '%' || c == '_' {
			return out.String(), true, escaped, c == '%' && strings.Trim(pattern[i:], "%") == ""
		}
		out.WriteByte(c)
	}
	return out.String(), false, escaped, false
}

func prefixStringDomain(typ plan.Type) (binary, usable bool) {
	oid := types.T(typ.Id)
	if !oid.IsMySQLString() || oid == types.T_char || typ.PadSpace || types.IsUnicodeCollation(uint8(typ.Charset)) {
		return false, false
	}
	return types.NewWithCharset(oid, typ.Width, typ.Scale, uint8(typ.Charset)).Charset == types.CharsetBinary, true
}

// stringPrefixSuccessor returns a typed string boundary, never an arbitrary
// invalid UTF-8 byte sequence in a text expression. No upper bound means no
// optional index candidate; the original predicate is unaffected.
func stringPrefixSuccessor(prefix string, binary bool) (string, bool) {
	if prefix == "" {
		return "", false
	}
	if binary {
		b := []byte(prefix)
		for i := len(b) - 1; i >= 0; i-- {
			if b[i] != 255 {
				b[i]++
				return string(b[:i+1]), true
			}
		}
		return "", false
	}
	if !utf8.ValidString(prefix) || strings.ContainsRune(prefix, utf8.RuneError) {
		return "", false
	}
	for end := len(prefix); end > 0; {
		r, size := utf8.DecodeLastRuneInString(prefix[:end])
		end -= size
		if r == utf8.MaxRune {
			continue
		}
		r++
		if r == 0xd800 {
			r = 0xe000
		}
		return prefix[:end] + string(r), true
	}
	return "", false
}

// logicalStringPrefix distinguishes a literal string-content prefix from a
// complete encoded tuple-field prefix. Parameters, including folded literals
// retaining Src, cannot publish execution-specific bounds into a cached plan.
func logicalStringPrefix(fn *plan.Function) (prefix, upper string, ok bool) {
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "prefix_eq" || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[0].GetCol() == nil {
		return "", "", false
	}
	binary, usable := prefixStringDomain(fn.Args[0].Typ)
	lit := fn.Args[1].GetLit()
	if !usable || lit == nil || lit.Isnull || lit.Src != nil {
		return "", "", false
	}
	value, literalString := lit.Value.(*plan.Literal_Sval)
	if !literalString {
		return "", "", false
	}
	upper, ok = stringPrefixSuccessor(value.Sval, binary)
	return value.Sval, upper, ok
}

// Table metadata, rather than a possibly unnamed ColRef, distinguishes user
// strings from the encoded strings used by compound and secondary-index keys.
func logicalPrefixOnTable(fn *plan.Function, table *plan.TableDef) bool {
	if _, _, ok := logicalStringPrefix(fn); !ok || table == nil {
		return false
	}
	pos := fn.Args[0].GetCol().ColPos
	if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil {
		return false
	}
	col := table.Cols[pos]
	if col.Hidden || col.Name == catalog.CPrimaryKeyColName || col.Name == catalog.IndexTableIndexColName || col.Name == catalog.IndexTablePrimaryColName || strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
		return false
	}
	_, usable := prefixStringDomain(col.Typ)
	return usable && col.Typ.Id == fn.Args[0].Typ.Id && col.Typ.Charset == fn.Args[0].Typ.Charset
}

func stringPrefixRange(ctx context.Context, fn *plan.Function) (*plan.Expr, bool) {
	prefix, upper, ok := logicalStringPrefix(fn)
	if !ok {
		return nil, false
	}
	binary, _ := prefixStringDomain(fn.Args[0].Typ)
	lowerExpr := makePlan2StringConstExprWithType(prefix, binary)
	upperExpr := makePlan2StringConstExprWithType(upper, binary)
	lowerExpr.Typ.Charset = fn.Args[0].Typ.Charset
	upperExpr.Typ.Charset = fn.Args[0].Typ.Charset
	expr, err := BindFuncExprImplByPlanExpr(ctx, "in_range", []*plan.Expr{DeepCopyExpr(fn.Args[0]), lowerExpr, upperExpr, makePlan2Uint8ConstExprWithType(2)})
	return expr, err == nil
}

// A distribution describes the analyzed snapshot, not a min/max interpolation.
// Reject mismatched identities and populations before using it for access cost.
func stringPrefixSelectivity(fn *plan.Function, stats *statspb.StatsInfo, node *plan.Node) (float64, bool) {
	// The session cache is keyed by table ID, not by the individual scan's
	// snapshot. A sibling current scan must never lend its sample to history.
	if node == nil || IsSnapshotValid(node.ScanSnapshot) {
		return 0, false
	}
	table := node.TableDef
	if !logicalPrefixOnTable(fn, table) || stats == nil || !finitePositive(stats.TableCnt) || stats.TableCnt > 1<<53 {
		return 0, false
	}
	col := table.Cols[fn.Args[0].GetCol().ColPos]
	d := stats.ByteRangeMap[col.Name]
	if d == nil || d.TypeId != uint32(col.Typ.Id) || d.Charset != col.Typ.Charset || d.CollationVersion != col.Typ.CollationVersion || d.TableVersion != table.Version || d.ColumnSeqnum != uint32(col.Seqnum) || d.PopulationRows > 1<<53 || float64(d.PopulationRows) != stats.TableCnt {
		return 0, false
	}
	lower, upper, _ := logicalStringPrefix(fn)
	return analyzestats.ByteRangeUpper(d, []byte(lower), []byte(upper))
}

func (ctx *encodedRegularIndexCostContext) skipPrefixIndexByStats(pos int32) bool {
	if ctx.force {
		return false
	}
	fact := ctx.filterFacts[pos]
	if !ctx.valid || !fact.prefixKnown {
		return true
	}
	limit := float64(GetInFilterCardLimitOnPK(ctx.builder.compCtx.GetProcess().GetService(), ctx.node.Stats.TableCnt))
	if catalog.IsFakePkName(ctx.node.TableDef.Pkey.PkeyColName) {
		limit = min(limit, float64(InFilterCardLimitNonPK))
	}
	return fact.prefixUpper > InFilterSelectivityLimit || ctx.node.Stats.TableCnt*fact.prefixUpper > limit
}
