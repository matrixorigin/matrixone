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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The scoped DDL replay identifies the schema used to preserve old expressions.
func crc32CopyColumn(replay *persistedDDLReplay, name string) *planpb.ColDef {
	if replay == nil || replay.target == nil {
		return nil
	}
	return FindColumn(replay.target.Cols, name)
}

func containsLegacyCRC32(owner *planpb.Expr) bool {
	found := false
	_ = planpb.VisitExprTree(owner, func(expr *planpb.Expr) error {
		f := expr.GetF()
		if f != nil && f.Func != nil && f.Func.Obj == function.EncodeOverloadID(function.CRC32, 0) && len(f.Args) != 0 && f.Args[0] != nil && f.Args[0].Typ.Id == int32(types.T_json) {
			found = true
		}
		return nil
	})
	return found
}

// The legacy DML planner fills omitted columns from defaults rather than
// evaluating GeneratedCol.Expr. Both virtual and stored generated columns are
// materialized by the modern planner, so neither is safe on that fallback.
func rejectLegacyCRC32GeneratedFallback(ctx context.Context, table *planpb.TableDef) error {
	if table == nil {
		return nil
	}
	for _, col := range table.Cols {
		if col != nil && col.GeneratedCol != nil && containsLegacyCRC32(col.GeneratedCol.Expr) {
			return moerr.NewNotSupported(ctx, "legacy DML planner cannot write a legacy CRC32 JSON generated column")
		}
	}
	return nil
}

// A recursive FK update already carries the old row into its insert branch.
// It can retain a stored legacy value if neither that column nor any of its
// generated dependencies can change, and the insert projection really uses
// the original column position. Other legacy fallback writes remain rejected.
func rejectLegacyCRC32GeneratedUpdateFallback(ctx context.Context, update *dmlPlanCtx) error {
	if update == nil || update.tableDef == nil {
		return nil
	}
	if rejectLegacyCRC32GeneratedFallback(ctx, update.tableDef) == nil {
		return nil
	}
	if unchangedLegacyCRC32GeneratedForFK(update) {
		return nil
	}
	return rejectLegacyCRC32GeneratedFallback(ctx, update.tableDef)
}

func unchangedLegacyCRC32GeneratedForFK(update *dmlPlanCtx) bool {
	if !update.isFkRecursionCall || len(update.updateColPosMap) == 0 {
		return false
	}
	cols := update.tableDef.Cols
	positions := make(map[string]int, len(cols))
	changed := make([]bool, len(cols))
	for i, col := range cols {
		if col == nil || col.Name == "" {
			return false
		}
		name := strings.ToLower(col.Name)
		if _, duplicate := positions[name]; duplicate {
			return false
		}
		positions[name] = i
		changed[i] = col.Hidden || col.OnUpdate != nil || col.Typ.AutoIncr
	}
	for name := range update.updateColPosMap {
		pos, ok := positions[strings.ToLower(catalog.ResolveAlias(name))]
		if !ok {
			return false
		}
		changed[pos] = true
	}

	// A generated input can itself depend on another generated column. Walk
	// the catalog-bound expressions and fail closed on invalid or unsupported
	// references, cycles, and functions that can change without a column update.
	state := make([]uint8, len(cols))
	var unchanged func(int) bool
	unchanged = func(pos int) bool {
		if changed[pos] || state[pos] == 1 {
			return false
		}
		if state[pos] == 2 || cols[pos].GeneratedCol == nil {
			return true
		}
		expr := cols[pos].GeneratedCol.Expr
		if expr == nil {
			return false
		}
		state[pos] = 1
		valid := true
		var refs []int
		_ = planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
			switch e := current.Expr.(type) {
			case *planpb.Expr_Col:
				if e.Col == nil || e.Col.RelPos != 0 || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(cols) || current.Typ.Id != cols[e.Col.ColPos].Typ.Id {
					valid = false
				} else {
					refs = append(refs, int(e.Col.ColPos))
				}
			case *planpb.Expr_F:
				if e.F == nil || e.F.Func == nil {
					valid = false
					break
				}
				impl, ok := function.GetFunctionByIdWithoutError(e.F.Func.Obj)
				if !ok || impl.IsVolatile() || impl.IsRealTimeRelated() {
					valid = false
				}
				for _, arg := range e.F.Args {
					valid = valid && arg != nil
				}
			case *planpb.Expr_Lit:
				valid = valid && e.Lit != nil
			case *planpb.Expr_T:
				valid = valid && e.T != nil
			case *planpb.Expr_List:
				if e.List == nil {
					valid = false
					break
				}
				for _, item := range e.List.List {
					valid = valid && item != nil
				}
			default:
				valid = false
			}
			return nil
		})
		if !valid {
			return false
		}
		for _, ref := range refs {
			if !unchanged(ref) {
				return false
			}
		}
		state[pos] = 2
		return true
	}

	visible := 0
	for pos, col := range cols {
		excluded := col.Hidden && col.Name != catalog.FakePrimaryKeyColName && !isFunctionalColumn(col)
		if col.GeneratedCol != nil && containsLegacyCRC32(col.GeneratedCol.Expr) {
			if excluded || !col.GeneratedCol.IsStored || !unchanged(pos) || visible >= len(update.insertColPos) || update.insertColPos[visible] != pos {
				return false
			}
		}
		if !excluded {
			visible++
		}
	}
	return true
}

// Formatting, identifier quoting and identifier/function case do not make a
// repeated generated expression a semantic change. Compare scanner tokens so
// string literal case stays significant. An invalid historical origin cannot
// establish either equality or an intentional change.
func sameLegacyCRC32Expression(left, right string) (bool, bool) {
	if left == "" || right == "" {
		return false, false
	}
	a, b := mysql.NewScanner(dialect.MYSQL, left), mysql.NewScanner(dialect.MYSQL, right)
	defer mysql.PutScanner(a)
	defer mysql.PutScanner(b)
	same := true
	for {
		apos, bpos := a.Pos, b.Pos
		at, av := a.Scan()
		bt, bv := b.Scan()
		if at == mysql.LEX_ERROR || bt == mysql.LEX_ERROR {
			return false, false
		}
		if at != bt {
			identifierPair := (at == mysql.ID && bt == mysql.QUOTE_ID) || (at == mysql.QUOTE_ID && bt == mysql.ID)
			if !identifierPair {
				// A quoted keyword can denote a column while the unquoted
				// spelling can be a literal or an operator. Do not infer
				// equivalence or a deliberate semantic change from the spelling.
				if (at == mysql.QUOTE_ID || bt == mysql.QUOTE_ID) && strings.EqualFold(av, bv) {
					return false, false
				}
				same = false
				continue
			}
		}
		if at == 0 {
			return same, true
		}
		if at == mysql.STRING {
			// Retain the exact formatted spelling as well as the decoded bytes.
			// The historical origin may have been authored under a different
			// backslash mode, so equal decoding alone is not a proof.
			if av == bv && strings.TrimSpace(left[apos:a.Pos]) != strings.TrimSpace(right[bpos:b.Pos]) {
				return false, false
			}
			if av != bv {
				same = false
			}
		} else if !strings.EqualFold(av, bv) {
			same = false
		}
	}
}

// CHANGE/MODIFY repeats the column definition even for a rename or reorder.
// Preserve its bound identity when the expression and type are unchanged. An
// intentionally different expression binds normally and COPY rebuilds values.
func preserveLegacyCRC32Generated(ctx context.Context, old *planpb.ColDef, attr *tree.AttributeGeneratedAlways, typ planpb.Type) (*planpb.GeneratedCol, bool, error) {
	if old.GeneratedCol == nil || !containsLegacyCRC32(old.GeneratedCol.Expr) {
		return nil, false, nil
	}
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(attr.Expr, attr.Expr, false)
	same, known := sameLegacyCRC32Expression(old.GeneratedCol.OriginString, fmtCtx.String())
	if !known {
		return nil, true, moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON generated column requires an explicit table rebuild")
	}
	if !same {
		return nil, false, nil
	}
	if old.GeneratedCol.IsStored != attr.Stored || (old.Typ.Id != typ.Id || old.Typ.Width != typ.Width || old.Typ.Scale != typ.Scale) {
		return nil, true, moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON generated column requires an explicit table rebuild")
	}
	owned := *old.GeneratedCol
	owned.Expr = DeepCopyExpr(owned.Expr)
	return &owned, true, nil
}

func crc32SourceColumn(sources []*planpb.ColDef) *planpb.ColDef {
	if len(sources) > 0 {
		return sources[0]
	}
	return nil
}

// A bound legacy expression cannot be copied onto a different input
// representation. Reject before COPY mutates the catalog, and also guard the
// internal CREATE fallback when ordinary persisted-expression replay declines it.
// The same contract covers generated columns and DEFAULT owners of a legacy
// CRC32 call: their ColRef types describe the hashed input representation.
func validateLegacyCRC32ExprInputs(ctx context.Context, expr *planpb.Expr, cols []*planpb.ColDef, subject string) error {
	if !containsLegacyCRC32(expr) {
		return nil
	}
	return planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
		ref := e.GetCol()
		if ref == nil {
			return nil
		}
		if ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(cols) || cols[ref.ColPos] == nil {
			return moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON "+subject+" requires an explicit table rebuild")
		}
		typ := cols[ref.ColPos].Typ
		if e.Typ.Id != typ.Id || e.Typ.Width != typ.Width || e.Typ.Scale != typ.Scale || e.Typ.AutoIncr != typ.AutoIncr ||
			e.Typ.Enumvalues != typ.Enumvalues || !e.Typ.SameCollation(typ) || e.Typ.PadSpace != typ.PadSpace {
			return moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON "+subject+" requires an explicit table rebuild")
		}
		return nil
	})
}

func validateLegacyCRC32GeneratedInputs(ctx context.Context, expr *planpb.Expr, cols []*planpb.ColDef) error {
	return validateLegacyCRC32ExprInputs(ctx, expr, cols, "generated column")
}

// Older constant defaults can lack Literal.Src. Preserve their authoritative
// stored value during COPY/LIKE instead of evaluating their display SQL again.
// A conservative textual match may also preserve an ordinary literal containing
// "crc32"; that is safe and does not infer or rewrite its execution identity.
func preserveCRC32Default(def *planpb.Default) bool {
	return def != nil && (containsLegacyCRC32(def.Expr) ||
		(def.Expr.GetLit() != nil && strings.Contains(strings.ToLower(def.OriginString), "crc32")))
}

// A string literal containing "crc32" is not itself a CRC32 call. Keep the
// existing conservative replay for such literals, but do not reject an
// ambiguous spelling change as though it necessarily used the old algorithm.
func crc32OriginMayCallFunction(origin string) bool {
	scanner := mysql.NewScanner(dialect.MYSQL, origin)
	defer mysql.PutScanner(scanner)
	previousCRC32 := false
	for {
		token, value := scanner.Scan()
		if token == mysql.LEX_ERROR {
			return true
		}
		if token == 0 {
			return false
		}
		if previousCRC32 && token == '(' {
			return true
		}
		// Double-quoted names can be identifiers under ANSI_QUOTES, even when
		// the current scanner classifies the same spelling as a string.
		previousCRC32 = (token == mysql.ID || token == mysql.QUOTE_ID || token == mysql.STRING) && strings.EqualFold(value, "crc32")
	}
}

// A CHANGE/MODIFY statement repeats defaults even when only the column name or
// position changes. Preserve an unchanged contract while allowing an explicitly
// different definition to bind normally. If historical spelling is ambiguous,
// do not silently replace a potentially unchanged legacy default.
func unchangedCRC32ColumnClauses(ctx context.Context, old *planpb.ColDef, col *tree.ColumnTableDef, typ planpb.Type, proc *process.Process) (*planpb.ColDef, error) {
	sameType := old.Typ.Id == typ.Id && old.Typ.Width == typ.Width && old.Typ.Scale == typ.Scale
	copy := &planpb.ColDef{}
	for _, attr := range col.Attributes {
		switch a := attr.(type) {
		case *tree.AttributeDefault:
			if preserveCRC32Default(old.Default) {
				fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
				fmtCtx.PrintExpr(a.Expr, a.Expr, false)
				same := fmtCtx.String() == old.Default.OriginString
				if !same {
					var known bool
					same, known = sameLegacyCRC32Expression(old.Default.OriginString, fmtCtx.String())
					if !known && (containsLegacyCRC32(old.Default.Expr) || crc32OriginMayCallFunction(old.Default.OriginString)) {
						return nil, moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON default requires an explicit table rebuild")
					}
				}
				if same {
					value := *old.Default
					if !sameType {
						if !containsLegacyCRC32(value.Expr) && !crc32OriginMayCallFunction(value.OriginString) {
							return nil, nil
						}
						// Convert the stored default, not its SQL spelling. Folding an
						// old literal also keeps it eligible for later COPY replay.
						converted, err := convertedPersistedAssignment(ctx, value.Expr, typ)
						if err != nil {
							return nil, err
						}
						if value.Expr.GetLit() != nil {
							converted, err = ConstantFold(batch.EmptyForConstFoldBatch, converted, proc, false, true)
							if err != nil {
								return nil, err
							}
						}
						value.Expr = converted
						if !preserveCRC32Default(&value) {
							return nil, moerr.NewNotSupported(ctx, "changing a legacy CRC32 default column type requires an explicit table rebuild")
						}
					}
					value.NullAbility = getColumnNullAbility(col)
					copy.Default = &value
				}
			}
		}
	}
	return copy, nil
}
