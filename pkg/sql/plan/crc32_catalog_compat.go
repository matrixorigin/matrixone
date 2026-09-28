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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Internal ALTER COPY and catalog LIKE/CLONE supply this execution-owned schema. SQL dump/replay
// remains new DDL. Never infer a legacy algorithm from display SQL or row values.
func crc32CopyColumn(ctx context.Context, name string) *planpb.ColDef {
	def, _ := ctx.Value(defines.CRC32CopyExpressionsKey{}).(*planpb.TableDef)
	if def == nil {
		return nil
	}
	return FindColumn(def.Cols, name)
}

func containsLegacyCRC32(owner *planpb.Expr) bool {
	found := false
	_ = planpb.VisitExprTree(owner, func(expr *planpb.Expr) error {
		f := expr.GetF()
		if f != nil && f.Func != nil && f.Func.Obj == function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload) && len(f.Args) != 0 && f.Args[0] != nil && f.Args[0].Typ.Id == int32(types.T_json) {
			found = true
		}
		return nil
	})
	return found
}

// CHANGE/MODIFY repeats the column definition even for a rename or reorder.
// Preserve its bound identity when the expression and type are unchanged. A
// semantic conversion needs an explicit rebuild, never a metadata-only rewrite.
func preserveLegacyCRC32Generated(ctx context.Context, old *planpb.ColDef, attr *tree.AttributeGeneratedAlways, typ planpb.Type) (*planpb.GeneratedCol, bool, error) {
	if old.GeneratedCol == nil || !containsLegacyCRC32(old.GeneratedCol.Expr) {
		return nil, false, nil
	}
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(attr.Expr, attr.Expr, false)
	if old.GeneratedCol.OriginString != fmtCtx.String() || old.GeneratedCol.IsStored != attr.Stored || (old.Typ.Id != typ.Id || old.Typ.Width != typ.Width || old.Typ.Scale != typ.Scale) {
		return nil, true, moerr.NewNotSupported(ctx, "changing a legacy CRC32 JSON generated column requires an explicit table rebuild")
	}
	owned := *old.GeneratedCol
	owned.Expr = DeepCopyExpr(owned.Expr)
	return &owned, true, nil
}

func crc32SourceColumn(ctx context.Context, name string, sources []*planpb.ColDef) *planpb.ColDef {
	if len(sources) > 0 && sources[0] != nil {
		return sources[0]
	}
	return crc32CopyColumn(ctx, name)
}

func tableHasLegacyCRC32(def *planpb.TableDef) bool {
	if def == nil {
		return false
	}
	for _, col := range def.Cols {
		if containsLegacyCRC32(col.GetGeneratedCol().GetExpr()) || preserveCRC32Default(col.GetDefault()) || containsLegacyCRC32(col.GetOnUpdate().GetExpr()) {
			return true
		}
	}
	for _, check := range def.Checks {
		if containsLegacyCRC32(check.GetCheck()) {
			return true
		}
	}
	return false
}

// Older constant defaults can lack Literal.Src. Preserve their authoritative
// stored value during COPY/LIKE instead of evaluating their display SQL again.
// A conservative textual match may also preserve an ordinary literal containing
// "crc32"; that is safe and does not infer or rewrite its execution identity.
func preserveCRC32Default(def *planpb.Default) bool {
	return def != nil && (containsLegacyCRC32(def.Expr) ||
		(def.Expr.GetLit() != nil && strings.Contains(strings.ToLower(def.OriginString), "crc32")))
}

// A CHANGE/MODIFY statement repeats defaults and ON UPDATE clauses even when
// only the column name or position changes. Preserve unchanged contracts while
// allowing an explicitly different definition to bind normally.
func unchangedCRC32ColumnClauses(old *planpb.ColDef, col *tree.ColumnTableDef, typ planpb.Type) *planpb.ColDef {
	if old.Typ.Id != typ.Id || old.Typ.Width != typ.Width || old.Typ.Scale != typ.Scale {
		return nil
	}
	copy := &planpb.ColDef{}
	for _, attr := range col.Attributes {
		switch a := attr.(type) {
		case *tree.AttributeDefault:
			if preserveCRC32Default(old.Default) {
				fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
				fmtCtx.PrintExpr(a.Expr, a.Expr, false)
				if fmtCtx.String() == old.Default.OriginString {
					value := *old.Default
					value.NullAbility = getColumnNullAbility(col)
					copy.Default = &value
				}
			}
		case *tree.AttributeOnUpdate:
			if old.OnUpdate != nil && containsLegacyCRC32(old.OnUpdate.Expr) && tree.String(a.Expr, dialect.MYSQL) == old.OnUpdate.OriginString {
				copy.OnUpdate = old.OnUpdate
			}
		}
	}
	return copy
}
