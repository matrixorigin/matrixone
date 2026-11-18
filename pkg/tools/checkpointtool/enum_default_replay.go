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

package checkpointtool

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// verifyCheckpointEnumDefault proves that a persisted default can be rebound
// against the type reconstructed by the emitted ENUM declaration. Metadata
// uncertainty is retained on the column and rejects script publication.
func verifyCheckpointEnumDefault(view *LogicalTableView, rowIdx int, row []string, hasCol, valueCol, notNullCol int, col *TableColumn) {
	fail := func(reason string) { col.enumDefaultError = reason }
	for _, idx := range []int{hasCol, valueCol, notNullCol} {
		if idx < 0 || idx >= len(row) {
			fail("missing default or nullability metadata cell")
			return
		}
		isNull, known := view.cellNull(rowIdx, idx)
		if !known || isNull {
			fail("missing, NULL or unverified default or nullability metadata cell")
			return
		}
	}
	nullFlag := strings.ToLower(strings.TrimSpace(row[notNullCol]))
	if nullFlag != "0" && nullFlag != "1" && nullFlag != "false" && nullFlag != "true" {
		fail("invalid nullability metadata flag")
		return
	}
	flag := strings.ToLower(strings.TrimSpace(row[hasCol]))
	if flag != "0" && flag != "1" && flag != "false" && flag != "true" {
		fail("invalid default metadata flag")
		return
	}
	hasEncoded := flag == "1" || flag == "true"
	raw := row[valueCol]
	if hasEncoded != (len(raw) > 0) {
		fail("inconsistent default metadata flag")
		return
	}
	var def plan.Default
	if err := types.Decode([]byte(raw), &def); err != nil {
		fail("invalid default expression encoding")
		return
	}
	if def.NullAbility == col.NotNull {
		fail("default and column nullability disagree")
		return
	}
	if def.Expr == nil {
		if def.OriginString != "" {
			fail("default text has no persisted expression")
			return
		}
		col.enumDefaultNoExpr = true
		return
	}
	if !hasEncoded || strings.TrimSpace(def.OriginString) == "" {
		fail("persisted default expression has no text")
		return
	}
	if err := proveCheckpointEnumDefault(col, &def); err != nil {
		fail(err.Error())
		return
	}
	col.Default = strings.TrimSpace(def.OriginString)
	col.enumDefaultLiteral = true
}

func proveCheckpointEnumDefault(col *TableColumn, def *plan.Default) error {
	origin := strings.TrimSpace(def.OriginString)
	ctx := context.Background()
	stmts, err := mysql.ParseWithSQLMode(ctx, "SELECT "+origin, 1, "")
	if err != nil {
		return moerr.NewInvalidInputNoCtx("cannot parse persisted default")
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return moerr.NewInvalidInputNoCtx("default is not a single literal")
	}
	sel, ok := stmts[0].(*tree.Select)
	if !ok || sel.IsPerform || sel.With != nil || sel.RewriteOption != nil || sel.TimeWindow != nil || len(sel.OrderBy) != 0 || sel.Limit != nil || sel.RankOption != nil || sel.Ep != nil || sel.SelectLockInfo != nil {
		return moerr.NewInvalidInputNoCtx("default has unsupported statement syntax")
	}
	clause, ok := sel.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || clause.Option != 0 || len(clause.Exprs) != 1 || (clause.Exprs[0].As != nil && !clause.Exprs[0].As.Empty()) || clause.Where != nil || clause.GroupBy != nil || clause.Having != nil || clause.From == nil || len(clause.From.Tables) != 1 {
		return moerr.NewInvalidInputNoCtx("default has unsupported SELECT syntax")
	}
	alias, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
	if !ok || alias.As.Alias != "" || len(alias.As.Cols) != 0 || len(alias.IndexHints) != 0 {
		return moerr.NewInvalidInputNoCtx("default references a table")
	}
	table, ok := alias.Expr.(*tree.TableName)
	if !ok || table.ObjectName != "" || table.ExplicitSchema || table.ExplicitCatalog || table.AtTsExpr != nil || table.IcebergRef != nil {
		return moerr.NewInvalidInputNoCtx("default references a table")
	}
	expr := clause.Exprs[0].Expr
	if tree.StringWithOpts(expr, dialect.MYSQL, tree.WithSingleQuoteString()) != origin {
		return moerr.NewInvalidInputNoCtx("default has noncanonical or trailing SQL")
	}
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = paren.Expr
	}
	literal, ok := expr.(*tree.NumVal)
	if !ok {
		return moerr.NewInvalidInputNoCtx("default is not a supported literal")
	}
	switch literal.ValType {
	case tree.P_char, tree.P_int64, tree.P_uint64:
	case tree.P_null:
		if col.NotNull {
			return moerr.NewInvalidInputNoCtx("NULL default is incompatible with NOT NULL")
		}
	default:
		return moerr.NewInvalidInputNoCtx("unsupported default literal")
	}
	members, err := types.DecodeEnumMembers(col.EnumValues, col.EnumEncoding)
	if err != nil {
		return moerr.NewInvalidInputNoCtx("invalid ENUM domain")
	}
	payload, encoding, err := types.EncodeEnumMembers(members)
	if err != nil {
		return moerr.NewInvalidInputNoCtx("cannot reconstruct ENUM domain")
	}
	target := plan.Type{Id: int32(types.T_enum), Enumvalues: payload, EnumEncoding: encoding, EnumCollation: col.EnumCollation}
	binder := sqlplan.NewDefaultBinder(ctx, nil, nil, target, nil)
	bound, err := binder.BindExpr(expr, 0, false)
	if err != nil {
		return moerr.NewInvalidInputNoCtx("cannot bind persisted default")
	}
	rebuilt, err := sqlplan.MakePlan2AssignmentCastExpr(ctx, bound, target)
	if err != nil || !proto.Equal(def.Expr, rebuilt) {
		return moerr.NewInvalidInputNoCtx("persisted default differs from reconstructed typed expression")
	}
	return nil
}
