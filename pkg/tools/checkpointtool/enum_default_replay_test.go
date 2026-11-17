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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func checkpointDefaultFixture(t *testing.T, members []string, origin string) (*LogicalTableView, *LogicalTableView, *plan.Default) {
	t.Helper()
	tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, members, nil)
	payload, encoding, err := types.EncodeEnumMembers(members)
	require.NoError(t, err)
	target := plan.Type{Id: int32(types.T_enum), Enumvalues: payload, EnumEncoding: encoding, EnumCollation: types.EnumCollationUTF8Bin}
	stmts, err := mysql.ParseWithSQLMode(context.Background(), "SELECT "+origin, 1, "")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	expr := stmts[0].(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	for {
		p, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = p.Expr
	}
	bound, err := sqlplan.NewDefaultBinder(context.Background(), nil, nil, target, nil).BindExpr(expr, 0, false)
	require.NoError(t, err)
	cast, err := sqlplan.MakePlan2AssignmentCastExpr(context.Background(), bound, target)
	require.NoError(t, err)
	def := &plan.Default{Expr: cast, OriginString: origin, NullAbility: true}
	setCheckpointDefault(t, columns, "1", def)
	return tables, columns, def
}

func setCheckpointDefault(t *testing.T, view *LogicalTableView, flag string, def *plan.Default) {
	t.Helper()
	raw, err := types.Encode(def)
	require.NoError(t, err)
	setCatalogCell(t, catalog.MoColumnsSchema, view.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_HasExpr, flag)
	setCatalogCell(t, catalog.MoColumnsSchema, view.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_DefaultExpr, string(raw))
}

func TestCheckpointEnumDefaultReplaysTypedLiteral(t *testing.T) {
	cases := []struct {
		name    string
		members []string
		origin  string
	}{
		{"apostrophe", []string{"it's", "it''s"}, `'it''s'`},
		{"backslash", []string{`a\b`, "ab"}, `'a\\b'`},
		{"empty", []string{"", "a"}, `''`},
		{"number label", []string{"2", "x"}, `'2'`},
		{"number ordinal", []string{"2", "x"}, `2`},
		{"zero ordinal", []string{"2", "x"}, `0`},
		{"explicit null", []string{"A", "a"}, `null`},
		{"parentheses", []string{"A", "a"}, `('A')`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tables, columns, _ := checkpointDefaultFixture(t, tc.members, tc.origin)
			schema := buildTableSchemaFromCatalogViews(101, tables, columns)
			require.NoError(t, ValidateEnumReplaySchema(schema))
			require.Contains(t, RenderCreateTableDDLFromSchema(cloneTableSchema(schema)), " DEFAULT "+tc.origin)
		})
	}
}

func TestCheckpointEnumDefaultNoExpression(t *testing.T) {
	for _, tc := range []struct {
		name, flag, notNull string
		def                 plan.Default
	}{
		{"nullable encoded", "1", "0", plan.Default{NullAbility: true}},
		{"not null empty", "0", "1", plan.Default{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
			setCheckpointDefault(t, columns, tc.flag, &tc.def)
			setCatalogCell(t, catalog.MoColumnsSchema, columns.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_NullAbility, tc.notNull)
			schema := buildTableSchemaFromCatalogViews(101, tables, columns)
			require.NoError(t, ValidateEnumReplaySchema(schema))
			ddl := RenderCreateTableDDLFromSchema(schema)
			require.NotContains(t, ddl, " DEFAULT ")
			if tc.notNull == "1" {
				require.Contains(t, ddl, " NOT NULL")
			}
		})
	}
}

func TestCheckpointEnumDefaultRejectsUnprovenMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *LogicalTableView, *plan.Default)
	}{
		{"missing expression cell", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			i := v.columnDataIndex(catalog.SystemColAttr_DefaultExpr)
			v.RowNulls[0][i/64] |= uint64(1) << uint(i%64)
		}},
		{"missing nullability", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			i := v.columnDataIndex(catalog.SystemColAttr_NullAbility)
			v.RowNulls[0][i/64] |= uint64(1) << uint(i%64)
		}},
		{"missing flag", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			i := v.columnDataIndex(catalog.SystemColAttr_HasExpr)
			v.RowNulls[0][i/64] |= uint64(1) << uint(i%64)
		}},
		{"invalid flag", func(t *testing.T, v *LogicalTableView, d *plan.Default) { setCheckpointDefault(t, v, "2", d) }},
		{"inconsistent flag", func(t *testing.T, v *LogicalTableView, d *plan.Default) { setCheckpointDefault(t, v, "0", d) }},
		{"empty default with nullable column", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			setCheckpointDefault(t, v, "0", &plan.Default{})
		}},
		{"malformed protobuf", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			setCatalogCell(t, catalog.MoColumnsSchema, v.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_DefaultExpr, "\xff")
		}},
		{"stale literal", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			d.OriginString = "'a'"
			setCheckpointDefault(t, v, "1", d)
		}},
		{"trailing comment", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			d.OriginString = "'A' -- rest"
			setCheckpointDefault(t, v, "1", d)
		}},
		{"trailing statement", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			d.OriginString = "'A'; select 1"
			setCheckpointDefault(t, v, "1", d)
		}},
		{"real from", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			d.OriginString = "'A' from colors"
			setCheckpointDefault(t, v, "1", d)
		}},
		{"nullability conflict", func(t *testing.T, v *LogicalTableView, d *plan.Default) {
			setCatalogCell(t, catalog.MoColumnsSchema, v.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_NullAbility, "1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, columns, def := checkpointDefaultFixture(t, []string{"A", "a"}, "'A'")
			tc.mutate(t, columns, def)
			schema := buildTableSchemaFromCatalogViews(101, tables, columns)
			require.ErrorContains(t, ValidateEnumReplaySchema(schema), "cannot verify checkpoint ENUM default")
			require.Empty(t, RenderCreateTableDDLFromSchema(schema))
		})
	}
	t.Run("explicit null on not null column", func(t *testing.T) {
		tables, columns, def := checkpointDefaultFixture(t, []string{"A", "a"}, "null")
		def.NullAbility = false
		setCheckpointDefault(t, columns, "1", def)
		setCatalogCell(t, catalog.MoColumnsSchema, columns.Rows[0][logicalViewMetaCols:], catalog.SystemColAttr_NullAbility, "1")
		schema := buildTableSchemaFromCatalogViews(101, tables, columns)
		require.ErrorContains(t, ValidateEnumReplaySchema(schema), "NULL default is incompatible")
	})
}
