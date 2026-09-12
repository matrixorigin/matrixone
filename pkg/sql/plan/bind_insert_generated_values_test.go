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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func buildGeneratedInsertPlan(t *testing.T, ctx *MockCompilerContext, stmt tree.Statement, prepared bool) (*Plan, error) {
	t.Helper()
	return BuildPlan(ctx, stmt, prepared)
}

func TestImplicitInsertValueColumnsIncludeVisibleGeneratedColumns(t *testing.T) {
	columns, insertColumns, hasGenerated := implicitInsertValueColumns(&planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "before"},
			{Name: "generated_middle", GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "hidden_internal", Hidden: true},
			{Name: "ordinary_after"},
			{Name: "generated_last", GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "hidden_generated", Hidden: true, GeneratedCol: &planpb.GeneratedCol{}},
		},
	})

	require.True(t, hasGenerated)
	require.Equal(t, tree.IdentifierList{"before", "generated_middle", "ordinary_after", "generated_last"}, columns)
	require.Equal(t, []string{"before", "ordinary_after"}, insertColumns)

	columns, insertColumns, hasGenerated = implicitInsertValueColumns(&planpb.TableDef{
		Cols: []*planpb.ColDef{{Name: "a"}, {Name: "hidden", Hidden: true}, {Name: "b"}},
	})
	require.False(t, hasGenerated)
	require.Nil(t, columns, "tables without generated columns should not allocate a synthetic input list")
	require.Equal(t, []string{"a", "b"}, insertColumns)
}

func TestStripImplicitGeneratedColumnsPreservesOrderAndSourceAST(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	tableDef := &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "a"},
			{Name: "g1", GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "b"},
			{Name: "g2", GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "hidden_internal", Hidden: true},
		},
		Name2ColIndex: map[string]int32{
			"a": 0, "g1": 1, "b": 2, "g2": 3, "hidden_internal": 4,
		},
	}
	columns, insertColumns, hasGenerated := implicitInsertValueColumns(tableDef)
	require.True(t, hasGenerated)
	require.Equal(t, tree.IdentifierList{"a", "g1", "b", "g2"}, columns)
	require.Equal(t, []string{"a", "b"}, insertColumns)

	builder := NewQueryBuilder(planpb.Query_INSERT, ctx, true, true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "insert into t values (1, default, 2, default)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	insert := stmt.(*tree.Insert)
	originalSelect := insert.Rows.Select
	originalValues := originalSelect.(*tree.ValuesClause)

	cleanedColumns, rowsForInsert, err := builder.stripGeneratedDefaultCols(columns, insert.Rows, tableDef)
	require.NoError(t, err)
	require.Equal(t, tree.IdentifierList{"a", "b"}, cleanedColumns)
	require.NotSame(t, insert.Rows, rowsForInsert)
	require.Same(t, originalSelect, insert.Rows.Select)
	require.Len(t, originalValues.Rows[0], 4)
	require.Len(t, rowsForInsert.Select.(*tree.ValuesClause).Rows[0], 2)

	badStmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "insert into t values (1, 10, 2, 20)", 1)
	require.NoError(t, err)
	defer badStmt.Free()
	badInsert := badStmt.(*tree.Insert)
	_, _, err = builder.stripGeneratedDefaultCols(columns, badInsert.Rows, tableDef)
	require.EqualError(t, err, "invalid input: the value specified for generated column 'g1' in table 't' is not allowed")
	require.Len(t, badInsert.Rows.Select.(*tree.ValuesClause).Rows[0], 4)
}

func TestImplicitInsertValuesWithGeneratedColumns(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantMySQL   uint16
		wantErrText string
	}{
		{
			name: "full tuple with generated default",
			sql:  "insert into constraint_test.t_on_update_gen values (1, 2, default, default)",
		},
		{
			name: "multiple full tuples",
			sql:  "insert into constraint_test.t_on_update_gen values (1, 2, default, default), (2, 3, default, default)",
		},
		{
			name: "empty tuple keeps all-default behavior",
			sql:  "insert into constraint_test.t_on_update_gen values ()",
		},
		{
			name:    "short tuple cannot omit generated position",
			sql:     "insert into constraint_test.t_on_update_gen values (1, 2, default)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 1",
		},
		{
			name:    "long tuple keeps row count error",
			sql:     "insert into constraint_test.t_on_update_gen values (1, 2, default, default, 3)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 1",
		},
		{
			name:        "generated value must be default",
			sql:         "insert into constraint_test.t_on_update_gen values (1, 2, default, current_timestamp)",
			wantErr:     true,
			wantErrText: "invalid input: the value specified for generated column 'g' in table 't_on_update_gen' is not allowed",
		},
		{
			name:    "row width is checked before generated value",
			sql:     "insert into constraint_test.t_on_update_gen values (1, 2, current_timestamp)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 1",
		},
		{
			name:    "second row width reports its row number",
			sql:     "insert into constraint_test.t_on_update_gen values (1, 2, default, default), (2, 3, default)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 2",
		},
		{
			name:    "explicit generated insert retains excess value for arity error",
			sql:     "insert into constraint_test.t_on_update_gen (id, val, updated_at, g) values (1, 2, default, default, 99)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 1",
		},
		{
			name:    "explicit generated replace reports excess later row",
			sql:     "replace into constraint_test.t_on_update_gen (id, val, updated_at, g) values (1, 2, default, default), (2, 3, default, default, 99)",
			wantErr: true, wantMySQL: moerr.ER_WRONG_VALUE_COUNT_ON_ROW,
			wantErrText: "Column count doesn't match value count at row 2",
		},
		{
			name: "explicit ordinary-column list remains valid",
			sql:  "insert into constraint_test.t_on_update_gen (id, val, updated_at) values (1, 2, default)",
		},
		{
			name: "replace uses the same full tuple contract",
			sql:  "replace into constraint_test.t_on_update_gen values (1, 2, default, default)",
		},
		{
			name: "implicit insert select mapping is unchanged",
			sql:  "insert into constraint_test.t_on_update_gen select 1, 2, current_timestamp",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = buildGeneratedInsertPlan(t, ctx, stmt, false)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.EqualError(t, err, tc.wantErrText)
			if tc.wantMySQL != 0 {
				moErr, ok := err.(*moerr.Error)
				require.True(t, ok)
				require.Equal(t, tc.wantMySQL, moErr.MySQLCode())
			}
		})
	}
}

func TestPreparedGeneratedInsertDoesNotMutateRetainedAST(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "implicit insert",
			sql:  "insert into constraint_test.t_on_update_gen values (1, 2, default, default)",
		},
		{
			name: "explicit insert default",
			sql:  "insert into constraint_test.t_on_update_gen (id, val, updated_at, g) values (1, 2, default, default)",
		},
		{
			name: "replace",
			sql:  "replace into constraint_test.t_on_update_gen values (1, 2, default, default)",
		},
		{
			name:    "failed bind after generated rewrite",
			sql:     "insert into constraint_test.t_on_update_gen values (1, 2, default, default) on duplicate key update missing_col = 1",
			wantErr: true,
		},
		{
			name:    "explicit insert arity failure preserves retained AST",
			sql:     "insert into constraint_test.t_on_update_gen (id, val, updated_at, g) values (1, 2, default, default, 99)",
			wantErr: true,
		},
		{
			name:    "explicit replace later-row arity failure preserves retained AST",
			sql:     "replace into constraint_test.t_on_update_gen (id, val, updated_at, g) values (1, 2, default, default), (2, 3, default, default, 99)",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			var originalRows *tree.Select
			switch insert := stmt.(type) {
			case *tree.Insert:
				originalRows = insert.Rows
			case *tree.Replace:
				originalRows = insert.Rows
			default:
				t.Fatalf("unexpected statement type %T", stmt)
			}
			originalSelect := originalRows.Select
			originalValues := originalSelect.(*tree.ValuesClause)
			originalRowsData := make([]tree.Exprs, len(originalValues.Rows))
			for i, row := range originalValues.Rows {
				originalRowsData[i] = append(tree.Exprs(nil), row...)
			}

			for range 2 {
				preparedPlan, _, err := getPreparePlan(ctx, stmt)
				if tc.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.NotNil(t, preparedPlan)
				}
				require.Same(t, originalRows, func() *tree.Select {
					switch insert := stmt.(type) {
					case *tree.Insert:
						return insert.Rows
					case *tree.Replace:
						return insert.Rows
					default:
						return nil
					}
				}())
				require.Same(t, originalSelect, originalRows.Select)
				require.Equal(t, originalRowsData, originalValues.Rows)
				require.IsType(t, &tree.DefaultVal{}, originalValues.Rows[0][3])
			}
		})
	}
}
