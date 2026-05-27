// Copyright 2021 Matrix Origin
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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// TestAnalyzeStmtFormat tests the Format output of AnalyzeStmt with single and multiple table entries.
func TestAnalyzeStmtFormat(t *testing.T) {
	tests := []struct {
		name     string
		entries  []*AnalyzeTableEntry
		expected string
	}{
		{
			name: "single table",
			entries: []*AnalyzeTableEntry{
				{Table: NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), Cols: IdentifierList{"a", "b"}},
			},
			expected: "analyze table t1(a, b)",
		},
		{
			name: "multiple tables",
			entries: []*AnalyzeTableEntry{
				{Table: NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), Cols: IdentifierList{"a", "b"}},
				{Table: NewTableName(Identifier("t2"), ObjectNamePrefix{}, nil), Cols: IdentifierList{"c", "d"}},
			},
			expected: "analyze table t1(a, b), t2(c, d)",
		},
		{
			name: "single column",
			entries: []*AnalyzeTableEntry{
				{Table: NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), Cols: IdentifierList{"col1"}},
			},
			expected: "analyze table t1(col1)",
		},
		{
			name:     "empty entries",
			entries:  []*AnalyzeTableEntry{},
			expected: "analyze table ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewAnalyzeStmt(tt.entries)
			ctx := NewFmtCtx(dialect.MYSQL)
			stmt.Format(ctx)
			require.Equal(t, tt.expected, ctx.String())
			stmt.Free()
		})
	}
}

func TestAnalyzeStmtMetadata(t *testing.T) {
	stmt := NewAnalyzeStmt([]*AnalyzeTableEntry{
		{Table: NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), Cols: IdentifierList{"a"}},
	})
	defer stmt.Free()

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Analyze Table", stmt.GetStatementType())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
}

// TestCheckTableStmt tests the CheckTableStmt AST node.
func TestCheckTableStmtFormat(t *testing.T) {
	tests := []struct {
		name     string
		tables   TableNames
		option   CheckTableOption
		expected string
	}{
		{
			name:     "single table no option",
			tables:   TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil)},
			option:   CheckTableOptionNone,
			expected: "check table t1",
		},
		{
			name:     "single table extended",
			tables:   TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil)},
			option:   CheckTableOptionExtended,
			expected: "check table t1 extended",
		},
		{
			name:     "multiple tables no option",
			tables:   TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), NewTableName(Identifier("t2"), ObjectNamePrefix{}, nil)},
			option:   CheckTableOptionNone,
			expected: "check table t1, t2",
		},
		{
			name:     "single table for upgrade",
			tables:   TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil)},
			option:   CheckTableOptionForUpgrade,
			expected: "check table t1 for upgrade",
		},
		{
			name:     "multiple tables extended",
			tables:   TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil), NewTableName(Identifier("t2"), ObjectNamePrefix{}, nil), NewTableName(Identifier("t3"), ObjectNamePrefix{}, nil)},
			option:   CheckTableOptionExtended,
			expected: "check table t1, t2, t3 extended",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewCheckTableStmt(tt.tables, tt.option)
			ctx := NewFmtCtx(dialect.MYSQL)
			stmt.Format(ctx)
			require.Equal(t, tt.expected, ctx.String())
		})
	}
}

func TestCheckTableStmtMetadata(t *testing.T) {
	stmt := NewCheckTableStmt(TableNames{NewTableName(Identifier("t1"), ObjectNamePrefix{}, nil)}, CheckTableOptionNone)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Check Table", stmt.GetStatementType())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
}

func TestCheckTableOptionValues(t *testing.T) {
	tests := []struct {
		name     string
		option   CheckTableOption
		expected CheckTableOption
	}{
		{"None", CheckTableOptionNone, CheckTableOptionNone},
		{"Extended", CheckTableOptionExtended, CheckTableOptionExtended},
		{"ForUpgrade", CheckTableOptionForUpgrade, CheckTableOptionForUpgrade},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.option)
		})
	}
}

// TestShowProfileStmt tests the ShowProfileStmt AST node.
func TestShowProfileStmtFormat(t *testing.T) {
	tests := []struct {
		name     string
		forQuery int64
		limit    *Limit
		expected string
	}{
		{
			name:     "basic show profile",
			forQuery: 0,
			limit:    nil,
			expected: "show profile",
		},
		{
			name:     "with for query",
			forQuery: 2,
			limit:    nil,
			expected: "show profile for query 2",
		},
		{
			name:     "with limit",
			forQuery: 0,
			limit:    &Limit{Count: NewNumVal[int64](10, "10", false, P_int64)},
			expected: "show profile limit 10",
		},
		{
			name:     "with for query and limit",
			forQuery: 2,
			limit:    &Limit{Count: NewNumVal[int64](10, "10", false, P_int64)},
			expected: "show profile for query 2 limit 10",
		},
		{
			name:     "with for query, limit and offset",
			forQuery: 2,
			limit:    &Limit{Offset: NewNumVal[int64](5, "5", false, P_int64), Count: NewNumVal[int64](10, "10", false, P_int64)},
			expected: "show profile for query 2 limit 10 offset 5",
		},
		{
			name:     "with for query 42",
			forQuery: 42,
			limit:    nil,
			expected: "show profile for query 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewShowProfileStmt(tt.forQuery, tt.limit)
			ctx := NewFmtCtx(dialect.MYSQL)
			stmt.Format(ctx)
			require.Equal(t, tt.expected, ctx.String())
		})
	}
}

func TestShowProfileStmtMetadata(t *testing.T) {
	stmt := NewShowProfileStmt(0, nil)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Show Profile", stmt.GetStatementType())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
}

func TestShowProfileEdgeCases(t *testing.T) {
	// TeSt: ForQuery = 0 means no FOR QUERY clause
	t.Run("ForQuery zero means omitted", func(t *testing.T) {
		stmt := NewShowProfileStmt(0, nil)
		ctx := NewFmtCtx(dialect.MYSQL)
		stmt.Format(ctx)
		require.Equal(t, "show profile", ctx.String())
		require.Equal(t, int64(0), stmt.ForQuery)
	})

	// Test: ForQuery negative (edge case - should not happen from parser but check behavior)
	t.Run("ForQuery negative", func(t *testing.T) {
		stmt := NewShowProfileStmt(-1, nil)
		ctx := NewFmtCtx(dialect.MYSQL)
		stmt.Format(ctx)
		// ForQuery > 0 check means negative values are not printed
		require.Equal(t, "show profile", ctx.String())
	})
}
