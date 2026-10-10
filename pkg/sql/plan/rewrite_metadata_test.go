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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestRewriteVisibleColumns(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		expected  map[string]struct{}
		all       bool
		supported bool
	}{
		{
			name:      "projection",
			sql:       "select id, amount from db.controlled where tenant = 1",
			expected:  map[string]struct{}{"id": {}, "amount": {}},
			supported: true,
		},
		{
			name:      "star",
			sql:       "select * from db.controlled where tenant = 1",
			all:       true,
			supported: true,
		},
		{
			name: "computed expression fails closed",
			sql:  "select id + 1 from db.controlled",
		},
		{
			name: "join fails closed",
			sql:  "select controlled.id from db.controlled join db.other on controlled.id = other.id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := parseRewriteSelect(t, test.sql)
			names, all, supported := rewriteVisibleColumns(stmt, "db", "controlled")
			require.Equal(t, test.expected, names)
			require.Equal(t, test.all, all)
			require.Equal(t, test.supported, supported)
		})
	}
}

func TestRewriteVisibleColumnsChainIntersects(t *testing.T) {
	inner := parseRewriteSelect(t, "select id, tenant, amount from db.controlled")
	outer := parseRewriteSelect(t, "select id, amount from db.controlled")
	left, leftAll, leftOK := rewriteVisibleColumns(inner, "db", "controlled")
	right, rightAll, rightOK := rewriteVisibleColumns(outer, "db", "controlled")
	require.True(t, leftOK && rightOK)
	require.False(t, leftAll || rightAll)
	require.Equal(t, map[string]struct{}{"id": {}, "amount": {}}, intersectRewriteColumnNames(left, right))
}

func TestApplyRewriteMetadataVisibilityFailClosed(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, true, false)
	ctx := NewBindContext(builder, nil)
	ctx.projectTag = 7
	ctx.headings = []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"}
	textType := planpb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}
	ctx.results = []*planpb.Expr{
		{Typ: textType},
		{Typ: textType},
		{Typ: textType},
	}
	ctx.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
		"db.controlled": {{DbName: "db", TableName: "controlled", Stmt: parseRewriteSelect(t, "select id + 1 from db.controlled")}},
	}}
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, ctx)
	filtered, err := builder.applyRewriteMetadataVisibility(child, ctx, INFORMATION_SCHEMA, informationSchemaColumnsTable)
	require.NoError(t, err)
	require.NotEqual(t, child, filtered)
	require.Equal(t, planpb.Node_FILTER, builder.qry.Nodes[filtered].NodeType)
	require.Len(t, builder.qry.Nodes[filtered].FilterList, 1)
	require.True(t, builder.qry.Nodes[filtered].NotCacheable)
}

func parseRewriteSelect(t *testing.T, sql string) *tree.Select {
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	selectStmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)
	return selectStmt
}
