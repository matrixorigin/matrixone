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

package plan

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSelectPlanStructure(t *testing.T) {
	assertColumn := func(t *testing.T, expr *Expr, name string, typ types.T) {
		t.Helper()
		require.NotNil(t, expr)
		require.NotNil(t, expr.GetCol())
		require.True(t, strings.HasSuffix(expr.GetCol().Name, "."+name), expr.GetCol().Name)
		require.Equal(t, int32(typ), expr.Typ.Id)
	}
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct{ name, sql string }{
		{"single_column", "SELECT l_orderkey FROM lineitem"},
		{"repeated_column", "SELECT l_orderkey, l_orderkey FROM lineitem"},
		{"extra_columns", "SELECT l_extendedprice, l_orderkey, l_discount, l_orderkey, l_linenumber FROM lineitem"},
		{"strict_columns", "SELECT l_orderkey, l_extendedprice FROM lineitem"},
		{"grouped_column", "SELECT l_orderkey, 2 FROM lineitem GROUP BY l_orderkey"},
		{"literal", "SELECT l_orderkey, 2 FROM lineitem"},
		{"expression", "SELECT l_orderkey, 1 + l_orderkey FROM lineitem"},
		{"join", "SELECT o.o_orderkey FROM orders o, lineitem l WHERE l.l_orderkey = o.o_orderkey"},
		{"self_join", "SELECT l.o_orderkey FROM orders l, orders r WHERE l.o_orderkey = r.o_orderkey"},
		{"count", "SELECT COUNT(n_nationkey) FROM nation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			query := built.GetQuery()
			require.NotNil(t, query)
			require.Equal(t, pbplan.Query_SELECT, query.StmtType)
			require.Len(t, query.Steps, 1)
			var scans, joins, aggregates []*Node
			// The shared walker validates every reachable node/child index and nil node.
			require.NoError(t, pbplan.WalkQueryNodes(query, func(_ int32, node *Node) error {
				switch node.NodeType {
				case pbplan.Node_TABLE_SCAN:
					scans = append(scans, node)
				case pbplan.Node_JOIN:
					joins = append(joins, node)
				case pbplan.Node_AGG:
					aggregates = append(aggregates, node)
				}
				return nil
			}))
			root := query.Nodes[query.Steps[0]]
			require.Equal(t, pbplan.Node_PROJECT, root.NodeType)
			require.Len(t, root.Children, 1)
			require.NotEmpty(t, root.ProjectList)
			child := query.Nodes[root.Children[0]]
			for _, scan := range scans {
				require.NotNil(t, scan.TableDef)
				for _, col := range scan.TableDef.Cols {
					require.NotNil(t, col)
				}
			}
			if tc.name != "join" && tc.name != "self_join" {
				require.Len(t, scans, 1)
				key, table := "l_orderkey", "lineitem"
				if tc.name == "count" {
					key, table = "n_nationkey", "nation"
				}
				require.Equal(t, table, scans[0].TableDef.Name)
				columns := make([]string, 0, len(scans[0].TableDef.Cols))
				for _, col := range scans[0].TableDef.Cols {
					columns = append(columns, col.Name)
				}
				require.Contains(t, columns, key)
			}
			switch tc.name {
			case "single_column", "repeated_column", "extra_columns", "strict_columns", "literal", "expression":
				require.Same(t, scans[0], child)
			}
			switch tc.name {
			case "single_column", "grouped_column":
				assertColumn(t, root.ProjectList[0], "l_orderkey", types.T_int64)
			case "repeated_column":
				require.Len(t, root.ProjectList, 2)
				for _, expr := range root.ProjectList {
					assertColumn(t, expr, "l_orderkey", types.T_int64)
				}
			case "extra_columns":
				var found bool
				for _, expr := range root.ProjectList {
					require.NotNil(t, expr)
					if col := expr.GetCol(); col != nil && strings.HasSuffix(col.Name, ".l_orderkey") {
						found = true
					}
				}
				require.True(t, found)
			case "strict_columns":
				require.Len(t, root.ProjectList, 2)
				assertColumn(t, root.ProjectList[0], "l_orderkey", types.T_int64)
				assertColumn(t, root.ProjectList[1], "l_extendedprice", types.T_decimal64)
				require.Len(t, scans[0].TableDef.Cols, 2)
				columns := make(map[string]int32)
				for _, col := range scans[0].TableDef.Cols {
					columns[col.Name] = col.Typ.Id
				}
				wantColumns := map[string]int32{
					"l_orderkey": int32(types.T_int64), "l_extendedprice": int32(types.T_decimal64),
				}
				require.Equal(t, wantColumns, columns)
				require.Len(t, scans[0].ProjectList, 2)
				projection := make(map[string]int32, len(scans[0].ProjectList))
				for _, expr := range scans[0].ProjectList {
					require.NotNil(t, expr)
					require.NotNil(t, expr.GetCol())
					name := expr.GetCol().Name
					projection[name[strings.LastIndexByte(name, '.')+1:]] = expr.Typ.Id
				}
				require.Equal(t, wantColumns, projection)
			case "literal":
				require.Len(t, root.ProjectList, 2)
				require.NotNil(t, root.ProjectList[1].GetLit())
				require.Equal(t, int64(2), root.ProjectList[1].GetLit().GetI64Val())
			case "expression":
				require.Len(t, root.ProjectList, 2)
				assertColumn(t, root.ProjectList[0], "l_orderkey", types.T_int64)
				expr := root.ProjectList[1]
				require.NotNil(t, expr)
				require.Equal(t, int32(types.T_int64), expr.Typ.Id)
				fn := expr.GetF()
				require.NotNil(t, fn)
				require.Equal(t, "+", fn.GetFunc().GetObjName())
				require.Len(t, fn.Args, 2)
				require.NotNil(t, fn.Args[0].GetLit())
				require.Equal(t, int64(1), fn.Args[0].GetLit().GetI64Val())
				assertColumn(t, fn.Args[1], "l_orderkey", types.T_int64)
			case "join", "self_join":
				require.Len(t, scans, 2)
				require.Len(t, joins, 1)
				join := joins[0]
				require.Equal(t, pbplan.Node_INNER, join.JoinType)
				require.Empty(t, join.FilterList)
				require.Len(t, join.Children, 2)
				tables := make([]string, 0, len(join.Children))
				for _, id := range join.Children {
					scan := query.Nodes[id]
					require.Equal(t, pbplan.Node_TABLE_SCAN, scan.NodeType)
					require.NotNil(t, scan.TableDef)
					key := "o_orderkey"
					if scan.TableDef.Name == "lineitem" {
						key = "l_orderkey"
					}
					found := false
					for _, col := range scan.TableDef.Cols {
						if col.Name == key {
							found = true
							break
						}
					}
					require.True(t, found, "join scan %d (%s) missing %s", id, scan.TableDef.Name, key)
					tables = append(tables, scan.TableDef.Name)
				}
				wantTables, wantColumns := []string{"lineitem", "orders"}, []string{"l_orderkey", "o_orderkey"}
				if tc.name == "self_join" {
					wantTables, wantColumns = []string{"orders", "orders"}, []string{"o_orderkey", "o_orderkey"}
				}
				require.ElementsMatch(t, wantTables, tables)
				require.Len(t, join.OnList, 1)
				equality := join.OnList[0].GetF()
				require.NotNil(t, equality)
				require.Equal(t, "=", equality.GetFunc().GetObjName())
				require.Len(t, equality.Args, 2)
				columns := make([]string, 0, len(equality.Args))
				for _, arg := range equality.Args {
					require.NotNil(t, arg.GetCol())
					name := arg.GetCol().Name
					columns = append(columns, name[strings.LastIndexByte(name, '.')+1:])
				}
				require.ElementsMatch(t, wantColumns, columns)
			case "count":
				require.Len(t, aggregates, 1)
				require.Same(t, aggregates[0], child)
				require.Len(t, child.Children, 1)
				require.Same(t, scans[0], query.Nodes[child.Children[0]])
				require.Len(t, child.AggList, 1)
				count := child.AggList[0].GetF()
				require.NotNil(t, count)
				require.Contains(t, []string{"count", "starcount"}, count.GetFunc().GetObjName())
				require.Len(t, count.Args, 1)
				assertColumn(t, count.Args[0], "n_nationkey", types.T_int32)
				require.Len(t, root.ProjectList, 1)
				require.NotNil(t, root.ProjectList[0].GetCol())
				name := strings.ToLower(root.ProjectList[0].GetCol().Name)
				require.True(t, strings.HasPrefix(name, "count(") || strings.HasPrefix(name, "starcount("), name)
			default:
				t.Fatalf("missing structural oracle for %q", tc.name)
			}
		})
	}
}
