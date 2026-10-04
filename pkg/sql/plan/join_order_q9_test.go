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
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestQ9JoinsFilteredBranchBeforeOrders(t *testing.T) {
	sql, err := os.ReadFile("tpch/q9.sql")
	require.NoError(t, err)
	for _, scale := range []float64{1, 100, 1000} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			cache := NewStatsCache()
			rows := map[string]float64{"nation": 25, "part": 200000 * scale, "supplier": 10000 * scale, "partsupp": 800000 * scale, "orders": 1500000 * scale, "lineitem": 6001215 * scale}
			for name, count := range rows {
				table := mock.tables[name]
				require.NotNil(t, table)
				stats := NewStatsInfo()
				stats.TableName, stats.TableCnt = name, count
				stats.AccurateObjectNumber = 1
				stats.BlockNumber = int64(count/8192) + 1
				for _, col := range table.Cols {
					ndv := count
					switch {
					case strings.HasSuffix(col.Name, "partkey"):
						ndv = rows["part"]
					case strings.HasSuffix(col.Name, "suppkey"):
						ndv = rows["supplier"]
					case strings.HasSuffix(col.Name, "orderkey"):
						ndv = rows["orders"]
					case strings.HasSuffix(col.Name, "nationkey"):
						ndv = 25
					}
					stats.NdvMap[col.Name] = ndv
					stats.MinValMap[col.Name], stats.MaxValMap[col.Name] = 1, ndv
					stats.DataTypeMap[col.Name] = uint64(col.Typ.Id)
					stats.SizeMap[col.Name] = uint64(count) * 8
				}
				cache.Set(table.TblId, stats)
			}
			ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{MockCompilerContext: mock, statsCache: cache}}
			stmts, err := mysql.Parse(ctx.GetContext(), string(sql), 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := NewBaseOptimizer(ctx).Optimize(stmts[0], false)
			require.NoError(t, err)
			var tables func(int32) []string
			tables = func(id int32) []string {
				n := query.Nodes[id]
				if n.NodeType == planpb.Node_TABLE_SCAN {
					return []string{n.TableDef.Name}
				}
				var names []string
				for _, c := range n.Children {
					names = append(names, tables(c)...)
				}
				sort.Strings(names)
				return names
			}
			joins := 0
			filteredLineitem := false
			var visit func(int32)
			visit = func(id int32) {
				n := query.Nodes[id]
				if n.NodeType == planpb.Node_JOIN {
					joins++
					names := tables(id)
					require.NotEqual(t, []string{"lineitem", "orders"}, names, "do not join the two unfiltered large tables first")
					require.NotEmpty(t, n.OnList, "Q9's connected joins must retain their predicates")
					if slices.Contains(names, "orders") {
						require.Len(t, names, 6, "orders must join after the selective branch reaches lineitem")
					}
					if strings.Join(names, ",") == "lineitem,nation,part,partsupp,supplier" {
						filteredLineitem = true
						require.Equal(t, []string{"lineitem"}, tables(n.Children[0]))
						require.Equal(t, []string{"nation", "part", "partsupp", "supplier"}, tables(n.Children[1]))
						require.LessOrEqual(t, query.Nodes[n.Children[1]].Stats.Outcnt, rows["partsupp"]*.2)
					}
				}
				for _, child := range n.Children {
					visit(child)
				}
			}
			for _, root := range query.Steps {
				visit(root)
			}
			require.Equal(t, 5, joins)
			require.True(t, filteredLineitem, "the filtered branch must join lineitem before orders")

		})
	}
}
