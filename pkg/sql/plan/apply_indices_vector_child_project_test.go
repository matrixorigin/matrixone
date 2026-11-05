// Copyright 2024 Matrix Origin
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
	"context"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// A vector Top-K over PROJECTs between its sort and the scan reads the distance in the scan's
// columns and remaps the PROJECTs' columns when the index replaces them.
func TestVectorTopKOverChildProject(t *testing.T) {
	queries := []string{
		"select id from ft where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2",
		"select id from (select * from ft) d where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2",
		"with d as (select * from ft) select id from d where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2",
		"select id from (select v, id, title, body from ft) d where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2",
		"select id from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, r from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, l2_distance(v,'[1,2,3]') dist from (select * from ft) d where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2",
		"with d as (select * from ft) select id, l2_distance(v,'[1,2,3]') dist from d where match(title,body) against('hello') order by dist limit 2",
		"select group_concat(id) from (select id, l2_distance(v,'[1,2,3]') dist from (select * from ft) d where match(title,body) against('hello') order by l2_distance(v,'[1,2,3]') limit 2) x",
		"select id, l2_distance(v,'[1,2,3]')*2 x from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, round(l2_distance(v,'[1,2,3]'),3) x from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2 offset 1",
	}
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		for _, ft2 := range []bool{false, true} {
			for _, sql := range queries {
				query := buildBooleanFullTextPublicPlan(t, sql, ft2, false, algo)
				searches := 0
				for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
					if n.NodeType == planpb.Node_INDEX_SEARCH_SCAN && n.IndexSearchScan.Index.IndexAlgo == algo {
						searches++
					}
				}
				require.Equal(t, 1, searches, "%s ft2=%v: %s", algo, ft2, sql)
			}
		}
	}
}

// The pagination of a derived table below the Top-K bounds the rows it ranks, so it is kept.
func TestVectorTopKKeepsDerivedTablePagination(t *testing.T) {
	queries := []string{
		"select id, r from (select id, v, rand() r from ft limit 1 offset 1) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, r from (select v, id, rand() r from ft limit 1 offset 1) d order by l2_distance(v,'[1,2,3]') limit 2 by rank with option 'mode=post'",
		"select id from (select id, v from ft limit 1 offset 1) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id from (select v, id from ft limit 1 offset 1) d order by l2_distance(v,'[1,2,3]') limit 2",
	}
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		for _, sql := range queries {
			for _, hints := range []string{"", "pushDownLimitToScan=1"} {
				cc := newFullTextJoinMockCompilerContext(t)
				cc.SetContext(context.WithValue(cc.GetContext(), defines.OptimizerHints{}, hints))
				query, _ := buildBooleanFullTextPublicPlanIn(t, cc, sql, false, false, algo)
				inner := false
				for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
					if n.NodeType == planpb.Node_INDEX_SEARCH_SCAN {
						t.Fatalf("%s %s: index search over a paginated derived table: %s", algo, hints, sql)
					}
					if limit, ok := getLiteralUint64(n.Limit); ok && limit == 1 {
						offset, ok := getLiteralUint64(n.Offset)
						inner = inner || (ok && offset == 1)
					}
				}
				require.True(t, inner, "%s %s: inner LIMIT 1 OFFSET 1 lost: %s", algo, hints, sql)
			}
		}
	}
}

// Merging the PROJECTs below a Top-K does not copy a computed column into each of its references.
func TestVectorTopKMergeKeepsSharedComputedColumns(t *testing.T) {
	exprSize := func(query *planpb.Query) int {
		n := 0
		var walk func(*planpb.Expr)
		walk = func(e *planpb.Expr) {
			if e == nil {
				return
			}
			n++
			if f := e.GetF(); f != nil {
				for _, arg := range f.Args {
					walk(arg)
				}
			}
		}
		for _, node := range query.Nodes {
			for _, e := range node.ProjectList {
				walk(e)
			}
		}
		return n
	}
	nested := func(depth int) string {
		sql := "select id, v, id x from ft"
		for i := 0; i < depth; i++ {
			sql = "select id, v, x+x as x from (" + sql + ") d" + strconv.Itoa(i)
		}
		return "select id, x, l2_distance(v,'[1,2,3]') dist from (" + sql + ") d order by l2_distance(v,'[1,2,3]') limit 2"
	}
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		shallow := exprSize(buildBooleanFullTextPublicPlan(t, nested(4), false, false, algo))
		deep := exprSize(buildBooleanFullTextPublicPlan(t, nested(12), false, false, algo))
		require.Less(t, deep, 4*shallow, "%s: %d expression nodes at depth 12, %d at depth 4", algo, deep, shallow)
	}
}

// A volatile column of the derived table below a Top-K is evaluated once however often it is read.
func TestVectorTopKKeepsSharedVolatileColumns(t *testing.T) {
	queries := []string{
		"select id, r as a, r as b from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, r - r from (select id, v, rand() r from ft) d order by l2_distance(v,'[1,2,3]') limit 2",
		"select id, r as a, r as b from (select id, v, rand() r, l2_distance(v,'[1,2,3]') dist from ft) d order by dist limit 2",
	}
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		for _, sql := range queries {
			query := buildBooleanFullTextPublicPlan(t, sql, false, false, algo)
			rands := 0
			var walk func(*planpb.Expr)
			walk = func(e *planpb.Expr) {
				if f := e.GetF(); f != nil {
					if f.Func.ObjName == "rand" {
						rands++
					}
					for _, arg := range f.Args {
						walk(arg)
					}
				}
			}
			for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
				for _, e := range n.ProjectList {
					walk(e)
				}
			}
			require.Equal(t, 1, rands, "%s: %s", algo, sql)
		}
	}
}
