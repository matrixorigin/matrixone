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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
