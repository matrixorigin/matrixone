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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The PRE plan of hnsw, cagra and ivfpq runs a filter-only MATCH once, on the
// membership producer, as ivfflat does.
func TestVectorPreFulltextMembershipSearchesOnce(t *testing.T) {
	for _, idx := range []struct {
		algo  string
		roles []string
	}{
		{catalog.MoIndexHnswAlgo.ToString(), []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage}},
		{catalog.MoIndexCagraAlgo.ToString(), []string{catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage}},
		{catalog.MoIndexIvfpqAlgo.ToString(), []string{catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage}},
	} {
		for _, tc := range []struct {
			name, query string
			searches    int
		}{
			{"filter only", "select id from docs where match(body) against('needle')", 1},
			{"scalar", "select id from docs where match(body) against('needle') and tag = 2", 1},
			{"projected score", "select id, match(body) against('needle') from docs where match(body) against('needle')", 2},
		} {
			t.Run(idx.algo+"/"+tc.name, func(t *testing.T) {
				ctx := newIvfFulltextMembershipTestContext(t, false)
				table := ctx.tables["docs"]
				indexes := table.Indexes[:0]
				for _, def := range table.Indexes {
					if def.IndexAlgo != catalog.MoIndexIvfFlatAlgo.ToString() {
						indexes = append(indexes, def)
					}
				}
				for _, role := range idx.roles {
					def := &planpb.IndexDef{
						IndexName: "idx_v", IndexAlgo: idx.algo, IndexAlgoTableType: role,
						IndexTableName: idx.algo + "_" + role, Parts: []string{"v"},
						IndexAlgoParams: `{"op_type":"vector_l2_ops"}`, TableExist: true,
					}
					indexes = append(indexes, def)
					ctx.tables[def.IndexTableName] = &planpb.TableDef{Name: def.IndexTableName, TblId: 503}
					ctx.objects[def.IndexTableName] = &planpb.ObjectRef{Obj: 503, SchemaName: "test", ObjName: def.IndexTableName}
				}
				table.Indexes = indexes

				qry := buildIvfFulltextMembershipTestPlan(t, ctx,
					tc.query+" order by l2_distance(v,'[0,0]') limit 7 by rank with option 'mode=pre'", false)
				fulltext, vector, membership := 0, 0, 0
				var visit func(int32)
				visit = func(id int32) {
					n := qry.Nodes[id]
					if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_SEMI {
						membership++
					}
					if n.IndexSearchScan != nil {
						switch n.IndexSearchScan.Index.IndexAlgo {
						case catalog.MOIndexFullTextAlgo.ToString():
							fulltext++
						case idx.algo:
							vector++
						}
					}
					for _, child := range n.Children {
						visit(child)
					}
				}
				for _, root := range qry.Steps {
					visit(root)
				}
				require.Equal(t, 1, vector)
				require.Positive(t, membership)
				require.Equal(t, tc.searches, fulltext)
			})
		}
	}
}
