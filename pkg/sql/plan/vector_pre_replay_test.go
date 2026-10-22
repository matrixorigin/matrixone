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

// pre and iterative with a sequence or volatile filter run the exact sort on every
// vector index: the filter is evaluated by one scan, and no index search is planned.
func TestVectorPreVolatileFilterRunsOnce(t *testing.T) {
	for _, idx := range []struct {
		algo  string
		roles []string
	}{
		{catalog.MoIndexIvfFlatAlgo.ToString(), nil},
		{catalog.MoIndexHnswAlgo.ToString(), []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage}},
		{catalog.MoIndexCagraAlgo.ToString(), []string{catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage}},
		{catalog.MoIndexIvfpqAlgo.ToString(), []string{catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage}},
	} {
		for _, mode := range []string{"pre", "iterative"} {
			for _, tc := range []struct {
				name, filter string
				searches     int
				volatile     int
			}{
				{"scalar", "tag = 2", 1, 0},
				{"rand", "rand() < 2", 0, 1},
				{"rand and scalar", "tag = 2 and rand() < 2", 0, 1},
				{"nextval", "nextval('s') = '1'", 0, 1},
			} {
				t.Run(idx.algo+"/"+mode+"/"+tc.name, func(t *testing.T) {
					ctx := newIvfFulltextMembershipTestContext(t, false)
					if idx.roles != nil {
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
					}

					qry := buildIvfFulltextMembershipTestPlan(t, ctx,
						"select id from docs where "+tc.filter+
							" order by l2_distance(v,'[0,0]') limit 7 by rank with option 'mode="+mode+"'", false)
					searches, volatile := 0, 0
					var visit func(int32)
					visit = func(id int32) {
						n := qry.Nodes[id]
						if n.IndexSearchScan != nil && n.IndexSearchScan.Index.IndexAlgo == idx.algo {
							searches++
						}
						for _, exprs := range [][]*planpb.Expr{n.FilterList, n.OnList, n.BlockFilterList} {
							for _, expr := range exprs {
								if ContainsVolatileFunction(expr) || ContainsSequenceFunction(expr) {
									volatile++
								}
							}
						}
						for _, child := range n.Children {
							visit(child)
						}
					}
					for _, root := range qry.Steps {
						visit(root)
					}
					require.Equal(t, tc.searches, searches)
					require.Equal(t, tc.volatile, volatile)
				})
			}
		}
	}
}
