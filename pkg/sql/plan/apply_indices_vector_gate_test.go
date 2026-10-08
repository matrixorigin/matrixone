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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexSupportsContext(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	tableDef := makeFullTextJoinTestTableDef("ft", true)
	match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, []*planpb.Expr{match}), ctx)
	matchScan := builder.qry.Nodes[scanID]
	plainScan := builder.qry.Nodes[builder.appendNode(makeFullTextJoinTestScan(tableDef, builder.genNewBindTag(), nil), ctx)]

	ivfflat := catalog.MoIndexIvfFlatAlgo.ToString()
	hnsw := catalog.MoIndexHnswAlgo.ToString()
	for _, c := range []struct {
		name string
		ctx  *vectorSortContext
		algo string
		want bool
	}{
		{"no context", nil, hnsw, true},
		{"plain scan", &vectorSortContext{scanNode: plainScan}, hnsw, true},
		{"MATCH filter, hnsw", &vectorSortContext{scanNode: matchScan}, hnsw, false},
		{"MATCH filter, ivfflat", &vectorSortContext{scanNode: matchScan}, ivfflat, true},
		{"membership, hnsw", &vectorSortContext{scanNode: plainScan, hasMembership: true}, hnsw, false},
		{"membership, ivfflat", &vectorSortContext{scanNode: plainScan, hasMembership: true}, ivfflat, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, builder.vectorIndexSupportsContext(c.ctx, c.algo))
		})
	}
}
