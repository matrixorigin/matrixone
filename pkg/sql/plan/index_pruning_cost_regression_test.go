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

func TestRegularIndexCoveringCostWithoutBlockPruning(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		filter := makeStringEqFilterExpr(0, 2, "READY")
		if prefix {
			filter.GetF().Func.ObjName = "prefix_eq"
		}
		filter.Selectivity = 1.0 / 100_000
		builder, scanID, index, refs := newEncodedIndexCostTestCase(t,
			[]string{"category", catalog.CreateAlias(catalog.CPrimaryKeyColName)},
			[]*planpb.Expr{filter},
			&planpb.Stats{TableCnt: 100_000, Outcnt: 1, Cost: 100_000},
			map[int32]int{2: 1}, false)
		node := builder.qry.Nodes[scanID]
		filter.GetF().Args[0].GetCol().RelPos = node.BindingTags[0]
		ctx := builder.newEncodedRegularIndexCostContext(node, refs)
		normalWork, normalReject, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.False(t, normalReject)
		builder.optimizerHints = &OptimizerHints{blockFilter: 2}
		ctx = builder.newEncodedRegularIndexCostContext(node, refs)
		work, reject, valid := ctx.score(index, []int32{0}, nil, encodedRegularIndexCostIndexOnly)
		require.True(t, valid)
		require.Equal(t, prefix, reject, "the additional admission guard applies only to new prefix access")
		require.Equal(t, normalWork, work, "all candidates retain the shared ranking model")
	}
}
