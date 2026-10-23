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

package dependent

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRecursiveMergeCTEUsesVisibleEqualityKeys(t *testing.T) {
	key := &plan.Expr{Typ: plan.Type{Id: 23}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	node := &plan.Node{
		SourceStep:              []int32{0, 1},
		RecursiveUnionDistinct:  true,
		PhysicalEqualityKeyList: []*plan.Expr{key},
	}

	arg := recursiveMergeCTEArgument(node)
	require.True(t, arg.Distinct)
	require.Equal(t, 1, arg.NodeCnt)
	require.Len(t, arg.KeyExprs, 1)
	require.Same(t, key, arg.KeyExprs[0],
		"recursive dedup must hash only the SQL equality columns, not BOOL payload passengers")
	arg.Release()

	unionAll := recursiveMergeCTEArgument(&plan.Node{SourceStep: []int32{0, 1}})
	require.False(t, unionAll.Distinct)
	require.Empty(t, unionAll.KeyExprs)
	unionAll.Release()
}
