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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMakeCastExprKeepsDirectParallelLoadVector(t *testing.T) {
	vectorType := plan.Type{Id: int32(types.T_array_float32), Width: 3}
	original := &plan.Expr{
		Typ:  vectorType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "v"}},
	}
	node := &plan.Node{ProjectList: []*plan.Expr{original}}
	tableDef := &plan.TableDef{Cols: []*plan.ColDef{{Name: "v", Typ: vectorType}}}

	result := makeCastExpr(&tree.Load{}, "vectors.csv", tableDef, node)
	require.Len(t, result, 1)
	require.Same(t, original, result[0])
}
