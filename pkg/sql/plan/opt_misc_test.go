// Copyright 2023 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRemapWindowClause(t *testing.T) {
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_timestamp)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 3,
				ColPos: 3,
				Name:   "test",
			},
		},
	}

	f := &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(1, "n"),
				Args: []*Expr{expr},
			},
		},
		Typ: plan.Type{},
	}
	colMap := make(map[[2]int32][2]int32)
	var b *QueryBuilder = &QueryBuilder{
		compCtx: &MockCompilerContext{
			ctx: context.Background(),
		},
		optimizationHistory: []string{"test optimization history"},
	}
	err := b.remapWindowClause(f, 1, 1, colMap, nil)
	t.Log(err)
	require.Error(t, err)
}
