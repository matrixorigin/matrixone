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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestJSONStringConsumersBindJSONDirectly(t *testing.T) {
	for _, name := range []string{"md5", "sha", "sha1", "sha2", "compress", "quote", "soundex"} {
		t.Run(name, func(t *testing.T) {
			json := &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_json)},
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
			}
			args := []*planpb.Expr{json}
			if name == "sha2" {
				args = append(args, MakePlan2Int64ConstExprWithType(256))
			}

			bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err)
			require.NotNil(t, bound.GetF())
			require.Equal(t, json, bound.GetF().GetArgs()[0])
		})
	}
}
