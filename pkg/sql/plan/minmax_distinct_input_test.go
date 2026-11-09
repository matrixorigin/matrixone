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

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestMinMaxDistinctPreservesInput(t *testing.T) {
	const value = "coalesce(cast(n_name as char(8)), cast(n_comment as varchar(8)))"
	for _, name := range []string{"min", "max"} {
		t.Run(name, func(t *testing.T) {
			args := make([]*planpb.Expr, 0, 2)
			for _, distinct := range []string{"", "distinct "} {
				sql := "select " + name + "(" + distinct + value + ") from nation"
				p, err := runOneStmt(NewMockOptimizer(true), t, sql)
				require.NoError(t, err)
				var agg *planpb.Expr
				for _, node := range p.GetQuery().Nodes {
					if node.NodeType == planpb.Node_AGG && len(node.AggList) == 1 {
						agg = node.AggList[0]
						break
					}
				}
				require.NotNil(t, agg, sql)
				f := agg.GetF()
				require.NotNil(t, f, sql)
				require.Zero(t, uint64(f.Func.Obj)&function.Distinct, sql)
				require.Len(t, f.Args, 1, sql)
				require.NotNil(t, f.Args[0].GetF(), sql)
				require.Equal(t, "coalesce", f.Args[0].GetF().Func.ObjName, sql)
				args = append(args, f.Args[0])
			}
			require.True(t, proto.Equal(args[0], args[1]), "DISTINCT must not change the input")
		})
	}
}
