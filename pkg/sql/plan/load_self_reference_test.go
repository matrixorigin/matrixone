// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadSelfReferenceChecks(t *testing.T) {
	for _, tc := range []struct {
		name, sql     string
		ordered, auto bool
	}{
		{"ordinary", "load data inline format='csv', data='1,999,x' into table self_ref fields terminated by ','", false, false},
		{"ignore", "load data inline format='csv', data='1,999,x' ignore into table self_ref fields terminated by ','", true, false},
		{"insert_ignore", "insert ignore into self_ref values (1,1,'root'),(2,1,'child')", true, false},
		{"auto_ignore", "insert ignore into self_ref values (1,1,'root'),(2,1,'child')", true, true},
		{"disabled", "load data inline format='csv', data='1,999,x' ignore into table self_ref fields terminated by ','", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			mock.ctxt.tables["self_ref"].Cols[0].Typ.AutoIncr = tc.auto
			if tc.name == "disabled" {
				mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
					if name == "foreign_key_checks" {
						return int64(0), nil
					}
					if name == "sql_mode" {
						return "", nil
					}
					return nil, nil
				}
			}
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			require.True(t, query.HasForeignKeyAction)
			if tc.name == "disabled" {
				require.Empty(t, query.DetectSqls)
				for _, node := range query.Nodes {
					require.Empty(t, node.GetPreInsertUkCtx().GetOrderedForeignKeys())
				}
				return
			}
			if !tc.ordered {
				require.NotEmpty(t, query.DetectSqls)
				return
			}
			require.Empty(t, query.DetectSqls)
			found := false
			for _, node := range query.Nodes {
				ctx := node.GetPreInsertUkCtx()
				fks := ctx.GetOrderedForeignKeys()
				if spec := ctx.GetOrderedAutoIncrement(); spec != nil {
					fks = spec.ForeignKeys
				}
				if len(fks) == 0 {
					continue
				}
				found = true
				require.True(t, ctx.InsertIgnoreMultiDedup)
				require.Len(t, fks, 1)
				fk := fks[0]
				require.Len(t, fk.SelfParentColumns, 1)
				child := query.Nodes[node.Children[0]]
				require.GreaterOrEqual(t, fk.ChildColumns[0].ColPos, int32(0))
				require.Less(t, int(fk.ChildColumns[0].ColPos), len(child.ProjectList))
				require.Less(t, int(fk.SelfParentColumns[0].ColPos), len(child.ProjectList))
				copied := DeepCopyPreInsertUkCtx(ctx)
				require.True(t, proto.Equal(ctx, copied))
				wire, err := ctx.Marshal()
				require.NoError(t, err)
				decoded := &plan.PreInsertUkCtx{}
				require.NoError(t, decoded.Unmarshal(wire))
				require.True(t, proto.Equal(ctx, decoded))
				if !tc.auto {
					require.NotSame(t, fk, copied.OrderedForeignKeys[0])
					require.Nil(t, ctx.OrderedAutoIncrement)
				}
			}
			require.True(t, found)
		})
	}
}
