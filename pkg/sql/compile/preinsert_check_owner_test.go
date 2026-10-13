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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileDeferredPreInsertKeepsParallelSelection(t *testing.T) {
	for _, tc := range []struct {
		name            string
		scopes, workers int
		check, merged   bool
	}{
		{"one_worker", 1, 1, true, false},
		{"parallel_workers", 1, 4, true, false},
		{"multiple_scopes", 2, 1, true, false},
		{"ordinary_preinsert", 1, 4, false, false},
		{"empty_input", 0, 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			nodes := []*plan.Node{{}, {Children: []int32{0}, PreInsertCtx: &plan.PreInsertCtx{
				Ref: &plan.ObjectRef{}, HasAutoCol: true, TrackAutoIncrementGenerated: true, DeferAutoIncrement: true,
				AutoIncrementGeneratedColumn: 2,
				TableDef: &plan.TableDef{TblId: 44, IsTemporary: true,
					Cols: []*plan.ColDef{
						{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), AutoIncr: true}},
						{Name: "v", Typ: plan.Type{Id: int32(types.T_int64)}},
					}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
			}}}
			if tc.check {
				nodes[1].PreInsertCtx.CheckEligibility = &plan.ColRef{ColPos: 2}
			}
			c.anal = &AnalyzeModule{qry: &plan.Query{Nodes: nodes}}
			var inputs []*Scope
			for i := 0; i < tc.scopes; i++ {
				s := newScope(Merge)
				s.NodeInfo = engine.Node{Addr: "127.0.0.1:18000", Mcpu: tc.workers}
				s.Proc = c.proc.NewNoContextChildProc(0)
				s.setRootOperator(colexec.NewMockOperator())
				inputs = append(inputs, s)
			}
			result, err := c.compilePreInsert(nodes, nodes[1], inputs)
			t.Cleanup(func() {
				for _, scope := range result {
					scope.FreeOperator(c)
					scope.release()
				}
			})
			require.NoError(t, err)
			if tc.scopes == 0 {
				require.Empty(t, result)
				return
			}
			if tc.merged {
				require.Len(t, result, 1)
				require.Equal(t, 1, result[0].NodeInfo.Mcpu)
				require.NotSame(t, inputs[0], result[0])
				require.Equal(t, inputs, result[0].PreScopes)
			} else {
				require.Equal(t, inputs, result)
			}
			for _, s := range result {
				op, ok := s.RootOp.(*preinsert.PreInsert)
				require.True(t, ok)
				require.Equal(t, nodes[1].PreInsertCtx.CheckEligibility, op.CheckEligibility)
				require.True(t, op.DeferAutoIncrement)
			}
		})
	}
}
