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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestCompileMultiUpdateAnalyzeOwnership(t *testing.T) {
	for _, tc := range []struct {
		name        string
		writeS3     bool
		scopes, dop int
		notFirst    bool
	}{
		{"single-writer", true, 1, 1, false},
		{"existing-input-boundary", true, 1, 1, true},
		{"parallel-writers", true, 1, 2, false},
		{"multiple-scopes", true, 2, 1, false},
		{"direct-write", false, 1, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			defer c.proc.Free()
			c.ncpu = 1
			c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: !tc.notFirst, qry: &plan.Query{StmtType: plan.Query_INSERT, LoadWriteS3: tc.writeS3}}
			sources := make([]*Scope, tc.scopes)
			for i := range sources {
				sources[i] = newScope(Merge)
				sources[i].Proc = c.proc.NewNoContextChildProc(0)
				sources[i].NodeInfo = getEngineNode(c)
				sources[i].NodeInfo.Mcpu = tc.dop
				scan := external.NewArgument()
				scan.SetAnalyzeControl(0, true)
				scan.IsLast = true
				sources[i].setRootOperator(scan)
			}
			result, err := c.compileMultiUpdate(&plan.Node{NodeType: plan.Node_MULTI_UPDATE, Stats: &plan.Stats{}}, sources)
			require.NoError(t, err)
			defer ReleaseScopes(result)
			require.Len(t, result, 1)
			// The final DML output boundary must never inherit the scan's node 0.
			updateScopesLastFlag(result)
			root := result[0].RootOp.(*multi_update.MultiUpdate)
			require.Equal(t, 1, root.GetIdx())
			require.True(t, root.IsLast)
			require.Equal(t, !tc.writeS3, root.IsFirst)
			wantAction := multi_update.UpdateWriteTable
			if tc.writeS3 {
				wantAction = multi_update.UpdateFlushS3Info
			}
			require.Equal(t, wantAction, root.Action)
			writers := 0
			var visit func(*Scope)
			visit = func(s *Scope) {
				err := vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
					if writer, ok := op.(*multi_update.MultiUpdate); ok && writer.Action == multi_update.UpdateWriteS3 {
						writers++
						require.Equal(t, 1, writer.GetIdx())
						require.Equal(t, !tc.notFirst, writer.IsFirst)
						require.False(t, writer.IsLast)
					}
					return nil
				})
				require.NoError(t, err)
				for _, child := range s.PreScopes {
					visit(child)
				}
			}
			visit(result[0])
			if tc.writeS3 {
				require.Equal(t, tc.scopes, writers)
			} else {
				require.Zero(t, writers)
			}
		})
	}
}
