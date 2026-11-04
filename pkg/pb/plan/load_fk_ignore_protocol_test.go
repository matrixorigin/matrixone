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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadFKIgnoreCheckFeature(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      int64
		display string
		want    bool
	}{
		{"encoded identity", 602, "", true},
		{"renamed identity", 602, "assert", true},
		{"name is not identity", 0, "_load_fk_ignore_check", false},
		{"table collation control", 600, "_load_fk_ignore_check", false},
		{"timestampadd control", 601, "_load_fk_ignore_check", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &Expr{Typ: Type{Id: 10}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: tc.id << 32, ObjName: tc.display}, Args: []*Expr{{Typ: Type{Id: 10}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}}}}}
			q := &Query{StmtType: Query_INSERT, Nodes: []*Node{{NodeType: Node_FILTER, FilterIsBarrier: true, FilterList: []*Expr{expr}}}, Steps: []int32{0}}
			wire, err := q.Marshal()
			require.NoError(t, err)
			var decoded Query
			require.NoError(t, decoded.Unmarshal(wire))
			for _, owner := range []any{expr, q, &decoded} {
				f, err := RequiredRemoteExpressionFeatures(owner)
				require.NoError(t, err)
				require.Equal(t, tc.want, f.LoadFKIgnoreCheck)
				require.Equal(t, tc.want || tc.id == 600 || tc.id == 601, f.Any())
				require.Equal(t, tc.id == 600, f.TableCollationFunction)
				require.Equal(t, tc.id == 601, f.TimestampAddSecondIntervals)
			}
		})
	}
	require.True(t, RemoteExpressionFeatures{LoadFKIgnoreCheck: true}.Any())
}
