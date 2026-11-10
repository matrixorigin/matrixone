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

	"github.com/stretchr/testify/require"
)

func TestBoundStringVariableWireAndOwnerTraversal(t *testing.T) {
	var legacy VarRef
	require.NoError(t, legacy.Unmarshal([]byte{0x0a, 1, 's'}))
	require.Equal(t, "s", legacy.Name)
	require.Zero(t, legacy.BoundStringDomain)
	for _, domain := range []uint32{0, 1, 2, 3, 4, 256, ^uint32(0)} {
		variable := &Expr{Expr: &Expr_V{V: &VarRef{Name: "s", BoundStringDomain: domain}}}
		payload, err := variable.Marshal()
		require.NoError(t, err)
		var restored Expr
		require.NoError(t, restored.Unmarshal(payload))
		require.Equal(t, domain, restored.GetV().BoundStringDomain, "invalid raw values must not truncate")

		for _, root := range []*Expr{
			&restored,
			{Expr: &Expr_F{F: &Function{Args: []*Expr{&restored}}}},
			{Expr: &Expr_Lit{Lit: &Literal{Src: &restored}}},
			{Expr: &Expr_Col{Col: &ColRef{}}, PreparedNumeric: &PreparedNumericMetadata{StringDomainSource: &restored}},
		} {
			owner := &Plan{Plan: &Plan_Query{Query: &Query{Nodes: []*Node{{ProjectList: []*Expr{root}}}}}}
			require.Equal(t, domain != 0, HasBoundStringVariable(owner))
		}
	}
	require.False(t, HasBoundStringVariable(nil))
	require.False(t, HasBoundStringVariable((*Plan)(nil)))
	require.False(t, HasBoundStringVariable(&Expr{Expr: &Expr_P{P: &ParamRef{Pos: 0}}}))
}
