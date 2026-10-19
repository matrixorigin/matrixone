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

func TestUserVariableAssignmentWireAndCorrelationFields(t *testing.T) {
	// Independent wire samples pin the three adjacent oneof tags. The existing
	// correlation tags must retain their meaning when assignment is added.
	var dependent, slot, assignment Expr
	require.NoError(t, dependent.Unmarshal([]byte{0xc2, 1, 2, 8, 1}))
	require.NotNil(t, dependent.GetDependent())
	require.Equal(t, uint32(1), dependent.GetDependent().ProgramId)
	require.NoError(t, slot.Unmarshal([]byte{0xca, 1, 4, 8, 1, 16, 2}))
	require.NotNil(t, slot.GetSlot())
	require.Equal(t, uint32(2), slot.GetSlot().Ordinal)
	require.NoError(t, assignment.Unmarshal([]byte{0xd2, 1, 9, 10, 1, 'a', 18, 4, 26, 2, 8, 2}))
	require.NotNil(t, assignment.GetUserVarAssignment())
	require.Equal(t, "a", assignment.GetUserVarAssignment().Name)
	require.Equal(t, int32(2), assignment.GetUserVarAssignment().Value.GetP().Pos)

	dependent.GetDependent().Inputs = []*Expr{&assignment, &slot}
	wire, err := dependent.Marshal()
	require.NoError(t, err)
	var restored Expr
	require.NoError(t, restored.Unmarshal(wire))
	var variants []string
	require.NoError(t, VisitExprTree(&restored, func(e *Expr) error {
		switch e.Expr.(type) {
		case *Expr_Dependent:
			variants = append(variants, "dependent")
		case *Expr_UserVarAssignment:
			variants = append(variants, "assignment")
		case *Expr_P:
			variants = append(variants, "parameter")
		case *Expr_Slot:
			variants = append(variants, "slot")
		}
		return nil
	}))
	require.Equal(t, []string{"dependent", "assignment", "parameter", "slot"}, variants)
}
