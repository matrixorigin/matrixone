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

package process

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentOverlayPublishesFinalValueOnce(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	var committed []UserVariableAssignment
	proc.SetUserVariableAssignmentCommitFunc(func(value UserVariableAssignment) error {
		committed = append(committed, value)
		return nil
	})

	proc.BeginUserVariableAssignment("A", 2)
	proc.StageUserVariableAssignment(UserVariableAssignment{
		Name:  "A",
		Value: int64(1),
		Type:  types.T_int64.ToType().PlanType(),
	}, 0)
	proc.StageUserVariableAssignment(UserVariableAssignment{
		Name:  "A",
		Value: int64(2),
		Type:  types.T_int64.ToType().PlanType(),
	}, 1)

	row, ok := proc.ResolveUserVariableAssignment("a", 0)
	require.True(t, ok)
	require.Equal(t, int64(1), row.Value)
	rows, valid, ok := proc.GetUserVariableAssignmentRows("a", 2)
	require.True(t, ok)
	require.Equal(t, []bool{true, true}, valid)
	require.Equal(t, int64(2), rows[1].Value)

	require.NoError(t, proc.CommitUserVariableAssignments())
	require.Len(t, committed, 1)
	require.Equal(t, "a", committed[0].Name)
	require.Equal(t, int64(2), committed[0].Value)
	_, ok = proc.ResolveUserVariableAssignment("a", 0)
	require.False(t, ok)
}

func TestUserVariableAssignmentOverlayCanBeDiscarded(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	proc.BeginUserVariableAssignment("a", 1)
	proc.StageUserVariableAssignment(UserVariableAssignment{Name: "a", Value: int64(1)}, 0)
	proc.DiscardUserVariableAssignments()
	_, ok := proc.ResolveUserVariableAssignment("a", 0)
	require.False(t, ok)
}

func TestUserVariableAssignmentOverlayEdgeCases(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	require.Equal(t, -1, proc.GetUserVariableAssignmentCurrentRow())

	// Negative batch sizes are normalized, and a nested invocation grows its
	// row history when the child advertises a larger effective batch.
	proc.BeginUserVariableAssignment("a", -1)
	proc.BeginUserVariableAssignment("b", 1)
	proc.SetUserVariableAssignmentCurrentRow(0)
	proc.BeginUserVariableAssignment("b", 2)
	proc.StageUserVariableAssignment(UserVariableAssignment{Name: "b", Value: int64(1)}, 0)
	proc.StageUserVariableAssignment(UserVariableAssignment{Name: "b", Value: int64(2)}, 1)
	proc.SetUserVariableAssignmentCurrentRow(-1)
	rows, valid, ok := proc.GetUserVariableAssignmentRows("b", 2)
	require.True(t, ok)
	require.Len(t, rows, 2)
	require.Equal(t, []bool{true, true}, valid)

	// Stage and snapshot also initialize their maps when called on a fresh
	// process, which is useful for callers that do not have an executor yet.
	staged := &Process{Base: &BaseProcess{}}
	staged.StageUserVariableAssignment(UserVariableAssignment{Name: "x", Value: int64(1)}, -1)
	snapshot := &Process{Base: &BaseProcess{}}
	snapshot.SnapshotUserVariableAssignmentRow(UserVariableAssignment{Name: "x", Value: int64(1)}, -1)

	// A partially populated row history is not exposed as a complete vector.
	partial := &Process{Base: &BaseProcess{}}
	partial.BeginUserVariableAssignment("x", 2)
	partial.StageUserVariableAssignment(UserVariableAssignment{Name: "x", Value: int64(1)}, 0)
	_, _, ok = partial.GetUserVariableAssignmentRows("x", 2)
	require.False(t, ok)

	// Commit order is deterministic, and publisher errors are returned after
	// the overlay has been cleared.
	committed := &Process{Base: &BaseProcess{}}
	committed.StageUserVariableAssignment(UserVariableAssignment{Name: "z", Value: int64(3)}, -1)
	committed.StageUserVariableAssignment(UserVariableAssignment{Name: "a", Value: int64(1)}, -1)
	var names []string
	committed.SetUserVariableAssignmentCommitFunc(func(value UserVariableAssignment) error {
		names = append(names, value.Name)
		return errors.New("publisher stopped")
	})
	require.Error(t, committed.CommitUserVariableAssignments())
	require.Equal(t, []string{"a"}, names)
	_, ok = committed.ResolveUserVariableAssignment("a", -1)
	require.False(t, ok)
}
