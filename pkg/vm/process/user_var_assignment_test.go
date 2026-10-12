package process

import (
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
