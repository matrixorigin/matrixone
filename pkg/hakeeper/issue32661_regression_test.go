package hakeeper

import (
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestDeleteCNStoreUsesReplicatedTime(t *testing.T) {
	a := NewStateMachine(0, 1).(*stateMachine)
	b := NewStateMachine(0, 2).(*stateMachine)
	store := pb.CNStoreInfo{SQLAddress: "10.0.0.1:6001", UpTime: 123}
	a.state.CNState.Stores["cn-1"] = store
	b.state.CNState.Stores["cn-1"] = store
	cmd := GetDeleteCNStoreCmd(pb.DeleteCNStore{StoreID: "cn-1", DownTime: 123456789})
	_, err := a.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	_, err = b.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	require.Len(t, a.state.DeletedStores, 1)
	require.Len(t, b.state.DeletedStores, 1)
	require.Equal(t, a.state.DeletedStores, b.state.DeletedStores)
	require.Equal(t, int64(123456789), a.state.DeletedStores[0].DownTime)
}

func TestDeleteCNStoreLegacyAndUnsortedPruning(t *testing.T) {
	first := NewStateMachine(0, 1).(*stateMachine)
	second := NewStateMachine(0, 2).(*stateMachine)
	for _, machine := range []*stateMachine{first, second} {
		machine.state.CNState.Stores["legacy"] = pb.CNStoreInfo{UpTime: 42}
	}
	legacy := GetDeleteCNStoreCmd(pb.DeleteCNStore{StoreID: "legacy"})
	_, err := first.Update(sm.Entry{Cmd: legacy})
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	_, err = second.Update(sm.Entry{Cmd: legacy})
	require.NoError(t, err)
	require.Equal(t, first.state.DeletedStores, second.state.DeletedStores)
	require.Zero(t, first.state.DeletedStores[0].DownTime)

	const day = int64(24 * time.Hour)
	first.state.DeletedStores = []pb.DeletedStore{
		{UUID: "legacy", DownTime: 0},
		{UUID: "old", DownTime: day},
		{UUID: "recent", DownTime: 9 * day},
		{UUID: "older", DownTime: 2 * day},
		{UUID: "future", DownTime: 11 * day},
	}
	first.handleDeleteCNCmd("missing", 10*day)
	require.Equal(t, []pb.DeletedStore{
		{UUID: "recent", DownTime: 9 * day},
		{UUID: "future", DownTime: 11 * day},
	}, first.state.DeletedStores)
}
