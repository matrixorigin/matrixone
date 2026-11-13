package logservice

import (
	"context"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestTruncateRequiresCurrentLease(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, id := range []uint64{100, 101} {
			resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT, LogRequest: pb.LogRequest{ShardID: 1, TNID: id}})
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		for i := 0; i < 10; i++ {
			resp := s.handleAppend(ctx, pb.Request{Method: pb.APPEND, LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(101, []byte("wal")))
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		before := s.handleGetTruncatedIndex(ctx, pb.Request{Method: pb.GET_TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1}})
		require.Equal(t, uint32(moerr.Ok), before.ErrorCode)
		resp := s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, TNID: 100, Lsn: 8}})
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		after := s.handleGetTruncatedIndex(ctx, pb.Request{Method: pb.GET_TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1}})
		require.Equal(t, uint32(moerr.Ok), after.ErrorCode)
		require.Equal(t, before.LogResponse.Lsn, after.LogResponse.Lsn)
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, TNID: 101, Lsn: 8}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, TNID: 101, Lsn: 1_000_000}})
		require.Equal(t, uint32(moerr.ErrInvalidTruncateLsn), resp.ErrorCode)
		final := s.handleGetTruncatedIndex(ctx, pb.Request{Method: pb.GET_TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1}})
		require.Equal(t, uint64(8), final.LogResponse.Lsn)
		resp = s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL, LogRequest: pb.LogRequest{ShardID: 1, TNID: 100, Lsn: 9, CheckpointedDSN: 1}})
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		final = s.handleGetTruncatedIndex(ctx, pb.Request{Method: pb.GET_TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1}})
		require.Equal(t, uint64(8), final.LogResponse.Lsn)
		require.Zero(t, final.LogResponse.CheckpointedDSN)
		resp = s.handleTruncateWAL(ctx, pb.Request{Method: pb.TRUNCATE_WAL, LogRequest: pb.LogRequest{ShardID: 1, TNID: 101, Lsn: 9, CheckpointedDSN: 1}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		final = s.handleGetTruncatedIndex(ctx, pb.Request{Method: pb.GET_TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1}})
		require.Equal(t, uint64(9), final.LogResponse.Lsn)
		require.Equal(t, uint64(1), final.LogResponse.CheckpointedDSN)
	})
}

func TestTruncateRaftCommandCompatibilityAndFence(t *testing.T) {
	machine := newStateMachine(1, 1).(*stateMachine)
	apply := func(index uint64, cmd []byte) sm.Result {
		result, err := machine.Update(sm.Entry{Index: index, Cmd: cmd})
		require.NoError(t, err)
		return result
	}
	apply(1, getSetLeaseHolderCmd(101))
	require.Zero(t, apply(10, getSetTruncatedLsnCmd(3)).Value) // persisted old entry
	require.Equal(t, uint64(3), machine.state.TruncatedLsn)
	require.Len(t, apply(11, getSetTruncatedLsnCmd(4, 100)).Data, 8)
	require.Equal(t, uint64(3), machine.state.TruncatedLsn)
	malformed := getSetTruncatedLsnCmd(4)[:headerSize+7]
	require.NotZero(t, apply(12, malformed).Value)
	require.Equal(t, uint64(3), machine.state.TruncatedLsn)
	require.Zero(t, apply(13, getSetTruncatedLsnCmd(4, 101)).Value)
	require.Len(t, apply(14, getWALTruncateCmd(5, 1, 100)).Data, 8)
	require.Zero(t, machine.state.CheckpointedDSN)
	require.Zero(t, apply(15, getWALTruncateCmd(5, 1, 101)).Value)
	require.Equal(t, uint64(5), machine.state.TruncatedLsn)
	require.Equal(t, uint64(1), machine.state.CheckpointedDSN)
}
