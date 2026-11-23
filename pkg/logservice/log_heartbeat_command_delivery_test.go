package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLogHeartbeatRedeliversUnacknowledgedReplicaStart(t *testing.T) {
	runServiceTest(t, true, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		activateCommandDelivery(t, ctx, s)
		hb := s.store.getHeartbeatMessage()
		hb.UUID = "log-command-loss"
		req := pb.Request{Method: pb.LOG_HEARTBEAT, LogHeartbeat: &hb}
		registered := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), registered.ErrorCode)
		state, err := s.store.getCheckerStateWithContext(ctx)
		require.NoError(t, err)
		commands := operator.NewController().Dispatch(
			[]*operator.Operator{operator.CreateStartReplica("retry", hb.UUID, 80, 10)},
			state.LogState, state.TNState, state.CNState, state.ProxyState)
		require.Len(t, commands, 1)
		command := commands[0]
		require.False(t, command.Bootstrapping)
		require.NoError(t, s.store.addScheduleCommands(ctx, 1, commands))
		first := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), first.ErrorCode)
		require.Equal(t, []pb.ScheduleCommand{command}, first.CommandBatch.Commands)
		require.True(t, ScheduleCommandBatchHasStableIDs(*first.CommandBatch))
		// Simulate loss of the committed heartbeat's RPC response. The store
		// never sees the command, and its next heartbeat has no ack to send.
		second := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, uint32(moerr.Ok), second.ErrorCode)
		require.Equal(t, first.CommandBatch.Commands, second.CommandBatch.Commands)
		require.Equal(t, first.CommandBatch.CommandIDs, second.CommandBatch.CommandIDs)
		stored, err := s.store.getCommandBatch(ctx, command.UUID)
		require.NoError(t, err)
		require.Equal(t, []pb.ScheduleCommand{command}, stored.Commands)
		// The same replica ID in the wrong voting role is not completion.
		hb.Replicas = append(hb.Replicas, pb.LogReplicaInfo{
			LogShardInfo: pb.LogShardInfo{
				ShardID: 80, Epoch: 1,
				NonVotingReplicas: map[uint64]string{10: hb.UUID},
			},
			ReplicaID: 10, IsNonVoting: true,
		})
		wrongRole := s.handleLogHeartbeat(ctx, pb.Request{Method: pb.LOG_HEARTBEAT, LogHeartbeat: &hb})
		require.Equal(t, []pb.ScheduleCommand{command}, wrongRole.CommandBatch.Commands)
		hb.Replicas[len(hb.Replicas)-1].IsNonVoting = false
		completed := s.handleLogHeartbeat(ctx, pb.Request{Method: pb.LOG_HEARTBEAT, LogHeartbeat: &hb})
		require.Empty(t, completed.CommandBatch.Commands)
		stored, err = s.store.getCommandBatch(ctx, command.UUID)
		require.NoError(t, err)
		require.Empty(t, stored.Commands)
	})
}

func TestLaterStopCancelsOnlyMatchingPendingLogStart(t *testing.T) {
	runServiceTest(t, true, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		activateCommandDelivery(t, ctx, s)
		hb := s.store.getHeartbeatMessage()
		hb.UUID = "log-start-stop"
		req := pb.Request{Method: pb.LOG_HEARTBEAT, LogHeartbeat: &hb}
		require.Equal(t, uint32(moerr.Ok), s.handleLogHeartbeat(ctx, req).ErrorCode)
		start10 := pb.ScheduleCommand{
			UUID: hb.UUID, ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica:    pb.Replica{UUID: hb.UUID, ShardID: 80, ReplicaID: 10},
			},
		}
		start11 := start10
		start11.ConfigChange = &pb.ConfigChange{
			ChangeType: pb.StartReplica,
			Replica:    pb.Replica{UUID: hb.UUID, ShardID: 80, ReplicaID: 11},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{start10, start11}))
		first := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, []pb.ScheduleCommand{start10, start11}, first.CommandBatch.Commands)
		require.True(t, ScheduleCommandBatchHasStableIDs(*first.CommandBatch))
		stop10 := pb.ScheduleCommand{
			UUID: hb.UUID, ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StopReplica,
				Replica:    pb.Replica{UUID: hb.UUID, ShardID: 80, ReplicaID: 10},
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{stop10}))
		second := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, []pb.ScheduleCommand{start11, stop10}, second.CommandBatch.Commands)
		require.Equal(t, first.CommandBatch.CommandIDs[1], second.CommandBatch.CommandIDs[0])
	})
}
