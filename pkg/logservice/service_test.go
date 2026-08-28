// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	hapkg "github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failOpenFS struct {
	vfs.FS
	path string
	err  error
}

func (f *failOpenFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if name == f.path {
		return nil, f.err
	}
	return f.FS.Open(name, opts...)
}

func runServiceTest(t *testing.T,
	hakeeper bool, startReplica bool, fn func(*testing.T, *Service)) {
	defer leaktest.AfterTest(t)()
	var cfg Config
	genCfg := func() Config {
		cfg = getServiceTestConfig()
		return cfg
	}
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewServiceWithRetry(genCfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service.Close())
	}()

	if startReplica {
		shardID := hapkg.DefaultHAKeeperShardID
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = service.ID()
		if hakeeper {
			require.NoError(t, service.store.startHAKeeperReplica(1, peers, false))
		} else {
			shardID = 1
			require.NoError(t, service.store.startReplica(1, 1, peers, false))
		}

		// wait for leader to be elected
		done := false
		for i := 0; i < 1000; i++ {
			_, _, ok, err := service.store.nh.GetLeaderID(shardID)
			require.NoError(t, err)
			if ok {
				done = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.True(t, done)
	}

	fn(t, service)
}

func TestNewService(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cfg Config
	genCfg := func() Config {
		cfg = getServiceTestConfig()
		return cfg
	}
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewServiceWithRetry(genCfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	assert.NoError(t, service.Close())
}

func TestNewServiceClosesStoreOnMetadataFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	fs := cfg.FS
	defer vfs.ReportLeakedFD(fs, t)

	md := metadata.LogStore{UUID: cfg.UUID}
	require.NoError(t, createMetadataFile(cfg.DataDir, logMetadataFilename, &md, fs))

	injectedErr := errors.New("injected metadata open failure")
	cfg.FS = &failOpenFS{
		FS:   fs,
		path: fs.PathJoin(cfg.DataDir, logMetadataFilename),
		err:  injectedErr,
	}
	service, err := NewService(cfg, newFS(), nil)
	require.Nil(t, service)
	require.ErrorIs(t, err, injectedErr)

	// Reopening the same NodeHost proves that constructor failure released its
	// directory lock, network listeners, and background workers.
	cfg.FS = fs
	service, err = NewService(cfg, newFS(), nil)
	require.NoError(t, err)
	require.NoError(t, service.Close())
}

func TestNewServiceClosesStoreOnReplicaStartFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)

	service, err := NewService(cfg, newFS(), nil)
	require.NoError(t, err)
	members := map[uint64]dragonboat.Target{1: service.ID()}
	require.NoError(t, service.store.startReplica(1, 1, members, false))
	require.NoError(t, service.Close())

	record := metadata.LogShard{
		LogShardRecord: metadata.LogShardRecord{ShardID: 1},
		ReplicaID:      1,
	}
	md := metadata.LogStore{
		UUID:   cfg.UUID,
		Shards: []metadata.LogShard{record, record},
	}
	require.NoError(t, createMetadataFile(cfg.DataDir, logMetadataFilename, &md, cfg.FS))

	service, err = NewService(cfg, newFS(), nil)
	require.Nil(t, service)
	require.ErrorIs(t, err, dragonboat.ErrShardAlreadyExist)

	md.Shards = md.Shards[:1]
	require.NoError(t, createMetadataFile(cfg.DataDir, logMetadataFilename, &md, cfg.FS))
	service, err = NewService(cfg, newFS(), nil)
	require.NoError(t, err)
	require.NoError(t, service.Close())
}

func TestNewServiceRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg0 := getServiceTestConfig()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })
	cfg0.RaftAddress = occupied.Addr().String()
	defer vfs.ReportLeakedFD(cfg0.FS, t)

	var cfg Config
	attempts := 0
	first := true
	genCfg := func() Config {
		attempts++
		if first {
			first = false
			return cfg0
		}
		if attempts == 2 {
			require.NoError(t, occupied.Close())
		}
		cfg = getServiceTestConfig()
		return cfg
	}
	defer func() { vfs.ReportLeakedFD(cfg.FS, t) }()
	service, err := NewServiceWithRetry(genCfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, attempts, 2)
	assert.NoError(t, service.Close())
}

func TestNotSupportCmd(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: 999,
		}
		resp, _ := s.handle(ctx, req, nil)
		assert.Equal(t, uint32(moerr.ErrNotSupported), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceUpdateLeaseholderID(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.UPDATE_LEASEHOLDER_ID,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleUpdateLeaseholderID(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceConnect(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceConnectTimeout(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.ErrDragonboatTimeout), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceConnectRO(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func getTestAppendCmd(id uint64, data []byte) []byte {
	cmd := make([]byte, len(data)+headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], id)
	copy(cmd[headerSize+8:], data)
	return cmd
}

func TestServiceHandleLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.LOG_HEARTBEAT,
			LogHeartbeat: &pb.LogStoreHeartbeat{
				UUID: "uuid1",
			},
		}
		sc1 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 1,
				},
			},
		}
		sc2 := pb.ScheduleCommand{
			UUID: "uuid2",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 2,
				},
			},
		}
		sc3 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 3,
				},
			},
		}
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{sc1, sc2, sc3}))
		resp := s.handleLogHeartbeat(ctx, req)
		require.Equal(t, []pb.ScheduleCommand{sc1, sc3}, resp.CommandBatch.Commands)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleCNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CN_HEARTBEAT,
			CNHeartbeat: &pb.CNStoreHeartbeat{
				UUID: "uuid1",
			},
		}
		resp := s.handleCNHeartbeat(ctx, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleTNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.TN_HEARTBEAT,
			TNHeartbeat: &pb.TNStoreHeartbeat{
				UUID: "uuid1",
			},
		}
		sc1 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 1,
				},
			},
		}
		sc2 := pb.ScheduleCommand{
			UUID: "uuid2",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 2,
				},
			},
		}
		sc3 := pb.ScheduleCommand{
			UUID: "uuid1",
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID: 3,
				},
			},
		}
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{sc1, sc2, sc3}))
		resp := s.handleTNHeartbeat(ctx, req)
		require.Equal(t, []pb.ScheduleCommand{sc1, sc3}, resp.CommandBatch.Commands)
	}
	runServiceTest(t, true, true, fn)
}

func TestServicePollCommandsIsNonDestructiveAndDeduplicable(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		activateCommandDelivery(t, ctx, s)

		command := pb.ScheduleCommand{
			UUID:        "uuid1",
			ServiceType: pb.TNService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 2,
				},
			},
		}
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{command}))

		pollResp := s.handleGetScheduleCommands(ctx, pb.Request{
			Method: pb.GET_SCHEDULE_COMMANDS,
			ScheduleCommandQuery: &pb.ScheduleCommandQuery{
				UUID:        "uuid1",
				ServiceType: pb.TNService,
			},
		})
		require.Equal(t, uint32(moerr.Ok), pollResp.ErrorCode)
		require.Equal(t, []pb.ScheduleCommand{command}, pollResp.CommandBatch.Commands)
		require.NotZero(t, pollResp.CommandBatch.BatchID)
		require.True(t, ScheduleCommandBatchHasStableIDs(*pollResp.CommandBatch))

		// A retry observes the same stable batch ID and does not mutate the RSM.
		secondResp := s.handleGetScheduleCommands(ctx, pb.Request{
			Method: pb.GET_SCHEDULE_COMMANDS,
			ScheduleCommandQuery: &pb.ScheduleCommandQuery{
				UUID:        "uuid1",
				ServiceType: pb.TNService,
			},
		})
		require.Equal(t, *pollResp.CommandBatch, *secondResp.CommandBatch)

		// An upgraded heartbeat remains the delivery path; a heartbeat without
		// the capability is intentionally handled as an admission-safe no-op.
		heartbeatResp := s.handleTNHeartbeat(ctx, pb.Request{
			Method: pb.TN_HEARTBEAT,
			TNHeartbeat: &pb.TNStoreHeartbeat{
				UUID:                        "uuid1",
				CommandDeliveryAckSupported: true,
			},
		})
		require.Equal(t, *pollResp.CommandBatch, *heartbeatResp.CommandBatch)
		ackedResp := s.handleTNHeartbeat(ctx, pb.Request{
			Method: pb.TN_HEARTBEAT,
			TNHeartbeat: &pb.TNStoreHeartbeat{
				UUID:                        "uuid1",
				AckedCommandBatchID:         pollResp.CommandBatch.BatchID,
				CommandDeliveryAckSupported: true,
			},
		})
		require.Empty(t, ackedResp.CommandBatch.Commands)

		afterDelivery := s.handleGetScheduleCommands(ctx, pb.Request{
			Method: pb.GET_SCHEDULE_COMMANDS,
			ScheduleCommandQuery: &pb.ScheduleCommandQuery{
				UUID:        "uuid1",
				ServiceType: pb.TNService,
			},
		})
		require.Empty(t, afterDelivery.CommandBatch.Commands)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceRejectsInvalidScheduleCommandQueries(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for _, req := range []pb.Request{
			{Method: pb.GET_SCHEDULE_COMMANDS},
			{
				Method: pb.GET_SCHEDULE_COMMANDS,
				ScheduleCommandQuery: &pb.ScheduleCommandQuery{
					UUID:        "uuid1",
					ServiceType: pb.LogService,
				},
			},
		} {
			resp := s.handleGetScheduleCommands(ctx, req)
			require.NotEqual(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		activateCommandDelivery(t, ctx, s)

		command := pb.ScheduleCommand{
			UUID:        "uuid1",
			ServiceType: pb.CNService,
		}
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{command}))
		resp := s.handleGetScheduleCommands(ctx, pb.Request{
			Method: pb.GET_SCHEDULE_COMMANDS,
			ScheduleCommandQuery: &pb.ScheduleCommandQuery{
				UUID:        "uuid1",
				ServiceType: pb.TNService,
			},
		})
		require.NotEqual(t, uint32(moerr.Ok), resp.ErrorCode)

		// Validation happens after a read, not a consume. The intended service
		// can still retrieve the batch after a wrong-type request.
		resp = s.handleGetScheduleCommands(ctx, pb.Request{
			Method: pb.GET_SCHEDULE_COMMANDS,
			ScheduleCommandQuery: &pb.ScheduleCommandQuery{
				UUID:        "uuid1",
				ServiceType: pb.CNService,
			},
		})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		require.Equal(t, []pb.ScheduleCommand{command}, resp.CommandBatch.Commands)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceCommandDeliveryActivationWaitsForServiceCapabilities(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		sendCNHeartbeat := func(supported bool) pb.Response {
			return s.handleCNHeartbeat(ctx, pb.Request{
				Method: pb.CN_HEARTBEAT,
				CNHeartbeat: &pb.CNStoreHeartbeat{
					UUID:                        "cn-1",
					CommandDeliveryAckSupported: supported,
				},
			})
		}
		sendTNHeartbeat := func(supported bool) pb.Response {
			return s.handleTNHeartbeat(ctx, pb.Request{
				Method: pb.TN_HEARTBEAT,
				TNHeartbeat: &pb.TNStoreHeartbeat{
					UUID:                        "tn-1",
					CommandDeliveryAckSupported: supported,
				},
			})
		}
		sendLogHeartbeat := func() pb.Response {
			logHeartbeat := s.store.getHeartbeatMessage()
			return s.handleLogHeartbeat(ctx, pb.Request{
				Method:       pb.LOG_HEARTBEAT,
				LogHeartbeat: &logHeartbeat,
			})
		}
		sendLegacyLogHeartbeat := func(supported bool) pb.Response {
			return s.handleLogHeartbeat(ctx, pb.Request{
				Method: pb.LOG_HEARTBEAT,
				LogHeartbeat: &pb.LogStoreHeartbeat{
					UUID:                     "legacy-log",
					CommandDeliverySupported: supported,
				},
			})
		}

		require.Equal(t, uint32(moerr.Ok), sendCNHeartbeat(false).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), sendTNHeartbeat(false).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), sendLegacyLogHeartbeat(false).ErrorCode)
		// HAKeeper/logservice may already be upgraded, but activation cannot
		// begin while a current command target still advertises the old protocol.
		require.False(t, advanceCommandDelivery(t, ctx, s))
		first := sendLogHeartbeat()
		require.Equal(t, uint32(moerr.Ok), first.ErrorCode)
		require.False(t, s.store.commandDeliveryEnabled.Load())

		require.Equal(t, uint32(moerr.Ok), sendCNHeartbeat(true).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), sendTNHeartbeat(true).ErrorCode)
		require.False(t, advanceCommandDelivery(t, ctx, s))
		delivery, err := s.store.getCommandDeliveryState(ctx)
		require.NoError(t, err)
		require.False(t, delivery.Preparing,
			"a live legacy LogStore remains eligible for HAKeeper admission")
		require.Equal(t, uint32(moerr.Ok), sendLegacyLogHeartbeat(true).ErrorCode)
		require.False(t, advanceCommandDelivery(t, ctx, s))
		delivery, err = s.store.getCommandDeliveryState(ctx)
		require.NoError(t, err)
		require.True(t, delivery.Preparing)
		phaseOne := sendLogHeartbeat()
		require.Equal(t, uint32(moerr.Ok), phaseOne.ErrorCode)
		require.False(t, s.store.commandDeliveryEnabled.Load())

		// Phase-one is a replicated cutover point. The capability observations
		// above are intentionally insufficient; repeat them after the barrier.
		require.Equal(t, uint32(moerr.Ok), sendCNHeartbeat(true).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), sendTNHeartbeat(true).ErrorCode)
		require.True(t, advanceCommandDelivery(t, ctx, s))
		phaseTwo := sendLogHeartbeat()
		require.Equal(t, uint32(moerr.Ok), phaseTwo.ErrorCode)
		require.True(t, s.store.commandDeliveryEnabled.Load())
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceCommandDeliveryActivationWaitsForPendingHAKeeperAdmission(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		heartbeat := func(uuid string, supported bool) pb.Response {
			message := pb.LogStoreHeartbeat{
				UUID:                     uuid,
				CommandDeliverySupported: supported,
			}
			if uuid == s.store.id() {
				message = s.store.getHeartbeatMessage()
			}
			return s.handleLogHeartbeat(ctx, pb.Request{
				Method:       pb.LOG_HEARTBEAT,
				LogHeartbeat: &message,
			})
		}

		require.Equal(t, uint32(moerr.Ok), heartbeat(s.store.id(), true).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), heartbeat("legacy", false).ErrorCode)
		command := pb.ScheduleCommand{
			UUID:        s.store.id(),
			ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.AddReplica,
				Replica: pb.Replica{
					UUID:    "legacy",
					ShardID: hapkg.DefaultHAKeeperShardID,
				},
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{command}))

		// The leader-side read prevents even proposing an update tag that the
		// pending legacy member could later replay.
		require.False(t, advanceCommandDelivery(t, ctx, s))
		delivery, err := s.store.getCommandDeliveryState(ctx)
		require.NoError(t, err)
		require.False(t, delivery.Preparing)

		require.Equal(t, uint32(moerr.Ok), heartbeat("legacy", true).ErrorCode)
		response := heartbeat(s.store.id(), true)
		require.Equal(t, uint32(moerr.Ok), response.ErrorCode)
		require.Equal(t, []pb.ScheduleCommand{command}, response.CommandBatch.Commands)
		require.False(t, advanceCommandDelivery(t, ctx, s))
		delivery, err = s.store.getCommandDeliveryState(ctx)
		require.NoError(t, err)
		require.True(t, delivery.Preparing)
	}
	runServiceTest(t, true, true, fn)
}

func TestLogHeartbeatDoesNotDriveCommandDeliveryActivation(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for range 2 {
			heartbeat := s.store.getHeartbeatMessage()
			resp := s.handleLogHeartbeat(ctx, pb.Request{
				Method:       pb.LOG_HEARTBEAT,
				LogHeartbeat: &heartbeat,
			})
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		delivery, err := s.store.getCommandDeliveryState(ctx)
		require.NoError(t, err)
		require.False(t, delivery.Preparing,
			"the high-frequency heartbeat path must not propose activation")
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceViewMetadataAdmissionActivation(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		logHeartbeat := s.store.getHeartbeatMessage()
		response := s.handleLogHeartbeat(ctx, pb.Request{
			Method:       pb.LOG_HEARTBEAT,
			LogHeartbeat: &logHeartbeat,
		})
		require.Equal(t, uint32(moerr.Ok), response.ErrorCode)
		cnHeartbeat := func(observed, catalog uint64) pb.Response {
			return s.handleCNHeartbeat(ctx, pb.Request{
				Method: pb.CN_HEARTBEAT,
				CNHeartbeat: &pb.CNStoreHeartbeat{
					UUID:                            "cn-admission",
					ViewMetadataAdmissionSupported:  true,
					ViewMetadataAdmissionGeneration: 10,
					ViewMetadataObservedEpoch:       observed,
					ViewMetadataCatalogFencedEpoch:  catalog,
					CommandDeliveryAckSupported:     true,
				},
			})
		}
		proxyHeartbeat := func(observed uint64) pb.Response {
			return s.handleProxyHeartbeat(ctx, pb.Request{
				Method: pb.PROXY_HEARTBEAT,
				ProxyHeartbeat: &pb.ProxyHeartbeat{
					UUID:                            "proxy-admission",
					ViewMetadataAdmissionSupported:  true,
					ViewMetadataAdmissionGeneration: 20,
					ViewMetadataObservedEpoch:       observed,
				},
			})
		}
		require.Equal(t, uint32(moerr.Ok), cnHeartbeat(0, 0).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), proxyHeartbeat(0).ErrorCode)

		state, err := s.store.getCheckerStateWithContext(ctx)
		require.NoError(t, err)
		enabled, err := s.store.tryEnableViewMetadataAdmission(ctx, state)
		require.NoError(t, err)
		require.False(t, enabled)
		admission, err := s.store.getViewMetadataAdmissionState(ctx)
		require.NoError(t, err)
		require.True(t, admission.Preparing)

		// Every pre-barrier observation is deliberately insufficient.
		state, err = s.store.getCheckerStateWithContext(ctx)
		require.NoError(t, err)
		enabled, err = s.store.tryEnableViewMetadataAdmission(ctx, state)
		require.NoError(t, err)
		require.False(t, enabled)

		logHeartbeat = s.store.getHeartbeatMessage()
		response = s.handleLogHeartbeat(ctx, pb.Request{
			Method:       pb.LOG_HEARTBEAT,
			LogHeartbeat: &logHeartbeat,
		})
		require.Equal(t, uint32(moerr.Ok), response.ErrorCode)
		require.Equal(t, uint32(moerr.Ok), cnHeartbeat(1, 1).ErrorCode)
		require.Equal(t, uint32(moerr.Ok), proxyHeartbeat(1).ErrorCode)

		state, err = s.store.getCheckerStateWithContext(ctx)
		require.NoError(t, err)
		enabled, err = s.store.tryEnableViewMetadataAdmission(ctx, state)
		require.NoError(t, err)
		require.True(t, enabled)
		admission, err = s.store.getViewMetadataAdmissionState(ctx)
		require.NoError(t, err)
		require.True(t, admission.Enabled)
	}
	runServiceTest(t, true, true, fn)
}

func advanceCommandDelivery(
	t *testing.T,
	ctx context.Context,
	s *Service,
) bool {
	t.Helper()
	state, err := s.store.getCheckerStateWithContext(ctx)
	require.NoError(t, err)
	enabled, err := s.store.tryEnableCommandDelivery(ctx, state)
	require.NoError(t, err)
	return enabled
}

func activateCommandDelivery(t *testing.T, ctx context.Context, s *Service) {
	t.Helper()
	// Seed the leader's capability view before the checker is allowed to enter
	// phase one. Heartbeats only publish capability and advertise cached state;
	// the checker owns activation progress.
	logHeartbeat := s.store.getHeartbeatMessage()
	priming := s.handleLogHeartbeat(ctx, pb.Request{
		Method:       pb.LOG_HEARTBEAT,
		LogHeartbeat: &logHeartbeat,
	})
	require.Equal(t, uint32(moerr.Ok), priming.ErrorCode)
	require.False(t, s.store.commandDeliveryEnabled.Load())
	for phase := 0; phase < 2; phase++ {
		enabled := advanceCommandDelivery(t, ctx, s)
		logHeartbeat = s.store.getHeartbeatMessage()
		activation := s.handleLogHeartbeat(ctx, pb.Request{
			Method:       pb.LOG_HEARTBEAT,
			LogHeartbeat: &logHeartbeat,
		})
		require.Equal(t, uint32(moerr.Ok), activation.ErrorCode)
		if phase == 0 {
			require.False(t, enabled)
			require.False(t, s.store.commandDeliveryEnabled.Load(),
				"the first checker pass establishes the replicated upgrade barrier")
		} else {
			require.True(t, enabled)
			require.True(t, s.store.commandDeliveryEnabled.Load())
		}
	}
}

func TestServiceHandleAppend(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.TNID, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceHandleRead(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.TNID, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.READ,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     1,
				MaxSize: 1024 * 32,
			},
		}
		resp, records := s.handleRead(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(1), resp.LogResponse.LastLsn)
		require.Equal(t, 4, len(records.Records))
		assert.Equal(t, pb.Internal, records.Records[0].Type)
		assert.Equal(t, pb.Internal, records.Records[1].Type)
		assert.Equal(t, pb.LeaseUpdate, records.Records[2].Type)
		assert.Equal(t, pb.UserRecord, records.Records[3].Type)
		assert.Equal(t, cmd, records.Records[3].Data)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceTruncate(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.TNID, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetTruncatedIndex(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     3,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.ErrInvalidTruncateLsn), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceTsoUpdate(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.TSO_UPDATE,
			TsoRequest: &pb.TsoRequest{
				Count: 100,
			},
		}
		resp := s.handleTsoUpdate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(1), resp.TsoResponse.Value)

		req.TsoRequest.Count = 1000
		resp = s.handleTsoUpdate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(101), resp.TsoResponse.Value)

		resp = s.handleTsoUpdate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(1101), resp.TsoResponse.Value)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceCheckHAKeeper(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CHECK_HAKEEPER,
		}
		resp := s.handleCheckHAKeeper(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.False(t, resp.IsHAKeeper)
	}
	runServiceTest(t, false, false, fn)

	fn = func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		init := make(map[uint64]dragonboat.Target)
		init[1] = s.ID()
		require.NoError(t, s.store.startHAKeeperReplica(1, init, false))
		req := pb.Request{
			Method: pb.CHECK_HAKEEPER,
		}
		resp := s.handleCheckHAKeeper(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.True(t, resp.IsHAKeeper)
	}
	runServiceTest(t, false, false, fn)
}

func TestShardInfoCanBeQueried(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			cfg1 := DefaultConfig()
			cfg1.UUID = uuid.New().String()
			cfg1.FS = vfs.NewStrictMem()
			cfg1.DeploymentID = 1
			cfg1.RTTMillisecond = 5
			cfg1.DataDir = "data-1"
			cfg1.LogServicePort = 9002
			cfg1.RaftPort = 9000
			cfg1.GossipPort = 9001
			cfg1.GossipSeedAddresses = []string{"127.0.0.1:9011"}
			cfg1.DisableWorkers = true
			cfg2 := DefaultConfig()
			cfg2.UUID = uuid.New().String()
			cfg2.FS = vfs.NewStrictMem()
			cfg2.DeploymentID = 1
			cfg2.RTTMillisecond = 5
			cfg2.DataDir = "data-2"
			cfg2.LogServicePort = 9012
			cfg2.RaftPort = 9010
			cfg2.GossipPort = 9011
			cfg2.GossipSeedAddresses = []string{"127.0.0.1:9001"}
			cfg2.DisableWorkers = true

			runtime.SetupServiceBasedRuntime(cfg1.UUID, rt)
			runtime.SetupServiceBasedRuntime(cfg2.UUID, rt)

			service1, err := NewService(
				cfg1,
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, service1.Close())
			}()
			peers1 := make(map[uint64]dragonboat.Target)
			peers1[1] = service1.ID()
			assert.NoError(t, service1.store.startReplica(1, 1, peers1, false))
			service2, err := NewService(cfg2,
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, service2.Close())
			}()
			peers2 := make(map[uint64]dragonboat.Target)
			peers2[1] = service2.ID()
			assert.NoError(t, service2.store.startReplica(2, 1, peers2, false))

			nhID1 := service1.ID()
			nhID2 := service2.ID()

			done := false

			// FIXME:
			// as per #3478, this test is flaky, increased loop count to 6000 to
			// see whether gossip can finish syncing in 6 seconds time. also added some
			// logging to get collect more details
			for i := 0; i < 6000; i++ {
				si1, ok := service1.getShardInfo(context.Background(), 1, false, false)
				if !ok || si1.LeaderID != 1 {
					testLogger.Error("shard 1 info missing on service 1")
					time.Sleep(time.Millisecond)
					continue
				}
				assert.Equal(t, 1, len(si1.Replicas))
				require.Equal(t, uint64(1), si1.ShardID)
				ri, ok := si1.Replicas[1]
				assert.True(t, ok)
				assert.Equal(t, nhID1, ri.UUID)
				assert.Equal(t, cfg1.LogServiceServiceAddr(), ri.ServiceAddress)

				si2, ok := service1.getShardInfo(context.Background(), 2, false, false)
				if !ok || si2.LeaderID != 1 {
					testLogger.Error("shard 2 info missing on service 1")
					time.Sleep(time.Millisecond)
					continue
				}
				assert.Equal(t, 1, len(si2.Replicas))
				require.Equal(t, uint64(2), si2.ShardID)
				ri, ok = si2.Replicas[1]
				assert.True(t, ok)
				assert.Equal(t, nhID2, ri.UUID)
				assert.Equal(t, cfg2.LogServiceServiceAddr(), ri.ServiceAddress)

				si1, ok = service2.getShardInfo(context.Background(), 1, false, false)
				if !ok || si1.LeaderID != 1 {
					testLogger.Error("shard 1 info missing on service 2")
					time.Sleep(time.Millisecond)
					continue
				}
				assert.Equal(t, 1, len(si1.Replicas))
				require.Equal(t, uint64(1), si1.ShardID)
				ri, ok = si1.Replicas[1]
				assert.True(t, ok)
				assert.Equal(t, nhID1, ri.UUID)
				assert.Equal(t, cfg1.LogServiceServiceAddr(), ri.ServiceAddress)

				si2, ok = service2.getShardInfo(context.Background(), 2, false, false)
				if !ok || si2.LeaderID != 1 {
					testLogger.Error("shard 2 info missing on service 2")
					time.Sleep(time.Millisecond)
					continue
				}
				assert.Equal(t, 1, len(si2.Replicas))
				require.Equal(t, uint64(2), si2.ShardID)
				ri, ok = si2.Replicas[1]
				assert.True(t, ok)
				assert.Equal(t, nhID2, ri.UUID)
				assert.Equal(t, cfg2.LogServiceServiceAddr(), ri.ServiceAddress)

				done = true
				break
			}
			assert.True(t, done)
		},
	)
}

func TestGossipInSimulatedCluster(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			previousMemoryLimit := debug.SetMemoryLimit(1 << 30)
			defer debug.SetMemoryLimit(previousMemoryLimit)
			// The full topology remains available for explicit stress runs. The
			// short race suite only needs two three-node shards to cover gossip
			// aggregation, replica addition, and restart convergence.
			nodeCount := 24
			if testing.Short() {
				nodeCount = 6
			}
			shardCount := nodeCount / 3
			maxNotReady := 1
			if testing.Short() {
				// With only two shards/nodesets, allowing one miss would accept
				// half-converged gossip state. Require complete short-suite coverage.
				maxNotReady = 0
			}
			seedCount := min(nodeCount, 10)
			seedAddresses := make([]string, seedCount)
			for i := range seedCount {
				seedAddresses[i] = fmt.Sprintf("127.0.0.1:%d", 26002+10*i)
			}
			configs := make([]Config, 0, nodeCount)
			services := make([]*Service, 0, nodeCount)
			for i := 0; i < nodeCount; i++ {
				cfg := DefaultConfig()
				cfg.FS = vfs.NewStrictMem()
				cfg.UUID = uuid.New().String()
				cfg.DeploymentID = 1
				cfg.RTTMillisecond = 200
				cfg.DataDir = fmt.Sprintf("data-%d", i)
				cfg.LogServicePort = 26000 + 10*i
				cfg.RaftPort = 26000 + 10*i + 1
				cfg.GossipPort = 26000 + 10*i + 2
				cfg.GossipSeedAddresses = append([]string(nil), seedAddresses...)
				cfg.DisableWorkers = true
				cfg.LogDBBufferSize = 1024 * 16
				cfg.GossipProbeInterval.Duration = 350 * time.Millisecond
				configs = append(configs, cfg)

				runtime.SetupServiceBasedRuntime(cfg.UUID, rt)

				service, err := NewService(cfg,
					newFS(),
					nil,
					WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
						return true
					}),
				)
				require.NoError(t, err)
				services = append(services, service)
			}
			defer func() {
				testLogger.Info("going to close all services")
				var wg sync.WaitGroup
				var closeErr error
				var closeErrMu sync.Mutex
				for _, s := range services {
					if s != nil {
						selected := s
						wg.Add(1)
						go func() {
							defer wg.Done()
							if err := selected.Close(); err != nil {
								closeErrMu.Lock()
								closeErr = errors.Join(closeErr, err)
								closeErrMu.Unlock()
							}
							testLogger.Info("closed a service")
						}()
					}
				}
				wg.Wait()
				require.NoError(t, closeErr)
			}()
			// start all replicas
			// shardID: [1, 16]
			id := uint64(100)
			for i := uint64(0); i < uint64(shardCount); i++ {
				shardID := i + 1
				r1 := id
				r2 := id + 1
				r3 := id + 2
				id += 3
				replicas := make(map[uint64]dragonboat.Target)
				replicas[r1] = services[i*3].ID()
				replicas[r2] = services[i*3+1].ID()
				replicas[r3] = services[i*3+2].ID()
				require.NoError(t, services[i*3+0].store.startReplica(shardID, r1, replicas, false))
				require.NoError(t, services[i*3+1].store.startReplica(shardID, r2, replicas, false))
				require.NoError(t, services[i*3+2].store.startReplica(shardID, r3, replicas, false))
			}
			wait := func() {
				time.Sleep(50 * time.Millisecond)
			}
			// check & wait all leaders to be elected and known to all services
			cci := uint64(0)
			iterations := 1000
			for retry := 0; retry < iterations; retry++ {
				notReady := 0
				for i := 0; i < nodeCount; i++ {
					shardID := uint64(i/3 + 1)
					service := services[i]
					info, ok := service.getShardInfo(context.Background(), shardID, false, false)
					if !ok || info.LeaderID == 0 {
						notReady++
						continue
					}
					if shardID == 1 && info.Epoch != 0 {
						cci = info.Epoch
					}
				}
				if notReady <= maxNotReady {
					break
				}
				require.True(t, retry < iterations-1)
				wait()
			}
			require.True(t, cci != 0)
			// all good now, add a replica to shard 1
			id += 1

			for i := 0; i < iterations; i++ {
				err := services[0].store.addReplica(1, id, services[3].ID(), cci)
				if err == nil {
					break
				} else if err == dragonboat.ErrTimeout || err == dragonboat.ErrSystemBusy ||
					err == dragonboat.ErrInvalidDeadline || err == dragonboat.ErrTimeoutTooSmall {
					info, ok := services[0].getShardInfo(context.Background(), 1, false, false)
					if ok && info.LeaderID != 0 && len(info.Replicas) == 4 {
						break
					}
					wait()
					continue
				} else if err == dragonboat.ErrRejected {
					break
				}
				t.Fatalf("failed to add replica, %v", err)
			}

			// check the above change can be observed by all services
			for retry := 0; retry < iterations; retry++ {
				notReady := 0
				for i := 0; i < nodeCount; i++ {
					service := services[i]
					info, ok := service.getShardInfo(context.Background(), 1, false, false)
					if !ok || info.LeaderID == 0 || len(info.Replicas) != 4 {
						notReady++
						continue
					}
				}
				if notReady <= maxNotReady {
					break
				}
				require.True(t, retry < iterations-1)
				wait()
			}
			// restart a service, watch how long will it take to get all required
			// shard info
			restartIndex := 12
			if testing.Short() {
				restartIndex = 0
			}
			require.NoError(t, services[restartIndex].Close())
			services[restartIndex] = nil
			service, err := NewService(configs[restartIndex],
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, service.Close())
			}()
			for retry := 0; retry < iterations; retry++ {
				notReady := 0
				for i := uint64(0); i < uint64(shardCount); i++ {
					shardID := i + 1
					info, ok := service.getShardInfo(context.Background(), shardID, false, false)
					if !ok || info.LeaderID == 0 {
						notReady++
						continue
					}
				}
				if notReady <= maxNotReady {
					break
				}
				require.True(t, retry < iterations-1)
				wait()
			}
		},
	)
}

func TestServiceHandleCNUpdateLabel(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		uuid := "uuid1"
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method: pb.UPDATE_CN_LABEL,
			CNStoreLabel: &pb.CNStoreLabel{
				UUID: uuid,
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"a", "b"}},
					"role":    {Labels: []string{"1", "2"}},
				},
			},
		}
		resp := s.handleUpdateCNLabel(ctx0, req)
		assert.Equal(t, uint32(20101), resp.ErrorCode)
		assert.Equal(t, fmt.Sprintf("internal error: CN [%s] does not exist", uuid), resp.ErrorMessage)

		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		req = pb.Request{
			Method: pb.CN_HEARTBEAT,
			CNHeartbeat: &pb.CNStoreHeartbeat{
				UUID: uuid,
			},
		}
		resp = s.handleCNHeartbeat(ctx1, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		req = pb.Request{
			Method: pb.UPDATE_CN_LABEL,
			CNStoreLabel: &pb.CNStoreLabel{
				UUID: uuid,
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"a", "b"}},
					"role":    {Labels: []string{"1", "2"}},
				},
			},
		}
		resp = s.handleUpdateCNLabel(ctx2, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok1 := resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok1)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		ctx4, cancel4 := context.WithTimeout(context.Background(), time.Second)
		defer cancel4()
		req = pb.Request{
			Method: pb.UPDATE_CN_LABEL,
			CNStoreLabel: &pb.CNStoreLabel{
				UUID: uuid,
				Labels: map[string]metadata.LabelList{
					"role": {Labels: []string{"1", "2"}},
				},
			},
		}
		resp = s.handleUpdateCNLabel(ctx4, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second)
		defer cancel5()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx5, req)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok4 := resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok4)
		_, ok5 := info.Labels["account"]
		assert.False(t, ok5)
		labels3, ok6 := info.Labels["role"]
		assert.True(t, ok6)
		assert.Equal(t, labels3.Labels, []string{"1", "2"})
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleCNUpdateWorkState(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		uuid := "uuid1"
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method: pb.UPDATE_CN_WORK_STATE,
			CNWorkState: &pb.CNWorkState{
				UUID:  uuid,
				State: metadata.WorkState_Working,
			},
		}
		resp := s.handleUpdateCNWorkState(ctx0, req)
		assert.Equal(t, uint32(20101), resp.ErrorCode)
		assert.Equal(t, fmt.Sprintf("internal error: CN [%s] does not exist", uuid), resp.ErrorMessage)

		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		req = pb.Request{
			Method: pb.CN_HEARTBEAT,
			CNHeartbeat: &pb.CNStoreHeartbeat{
				UUID: uuid,
			},
		}
		resp = s.handleCNHeartbeat(ctx1, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		req = pb.Request{
			Method: pb.UPDATE_CN_WORK_STATE,
			CNWorkState: &pb.CNWorkState{
				UUID:  uuid,
				State: metadata.WorkState_Working,
			},
		}
		resp = s.handleUpdateCNWorkState(ctx2, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok1 := resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)

		ctx4, cancel4 := context.WithTimeout(context.Background(), time.Second)
		defer cancel4()
		req = pb.Request{
			Method: pb.UPDATE_CN_WORK_STATE,
			CNWorkState: &pb.CNWorkState{
				UUID:  uuid,
				State: metadata.WorkState_Unknown,
			},
		}
		resp = s.handleUpdateCNWorkState(ctx4, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second)
		defer cancel5()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx5, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok1 = resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleCNPatchStore(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		uuid := "uuid1"
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method: pb.PATCH_CN_STORE,
			CNStateLabel: &pb.CNStateLabel{
				UUID:  uuid,
				State: metadata.WorkState_Working,
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"a", "b"}},
					"role":    {Labels: []string{"1", "2"}},
				},
			},
		}
		resp := s.handlePatchCNStore(ctx0, req)
		assert.Equal(t, uint32(20101), resp.ErrorCode)
		assert.Equal(t, fmt.Sprintf("internal error: CN [%s] does not exist", uuid), resp.ErrorMessage)

		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		req = pb.Request{
			Method: pb.CN_HEARTBEAT,
			CNHeartbeat: &pb.CNStoreHeartbeat{
				UUID: uuid,
			},
		}
		resp = s.handleCNHeartbeat(ctx1, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		req = pb.Request{
			Method: pb.PATCH_CN_STORE,
			CNStateLabel: &pb.CNStateLabel{
				UUID:  uuid,
				State: metadata.WorkState_Working,
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"a", "b"}},
					"role":    {Labels: []string{"1", "2"}},
				},
			},
		}
		resp = s.handlePatchCNStore(ctx2, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok1 := resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Working, info.WorkState)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		ctx4, cancel4 := context.WithTimeout(context.Background(), time.Second)
		defer cancel4()
		req = pb.Request{
			Method: pb.PATCH_CN_STORE,
			CNStateLabel: &pb.CNStateLabel{
				UUID:  uuid,
				State: metadata.WorkState_Draining,
			},
		}
		resp = s.handlePatchCNStore(ctx4, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second)
		defer cancel5()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx5, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		info, ok1 = resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok1)
		assert.Equal(t, metadata.WorkState_Draining, info.WorkState)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 = info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleCNDeleteStore(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		uuid := "uuid1"
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method: pb.CN_HEARTBEAT,
			CNHeartbeat: &pb.CNStoreHeartbeat{
				UUID: uuid,
			},
		}
		resp := s.handleCNHeartbeat(ctx0, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx1, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		_, ok := resp.CheckerState.CNState.Stores[uuid]
		assert.True(t, ok)

		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		req = pb.Request{
			Method: pb.DELETE_CN_STORE,
			DeleteCNStore: &pb.DeleteCNStore{
				StoreID: uuid,
			},
		}
		resp = s.handleDeleteCNStore(ctx2, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		_, ok = resp.CheckerState.CNState.Stores[uuid]
		assert.False(t, ok)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleProxyHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req := pb.Request{
			Method: pb.PROXY_HEARTBEAT,
			ProxyHeartbeat: &pb.ProxyHeartbeat{
				UUID: "uuid1",
			},
		}
		resp := s.handleProxyHeartbeat(ctx, req)
		assert.Equal(t, &pb.CommandBatch{}, resp.CommandBatch)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleUpdateNonVotingReplicaNum(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method:              pb.UPDATE_NON_VOTING_REPLICA_NUM,
			NonVotingReplicaNum: 3,
		}
		resp := s.handleUpdateNonVotingReplicaNum(ctx0, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		assert.Equal(t, uint64(3), resp.CheckerState.NonVotingReplicaNum)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleUpdateNonVotingLocality(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx0, cancel0 := context.WithTimeout(context.Background(), time.Second)
		defer cancel0()
		req := pb.Request{
			Method: pb.UPDATE_NON_VOTING_LOCALITY,
			NonVotingLocality: &pb.Locality{
				Value: map[string]string{
					"region": "east",
					"type":   "mysql",
				},
			},
		}
		resp := s.handleUpdateNonVotingLocality(ctx0, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
		defer cancel1()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx1, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		assert.Equal(t, 2, len(resp.CheckerState.NonVotingLocality.Value))
		v, ok := resp.CheckerState.NonVotingLocality.Value["region"]
		assert.True(t, ok)
		assert.Equal(t, "east", v)
		v, ok = resp.CheckerState.NonVotingLocality.Value["type"]
		assert.True(t, ok)
		assert.Equal(t, "mysql", v)

		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		req = pb.Request{
			Method: pb.UPDATE_NON_VOTING_LOCALITY,
			NonVotingLocality: &pb.Locality{
				Value: map[string]string{
					"zone": "asia",
				},
			},
		}
		resp = s.handleUpdateNonVotingLocality(ctx2, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
		defer cancel3()
		req = pb.Request{
			Method: pb.GET_CLUSTER_STATE,
		}
		resp = s.handleGetCheckerState(ctx3, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NotEmpty(t, resp.CheckerState)
		assert.Equal(t, 1, len(resp.CheckerState.NonVotingLocality.Value))
		v, ok = resp.CheckerState.NonVotingLocality.Value["zone"]
		assert.True(t, ok)
		assert.Equal(t, "asia", v)
	}
	runServiceTest(t, true, true, fn)
}

func TestServiceHandleGetLatestLsn(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.TNID, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_LATEST_LSN,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetLatestLsn(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceRequiredLsn(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    100,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.TNID, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.SET_REQUIRED_LSN,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleSetRequiredLsn(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_REQUIRED_LSN,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetRequiredLsn(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceLeaderID(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req := pb.Request{
			Method: pb.GET_LEADER_ID,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp := s.handleGetLeaderID(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(1), resp.LogResponse.LeaderID)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceCheckHealth(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = s.ID()
		require.NoError(t, s.store.startHAKeeperReplica(1, peers, false))
		s.store.hakeeperTick()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		_, err := s.store.addLogStoreHeartbeat(ctx, s.store.getHeartbeatMessage())
		assert.NoError(t, err)

		req := pb.Request{
			Method: pb.CHECK_HEALTH,
			CheckHealth: &pb.CheckHealth{
				ShardID: 1,
			},
		}
		resp := s.handleCheckHealth(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	}
	runServiceTest(t, false, true, fn)
}

func TestServiceReadLsn(t *testing.T) {
	orig := defaultLogDBMaxLogFileSize
	defaultLogDBMaxLogFileSize = 500
	defaultArchiverEnabled = true
	defer func() {
		defaultArchiverEnabled = false
		defaultLogDBMaxLogFileSize = orig
	}()
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		for i := 0; i < 50; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(100, data)
			req := pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp := s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(0), resp.ErrorCode)
		}
		searchTime := time.Now()
		for i := 0; i < 50; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(100, data)
			req := pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp := s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}

		req := pb.Request{
			Method: pb.READ_LSN,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TS:      time.Now(),
			},
		}
		resp, _ := s.handleReadLsn(ctx, req)
		assert.NotEqual(t, uint32(moerr.Ok), resp.ErrorCode)

		resp = s.handleGetLatestLsn(ctx, pb.Request{
			Method: pb.GET_LATEST_LSN,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		})
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		lsn := resp.LogResponse.Lsn
		t.Logf("lastest lsn is: %d", lsn)

		opts := dragonboat.SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionIndex:            lsn - 1,
		}
		_, err := s.store.nh.SyncRequestSnapshot(ctx, 1, opts)
		assert.NoError(t, err)

		timeout := time.NewTimer(time.Second * 5)
		defer timeout.Stop()
		tick := time.NewTicker(time.Millisecond * 10)
		defer tick.Stop()
		var readLsn uint64
	FOR:
		for {
			select {
			case <-timeout.C:
				panic("the lsn is not valid")

			case <-tick.C:
				req := pb.Request{
					Method: pb.READ_LSN,
					LogRequest: pb.LogRequest{
						ShardID: 1,
						TS:      searchTime,
					},
				}
				resp, _ := s.handleReadLsn(ctx, req)
				if resp.ErrorCode == 0 && resp.LogResponse.Lsn > 0 {
					t.Logf("lsn is %d", resp.LogResponse.Lsn)
					readLsn = resp.LogResponse.Lsn
					break FOR
				}
			}
		}

		req = pb.Request{
			Method: pb.READ,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     readLsn - 10,
				MaxSize: math.MaxUint64,
			},
		}
		resp, logRec := s.handleRead(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, readLsn-10, resp.LogResponse.LastLsn)
		require.NotEqual(t, 0, len(logRec.Records))
	}
	runServiceTest(t, false, true, fn)
}
