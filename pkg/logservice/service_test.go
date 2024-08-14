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
	"fmt"
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

func runServiceTest(t *testing.T,
	hakeeper bool, startReplica bool, fn func(*testing.T, *Service)) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg,
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
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
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

func TestServiceHandleAppendWhenNotBeingTheLeaseHolder(t *testing.T) {
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
		cmd := getTestAppendCmd(req.LogRequest.TNID+1, data)
		req = pb.Request{
			Method: pb.APPEND,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(ctx, req, cmd)
		assert.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)
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
				si1, ok := service1.getShardInfo(1)
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

				si2, ok := service1.getShardInfo(2)
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

				si1, ok = service2.getShardInfo(1)
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

				si2, ok = service2.getShardInfo(2)
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
			debug.SetMemoryLimit(1 << 30)
			// start all services
			nodeCount := 24
			shardCount := nodeCount / 3
			configs := make([]Config, 0)
			services := make([]*Service, 0)
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
				cfg.GossipSeedAddresses = []string{
					"127.0.0.1:26002",
					"127.0.0.1:26012",
					"127.0.0.1:26022",
					"127.0.0.1:26032",
					"127.0.0.1:26042",
					"127.0.0.1:26052",
					"127.0.0.1:26062",
					"127.0.0.1:26072",
					"127.0.0.1:26082",
					"127.0.0.1:26092",
				}
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
				for _, s := range services {
					if s != nil {
						selected := s
						wg.Add(1)
						go func() {
							require.NoError(t, selected.Close())
							wg.Done()
							testLogger.Info("closed a service")
						}()
					}
				}
				wg.Wait()
				time.Sleep(time.Second * 2)
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
					info, ok := service.getShardInfo(shardID)
					if !ok || info.LeaderID == 0 {
						notReady++
						wait()
						continue
					}
					if shardID == 1 && info.Epoch != 0 {
						cci = info.Epoch
					}
				}
				if notReady <= 1 {
					break
				}
				require.True(t, retry < iterations-1)
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
					info, ok := services[0].getShardInfo(1)
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
					info, ok := service.getShardInfo(1)
					if !ok || info.LeaderID == 0 || len(info.Replicas) != 4 {
						notReady++
						wait()
						continue
					}
				}
				if notReady <= 1 {
					break
				}
				require.True(t, retry < iterations-1)
			}
			// restart a service, watch how long will it take to get all required
			// shard info
			require.NoError(t, services[12].Close())
			services[12] = nil
			time.Sleep(2 * time.Second)
			service, err := NewService(configs[12],
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
					info, ok := service.getShardInfo(shardID)
					if !ok || info.LeaderID == 0 {
						notReady++
						wait()
						continue
					}
				}
				if notReady <= 1 {
					break
				}
				require.True(t, retry < iterations-1)
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
