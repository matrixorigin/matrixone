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
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	testServiceAddress = "localhost:9000"
)

func getServiceTestConfig() Config {
	return Config{
		RTTMillisecond:       10,
		GossipSeedAddresses:  []string{"127.0.0.1:9000"},
		DeploymentID:         1,
		FS:                   vfs.NewStrictMem(),
		ServiceListenAddress: testServiceAddress,
		ServiceAddress:       testServiceAddress,
	}
}

func runServiceTest(t *testing.T, hakeeper bool, fn func(*testing.T, *Service)) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg)
	require.NoError(t, err)
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = service.ID()
	if hakeeper {
		require.NoError(t, service.store.StartHAKeeperReplica(1, peers, false))
	} else {
		require.NoError(t, service.store.StartReplica(1, 1, peers, false))
	}
	defer func() {
		assert.NoError(t, service.Close())
	}()
	fn(t, service)
}

func TestNewService(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg)
	require.NoError(t, err)
	assert.NoError(t, service.Close())
}

func TestServiceConnect(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, false, fn)
}

func TestServiceConnectTimeout(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT,
			Timeout: 50 * int64(time.Millisecond),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.Timeout, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, false, fn)
}

func TestServiceConnectRO(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT_RO,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, false, fn)
}

func getTestAppendCmd(id uint64, data []byte) []byte {
	cmd := make([]byte, len(data)+headerSize+8)
	binaryEnc.PutUint16(cmd, userEntryTag)
	binaryEnc.PutUint64(cmd[headerSize:], id)
	copy(cmd[headerSize+8:], data)
	return cmd
}

func TestServiceHandleLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.LOG_HEARTBEAT,
			Timeout: int64(time.Second),
			LogHeartbeat: pb.LogStoreHeartbeat{
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{sc1, sc2, sc3}))
		resp := s.handleLogHeartbeat(req)
		require.Equal(t, []pb.ScheduleCommand{sc1, sc3}, resp.CommandBatch.Commands)
	}
	runServiceTest(t, true, fn)
}

func TestServiceHandleDNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.DN_HEARTBEAT,
			Timeout: int64(time.Second),
			DNHeartbeat: pb.DNStoreHeartbeat{
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 1, []pb.ScheduleCommand{sc1, sc2, sc3}))
		resp := s.handleDNHeartbeat(req)
		require.Equal(t, []pb.ScheduleCommand{sc1, sc3}, resp.CommandBatch.Commands)
	}
	runServiceTest(t, true, fn)
}

func TestServiceHandleAppend(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT_RO,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.DNID, data)
		req = pb.Request{
			Method:  pb.APPEND,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.LogResponse.Index)
	}
	runServiceTest(t, false, fn)
}

func TestServiceHandleAppendWhenNotBeingTheLeaseHolder(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT_RO,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.DNID+1, data)
		req = pb.Request{
			Method:  pb.APPEND,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.NotLeaseHolder, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(0), resp.LogResponse.Index)
	}
	runServiceTest(t, false, fn)
}

func TestServiceHandleRead(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT_RO,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.DNID, data)
		req = pb.Request{
			Method:  pb.APPEND,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.LogResponse.Index)

		req = pb.Request{
			Method:  pb.READ,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Index:   1,
				MaxSize: 1024 * 32,
			},
		}
		resp, records := s.handleRead(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(1), resp.LogResponse.LastIndex)
		require.Equal(t, 4, len(records.Records))
		assert.Equal(t, pb.Internal, records.Records[0].Type)
		assert.Equal(t, pb.Internal, records.Records[1].Type)
		assert.Equal(t, pb.LeaseUpdate, records.Records[2].Type)
		assert.Equal(t, pb.UserRecord, records.Records[3].Type)
		assert.Equal(t, cmd, records.Records[3].Data)
	}
	runServiceTest(t, false, fn)
}

func TestServiceTruncate(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.CONNECT_RO,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    100,
			},
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.LogRequest.DNID, data)
		req = pb.Request{
			Method:  pb.APPEND,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.LogResponse.Index)

		req = pb.Request{
			Method:  pb.TRUNCATE,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Index:   4,
			},
		}
		resp = s.handleTruncate(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(0), resp.LogResponse.Index)

		req = pb.Request{
			Method:  pb.GET_TRUNCATE,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetTruncatedIndex(req)
		assert.Equal(t, pb.NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.LogResponse.Index)

		req = pb.Request{
			Method:  pb.TRUNCATE,
			Timeout: int64(time.Second),
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Index:   3,
			},
		}
		resp = s.handleTruncate(req)
		assert.Equal(t, pb.IndexAlreadyTruncated, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, false, fn)
}

func TestShardInfoCanBeQueried(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg1 := Config{
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9002",
		RaftAddress:         "127.0.0.1:9000",
		GossipAddress:       "127.0.0.1:9001",
		GossipSeedAddresses: []string{"127.0.0.1:9011"},
	}
	cfg2 := Config{
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-2",
		ServiceAddress:      "127.0.0.1:9012",
		RaftAddress:         "127.0.0.1:9010",
		GossipAddress:       "127.0.0.1:9011",
		GossipSeedAddresses: []string{"127.0.0.1:9001"},
	}

	service1, err := NewService(cfg1)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service1.Close())
	}()
	peers1 := make(map[uint64]dragonboat.Target)
	peers1[1] = service1.ID()
	assert.NoError(t, service1.store.StartReplica(1, 1, peers1, false))

	service2, err := NewService(cfg2)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	peers2 := make(map[uint64]dragonboat.Target)
	peers2[1] = service2.ID()
	assert.NoError(t, service2.store.StartReplica(2, 1, peers2, false))

	nhID1 := service1.ID()
	nhID2 := service2.ID()

	done := false

	// FIXME:
	// as per #3478, this test is flaky, increased loop count to 6000 to
	// see whether gossip can finish syncing in 6 seconds time. also added some
	// logging to get collect more details
	for i := 0; i < 6000; i++ {
		si1, ok := service1.GetShardInfo(1)
		if !ok || si1.LeaderID != 1 {
			plog.Errorf("shard 1 info missing on service 1")
			time.Sleep(time.Millisecond)
			continue
		}
		assert.Equal(t, 1, len(si1.Replicas))
		require.Equal(t, uint64(1), si1.ShardID)
		ri, ok := si1.Replicas[1]
		assert.True(t, ok)
		assert.Equal(t, nhID1, ri.UUID)
		assert.Equal(t, cfg1.ServiceAddress, ri.ServiceAddress)

		si2, ok := service1.GetShardInfo(2)
		if !ok || si2.LeaderID != 1 {
			plog.Errorf("shard 2 info missing on service 1")
			time.Sleep(time.Millisecond)
			continue
		}
		assert.Equal(t, 1, len(si2.Replicas))
		require.Equal(t, uint64(2), si2.ShardID)
		ri, ok = si2.Replicas[1]
		assert.True(t, ok)
		assert.Equal(t, nhID2, ri.UUID)
		assert.Equal(t, cfg2.ServiceAddress, ri.ServiceAddress)

		si1, ok = service2.GetShardInfo(1)
		if !ok || si1.LeaderID != 1 {
			plog.Errorf("shard 1 info missing on service 2")
			time.Sleep(time.Millisecond)
			continue
		}
		assert.Equal(t, 1, len(si1.Replicas))
		require.Equal(t, uint64(1), si1.ShardID)
		ri, ok = si1.Replicas[1]
		assert.True(t, ok)
		assert.Equal(t, nhID1, ri.UUID)
		assert.Equal(t, cfg1.ServiceAddress, ri.ServiceAddress)

		si2, ok = service2.GetShardInfo(2)
		if !ok || si2.LeaderID != 1 {
			plog.Errorf("shard 2 info missing on service 2")
			time.Sleep(time.Millisecond)
			continue
		}
		assert.Equal(t, 1, len(si2.Replicas))
		require.Equal(t, uint64(2), si2.ShardID)
		ri, ok = si2.Replicas[1]
		assert.True(t, ok)
		assert.Equal(t, nhID2, ri.UUID)
		assert.Equal(t, cfg2.ServiceAddress, ri.ServiceAddress)

		done = true
		break
	}
	assert.True(t, done)
}

// TODO: re-enable this test.
// this test will fail on go1.18 when -race is enabled as it will hit the 8192
// goroutine. it works fine with go1.19 beta 1 as it has the goroutine limit
// removed.

/*
func TestGossipConvergeDelay(t *testing.T) {
	if os.Getenv("LONG_TEST") == "" {
		t.Skip("Skipping long test")
	}
	defer leaktest.AfterTest(t)()
	// start all services
	configs := make([]Config, 0)
	services := make([]*Service, 0)
	for i := 0; i < 48; i++ {
		cfg := Config{
			FS:                  vfs.NewStrictMem(),
			DeploymentID:        1,
			RTTMillisecond:      5,
			DataDir:             fmt.Sprintf("data-%d", i),
			ServiceAddress:      fmt.Sprintf("127.0.0.1:%d", 6000+10*i),
			RaftAddress:         fmt.Sprintf("127.0.0.1:%d", 6000+10*i+1),
			GossipAddress:       fmt.Sprintf("127.0.0.1:%d", 6000+10*i+2),
			GossipSeedAddresses: []string{"127.0.0.1:6002", "127.0.0.1:6012"},
		}
		configs = append(configs, cfg)
		service, err := NewService(cfg)
		require.NoError(t, err)
		services = append(services, service)
	}
	defer func() {
		plog.Infof("going to close all services")
		var wg sync.WaitGroup
		for _, s := range services {
			if s != nil {
				selected := s
				wg.Add(1)
				go func() {
					require.NoError(t, selected.Close())
					wg.Done()
					plog.Infof("closed a service")
				}()
			}
		}
		wg.Wait()
	}()
	// start all replicas
	// shardID: [1, 16]
	id := uint64(100)
	for i := uint64(0); i < 16; i++ {
		shardID := i + 1
		r1 := id
		r2 := id + 1
		r3 := id + 2
		id += 3
		replicas := make(map[uint64]dragonboat.Target)
		replicas[r1] = services[i*3].ID()
		replicas[r2] = services[i*3+1].ID()
		replicas[r3] = services[i*3+2].ID()
		require.NoError(t, services[i*3+0].store.StartReplica(shardID, r1, replicas))
		require.NoError(t, services[i*3+1].store.StartReplica(shardID, r2, replicas))
		require.NoError(t, services[i*3+2].store.StartReplica(shardID, r3, replicas))
	}
	wait := func() {
		time.Sleep(10 * time.Millisecond)
	}
	// check & wait all leaders to be elected and known to all services
	cci := uint64(0)
	for retry := 0; retry < 500; retry++ {
		done := true
		for i := 0; i < 48; i++ {
			shardID := uint64(i/3 + 1)
			service := services[i]
			info, ok := service.GetShardInfo(shardID)
			if !ok || info.LeaderID == 0 {
				done = false
				wait()
				break
			}
			if shardID == 1 {
				cci = info.Epoch
			}
		}
		if done {
			break
		}
		require.True(t, retry < 499)
	}
	require.True(t, cci != 0)
	// all good now, add a replica to shard 1
	id += 1
	require.NoError(t, services[0].store.addReplica(1, id, services[3].ID(), cci))
	// check the above change can be observed by all services
	for retry := 0; retry < 500; retry++ {
		done := true
		for i := 0; i < 48; i++ {
			service := services[i]
			info, ok := service.GetShardInfo(1)
			if !ok || info.LeaderID == 0 || len(info.Replicas) != 4 {
				done = false
				wait()
				break
			}
		}
		if done {
			break
		}
		require.True(t, retry < 499)
	}
	// restart a service, watch how long will it take to get all required
	// shard info
	require.NoError(t, services[12].Close())
	services[12] = nil
	time.Sleep(2 * time.Second)
	service, err := NewService(configs[12])
	require.NoError(t, err)
	defer func() {
		require.NoError(t, service.Close())
	}()
	for retry := 0; retry < 500; retry++ {
		done := true
		for i := uint64(0); i < 16; i++ {
			shardID := i + 1
			info, ok := service.GetShardInfo(shardID)
			if !ok || info.LeaderID == 0 {
				done = false
				wait()
				break
			}
		}
		if done {
			break
		}
		require.True(t, retry < 499)
	}
}*/
