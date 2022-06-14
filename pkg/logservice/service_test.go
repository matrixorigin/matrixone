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

func runServiceTest(t *testing.T, fn func(*testing.T, *Service)) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg)
	require.NoError(t, err)
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = service.ID()
	require.NoError(t, service.store.StartReplica(1, 1, peers))
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
			Method:  pb.MethodType_CONNECT,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, fn)
}

func TestServiceConnectTimeout(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT,
			ShardID: 1,
			Timeout: 50 * int64(time.Millisecond),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_Timeout, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, fn)
}

func TestServiceConnectRO(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT_RO,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, fn)
}

func getTestAppendCmd(id uint64, data []byte) []byte {
	cmd := make([]byte, len(data)+headerSize+8)
	binaryEnc.PutUint16(cmd, userEntryTag)
	binaryEnc.PutUint64(cmd[headerSize:], id)
	copy(cmd[headerSize+8:], data)
	return cmd
}

func TestServiceHandleAppend(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT_RO,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.DNID, data)
		req = pb.Request{
			Method:  pb.MethodType_APPEND,
			ShardID: 1,
			Timeout: int64(time.Second),
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.Index)
	}
	runServiceTest(t, fn)
}

func TestServiceHandleAppendWhenNotBeingTheLeaseHolder(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT_RO,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.DNID+1, data)
		req = pb.Request{
			Method:  pb.MethodType_APPEND,
			ShardID: 1,
			Timeout: int64(time.Second),
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.ErrorCode_NotLeaseHolder, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(0), resp.Index)
	}
	runServiceTest(t, fn)
}

func TestServiceHandleRead(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT_RO,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.DNID, data)
		req = pb.Request{
			Method:  pb.MethodType_APPEND,
			ShardID: 1,
			Timeout: int64(time.Second),
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.Index)

		req = pb.Request{
			Method:  pb.MethodType_READ,
			ShardID: 1,
			Timeout: int64(time.Second),
			Index:   1,
			MaxSize: 1024 * 32,
		}
		resp, records := s.handleRead(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(1), resp.LastIndex)
		require.Equal(t, 1, len(records.Records))
		assert.Equal(t, cmd, records.Records[0].Data)
	}
	runServiceTest(t, fn)
}

func TestServiceTruncate(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		req := pb.Request{
			Method:  pb.MethodType_CONNECT_RO,
			ShardID: 1,
			Timeout: int64(time.Second),
			DNID:    100,
		}
		resp := s.handleConnect(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)

		data := make([]byte, 8)
		cmd := getTestAppendCmd(req.DNID, data)
		req = pb.Request{
			Method:  pb.MethodType_APPEND,
			ShardID: 1,
			Timeout: int64(time.Second),
		}
		resp = s.handleAppend(req, cmd)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.Index)

		req = pb.Request{
			Method:  pb.MethodType_TRUNCATE,
			ShardID: 1,
			Timeout: int64(time.Second),
			Index:   4,
		}
		resp = s.handleTruncate(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(0), resp.Index)

		req = pb.Request{
			Method:  pb.MethodType_GET_TRUNCATE,
			ShardID: 1,
			Timeout: int64(time.Second),
		}
		resp = s.handleGetTruncatedIndex(req)
		assert.Equal(t, pb.ErrorCode_NoError, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
		assert.Equal(t, uint64(4), resp.Index)

		req = pb.Request{
			Method:  pb.MethodType_TRUNCATE,
			ShardID: 1,
			Timeout: int64(time.Second),
			Index:   3,
		}
		resp = s.handleTruncate(req)
		assert.Equal(t, pb.ErrorCode_IndexAlreadyTruncated, resp.ErrorCode)
		assert.Equal(t, "", resp.ErrorMessage)
	}
	runServiceTest(t, fn)
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
	assert.NoError(t, service1.store.StartReplica(1, 1, peers1))

	service2, err := NewService(cfg2)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	peers2 := make(map[uint64]dragonboat.Target)
	peers2[1] = service2.ID()
	assert.NoError(t, service2.store.StartReplica(2, 1, peers2))

	nhID1 := service1.ID()
	nhID2 := service2.ID()

	done := false
	for i := 0; i < 3000; i++ {
		si1, ok := service1.GetShardInfo(1)
		if !ok || si1.LeaderID != 1 {
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
	}
	assert.True(t, done)
}
