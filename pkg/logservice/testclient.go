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
	"github.com/google/uuid"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func NewTestService(fs vfs.FS) (*Service, ClientConfig, error) {
	addr := []string{"localhost:9000"}
	cfg := DefaultConfig()
	cfg.UUID = uuid.New().String()
	cfg.RTTMillisecond = 10
	cfg.GossipSeedAddresses = []string{DefaultGossipServiceAddress}
	cfg.DeploymentID = 1
	cfg.FS = fs
	cfg.LogServicePort = 9000
	cfg.DisableWorkers = true
	service, err := NewService(cfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	if err != nil {
		return nil, ClientConfig{}, err
	}

	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StartReplica,
			Replica: pb.Replica{
				ShardID:   1,
				ReplicaID: 1,
			},
			InitialMembers: map[uint64]string{1: service.ID()},
		},
	}
	service.handleCommands([]pb.ScheduleCommand{cmd})

	ccfg := ClientConfig{
		LogShardID:       1,
		DNReplicaID:      10,
		ServiceAddresses: addr,
	}
	return service, ccfg, nil
}

func newFS() *fileservice.FileServices {
	local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	s3, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	etl, err := fileservice.NewMemoryFS(defines.ETLFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	fs, err := fileservice.NewFileServices(
		"local",
		local,
		s3,
		etl,
	)
	if err != nil {
		panic(err)
	}
	return fs
}
