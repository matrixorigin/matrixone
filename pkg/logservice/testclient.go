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
	"github.com/lni/vfs"

	"github.com/google/uuid"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func NewTestService(fs vfs.FS) (*Service, ClientConfig, error) {
	addr := "localhost:9000"
	cfg := Config{
		UUID:                 uuid.New().String(),
		RTTMillisecond:       10,
		GossipSeedAddresses:  addr,
		DeploymentID:         1,
		FS:                   fs,
		ServiceListenAddress: addr,
		ServiceAddress:       addr,
		DisableWorkers:       true,
	}
	cfg.Fill()
	service, err := NewService(cfg)
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
		ServiceAddresses: []string{addr},
	}
	return service, ccfg, nil
}
