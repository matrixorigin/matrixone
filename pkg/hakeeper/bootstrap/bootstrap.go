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

package bootstrap

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/mohae/deepcopy"
	"go.uber.org/zap"
)

type Manager struct {
	cluster pb.ClusterInfo

	logger *zap.Logger
}

func NewBootstrapManager(cluster pb.ClusterInfo, logger *zap.Logger) *Manager {
	copied := deepcopy.Copy(cluster)
	nc, ok := copied.(pb.ClusterInfo)
	if !ok {
		panic("deep copy failed")
	}

	manager := &Manager{
		cluster: nc,
		logger:  logutil.GetGlobalLogger().Named("hakeeper"),
	}

	return manager
}

func (bm *Manager) Bootstrap(alloc util.IDAllocator,
	dn pb.DNState, log pb.LogState) ([]pb.ScheduleCommand, error) {
	logCommands, err := bm.bootstrapLogService(alloc, log)
	dnCommands := bm.bootstrapDN(alloc, dn)
	if err != nil {
		return nil, err
	}

	commands := append(logCommands, dnCommands...)
	for _, command := range commands {
		bm.logger.Info("schedule command generated", zap.String("command", command.LogString()))
	}
	if len(commands) != 0 {
		bm.logger.Info("bootstrap commands generated")
	}
	return commands, nil
}

func (bm *Manager) bootstrapLogService(alloc util.IDAllocator,
	log pb.LogState) (commands []pb.ScheduleCommand, err error) {
	logStores := logStoresSortedByTick(log.Stores)

	for _, shardRecord := range bm.cluster.LogShards {
		// skip HAKeeper shard
		if shardRecord.ShardID == 0 {
			continue
		}

		if shardRecord.NumberOfReplicas > uint64(len(logStores)) {
			return nil, moerr.NewInternalError("not enough log stores")
		}

		initialMembers := make(map[uint64]string)
		for i := uint64(0); i < shardRecord.NumberOfReplicas; i++ {
			replicaID, ok := alloc.Next()
			if !ok {
				return nil, moerr.NewInternalError("id allocator error")
			}

			initialMembers[replicaID] = logStores[i]
		}

		for replicaID, uuid := range initialMembers {
			commands = append(commands,
				pb.ScheduleCommand{
					UUID:          uuid,
					Bootstrapping: true,
					ConfigChange: &pb.ConfigChange{
						Replica: pb.Replica{
							UUID:      uuid,
							ShardID:   shardRecord.ShardID,
							ReplicaID: replicaID,
						},
						ChangeType:     pb.StartReplica,
						InitialMembers: initialMembers,
					},
					ServiceType: pb.LogService,
				})
		}
	}
	return
}

func (bm *Manager) bootstrapDN(alloc util.IDAllocator, dn pb.DNState) (commands []pb.ScheduleCommand) {
	dnStores := dnStoresSortedByTick(dn.Stores)
	if len(dnStores) < len(bm.cluster.DNShards) {
		return nil
	}

	for i, dnRecord := range bm.cluster.DNShards {
		if i >= len(dnStores) {
			i = i % len(dnStores)
		}
		replicaID, ok := alloc.Next()
		if !ok {
			return nil
		}

		commands = append(commands, pb.ScheduleCommand{
			UUID:          dnStores[i],
			Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					UUID:       dnStores[i],
					ShardID:    dnRecord.ShardID,
					ReplicaID:  replicaID,
					LogShardID: dnRecord.LogShardID,
				},
				ChangeType: pb.StartReplica,
			},
			ServiceType: pb.DNService,
		})
	}

	return
}

func (bm *Manager) CheckBootstrap(log pb.LogState) bool {
	for _, shardRecord := range bm.cluster.LogShards {
		shardInfo, ok := log.Shards[shardRecord.ShardID]
		if !ok {
			return false
		}

		if uint64(len(shardInfo.Replicas))*2 <= shardRecord.NumberOfReplicas {
			return false
		}
	}

	return true
}

func logStoresSortedByTick(logStores map[string]pb.LogStoreInfo) []string {
	uuidSlice := make([]string, 0, len(logStores))

	for uuid := range logStores {
		uuidSlice = append(uuidSlice, uuid)
	}

	sort.Slice(uuidSlice, func(i, j int) bool {
		return logStores[uuidSlice[i]].Tick > logStores[uuidSlice[j]].Tick
	})

	return uuidSlice
}

func dnStoresSortedByTick(dnStores map[string]pb.DNStoreInfo) []string {
	uuidSlice := make([]string, 0, len(dnStores))
	for uuid := range dnStores {
		uuidSlice = append(uuidSlice, uuid)
	}

	sort.Slice(uuidSlice, func(i, j int) bool {
		return dnStores[uuidSlice[i]].Tick > dnStores[uuidSlice[j]].Tick
	})

	return uuidSlice
}
