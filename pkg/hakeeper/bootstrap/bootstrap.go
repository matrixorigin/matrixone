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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/mohae/deepcopy"
	"go.uber.org/zap"
)

type Manager struct {
	cluster pb.ClusterInfo
}

func NewBootstrapManager(cluster pb.ClusterInfo) *Manager {
	nc, ok := deepcopy.Copy(cluster).(pb.ClusterInfo)
	if !ok {
		panic("deep copy failed")
	}

	return &Manager{
		cluster: nc,
	}
}

func (bm *Manager) Bootstrap(
	service string,
	alloc util.IDAllocator,
	tn pb.TNState,
	log pb.LogState,
) ([]pb.ScheduleCommand, error) {
	logCommands, err := bm.bootstrapLogService(alloc, log)
	tnCommands := bm.bootstrapTN(alloc, tn)
	if err != nil {
		return nil, err
	}

	commands := append(logCommands, tnCommands...)
	for _, command := range commands {
		runtime.ServiceRuntime(service).SubLogger(runtime.SystemInit).Info("schedule command generated",
			zap.String("command", command.LogString()))
	}
	if len(commands) != 0 {
		runtime.ServiceRuntime(service).SubLogger(runtime.SystemInit).Info("schedule command generated")
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
			return nil, moerr.NewInternalErrorNoCtx("not enough log stores")
		}

		initialMembers := make(map[uint64]string)
		for i := uint64(0); i < shardRecord.NumberOfReplicas; i++ {
			replicaID, ok := alloc.Next()
			if !ok {
				return nil, moerr.NewInternalErrorNoCtx("id allocator error")
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

func (bm *Manager) bootstrapTN(alloc util.IDAllocator, tn pb.TNState) (commands []pb.ScheduleCommand) {
	tnStores := tnStoresSortedByTick(tn.Stores)
	if len(tnStores) < len(bm.cluster.TNShards) {
		return nil
	}

	for i, tnRecord := range bm.cluster.TNShards {
		if i >= len(tnStores) {
			i = i % len(tnStores)
		}
		replicaID, ok := alloc.Next()
		if !ok {
			return nil
		}

		commands = append(commands, pb.ScheduleCommand{
			UUID:          tnStores[i],
			Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					UUID:       tnStores[i],
					ShardID:    tnRecord.ShardID,
					ReplicaID:  replicaID,
					LogShardID: tnRecord.LogShardID,
				},
				ChangeType: pb.StartReplica,
			},
			ServiceType: pb.TNService,
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
		// FIXME(volgariver6): currently, the locality of log stores should be empty,
		// but it could not be empty actually.
		if len(logStores[uuidSlice[i]].Locality.Value) == 0 &&
			len(logStores[uuidSlice[j]].Locality.Value) > 0 {
			return true
		}
		if len(logStores[uuidSlice[i]].Locality.Value) > 0 &&
			len(logStores[uuidSlice[j]].Locality.Value) == 0 {
			return false
		}
		return logStores[uuidSlice[i]].Tick > logStores[uuidSlice[j]].Tick
	})

	return uuidSlice
}

func tnStoresSortedByTick(tnStores map[string]pb.TNStoreInfo) []string {
	uuidSlice := make([]string, 0, len(tnStores))
	for uuid := range tnStores {
		uuidSlice = append(uuidSlice, uuid)
	}

	sort.Slice(uuidSlice, func(i, j int) bool {
		return tnStores[uuidSlice[i]].Tick > tnStores[uuidSlice[j]].Tick
	})

	return uuidSlice
}
