package bootstrap

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func Bootstrap(alloc util.IDAllocator, cluster hapb.ClusterInfo,
	dn hapb.DNState, log hapb.LogState) (commands []*hapb.ScheduleCommand) {

	for _, shardRecord := range cluster.LogShards {
		initialMembers := make(map[uint64]string)

		for uuid := range log.Stores {
			if uint64(len(initialMembers)) == shardRecord.NumberOfReplicas {
				break
			}

			replicaID, ok := alloc.Next()
			if !ok {
				panic("alloc id error")
			}

			initialMembers[replicaID] = uuid
		}

		for replicaID, uuid := range initialMembers {
			commands = append(commands,
				&hapb.ScheduleCommand{
					UUID: uuid,
					ConfigChange: &hapb.ConfigChange{
						Replica: hapb.Replica{
							UUID:      uuid,
							ShardID:   shardRecord.ShardID,
							ReplicaID: replicaID,
						},
						ChangeType:     hapb.AddReplica,
						InitialMembers: initialMembers,
					},
					ServiceType: hapb.LogService,
				})
		}
	}

	for _, dnRecord := range cluster.DNShards {
		for uuid := range dn.Stores {
			replicaID, ok := alloc.Next()
			if !ok {
				panic("alloc id error")
			}

			commands = append(commands, &hapb.ScheduleCommand{
				UUID: uuid,
				ConfigChange: &hapb.ConfigChange{
					Replica: hapb.Replica{
						UUID:      uuid,
						ShardID:   dnRecord.ShardID,
						ReplicaID: replicaID,
					},
					ChangeType: hapb.AddReplica,
				},
				ServiceType: hapb.DnService,
			})
			break
		}
	}

	return
}

func CheckBootstrap(cluster hapb.ClusterInfo, log hapb.LogState) bool {
	for _, shardInfo := range log.Shards {
		var shardRecord metadata.LogShardRecord
		for _, record := range cluster.LogShards {
			if record.ShardID == shardInfo.ShardID {
				shardRecord = record
				break
			}
		}
		if shardRecord.ShardID == 0 {
			return false
		}

		if uint64(len(shardInfo.Replicas))*2 <= shardRecord.NumberOfReplicas {
			return false
		}
	}

	return true
}
