package operator

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func CreateAddReplica(uuid util.StoreID, shardInfo logservice.LogShardInfo, replicaID uint64) (*Operator, error) {
	return NewBuilder("", shardInfo).AddPeer(string(uuid), replicaID).Build()
}

func CreateRemoveReplica(uuid util.StoreID, shardInfo logservice.LogShardInfo) (*Operator, error) {
	return NewBuilder("", shardInfo).RemovePeer(string(uuid)).Build()
}

func CreateStopReplica(brief string, uuid util.StoreID, shardID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StopLogService{StoreID: string(uuid), ShardID: shardID})
}

func CreateStartReplica(brief string, uuid util.StoreID, shardID, replicaID uint64) *Operator {
	return NewOperator(brief, shardID, 0,
		StartLogService{StoreID: string(uuid), ShardID: shardID, ReplicaID: replicaID})
}
