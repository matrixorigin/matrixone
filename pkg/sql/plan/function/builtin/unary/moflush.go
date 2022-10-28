package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

func MoFlushTable(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vectors) != 2 {
		return nil, moerr.NewInvalidInput("no name")
	}
	inputDbName := vectors[0]
	inputTableName := vectors[1]
	dbName := vector.MustStrCols(inputDbName)
	tableName := vector.MustStrCols(inputTableName)
	entries := make([]*api.Entry, 1)
	entries[0] = &api.Entry{DatabaseName: dbName[0], TableName: tableName[0]}
	UserId := proc.SessionInfo.UserId
	RoleId := proc.SessionInfo.RoleId
	AccountId := proc.SessionInfo.AccountId
	payload, err := types.Encode(api.PrecommitWriteCmd{
		UserId:    UserId,
		RoleId:    RoleId,
		AccountId: AccountId,
		EntryList: entries})
	if err != nil {
		return nil, moerr.NewInvalidInput("payload encode err")
	}

	reqs := make([]txn.TxnRequest, len(proc.TxnOperator.Txn().DNShards))
	for i, info := range proc.TxnOperator.Txn().DNShards {
		reqs[i] = txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  uint32(api.OpCode_OpDebug),
				Payload: payload,
				Target: metadata.DNShard{
					DNShardRecord: metadata.DNShardRecord{
						ShardID: info.ShardID,
					},
					ReplicaID: info.ReplicaID,
					Address:   info.Address,
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []int32{
					// dn shard not found
					int32(moerr.ErrDNShardNotFound),
				},
				RetryInterval: int64(time.Second),
			},
		}
	}
	return nil, nil
}
