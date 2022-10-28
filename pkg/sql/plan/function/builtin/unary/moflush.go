package unary

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
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
	UserId := uint32(0)
	RoleId := uint32(0)
	AccountId := uint32(0)
	payload, err := types.Encode(db.FlushTable{
		DatabaseName: dbName[0],
		TableName:    tableName[0],
		AccessInfo: db.AccessInfo{
			AccountID: AccountId,
			UserID:    UserId,
			RoleID:    RoleId,
		},
	})
	if err != nil {
		return nil, moerr.NewInvalidInput("payload encode err")
	}
	if proc.TxnOperator == nil {
		logutil.Infof("proc.TxnOperator is nilllll")
	}
	TxnOperator, err := proc.TxnClient.New()
	if err != nil {
		logutil.Infof("err is nilllll")
	}
	reqs := make([]txn.TxnRequest, len(TxnOperator.Txn().DNShards))
	for i, info := range TxnOperator.Txn().DNShards {
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
	TxnOperator.Read(context.Background(), reqs)
	return nil, nil
}
