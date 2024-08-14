// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	util "github.com/matrixorigin/matrixone/pkg/util/logservice"
)

// logserviceReplicasCols defines the columns of result of statement
// "show logservice replicas".
var logserviceReplicasCols = []Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "shard_id",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "replica_id",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "replica_role",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "replica_is_non_voting",
			columnType: defines.MYSQL_TYPE_BOOL,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "term",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "epoch",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "store_info",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

// logserviceReplicasCols defines the columns of result of statement
// "show logservice stores".
var logserviceStoresCols = []Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "store_id",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "tick",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "replica_num",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "replicas",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "locality",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "raft_address",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "service_address",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "gossip_address",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

// logserviceReplicasCols defines the columns of result of statement
// "show logservice settings".
var logserviceSettingsCols = []Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "name",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "value",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

type logserviceReplicaInfo struct {
	index              int // for test
	hasReplica         bool
	shardID            uint64
	replicaID          uint64
	replicaRole        string
	replicaIsNonVoting bool
	term               uint64
	epoch              uint64
	storeID            string
	serviceAddr        string
}

const (
	Leader   = "Leader"
	Follower = "Follower"
)

func handleShowLogserviceReplicas(execCtx *ExecCtx, ses *Session) error {
	pu := getGlobalPu()
	if pu == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "global pu is not set")
	}
	state, err := pu.HAKeeperClient.GetClusterState(execCtx.reqCtx)
	if err != nil {
		return err
	}
	var infoList []logserviceReplicaInfo
	for storeID, storeInfo := range state.LogState.Stores {
		if len(storeInfo.Replicas) == 0 {
			info := logserviceReplicaInfo{
				hasReplica:  false,
				storeID:     storeID,
				serviceAddr: storeInfo.ServiceAddress,
			}
			infoList = append(infoList, info)
			continue
		}
		for _, replica := range storeInfo.Replicas {
			info := logserviceReplicaInfo{
				hasReplica: true,
				shardID:    replica.ShardID,
				replicaID:  replica.ReplicaID,
				// TODO(volgariver6): support for non-voting
				replicaRole: Follower,
				term:        replica.Term,
				epoch:       replica.Epoch,
				storeID:     storeID,
				serviceAddr: storeInfo.ServiceAddress,
			}
			if replica.ReplicaID == replica.LeaderID {
				info.replicaRole = Leader
			}
			if replica.IsNonVoting {
				info.replicaIsNonVoting = true
			}
			infoList = append(infoList, info)
		}
	}
	buildLogserivceReplicasResult(ses, infoList)
	return nil
}

func sortLogserviceReplicasResult(infoList []logserviceReplicaInfo) []logserviceReplicaInfo {
	sort.Slice(infoList, func(i, j int) bool {
		if !infoList[i].hasReplica {
			return false
		}
		if !infoList[j].hasReplica {
			return true
		}
		if infoList[i].shardID != infoList[j].shardID {
			return infoList[i].shardID < infoList[j].shardID
		}
		if infoList[i].replicaRole != infoList[j].replicaRole {
			if infoList[i].replicaRole == Leader {
				return true
			} else if infoList[j].replicaRole == Leader {
				return false
			}
			return infoList[i].replicaRole < infoList[j].replicaRole
		}
		return infoList[i].replicaID < infoList[j].replicaID
	})
	return infoList
}

func buildLogserivceReplicasResult(ses *Session, infoList []logserviceReplicaInfo) {
	mrs := ses.GetMysqlResultSet()
	for _, col := range logserviceReplicasCols {
		mrs.AddColumn(col)
	}
	infoList = sortLogserviceReplicasResult(infoList)
	for _, info := range infoList {
		row := make([]any, len(logserviceReplicasCols))
		if info.hasReplica {
			row[0] = info.shardID
			row[1] = info.replicaID
			row[2] = info.replicaRole
			row[3] = info.replicaIsNonVoting
			row[4] = info.term
			row[5] = info.epoch
			row[6] = info.storeID
		} else {
			row[6] = info.storeID
		}
		mrs.AddRow(row)
	}
}

type logserviceStoreInfo struct {
	index       int // for test
	storeID     string
	tick        uint64
	replicaNum  int
	replicas    string
	locality    string
	raftAddr    string
	serviceAddr string
	gossipAddr  string
}

type replica struct {
	shardID   uint64
	replicaID uint64
}

func (r *replica) String() string {
	return fmt.Sprintf("%d:%d", r.shardID, r.replicaID)
}

type replicas []replica

func (rs replicas) String() string {
	var str string
	l := len(rs)
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].shardID < rs[j].shardID
	})
	for i, r := range rs {
		str += r.String()
		if i != l-1 {
			str += ";"
		}
	}
	return str
}

func handleShowLogserviceStores(execCtx *ExecCtx, ses *Session) error {
	pu := getGlobalPu()
	if pu == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "global pu is not set")
	}
	state, err := pu.HAKeeperClient.GetClusterState(execCtx.reqCtx)
	if err != nil {
		return err
	}

	infoList := make([]logserviceStoreInfo, 0, len(state.LogState.Stores))
	for storeID, storeInfo := range state.LogState.Stores {
		rs := make(replicas, 0, len(storeInfo.Replicas))
		for _, rep := range storeInfo.Replicas {
			rs = append(rs, replica{shardID: rep.ShardID, replicaID: rep.ReplicaID})
		}
		info := logserviceStoreInfo{
			storeID:     storeID,
			tick:        storeInfo.Tick,
			replicaNum:  len(storeInfo.Replicas),
			replicas:    rs.String(),
			locality:    storeInfo.Locality.Format(),
			raftAddr:    storeInfo.RaftAddress,
			serviceAddr: storeInfo.ServiceAddress,
			gossipAddr:  storeInfo.GossipAddress,
		}
		infoList = append(infoList, info)
	}
	buildLogserivceStoresResult(ses, infoList)
	return nil
}

func sortLogserviceStoresResult(infoList []logserviceStoreInfo) []logserviceStoreInfo {
	sort.Slice(infoList, func(i, j int) bool {
		return infoList[i].storeID < infoList[j].storeID
	})
	return infoList
}

func buildLogserivceStoresResult(ses *Session, infoList []logserviceStoreInfo) {
	mrs := ses.GetMysqlResultSet()
	for _, col := range logserviceStoresCols {
		mrs.AddColumn(col)
	}
	infoList = sortLogserviceStoresResult(infoList)
	for _, info := range infoList {
		row := make([]any, len(logserviceStoresCols))
		row[0] = info.storeID
		row[1] = info.tick
		row[2] = info.replicaNum
		row[3] = info.replicas
		row[4] = info.locality
		row[5] = info.raftAddr
		row[6] = info.serviceAddr
		row[7] = info.gossipAddr
		mrs.AddRow(row)
	}
}

const (
	// NonVotingReplicaNum is the number of logservice non-voting replicas.
	NonVotingReplicaNum = "non_voting_replica_num"
	// NonVotingLocality is the locality of logservice non-voting locality.
	NonVotingLocality = "non_voting_locality"
)

func handleShowLogserviceSettings(execCtx *ExecCtx, ses *Session) error {
	pu := getGlobalPu()
	if pu == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "global pu is not set")
	}
	state, err := pu.HAKeeperClient.GetClusterState(execCtx.reqCtx)
	if err != nil {
		return err
	}
	mrs := ses.GetMysqlResultSet()
	for _, col := range logserviceSettingsCols {
		mrs.AddColumn(col)
	}
	row := make([]any, 2)
	row[0] = NonVotingReplicaNum
	row[1] = strconv.FormatUint(state.NonVotingReplicaNum, 10)
	mrs.AddRow(row)

	row = make([]any, 2)
	row[0] = NonVotingLocality
	row[1] = formatLocalityValue(state.NonVotingLocality.Value)
	mrs.AddRow(row)
	return nil
}

func formatLocalityValue(vv map[string]string) string {
	sep1 := ":"
	sep2 := ";"
	var vs string
	for k, v := range vv {
		vs += k + sep1 + v + sep2
	}
	return vs
}

func handleSetLogserviceSettings(execCtx *ExecCtx, ses *Session, stmt tree.Statement) error {
	pu := getGlobalPu()
	if pu == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "global pu is not set")
	}
	setStmt, ok := stmt.(*tree.SetLogserviceSettings)
	if !ok {
		return moerr.NewInternalError(execCtx.reqCtx, "wrong stmt type")
	}
	v, err := getExprValue(setStmt.Value, ses, execCtx)
	if err != nil {
		return err
	}
	switch setStmt.Name {
	case NonVotingReplicaNum:
		num, ok := v.(int64)
		if !ok {
			return moerr.NewInternalError(execCtx.reqCtx,
				fmt.Sprintf("wrong value type: %v", v))
		}
		if err := setNonVotingReplicaNum(execCtx.reqCtx, pu.HAKeeperClient, uint64(num)); err != nil {
			return err
		}
	case NonVotingLocality:
		lv, ok := v.(string)
		if !ok {
			return moerr.NewInternalError(execCtx.reqCtx,
				fmt.Sprintf("wrong value type: %v", v))
		}
		if err := setNonVotingLocality(execCtx.reqCtx, pu.HAKeeperClient, lv); err != nil {
			return err
		}
	default:
		return moerr.NewInternalError(execCtx.reqCtx,
			fmt.Sprintf("unsupported logservice settings var: %s", setStmt.Name))
	}
	return nil
}

func setNonVotingReplicaNum(ctx context.Context, client logservice.CNHAKeeperClient, num uint64) error {
	state, err := client.GetClusterState(ctx)
	if err != nil {
		return err
	}
	if len(state.ClusterInfo.LogShards) == 0 {
		panic("number of log shards is 0")
	}
	replicaNum := state.ClusterInfo.LogShards[0].NumberOfReplicas
	storeNum := len(state.LogState.Stores)

	// check if the non-voting replica locality is set already.
	if len(state.NonVotingLocality.Value) == 0 {
		return moerr.NewInternalError(ctx, "non-voting replica locality has not been set")
	}

	// check if there are enough stores to start all replicas.
	if replicaNum+num > uint64(storeNum) {
		return moerr.NewInternalError(ctx,
			fmt.Sprintf("there are not enough log store number: only %d stores are avaiable",
				uint64(storeNum)-replicaNum))
	}

	if err := client.UpdateNonVotingReplicaNum(ctx, num); err != nil {
		return err
	}
	return nil
}

func setNonVotingLocality(ctx context.Context, client logservice.CNHAKeeperClient, localityStr string) error {
	if err := client.UpdateNonVotingLocality(ctx, pb.Locality{
		Value: util.GetLocalityFromStr(localityStr),
	}); err != nil {
		return err
	}
	return nil
}
