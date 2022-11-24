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
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type ShardInfo struct {
	// ReplicaID is the ID of the replica recommended to use
	ReplicaID uint64
	// Replicas is a map of replica ID to their service addresses
	Replicas map[uint64]string
}

// GetShardInfo is to be invoked when querying ShardInfo on a Log Service node.
// address is usually the reverse proxy that randomly redirect the request to
// a known Log Service node.
func GetShardInfo(address string, shardID uint64) (ShardInfo, bool, error) {
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cc, err := getRPCClient(ctx, address, respPool, defaultMaxMessageSize, "GetShardInfo")
	if err != nil {
		return ShardInfo{}, false, err
	}
	defer func() {
		if err := cc.Close(); err != nil {
			logutil.Error("failed to close client", zap.Error(err))
		}
	}()
	ctx, span := trace.Debug(ctx, "GetShardInfo")
	defer span.End()
	req := pb.Request{
		Method: pb.GET_SHARD_INFO,
		LogRequest: pb.LogRequest{
			ShardID: shardID,
		},
	}
	rpcReq := &RPCRequest{
		Request: req,
	}
	future, err := cc.Send(ctx, address, rpcReq)
	if err != nil {
		return ShardInfo{}, false, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return ShardInfo{}, false, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(resp)
	if err != nil {
		return ShardInfo{}, false, err
	}
	si := *resp.ShardInfo
	if reflect.DeepEqual(si, pb.ShardInfoQueryResult{}) {
		return ShardInfo{}, false, nil
	}
	// leader address is unknown
	if _, ok := si.Replicas[si.LeaderID]; !ok {
		return ShardInfo{}, false, nil
	}
	result := ShardInfo{
		ReplicaID: si.LeaderID,
		Replicas:  make(map[uint64]string),
	}
	for replicaID, info := range si.Replicas {
		result.Replicas[replicaID] = info.ServiceAddress
	}
	return result, true, nil
}

func (s *Service) getShardInfo(shardID uint64) (pb.ShardInfoQueryResult, bool) {
	r, ok := s.store.nh.GetNodeHostRegistry()
	if !ok {
		panic(moerr.NewInvalidState("gossip registry not enabled"))
	}
	shard, ok := r.GetShardInfo(shardID)
	if !ok {
		return pb.ShardInfoQueryResult{}, false
	}
	result := pb.ShardInfoQueryResult{
		ShardID:  shard.ShardID,
		Epoch:    shard.ConfigChangeIndex,
		LeaderID: shard.LeaderID,
		Term:     shard.Term,
		Replicas: make(map[uint64]pb.ReplicaInfo),
	}
	for nodeID, uuid := range shard.Nodes {
		data, ok := r.GetMeta(uuid)
		if !ok {
			return pb.ShardInfoQueryResult{}, false
		}
		var md storeMeta
		md.unmarshal(data)
		result.Replicas[nodeID] = pb.ReplicaInfo{
			UUID:           uuid,
			ServiceAddress: md.serviceAddress,
		}
	}
	return result, true
}
