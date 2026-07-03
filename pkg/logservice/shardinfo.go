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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
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
//
// Entries without a resolved ServiceAddress (gossip has delivered the
// membership entry but not yet the owning store's metadata) are stripped
// from the returned Replicas map. The leader-known filter likewise treats
// a leader entry with an empty ServiceAddress as "leader unknown" and
// returns ok=false. Callers that dial Replicas[id] therefore only ever
// see addresses that are at least potentially reachable, matching the
// behavior before Service.getShardInfo stopped filtering on meta presence.
func GetShardInfo(
	sid string,
	address string,
	shardID uint64,
) (ShardInfo, bool, error) {
	si, ok, err := queryShardInfoRawFn(context.Background(), sid, address, shardID, false)
	if err != nil || !ok {
		return ShardInfo{}, false, err
	}
	leader, present := si.Replicas[si.LeaderID]
	if !present || leader.ServiceAddress == "" {
		// leader address is unknown
		return ShardInfo{}, false, nil
	}
	result := ShardInfo{
		ReplicaID: si.LeaderID,
		Replicas:  make(map[uint64]string),
	}
	for replicaID, info := range si.Replicas {
		if info.ServiceAddress == "" {
			continue
		}
		result.Replicas[replicaID] = info.ServiceAddress
	}
	return result, true, nil
}

// queryShardInfoRawFn is overridable in tests. Production uses the real RPC.
var queryShardInfoRawFn = queryShardInfoRaw

// getShardMembership returns the live membership of the given shard as the
// queried peer sees it. Unlike GetShardInfo it does NOT require an elected
// leader: as long as the peer's gossip registry knows the shard, ok=true is
// returned with whatever members the peer has on file. This is the form the
// zombie self-check needs, because the scenario that produces zombies is
// exactly the one where HAKeeper has no leader.
func getShardMembership(
	ctx context.Context,
	sid string,
	address string,
	shardID uint64,
) (map[uint64]string, bool, error) {
	si, ok, err := queryShardInfoRawFn(ctx, sid, address, shardID, true)
	if err != nil || !ok {
		return nil, false, err
	}
	members := make(map[uint64]string, len(si.Replicas))
	for replicaID, info := range si.Replicas {
		members[replicaID] = info.ServiceAddress
	}
	return members, true, nil
}

// queryShardInfoRaw issues the GET_SHARD_INFO RPC and returns the raw response
// without applying the leader-known filter. ok=false means the queried peer
// has no record of the shard in its gossip registry.
func queryShardInfoRaw(
	ctx context.Context,
	sid string,
	address string,
	shardID uint64,
	includeExpiredReplicaAddresses bool,
) (pb.ShardInfoQueryResult, bool, error) {
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	ctx, cancel := context.WithTimeoutCause(ctx, time.Second, moerr.CauseGetShardInfo)
	defer cancel()
	cc, err := getRPCClient(
		ctx,
		sid,
		address,
		respPool,
		defaultMaxMessageSize,
		false,
		time.Second*10,
		"GetShardInfo",
	)
	if err != nil {
		return pb.ShardInfoQueryResult{}, false, err
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
			ShardID:                        shardID,
			IncludeExpiredReplicaAddresses: includeExpiredReplicaAddresses,
		},
	}
	rpcReq := &RPCRequest{
		Request: req,
	}
	future, err := cc.Send(ctx, address, rpcReq)
	if err != nil {
		return pb.ShardInfoQueryResult{}, false, moerr.AttachCause(ctx, err)
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.ShardInfoQueryResult{}, false, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(ctx, resp)
	if err != nil {
		return pb.ShardInfoQueryResult{}, false, err
	}
	si := *resp.ShardInfo
	if reflect.DeepEqual(si, pb.ShardInfoQueryResult{}) {
		return pb.ShardInfoQueryResult{}, false, nil
	}
	return si, true, nil
}

func (s *Service) getShardInfo(
	shardID uint64,
	includeExpiredReplicaAddresses bool,
) (pb.ShardInfoQueryResult, bool) {
	r, ok := s.store.nh.GetNodeHostRegistry()
	if !ok {
		panic(moerr.NewInvalidStateNoCtx("gossip registry not enabled"))
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
	expiredState := s.getExpiredLogStoreState(includeExpiredReplicaAddresses)
	for nodeID, uuid := range shard.Nodes {
		replica := pb.ReplicaInfo{UUID: uuid}
		if data, ok := r.GetMeta(uuid); ok {
			var md storeMeta
			md.unmarshal(data)
			replica.ServiceAddress = s.filterExpiredReplicaAddress(
				md.serviceAddress,
				uuid,
				includeExpiredReplicaAddresses,
				expiredState,
			)
		}
		// Record every membership entry from the authoritative shard view,
		// even when gossip has not yet carried that node's metadata. The
		// zombie self-check consumes this map to decide whether a locally
		// persisted replicaID is still a cluster member; filtering by
		// "has metadata" would let gossip skew misclassify a healthy
		// replica whose meta has not yet propagated. Existing callers that
		// dial Replicas[id] already handle an empty ServiceAddress (they
		// treat it as "leader unknown" and fall back).
		result.Replicas[nodeID] = replica
	}
	return result, true
}

func (s *Service) filterExpiredReplicaAddress(
	address string,
	uuid string,
	includeExpiredReplicaAddresses bool,
	state *pb.CheckerState,
) string {
	if includeExpiredReplicaAddresses ||
		!isExpiredLogStoreInState(s.cfg.GetHAKeeperConfig(), state, uuid) {
		return address
	}
	return ""
}

func (s *Service) getExpiredLogStoreState(includeExpiredReplicaAddresses bool) *pb.CheckerState {
	if includeExpiredReplicaAddresses {
		return nil
	}
	state, err := s.store.getCheckerState()
	if err != nil {
		return nil
	}
	return state
}

func isExpiredLogStoreInState(cfg hakeeper.Config, state *pb.CheckerState, uuid string) bool {
	if state == nil || state.Tick == 0 {
		return false
	}
	storeInfo, ok := state.LogState.Stores[uuid]
	if !ok {
		return false
	}
	return cfg.LogStoreExpired(storeInfo.Tick, state.Tick)
}
