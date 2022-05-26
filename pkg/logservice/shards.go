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

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	pb "github.com/lni/dragonboat/v3/raftpb"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrInvalidTruncateIndex = moerr.NewError(moerr.INVALID_INPUT, "invalid input")
	ErrNotLeaseHolder       = moerr.NewError(moerr.INVALID_STATE, "not lease holder")
	ErrOutOfRange           = moerr.NewError(moerr.INVALID_INPUT, "query out of range")
)

func getNodeHostConfig(cfg Config) config.NodeHostConfig {
	return config.NodeHostConfig{
		DeploymentID:        cfg.DeploymentID,
		NodeHostDir:         cfg.DataDir,
		RTTMillisecond:      200,
		AddressByNodeHostID: true,
		RaftAddress:         cfg.RaftAddress,
		ListenAddress:       cfg.RaftListenAddress,
		Gossip: config.GossipConfig{
			BindAddress:      cfg.GossipListenAddress,
			AdvertiseAddress: cfg.GossipAddress,
			Seed:             cfg.GossipSeedAddresses,
			// TODO: pass the service address to the Meta []byte
		},
	}
}

func getRaftConfig(shardID uint64, replicaID uint64) config.Config {
	return config.Config{
		ClusterID:           shardID,
		NodeID:              replicaID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         10,
		HeartbeatRTT:        1,
		OrderedConfigChange: true,
	}
}

type ShardManager struct {
	nh *dragonboat.NodeHost
}

func NewShardManager(cfg Config) (*ShardManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.Fill()
	nh, err := dragonboat.NewNodeHost(getNodeHostConfig(cfg))
	if err != nil {
		return nil, err
	}

	return &ShardManager{nh: nh}, nil
}

func (l *ShardManager) Close() error {
	l.nh.Close()
	return nil
}

func (l *ShardManager) StartHAKeeperReplica(replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target) error {
	raftConfig := getRaftConfig(defaultHAKeeperShardID, replicaID)
	// FIXME: why join is always true
	return l.nh.StartCluster(initialReplicas, true, newHAKeeperStateMachine, raftConfig)
}

func (l *ShardManager) StartReplica(shardID uint64, replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target) error {
	raftConfig := getRaftConfig(shardID, replicaID)
	// FIXME: why join is always true
	return l.nh.StartConcurrentCluster(initialReplicas, true, newStateMachine, raftConfig)
}

func (l *ShardManager) GetOrExtendDNLease(ctx context.Context,
	shardID uint64, dnID uint64) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetLeaseHolderCmd(dnID)
	_, err := l.nh.SyncPropose(ctx, session, cmd)
	return err
}

func (l *ShardManager) TruncateLog(ctx context.Context,
	shardID uint64, index uint64) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetTruncatedIndexCmd(index)
	result, err := l.nh.SyncPropose(ctx, session, cmd)
	if err != nil {
		return err
	}
	if result.Value > 0 {
		return errors.Wrapf(ErrInvalidTruncateIndex, "already truncated to %d", result.Value)
	}
	return nil
}

func (l *ShardManager) Append(ctx context.Context,
	shardID uint64, cmd []byte) error {
	if !isUserUpdate(cmd) {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not user update"))
	}
	session := l.nh.GetNoOPSession(shardID)
	result, err := l.nh.SyncPropose(ctx, session, cmd)
	if err != nil {
		return err
	}
	if result.Value > 0 {
		return errors.Wrapf(ErrNotLeaseHolder, "lease holder ID %d", result.Value)
	}
	return nil
}

func (l *ShardManager) GetTruncatedIndex(ctx context.Context,
	shardID uint64) (uint64, error) {
	v, err := l.nh.SyncRead(ctx, shardID, truncatedIndexTag)
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

// TODO: update QueryLog to provide clear indication whether there is any more
// log to recover.
func (l *ShardManager) QueryLog(ctx context.Context, shardID uint64,
	firstIndex uint64, lastIndex uint64, maxSize uint64) ([]pb.Entry, error) {
	rs, err := l.nh.QueryRaftLog(shardID, firstIndex, lastIndex, maxSize)
	if err != nil {
		return nil, err
	}
	select {
	case v := <-rs.CompletedC:
		if v.Completed() {
			entries, _ := v.RaftLogs()
			return entries, nil
		} else if v.RequestOutOfRange() {
			return nil, ErrOutOfRange
		}
		panic(moerr.NewError(moerr.INVALID_STATE, "unexpected rs state"))
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
