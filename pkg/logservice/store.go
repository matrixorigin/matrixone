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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/goutils/syncutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice/pb/heartbeat"
)

var (
	ErrInvalidTruncateIndex = moerr.NewError(moerr.INVALID_INPUT, "invalid input")
	ErrNotLeaseHolder       = moerr.NewError(moerr.INVALID_STATE, "not lease holder")
	ErrOutOfRange           = moerr.NewError(moerr.INVALID_INPUT, "query out of range")
	ErrInvalidShardID       = moerr.NewError(moerr.INVALID_INPUT, "invalid shard ID")
)

type logStoreMeta struct {
	serviceAddress string
}

func (l *logStoreMeta) marshal() []byte {
	return []byte(l.serviceAddress)
}

func (l *logStoreMeta) unmarshal(data []byte) {
	l.serviceAddress = string(data)
}

func getNodeHostConfig(cfg Config) config.NodeHostConfig {
	meta := logStoreMeta{
		serviceAddress: cfg.ServiceAddress,
	}
	return config.NodeHostConfig{
		DeploymentID:        cfg.DeploymentID,
		NodeHostID:          cfg.NodeHostID,
		NodeHostDir:         cfg.DataDir,
		RTTMillisecond:      cfg.RTTMillisecond,
		AddressByNodeHostID: true,
		RaftAddress:         cfg.RaftAddress,
		ListenAddress:       cfg.RaftListenAddress,
		Expert: config.ExpertConfig{
			FS: cfg.FS,
			// FIXME: dragonboat need to be updated to make this field a first class
			// citizen
			TestGossipProbeInterval: 50 * time.Millisecond,
		},
		Gossip: config.GossipConfig{
			BindAddress:      cfg.GossipListenAddress,
			AdvertiseAddress: cfg.GossipAddress,
			Seed:             cfg.GossipSeedAddresses,
			Meta:             meta.marshal(),
		},
	}
}

func getRaftConfig(shardID uint64, replicaID uint64) config.Config {
	return config.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         10,
		HeartbeatRTT:        1,
		OrderedConfigChange: true,
	}
}

type logStore struct {
	cfg     Config
	nh      *dragonboat.NodeHost
	stopper *syncutil.Stopper

	mu struct {
		sync.Mutex
		truncateCh      chan struct{}
		pendingTruncate map[uint64]struct{}
	}
}

func newLogStore(cfg Config) (*logStore, error) {
	nh, err := dragonboat.NewNodeHost(getNodeHostConfig(cfg))
	if err != nil {
		return nil, err
	}
	ls := &logStore{
		cfg:     cfg,
		nh:      nh,
		stopper: syncutil.NewStopper(),
	}
	ls.mu.truncateCh = make(chan struct{})
	ls.mu.pendingTruncate = make(map[uint64]struct{})
	ls.stopper.RunWorker(func() {
		ls.truncationWorker()
	})
	return ls, nil
}

func (l *logStore) Close() error {
	l.stopper.Stop()
	if l.nh != nil {
		l.nh.Close()
	}
	return nil
}

func (l *logStore) ID() string {
	return l.nh.ID()
}

func (l *logStore) GetServiceAddress(nhID string) (string, bool) {
	r, ok := l.nh.GetNodeHostRegistry()
	if !ok {
		panic(moerr.NewError(moerr.INVALID_STATE, "gossip registry not enabled"))
	}
	data, ok := r.GetMeta(nhID)
	if !ok {
		return "", false
	}
	var md logStoreMeta
	md.unmarshal(data)
	return md.serviceAddress, true
}

func (l *logStore) GetShardInfo(shardID uint64) (dragonboat.ShardView, bool) {
	r, ok := l.nh.GetNodeHostRegistry()
	if !ok {
		panic(moerr.NewError(moerr.INVALID_STATE, "gossip registry not enabled"))
	}
	ci, ok := r.GetShardInfo(shardID)
	if !ok {
		return dragonboat.ShardView{}, false
	}
	return ci, true
}

func (l *logStore) StartHAKeeperReplica(replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target) error {
	raftConfig := getRaftConfig(defaultHAKeeperShardID, replicaID)
	// TODO: add another API for joining
	return l.nh.StartReplica(initialReplicas, false, newHAKeeperStateMachine, raftConfig)
}

func (l *logStore) StartReplica(shardID uint64, replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target) error {
	if shardID == defaultHAKeeperShardID {
		return ErrInvalidShardID
	}
	raftConfig := getRaftConfig(shardID, replicaID)
	// TODO: add another API for joining
	return l.nh.StartConcurrentReplica(initialReplicas, false, newStateMachine, raftConfig)
}

func (l *logStore) propose(ctx context.Context,
	session *client.Session, cmd []byte) (sm.Result, error) {
	for {
		result, err := l.nh.SyncPropose(ctx, session, cmd)
		if err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				time.Sleep(time.Duration(l.nh.NodeHostConfig().RTTMillisecond) * time.Millisecond)
				continue
			}
			return sm.Result{}, err
		}
		return result, nil
	}
}

func (l *logStore) read(ctx context.Context,
	shardID uint64, query interface{}) (interface{}, error) {
	for {
		result, err := l.nh.SyncRead(ctx, shardID, query)
		if err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				time.Sleep(time.Duration(l.nh.NodeHostConfig().RTTMillisecond) * time.Millisecond)
				continue
			}
			return result, err
		}
		return result, nil
	}
}

func (l *logStore) GetOrExtendDNLease(ctx context.Context,
	shardID uint64, dnID uint64) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetLeaseHolderCmd(dnID)
	_, err := l.propose(ctx, session, cmd)
	return err
}

func (l *logStore) TruncateLog(ctx context.Context,
	shardID uint64, index Lsn) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetTruncatedIndexCmd(index)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		plog.Errorf("propose truncate log cmd failed, %v", err)
		return err
	}
	if result.Value > 0 {
		plog.Errorf("shardID %d already truncated to index %d", shardID, result.Value)
		return errors.Wrapf(ErrInvalidTruncateIndex, "already truncated to %d", result.Value)
	}
	l.mu.Lock()
	l.mu.pendingTruncate[shardID] = struct{}{}
	l.mu.Unlock()
	l.mu.truncateCh <- struct{}{}
	return nil
}

func (l *logStore) Append(ctx context.Context,
	shardID uint64, cmd []byte) (Lsn, error) {
	if !isUserUpdate(cmd) {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not user update"))
	}
	session := l.nh.GetNoOPSession(shardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		plog.Errorf("propose failed, %v", err)
		return 0, err
	}
	if len(result.Data) > 0 {
		plog.Errorf("not current lease holder (%d)", binaryEnc.Uint64(result.Data))
		return 0, errors.Wrapf(ErrNotLeaseHolder,
			"current lease holder ID %d", binaryEnc.Uint64(result.Data))
	}
	if result.Value == 0 {
		panic(moerr.NewError(moerr.INVALID_STATE, "unexpected Lsn value"))
	}
	return result.Value, nil
}

func (l *logStore) GetTruncatedIndex(ctx context.Context,
	shardID uint64) (uint64, error) {
	v, err := l.read(ctx, shardID, truncatedIndexTag)
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (l *logStore) getLeaseHolderID(ctx context.Context,
	shardID uint64, entries []pb.Entry) (uint64, error) {
	if len(entries) == 0 {
		panic("empty entries")
	}
	// first entry is a update lease cmd
	e := entries[0]
	if isSetLeaseHolderUpdate(e.Cmd) {
		return parseLeaseHolderID(e.Cmd), nil
	}
	v, err := l.read(ctx, shardID, leaseHistoryQuery{index: e.Index})
	if err != nil {
		plog.Errorf("failed to read, %v", err)
		return 0, err
	}
	return v.(uint64), nil
}

func (l *logStore) decodeCmd(e pb.Entry) []byte {
	if e.Type == pb.ApplicationEntry {
		panic(moerr.NewError(moerr.INVALID_STATE, "unexpected entry type"))
	}
	if e.Type == pb.EncodedEntry {
		if e.Cmd[0] != 0 {
			panic(moerr.NewError(moerr.INVALID_STATE, "unexpected cmd header"))
		}
		return e.Cmd[1:]
	}
	panic(moerr.NewError(moerr.INVALID_STATE, "invalid cmd"))
}

func isRaftInternalEntry(e pb.Entry) bool {
	if len(e.Cmd) == 0 {
		return true
	}
	return e.Type == pb.ConfigChangeEntry || e.Type == pb.MetadataEntry
}

func (l *logStore) filterEntries(ctx context.Context,
	shardID uint64, entries []pb.Entry) ([]LogRecord, error) {
	if len(entries) == 0 {
		return []LogRecord{}, nil
	}
	leaseHolderID, err := l.getLeaseHolderID(ctx, shardID, entries)
	if err != nil {
		return nil, err
	}
	result := make([]LogRecord, 0)
	for _, e := range entries {
		if isRaftInternalEntry(e) {
			// raft internal stuff
			continue
		}
		cmd := l.decodeCmd(e)
		if isSetLeaseHolderUpdate(cmd) {
			leaseHolderID = parseLeaseHolderID(cmd)
			continue
		}
		if isUserUpdate(cmd) {
			if parseLeaseHolderID(cmd) != leaseHolderID {
				// lease not match, skip
				continue
			}
			result = append(result, LogRecord{Data: cmd})
		}
	}
	return result, nil
}

func getNextIndex(entries []pb.Entry, firstIndex Lsn, lastIndex Lsn) Lsn {
	if len(entries) == 0 {
		return firstIndex
	}
	lastResultIndex := entries[len(entries)-1].Index
	if lastResultIndex+1 < lastIndex {
		return lastResultIndex + 1
	}
	return firstIndex
}

func (l *logStore) QueryLog(ctx context.Context, shardID uint64,
	firstIndex Lsn, maxSize uint64) ([]LogRecord, Lsn, error) {
	v, err := l.read(ctx, shardID, indexTag)
	if err != nil {
		return nil, 0, err
	}
	lastIndex := v.(uint64)
	rs, err := l.nh.QueryRaftLog(shardID, firstIndex, lastIndex+1, maxSize)
	if err != nil {
		plog.Errorf("QueryRaftLog failed, %v", err)
		return nil, 0, err
	}
	select {
	case v := <-rs.CompletedC:
		if v.Completed() {
			entries, logRange := v.RaftLogs()
			next := getNextIndex(entries, firstIndex, logRange.LastIndex)
			results, err := l.filterEntries(ctx, shardID, entries)
			if err != nil {
				plog.Errorf("filterEntries failed, %v", err)
				return nil, 0, err
			}
			return results, next, nil
		} else if v.RequestOutOfRange() {
			plog.Errorf("OutOfRange query found")
			return nil, 0, ErrOutOfRange
		}
		panic(moerr.NewError(moerr.INVALID_STATE, "unexpected rs state"))
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (l *logStore) truncationWorker() {
	for {
		select {
		case <-l.stopper.ShouldStop():
			return
		case <-l.mu.truncateCh:
			if err := l.truncateLog(); err != nil {
				panic(err)
			}
		}
	}
}

// TODO: add tests for this
func (l *logStore) truncateLog() error {
	l.mu.Lock()
	pendings := l.mu.pendingTruncate
	l.mu.pendingTruncate = make(map[uint64]struct{})
	l.mu.Unlock()

	for shardID := range pendings {
		select {
		case <-l.stopper.ShouldStop():
			return nil
		default:
		}

		if err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			index, err := l.GetTruncatedIndex(ctx, shardID)
			if err != nil {
				plog.Errorf("GetTruncatedIndex failed, %v", err)
				// FIXME: check error type, see whether it is a tmp one
				return err
			}
			// the first 4 entries for a 3-replica raft group are tiny anyway
			if index > 1 {
				opts := dragonboat.SnapshotOption{
					OverrideCompactionOverhead: true,
					CompactionIndex:            index - 1,
				}
				if _, err := l.nh.SyncRequestSnapshot(ctx, shardID, opts); err != nil {
					plog.Errorf("SyncRequestSnapshot failed, %v", err)
					// FIXME: check error type, see whether it is a tmp one
					return err
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) getHeartbeatMessage() heartbeat.LogStoreHeartbeat {
	m := heartbeat.LogStoreHeartbeat{
		UUID:           l.cfg.NodeHostID,
		RaftAddress:    l.cfg.RaftAddress,
		ServiceAddress: l.cfg.ServiceAddress,
		GossipAddress:  l.cfg.GossipAddress,
		Shards:         make([]*heartbeat.ShardInfo, 0),
	}
	opts := dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	}
	nhi := l.nh.GetNodeHostInfo(opts)
	for _, ci := range nhi.ShardInfoList {
		shardInfo := &heartbeat.ShardInfo{
			ShardID:  ci.ShardID,
			Replicas: ci.Nodes,
			Epoch:    ci.ConfigChangeIndex,
			LeaderID: ci.LeaderID,
			Term:     ci.Term,
		}
		m.Shards = append(m.Shards, shardInfo)
	}

	return m
}
