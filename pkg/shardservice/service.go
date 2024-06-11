// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

type Option func(*service)

func withDisableHeartbeat() Option {
	return func(s *service) {
		s.options.disableHeartbeat.Store(true)
	}
}

func withDisableAppendDeleteCallback() Option {
	return func(s *service) {
		s.options.disableAppendDeleteCallback = true
	}
}

func withDisableAppendCreateCallback() Option {
	return func(s *service) {
		s.options.disableAppendCreateCallback = true
	}
}

type service struct {
	logger  *log.MOLogger
	cfg     Config
	storage ShardStorage

	stopper *stopper.Stopper
	createC chan uint64
	deleteC chan uint64

	cache struct {
		sync.Mutex

		noneSharding atomic.Pointer[roaring64.Bitmap]
		// allocate all shards which allocated in the current node
		allocate atomic.Pointer[allocatedCache]
		// read all shards which read in the current node
		read atomic.Pointer[readCache]
	}

	remote struct {
		cluster clusterservice.MOCluster
		pool    morpc.MessagePool[*pb.Request, *pb.Response]
		client  morpc.MethodBasedClient[*pb.Request, *pb.Response]
		server  morpc.MethodBasedServer[*pb.Request, *pb.Response]
	}

	atomic struct {
		abort   atomic.Uint64
		skip    atomic.Uint64
		removed atomic.Uint64
		added   atomic.Uint64
	}

	options struct {
		disableHeartbeat            atomic.Bool
		disableAppendDeleteCallback bool
		disableAppendCreateCallback bool
	}
}

func NewService(
	cfg Config,
	storage ShardStorage,
	opts ...Option,
) ShardService {
	logger := getLogger().With(zap.String("service", cfg.ServiceID))
	s := &service{
		logger:  logger,
		cfg:     cfg,
		storage: storage,
		createC: make(chan uint64, 16),
		deleteC: make(chan uint64, 16),
		stopper: stopper.NewStopper(
			"shard-service",
			stopper.WithLogger(logger.RawLogger()),
		),
	}

	s.cache.read.Store(newReadCache())
	s.cache.allocate.Store(newAllocatedCache())
	s.cache.noneSharding.Store(roaring64.New())

	for _, opt := range opts {
		opt(s)
	}

	s.validate()
	s.initRemote()
	if err := s.stopper.RunTask(s.doTask); err != nil {
		panic(err)
	}
	return s
}

func (s *service) validate() {
	if s.storage == nil {
		panic("storage is nil")
	}
	s.cfg.Validate()
}

func (s *service) Close() error {
	s.stopper.Stop()
	close(s.createC)
	close(s.deleteC)
	return s.remote.client.Close()
}

func (s *service) Create(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) error {
	if !s.cfg.Enable {
		return nil
	}

	created, err := s.storage.Create(
		ctx,
		table,
		txnOp,
	)
	if err != nil || !created {
		s.atomic.skip.Add(1)
		return err
	}

	if !s.options.disableAppendCreateCallback {
		txnOp.AppendEventCallback(
			client.ClosedEvent,
			func(txn client.TxnEvent) {
				if txn.Committed() {
					// The callback here is not guaranteed to execute after the transaction has
					// already committed.
					// The creation will lazy execute in Read.
					s.createC <- table
				} else {
					s.atomic.abort.Add(1)
				}
			},
		)
	}

	return nil
}

func (s *service) Delete(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) error {
	if !s.cfg.Enable {
		return nil
	}

	deleted, err := s.storage.Delete(ctx, table, txnOp)
	if err != nil || !deleted {
		s.atomic.skip.Add(1)
		return err
	}

	if !s.options.disableAppendDeleteCallback {
		txnOp.AppendEventCallback(
			client.ClosedEvent,
			func(txn client.TxnEvent) {
				if txn.Committed() {
					s.deleteC <- table
				} else {
					s.atomic.abort.Add(1)
				}
			},
		)
	}

	return nil
}

func (s *service) GetShardInfo(
	table uint64,
) (uint64, pb.Policy, bool, error) {
	old := s.cache.noneSharding.Load()
	if old.Contains(table) {
		return 0, 0, false, nil
	}

	r := s.getReadCache()
	v, ok := r.shards[table]
	if ok {
		return table, v.metadata.Policy, true, nil
	}
	for tid, sc := range r.shards {
		if sc.metadata.Policy != pb.Policy_Partition {
			continue
		}
		for _, id := range sc.metadata.ShardIDs {
			if id == table {
				return tid, sc.metadata.Policy, true, nil
			}
		}
	}

	tid, metadata, err := s.storage.Get(table)
	if err != nil {
		return 0, 0, false, err
	}
	if !metadata.IsEmpty() {
		return tid, metadata.Policy, true, nil
	}

	new := old.Clone()
	new.Add(table)
	s.cache.noneSharding.CompareAndSwap(old, new)
	return 0, 0, false, nil
}

func (s *service) ReplicaCount() int64 {
	return int64(s.cache.allocate.Load().replicasCount())
}

func (s *service) removeReadCache(
	table uint64,
) {
	old := s.getReadCache()
	if !old.hasTableCache(table) {
		return
	}

	new := old.clone()
	delete(new.shards, table)
	s.cache.read.CompareAndSwap(old, new)
}

func (s *service) getShards(
	table uint64,
) (*readCache, error) {
	cache := s.getReadCache()
	if cache.hasTableCache(table) {
		return cache, nil
	}

	// make sure only one goroutine to get shards from shard server
	s.cache.Lock()
	defer s.cache.Unlock()

	fn := func() (pb.ShardsMetadata, []pb.TableShard, error) {
		_, metadata, err := s.storage.Get(table)
		if err != nil {
			return pb.ShardsMetadata{}, nil, err
		}
		if metadata.Policy == pb.Policy_None {
			panic("none policy cannot call GetShards")
		}

		req := s.remote.pool.AcquireRequest()
		req.RPCMethod = pb.Method_GetShards
		req.GetShards.ID = table
		req.GetShards.Metadata = metadata

		resp, err := s.send(req)
		if err != nil {
			return pb.ShardsMetadata{}, nil, err
		}
		defer s.remote.pool.ReleaseResponse(resp)
		return metadata, resp.GetShards.Shards, nil
	}

OUT:
	for {
		cache := s.getReadCache()
		if cache.hasTableCache(table) {
			return cache, nil
		}

		metadata, shards, err := fn()
		if err != nil || len(shards) == 0 {
			s.logger.Error("failed to get table shards",
				zap.Error(err),
				zap.Int("shards", len(shards)))
			time.Sleep(time.Second)
			continue
		}
		for _, shard := range shards {
			if !shard.HasReplicaWithState(pb.ReplicaState_Running) {
				s.logger.Warn("shard is not running",
					zap.String("shard", shard.String()))
				time.Sleep(time.Second)
				continue OUT
			}
		}
		cache = s.cache.read.Load()
		cache = cache.clone()
		cache.addShards(table, metadata, shards)
		s.cache.read.Store(cache)
		return cache, nil
	}
}

func (s *service) getReadCache() *readCache {
	cache := s.cache.read.Load()
	if cache != nil {
		return cache
	}
	return nil
}

func (s *service) getAllocatedShards() []pb.TableShard {
	shards := s.cache.allocate.Load()
	return shards.values
}

func (s *service) getAllocatedShard(
	table uint64,
	shardID uint64,
) (pb.TableShard, bool) {
	shards := s.cache.allocate.Load()
	if len(shards.values) == 0 {
		return pb.TableShard{}, false
	}
	for _, shard := range shards.values {
		if shard.TableID == table &&
			shard.ShardID == shardID {
			return shard, true
		}
	}
	return pb.TableShard{}, false
}

func (s *service) doTask(
	ctx context.Context,
) {
	timer := time.NewTimer(s.cfg.HeartbeatDuration.Duration)
	defer timer.Stop()

	checkChangedTimer := time.NewTimer(s.cfg.CheckChangedDuration.Duration)
	defer checkChangedTimer.Stop()

	m := make(map[uint64]bool)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := s.doHeartbeat(m); err != nil {
				s.logger.Error("failed to heartbeat",
					zap.Error(err))
			}
			v2.ReplicaCountGauge.Set(float64(s.cache.allocate.Load().replicasCount()))
			timer.Reset(s.cfg.HeartbeatDuration.Duration)
		case table := <-s.createC:
			if err := s.handleCreateTable(table); err != nil {
				s.logger.Error("failed to create table shards",
					zap.Uint64("table", table),
					zap.Error(err))
			}
		case table := <-s.deleteC:
			if err := s.handleDeleteTable(table); err != nil {
				s.logger.Error("failed to delete table shards",
					zap.Uint64("table", table),
					zap.Error(err))
			}
		case <-checkChangedTimer.C:
			if err := s.handleCheckChanged(); err != nil {
				s.logger.Error("failed to check table shards changed",
					zap.Error(err))
			}
			checkChangedTimer.Reset(s.cfg.CheckChangedDuration.Duration)
		}
	}
}

func (s *service) doHeartbeat(
	m map[uint64]bool,
) error {
	if s.options.disableHeartbeat.Load() {
		return nil
	}

	req := s.remote.pool.AcquireRequest()
	req.RPCMethod = pb.Method_Heartbeat
	req.Heartbeat.CN = s.cfg.ServiceID
	req.Heartbeat.Shards = s.getAllocatedShards()

	resp, err := s.send(req)
	if err != nil {
		return err
	}
	defer s.remote.pool.ReleaseResponse(resp)

	ops := resp.Heartbeat.Operators
	if len(ops) == 0 {
		return nil
	}

	s.logger.Info(
		"receive new heartbeat operator",
		zap.Int("count", len(ops)),
	)

	newShards := s.cache.allocate.Load().clone()
	for _, op := range ops {
		switch op.Type {
		case pb.OpType_AddReplica:
			v2.AddReplicaOperatorCounter.Inc()

			s.logger.Info(
				"handle add replica",
				zap.String("shard", op.TableShard.String()),
				zap.String("replica", op.Replica.String()),
			)

			s.handleAddReplica(
				newShards,
				op.TableShard,
				op.Replica,
			)
		case pb.OpType_DeleteReplica:
			v2.DeleteReplicaOperatorCounter.Inc()

			s.logger.Info(
				"handle delete replica",
				zap.String("shard", op.TableShard.String()),
				zap.String("replica", op.Replica.String()),
			)

			s.handleDeleteReplica(
				newShards,
				op.TableShard,
				op.Replica,
			)
		case pb.OpType_DeleteAll:
			v2.DeleteAllReplicaOperatorCounter.Inc()

			s.logger.Info(
				"handle delete all replicas",
			)

			s.handleDeleteAll(
				newShards,
			)
		case pb.OpType_CreateTable:
			s.logger.Info(
				"handle create shards",
				zap.String("service", s.cfg.ServiceID),
				zap.Uint64("table", op.TableID),
			)

			s.handleCreateTable(
				op.TableID,
			)
		}
	}

	for k := range m {
		delete(m, k)
	}
	for _, op := range newShards.ops {
		for {
			var err error
			unsubscribed := m[op]
			if !unsubscribed {
				err = s.storage.Unsubscribe(op)
			}
			if err == nil {
				m[op] = true
				break
			}
			s.logger.Error("failed to unsubscribe",
				zap.Uint64("table", op),
				zap.Error(err),
			)
			time.Sleep(time.Second)
		}
	}
	s.cache.allocate.Store(newShards)
	return nil
}

func (s *service) handleAddReplica(
	newShards *allocatedCache,
	shard pb.TableShard,
	replica pb.ShardReplica,
) {
	if !newShards.add(shard, replica) {
		return
	}
	s.atomic.added.Add(1)
	if shard.Policy != pb.Policy_Partition &&
		newShards.count(shard.TableID) > 1 {
		newShards.addUnsubscribe(shard.GetRealTableID())
	}
}

func (s *service) handleDeleteReplica(
	newShards *allocatedCache,
	shard pb.TableShard,
	replica pb.ShardReplica,
) {
	if !newShards.delete(shard, replica) {
		return
	}
	s.atomic.removed.Add(1)
	if shard.Policy == pb.Policy_Partition ||
		newShards.count(shard.TableID) == 0 {
		newShards.addUnsubscribe(shard.GetRealTableID())
	}
}

func (s *service) handleDeleteAll(
	newShards *allocatedCache,
) {
	for _, shard := range newShards.values {
		newShards.addUnsubscribe(shard.GetRealTableID())
	}
	newShards.clean()
}

func (s *service) handleCreateTable(
	tableID uint64,
) error {
	_, metadata, err := s.storage.Get(tableID)
	if err != nil {
		return err
	}
	if metadata.Policy == pb.Policy_None {
		return nil
	}

	req := s.remote.pool.AcquireRequest()
	req.RPCMethod = pb.Method_CreateShards
	req.CreateShards.ID = tableID
	req.CreateShards.Metadata = metadata

	resp, err := s.send(req)
	if err != nil {
		return err
	}
	s.remote.pool.ReleaseResponse(resp)

	s.logger.Info("table shards created",
		zap.Uint64("table", tableID),
		zap.String("shards", metadata.String()))
	return nil
}

func (s *service) handleDeleteTable(
	tableID uint64,
) error {
	s.removeCache(tableID)

	req := s.remote.pool.AcquireRequest()
	req.RPCMethod = pb.Method_DeleteShards
	req.DeleteShards.ID = tableID

	resp, err := s.send(req)
	if err != nil {
		return errors.Join(
			err,
			s.storage.Unsubscribe(tableID))
	}
	s.remote.pool.ReleaseResponse(resp)

	return s.storage.Unsubscribe(tableID)
}

func (s *service) handleCheckChanged() error {
	shards := s.getAllocatedShards()
	if len(shards) == 0 {
		return nil
	}

	m := make(map[uint64]uint32)
	for _, shard := range shards {
		m[shard.TableID] = shard.Version
	}
	return s.storage.GetChanged(
		m,
		func(deleted uint64) {
			select {
			case s.deleteC <- deleted:
			default:
			}
		},
		func(changed uint64) {
			select {
			case s.createC <- changed:
			default:
			}
		},
	)
}

func (s *service) removeCache(
	tableID uint64,
) {
	cache := s.cache.read.Load()
	if cache != nil {
		cache.delete(tableID)
		s.cache.read.Store(cache)
	}
}

func (s *service) maybeRemoveReadCache(
	table uint64,
	err error,
) {
	if moerr.IsMoErrCode(err, moerr.ErrReplicaNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrReplicaNotMatch) {
		s.removeReadCache(table)
	}
}

func (s *service) waitShardCreated(
	ctx context.Context,
	tableID uint64,
	shardID uint64,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			cache, err := s.getShards(tableID)
			if err != nil {
				return err
			}

			if cache.hasShard(tableID, shardID) {
				return nil
			}

			if cache.hasTableCache(tableID) {
				s.removeReadCache(tableID)
			}
		}
		time.Sleep(s.cfg.HeartbeatDuration.Duration)
	}
}

type allocatedCache struct {
	values []pb.TableShard
	ops    []uint64
}

func newAllocatedCache() *allocatedCache {
	return &allocatedCache{}
}

func (s *allocatedCache) add(
	shard pb.TableShard,
	replica pb.ShardReplica,
) bool {
	for i, v := range s.values {
		if !v.Same(shard) {
			continue
		}
		if v.GetReplica(replica) != -1 {
			return false
		}
		s.values[i].Replicas = append(s.values[i].Replicas, replica)
		return true
	}

	shard.Replicas = []pb.ShardReplica{replica}
	s.values = append(s.values, shard)
	return true
}

func (s *allocatedCache) delete(
	shard pb.TableShard,
	replica pb.ShardReplica,
) bool {
	for i, v := range s.values {
		if !v.Same(shard) {
			continue
		}

		idx := v.GetReplica(replica)
		if idx == -1 {
			return false
		}
		// remove shard
		if len(v.Replicas) == 1 {
			s.values = append(s.values[:i], s.values[i+1:]...)
			return true
		}
		// only remove replica
		s.values[i].Replicas = append(v.Replicas[:idx], v.Replicas[idx+1:]...)
		return true
	}
	return false
}

func (s *allocatedCache) count(
	tableID uint64,
) int {
	count := 0
	for _, v := range s.values {
		if v.TableID == tableID {
			count += len(v.Replicas)
		}
	}
	return count
}

func (s *allocatedCache) String() string {
	return fmt.Sprintf("%+v", s.values)
}

func (s *allocatedCache) clean() {
	s.values = s.values[:0]
}

func (s *allocatedCache) clone() *allocatedCache {
	clone := newAllocatedCache()
	clone.values = append(([]pb.TableShard)(nil), s.values...)
	return clone
}

func (s *allocatedCache) replicasCount() int {
	n := 0
	for _, v := range s.values {
		n += len(v.Replicas)
	}
	return n
}

func (s *allocatedCache) addUnsubscribe(
	tableID uint64,
) {
	s.ops = append(s.ops, tableID)
}

func (s *service) isLocalReplica(
	replica pb.ShardReplica,
) bool {
	return s.cfg.ServiceID == replica.CN
}

type readCache struct {
	shards map[uint64]shardsCache
}

func newReadCache() *readCache {
	return &readCache{
		shards: make(map[uint64]shardsCache),
	}
}

func (c *readCache) selectReplicas(
	tableID uint64,
	apply func(pb.ShardsMetadata, pb.TableShard, pb.ShardReplica) bool,
) {
	sc, ok := c.shards[tableID]
	if !ok {
		panic("shards is empty")
	}

	sc.selectReplicas(apply)
}

func (c *readCache) hasTableCache(
	tableID uint64,
) bool {
	if c == nil {
		return false
	}
	_, ok := c.shards[tableID]
	return ok
}

func (c *readCache) hasShard(
	tableID uint64,
	shardID uint64,
) bool {
	sc, ok := c.shards[tableID]
	if !ok {
		return false
	}
	for _, s := range sc.shards {
		if s.ShardID == shardID {
			return true
		}
	}
	return false
}

func (c *readCache) clone() *readCache {
	clone := newReadCache()
	for k, v := range c.shards {
		clone.shards[k] = v
	}
	return clone
}

func (c *readCache) addShards(
	table uint64,
	metadata pb.ShardsMetadata,
	shards []pb.TableShard,
) {
	// skip tombstone replica
	for i := range shards {
		replicas := shards[i].Replicas[:0]
		for _, r := range shards[i].Replicas {
			if r.State == pb.ReplicaState_Tombstone {
				continue
			}
			replicas = append(replicas, r)
		}
		shards[i].Replicas = replicas
	}

	c.shards[table] = shardsCache{
		metadata: metadata,
		shards:   shards,
		ops:      make([]uint64, len(shards)),
	}
}

func (c *readCache) delete(
	tableID uint64,
) {
	delete(c.shards, tableID)
}

func getTNAddress(
	cluster clusterservice.MOCluster,
) string {
	address := ""
	cluster.GetTNService(
		clusterservice.NewSelector(),
		func(t metadata.TNService) bool {
			address = t.ShardServiceAddress
			return true
		},
	)
	return address
}

func getCNAddress(
	cn string,
	cluster clusterservice.MOCluster,
) string {
	address := ""
	cluster.GetCNService(
		clusterservice.NewSelector().SelectByServiceID(cn),
		func(t metadata.CNService) bool {
			address = t.ShardServiceAddress
			return true
		},
	)
	return address
}

type shardsCache struct {
	metadata pb.ShardsMetadata
	shards   []pb.TableShard
	ops      []uint64
}

func (sc *shardsCache) selectReplicas(
	apply func(pb.ShardsMetadata, pb.TableShard, pb.ShardReplica) bool,
) {
	for i, shard := range sc.shards {
		seq := atomic.AddUint64(&sc.ops[i], 1)
		if !apply(
			sc.metadata,
			shard,
			shard.Replicas[seq%uint64(len(shard.Replicas))],
		) {
			return
		}
	}
}
