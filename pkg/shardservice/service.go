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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

type Option func(*service)

func withDisableHeartbeat() Option {
	return func(s *service) {
		s.options.disableHeartbeat.Store(true)
	}
}

type service struct {
	cfg     Config
	storage ShardStorage

	stopper *stopper.Stopper
	createC chan uint64
	deleteC chan uint64

	cache struct {
		sync.Mutex

		// allocate all shards which allocated in the current node
		allocate atomic.Pointer[allocatedCache]
		// read all shards which read in the current node
		read atomic.Pointer[readCache]
	}

	remote struct {
		cluster clusterservice.MOCluster
		pool    morpc.MessagePool[*pb.Request, *pb.Response]
		rpc     morpc.MethodBasedClient[*pb.Request, *pb.Response]
	}

	atomic struct {
		abort atomic.Uint64
		skip  atomic.Uint64
	}

	options struct {
		disableHeartbeat atomic.Bool
	}
}

func NewService(
	cfg Config,
	storage ShardStorage,
	opts ...Option,
) ShardService {
	s := &service{
		cfg:     cfg,
		storage: storage,
		createC: make(chan uint64, 16),
		deleteC: make(chan uint64, 16),
		stopper: stopper.NewStopper(
			"shard-service",
			stopper.WithLogger(getLogger().RawLogger()),
		),
	}

	s.cache.read.Store(newReadCache())
	s.cache.allocate.Store(newAllocatedCache())

	for _, opt := range opts {
		opt(s)
	}

	s.validate()
	s.initRemote()
	if err := s.stopper.RunTask(s.heartbeat); err != nil {
		panic(err)
	}
	return s
}

func (s *service) validate() {
	if s.storage == nil {
		panic("storage is nil")
	}
}

func (s *service) Close() error {
	s.stopper.Stop()
	close(s.createC)
	close(s.deleteC)
	return s.remote.rpc.Close()
}

func (s *service) Create(
	table uint64,
	txnOp client.TxnOperator,
) error {
	created, err := s.storage.Create(
		table,
		txnOp,
	)
	if err != nil || !created {
		s.atomic.skip.Add(1)
		return err
	}

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		func(txn client.TxnEvent) {
			if txn.Committed() {
				s.createC <- table
			} else {
				s.atomic.abort.Add(1)
			}
		},
	)
	return nil
}

func (s *service) Delete(
	table uint64,
	txnOp client.TxnOperator,
) error {
	deleted, err := s.storage.Delete(table, txnOp)
	if err != nil || !deleted {
		s.atomic.skip.Add(1)
		return err
	}

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
	return nil
}

func (s *service) GetShards(
	table uint64,
) ([]pb.TableShard, error) {
	shards := s.doGetReadCache(table)
	if len(shards) > 0 {
		return shards, nil
	}

	// make sure only one goroutine to get shards from
	// shard server
	s.cache.Lock()
	defer s.cache.Unlock()

	fn := func() ([]pb.TableShard, error) {
		shards = s.doGetReadCache(table)
		if len(shards) > 0 {
			return shards, nil
		}

		metadata, err := s.storage.Get(table)
		if err != nil {
			return nil, err
		}
		if metadata.Policy == pb.Policy_None {
			panic("none shards cannot call GetShards")
		}

		req := s.remote.pool.AcquireRequest()
		req.RPCMethod = pb.Method_GetShards
		req.GetShards.ID = table
		req.GetShards.Metadata = metadata

		resp, err := s.send(req)
		if err != nil {
			return nil, err
		}
		defer s.remote.pool.ReleaseResponse(resp)
		return resp.GetShards.Shards, nil
	}

OUT:
	for {
		shards, err := fn()
		if err != nil || len(shards) == 0 {
			getLogger().Error("failed to get table shards",
				zap.Error(err),
				zap.Int("shards", len(shards)))
			time.Sleep(time.Second)
			continue
		}
		for _, shard := range shards {
			if shard.State != pb.ShardState_Running {
				getLogger().Warn("shard is not running",
					zap.String("shard", shard.String()))
				time.Sleep(time.Second)
				continue OUT
			}
		}
		cache := s.cache.read.Load()
		cache = cache.clone()
		cache.shards[table] = shards
		return shards, nil
	}
}

func (s *service) initRemote() {
	s.remote.cluster = clusterservice.GetMOCluster()

	s.remote.pool = morpc.NewMessagePool(
		func() *pb.Request {
			return &pb.Request{}
		},
		func() *pb.Response {
			return &pb.Response{}
		},
	)

	c, err := morpc.NewMethodBasedClient(
		"shard-client",
		s.cfg.RPC,
		s.remote.pool,
	)
	if err != nil {
		panic(err)
	}
	s.remote.rpc = c

	// register rpc method
	s.remote.rpc.RegisterMethod(
		uint32(pb.Method_Heartbeat),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.rpc.RegisterMethod(
		uint32(pb.Method_CreateShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.rpc.RegisterMethod(
		uint32(pb.Method_DeleteShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.rpc.RegisterMethod(
		uint32(pb.Method_GetShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
}

func (s *service) doGetReadCache(
	table uint64,
) []pb.TableShard {
	cache := s.cache.read.Load()
	if cache != nil {
		return cache.shards[table]
	}
	return nil
}

func (s *service) getAllocatedShards() []pb.TableShard {
	shards := s.cache.allocate.Load()
	return shards.values
}

func (s *service) heartbeat(
	ctx context.Context,
) {
	timer := time.NewTimer(s.cfg.CNHeartbeatDuration.Duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := s.doHeartbeat(); err != nil {
				getLogger().Error("failed to heartbeat",
					zap.Error(err))
			}
			timer.Reset(s.cfg.CNHeartbeatDuration.Duration)
		case table := <-s.createC:
			if err := s.handleCreateTable(table); err != nil {
				getLogger().Error("failed to create table shards",
					zap.Uint64("table", table),
					zap.Error(err))
			}
		case table := <-s.deleteC:
			if err := s.handleDeleteTable(table); err != nil {
				getLogger().Error("failed to delete table shards",
					zap.Uint64("table", table),
					zap.Error(err))
			}
		}
	}
}

func (s *service) doHeartbeat() error {
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

	getLogger().Info(
		"receive new heartbeat operator",
		zap.Int("count", len(ops)),
	)

	newShards := s.cache.allocate.Load().clone()
	for _, op := range ops {
		getLogger().Info(
			"handle heartbeat operator",
			zap.String("op", op.String()),
		)
		switch op.Type {
		case pb.OpType_AddShard:
			s.handleAddShard(
				newShards,
				op.TableShard,
			)
		case pb.OpType_DeleteShard:
			s.handleDeleteShard(
				newShards,
				op.TableShard,
			)
		case pb.OpType_DeleteALL:
			s.handleDeleteAll(
				newShards,
			)
		case pb.OpType_CreateTable:
			s.handleCreateTable(
				op.TableID,
			)
		}
	}

	for _, op := range newShards.ops {
		for {
			var err error
			if op.isSubscribe() {
				err = s.storage.Subscribe(op.tableID)
			} else {
				err = s.storage.Unsubscribe(op.tableID)
			}
			if err == nil {
				break
			}
			getLogger().Error("failed to subscribe/unsubscribe",
				zap.Uint64("table", op.tableID),
				zap.Error(err),
				zap.Bool("subscribe", op.isSubscribe()),
			)
			time.Sleep(time.Second)
		}
	}
	s.cache.allocate.Store(newShards)
	return nil
}

func (s *service) handleAddShard(
	newShards *allocatedCache,
	shard pb.TableShard,
) {
	if !newShards.add(shard) {
		return
	}

	if shard.Policy != pb.Policy_Partition &&
		newShards.count(shard.TableID) > 1 {
		newShards.addSubscribeOp(shard.GetPhysicalTableID(), forceUnsubscribe)
	}
	newShards.addSubscribeOp(shard.GetPhysicalTableID(), subscribe)
}

func (s *service) handleDeleteShard(
	newShards *allocatedCache,
	shard pb.TableShard,
) {
	if !newShards.delete(shard) {
		return
	}
	if shard.Policy == pb.Policy_Partition ||
		newShards.count(shard.TableID) == 0 {
		newShards.addSubscribeOp(shard.GetPhysicalTableID(), unsubscribe)
	}
}

func (s *service) handleDeleteAll(
	newShards *allocatedCache,
) {
	for _, shard := range newShards.values {
		newShards.addSubscribeOp(shard.GetPhysicalTableID(), unsubscribe)
	}
	newShards.clean()
}

func (s *service) handleCreateTable(
	tableID uint64,
) error {
	shards, err := s.storage.Get(tableID)
	if err != nil {
		return err
	}
	if shards.Policy == pb.Policy_None {
		return nil
	}

	req := s.remote.pool.AcquireRequest()
	req.RPCMethod = pb.Method_CreateShards
	req.CreateShards.ID = tableID
	req.CreateShards.Metadata = shards

	resp, err := s.send(req)
	if err != nil {
		return err
	}
	s.remote.pool.ReleaseResponse(resp)

	getLogger().Info("table shards created",
		zap.Uint64("table", tableID),
		zap.String("shards", shards.String()))
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

func (s *service) removeCache(
	tableID uint64,
) {
	cache := s.cache.read.Load()
	if cache != nil {
		cache.delete(tableID)
		s.cache.read.Store(cache)
	}
}

func (s *service) send(
	req *pb.Request,
) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := s.remote.rpc.Send(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := resp.UnwrapError(); err != nil {
		s.remote.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}

var (
	subscribe        = 0
	unsubscribe      = 1
	forceUnsubscribe = 2
)

type subscribeOp struct {
	tableID uint64
	flag    int
}

func (s subscribeOp) same(flag int) bool {
	return s.flag == flag
}

func (s subscribeOp) isSubscribe() bool {
	return s.flag == subscribe
}

type allocatedCache struct {
	values []pb.TableShard
	ops    []subscribeOp
}

func newAllocatedCache() *allocatedCache {
	return &allocatedCache{}
}

func (s *allocatedCache) add(
	shard pb.TableShard,
) bool {
	for _, v := range s.values {
		if v.Same(shard) {
			return false
		}
	}
	s.values = append(s.values, shard)
	return true
}

func (s *allocatedCache) delete(
	shard pb.TableShard,
) bool {
	for i, v := range s.values {
		if !v.Same(shard) {
			continue
		}
		s.values = append(s.values[:i], s.values[i+1:]...)
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
			count++
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

func (s *allocatedCache) addSubscribeOp(
	tableID uint64,
	flag int,
) {
	skip := -1
	if flag != forceUnsubscribe {
		for i, op := range s.ops {
			if op.flag == forceUnsubscribe ||
				op.tableID != tableID {
				continue
			}

			if op.same(flag) {
				return
			} else {
				// here, means conflict, need to skip
				skip = i
				break
			}
		}
	}

	s.ops = append(s.ops, subscribeOp{tableID: tableID, flag: flag})
	if skip != -1 {
		s.ops = append(s.ops[:skip], s.ops[skip+1:]...)
	}
}

type readCache struct {
	shards map[uint64][]pb.TableShard
}

func newReadCache() *readCache {
	return &readCache{shards: make(map[uint64][]pb.TableShard)}
}

func (c *readCache) clone() *readCache {
	clone := newReadCache()
	for k, v := range c.shards {
		clone.shards[k] = append(([]pb.TableShard)(nil), v...)
	}
	return clone
}

func (c *readCache) delete(
	tableID uint64,
) {
	delete(c.shards, tableID)
}

func getTNAddress(cluster clusterservice.MOCluster) string {
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
