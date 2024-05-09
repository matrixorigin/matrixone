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

package tnservice

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

var (
	retryCreateStorageInterval = time.Second * 5
)

// WithConfigAdjust set adjust config func
func WithConfigAdjust(adjustConfigFunc func(c *Config)) Option {
	return func(s *store) {
		s.options.adjustConfigFunc = adjustConfigFunc
	}
}

// WithBackendFilter set filtering txn.TxnRequest sent to other DNShard
func WithBackendFilter(filter func(morpc.Message, string) bool) Option {
	return func(s *store) {
		s.options.backendFilter = filter
	}
}

// WithHAKeeperClientFactory set hakeeper client factory
func WithHAKeeperClientFactory(factory func() (logservice.TNHAKeeperClient, error)) Option {
	return func(s *store) {
		s.options.hakeekerClientFactory = factory
	}
}

// WithLogServiceClientFactory set log service client factory
func WithLogServiceClientFactory(factory func(metadata.TNShard) (logservice.Client, error)) Option {
	return func(s *store) {
		s.options.logServiceClientFactory = factory
	}
}

// WithTaskStorageFactory setup the special task strorage factory
func WithTaskStorageFactory(factory taskservice.TaskStorageFactory) Option {
	return func(s *store) {
		s.task.storageFactory = factory
	}
}

// WithConfigData saves the data from the config file
func WithConfigData(data map[string]*logservicepb.ConfigItem) Option {
	return func(s *store) {
		if s.config == nil {
			s.config = util.NewConfigData(data)
		} else {
			util.MergeConfig(s.config, data)
		}
	}
}

type store struct {
	cfg                 *Config
	rt                  runtime.Runtime
	sender              rpc.TxnSender
	server              rpc.TxnServer
	hakeeperClient      logservice.TNHAKeeperClient
	fileService         fileservice.FileService
	metadataFileService fileservice.ReplaceableFileService
	lockTableAllocator  lockservice.LockTableAllocator
	moCluster           clusterservice.MOCluster
	replicas            *sync.Map
	stopper             *stopper.Stopper
	shutdownC           chan struct{}

	options struct {
		logServiceClientFactory func(metadata.TNShard) (logservice.Client, error)
		hakeekerClientFactory   func() (logservice.TNHAKeeperClient, error)
		backendFilter           func(msg morpc.Message, backendAddr string) bool
		adjustConfigFunc        func(c *Config)
	}

	mu struct {
		sync.RWMutex
		metadata metadata.TNStore
	}

	task struct {
		sync.RWMutex
		serviceCreated bool
		serviceHolder  taskservice.TaskServiceHolder
		storageFactory taskservice.TaskStorageFactory
	}

	addressMgr address.AddressManager

	config *util.ConfigData
	// queryService for getting cache info from tnservice
	queryService queryservice.QueryService
}

// NewService create TN Service
func NewService(
	cfg *Config,
	rt runtime.Runtime,
	fileService fileservice.FileService,
	shutdownC chan struct{},
	opts ...Option) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	configKVMap, _ := dumpTnConfig(*cfg)
	opts = append(opts, WithConfigData(configKVMap))

	// start common stuff
	common.InitTAEMPool()

	// get metadata fs
	metadataFS, err := fileservice.Get[fileservice.ReplaceableFileService](fileService, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}

	// start I/O pipeline
	blockio.Start()

	s := &store{
		cfg:                 cfg,
		rt:                  rt,
		fileService:         fileService,
		metadataFileService: metadataFS,
		shutdownC:           shutdownC,
		addressMgr:          address.NewAddressManager(cfg.ServiceHost, cfg.PortBase),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerServices()
	s.replicas = &sync.Map{}
	s.stopper = stopper.NewStopper("dn-store",
		stopper.WithLogger(s.rt.Logger().RawLogger()))
	s.mu.metadata = metadata.TNStore{UUID: cfg.UUID}
	if s.options.adjustConfigFunc != nil {
		s.options.adjustConfigFunc(s.cfg)
	}

	if err := s.initClocker(); err != nil {
		return nil, err
	}
	if err := s.initHAKeeperClient(); err != nil {
		return nil, err
	}
	if err := s.initLockTableAllocator(); err != nil {
		return nil, err
	}
	if err := s.initTxnSender(); err != nil {
		return nil, err
	}
	if err := s.initTxnServer(); err != nil {
		return nil, err
	}
	if err := s.initMetadata(); err != nil {
		return nil, err
	}

	s.initQueryService(cfg.InStandalone)

	s.initTaskHolder()
	s.initSqlWriterFactory()
	s.setupStatusServer()

	return s, nil
}

func (s *store) Start() error {
	if err := s.startTNShards(); err != nil {
		return err
	}
	if err := s.server.Start(); err != nil {
		return err
	}
	if s.queryService != nil {
		if err := s.queryService.Start(); err != nil {
			return err
		}
	}
	s.rt.SubLogger(runtime.SystemInit).Info("dn heartbeat task started")
	return s.stopper.RunTask(s.heartbeatTask)
}

func (s *store) Close() error {
	s.stopper.Stop()
	s.moCluster.Close()
	err := errors.Join(
		s.hakeeperClient.Close(),
		s.sender.Close(),
		s.server.Close(),
		s.lockTableAllocator.Close(),
	)
	s.replicas.Range(func(_, value any) bool {
		r := value.(*replica)
		if e := r.close(false); e != nil {
			err = errors.Join(e, err)
		}
		return true
	})
	s.task.RLock()
	ts := s.task.serviceHolder
	s.task.RUnlock()
	if ts != nil {
		err = errors.Join(err, ts.Close())
	}
	// stop I/O pipeline
	blockio.Stop()
	return err
}

func (s *store) StartTNReplica(shard metadata.TNShard) error {
	return s.createReplica(shard)
}

func (s *store) CloseTNReplica(shard metadata.TNShard) error {
	return s.removeReplica(shard.ShardID)
}

func (s *store) startTNShards() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, shard := range s.mu.metadata.Shards {
		if err := s.createReplica(shard); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) getTNShardInfo() []logservicepb.TNShardInfo {
	var shards []logservicepb.TNShardInfo
	s.replicas.Range(func(_, value any) bool {
		r := value.(*replica)
		shards = append(shards, logservicepb.TNShardInfo{
			ShardID:   r.shard.ShardID,
			ReplicaID: r.shard.ReplicaID,
		})
		return true
	})
	return shards
}

func (s *store) createReplica(shard metadata.TNShard) error {
	r := newReplica(shard, s.rt)
	v, ok := s.replicas.LoadOrStore(shard.ShardID, r)
	if ok {
		s.rt.Logger().Debug("DNShard already created",
			zap.String("new", shard.DebugString()),
			zap.String("exist", v.(*replica).shard.DebugString()))
		return nil
	}

	err := s.stopper.RunTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				storage, err := s.createTxnStorage(ctx, shard)
				if err != nil {
					r.logger.Error("start DNShard failed",
						zap.Error(err))
					time.Sleep(retryCreateStorageInterval)
					continue
				}

				err = r.start(service.NewTxnService(shard, storage, s.sender, s.cfg.Txn.ZombieTimeout.Duration, s.lockTableAllocator))
				if err != nil {
					r.logger.Fatal("start DNShard failed",
						zap.Error(err))
				}
				return
			}
		}
	})
	if err != nil {
		return err
	}

	s.addTNShardLocked(shard)
	return nil
}

func (s *store) removeReplica(tnShardID uint64) error {
	if r := s.getReplica(tnShardID); r != nil {
		err := r.close(true)
		s.replicas.Delete(tnShardID)
		s.removeTNShard(tnShardID)
		return err
	}
	return nil
}

func (s *store) getReplica(id uint64) *replica {
	v, ok := s.replicas.Load(id)
	if !ok {
		return nil
	}
	return v.(*replica)
}

func (s *store) initTxnSender() error {
	s.cfg.RPC.BackendOptions = append(s.cfg.RPC.BackendOptions,
		morpc.WithBackendFilter(func(m morpc.Message, backendAddr string) bool {
			return s.options.backendFilter == nil || s.options.backendFilter(m.(*txn.TxnRequest), backendAddr)
		}))
	sender, err := rpc.NewSender(
		s.cfg.RPC,
		s.rt,
		rpc.WithSenderLocalDispatch(s.dispatchLocalRequest))
	if err != nil {
		return err
	}
	s.sender = sender
	return nil
}

func (s *store) initTxnServer() error {
	server, err := rpc.NewTxnServer(
		s.txnServiceListenAddr(),
		s.rt,
		rpc.WithServerQueueBufferSize(s.cfg.RPC.ServerBufferQueueSize),
		rpc.WithServerQueueWorkers(s.cfg.RPC.ServerWorkers),
		rpc.WithServerMaxMessageSize(int(s.cfg.RPC.MaxMessageSize)),
		rpc.WithServerEnableCompress(s.cfg.RPC.EnableCompress))
	if err != nil {
		return err
	}
	s.server = server
	s.registerRPCHandlers()
	return nil
}

func (s *store) initClocker() error {
	if s.rt.Clock() == nil {
		return moerr.NewBadConfigNoCtx("missing txn clock")
	}
	return nil
}

func (s *store) initLockTableAllocator() error {
	s.lockTableAllocator = lockservice.NewLockTableAllocator(
		s.lockServiceListenAddr(),
		s.cfg.LockService.KeepBindTimeout.Duration,
		s.cfg.RPC)
	return nil
}

func (s *store) initHAKeeperClient() error {
	if s.options.hakeekerClientFactory != nil {
		client, err := s.options.hakeekerClientFactory()
		if err != nil {
			return err
		}
		s.hakeeperClient = client
		s.initClusterService()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.DiscoveryTimeout.Duration)
	defer cancel()
	client, err := logservice.NewTNHAKeeperClient(ctx, s.cfg.HAKeeper.ClientConfig)
	if err != nil {
		return err
	}
	s.hakeeperClient = client
	s.initClusterService()
	return nil
}

func (s *store) initClusterService() {
	s.moCluster = clusterservice.NewMOCluster(s.hakeeperClient,
		s.cfg.Cluster.RefreshInterval.Duration)
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, s.moCluster)
}

// initQueryService
// inStandalone:
//
//	true: tn is boosted in a standalone cluster. cn has a queryservice already.
//	false: tn is boosted in an independent process. tn needs a queryservice.
func (s *store) initQueryService(inStandalone bool) {
	if inStandalone {
		s.queryService = nil
		return
	}
	var err error
	s.queryService, err = queryservice.NewQueryService(s.cfg.UUID,
		s.queryServiceListenAddr(), s.cfg.RPC)
	if err != nil {
		panic(err)
	}
	s.initQueryCommandHandler()
}

func (s *store) initQueryCommandHandler() {
	s.queryService.AddHandleFunc(query.CmdMethod_GetCacheInfo, s.handleGetCacheInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetLatestBind, s.handleGetLatestBind, false)
}

func (s *store) handleGetCacheInfo(ctx context.Context, req *query.Request, resp *query.Response) error {
	resp.GetCacheInfoResponse = new(query.GetCacheInfoResponse)
	perfcounter.GetCacheStats(func(infos []*query.CacheInfo) {
		for _, info := range infos {
			if info != nil {
				resp.GetCacheInfoResponse.CacheInfoList = append(resp.GetCacheInfoResponse.CacheInfoList, info)
			}
		}
	})

	return nil
}

func (s *store) handleGetLatestBind(ctx context.Context, req *query.Request, resp *query.Response) error {
	resp.GetLatestBind = &query.GetLatestBindResponse{
		Bind: s.lockTableAllocator.GetLatest(
			req.GetLatestBind.GroupID,
			req.GetLatestBind.TableID).
			DebugString(),
	}
	return nil
}

func (s *store) setupStatusServer() {
	ss, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.StatusServer)
	if ok {
		ss.(*status.Server).SetHAKeeperClient(s.hakeeperClient)
	}

}
