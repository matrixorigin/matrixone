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

package dnservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var (
	retryCreateStorageInterval = time.Second * 5
)

// WithLogger set logger
func WithLogger(logger *zap.Logger) Option {
	return func(s *store) {
		s.logger = logger
	}
}

// WithClock set clock
func WithClock(clock clock.Clock) Option {
	return func(s *store) {
		s.clock = clock
	}
}

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
func WithHAKeeperClientFactory(factory func() (logservice.DNHAKeeperClient, error)) Option {
	return func(s *store) {
		s.options.hakeekerClientFactory = factory
	}
}

// WithLogServiceClientFactory set log service client factory
func WithLogServiceClientFactory(factory func(metadata.DNShard) (logservice.Client, error)) Option {
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

type store struct {
	cfg                 *Config
	logger              *zap.Logger
	clock               clock.Clock
	sender              rpc.TxnSender
	server              rpc.TxnServer
	hakeeperClient      logservice.DNHAKeeperClient
	fileService         fileservice.FileService
	metadataFileService fileservice.ReplaceableFileService
	replicas            *sync.Map
	stopper             *stopper.Stopper

	options struct {
		logServiceClientFactory func(metadata.DNShard) (logservice.Client, error)
		hakeekerClientFactory   func() (logservice.DNHAKeeperClient, error)
		backendFilter           func(msg morpc.Message, backendAddr string) bool
		adjustConfigFunc        func(c *Config)
	}

	mu struct {
		sync.RWMutex
		metadata metadata.DNStore
	}

	task struct {
		sync.RWMutex
		serviceCreated bool
		serviceHolder  taskservice.TaskServiceHolder
		storageFactory taskservice.TaskStorageFactory
	}
}

// NewService create DN Service
func NewService(cfg *Config,
	fileService fileservice.FileService,
	opts ...Option) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// start common stuff
	common.InitTAEMPool()

	// get metadata fs
	metadataFS, err := fileservice.Get[fileservice.ReplaceableFileService](fileService, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}

	s := &store{
		cfg:                 cfg,
		fileService:         fileService,
		metadataFileService: metadataFS,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.logger = logutil.Adjust(s.logger)
	s.replicas = &sync.Map{}
	s.stopper = stopper.NewStopper("dn-store", stopper.WithLogger(s.logger))
	s.mu.metadata = metadata.DNStore{UUID: cfg.UUID}
	if s.options.adjustConfigFunc != nil {
		s.options.adjustConfigFunc(s.cfg)
	}

	if err := s.initClocker(); err != nil {
		return nil, err
	}
	if err := s.initHAKeeperClient(); err != nil {
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
	s.initTaskHolder()
	return s, nil
}

func (s *store) Start() error {
	if err := s.startDNShards(); err != nil {
		return err
	}
	if err := s.server.Start(); err != nil {
		return err
	}
	s.logger.Info("dn heartbeat task started")
	return s.stopper.RunTask(s.heartbeatTask)
}

func (s *store) Close() error {
	s.stopper.Stop()
	var err error
	if e := s.hakeeperClient.Close(); e != nil {
		err = multierr.Append(e, err)
	}
	if e := s.sender.Close(); e != nil {
		err = multierr.Append(e, err)
	}
	if e := s.server.Close(); e != nil {
		err = multierr.Append(e, err)
	}
	s.replicas.Range(func(_, value any) bool {
		r := value.(*replica)
		if e := r.close(false); e != nil {
			err = multierr.Append(e, err)
		}
		return true
	})
	s.task.RLock()
	ts := s.task.serviceHolder
	s.task.RUnlock()
	if ts != nil {
		err = ts.Close()
	}
	return err
}

func (s *store) StartDNReplica(shard metadata.DNShard) error {
	return s.createReplica(shard)
}

func (s *store) CloseDNReplica(shard metadata.DNShard) error {
	return s.removeReplica(shard.ShardID)
}

func (s *store) startDNShards() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, shard := range s.mu.metadata.Shards {
		if err := s.createReplica(shard); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) getDNShardInfo() []logservicepb.DNShardInfo {
	var shards []logservicepb.DNShardInfo
	s.replicas.Range(func(_, value any) bool {
		r := value.(*replica)
		shards = append(shards, logservicepb.DNShardInfo{
			ShardID:   r.shard.ShardID,
			ReplicaID: r.shard.ReplicaID,
		})
		return true
	})
	return shards
}

func (s *store) createReplica(shard metadata.DNShard) error {
	r := newReplica(shard, s.logger.With(util.TxnDNShardField(shard)))
	v, ok := s.replicas.LoadOrStore(shard.ShardID, r)
	if ok {
		s.logger.Debug("DNShard already created",
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
				storage, err := s.createTxnStorage(shard)
				if err != nil {
					r.logger.Error("start DNShard failed",
						zap.Error(err))
					time.Sleep(retryCreateStorageInterval)
					continue
				}

				err = r.start(service.NewTxnService(r.logger,
					shard,
					storage,
					s.sender,
					s.clock,
					s.cfg.Txn.ZombieTimeout.Duration))
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

	s.addDNShardLocked(shard)
	return nil
}

func (s *store) removeReplica(dnShardID uint64) error {
	if r := s.getReplica(dnShardID); r != nil {
		err := r.close(true)
		s.replicas.Delete(dnShardID)
		s.removeDNShard(dnShardID)
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
	sender, err := rpc.NewSenderWithConfig(s.cfg.RPC,
		s.clock,
		s.logger,
		rpc.WithSenderBackendOptions(morpc.WithBackendFilter(func(m morpc.Message, backendAddr string) bool {
			return s.options.backendFilter == nil || s.options.backendFilter(m.(*txn.TxnRequest), backendAddr)
		})),
		rpc.WithSenderLocalDispatch(s.dispatchLocalRequest))
	if err != nil {
		return err
	}
	s.sender = sender
	return nil
}

func (s *store) initTxnServer() error {
	server, err := rpc.NewTxnServer(s.cfg.ListenAddress,
		s.clock,
		s.logger,
		rpc.WithServerMaxMessageSize(int(s.cfg.RPC.MaxMessageSize)))
	if err != nil {
		return err
	}
	s.server = server
	s.registerRPCHandlers()
	return nil
}

func (s *store) initClocker() error {
	if s.clock == nil {
		s.clock = clock.DefaultClock()
	}

	if s.clock == nil {
		return moerr.NewBadConfig("missing txn clock")
	}
	return nil
}

func (s *store) initHAKeeperClient() error {
	if s.options.hakeekerClientFactory != nil {
		client, err := s.options.hakeekerClientFactory()
		if err != nil {
			return err
		}
		s.hakeeperClient = client
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.DiscoveryTimeout.Duration)
	defer cancel()
	client, err := logservice.NewDNHAKeeperClient(ctx, s.cfg.HAKeeper.ClientConfig)
	if err != nil {
		return err
	}
	s.hakeeperClient = client
	return nil
}
