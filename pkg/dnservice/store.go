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

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// WithLogger set logger
func WithLogger(logger *zap.Logger) Option {
	return func(s *store) {
		s.logger = logger
	}
}

// WithConfigAdjust set adjust config func
func WithConfigAdjust(adjustConfigFunc func(c *Config)) Option {
	return func(s *store) {
		s.options.adjustConfigFunc = adjustConfigFunc
	}
}

// WithRequestFilter set filtering txn.TxnRequest sent to other DNShard
func WithRequestFilter(filter func(*txn.TxnRequest) bool) Option {
	return func(s *store) {
		s.options.requestFilter = filter
	}
}

// WithMetadataFSFactory set metadata file service factory
func WithMetadataFSFactory(factory func() fileservice.ReplaceableFileService) Option {
	return func(s *store) {
		s.options.metadataFSFactory = factory
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

type store struct {
	cfg            *Config
	logger         *zap.Logger
	clock          clock.Clock
	sender         rpc.TxnSender
	server         rpc.TxnServer
	hakeeperClient logservice.DNHAKeeperClient
	metadataFS     fileservice.ReplaceableFileService
	dataFS         fileservice.FileService
	replicas       *sync.Map
	stopper        *stopper.Stopper

	options struct {
		logServiceClientFactory func(metadata.DNShard) (logservice.Client, error)
		hakeekerClientFactory   func() (logservice.DNHAKeeperClient, error)
		metadataFSFactory       func() fileservice.ReplaceableFileService
		requestFilter           func(*txn.TxnRequest) bool
		adjustConfigFunc        func(c *Config)
	}

	mu struct {
		sync.RWMutex
		metadata metadata.DNStore
	}
}

// NewService create DN Service
func NewService(cfg *Config, opts ...Option) (Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	s := &store{
		cfg: cfg,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.logger = logutil.Adjust(s.logger).With(zap.String("dn-store", cfg.UUID))
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
	if err := s.initFileService(); err != nil {
		return nil, err
	}
	if err := s.initMetadata(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *store) Start() error {
	if err := s.startDNShards(); err != nil {
		return err
	}
	if err := s.server.Start(); err != nil {
		return err
	}
	return s.stopper.RunTask(s.heartbeatTask)
}

func (s *store) Close() error {
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
	s.stopper.Stop()
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

func (s *store) heartbeatTask(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HAKeeper.HeatbeatDuration.Duration)
	defer ticker.Stop()

	s.logger.Info("DNShard heartbeat started")
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("DNShard heartbeat stopped")
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.HeatbeatTimeout.Duration)
			commands, err := s.hakeeperClient.SendDNHeartbeat(ctx, logservicepb.DNStoreHeartbeat{
				UUID:           s.cfg.UUID,
				ServiceAddress: s.cfg.ServiceAddress,
				Shards:         s.getDNShardInfo(),
			})
			cancel()

			if err != nil {
				s.logger.Error("send DNShard heartbeat request failed",
					zap.Error(err))
				continue
			}

			for _, cmd := range commands.Commands {
				if cmd.ServiceType != logservicepb.DnService {
					s.logger.Fatal("receive invalid schedule command",
						zap.String("type", cmd.ServiceType.String()))
				}
				if cmd.ConfigChange != nil {
					var err error
					switch cmd.ConfigChange.ChangeType {
					case logservicepb.AddReplica, logservicepb.StartReplica:
						err = s.createReplica(metadata.DNShard{
							DNShardRecord: metadata.DNShardRecord{
								ShardID:    cmd.ConfigChange.Replica.ShardID,
								LogShardID: cmd.ConfigChange.Replica.LogShardID,
							},
							ReplicaID: cmd.ConfigChange.Replica.ReplicaID,
							Address:   s.cfg.ServiceAddress,
						})
					case logservicepb.RemoveReplica, logservicepb.StopReplica:
						err = s.removeReplica(cmd.ConfigChange.Replica.ShardID)
					}
					if err != nil {
						s.logger.Error("handle schedule command failed",
							zap.String("command", cmd.String()),
							zap.Error(err))
					}
				}
			}
		}
	}
}

func (s *store) createReplica(shard metadata.DNShard) error {
	r := newReplica(shard, s.logger.With(util.TxnDNShardField(shard)))
	_, ok := s.replicas.LoadOrStore(shard.ShardID, r)
	if ok {
		return nil
	}

	storage, err := s.createTxnStorage(shard)
	if err != nil {
		return err
	}
	err = s.stopper.RunTask(func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.start(service.NewTxnService(r.logger,
				shard,
				storage,
				s.sender,
				s.clock,
				s.cfg.Txn.ZombieTimeout.Duration))
			if err != nil {
				r.logger.Fatal("start DNShard failed",
					zap.Error(err))
			}
		}
	})
	if err != nil {
		return err
	}

	s.addDNShard(shard)
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
	sender, err := rpc.NewSender(s.logger,
		rpc.WithSenderBackendOptions(s.getBackendOptions()...),
		rpc.WithSenderClientOptions(s.getClientOptions()...),
		rpc.WithSenderLocalDispatch(s.dispatchLocalRequest))
	if err != nil {
		return err
	}
	s.sender = sender
	return nil
}

func (s *store) initTxnServer() error {
	server, err := rpc.NewTxnServer(s.cfg.ListenAddress, s.logger)
	if err != nil {
		return err
	}
	s.server = server
	s.registerRPCHandlers()
	return nil
}

func (s *store) initClocker() error {
	v, err := s.createClock()
	if err != nil {
		return err
	}
	s.clock = v
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

func (s *store) initFileService() error {
	if s.options.metadataFSFactory != nil {
		s.metadataFS = s.options.metadataFSFactory()
	} else {
		fs, err := fileservice.NewLocalFS(s.cfg.getMetadataDir())
		if err != nil {
			return err
		}
		s.metadataFS = fs
	}

	fs, err := s.createFileService()
	if err != nil {
		return err
	}
	s.dataFS = fs
	return nil
}

func (s *store) getBackendOptions() []morpc.BackendOption {
	return []morpc.BackendOption{
		morpc.WithBackendLogger(s.logger),
		morpc.WithBackendFilter(func(m morpc.Message) bool {
			return s.options.requestFilter == nil || s.options.requestFilter(m.(*txn.TxnRequest))
		}),
		morpc.WithBackendBusyBufferSize(s.cfg.RPC.BusyQueueSize),
		morpc.WithBackendBufferSize(s.cfg.RPC.SendQueueSize),
		morpc.WithBackendGoettyOptions(goetty.WithSessionRWBUfferSize(int(s.cfg.RPC.ReadBufferSize),
			int(s.cfg.RPC.WriteBufferSize))),
	}
}

func (s *store) getClientOptions() []morpc.ClientOption {
	return []morpc.ClientOption{
		morpc.WithClientLogger(s.logger),
		morpc.WithClientMaxBackendPerHost(s.cfg.RPC.MaxConnections),
		morpc.WithClientMaxBackendMaxIdleDuration(s.cfg.RPC.MaxIdleDuration.Duration),
	}
}
