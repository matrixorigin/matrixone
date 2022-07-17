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

package dn

import (
	"context"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"go.uber.org/zap"
)

// WithLogger set logger
func WithLogger(logger *zap.Logger) Option {
	return func(s *store) {
		s.logger = logger
	}
}

// WithRequestFilter set filtering txn.TxnRequest sent to other DNShard
func WithRequestFilter(filter func(*txn.TxnRequest) bool) Option {
	return func(s *store) {
		s.options.requestFilter = filter
	}
}

type store struct {
	cfg            *Config
	logger         *zap.Logger
	clock          clock.Clock
	sender         rpc.TxnSender
	server         rpc.TxnServer
	hakeeperClient logservice.DNHAKeeperClient
	stopper        *stopper.Stopper

	options struct {
		requestFilter func(*txn.TxnRequest) bool
	}
}

func NewStore(cfg *Config, opts ...Option) (Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	s := &store{}
	for _, opt := range opts {
		opt(s)
	}
	s.logger = logutil.Adjust(s.logger).With(zap.String("dn-store-id", cfg.StoreID))
	s.stopper = stopper.NewStopper("dn-store", stopper.WithLogger(s.logger))

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
	return nil, nil
}

func (s *store) Start() error {
	return nil
}

func (s *store) startDNShards() {

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
	switch s.cfg.Txn.Clock.Source {
	case localClockSource:
		s.createMemClock()
	case hlcClockSource:
		panic("not implement")
	}
	return nil
}

func (s *store) initHAKeeperClient() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.DiscoveryTimeout.Duration)
	defer cancel()
	client, err := logservice.NewDNHAKeeperClient(ctx, s.cfg.HAKeeper.ClientConfig)
	if err != nil {
		return err
	}
	s.hakeeperClient = client
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
		morpc.WithBackendGoettyOptions(goetty.WithBufSize(int(s.cfg.RPC.ReadBufferSize), int(s.cfg.RPC.WriteBufferSize))),
	}
}

func (s *store) getClientOptions() []morpc.ClientOption {
	return []morpc.ClientOption{
		morpc.WithClientLogger(s.logger),
		morpc.WithClientMaxBackendPerHost(s.cfg.RPC.MaxConnections),
	}
}
