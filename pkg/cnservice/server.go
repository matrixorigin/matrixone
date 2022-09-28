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

package cnservice

import (
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

type Options func(*service)

func NewService(
	cfg *Config,
	ctx context.Context,
	fileService fileservice.FileService,
	options ...Options,
) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// get metadata fs
	fs, err := fileservice.Get[fileservice.ReplaceableFileService](fileService, "LOCAL")
	if err != nil {
		return nil, err
	}

	srv := &service{
		logger: logutil.GetGlobalLogger().Named("cnservice"),
		metadata: metadata.CNStore{
			UUID: cfg.UUID,
			Role: metadata.MustParseCNRole(cfg.Role),
		},
		cfg:         cfg,
		metadataFS:  fs,
		fileService: fileService,
	}
	srv.stopper = stopper.NewStopper("cn-service", stopper.WithLogger(srv.logger))

	if err := srv.initMetadata(); err != nil {
		return nil, err
	}

	srv.responsePool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	pu := config.NewParameterUnit(&cfg.Frontend, nil, nil, nil, nil, nil)
	cfg.Frontend.SetDefaultValues()
	if err = srv.initMOServer(ctx, pu); err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage),
		morpc.WithServerGoettyOptions(goetty.WithSessionRWBUfferSize(cfg.ReadBufferSize, cfg.WriteBufferSize)))
	if err != nil {
		return nil, err
	}
	server.RegisterRequestHandler(compile.NewServer().HandleRequest)
	srv.server = server

	srv.requestHandler = defaultRequestHandler
	for _, opt := range options {
		opt(srv)
	}

	return srv, nil
}

func (s *service) Start() error {
	err := s.runMoServer()
	if err != nil {
		return err
	}
	if err := s.startCNStoreHeartbeat(); err != nil {
		return err
	}
	return s.server.Start()
}

func (s *service) Close() error {
	err := s.serverShutdown(true)
	if err != nil {
		return err
	}
	s.cancelMoServerFunc()
	s.stopper.Stop()
	return s.server.Close()
}

func (s *service) acquireMessage() morpc.Message {
	return s.responsePool.Get().(*pipeline.Message)
}

func defaultRequestHandler(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	return nil
}

//func (s *service) handleRequest(ctx context.Context, req morpc.Message, _ uint64, cs morpc.ClientSession) error {
//	return s.requestHandler(ctx, req, cs)
//}

func (s *service) initMOServer(ctx context.Context, pu *config.ParameterUnit) error {
	var err error
	logutil.Infof("Shutdown The Server With Ctrl+C | Ctrl+\\.")
	cancelMoServerCtx, cancelMoServerFunc := context.WithCancel(ctx)
	s.cancelMoServerFunc = cancelMoServerFunc

	pu.HostMmu = host.New(pu.SV.HostMmuLimitation)

	pu.FileService = s.fileService

	logutil.Info("Initialize the engine ...")
	err = s.initEngine(ctx, cancelMoServerCtx, pu)
	if err != nil {
		return err
	}

	s.createMOServer(cancelMoServerCtx, pu)

	return nil
}

func (s *service) initEngine(
	ctx context.Context,
	cancelMoServerCtx context.Context,
	pu *config.ParameterUnit,
) error {

	switch s.cfg.Engine.Type {

	case EngineTAE:
		if err := initTAE(cancelMoServerCtx, pu); err != nil {
			return err
		}

	case EngineDistributedTAE:
		if err := s.initDistributedTAE(cancelMoServerCtx, pu); err != nil {
			return err
		}

	case EngineMemory:
		if err := s.initMemoryEngine(cancelMoServerCtx, pu); err != nil {
			return err
		}

	case EngineNonDistributedMemory:
		if err := s.initMemoryEngineNonDist(cancelMoServerCtx, pu); err != nil {
			return err
		}

	default:
		return moerr.NewInternalError("unknown engine type: %s", s.cfg.Engine.Type)

	}

	return nil
}

func (s *service) createMOServer(inputCtx context.Context, pu *config.ParameterUnit) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	s.mo = frontend.NewMOServer(moServerCtx, address, pu)

	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(pu)
	}
	if err := trace.InitSchema(moServerCtx, ieFactory); err != nil {
		panic(err)
	}
	if err := metric.InitSchema(moServerCtx, ieFactory); err != nil {
		panic(err)
	}
	frontend.InitServerVersion(pu.SV.MoVersion)
	err := frontend.InitSysTenant(moServerCtx)
	if err != nil {
		panic(err)
	}
}

func (s *service) runMoServer() error {
	return s.mo.Start()
}

func (s *service) serverShutdown(isgraceful bool) error {
	return s.mo.Stop()
}

func (s *service) getHAKeeperClient() (client logservice.CNHAKeeperClient, err error) {
	s.initHakeeperClientOnce.Do(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			s.cfg.HAKeeper.DiscoveryTimeout.Duration,
		)
		defer cancel()
		client, err = logservice.NewCNHAKeeperClient(ctx, s.cfg.HAKeeper.ClientConfig)
		if err != nil {
			return
		}
		s._hakeeperClient = client
	})
	client = s._hakeeperClient
	return
}

func (s *service) getTxnSender() (sender rpc.TxnSender, err error) {
	s.initTxnSenderOnce.Do(func() {
		sender, err = rpc.NewSenderWithConfig(s.cfg.RPC, clock.DefaultClock(), s.logger)
		if err != nil {
			return
		}
		s._txnSender = sender
	})
	sender = s._txnSender
	return
}

func (s *service) getTxnClient() (c client.TxnClient, err error) {
	s.initTxnClientOnce.Do(func() {
		var sender rpc.TxnSender
		sender, err = s.getTxnSender()
		if err != nil {
			return
		}
		c = client.NewTxnClient(sender)
		s._txnClient = c
	})
	c = s._txnClient
	return
}

func WithMessageHandle(f func(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error) Options {
	return func(s *service) {
		s.requestHandler = f
	}
}
