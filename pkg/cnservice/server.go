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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"

	"github.com/google/uuid"
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

	srv := &service{
		cfg:         cfg,
		fileService: fileService,
	}
	srv.logger = logutil.Adjust(srv.logger)
	srv.responsePool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	pu := config.NewParameterUnit(&cfg.Frontend, nil, nil, nil, nil, nil)
	cfg.Frontend.SetDefaultValues()
	err := srv.initMOServer(ctx, pu)
	if err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage, cfg.PayLoadCopyBufferSize),
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
	return s.server.Start()
}

func (s *service) Close() error {
	err := s.serverShutdown(true)
	if err != nil {
		return err
	}
	s.cancelMoServerFunc()
	_ = trace.Shutdown(context.TODO())
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

	if _, err = s.createMOServer(cancelMoServerCtx, pu); err != nil {
		return err
	}

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

	default:
		return fmt.Errorf("unknown engine type: %s", s.cfg.Engine.Type)

	}

	return nil
}

func (s *service) createMOServer(inputCtx context.Context, pu *config.ParameterUnit) (context.Context, error) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	s.mo = frontend.NewMOServer(moServerCtx, address, pu)

	// init trace/log/error framework
	var writerFactory export.FSWriterFactory
	// validate node_uuid
	var uuidErr error
	var nodeUUID uuid.UUID
	if nodeUUID, uuidErr = uuid.Parse(pu.SV.NodeUUID); uuidErr != nil {
		nodeUUID = uuid.New()
		pu.SV.NodeUUID = nodeUUID.String()
	}
	if !pu.SV.DisableTrace || !pu.SV.DisableMetric {
		writerFactory = export.GetFSWriterFactory(s.fileService, pu.SV.NodeUUID, trace.NodeTypeCN.String())
	}
	if err := util.SetUUIDNodeID(nodeUUID[:]); err != nil {
		return nil, moerr.NewPanicError(err)
	} else if moServerCtx, err = trace.Init(moServerCtx,
		trace.WithMOVersion(pu.SV.MoVersion),
		trace.WithNode(pu.SV.NodeUUID, trace.NodeTypeCN),
		trace.EnableTracer(!pu.SV.DisableTrace),
		trace.WithBatchProcessMode(pu.SV.TraceBatchProcessor),
		trace.WithFSWriterFactory(writerFactory),
		trace.DebugMode(pu.SV.EnableTraceDebug),
		trace.WithSQLExecutor(func() ie.InternalExecutor {
			return frontend.NewInternalExecutor(pu)
		}),
	); err != nil {
		return nil, err
	}

	if !pu.SV.DisableMetric {
		ieFactory := func() ie.InternalExecutor {
			return frontend.NewInternalExecutor(pu)
		}
		metric.InitMetric(moServerCtx, ieFactory, pu.SV, pu.SV.NodeUUID, metric.ALL_IN_ONE_MODE,
			metric.WithWriterFactory(writerFactory), metric.WithInitAction(true))
	}
	frontend.InitServerVersion(pu.SV.MoVersion)
	err := frontend.InitSysTenant(moServerCtx)
	if err != nil {
		panic(err)
	}
	return moServerCtx, nil
}

func (s *service) runMoServer() error {
	return s.mo.Start()
}

func (s *service) serverShutdown(isgraceful bool) error {
	// flush trace/log/error framework
	if err := trace.Shutdown(trace.DefaultContext()); err != nil {
		logutil.Errorf("Shutdown trace err: %v", err)
	}
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
		sender, err = rpc.NewSenderWithConfig(s.cfg.RPC, s.logger)
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
