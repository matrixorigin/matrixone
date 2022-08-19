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
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

func NewService(cfg *Config, ctx context.Context) (Service, error) {

	srv := &service{cfg: cfg}
	srv.logger = logutil.Adjust(srv.logger)
	srv.pool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	if err := srv.initHAKeeperClient(); err != nil {
		return nil, err
	}
	if err := srv.initTxnSender(); err != nil {
		return nil, err
	}
	if err := srv.initTxnClient(); err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage, 16<<20),
		morpc.WithServerGoettyOptions(goetty.WithSessionRWBUfferSize(1<<20, 1<<20)))
	if err != nil {
		return nil, err
	}
	server.RegisterRequestHandler(srv.handleRequest)
	srv.server = server

	pu := config.NewParameterUnit(&cfg.Frontend, nil, nil, nil, nil, nil)
	cfg.Frontend.SetDefaultValues()
	err = srv.initMOServer(ctx, pu)
	if err != nil {
		return nil, err
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
	return s.server.Close()
}

func (s *service) acquireMessage() morpc.Message {
	return s.pool.Get().(*pipeline.Message)
}

/*
func (s *service) releaseMessage(msg *pipeline.Message) {
	msg.Reset()
	s.pool.Put(msg)
}
*/

func (s *service) handleRequest(req morpc.Message, _ uint64, cs morpc.ClientSession) error {
	return nil
}

func (s *service) initMOServer(ctx context.Context, pu *config.ParameterUnit) error {
	var err error
	logutil.Infof("Shutdown The Server With Ctrl+C | Ctrl+\\.")
	cancelMoServerCtx, cancelMoServerFunc := context.WithCancel(ctx)
	s.cancelMoServerFunc = cancelMoServerFunc

	pu.HostMmu = host.New(pu.SV.HostMmuLimitation)

	fmt.Println("Initialize the engine ...")
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
		//TODO

	case EngineMemory:
		//TODO

	default:
		return fmt.Errorf("unknown engine type: %s", s.cfg.Engine.Type)

	}

	return nil
}

func (s *service) createMOServer(inputCtx context.Context, pu *config.ParameterUnit) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	s.mo = frontend.NewMOServer(moServerCtx, address, pu)
	{
		// init trace/log/error framework
		if _, err := trace.Init(moServerCtx,
			trace.WithMOVersion(pu.SV.MoVersion),
			trace.WithNode("node_uuid", trace.NodeTypeCN),
			trace.EnableTracer(!pu.SV.DisableTrace),
			trace.WithBatchProcessMode(pu.SV.TraceBatchProcessor),
			trace.DebugMode(pu.SV.EnableTraceDebug),
			trace.WithSQLExecutor(func() ie.InternalExecutor {
				return frontend.NewInternalExecutor(pu)
			}),
		); err != nil {
			panic(err)
		}
	}

	if !pu.SV.DisableMetric {
		ieFactory := func() ie.InternalExecutor {
			return frontend.NewInternalExecutor(pu)
		}
		metric.InitMetric(moServerCtx, ieFactory, pu, 0, metric.ALL_IN_ONE_MODE)
	}
	frontend.InitServerVersion(pu.SV.MoVersion)
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

func (s *service) initHAKeeperClient() error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		s.cfg.HAKeeper.DiscoveryTimeout.Duration,
	)
	defer cancel()
	client, err := logservice.NewCNHAKeeperClient(ctx, s.cfg.HAKeeper.ClientConfig)
	if err != nil {
		return err
	}
	s.hakeeperClient = client
	return nil
}

func (s *service) initTxnSender() error {
	sender, err := rpc.NewSender(s.logger) //TODO set proper options
	if err != nil {
		return err
	}
	s.txnSender = sender
	return nil
}

func (s *service) initTxnClient() error {
	txnClient := client.NewTxnClient(s.txnSender) //TODO other options
	s.txnClient = txnClient
	return nil
}
