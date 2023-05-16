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

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func NewService(
	cfg *Config,
	ctx context.Context,
	fileService fileservice.FileService,
	options ...Option,
) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// get metadata fs
	metadataFS, err := fileservice.Get[fileservice.ReplaceableFileService](fileService, defines.LocalFileServiceName)
	if err != nil {
		return nil, err
	}
	// get etl fs
	etlFS, err := fileservice.Get[fileservice.FileService](fileService, defines.ETLFileServiceName)
	if err != nil {
		return nil, err
	}

	srv := &service{
		metadata: metadata.CNStore{
			UUID: cfg.UUID,
			Role: metadata.MustParseCNRole(cfg.Role),
		},
		cfg:         cfg,
		metadataFS:  metadataFS,
		etlFS:       etlFS,
		fileService: fileService,
	}
	for _, opt := range options {
		opt(srv)
	}
	srv.logger = logutil.Adjust(srv.logger)
	srv.stopper = stopper.NewStopper("cn-service", stopper.WithLogger(srv.logger))

	if err := srv.initMetadata(); err != nil {
		return nil, err
	}

	srv.responsePool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	pu := config.NewParameterUnit(
		&cfg.Frontend,
		nil,
		nil,
		engine.Nodes{engine.Node{
			Addr: cfg.ServiceAddress,
		}})

	cfg.Frontend.SetDefaultValues()
	cfg.Frontend.SetMaxMessageSize(uint64(cfg.RPC.MaxMessageSize))
	frontend.InitServerVersion(pu.SV.MoVersion)

	srv.pu = pu
	if err = srv.initMOServer(ctx, pu); err != nil {
		return nil, err
	}
	if _, err = srv.getHAKeeperClient(); err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage,
			morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize))),
		morpc.WithServerLogger(srv.logger),
		morpc.WithServerGoettyOptions(
			goetty.WithSessionRWBUfferSize(cfg.ReadBufferSize, cfg.WriteBufferSize),
			goetty.WithSessionReleaseMsgFunc(func(v any) {
				m := v.(morpc.RPCMessage)
				if !m.InternalMessage() {
					srv.releaseMessage(m.Message.(*pipeline.Message))
				}
			}),
		),
		morpc.WithServerDisableAutoCancelContext())
	if err != nil {
		return nil, err
	}
	server.RegisterRequestHandler(srv.handleRequest)
	srv.server = server
	srv.storeEngine = pu.StorageEngine
	srv._txnClient = pu.TxnClient

	srv.requestHandler = func(ctx context.Context,
		cnAddr string,
		message morpc.Message,
		cs morpc.ClientSession,
		engine engine.Engine,
		fService fileservice.FileService,
		lockService lockservice.LockService,
		cli client.TxnClient,
		messageAcquirer func() morpc.Message) error {
		return nil
	}
	for _, opt := range options {
		opt(srv)
	}

	srv.initCtlService()
	return srv, nil
}

func (s *service) Start() error {
	s.initTaskServiceHolder()

	if err := s.ctlservice.Start(); err != nil {
		return err
	}

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
	defer logutil.LogClose(s.logger, "cnservice")()

	s.stopper.Stop()
	if err := s.stopFrontend(); err != nil {
		return err
	}
	if err := s.stopTask(); err != nil {
		return err
	}
	if err := s.stopRPCs(); err != nil {
		return err
	}
	// stop I/O pipeline
	blockio.Stop()
	return s.server.Close()
}

func (s *service) stopFrontend() error {
	defer logutil.LogClose(s.logger, "cnservice/frontend")()

	if err := s.serverShutdown(true); err != nil {
		return err
	}
	s.cancelMoServerFunc()
	return nil
}

func (s *service) stopRPCs() error {
	if s._txnClient != nil {
		if err := s._txnClient.Close(); err != nil {
			return err
		}
	}
	if s._hakeeperClient != nil {
		s.moCluster.Close()
		if err := s._hakeeperClient.Close(); err != nil {
			return err
		}
	}
	if s._txnSender != nil {
		if err := s._txnSender.Close(); err != nil {
			return err
		}
	}
	if s.lockService != nil {
		if err := s.lockService.Close(); err != nil {
			return err
		}
	}
	if s.ctlservice != nil {
		if err := s.ctlservice.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) acquireMessage() morpc.Message {
	return s.responsePool.Get().(*pipeline.Message)
}

func (s *service) releaseMessage(m *pipeline.Message) {
	if s.responsePool != nil {
		m.Reset()
		s.responsePool.Put(m)
	}
}

func (s *service) handleRequest(
	ctx context.Context,
	req morpc.Message,
	_ uint64,
	cs morpc.ClientSession) error {
	msg, ok := req.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", req)
		panic("cn server receive a message with unexpected type")
	}
	switch msg.GetSid() {
	case pipeline.WaitingNext:
		return handleWaitingNextMsg(ctx, req, cs)
	case pipeline.Last:
		if msg.IsPipelineMessage() { // only pipeline type need assemble msg now.
			if err := handleAssemblePipeline(ctx, req, cs); err != nil {
				return err
			}
		}
	}
	go s.requestHandler(ctx,
		s.cfg.ServiceAddress,
		req,
		cs,
		s.storeEngine,
		s.fileService,
		s.lockService,
		s._txnClient,
		s.acquireMessage)
	return nil
}

func (s *service) initMOServer(ctx context.Context, pu *config.ParameterUnit) error {
	var err error
	logutil.Infof("Shutdown The Server With Ctrl+C | Ctrl+\\.")
	cancelMoServerCtx, cancelMoServerFunc := context.WithCancel(ctx)
	s.cancelMoServerFunc = cancelMoServerFunc

	pu.FileService = s.fileService
	pu.LockService = s.lockService

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
		return moerr.NewInternalError(ctx, "unknown engine type: %s", s.cfg.Engine.Type)

	}

	return nil
}

func (s *service) createMOServer(inputCtx context.Context, pu *config.ParameterUnit) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	s.mo = frontend.NewMOServer(moServerCtx, address, pu)
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

		runtime.ProcessLevelRuntime().SetGlobalVariables(
			runtime.HAKeeperClient,
			client)
		s._hakeeperClient = client
		s.pu.HAKeeperClient = client
		s.initClusterService()
		s.initLockService()
	})
	client = s._hakeeperClient
	return
}

func (s *service) initClusterService() {
	s.moCluster = clusterservice.NewMOCluster(s._hakeeperClient,
		s.cfg.Cluster.RefreshInterval.Duration)
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, s.moCluster)
}

func (s *service) getTxnSender() (sender rpc.TxnSender, err error) {
	// handleTemp is used to manipulate memorystorage stored for temporary table created by sessions.
	// processing of temporary table is currently on local, so we need to add a WithLocalDispatch logic to service.
	handleTemp := func(d metadata.DNShard) rpc.TxnRequestHandleFunc {
		if d.Address != defines.TEMPORARY_TABLE_DN_ADDR {
			return nil
		}

		// read, write, commit and rollback for temporary tables
		return func(ctx context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) (err error) {
			storage, ok := ctx.Value(defines.TemporaryDN{}).(*memorystorage.Storage)
			if !ok {
				panic("tempStorage should never be nil")
			}

			resp.RequestID = req.RequestID
			resp.Txn = &req.Txn
			resp.Method = req.Method
			resp.Flag = req.Flag

			switch req.Method {
			case txn.TxnMethod_Read:
				res, err := storage.Read(
					ctx,
					req.Txn,
					req.CNRequest.OpCode,
					req.CNRequest.Payload,
				)
				if err != nil {
					resp.TxnError = txn.WrapError(err, moerr.ErrTAERead)
				} else {
					payload, err := res.Read()
					if err != nil {
						panic(err)
					}
					resp.CNOpResponse = &txn.CNOpResponse{Payload: payload}
					res.Release()
				}
			case txn.TxnMethod_Write:
				payload, err := storage.Write(
					ctx,
					req.Txn,
					req.CNRequest.OpCode,
					req.CNRequest.Payload,
				)
				if err != nil {
					resp.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
				} else {
					resp.CNOpResponse = &txn.CNOpResponse{Payload: payload}
				}
			case txn.TxnMethod_Commit:
				_, err = storage.Commit(ctx, req.Txn)
				if err == nil {
					resp.Txn.Status = txn.TxnStatus_Committed
				}
			case txn.TxnMethod_Rollback:
				err = storage.Rollback(ctx, req.Txn)
				if err == nil {
					resp.Txn.Status = txn.TxnStatus_Aborted
				}
			default:
				panic("should never happen")
			}
			return err
		}
	}

	s.initTxnSenderOnce.Do(func() {
		sender, err = rpc.NewSender(
			s.cfg.RPC,
			runtime.ProcessLevelRuntime(),
			rpc.WithSenderLocalDispatch(handleTemp),
		)
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
		s.timestampWaiter = client.NewTimestampWaiter()

		rt := runtime.ProcessLevelRuntime()
		client.SetupRuntimeTxnOptions(
			rt,
			txn.GetTxnMode(s.cfg.Txn.Mode),
			txn.GetTxnIsolation(s.cfg.Txn.Isolation),
		)
		var sender rpc.TxnSender
		sender, err = s.getTxnSender()
		if err != nil {
			return
		}
		var opts []client.TxnClientCreateOption
		if s.cfg.Txn.EnableSacrificingFreshness {
			opts = append(opts,
				client.WithEnableSacrificingFreshness(s.timestampWaiter))
		}
		if s.cfg.Txn.EnableCNBasedConsistency {
			opts = append(opts,
				client.WithEnableCNBasedConsistency())
		}
		if s.cfg.Txn.EnableRefreshExpression {
			opts = append(opts,
				client.WithEnableRefreshExpression())
		}
		opts = append(opts, client.WithLockService(s.lockService))
		c = client.NewTxnClient(
			sender,
			opts...)
		s._txnClient = c
	})
	c = s._txnClient
	return
}

func (s *service) initLockService() {
	cfg := s.cfg.getLockServiceConfig()
	s.lockService = lockservice.NewLockService(cfg)
}

// put the waiting-next type msg into client session's cache and return directly
func handleWaitingNextMsg(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	msg, _ := message.(*pipeline.Message)
	switch msg.GetCmd() {
	case pipeline.PipelineMessage:
		var cache morpc.MessageCache
		var err error
		if cache, err = cs.CreateCache(ctx, message.GetID()); err != nil {
			return err
		}
		cache.Add(message)
	}
	return nil
}

func handleAssemblePipeline(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	var data []byte

	cnt := uint64(0)
	cache, err := cs.CreateCache(ctx, message.GetID())
	if err != nil {
		return err
	}
	for {
		msg, ok, err := cache.Pop()
		if err != nil {
			return err
		}
		if !ok {
			cache.Close()
			break
		}
		if cnt != msg.(*pipeline.Message).GetSequence() {
			return moerr.NewInternalErrorNoCtx("Pipeline packages passed by morpc are out of order")
		}
		cnt++
		data = append(data, msg.(*pipeline.Message).GetData()...)
	}
	msg := message.(*pipeline.Message)
	msg.SetData(append(data, msg.GetData()...))
	return nil
}
