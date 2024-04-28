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
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/profile"
	"go.uber.org/zap"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func NewService(
	cfg *Config,
	ctx context.Context,
	fileService fileservice.FileService,
	gossipNode *gossip.Node,
	options ...Option,
) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	//set frontend parameters
	cfg.Frontend.SetDefaultValues()
	cfg.Frontend.SetMaxMessageSize(uint64(cfg.RPC.MaxMessageSize))

	configKVMap, _ := dumpCnConfig(*cfg)
	options = append(options, WithConfigData(configKVMap))
	options = append(options, WithBootstrapOptions(bootstrap.WithUpgradeTenantBatch(cfg.UpgradeTenantBatchSize)))

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
		logger:      logutil.GetGlobalLogger().Named("cn-service"),
		metadataFS:  metadataFS,
		etlFS:       etlFS,
		fileService: fileService,
		sessionMgr:  queryservice.NewSessionManager(),
		addressMgr:  address.NewAddressManager(cfg.ServiceHost, cfg.PortBase),
		gossipNode:  gossipNode,
	}

	srv.requestHandler = func(ctx context.Context,
		cnAddr string,
		message morpc.Message,
		cs morpc.ClientSession,
		engine engine.Engine,
		fService fileservice.FileService,
		lockService lockservice.LockService,
		queryClient qclient.QueryClient,
		hakeeper logservice.CNHAKeeperClient,
		udfService udf.Service,
		cli client.TxnClient,
		aicm *defines.AutoIncrCacheManager,
		messageAcquirer func() morpc.Message) error {
		return nil
	}

	for _, opt := range options {
		opt(srv)
	}
	srv.stopper = stopper.NewStopper("cn-service", stopper.WithLogger(srv.logger))

	srv.registerServices()
	if _, err = srv.getHAKeeperClient(); err != nil {
		return nil, err
	}
	if err := srv.initQueryService(); err != nil {
		return nil, err
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

	pu := config.NewParameterUnit(
		&cfg.Frontend,
		nil,
		nil,
		engine.Nodes{engine.Node{
			Addr: srv.pipelineServiceServiceAddr(),
		}})
	pu.HAKeeperClient = srv._hakeeperClient
	frontend.InitServerVersion(pu.SV.MoVersion)

	// Init the autoIncrCacheManager after the default value is set before the init of moserver.
	srv.aicm = &defines.AutoIncrCacheManager{
		AutoIncrCaches: make(map[string]defines.AutoIncrCache),
		Mu:             &sync.Mutex{},
		MaxSize:        pu.SV.AutoIncrCacheSize,
	}

	// init UdfService
	var udfServices []udf.Service
	// add python client to handle python udf
	if srv.cfg.PythonUdfClient.ServerAddress != "" {
		pc, err := pythonservice.NewClient(srv.cfg.PythonUdfClient)
		if err != nil {
			panic(err)
		}
		udfServices = append(udfServices, pc)
	}
	srv.udfService, err = udf.NewService(udfServices...)
	if err != nil {
		panic(err)
	}

	srv.pu = pu
	srv.pu.LockService = srv.lockService
	srv.pu.HAKeeperClient = srv._hakeeperClient
	srv.pu.QueryClient = srv.queryClient
	srv.pu.UdfService = srv.udfService
	srv._txnClient = pu.TxnClient

	if err = srv.initMOServer(ctx, pu, srv.aicm); err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer("pipeline-server", srv.pipelineServiceListenAddr(),
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

	srv.requestHandler = func(ctx context.Context,
		cnAddr string,
		message morpc.Message,
		cs morpc.ClientSession,
		engine engine.Engine,
		fService fileservice.FileService,
		lockService lockservice.LockService,
		queryClient qclient.QueryClient,
		hakeeper logservice.CNHAKeeperClient,
		udfService udf.Service,
		cli client.TxnClient,
		aicm *defines.AutoIncrCacheManager,
		messageAcquirer func() morpc.Message) error {
		return nil
	}
	for _, opt := range options {
		opt(srv)
	}

	// TODO: global client need to refactor
	err = cnclient.NewCNClient(
		srv.pipelineServiceServiceAddr(),
		&cnclient.ClientConfig{RPC: cfg.RPC})
	if err != nil {
		panic(err)
	}
	return srv, nil
}

func (s *service) Start() error {
	s.initSqlWriterFactory()

	if err := s.queryService.Start(); err != nil {
		return err
	}

	err := s.runMoServer()
	if err != nil {
		return err
	}

	return s.server.Start()
}

func (s *service) Close() error {
	defer logutil.LogClose(s.logger, "cnservice")()

	s.stopper.Stop()
	if err := s.bootstrapService.Close(); err != nil {
		return err
	}
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

	if s.gossipNode != nil {
		if err := s.gossipNode.Leave(time.Second); err != nil {
			return err
		}
	}

	if err := s.server.Close(); err != nil {
		return err
	}
	return s.lockService.Close()
}

// ID implements the frontend.BaseService interface.
func (s *service) ID() string {
	return s.cfg.UUID
}

// SQLAddress implements the frontend.BaseService interface.
func (s *service) SQLAddress() string {
	return s.cfg.SQLAddress
}

// SessionMgr implements the frontend.BaseService interface.
func (s *service) SessionMgr() *queryservice.SessionManager {
	return s.sessionMgr
}

func (s *service) CheckTenantUpgrade(_ context.Context, tenantID int64) error {
	finalVersion := s.GetFinalVersion()
	tenantFetchFunc := func() (int32, string, error) {
		return int32(tenantID), finalVersion, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	if _, err := s.bootstrapService.MaybeUpgradeTenant(ctx, tenantFetchFunc, nil); err != nil {
		return err
	}
	return nil
}

// UpgradeTenant Manual command tenant upgrade entrance
func (s *service) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*120)
	defer cancel()
	if _, err := s.bootstrapService.UpgradeTenant(ctx, tenantName, retryCount, isALLAccount); err != nil {
		return err
	}
	return nil
}

func (s *service) GetFinalVersion() string {
	return s.bootstrapService.GetFinalVersion()
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
	if s.queryService != nil {
		if err := s.queryService.Close(); err != nil {
			return err
		}
	}
	if s.queryClient != nil {
		if err := s.queryClient.Close(); err != nil {
			return err
		}
	}
	s.timestampWaiter.Close()
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
	value morpc.RPCMessage,
	_ uint64,
	cs morpc.ClientSession) error {
	req := value.Message
	msg, ok := req.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", req)
		panic("cn server receive a message with unexpected type")
	}
	switch msg.GetSid() {
	case pipeline.Status_WaitingNext:
		return handleWaitingNextMsg(ctx, req, cs)
	case pipeline.Status_Last:
		if msg.IsPipelineMessage() { // only pipeline type need assemble msg now.
			if err := handleAssemblePipeline(ctx, req, cs); err != nil {
				return err
			}
		}
	}

	go func() {
		defer value.Cancel()
		s.pipelines.counter.Add(1)
		defer s.pipelines.counter.Add(-1)
		s.requestHandler(ctx,
			s.pipelineServiceServiceAddr(),
			req,
			cs,
			s.storeEngine,
			s.fileService,
			s.lockService,
			s.queryClient,
			s._hakeeperClient,
			s.udfService,
			s._txnClient,
			s.aicm,
			s.acquireMessage)
	}()
	return nil
}

func (s *service) initMOServer(ctx context.Context, pu *config.ParameterUnit, aicm *defines.AutoIncrCacheManager) error {
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

	s.createMOServer(cancelMoServerCtx, pu, aicm)
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

	return s.bootstrap()
}

func (s *service) createMOServer(
	inputCtx context.Context,
	pu *config.ParameterUnit,
	aicm *defines.AutoIncrCacheManager,
) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	s.mo = frontend.NewMOServer(moServerCtx, address, pu, aicm, s)
}

func (s *service) runMoServer() error {
	return s.mo.Start()
}

func (s *service) serverShutdown(isgraceful bool) error {
	return s.mo.Stop()
}

func (s *service) getHAKeeperClient() (client logservice.CNHAKeeperClient, err error) {
	s.initHakeeperClientOnce.Do(func() {
		s.hakeeperConnected = make(chan struct{})

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
		s.initClusterService()
		s.initLockService()

		ss, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.StatusServer)
		if ok {
			ss.(*status.Server).SetHAKeeperClient(client)
		}

		if err = s.startCNStoreHeartbeat(); err != nil {
			return
		}
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
	handleTemp := func(d metadata.TNShard) rpc.TxnRequestHandleFunc {
		if d.Address != defines.TEMPORARY_TABLE_TN_ADDR {
			return nil
		}

		// read, write, commit and rollback for temporary tables
		return func(ctx context.Context, req *txn.TxnRequest, resp *txn.TxnResponse) (err error) {
			storage, ok := ctx.Value(defines.TemporaryTN{}).(*memorystorage.Storage)
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
				return moerr.NewNotSupported(ctx, "unknown txn request method: %s", req.Method.String())
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
		opts = append(opts,
			client.WithTimestampWaiter(s.timestampWaiter))
		if s.cfg.Txn.EnableSacrificingFreshness == 1 {
			opts = append(opts,
				client.WithEnableSacrificingFreshness())
		}
		if s.cfg.Txn.EnableCNBasedConsistency == 1 {
			opts = append(opts,
				client.WithEnableCNBasedConsistency())
		}
		if s.cfg.Txn.EnableRefreshExpression == 1 {
			opts = append(opts,
				client.WithEnableRefreshExpression())
		}
		if s.cfg.Txn.EnableLeakCheck == 1 {
			opts = append(opts, client.WithEnableLeakCheck(
				s.cfg.Txn.MaxActiveAges.Duration,
				func(actives []client.ActiveTxn) {
					name, _ := uuid.NewV7()
					profPath := catalog.BuildProfilePath("routine", name.String())

					for _, txn := range actives {
						fields := []zap.Field{
							zap.String("txn-id", hex.EncodeToString(txn.ID)),
							zap.Time("create-at", txn.CreateAt),
							zap.String("options", txn.Options.String()),
							zap.String("profile", profPath),
						}
						if txn.Options.InRunSql {
							//the txn runs sql in compile.Run() and doest not exist
							v2.TxnLongRunningCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found long running txn", fields...)
						} else if txn.Options.InCommit {
							v2.TxnInCommitCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found txn in commit", fields...)
						} else if txn.Options.InRollback {
							v2.TxnInRollbackCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found txn in rollback", fields...)
						} else {
							v2.TxnLeakCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found leak txn", fields...)
						}
					}

					s.saveProfile(profPath)
				}))
		}
		if s.cfg.Txn.Limit > 0 {
			opts = append(opts,
				client.WithTxnLimit(s.cfg.Txn.Limit))
		}
		if s.cfg.Txn.MaxActive > 0 {
			opts = append(opts,
				client.WithMaxActiveTxn(s.cfg.Txn.MaxActive))
		}
		if s.cfg.Txn.PkDedupCount > 0 {
			opts = append(opts, client.WithCheckDup())
		}
		opts = append(opts,
			client.WithLockService(s.lockService),
			client.WithNormalStateNoWait(s.cfg.Txn.NormalStateNoWait),
			client.WithTxnOpenedCallback([]func(op client.TxnOperator){
				func(op client.TxnOperator) {
					trace.GetService().TxnCreated(op)
				},
			}),
		)
		c = client.NewTxnClient(
			sender,
			opts...)
		s._txnClient = c
	})
	c = s._txnClient
	return
}

func (s *service) initLockService() {
	cfg := s.getLockServiceConfig()
	s.lockService = lockservice.NewLockService(
		cfg,
		lockservice.WithWait(func() {
			<-s.hakeeperConnected
		}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.LockService, s.lockService)
	lockservice.SetLockServiceByServiceID(s.lockService.GetServiceID(), s.lockService)

	ss, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.StatusServer)
	if ok {
		ss.(*status.Server).SetLockService(s.cfg.UUID, s.lockService)
	}
}

func (s *service) GetSQLExecutor() executor.SQLExecutor {
	return s.sqlExecutor
}

func (s *service) GetBootstrapService() bootstrap.Service {
	return s.bootstrapService
}

// put the waiting-next type msg into client session's cache and return directly
func handleWaitingNextMsg(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	msg, _ := message.(*pipeline.Message)
	switch msg.GetCmd() {
	case pipeline.Method_PipelineMessage:
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

func (s *service) initInternalSQlExecutor(mp *mpool.MPool) {
	s.sqlExecutor = compile.NewSQLExecutor(
		s.pipelineServiceServiceAddr(),
		s.storeEngine,
		mp,
		s._txnClient,
		s.fileService,
		s.queryClient,
		s._hakeeperClient,
		s.udfService,
		s.aicm)
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.InternalSQLExecutor, s.sqlExecutor)
}

func (s *service) initIncrService() {
	store, err := incrservice.NewSQLStore(s.sqlExecutor)
	if err != nil {
		panic(err)
	}
	incrService := incrservice.NewIncrService(
		s.cfg.UUID,
		store,
		s.cfg.AutoIncrement)
	runtime.ProcessLevelRuntime().SetGlobalVariables(
		runtime.AutoIncrementService,
		incrService)
	incrservice.SetAutoIncrementServiceByID(s.cfg.UUID, incrService)
}

func (s *service) bootstrap() error {
	s.initIncrService()
	s.initTxnTraceService()

	rt := runtime.ProcessLevelRuntime()
	s.bootstrapService = bootstrap.NewService(
		&locker{hakeeperClient: s._hakeeperClient},
		rt.Clock(),
		s._txnClient,
		s.sqlExecutor,
		s.options.bootstrapOptions...)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	ctx = context.WithValue(ctx, config.ParameterUnitKey, s.pu)
	defer cancel()

	// bootstrap cannot fail. We panic here to make sure the service can not start.
	// If bootstrap failed, need clean all data to retry.
	if err := s.bootstrapService.Bootstrap(ctx); err != nil {
		panic(err)
	}

	if s.cfg.AutomaticUpgrade {
		return s.stopper.RunTask(func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, time.Minute*120)
			defer cancel()
			if err := s.bootstrapService.BootstrapUpgrade(ctx); err != nil {
				if err != context.Canceled {
					runtime.DefaultRuntime().Logger().Error("bootstrap system automatic upgrade failed by: ", zap.Error(err))
					//panic(err)
				}
			}
		})
	}
	return nil
}

func (s *service) initTxnTraceService() {
	rt := runtime.ProcessLevelRuntime()
	ts, err := trace.NewService(
		s.options.traceDataPath,
		s.cfg.UUID,
		s._txnClient,
		rt.Clock(),
		s.sqlExecutor,
		trace.WithEnable(s.cfg.Txn.Trace.Enable, s.cfg.Txn.Trace.Tables),
		trace.WithBufferSize(s.cfg.Txn.Trace.BufferSize),
		trace.WithFlushBytes(int(s.cfg.Txn.Trace.FlushBytes)),
		trace.WithFlushDuration(s.cfg.Txn.Trace.FlushDuration.Duration))
	if err != nil {
		panic(err)
	}
	rt.SetGlobalVariables(runtime.TxnTraceService, ts)
}

func (s *service) saveProfile(profilePath string) {
	reader, writer := io.Pipe()
	go func() {
		// dump all goroutines
		_ = profile.ProfileGoroutine(writer, 2)
		_ = writer.Close()
	}()
	writeVec := fileservice.IOVector{
		FilePath: profilePath,
		Entries: []fileservice.IOEntry{
			{
				Offset:         0,
				ReaderForWrite: reader,
				Size:           -1,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*3)
	defer cancel()
	err := s.etlFS.Write(ctx, writeVec)
	if err != nil {
		logutil.Errorf("save profile %s failed. err:%v", profilePath, err)
		return
	}
}

type locker struct {
	hakeeperClient logservice.CNHAKeeperClient
}

func (l *locker) Get(
	ctx context.Context,
	key string) (bool, error) {
	v, err := l.hakeeperClient.AllocateIDByKeyWithBatch(ctx, key, 1)
	if err != nil {
		return false, err
	}
	return v == 1, nil
}
