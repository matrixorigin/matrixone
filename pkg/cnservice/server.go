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
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergmaintenance "github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/profile"
	"github.com/matrixorigin/matrixone/pkg/util/status"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	rssCacheFamilyEvictTimeout   = 10 * time.Second
	rssCacheAdmissionPressureTTL = 2 * time.Minute
	rssCachePressureTargetOwner  = "cn-rss"
	bootstrapRetryInterval       = 100 * time.Millisecond
	txnTraceDirectoryKeyPrefix   = "cn-"
)

var (
	evictMemoryCachesToCapacityPercent = fileservice.EvictMemoryCachesToCapacityPercent
)

func makeRSSCacheEvictor(timeout time.Duration) func(context.Context, int64) {
	return func(ctx context.Context, targetPercent int64) {
		memoryCtx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseRSSCacheEvict)
		defer cancel()
		evictMemoryCachesToCapacityPercent(memoryCtx, targetPercent)
	}
}

func setRSSCachePressureTarget(targetPercent int64) {
	fileservice.SetMemoryCachePressureTargetPercentByOwner(
		rssCachePressureTargetOwner,
		targetPercent,
		time.Now().Add(rssCacheAdmissionPressureTTL),
	)
}

func clearRSSCachePressureTarget() {
	fileservice.ClearMemoryCachePressureTargetByOwner(rssCachePressureTargetOwner)
}

func NewService(
	cfg *Config,
	ctx context.Context,
	fileService fileservice.FileService,
	gossipNode *gossip.Node,
	options ...Option,
) (result Service, err error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	//set frontend parameters
	cfg.Frontend.SetDefaultValues()
	if err := cfg.Frontend.Iceberg.Validate(ctx); err != nil {
		return nil, err
	}
	if err := cfg.Frontend.MongoDB.Validate(ctx); err != nil {
		return nil, err
	}
	cfg.Frontend.SetMaxMessageSize(uint64(cfg.RPC.MaxMessageSize))

	configKVMap, _ := dumpCnConfig(*cfg)
	options = append(options, WithConfigData(configKVMap))

	options = append(options, WithBootstrapOptions(
		bootstrap.WithUpgradeTenantBatch(cfg.UpgradeTenantBatchSize),
		bootstrap.WithKek(cfg.Frontend.KeyEncryptionKey),
	))

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
	srv.colexecServer = colexec.NewServer(cfg.UUID)

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

	pu := config.NewParameterUnit(
		&cfg.Frontend,
		nil,
		nil,
		engine.Nodes{engine.Node{
			Addr: srv.pipelineServiceServiceAddr(),
		}})

	frontend.InitServerVersion(pu.SV.MoVersion)
	srv.pu = pu

	if _, err = srv.getHAKeeperClient(); err != nil {
		return nil, err
	}
	if err = srv.initViewMetadataAdmission(ctx); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			srv.closeViewMetadataAdmission()
		}
	}()
	if err = srv.initQueryService(); err != nil {
		return nil, err
	}

	if err = srv.initMetadata(); err != nil {
		return nil, err
	}
	srv.initTaskServiceHolder()

	srv.responsePool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}

	pu.HAKeeperClient = srv._hakeeperClient

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
		var pc *pythonservice.Client
		pc, err = pythonservice.NewClient(srv.cfg.PythonUdfClient)
		if err != nil {
			panic(err)
		}
		udfServices = append(udfServices, pc)
	}
	srv.udfService, err = udf.NewService(udfServices...)
	if err != nil {
		panic(err)
	}

	srv.CNMemoryThrottler = rscthrottler.NewMemThrottler(
		"CNFlushS3",
		90.0/100.0,
		rscthrottler.WithAcquirePolicy(rscthrottler.AcquirePolicyForCNFlushS3),
		rscthrottler.WithRSSScavenging(),
		rscthrottler.WithRSSCachePressureTarget(
			setRSSCachePressureTarget,
			clearRSSCachePressureTarget,
		),
		rscthrottler.WithRSSCacheEvictor(makeRSSCacheEvictor(rssCacheFamilyEvictTimeout)),
	)

	srv.pu.LockService = srv.lockService
	srv.pu.HAKeeperClient = srv._hakeeperClient
	srv.pu.QueryClient = srv.queryClient
	srv.pu.UdfService = srv.udfService
	srv._txnClient = pu.TxnClient
	srv.pu.CNMemoryThrottler = srv.CNMemoryThrottler

	if err = srv.initMOServer(ctx, pu, srv.aicm); err != nil {
		return nil, err
	}

	server, err := morpc.NewRPCServer(
		"pipeline-server",
		srv.pipelineServiceListenAddr(),
		morpc.NewMessageCodec(
			cfg.UUID,
			srv.acquireMessage,
			morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize)),
		),
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

	// TODO: global client need to refactor
	c, err := cnclient.NewPipelineClient(
		cfg.UUID,
		srv.pipelineServiceServiceAddr(),
		&cnclient.PipelineConfig{RPC: cfg.RPC},
	)
	if err != nil {
		panic(err)
	}
	srv.pipelines.client = c

	rt := runtime.ServiceRuntime(cfg.UUID)
	rt.SetGlobalVariables("parameter-unit", pu)
	rt.SetGlobalVariables(runtime.PipelineClient, c)
	rt.SetGlobalVariables(runtime.CNMemoryThrottler, srv.CNMemoryThrottler)
	if err := compile.RegisterDefaultIcebergScanPlanner(ctx, cfg.UUID, pu.SV.Iceberg); err != nil {
		return nil, err
	}
	if err := srv.registerDefaultIcebergMaintenanceExecutor(ctx); err != nil {
		return nil, err
	}
	// Start control-plane workers only after every schedule-command target is
	// initialized and construction can no longer fail. In particular,
	// task-service creation and gossip join must not race partially constructed
	// service state or escape from a failed NewService call.
	if err := srv.startCNStoreHeartbeat(); err != nil {
		return nil, err
	}
	return srv, nil
}

func (s *service) registerDefaultIcebergMaintenanceExecutor(ctx context.Context) error {
	cfg, err := icebergapi.NewConfigFromParameters(ctx, s.cfg.Frontend.Iceberg)
	if err != nil {
		return err
	}
	restOptions := []icebergcatalog.RESTClientOption{
		icebergcatalog.WithTokenProvider(compile.NewRuntimeIcebergTokenProvider(s.cfg.UUID)),
	}
	if compile.IcebergAllowPlainHTTPFromEnv() {
		restOptions = append(restOptions, icebergcatalog.WithAllowPlainHTTP(true))
	}
	catalogFactory := icebergcatalog.NewFactory(
		icebergcatalog.WithNativeRESTOptions(restOptions...),
		icebergcatalog.WithAdapter(
			icebergcatalog.AdapterIcebergGo,
			icebergcatalog.UnsupportedAdapterFactory{Name: icebergcatalog.AdapterIcebergGo},
		),
	)
	executor := sqliceberg.NewMaintenanceProcedureExecutorFromInternalSQLExecutor(
		s.sqlExecutor,
		sqliceberg.MaintenanceProcedureExecutorOptions{
			Config:                    cfg,
			Account:                   sqliceberg.AccountConfigForFeatureGate(cfg, 0),
			CatalogFactory:            catalogFactory,
			CommitVerifier:            icebergmaintenance.CatalogFactoryCommitVerifier{CatalogFactory: catalogFactory},
			OrphanTTL:                 cfg.Write.OrphanTTL,
			UseNativeRewriteManifests: true,
			UseNativeRewriteDataFiles: true,
			UseNativeExpireSnapshots:  true,
		},
	)
	var tableCache icebergwritecore.TableCache
	if rt := runtime.ServiceRuntime(s.cfg.UUID); rt != nil {
		if value, ok := rt.GetGlobalVariables(icebergapi.CacheInvalidatorRuntimeKey); ok {
			tableCache, _ = value.(icebergwritecore.TableCache)
		}
	}
	cacheInvalidator := icebergwritecore.MetadataCacheInvalidator{Cache: tableCache}
	dmlFactory := sqliceberg.NewDMLDeleteRuntimeCoordinatorFactoryFromInternalSQLExecutor(
		s.sqlExecutor,
		sqliceberg.DMLDeleteRuntimeCoordinatorFactoryOptions{
			Config:           cfg,
			Account:          sqliceberg.AccountConfigForFeatureGate(cfg, 0),
			CatalogFactory:   catalogFactory,
			CacheInvalidator: cacheInvalidator,
		},
	)
	appendFactory := sqliceberg.NewAppendRuntimeCoordinatorFactoryFromInternalSQLExecutor(
		s.sqlExecutor,
		sqliceberg.AppendRuntimeCoordinatorFactoryOptions{
			Config:           cfg,
			Account:          sqliceberg.AccountConfigForFeatureGate(cfg, 0),
			CatalogFactory:   catalogFactory,
			CacheInvalidator: cacheInvalidator,
		},
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		compile.IcebergAppendCoordinatorFactoryRuntimeKey,
		sqliceberg.WriteRuntimeCoordinatorFactory{
			Append: appendFactory,
			DML:    dmlFactory,
		},
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		frontend.IcebergMaintenanceCallExecutorRuntimeKey,
		frontend.IcebergMaintenanceProcedureExecutor{Executor: executor},
	)
	return nil
}

func (s *service) checkViewMetadataGenerationRevoked() error {
	if !s.viewMetadataGenerationRevoked.Load() {
		return nil
	}
	s.viewMetadataIngressReady.Store(false)
	s.task.runnerReady.Store(false)
	return moerr.NewInvalidStateNoCtx("CN view metadata admission generation revoked")
}

func (s *service) startUnlessViewMetadataGenerationRevoked(start func() error) error {
	if err := s.checkViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	if err := start(); err != nil {
		return err
	}
	return s.checkViewMetadataGenerationRevoked()
}

func (s *service) startFrontendUnlessViewMetadataGenerationRevoked() error {
	s.frontendLifecycleMu.Lock()
	defer s.frontendLifecycleMu.Unlock()
	return s.startUnlessViewMetadataGenerationRevoked(s.runMoServer)
}

func (s *service) Start() (err error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycle != serviceInitialized {
		return moerr.NewInvalidStateNoCtx("CN service already started or closed")
	}
	s.lifecycle = serviceStarting
	defer func() {
		if err != nil {
			s.lifecycle = serviceClosing
			err = errors.Join(err, s.closeService())
			s.lifecycle = serviceClosed
			return
		}
		s.lifecycle = serviceStarted
	}()

	if err = s.waitForClusterSelfReady(); err != nil {
		return err
	}
	if err = s.bootstrap(); err != nil {
		return err
	}
	s.viewMetadataCatalogFenceReady.Store(true)
	if err = s.waitForViewMetadataAdmission(); err != nil {
		return err
	}
	if err = s.startUnlessViewMetadataGenerationRevoked(func() error {
		return s.startSiriusRuntime(context.Background())
	}); err != nil {
		return err
	}

	s.initSqlWriterFactory()

	if err = s.startUnlessViewMetadataGenerationRevoked(s.queryService.Start); err != nil {
		return err
	}
	if err = s.startFrontendUnlessViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	if err = s.startUnlessViewMetadataGenerationRevoked(s.server.Start); err != nil {
		return err
	}

	// Admission authorizes local initialization; it does not make this CN
	// routable. Revalidate after every remote entry point is listening, then
	// linearize authoritative snapshot validation and ingress publication with
	// heartbeat snapshot storage. Keep the automatic upgrade owner alive until
	// this final handoff closes.
	if err = s.waitForViewMetadataIngressAdmission(); err != nil {
		return err
	}
	s.completeBootstrapUpgradeStartupWait()
	if err = s.checkViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	s.notifyHeartbeat()

	if err = s.checkViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	if err = s.publishTaskRunner(); err != nil {
		return err
	}
	return s.checkViewMetadataGenerationRevoked()
}

func (s *service) Close() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycle != serviceClosed {
		s.lifecycle = serviceClosing
	}
	err := s.closeService()
	s.lifecycle = serviceClosed
	return err
}

func (s *service) closeService() error {
	s.closeOnce.Do(func() {
		defer logutil.LogClose(s.logger, "cnservice")()

		s.closeViewMetadataAdmission()
		// Stop waits for any in-flight periodic heartbeat before teardown. Keep
		// ingress published until all local entry points and work have drained;
		// withdrawal below is the ownership handoff linearization point.
		s.stopper.Stop()

		s.closeErr = closeCNServiceSteps(
			// Query commands can reach frontend, task, engine, lock, shard,
			// auto-increment, and transaction state. Stop and drain this remote
			// ingress before clearing any of those dependencies.
			s.closeQueryService,
			s.stopFrontendSerialized,
			s.closeSiriusRuntime,
			s.closeBootstrapService,
			// Frontend shutdown stops accepting interactive work, while stopTask
			// drains scheduled ingestion statements. Only after both producers have
			// stopped may the MongoDB pool disconnect clients still leased by a
			// MongoScan operator.
			s.stopTask,
			s.closeMongoDBRuntime,
			s.closePipelineAdmission,
			s.server.Close,
			// Pipeline handlers and the auto-increment cleanup worker can issue
			// transactions. Drain both before closing their transaction and RPC
			// dependencies, while keeping the trace consumer alive for final events.
			s.waitPipelineHandlers,
			s.closeIncrService,
			s.withdrawViewMetadataAdmission,
			s.stopRPCs,
			s.closeTxnTraceService,
			func() error {
				// stop I/O pipeline
				ioutil.Stop(s.cfg.UUID)
				return nil
			},
			func() error {
				if s.gossipNode != nil {
					return s.gossipNode.Leave(time.Second)
				}
				return nil
			},
			s.lockService.Close,
			func() error {
				if s.shardService != nil {
					return s.shardService.Close()
				}
				return nil
			},
			func() error {
				if s.pipelines.client != nil {
					return s.pipelines.client.Close()
				}
				return nil
			},
		)
	})
	return s.closeErr
}

func (s *service) closePipelineAdmission() error {
	s.pipelines.mu.Lock()
	s.pipelines.closing = true
	cancels := make([]context.CancelFunc, 0, len(s.pipelines.cancels))
	for _, cancel := range s.pipelines.cancels {
		cancels = append(cancels, cancel)
	}
	s.pipelines.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

func (s *service) admitPipelineHandler(ctx context.Context) (context.Context, func(), bool) {
	s.pipelines.mu.Lock()
	if s.pipelines.closing {
		s.pipelines.mu.Unlock()
		return nil, nil, false
	}
	if s.pipelines.cancels == nil {
		s.pipelines.cancels = make(map[uint64]context.CancelFunc)
	}
	s.pipelines.nextID++
	id := s.pipelines.nextID
	handlerCtx, cancel := context.WithCancel(ctx)
	s.pipelines.cancels[id] = cancel
	s.pipelines.wg.Add(1)
	s.pipelines.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			cancel()
			s.pipelines.mu.Lock()
			delete(s.pipelines.cancels, id)
			s.pipelines.mu.Unlock()
			s.pipelines.wg.Done()
		})
	}
	return handlerCtx, release, true
}

func (s *service) waitPipelineHandlers() error {
	s.pipelines.wg.Wait()
	return nil
}

func (s *service) closeBootstrapService() error {
	if s.beforeBootstrapClose != nil {
		s.beforeBootstrapClose()
	}
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if s.bootstrapService == nil {
		return nil
	}
	service := s.bootstrapService
	s.bootstrapService = nil
	return service.Close()
}

func (s *service) closeTxnTraceService() error {
	if s.txnTraceService == nil {
		return nil
	}
	service := s.txnTraceService
	s.txnTraceService = nil
	service.Close()
	runtime.ServiceRuntime(s.cfg.UUID).CompareAndDeleteGlobalVariables(runtime.TxnTraceService, service)
	return nil
}

func (s *service) closeIncrService() error {
	if s.incrservice == nil {
		return nil
	}
	service := s.incrservice
	s.incrservice = nil
	service.Close()
	runtime.ServiceRuntime(s.cfg.UUID).CompareAndDeleteGlobalVariables(runtime.AutoIncrementService, service)
	return nil
}

func closeCNServiceSteps(steps ...func() error) error {
	var err error
	for _, step := range steps {
		err = errors.Join(err, step())
	}
	return err
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
	s.bootstrapMu.RLock()
	defer s.bootstrapMu.RUnlock()
	if s.bootstrapService == nil {
		return moerr.NewInvalidStateNoCtx("bootstrap service is closed")
	}
	finalVersion := s.bootstrapService.GetFinalVersion()
	tenantFetchFunc := func() (int32, string, error) {
		return int32(tenantID), finalVersion, nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*30, moerr.CauseCheckTenantUpgrade)
	defer cancel()
	if _, err := s.bootstrapService.MaybeUpgradeTenant(ctx, tenantFetchFunc, nil); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	return nil
}

// UpgradeTenant Manual command tenant upgrade entrance
func (s *service) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	s.bootstrapMu.RLock()
	defer s.bootstrapMu.RUnlock()
	if s.bootstrapService == nil {
		return moerr.NewInvalidStateNoCtx("bootstrap service is closed")
	}
	ctx, cancel := context.WithTimeoutCause(ctx, time.Minute*120, moerr.CauseUpgradeTenant)
	defer cancel()
	if _, err := s.bootstrapService.UpgradeTenant(ctx, tenantName, retryCount, isALLAccount); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	return nil
}

func (s *service) GetFinalVersion() string {
	s.bootstrapMu.RLock()
	defer s.bootstrapMu.RUnlock()
	if s.bootstrapService == nil {
		return ""
	}
	return s.bootstrapService.GetFinalVersion()
}

func (s *service) stopFrontend() error {
	defer logutil.LogClose(s.logger, "cnservice/frontend")()

	err := s.serverShutdown(true)
	if s.cancelMoServerFunc != nil {
		s.cancelMoServerFunc()
	}
	return err
}

func (s *service) stopFrontendSerialized() error {
	s.frontendLifecycleMu.Lock()
	defer s.frontendLifecycleMu.Unlock()
	return s.stopFrontend()
}

func (s *service) stopRPCs() error {
	var err error
	if s._txnClient != nil {
		err = errors.Join(err, s._txnClient.Close())
	}
	if s._hakeeperClient != nil {
		s.moCluster.Close()
		err = errors.Join(err, s._hakeeperClient.Close())
	}
	if s._txnSender != nil {
		err = errors.Join(err, s._txnSender.Close())
	}
	if s.lockService != nil {
		err = errors.Join(err, s.lockService.Close())
	}
	if s.queryClient != nil {
		err = errors.Join(err, s.queryClient.Close())
	}
	if s.timestampWaiter != nil {
		s.timestampWaiter.Close()
	}
	return err
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
	if s.pipelines.beforeAdmission != nil {
		s.pipelines.beforeAdmission()
	}
	handlerCtx, release, admitted := s.admitPipelineHandler(ctx)
	if !admitted {
		if value.Cancel != nil {
			value.Cancel()
		}
		return moerr.NewServiceUnavailableNoCtx("CN pipeline service is closing")
	}
	owned := true
	cancelOwned := value.Cancel != nil
	defer func() {
		if owned {
			release()
		}
		if cancelOwned {
			value.Cancel()
		}
	}()

	// the following comment is not related to my PR, but I suddenly saw this piece of code.
	// so I wrote it, hoping it can help future developers understand what this is doing.
	//
	// I'm not sure, but I think that's a logic to handle that
	// once an encoded-pipeline message was too large, it will be cut as multiple messages for sending.
	// and these codes keep receiving them, and then rebuild them as a big message.
	req := value.Message
	msg, ok := req.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", req)
		panic("cn server receive a message with unexpected type")
	}
	switch msg.GetSid() {
	case pipeline.Status_WaitingNext:
		transferred, err := handleWaitingNextMsg(ctx, value.Cancel, req, cs)
		if transferred {
			cancelOwned = false
		}
		return err
	case pipeline.Status_Last:
		if msg.IsPipelineMessage() { // only pipeline type need assemble msg now.
			if err := handleAssemblePipeline(ctx, req, cs); err != nil {
				return err
			}
		}
	}

	// start a goroutine to handle one received message.
	owned = false
	cancelOwned = false
	go func() {
		defer release()
		if value.Cancel != nil {
			defer value.Cancel()
		}
		s.pipelines.counter.Add(1)
		defer s.pipelines.counter.Add(-1)

		// there is no need to handle the return error, because the error will be logged in the function.
		_ = s.requestHandler(handlerCtx,
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

	default:
		return moerr.NewInternalErrorf(ctx, "unknown engine type: %s", s.cfg.Engine.Type)

	}

	return nil
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

		ctx, cancel := context.WithTimeoutCause(
			context.Background(),
			s.cfg.HAKeeper.DiscoveryTimeout.Duration,
			moerr.CauseGetHAKeeperClient,
		)
		defer cancel()
		client, err = logservice.NewCNHAKeeperClient(ctx, s.cfg.UUID, s.cfg.HAKeeper.ClientConfig)
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			return
		}
		s._hakeeperClient = client
		s.initClusterService()
		s.initLockService()

		ss, ok := runtime.ServiceRuntime(s.cfg.UUID).GetGlobalVariables(runtime.StatusServer)
		if ok {
			ss.(*status.Server).SetHAKeeperClient(client)
		}

	})
	client = s._hakeeperClient
	return
}

func (s *service) initClusterService() {
	s.moCluster = clusterservice.NewMOCluster(
		s.cfg.UUID,
		s._hakeeperClient,
		s.cfg.Cluster.RefreshInterval.Duration,
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(runtime.ClusterService, s.moCluster)
}

func (s *service) getTxnSender() (sender rpc.TxnSender, err error) {
	s.initTxnSenderOnce.Do(func() {
		sender, err = rpc.NewSender(
			s.cfg.RPC,
			runtime.ServiceRuntime(s.cfg.UUID),
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
		s.timestampWaiter = client.NewTimestampWaiter(runtime.ServiceRuntime(s.cfg.UUID).Logger())

		rt := runtime.ServiceRuntime(s.cfg.UUID)
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
					profPath := catalog.BuildProfilePath("CN", s.cfg.UUID, "leakcheck_routine", name.String()) + ".gz"

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
						} else if txn.Options.InIncrStmt {
							v2.TxnInIncrStmtCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found txn in incr statement", fields...)
						} else if txn.Options.InRollbackStmt {
							v2.TxnInRollbackStmtCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found txn in rollback statement", fields...)
						} else {
							v2.TxnLeakCounter.Inc()
							runtime.DefaultRuntime().Logger().Error("found leak txn", fields...)
						}
					}

					SaveProfile(profPath, profile.GOROUTINE, s.etlFS)
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
		traceService := trace.GetService(s.cfg.UUID)
		opts = append(opts,
			client.WithLockService(s.lockService),
			client.WithNormalStateNoWait(s.cfg.Txn.NormalStateNoWait),
			client.WithTxnOpenedCallback([]func(op client.TxnOperator){
				func(op client.TxnOperator) {
					traceService.TxnCreated(op)
				},
			}),
		)
		c = client.NewTxnClient(
			s.cfg.UUID,
			sender,
			opts...,
		)
		s._txnClient = c
	})
	c = s._txnClient
	return
}

func (s *service) initLockService() {
	cfg := s.getLockServiceConfig()
	s.lockService = lockservice.NewLockService(
		cfg,
		lockservice.WithWait(func(ctx context.Context) error {
			select {
			case <-s.hakeeperConnected:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(runtime.LockService, s.lockService)
	lockservice.SetLockServiceByServiceID(s.cfg.UUID, s.lockService)

	ss, ok := runtime.ServiceRuntime(s.cfg.UUID).GetGlobalVariables(runtime.StatusServer)
	if ok {
		ss.(*status.Server).SetLockService(s.cfg.UUID, s.lockService)
	}
}

func (s *service) initShardService() {
	cfg := s.getShardServiceConfig()
	if !cfg.Enable {
		return
	}

	store := shardservice.NewShardStorage(
		s.cfg.UUID,
		runtime.ServiceRuntime(s.cfg.UUID).Clock(),
		s.sqlExecutor,
		s.timestampWaiter,
		map[int]shardservice.ReadFunc{
			shardservice.ReadRows:                     disttae.HandleShardingReadRows,
			shardservice.ReadSize:                     disttae.HandleShardingReadSize,
			shardservice.ReadStats:                    disttae.HandleShardingReadStatus,
			shardservice.ReadApproxObjectsNum:         disttae.HandleShardingReadApproxObjectsNum,
			shardservice.ReadRanges:                   disttae.HandleShardingReadRanges,
			shardservice.ReadGetColumMetadataScanInfo: disttae.HandleShardingReadGetColumMetadataScanInfo,
			shardservice.ReadBuildReader:              disttae.HandleShardingReadBuildReader,
			shardservice.ReadPrimaryKeysMayBeModified: disttae.HandleShardingReadPrimaryKeysMayBeModified,
			shardservice.ReadPrimaryKeysMayBeUpserted: disttae.HandleShardingReadPrimaryKeysMayBeUpserted,
			shardservice.ReadMergeObjects:             disttae.HandleShardingReadMergeObjects,
			shardservice.ReadVisibleObjectStats:       disttae.HandleShardingReadVisibleObjectStats,
			shardservice.ReadClose:                    disttae.HandleShardingReadClose,
			shardservice.ReadNext:                     disttae.HandleShardingReadNext,
			shardservice.ReadCollectTombstones:        disttae.HandleShardingReadCollectTombstones,
		},
		s.storeEngine,
	)
	s.shardService = shardservice.NewService(
		cfg,
		store,
		shardservice.WithWaitCNReported(),
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		runtime.ShardService,
		s.shardService,
	)
}

func (s *service) initPartitionService() {
	store := partitionservice.NewStorage(
		s.cfg.UUID,
		s.sqlExecutor,
		s.storeEngine,
	)
	s.partitionService = partitionservice.NewService(
		s.getPartitionServiceConfig(),
		store,
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		runtime.PartitionService,
		s.partitionService,
	)
}

func (s *service) GetSQLExecutor() executor.SQLExecutor {
	return s.sqlExecutor
}

func (s *service) GetBootstrapService() bootstrap.Service {
	s.bootstrapMu.RLock()
	defer s.bootstrapMu.RUnlock()
	return s.bootstrapService
}

func (s *service) GetTimestampWaiter() client.TimestampWaiter {
	return s.timestampWaiter
}
func (s *service) GetEngine() engine.Engine {
	return s.storeEngine
}

func (s *service) GetClock() clock.Clock {
	return runtime.ServiceRuntime(s.cfg.UUID).Clock()
}

// put the waiting-next type msg into client session's cache and return directly
func handleWaitingNextMsg(
	ctx context.Context,
	cancel context.CancelFunc,
	message morpc.Message,
	cs morpc.ClientSession) (bool, error) {
	msg, _ := message.(*pipeline.Message)
	switch msg.GetCmd() {
	case pipeline.Method_PipelineMessage:
		var cache morpc.MessageCache
		var err error
		cache, err = cs.CreateCacheWithCancel(ctx, message.GetID(), cancel)
		if err != nil {
			return false, err
		}
		return true, cache.Add(message)
	default:
		return false, moerr.NewInvalidInputNoCtx("only pipeline messages may be fragmented")
	}
}

func handleAssemblePipeline(ctx context.Context, message morpc.Message, cs morpc.ClientSession) error {
	var data []byte

	cache, err := cs.CreateCache(ctx, message.GetID())
	if err != nil {
		return err
	}
	// CreateCache also returns a cache for an unfragmented message. Always
	// remove the map entry on every terminal assembly path, not just close the
	// queue object.
	defer cs.DeleteCache(message.GetID())
	finalMessage := message.(*pipeline.Message)
	for {
		cached, ok, err := cache.Pop()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		fragment, ok := cached.(*pipeline.Message)
		if !ok || fragment.GetCmd() != finalMessage.GetCmd() ||
			fragment.GetRequestedTeardownMode() != finalMessage.GetRequestedTeardownMode() ||
			fragment.GetRequestedBatchCreditCount() != finalMessage.GetRequestedBatchCreditCount() ||
			fragment.GetRequestedBatchCreditBytes() != finalMessage.GetRequestedBatchCreditBytes() {
			return moerr.NewInvalidInputNoCtx("inconsistent pipeline message fragments")
		}
		data = append(data, fragment.GetData()...)
	}
	finalMessage.SetData(append(data, finalMessage.GetData()...))
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
		s.pu.GetTaskService(),
	)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(runtime.InternalSQLExecutor, s.sqlExecutor)
	s.initMongoDBRuntime()
}

func (s *service) initMongoDBRuntime() {
	parameters := s.pu.SV.MongoDB
	allowedAccounts := make(map[uint32]struct{}, len(parameters.AllowedAccounts))
	for _, accountID := range parameters.AllowedAccounts {
		allowedAccounts[accountID] = struct{}{}
	}
	config := sqlmongodb.RuntimeConfig{
		Enable: parameters.Enable, EnablePerAccount: parameters.EnablePerAccount,
		AllowedAccounts: allowedAccounts, AllowLoopback: parameters.AllowLoopback,
		AllowedHostSuffixes:    append([]string(nil), parameters.AllowedHostSuffixes...),
		AllowedCIDRs:           append([]string(nil), parameters.AllowedCIDRs...),
		ConnectTimeout:         parameters.ConnectTimeout.Duration,
		ServerSelectionTimeout: parameters.ServerSelectionTimeout.Duration,
		SocketTimeout:          parameters.SocketTimeout.Duration,
		MaxPoolSize:            parameters.MaxPoolSize, MinPoolSize: parameters.MinPoolSize,
		MaxConnecting: parameters.MaxConnecting, MaxCachedClients: parameters.MaxCachedClients,
		BatchRows:     parameters.BatchRows,
		MaxBatchBytes: parameters.MaxBatchBytes, MaxValueBytes: parameters.MaxValueBytes,
		MaxScanRows: parameters.MaxScanRows, MaxScanBytes: parameters.MaxScanBytes,
		MaxConversionErrors: parameters.MaxConversionErrors, MaxConversionErrorRate: parameters.MaxConversionErrorRate,
		MaxSourceConcurrency: parameters.MaxSourceConcurrency,
	}
	pool := sqlmongodb.NewValidatedClientPool(
		sqlmongodb.OfficialClientFactory{},
		sqlmongodb.CatalogConnectionResolver{Executor: s.sqlExecutor},
		config.MaxCachedClients,
	)
	retirements := sqlmongodb.NewClientRetirementQueue(
		pool,
		sqlmongodb.ClusterRemoteClientRetirer{Cluster: s.moCluster, QueryClient: s.queryClient},
		sqlmongodb.DefaultClientRetirementQueueCapacity,
	)
	dependencies := &sqlmongodb.RuntimeDependencies{
		Config:      config,
		Connections: sqlmongodb.CatalogConnectionResolver{Executor: s.sqlExecutor},
		Mappings:    sqlmongodb.CatalogMappingResolver{Executor: s.sqlExecutor},
		Secrets:     sqlmongodb.EnvSecretResolver{},
		Pool:        pool, Limiter: sqlmongodb.NewSourceLimiter(config.MaxSourceConcurrency),
		Retirements: retirements,
	}
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(sqlmongodb.RuntimeDependenciesKey, dependencies)
}

func (s *service) closeMongoDBRuntime() error {
	rt := runtime.ServiceRuntime(s.cfg.UUID)
	value, ok := rt.GetGlobalVariables(sqlmongodb.RuntimeDependenciesKey)
	if !ok {
		return nil
	}
	dependencies, ok := value.(*sqlmongodb.RuntimeDependencies)
	if !ok || dependencies == nil || dependencies.Pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseShutdown)
	defer cancel()
	var err error
	if dependencies.Retirements != nil {
		err = dependencies.Retirements.Close(ctx)
	}
	return errors.Join(err, dependencies.Pool.Close(ctx))
}

func (s *service) initIncrService() {
	store, err := incrservice.NewSQLStore(
		s.sqlExecutor,
		s.lockService,
	)
	if err != nil {
		panic(err)
	}
	s.incrservice = incrservice.NewIncrService(
		s.cfg.UUID,
		store,
		s.cfg.AutoIncrement)
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		runtime.AutoIncrementService,
		s.incrservice)
	incrservice.SetAutoIncrementServiceByID(s.cfg.UUID, s.incrservice)
}

func (s *service) bootstrap() error {
	s.initIncrService()
	s.initTxnTraceService()

	rt := runtime.ServiceRuntime(s.cfg.UUID)
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if s.bootstrapService == nil {
		s.bootstrapService = bootstrap.NewService(
			s.cfg.UUID,
			&locker{hakeeperClient: s._hakeeperClient, requestID: s.cfg.UUID},
			rt.Clock(),
			s._txnClient,
			s.sqlExecutor,
			s.options.bootstrapOptions...,
		)
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Minute*5, moerr.CauseBootstrap)
	ctx = context.WithValue(ctx, config.ParameterUnitKey, s.pu)
	defer cancel()

	// Bootstrap owns retrying only the initialization phase after it has acquired
	// the bootstrap privilege. Retrying this whole state machine can allocate a
	// second lock ID after an uncertain allocation response.
	if err := s.bootstrapService.Bootstrap(ctx); err != nil {
		return handleBootstrapErr(ctx, err)
	}

	trace.GetService(s.cfg.UUID).EnableFlush()

	if s.cfg.AutomaticUpgrade {
		s.bootstrapUpgradeResult = make(chan error, 1)
		s.bootstrapUpgradeStartupReady = make(chan struct{})
		started := make(chan struct{})
		if err := s.stopper.RunTask(func(taskCtx context.Context) {
			ctx, cancel := context.WithTimeoutCause(taskCtx, time.Minute*120, moerr.CauseBootstrap2)
			s.bootstrapUpgradeContext = ctx
			close(started)
			defer cancel()

			s.bootstrapMu.RLock()
			defer s.bootstrapMu.RUnlock()
			var err error
			if s.bootstrapService == nil {
				err = moerr.NewInternalErrorNoCtx("bootstrap service closed during automatic upgrade")
			} else {
				err = s.bootstrapService.BootstrapUpgrade(ctx)
			}
			if err == nil {
				select {
				case <-s.bootstrapUpgradeStartupReady:
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
			if err != nil {
				err = moerr.AttachCause(ctx, err)
				if !errors.Is(err, context.Canceled) {
					runtime.DefaultRuntime().Logger().Error(
						"bootstrap system automatic upgrade failed by: ", zap.Error(err))
				}
			}
			// Serialize terminal-result publication with admission acceptance so
			// startup cannot commit a success after an already-completed failure.
			s.lockViewMetadataAdmission()
			s.bootstrapUpgradeResult <- err
			s.viewMetadataAdmissionMu.Unlock()
		}); err != nil {
			return err
		}
		// Publish the owner context before admission starts using it. The task
		// signals before taking bootstrapMu because bootstrap currently owns the
		// write lock until this method returns.
		<-started
	}
	return nil
}

func (s *service) completeBootstrapUpgradeStartupWait() {
	if s.bootstrapUpgradeStartupReady == nil {
		return
	}
	s.bootstrapUpgradeReadyOnce.Do(func() {
		close(s.bootstrapUpgradeStartupReady)
	})
}

// handleBootstrapErr preserves the bootstrap context cause and returns the
// failure to Start's caller. The caller owns rolling back the fully constructed
// service before it returns the error.
func handleBootstrapErr(ctx context.Context, err error) error {
	return moerr.AttachCause(ctx, err)
}

func resolveTxnTraceDataPath(rootDir, serviceID string) (string, error) {
	if err := validateCNServiceUUID(serviceID); err != nil {
		return "", err
	}
	if rootDir == "" {
		return "", nil
	}
	return filepath.Join(rootDir, txnTraceDirectoryKey(serviceID)), nil
}

func txnTraceDirectoryKey(serviceID string) string {
	// A fixed-length lowercase hash keeps the directory component below common
	// filesystem limits while remaining stable for the same CN service ID.
	digest := sha256.Sum256([]byte(serviceID))
	return txnTraceDirectoryKeyPrefix + hex.EncodeToString(digest[:])
}

func (s *service) initTxnTraceService() {
	traceDataPath, err := resolveTxnTraceDataPath(s.options.traceDataPath, s.cfg.UUID)
	if err != nil {
		panic(err)
	}
	rt := runtime.ServiceRuntime(s.cfg.UUID)
	ts, err := trace.NewService(
		traceDataPath,
		s.cfg.UUID,
		s._txnClient,
		rt.Clock(),
		s.sqlExecutor,
		trace.WithEnable(s.cfg.Txn.Trace.Enable, s.cfg.Txn.Trace.Tables),
		trace.WithBufferSize(s.cfg.Txn.Trace.BufferSize),
		trace.WithFlushBytes(int(s.cfg.Txn.Trace.FlushBytes)),
		trace.WithFlushDuration(s.cfg.Txn.Trace.FlushDuration.Duration),
		trace.WithLoadToS3(!s.cfg.Txn.Trace.LoadToMO, s.etlFS),
	)
	if err != nil {
		panic(err)
	}
	s.txnTraceService = ts
	rt.SetGlobalVariables(runtime.TxnTraceService, s.txnTraceService)
}

// SaveProfile saves profile into etl fs
// profileType defined in pkg/util/profile/profile.go
func SaveProfile(profilePath string, profileType string, etlFS fileservice.FileService) {
	if len(profilePath) == 0 || len(profileType) == 0 || etlFS == nil {
		return
	}

	//gzip compress
	buf := bytes.Buffer{}
	gzWriter := gzip.NewWriter(&buf)

	debug := 0
	if profile.GOROUTINE == profileType {
		debug = 2
	}
	err := profile.ProfileRuntime(profileType, gzWriter, debug)
	if err != nil {
		logutil.Error(
			"profile.save.runtime.failed",
			zap.String("path", profilePath),
			zap.Error(err),
		)
		return
	}
	err = gzWriter.Close()
	if err != nil {
		logutil.Error(
			"profile.writer.close.failed",
			zap.String("path", profilePath),
			zap.Error(err),
		)
		return
	}
	logutil.Info(
		"profile.save.get.ok",
		zap.String("path", profilePath),
	)
	writeVec := fileservice.IOVector{
		FilePath: profilePath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Data:   buf.Bytes(),
				Size:   int64(len(buf.Bytes())),
			},
		},
	}
	ctx, cancel := context.WithTimeoutCause(context.TODO(), time.Minute*3, moerr.CauseSaveProfile)
	defer cancel()
	err = etlFS.Write(ctx, writeVec)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logutil.Error(
			"profile.save.failed",
			zap.String("path", profilePath),
			zap.Error(err),
		)
		return
	}
}

type locker struct {
	hakeeperClient logservice.CNHAKeeperClient
	requestID      string
}

type idempotentKeyedIDAllocator interface {
	AllocateIDByKeyWithRequestID(ctx context.Context, key string, batch uint64, requestID string) (uint64, error)
}

func (l *locker) Get(
	ctx context.Context,
	key string) (bool, error) {
	allocator, ok := l.hakeeperClient.(idempotentKeyedIDAllocator)
	if !ok {
		return false, moerr.NewInternalError(ctx, "HAKeeper client does not support idempotent bootstrap lock allocation")
	}
	v, err := allocator.AllocateIDByKeyWithRequestID(ctx, key, 1, l.requestID)
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

func (s *service) initProcessCodecService() {
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		runtime.ProcessCodecService,
		process.NewCodecService(
			s._txnClient,
			s.fileService,
			s.lockService,
			s.partitionService,
			s.queryClient,
			s._hakeeperClient,
			s.udfService,
			s.storeEngine,
		),
	)
}

func (s *service) GetTxnClient() client.TxnClient {
	return s._txnClient
}
