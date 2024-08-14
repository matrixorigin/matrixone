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

/*
Package logservice implement MO's LogService component.
*/
package logservice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/tnservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

const (
	LogServiceRPCName = "logservice-server"
)

type Lsn = uint64

type LogRecord = pb.LogRecord

// TODO: move this to a better place
func firstError(err1 error, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

// Service is the top layer component of a log service node. It manages the
// underlying log store which in turn manages all log shards including the
// HAKeeper shard. The Log Service component communicates with LogService
// clients owned by TN nodes and the HAKeeper service via network, it can
// be considered as the interface layer of the LogService.
type Service struct {
	cfg         Config
	runtime     runtime.Runtime
	store       *store
	server      morpc.RPCServer
	pool        *sync.Pool
	respPool    *sync.Pool
	stopper     *stopper.Stopper
	haClient    LogHAKeeperClient
	fileService fileservice.FileService
	shutdownC   chan struct{}

	options struct {
		// morpc client would filter remote backend via this
		backendFilter func(msg morpc.Message, backendAddr string) bool
	}

	task struct {
		sync.RWMutex
		created        bool
		holder         taskservice.TaskServiceHolder
		storageFactory taskservice.TaskStorageFactory
	}

	config *util.ConfigData

	// dataSync is used to sync data to other modules.
	dataSync DataSync
}

func NewService(
	cfg Config,
	fileService fileservice.FileService,
	shutdownC chan struct{},
	opts ...Option,
) (*Service, error) {
	cfg.Fill()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	configKVMap, _ := dumpLogConfig(cfg)
	opts = append(opts, WithConfigData(configKVMap))

	service := &Service{
		cfg:         cfg,
		stopper:     stopper.NewStopper("log-service"),
		fileService: fileService,
		shutdownC:   shutdownC,
	}
	for _, opt := range opts {
		opt(service)
	}
	if service.runtime == nil {
		service.runtime = runtime.DefaultRuntime()
	}

	tnservice.InitCheckState(cfg.UUID)

	var onReplicaChanged func(shardID uint64, replicaID uint64, typ ChangeType)
	if service.dataSync != nil {
		onReplicaChanged = service.dataSync.NotifyReplicaID
	}
	store, err := newLogStore(cfg, service.getTaskService, onReplicaChanged, service.runtime)
	if err != nil {
		service.runtime.Logger().Error("failed to create log store", zap.Error(err))
		return nil, err
	}
	if err := store.loadMetadata(); err != nil {
		return nil, err
	}
	if err := store.startReplicas(); err != nil {
		return nil, err
	}
	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	mf := func() morpc.Message {
		return pool.Get().(*RPCRequest)
	}

	var codecOpts []morpc.CodecOption
	codecOpts = append(codecOpts, morpc.WithCodecPayloadCopyBufferSize(16*1024),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize)))
	if cfg.RPC.EnableCompress {
		codecOpts = append(codecOpts, morpc.WithCodecEnableCompress(malloc.GetDefault(nil)))
	}

	// TODO: check and fix all these magic numbers
	codec := morpc.NewMessageCodec(cfg.UUID, mf, codecOpts...)
	server, err := morpc.NewRPCServer(LogServiceRPCName, cfg.LogServiceListenAddr(), codec,
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
			msg := i.(morpc.RPCMessage)
			if !msg.InternalMessage() {
				respPool.Put(msg.Message)
			}
		})),
		morpc.WithServerLogger(service.runtime.Logger().RawLogger()),
	)
	if err != nil {
		return nil, err
	}

	service.store = store
	service.server = server
	service.pool = pool
	service.respPool = respPool

	server.RegisterRequestHandler(service.handleRPCRequest)
	// TODO: before making the service available to the outside world, restore all
	// replicas already known to the local store
	if err := server.Start(); err != nil {
		service.runtime.SubLogger(runtime.SystemInit).Error("failed to start the server", zap.Error(err))
		if err := store.close(); err != nil {
			service.runtime.SubLogger(runtime.SystemInit).Error("failed to close the store", zap.Error(err))
		}
		return nil, err
	}
	// start the heartbeat worker
	if !cfg.DisableWorkers {
		if err := service.stopper.RunNamedTask("log-heartbeat-worker", func(ctx context.Context) {
			service.runtime.SubLogger(runtime.SystemInit).Info("logservice heartbeat worker started")

			// transfer morpc options via context
			ctx = SetBackendOptions(ctx, service.getBackendOptions()...)
			ctx = SetClientOptions(ctx, service.getClientOptions()...)
			service.heartbeatWorker(ctx)
		}); err != nil {
			return nil, err
		}
	}
	service.initTaskHolder()
	service.initSqlWriterFactory()
	return service, nil
}

func (s *Service) Start() error {
	return nil
}

func (s *Service) Close() (err error) {
	s.stopper.Stop()
	if s.haClient != nil {
		err = firstError(err, s.haClient.Close())
	}
	err = firstError(err, s.server.Close())
	if s.store != nil {
		err = firstError(err, s.store.close())
	}
	s.task.RLock()
	ts := s.task.holder
	s.task.RUnlock()
	if ts != nil {
		err = firstError(err, ts.Close())
	}
	if s.dataSync != nil {
		err = firstError(err, s.dataSync.Close())
	}
	return err
}

func (s *Service) ID() string {
	return s.store.id()
}

func (s *Service) handleRPCRequest(
	ctx context.Context,
	msg morpc.RPCMessage,
	seq uint64,
	cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "Service.handleRPCRequest")
	defer span.End()

	req := msg.Message
	rr, ok := req.(*RPCRequest)
	if !ok {
		panic("unexpected message type")
	}
	defer rr.Release()
	resp, records := s.handle(ctx, rr.Request, rr.GetPayloadField())
	var recs []byte
	if len(records.Records) > 0 {
		recs = MustMarshal(&records)
	}
	resp.RequestID = rr.RequestID
	response := s.respPool.Get().(*RPCResponse)
	response.Response = resp
	response.payload = recs
	return cs.Write(ctx, response)
}

func (s *Service) handle(ctx context.Context, req pb.Request,
	payload []byte) (pb.Response, pb.LogRecordResponse) {
	ctx, span := trace.Debug(ctx, "Service.handle."+req.Method.String())
	defer span.End()
	switch req.Method {
	case pb.TSO_UPDATE:
		return s.handleTsoUpdate(ctx, req), pb.LogRecordResponse{}
	case pb.APPEND:
		return s.handleAppend(ctx, req, payload), pb.LogRecordResponse{}
	case pb.READ:
		return s.handleRead(ctx, req)
	case pb.TRUNCATE:
		return s.handleTruncate(ctx, req), pb.LogRecordResponse{}
	case pb.GET_TRUNCATE:
		return s.handleGetTruncatedIndex(ctx, req), pb.LogRecordResponse{}
	case pb.CONNECT:
		return s.handleConnect(ctx, req), pb.LogRecordResponse{}
	case pb.CONNECT_RO:
		return s.handleConnectRO(ctx, req), pb.LogRecordResponse{}
	case pb.LOG_HEARTBEAT:
		return s.handleLogHeartbeat(ctx, req), pb.LogRecordResponse{}
	case pb.CN_HEARTBEAT:
		return s.handleCNHeartbeat(ctx, req), pb.LogRecordResponse{}
	case pb.CN_ALLOCATE_ID:
		return s.handleCNAllocateID(ctx, req), pb.LogRecordResponse{}
	case pb.TN_HEARTBEAT:
		return s.handleTNHeartbeat(ctx, req), pb.LogRecordResponse{}
	case pb.CHECK_HAKEEPER:
		return s.handleCheckHAKeeper(ctx, req), pb.LogRecordResponse{}
	case pb.GET_CLUSTER_DETAILS:
		return s.handleGetClusterDetails(ctx, req), pb.LogRecordResponse{}
	case pb.GET_CLUSTER_STATE:
		return s.handleGetCheckerState(ctx, req), pb.LogRecordResponse{}
	case pb.GET_SHARD_INFO:
		return s.handleGetShardInfo(ctx, req), pb.LogRecordResponse{}
	case pb.UPDATE_CN_LABEL:
		return s.handleUpdateCNLabel(ctx, req), pb.LogRecordResponse{}
	case pb.UPDATE_CN_WORK_STATE:
		return s.handleUpdateCNWorkState(ctx, req), pb.LogRecordResponse{}
	case pb.PATCH_CN_STORE:
		return s.handlePatchCNStore(ctx, req), pb.LogRecordResponse{}
	case pb.DELETE_CN_STORE:
		return s.handleDeleteCNStore(ctx, req), pb.LogRecordResponse{}
	case pb.PROXY_HEARTBEAT:
		return s.handleProxyHeartbeat(ctx, req), pb.LogRecordResponse{}
	case pb.UPDATE_NON_VOTING_REPLICA_NUM:
		return s.handleUpdateNonVotingReplicaNum(ctx, req), pb.LogRecordResponse{}
	case pb.UPDATE_NON_VOTING_LOCALITY:
		return s.handleUpdateNonVotingLocality(ctx, req), pb.LogRecordResponse{}
	case pb.GET_LATEST_LSN:
		return s.handleGetLatestLsn(ctx, req), pb.LogRecordResponse{}
	case pb.SET_REQUIRED_LSN:
		return s.handleSetRequiredLsn(ctx, req), pb.LogRecordResponse{}
	case pb.GET_REQUIRED_LSN:
		return s.handleGetRequiredLsn(ctx, req), pb.LogRecordResponse{}
	case pb.GET_LEADER_ID:
		return s.handleGetLeaderID(ctx, req), pb.LogRecordResponse{}
	default:
		resp := getResponse(req)
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(
			moerr.NewNotSupported(ctx,
				fmt.Sprintf("logservice method type %d", req.Method)))
		return resp, pb.LogRecordResponse{}
	}
}

func getResponse(req pb.Request) pb.Response {
	return pb.Response{Method: req.Method}
}

func (s *Service) handleGetShardInfo(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if result, ok := s.getShardInfo(req.LogRequest.ShardID); !ok {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(dragonboat.ErrShardNotFound)
	} else {
		resp.ShardInfo = &result
	}
	return resp
}

func (s *Service) handleGetClusterDetails(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if v, err := s.store.getClusterDetails(ctx); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.ClusterDetails = &v
	}
	return resp
}

func (s *Service) handleGetCheckerState(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if v, err := s.store.getCheckerState(); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.CheckerState = v
	}
	return resp
}

func (s *Service) handleTsoUpdate(ctx context.Context, req pb.Request) pb.Response {
	r := req.TsoRequest
	resp := getResponse(req)
	if v, err := s.store.tsoUpdate(ctx, r.Count); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.TsoResponse = &pb.TsoResponse{Value: v}
	}
	return resp
}

func (s *Service) handleConnect(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.getOrExtendTNLease(ctx, r.ShardID, r.TNID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleConnectRO(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	// we only check whether the specified shard is available
	if _, err := s.store.getTruncatedLsn(ctx, r.ShardID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleAppend(ctx context.Context, req pb.Request, payload []byte) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.append(ctx, r.ShardID, payload)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
		s.onAppend(ctx, req, lsn, payload)
	}
	return resp
}

func (s *Service) onAppend(ctx context.Context, req pb.Request, lsn Lsn, payload []byte) {
	if s.dataSync != nil && req.LogRequest.TNID > 0 { // send the data only from the TN
		s.dataSync.Append(ctx, lsn, payload)
	}
}

func (s *Service) handleRead(ctx context.Context, req pb.Request) (pb.Response, pb.LogRecordResponse) {
	r := req.LogRequest
	resp := getResponse(req)
	records, lsn, err := s.store.queryLog(ctx, r.ShardID, r.Lsn, r.MaxSize)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.LastLsn = lsn
	}
	return resp, pb.LogRecordResponse{Records: records}
}

func (s *Service) handleTruncate(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.truncateLog(ctx, r.ShardID, r.Lsn); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleGetTruncatedIndex(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.getTruncatedLsn(ctx, r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
	}
	return resp
}

// TODO: add tests to see what happens when request is sent to non hakeeper stores
func (s *Service) handleLogHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	start := time.Now()
	defer func() {
		v2.LogHeartbeatRecvHistogram.Observe(time.Since(start).Seconds())
	}()
	hb := req.LogHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addLogStoreHeartbeat(ctx, *hb); err != nil {
		v2.LogHeartbeatRecvFailureCounter.Inc()
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = &cb
	}

	return resp
}

func (s *Service) handleCNHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	start := time.Now()
	defer func() {
		v2.CNHeartbeatRecvHistogram.Observe(time.Since(start).Seconds())
	}()
	hb := req.CNHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addCNStoreHeartbeat(ctx, *hb); err != nil {
		v2.CNHeartbeatRecvFailureCounter.Inc()
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = &cb
	}

	return resp
}

func (s *Service) handleCNAllocateID(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	firstID, err := s.store.cnAllocateID(ctx, *req.CNAllocateID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	resp.AllocateID = &pb.AllocateIDResponse{FirstID: firstID}
	return resp
}

func (s *Service) handleTNHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	start := time.Now()
	defer func() {
		v2.TNHeartbeatRecvHistogram.Observe(time.Since(start).Seconds())
	}()
	hb := req.TNHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addTNStoreHeartbeat(ctx, *hb); err != nil {
		v2.TNHeartbeatRecvFailureCounter.Inc()
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = &cb
	}

	return resp
}

func (s *Service) handleCheckHAKeeper(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if atomic.LoadUint64(&s.store.haKeeperReplicaID) != 0 {
		resp.IsHAKeeper = true
	}
	return resp
}

func (s *Service) handleUpdateCNLabel(ctx context.Context, req pb.Request) pb.Response {
	label := req.CNStoreLabel
	resp := getResponse(req)
	if err := s.store.updateCNLabel(ctx, *label); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handleUpdateCNWorkState(ctx context.Context, req pb.Request) pb.Response {
	workState := req.CNWorkState
	resp := getResponse(req)
	if err := s.store.updateCNWorkState(ctx, *workState); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handlePatchCNStore(ctx context.Context, req pb.Request) pb.Response {
	stateLabel := req.CNStateLabel
	resp := getResponse(req)
	if err := s.store.patchCNStore(ctx, *stateLabel); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handleDeleteCNStore(ctx context.Context, req pb.Request) pb.Response {
	cnStore := req.DeleteCNStore
	resp := getResponse(req)
	if err := s.store.deleteCNStore(ctx, *cnStore); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handleProxyHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if cb, err := s.store.addProxyHeartbeat(ctx, *req.ProxyHeartbeat); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = &cb
	}
	return resp
}

func (s *Service) handleUpdateNonVotingReplicaNum(ctx context.Context, req pb.Request) pb.Response {
	num := req.NonVotingReplicaNum
	resp := getResponse(req)
	if err := s.store.updateNonVotingReplicaNum(ctx, num); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handleUpdateNonVotingLocality(ctx context.Context, req pb.Request) pb.Response {
	resp := getResponse(req)
	if err := s.store.updateNonVotingLocality(ctx, *req.NonVotingLocality); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	return resp
}

func (s *Service) handleGetLatestLsn(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.getLatestLsn(ctx, r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
	}
	return resp
}

func (s *Service) handleSetRequiredLsn(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.setRequiredLsn(ctx, r.ShardID, r.Lsn); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleGetRequiredLsn(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.getRequiredLsn(ctx, r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
	}
	return resp
}

func (s *Service) handleGetLeaderID(ctx context.Context, req pb.Request) pb.Response {
	r := req.LogRequest
	resp := getResponse(req)
	leaderID, err := s.store.leaderID(r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.LeaderID = leaderID
	}
	return resp
}

func (s *Service) handleAddLogShard(cmd pb.ScheduleCommand) {
	shardID := cmd.AddLogShard.ShardID
	var wg sync.WaitGroup
	wg.Add(1)
	if err := s.stopper.RunNamedTask("add new log shard", func(ctx context.Context) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(ctx, time.Second*3)
		defer cancel()
		if err := s.store.addLogShard(ctx, pb.AddLogShard{ShardID: shardID}); err != nil {
			s.runtime.Logger().Error("failed to add shard",
				zap.Uint64("shard ID", shardID),
				zap.Error(err),
			)
		}
	}); err != nil {
		s.runtime.Logger().Error("failed to create add log shard task",
			zap.Uint64("shard ID", shardID),
			zap.Error(err),
		)
		wg.Done()
	}
	wg.Wait()
}

func (s *Service) handleBootstrapShard(cmd pb.ScheduleCommand) {
	if err := s.store.startReplica(
		cmd.BootstrapShard.ShardID,
		cmd.BootstrapShard.ReplicaID,
		cmd.BootstrapShard.InitialMembers,
		cmd.BootstrapShard.Join,
	); err != nil {
		s.runtime.Logger().Error("failed to bootstrap shard",
			zap.Uint64("shard ID", cmd.BootstrapShard.ShardID),
			zap.Error(err),
		)
	}
}

func (s *Service) getBackendOptions() []morpc.BackendOption {
	return []morpc.BackendOption{
		morpc.WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			m, ok := msg.(*RPCRequest)
			if !ok {
				return true
			}
			return s.options.backendFilter == nil || s.options.backendFilter(m, backendAddr)
		}),
	}
}

// NB: leave an empty method for future extension.
func (s *Service) getClientOptions() []morpc.ClientOption {
	return []morpc.ClientOption{}
}
