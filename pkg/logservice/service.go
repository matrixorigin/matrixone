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
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/dragonboat/v4"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	LogServiceRPCName = "logservice-rpc"
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
// clients owned by DN nodes and the HAKeeper service via network, it can
// be considered as the interface layer of the LogService.
type Service struct {
	cfg         Config
	logger      *zap.Logger
	store       *store
	server      morpc.RPCServer
	pool        *sync.Pool
	respPool    *sync.Pool
	stopper     *stopper.Stopper
	haClient    LogHAKeeperClient
	fileService fileservice.FileService

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
}

func NewService(
	cfg Config,
	fileService fileservice.FileService,
	opts ...Option,
) (*Service, error) {
	cfg.Fill()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	service := &Service{
		cfg:         cfg,
		stopper:     stopper.NewStopper("log-service"),
		fileService: fileService,
	}
	for _, opt := range opts {
		opt(service)
	}

	service.logger = logutil.Adjust(service.logger)

	store, err := newLogStore(cfg, service.getTaskService, service.logger)
	if err != nil {
		service.logger.Error("failed to create log store", zap.Error(err))
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
	// TODO: check and fix all these magic numbers
	codec := morpc.NewMessageCodec(mf,
		morpc.WithCodecPayloadCopyBufferSize(16*1024),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize)))
	server, err := morpc.NewRPCServer(LogServiceRPCName, cfg.ServiceListenAddress, codec,
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
			respPool.Put(i.(morpc.RPCMessage).Message)
		})),
		morpc.WithServerLogger(service.logger),
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
		service.logger.Error("failed to start the server", zap.Error(err))
		if err := store.close(); err != nil {
			service.logger.Error("failed to close the store", zap.Error(err))
		}
		return nil, err
	}
	// start the heartbeat worker
	if !cfg.DisableWorkers {
		if err := service.stopper.RunNamedTask("log-heartbeat-worker", func(ctx context.Context) {
			service.logger.Info("logservice heartbeat worker started")

			// transfer morpc options via context
			ctx = SetBackendOptions(ctx, service.getBackendOptions()...)
			ctx = SetClientOptions(ctx, service.getClientOptions()...)
			service.heartbeatWorker(ctx)
		}); err != nil {
			return nil, err
		}
	}
	service.initTaskHolder()
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
	return err
}

func (s *Service) ID() string {
	return s.store.id()
}

func (s *Service) handleRPCRequest(ctx context.Context, req morpc.Message,
	seq uint64, cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "Service.handleRPCRequest")
	defer span.End()
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
	case pb.DN_HEARTBEAT:
		return s.handleDNHeartbeat(ctx, req), pb.LogRecordResponse{}
	case pb.CHECK_HAKEEPER:
		return s.handleCheckHAKeeper(ctx, req), pb.LogRecordResponse{}
	case pb.GET_CLUSTER_DETAILS:
		return s.handleGetClusterDetails(ctx, req), pb.LogRecordResponse{}
	case pb.GET_CLUSTER_STATE:
		return s.handleGetCheckerState(ctx, req), pb.LogRecordResponse{}
	case pb.GET_SHARD_INFO:
		return s.handleGetShardInfo(ctx, req), pb.LogRecordResponse{}
	default:
		panic("unknown log service method type")
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
	if err := s.store.getOrExtendDNLease(ctx, r.ShardID, r.DNID); err != nil {
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
	}
	return resp
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
	hb := req.LogHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addLogStoreHeartbeat(ctx, *hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = &cb
	}

	return resp
}

func (s *Service) handleCNHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	hb := req.CNHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addCNStoreHeartbeat(ctx, *hb); err != nil {
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

func (s *Service) handleDNHeartbeat(ctx context.Context, req pb.Request) pb.Response {
	hb := req.DNHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addDNStoreHeartbeat(ctx, *hb); err != nil {
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

func (s *Service) getBackendOptions() []morpc.BackendOption {
	return []morpc.BackendOption{
		morpc.WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return s.options.backendFilter == nil ||
				s.options.backendFilter(msg.(*RPCRequest), backendAddr)
		}),
	}
}

// NB: leave an empty method for future extension.
func (s *Service) getClientOptions() []morpc.ClientOption {
	return []morpc.ClientOption{
		morpc.WithClientTag("log-heartbeat"),
	}
}
